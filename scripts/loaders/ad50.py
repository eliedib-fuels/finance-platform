"""
Finance Platform — AD50 SAC Export Loader
Loads account-level P&L data from SAC AD50 exports into plan_data table.

SAC Export format (5 columns):
  Col 0: empty
  Col 1: AD50 account line (forward-filled)
  Col 2: Organisation — "0577005C MOBILE (0577005C)"
  Col 3: Period — 202601 (numeric YYYYMM)
  Col 4: Amount — MTD local currency

Key rules:
  - C suffix only (S = statistical mirror, opposite sign)
  - Strip C suffix to get base BU code
  - Apply Canada migration (0879/0865 → 1033)
  - Exclude: line 16 (ratio), FTE lines (separate table)
  - FTE: store in fte_data, AVG aggregation in queries
  - Period from col 3 (not header row)
  - All amounts in local currency — FX applied at load
"""

import pandas as pd
import logging
import re
from pathlib import Path

log = logging.getLogger(__name__)


# ── helpers ──────────────────────────────────────────────────────────────────

def parse_period(period_val) -> tuple:
    """
    Parse period from SAC format.
    202601 → (2026, 1)
    """
    try:
        s = str(int(period_val)).strip()
        year   = int(s[:4])
        period = int(s[4:])
        return year, period
    except Exception:
        return None, None


def extract_bu_code(org_str: str, migration_map: dict) -> str:
    """
    Extract clean BU code from organisation string.

    "0577005C MOBILE (0577005C)" → "0577005"
    "1033090C OPT - Hamilton (1033090C)" → "1033090"
    "0879090C OPT - Hamilton (0879090C)" → "1033090" (migrated)

    Rules:
      1. Take first word
      2. Strip C or S suffix
      3. Apply Canada migration map
    """
    if not org_str or org_str == "nan":
        return None

    raw = str(org_str).strip().split(" ")[0]

    # Must end in C or S — skip if not
    if not raw or raw[-1] not in ("C", "S"):
        return None

    # We only process C suffix
    if raw[-1] == "S":
        return None

    bu = raw[:-1]  # strip C

    # Apply Canada migration
    bu = migration_map.get(bu, bu)

    return bu


def detect_company(bu_code: str) -> str:
    """
    Derive company code from BU code prefix.
    0577xxx → 00577
    1033xxx → 01033
    0569xxx → 00569  (Panama)
    0682xxx → 00682  (Puerto Rico)
    0684xxx → 00684  (St Lucia)
    0568xxx → 00568  (Mexico)
    """
    if not bu_code:
        return None
    prefix = bu_code[:4]
    mapping = {
        "0577": "00577",
        "1033": "01033",
        "0569": "00569",
        "0682": "00682",
        "0684": "00684",
        "0568": "00568",
    }
    return mapping.get(prefix)


def get_entity_currency(company: str) -> str:
    """Return functional currency for a company code."""
    from scripts.config import ENTITIES
    info = ENTITIES.get(company)
    return info["currency"] if info else "USD"


# ── main loader ───────────────────────────────────────────────────────────────

def load_ad50(file_path: str, dry_run: bool = False) -> dict:
    """
    Load SAC AD50 export into plan_data and fte_data tables.

    Args:
        file_path: Path to AD50 Excel export
        dry_run:   If True, parse and validate but don't write to DB

    Returns:
        Summary dict with counts and any warnings
    """
    from scripts.config import (
        CANADA_BU_MIGRATION, AD50_SUFFIX_TO_USE,
        AD50_EXCLUDE_LINES, AD50_FTE_LINE,
        AD50_CASH_LINES, COMPANY_LEVEL_BUS
    )
    from scripts.loaders.rates import convert_to_usd
    from scripts.db import get_conn, delete_plan_period

    log.info(f"Loading AD50 from {file_path} dry_run={dry_run}")

    # ── 1. Read file ─────────────────────────────────────────────────────────
    df = pd.read_excel(file_path, header=None)

    # ── 2. Find data rows (period = numeric YYYYMM) ───────────────────────────
    col3_numeric = pd.to_numeric(df.iloc[:, 3], errors="coerce")
    data_mask    = col3_numeric.notna() & (col3_numeric > 200000)
    df_data      = df[data_mask].copy()
    df_data.columns = ["_empty", "account_raw", "org_raw", "period_raw", "amount_raw"]

    if df_data.empty:
        raise ValueError(f"No data rows found in {file_path}")

    # ── 3. Forward-fill account column ────────────────────────────────────────
    df_data["account_raw"] = df_data["account_raw"].ffill()

    # ── 4. Parse period ───────────────────────────────────────────────────────
    periods = col3_numeric[data_mask].dropna().unique()
    if len(periods) != 1:
        raise ValueError(
            f"Expected exactly 1 period in file, found: {periods}. "
            f"File should contain current month only."
        )

    fiscal_year, fiscal_period = parse_period(periods[0])
    if not fiscal_year:
        raise ValueError(f"Cannot parse period from {periods[0]}")

    version = f"ACTUAL_{fiscal_year}"
    log.info(f"Period: {fiscal_year}/{fiscal_period:02d} | Version: {version}")

    # ── 5. Parse amounts and BU codes ─────────────────────────────────────────
    df_data["amount"]  = pd.to_numeric(df_data["amount_raw"], errors="coerce")
    df_data["bu_code"] = df_data["org_raw"].apply(
        lambda x: extract_bu_code(str(x), CANADA_BU_MIGRATION)
    )

    # Drop rows where BU extraction failed (S suffix, headers etc)
    df_data = df_data.dropna(subset=["bu_code", "amount"])
    df_data = df_data[df_data["bu_code"] != ""]

    # ── 6. Derive company from BU code ────────────────────────────────────────
    df_data["company"] = df_data["bu_code"].apply(detect_company)

    # Flag BUs with no company mapping
    unknown_company = df_data[df_data["company"].isna()]["bu_code"].unique()
    if len(unknown_company) > 0:
        log.warning(f"BU codes with unknown company: {unknown_company.tolist()}")

    df_data = df_data.dropna(subset=["company"])

    # ── 7. Clean account line ─────────────────────────────────────────────────
    df_data["ad50_line_raw"] = df_data["account_raw"].astype(str).str.strip()

    # Extract just the code portion: "03 Revenue (REV)" → "03"
    # or "07A1" → "07A1"
    def extract_ad50_code(raw: str) -> str:
        raw = raw.strip()
        # Match leading code: digits + optional letters before space
        m = re.match(r'^(\d+[A-Za-z]*\d*)', raw)
        if m:
            return m.group(1).upper()
        return raw.split(" ")[0].upper()

    df_data["ad50_code"] = df_data["ad50_line_raw"].apply(extract_ad50_code)

    # ── 8. Route rows by type ─────────────────────────────────────────────────
    # FTE rows
    fte_mask  = df_data["ad50_line_raw"].str.contains("FTE", case=False, na=False)
    # Excluded lines (ratios)
    excl_mask = df_data["ad50_line_raw"].str.contains(
        "|".join(re.escape(x) for x in AD50_EXCLUDE_LINES),
        case=False, na=False
    ) if AD50_EXCLUDE_LINES else pd.Series(False, index=df_data.index)
    # Cash lines (store but flag)
    cash_mask = df_data["ad50_code"].isin(
        [c.split(" ")[0] for c in AD50_CASH_LINES]
    )

    df_financial = df_data[~fte_mask & ~excl_mask].copy()
    df_fte       = df_data[fte_mask].copy()

    log.info(f"Rows: {len(df_financial)} financial | "
             f"{len(df_fte)} FTE | "
             f"{excl_mask.sum()} excluded")

    # ── 9. Apply FX conversion ────────────────────────────────────────────────
    def apply_fx(row):
        currency = get_entity_currency(row["company"])
        amt_usd, amt_eur, r_usd, r_eur = convert_to_usd(
            float(row["amount"]), currency, fiscal_year
        )
        return pd.Series({
            "amount_usd": amt_usd,
            "amount_eur": amt_eur,
            "currency_local": currency,
        })

    log.info("Applying FX rates...")
    fx_cols = df_financial.apply(apply_fx, axis=1)
    df_financial = pd.concat([df_financial, fx_cols], axis=1)

    # ── 10. Write to DB ───────────────────────────────────────────────────────
    if dry_run:
        log.info("DRY RUN — skipping DB write")
        return _build_summary(df_financial, df_fte, fiscal_year,
                              fiscal_period, dry_run=True)

    # Delete existing data for this period before reload
    companies = df_financial["company"].unique()
    for company in companies:
        delete_plan_period(
            company, fiscal_year, fiscal_period,
            "ACTUAL", version
        )

    # Load financial rows → plan_data
    plan_rows = []
    for _, row in df_financial.iterrows():
        plan_rows.append((
            row["company"],
            row["bu_code"],
            row["ad50_code"],
            row["ad50_line_raw"],
            fiscal_year,
            fiscal_period,
            "ACTUAL",
            version,
            float(row["amount"]),
            row["currency_local"],
            row.get("amount_usd"),
            row.get("amount_eur"),
            "actual",
            "sac",
        ))

    with get_conn() as conn:
        conn.executemany("""
            INSERT OR REPLACE INTO plan_data
            (company, bu_code, ad50_line, ad50_label,
             fiscal_year, fiscal_period,
             plan_type, version,
             amount_lc, currency_local, amount_usd, amount_eur,
             entry_type, source)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, plan_rows)

    log.info(f"Loaded {len(plan_rows)} rows to plan_data")

    # Load FTE rows → fte_data
    if not df_fte.empty:
        fte_rows = []
        for _, row in df_fte.iterrows():
            fte_rows.append((
                row["company"],
                row["bu_code"],
                fiscal_year,
                fiscal_period,
                "total",               # prod/npbo split added later
                float(row["amount"]),
                Path(file_path).name,
            ))

        with get_conn() as conn:
            conn.executemany("""
                INSERT OR REPLACE INTO fte_data
                (company, bu_code, fiscal_year, fiscal_period,
                 fte_type, fte_count, source_file)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, fte_rows)

        log.info(f"Loaded {len(fte_rows)} FTE rows to fte_data")

    # Log load history
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO load_history
            (source_file, file_type, fiscal_year, fiscal_period,
             rows_inserted, validation_status)
            VALUES (?, 'AD50', ?, ?, ?, 'LOADED')
        """, (
            Path(file_path).name,
            fiscal_year, fiscal_period,
            len(plan_rows),
        ))

    return _build_summary(df_financial, df_fte, fiscal_year, fiscal_period)


def _build_summary(df_fin, df_fte, year, period, dry_run=False) -> dict:
    """Build a human-readable load summary."""
    summary = {
        "fiscal_year":       year,
        "fiscal_period":     period,
        "financial_rows":    len(df_fin),
        "fte_rows":          len(df_fte),
        "companies":         sorted(df_fin["company"].unique().tolist())
                             if not df_fin.empty else [],
        "bu_count":          df_fin["bu_code"].nunique()
                             if not df_fin.empty else 0,
        "dry_run":           dry_run,
    }

    # Revenue total for sanity check
    rev_mask = df_fin["ad50_code"].isin(["03", "01"])
    if rev_mask.any():
        summary["revenue_usd"] = float(
            df_fin[rev_mask]["amount_usd"].sum()
        ) if "amount_usd" in df_fin.columns else None

    # Unmapped BUs (company is None before filter)
    print(f"\n{'='*55}")
    print(f"AD50 LOAD SUMMARY — {year}/{period:02d}"
          + (" [DRY RUN]" if dry_run else ""))
    print(f"{'='*55}")
    print(f"  Financial rows : {summary['financial_rows']:,}")
    print(f"  FTE rows       : {summary['fte_rows']:,}")
    print(f"  BUs loaded     : {summary['bu_count']}")
    print(f"  Companies      : {', '.join(summary['companies'])}")
    if summary.get("revenue_usd") is not None:
        print(f"  Revenue (USD)  : ${summary['revenue_usd']:>15,.0f}")
    print(f"{'='*55}\n")

    return summary
