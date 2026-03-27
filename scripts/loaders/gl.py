"""
Finance Platform — GL Transaction Loader
Loads transaction-level GL data from Hubble/Vision exports.

GL Export format (20 columns, header at row 10):
  Line | Ledger Type | Company | Account | Account Description |
  Business Unit Code | BU.OBJ Account | Period | GL Date | Amount |
  Document | Document Type | Reversing Entry Code |
  Explanation Alpha | Explanation Remark |
  Invoice Number | PO Number | Address Book Number |
  Batch Number | User ID

Key rules:
  - Filter ledgers: AA, GP, UE only
  - French IFRS account classification by first digit
  - BS accounts (1-5xxx) have no BU → entity level only
  - Delete-and-replace per company + period
  - Apply budget FX rates at load time
  - Store amount_local, amount_usd, amount_eur
"""

import pandas as pd
import numpy as np
import logging
import re
from pathlib import Path
from datetime import datetime

log = logging.getLogger(__name__)

# ── Column mapping from GL export ────────────────────────────────────────────
GL_COLUMNS = {
    "Ledger type":                        "ledger_type",
    "Ledger Type":                        "ledger_type",
    "#NAME?":                             "ledger_type",
    "#name?":                             "ledger_type",
    "Company":                            "company",
    "Account":                            "account_code",
    "Account description":                "account_desc",
    "Account Description":                "account_desc",
    "Business Unit code":                 "bu_code",
    "Business Unit Code":                 "bu_code",
    "BU.OBJ Account":                     "bu_obj_account",
    "Period":                             "period",
    "GL Date":                            "gl_date",
    "GL Date ":                           "gl_date",
    "Amount":                             "amount_local",
    "Document (Voucher, Invoice, etc.)":  "document_number",
    "Document Type":                      "document_type",
    "Reversing entry code":               "reversing_entry_code",
    "Reversing Entry Code":               "reversing_entry_code",
    "Explanation - Alpha":                "explanation_alpha",
    "Explanation Remark":                 "explanation_remark",
    "Invoice Number":                     "invoice_number",
    "P.O. Number":                        "po_number",
    "Address Book number":                "address_book_number",
    "Address Book Number":                "address_book_number",
    "Batch Number":                       "batch_number",
    "User ID":                            "user_id",
}


# ── Account classification ────────────────────────────────────────────────────

def classify_account(account_code: str) -> tuple:
    """
    Classify account using French IFRS first-digit rule.
    Returns (account_type, account_class)
    """
    from scripts.config import ACCOUNT_CLASSIFICATION
    if not account_code:
        return "Unknown", "Unknown"
    first = str(account_code).strip()[0]
    result = ACCOUNT_CLASSIFICATION.get(first)
    if result:
        return result
    return "Unknown", "Unknown"


def parse_period(period_str: str) -> tuple:
    """
    Parse JDE period format.
    "2026/001" → (2026, 1)
    "2026/002" → (2026, 2)
    """
    try:
        parts = str(period_str).strip().split("/")
        year   = int(parts[0])
        period = int(parts[1])
        return year, period
    except Exception:
        return None, None


def find_header_row(df: pd.DataFrame) -> int:
    """
    Find the header row in a Hubble GL export.
    Looks for a row containing both 'Account' and 'Period'.
    Handles #NAME? Excel errors in some columns.
    """
    for i, row in df.iterrows():
        vals = [str(v).strip().lower() for v in row.values]
        has_account = any(v in ("account", "account number",
                                "object account") for v in vals)
        has_period  = "period" in vals
        if has_account and has_period:
            return i
    raise ValueError(
        "Cannot find header row in GL file.\n"
        "Expected a row containing 'Account' and 'Period' columns.\n"
        "Check the file is a valid Hubble GL export."
    )


# ── Format detection ──────────────────────────────────────────────────────────

def detect_gl_format(df_raw: pd.DataFrame) -> str:
    """
    Detect GL export format.

    Format A (standard Hubble):
      - Has filter rows at top (Company <<, BU <<, etc.)
      - Header row contains 'Account', 'Period', 'Amount'
      - Header typically at row 10-11

    Format B (alternate Hubble):
      - Header at row 0 or low row number
      - Has 'BU.OBJ Account' column (combined BU + account)
      - Has 'GL Date' instead of 'Period'
      - No separate 'Account' or 'Business Unit code' columns
    """
    # Check first 15 rows for format indicators
    for i, row in df_raw.head(15).iterrows():
        vals = [str(v).strip() for v in row.values]
        vals_lower = [v.lower() for v in vals]

        # Format B indicator — BU.OBJ Account in header
        if "bu.obj account" in vals_lower:
            return "B"

        # Format A indicator — standard header
        if ("account" in vals_lower and
                "period" in vals_lower and
                "amount" in vals_lower):
            return "A"

    return "A"  # default to standard format


# ── Main loader ───────────────────────────────────────────────────────────────

def load_gl(file_path: str,
            dry_run: bool = False,
            sheet_name: str = "GL Detail") -> dict:
    """
    Load GL transaction export into gl_transactions table.
    Handles two Hubble export formats automatically.
    """
    from scripts.config import VALID_LEDGERS, ENTITIES
    from scripts.loaders.rates import convert_to_usd
    from scripts.db import get_conn, delete_period_data

    log.info(f"Loading GL from {file_path}")

    # ── 1. Read raw file ──────────────────────────────────────────────────────
    xl = pd.ExcelFile(file_path)

    # Find the right sheet
    # Priority: 1) GL Detail  2) first non-sample sheet with >100 rows
    df_raw     = None
    used_sheet = None

    # Try GL Detail first
    if "GL Detail" in xl.sheet_names:
        df_raw     = pd.read_excel(file_path, header=None,
                                    sheet_name="GL Detail")
        used_sheet = "GL Detail"
    else:
        # Scan sheets — skip sample/template sheets
        for sname in xl.sheet_names:
            if any(skip in str(sname).lower()
                   for skip in ["sample", "template", "qaa"]):
                continue
            candidate = pd.read_excel(file_path, header=None,
                                       sheet_name=sname)
            if len(candidate) > 100:
                df_raw     = candidate
                used_sheet = sname
                break

    if df_raw is None:
        raise ValueError(
            f"Cannot find data sheet in {file_path}. "
            f"Available sheets: {xl.sheet_names}"
        )

    log.info(f"Using sheet: '{used_sheet}' — "
             f"{len(df_raw)} rows × {len(df_raw.columns)} cols")

    # ── 2. Detect format ──────────────────────────────────────────────────────
    fmt = detect_gl_format(df_raw)
    log.info(f"GL format detected: {fmt}")

    if fmt == "A":
        df = _parse_format_a(df_raw)
    else:
        df = _parse_format_b(df_raw)

    if df is None or df.empty:
        log.warning("No data rows after parsing")
        return {"rows_loaded": 0, "warning": "No data rows"}

    original_count = len(df)

    # ── 3. Filter ledgers ─────────────────────────────────────────────────────
    df["ledger_type"] = df["ledger_type"].astype(str).str.strip()
    df = df[df["ledger_type"].isin(VALID_LEDGERS)]
    log.info(f"After ledger filter (AA/GP/UE): {len(df):,} rows "
             f"({original_count - len(df):,} excluded)")

    if df.empty:
        return {"rows_loaded": 0, "warning": "No valid ledger rows"}

    # ── 4. Classify accounts ──────────────────────────────────────────────────
    classifications = df["account_code"].apply(
        lambda a: pd.Series(classify_account(a),
                            index=["account_type", "account_class"])
    )
    df = pd.concat([df, classifications], axis=1)

    # ── 5. Parse periods ──────────────────────────────────────────────────────
    if "period" in df.columns:
        df[["fiscal_year", "fiscal_period"]] = df["period"].apply(
            lambda p: pd.Series(parse_period(str(p)))
        )
    elif "gl_date" in df.columns:
        # Derive period from GL Date
        df["gl_date_parsed"] = pd.to_datetime(df["gl_date"],
                                               errors="coerce")
        df["fiscal_year"]   = df["gl_date_parsed"].dt.year
        df["fiscal_period"] = df["gl_date_parsed"].dt.month
        df["period"] = (df["fiscal_year"].astype(str) + "/" +
                        df["fiscal_period"].apply(
                            lambda x: f"{int(x):03d}"
                            if pd.notna(x) else ""))

    df = df.dropna(subset=["fiscal_year"])
    df["fiscal_year"]   = df["fiscal_year"].astype(int)
    df["fiscal_period"] = df["fiscal_period"].astype(int)

    # ── 6. Clean amounts ──────────────────────────────────────────────────────
    df["amount_local"] = pd.to_numeric(df["amount_local"],
                                        errors="coerce").fillna(0)

    # ── 7. Determine entity currency ──────────────────────────────────────────
    company_currencies = {}
    for company in df["company"].unique():
        company = str(company).strip().zfill(5)
        info = ENTITIES.get(company)
        if info:
            company_currencies[company] = info["currency"]
        else:
            log.warning(f"Unknown company {company} — assuming USD")
            company_currencies[company] = "USD"

    df["company"] = df["company"].astype(str).str.strip().str.zfill(5)

    # ── 8. Apply FX rates ─────────────────────────────────────────────────────
    log.info(f"Applying FX rates for {len(df):,} rows...")

    for company, grp in df.groupby("company"):
        currency = company_currencies.get(company, "USD")
        for fy in grp["fiscal_year"].unique():
            mask = ((df["company"] == company) &
                    (df["fiscal_year"] == fy))
            amounts = df.loc[mask, "amount_local"].values
            _, _, r_usd, r_eur = convert_to_usd(1.0, currency, int(fy))
            df.loc[mask, "amount_usd"]    = amounts * r_usd
            df.loc[mask, "amount_eur"]    = amounts * r_eur
            df.loc[mask, "eur_rate_used"] = r_eur
            df.loc[mask, "usd_rate_used"] = r_usd
            df.loc[mask, "currency_local"] = currency

    # ── 9. Validation ─────────────────────────────────────────────────────────
    validation_results = []
    for (company, fy, fp), grp in df.groupby(
            ["company", "fiscal_year", "fiscal_period"]):
        total  = grp["amount_local"].sum()
        status = "PASS" if abs(total) < 1.0 else "WARN"
        if status == "WARN":
            log.warning(f"Double-entry: {company} {fy}/{fp:02d} "
                        f"sum={total:,.2f}")
        validation_results.append({
            "company": company, "fy": fy, "fp": fp,
            "total": total, "status": status
        })

    # ── 10. Write to DB ───────────────────────────────────────────────────────
    if dry_run:
        return _gl_summary(df, validation_results, dry_run=True)

    # Delete and reload
    periods_loaded = df.groupby(
        ["company", "fiscal_year", "fiscal_period"]
    ).size().reset_index()

    total_deleted = 0
    for _, row in periods_loaded.iterrows():
        deleted = delete_period_data(
            str(row["company"]),
            int(row["fiscal_year"]),
            int(row["fiscal_period"]),
            "gl_transactions"
        )
        total_deleted += deleted

    # Insert in chunks
    source_file = Path(file_path).name
    df["source_file"] = source_file

    db_cols = [
        "company", "bu_code", "account_code", "account_desc",
        "bu_obj_account", "account_type", "account_class",
        "ledger_type", "period", "gl_date", "fiscal_year",
        "fiscal_period", "document_number", "document_type",
        "reversing_entry_code", "batch_number", "user_id",
        "explanation_alpha", "explanation_remark",
        "invoice_number", "po_number", "address_book_number",
        "amount_local", "currency_local", "amount_usd",
        "amount_eur", "eur_rate_used", "usd_rate_used",
        "source_file",
    ]
    db_cols  = [c for c in db_cols if c in df.columns]
    df_out   = df[db_cols].copy()

    if "gl_date" in df_out.columns:
        df_out["gl_date"] = df_out["gl_date"].astype(str).replace(
            "NaT", None
        )

    CHUNK = 49
    total_inserted = 0
    with get_conn() as conn:
        for i in range(0, len(df_out), CHUNK):
            chunk = df_out.iloc[i:i + CHUNK]
            chunk.to_sql("gl_transactions", conn,
                         if_exists="append", index=False,
                         method="multi")
            total_inserted += len(chunk)

    log.info(f"GL loaded: {total_inserted:,} rows inserted, "
             f"{total_deleted:,} deleted")

    # Log history
    with get_conn() as conn:
        for _, row in periods_loaded.iterrows():
            conn.execute("""
                INSERT INTO load_history
                (source_file, file_type, company,
                 fiscal_year, fiscal_period,
                 rows_inserted, rows_deleted, validation_status)
                VALUES (?, 'GL', ?, ?, ?, ?, ?, ?)
            """, (
                source_file,
                str(row["company"]),
                int(row["fiscal_year"]),
                int(row["fiscal_period"]),
                int(df[
                    (df["company"] == row["company"]) &
                    (df["fiscal_year"] == row["fiscal_year"]) &
                    (df["fiscal_period"] == row["fiscal_period"])
                ].shape[0]),
                total_deleted,
                "PASS" if all(r["status"] == "PASS"
                              for r in validation_results)
                else "WARN",
            ))

    return _gl_summary(df, validation_results)


# ── Format parsers ────────────────────────────────────────────────────────────

def _parse_format_a(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Parse standard Hubble GL format.
    Header row contains Account, Period, Amount columns.
    """
    header_row = find_header_row(df_raw)
    df_raw.columns = df_raw.iloc[header_row].astype(str).str.strip()
    df = df_raw.iloc[header_row + 1:].reset_index(drop=True)
    df = df.rename(columns=GL_COLUMNS)

    required = ["ledger_type", "company", "account_code",
                "period", "amount_local"]
    missing  = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Format A missing columns: {missing}")

    df["company"]      = df["company"].astype(str).str.strip().str.zfill(5)
    df["account_code"] = df["account_code"].astype(str).str.strip()

    if "bu_code" in df.columns:
        df["bu_code"] = df["bu_code"].astype(str).str.strip().replace(
            ["nan", "", "None", "0"], None
        )
    if "gl_date" in df.columns:
        df["gl_date"] = pd.to_datetime(df["gl_date"], errors="coerce")

    return df


def _parse_format_b(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Parse alternate Hubble GL format.
    - Header row contains 'BU.OBJ Account', 'GL Date', 'Amount'
    - No separate Account or Period columns
    - BU and Account embedded in 'BU.OBJ Account' (e.g. 0568841.408000)
    - Period derived from GL Date
    - May have 'Currency Amount' as local currency
    """
    # Find header row — look for BU.OBJ Account
    header_row = None
    for i, row in df_raw.iterrows():
        vals = [str(v).strip().lower() for v in row.values]
        if "bu.obj account" in vals:
            header_row = i
            break

    if header_row is None:
        raise ValueError("Format B: cannot find header row "
                         "with 'BU.OBJ Account'")

    df_raw.columns = df_raw.iloc[header_row].astype(str).str.strip()
    df = df_raw.iloc[header_row + 1:].reset_index(drop=True)

    log.info(f"Format B columns: {list(df.columns)}")

    # ── Map columns ───────────────────────────────────────────────────────────
    col_map_b = {
        "Company":                           "company",
        "Batch Number":                      "batch_number",
        "Batch Numb":                        "batch_number",
        "Document Type":                     "document_type",
        "Document (Voucher, Invoice, etc.)": "document_number",
        "P.O. Number":                       "po_number",
        "GL Date":                           "gl_date",
        "GL Date ":                          "gl_date",
        "BU.OBJ Account":                    "bu_obj_account",
        "Amount":                            "amount_local",
        "Currency Amount":                   "currency_amount",
        "Explanation - Alpha":               "explanation_alpha",
        "Explanation Remark":                "explanation_remark",
        "User ID":                           "user_id",
        "Ledger type":                       "ledger_type",
        "Ledger Type":                       "ledger_type",
        "Business Unit description":         "bu_desc",
        "Reporting line":                    "ad50_line_ref",
        "Sub reporting line 3":              "ad50_subline_ref",
        "Contract":                          "contract",
        "Address Book number":               "address_book_number",
        "Address Book Number":               "address_book_number",
        "Reversing entry code":              "reversing_entry_code",
    }
    df = df.rename(columns={k: v for k, v in col_map_b.items()
                             if k in df.columns})

    # ── Parse BU.OBJ Account → bu_code + account_code ────────────────────────
    # Format: "0568841.408000" → bu=0568841, account=408000
    # Format: "0568.401000"   → bu=0568, account=401000
    def parse_bu_obj(val):
        s = str(val).strip()
        if "." in s:
            parts = s.split(".")
            bu      = parts[0].strip()
            account = parts[1].strip() if len(parts) > 1 else ""
            return bu, account
        return s, ""

    if "bu_obj_account" in df.columns:
        parsed = df["bu_obj_account"].apply(
            lambda x: pd.Series(parse_bu_obj(x),
                                 index=["bu_code", "account_code"])
        )
        df["bu_code"]      = parsed["bu_code"]
        df["account_code"] = parsed["account_code"]

    # ── Clean ─────────────────────────────────────────────────────────────────
    df["company"]      = df["company"].astype(str).str.strip().str.zfill(5)
    df["account_code"] = df["account_code"].astype(str).str.strip()
    df["bu_code"]      = df["bu_code"].astype(str).str.strip().replace(
        ["nan", "", "None", "0"], None
    )

    # Remove total/summary rows (no account code)
    df = df[df["account_code"].str.match(r'^\d', na=False) |
            df["account_code"].str.match(r'^[A-Za-z]', na=False)]
    df = df[df["account_code"] != ""]

    # GL date
    df["gl_date"] = pd.to_datetime(df["gl_date"], errors="coerce")

    # Amount column IS the local currency amount (MXN for Mexico)
    # Currency Amount is a partial conversion field — do NOT use it
    df["amount_local"] = pd.to_numeric(df["amount_local"],
                                        errors="coerce").fillna(0)

    log.info(f"Format B parsed: {len(df):,} rows")
    return df


# ── Summary helper ────────────────────────────────────────────────────────────

def _gl_summary(df: pd.DataFrame,
                validations: list,
                dry_run: bool = False) -> dict:
    """Print and return GL load summary."""

    by_company = df.groupby("company").agg(
        rows=("amount_local", "count"),
        total_usd=("amount_usd", "sum"),
    ).reset_index()

    by_ledger = df.groupby("ledger_type")["amount_local"].agg(
        ["count", "sum"]
    ).reset_index()

    passes = sum(1 for v in validations if v["status"] == "PASS")
    warns  = sum(1 for v in validations if v["status"] == "WARN")

    print(f"\n{'='*60}")
    print(f"GL LOAD SUMMARY" + (" [DRY RUN]" if dry_run else ""))
    print(f"{'='*60}")
    print(f"  Total rows:  {len(df):,}")
    print()
    print("  By company:")
    for _, row in by_company.iterrows():
        print(f"    {row['company']}  {row['rows']:>8,} rows  "
              f"USD {row['total_usd']:>15,.0f}")
    print()
    print("  By ledger:")
    for _, row in by_ledger.iterrows():
        print(f"    {row['ledger_type']}  {row['count']:>8,} rows  "
              f"sum {row['sum']:>15,.2f}")
    print()
    print(f"  Double-entry checks: {passes} PASS | {warns} WARN")
    print(f"{'='*60}\n")

    return {
        "rows_loaded":       len(df),
        "companies":         df["company"].unique().tolist(),
        "validation_passes": passes,
        "validation_warns":  warns,
        "dry_run":           dry_run,
    }
