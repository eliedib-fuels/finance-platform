"""
Finance Platform — Pre-Ingestion Checks
Runs before any data touches the DB.
Blocking errors stop the load entirely.
Warnings proceed with confirmation.

Checks for GL files:
  1. File readable and correct format
  2. Expected columns present
  3. Ledger types found
  4. Unmapped BU codes (BLOCKING — full list shown)
  5. Unmapped accounts (BLOCKING — full list shown)
  6. Period consistency (single period only)
  7. Large amounts (non-blocking, notify only)
  8. Duplicate load detection

Checks for AD50 files:
  1. File readable
  2. Expected format (5 columns, period in col 3)
  3. C suffix BUs present
  4. Unmapped BU codes (BLOCKING)
  5. Period consistency
"""

import pandas as pd
import logging
import re
from pathlib import Path
from datetime import datetime

log = logging.getLogger(__name__)


# ── Result container ──────────────────────────────────────────────────────────

class CheckResult:
    def __init__(self):
        self.errors   = []   # blocking — stop load
        self.warnings = []   # non-blocking — proceed with info
        self.info     = []   # informational only

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    @property
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0

    def add_error(self, check: str, detail: str):
        self.errors.append({"check": check, "detail": detail})
        log.error(f"[BLOCK] {check}: {detail}")

    def add_warning(self, check: str, detail: str):
        self.warnings.append({"check": check, "detail": detail})
        log.warning(f"[WARN]  {check}: {detail}")

    def add_info(self, check: str, detail: str):
        self.info.append({"check": check, "detail": detail})
        log.info(f"[INFO]  {check}: {detail}")

    def print_report(self, file_name: str):
        print(f"\n{'='*60}")
        print(f"PRE-INGESTION REPORT — {file_name}")
        print(f"Run at: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"{'='*60}")

        if self.info:
            for item in self.info:
                print(f"  ✅ {item['check']}: {item['detail']}")

        if self.warnings:
            print()
            for item in self.warnings:
                print(f"  ⚠️  {item['check']}")
                print(f"     {item['detail']}")

        if self.errors:
            print()
            for item in self.errors:
                print(f"  ❌ {item['check']} — LOAD BLOCKED")
                print(f"     {item['detail']}")

        print()
        if self.has_errors:
            print(f"  RESULT: ❌ BLOCKED — {len(self.errors)} error(s)")
            print(f"          Resolve all errors before loading.")
        elif self.has_warnings:
            print(f"  RESULT: ⚠️  {len(self.warnings)} warning(s) — load can proceed")
        else:
            print(f"  RESULT: ✅ ALL CHECKS PASSED")
        print(f"{'='*60}\n")


# ── GL pre-checks ─────────────────────────────────────────────────────────────

def check_gl_file(file_path: str) -> CheckResult:
    """
    Run all pre-ingestion checks on a GL export file.
    Returns CheckResult — caller decides whether to proceed.
    """
    from scripts.config import VALID_LEDGERS, LARGE_AMOUNT_THRESHOLD
    from scripts.db import query

    result = CheckResult()
    file_name = Path(file_path).name

    # ── Check 1: File exists and readable ─────────────────────────────────────
    path = Path(file_path)
    if not path.exists():
        result.add_error("File exists", f"File not found: {file_path}")
        result.print_report(file_name)
        return result

    result.add_info("File exists", f"{file_name} found")

    # ── Check 2: Read file and find header ────────────────────────────────────
    try:
        xl = pd.ExcelFile(file_path)
        # Format B files often have no GL Detail sheet
        # Scan all sheets to find the one with data
        df_raw = None
        fmt    = "A"

        # Priority: GL Detail sheet first
        if "GL Detail" in xl.sheet_names:
            df_raw = pd.read_excel(file_path, header=None,
                                   sheet_name="GL Detail")
            # Detect format
            for i, row in df_raw.head(20).iterrows():
                vals_lower = [str(v).strip().lower()
                              for v in row.values]
                if "bu.obj account" in vals_lower:
                    fmt = "B"
                    break
                if "account" in vals_lower and "period" in vals_lower:
                    fmt = "A"
                    break
        else:
            # Scan sheets for data — skip sample/template
            for sname in xl.sheet_names:
                if any(skip in str(sname).lower()
                       for skip in ["sample", "template", "qaa"]):
                    continue
                candidate = pd.read_excel(file_path, header=None,
                                          sheet_name=sname)
                if len(candidate) < 10:
                    continue
                for i, row in candidate.head(20).iterrows():
                    vals_lower = [str(v).strip().lower()
                                  for v in row.values]
                    if "bu.obj account" in vals_lower:
                        fmt    = "B"
                        df_raw = candidate
                        break
                    if ("account" in vals_lower and
                            "period" in vals_lower):
                        fmt    = "A"
                        df_raw = candidate
                        break
                if df_raw is not None:
                    break

        if df_raw is None:
            df_raw = pd.read_excel(file_path, header=None,
                                   sheet_name=0)

    except Exception as e:
        result.add_error("File readable", f"Cannot read Excel file: {e}")
        result.print_report(file_name)
        return result

    result.add_info("GL format", f"Format {fmt} detected")

    # Find header row — handles both Format A and B
    header_row = None
    for i, row in df_raw.iterrows():
        vals = [str(v).strip().lower() for v in row.values]
        has_account = any(v in ("account", "account number",
                                "object account") for v in vals)
        has_period  = "period" in vals
        has_buobj   = "bu.obj account" in vals
        if (has_account and has_period) or has_buobj:
            header_row = i
            break

    if header_row is None:
        result.add_error(
            "Header row",
            "Cannot find header row. Expected 'Account'+'Period' "
            "(Format A) or 'BU.OBJ Account' (Format B)."
        )
        result.print_report(file_name)
        return result

    result.add_info("Header row", f"Found at row {header_row}")

    # For Format B — set headers and skip standard column checks
    if fmt == "B":
        df_raw.columns = df_raw.iloc[header_row].astype(str).str.strip()
        df = df_raw.iloc[header_row + 1:].reset_index(drop=True)
        # Get ledger counts from Format B
        ledger_col = next((c for c in df.columns
                           if "ledger" in c.lower()), None)
        if ledger_col:
            lc = df[ledger_col].value_counts()
            for ledger, count in lc.items():
                if str(ledger).strip() in VALID_LEDGERS:
                    result.add_info(f"Ledger {ledger}",
                                    f"{count:,} rows")
        # BU check for Format B
        if "BU.OBJ Account" in df.columns:
            bus_b = set()
            for val in df["BU.OBJ Account"].dropna():
                s = str(val).strip()
                if "." in s:
                    bus_b.add(s.split(".")[0].strip())
            hier = query("""
                SELECT DISTINCT bu_code FROM org_hierarchy
                WHERE effective_to IS NULL
            """)
            bus_in_db  = set(hier["bu_code"].tolist()) \
                if not hier.empty else set()
            from scripts.config import COMPANY_LEVEL_BUS
            acceptable = bus_in_db | set(COMPANY_LEVEL_BUS)
            unmapped_b = sorted(b for b in bus_b
                                if b not in acceptable)
            if unmapped_b:
                details = "\n".join(
                    f"      {bu}" for bu in unmapped_b
                )
                result.add_error(
                    "Unmapped BU codes",
                    f"{len(unmapped_b)} BU(s) not in hierarchy:\n"
                    f"{details}"
                )
            else:
                result.add_info(
                    "BU mapping",
                    f"All {len(bus_b)} BUs mapped ✅"
                )
        result.print_report(file_name)
        return result

    # Set headers and get data (Format A)
    df_raw.columns = df_raw.iloc[header_row].astype(str).str.strip()
    df = df_raw.iloc[header_row + 1:].reset_index(drop=True)

    # ── Check 3: Required columns ─────────────────────────────────────────────
    required_cols = [
        "Account", "Period", "Amount",
        "Business Unit code", "Ledger type",
    ]
    # Also accept alternate names
    alt_names = {
        "Ledger Type": "Ledger type",
        "Business Unit Code": "Business Unit code",
        "GL Date ": "GL Date",
    }
    available = set(df.columns.tolist())
    for alt, std in alt_names.items():
        if alt in available:
            available.add(std)

    missing_cols = [c for c in required_cols if c not in available]
    if missing_cols:
        result.add_error(
            "Required columns",
            f"Missing columns: {missing_cols}\n"
            f"     Available: {list(df.columns)[:10]}..."
        )
    else:
        result.add_info("Required columns", "All present")

    if result.has_errors:
        result.print_report(file_name)
        return result

    # ── Check 4: Ledger types ─────────────────────────────────────────────────
    ledger_col = "Ledger type" if "Ledger type" in df.columns else "Ledger Type"
    ledger_counts = df[ledger_col].value_counts()
    valid_counts  = {k: v for k, v in ledger_counts.items()
                     if str(k).strip() in VALID_LEDGERS}
    invalid_found = {k: v for k, v in ledger_counts.items()
                     if str(k).strip() not in VALID_LEDGERS
                     and str(k) not in ["nan", "None"]}

    for ledger, count in valid_counts.items():
        result.add_info(f"Ledger {ledger}", f"{count:,} rows")

    if invalid_found:
        result.add_warning(
            "Unexpected ledgers",
            f"Will be excluded: {dict(invalid_found)}"
        )

    # ── Check 5: Period consistency ────────────────────────────────────────────
    periods = df["Period"].dropna().unique()
    # Filter to valid period format YYYY/NNN
    valid_periods = [p for p in periods
                     if re.match(r'^\d{4}/\d{3}$', str(p).strip())]

    if len(valid_periods) == 0:
        result.add_error("Period format",
                         f"No valid periods found. Sample: {periods[:3]}")
    elif len(valid_periods) == 1:
        result.add_info("Period", f"Single period confirmed: {valid_periods[0]}")
        year = int(str(valid_periods[0])[:4])
        period = int(str(valid_periods[0])[5:])
    else:
        result.add_info(
            "Multiple periods",
            f"Found {len(valid_periods)} periods: {valid_periods[:5]}"
            + ("..." if len(valid_periods) > 5 else "")
        )
        year   = int(str(valid_periods[0])[:4])
        period = int(str(valid_periods[0])[5:])

    # ── Check 6: Duplicate load detection ─────────────────────────────────────
    company_col = "Company"
    if company_col in df.columns and len(valid_periods) > 0:
        companies = df[company_col].astype(str).str.strip().str.zfill(5).unique()
        for company in companies:
            if not company or company == "00nan":
                continue
            existing = query("""
                SELECT COUNT(*) as cnt FROM load_history
                WHERE company = ?
                  AND fiscal_year = ?
                  AND fiscal_period = ?
                  AND file_type = 'GL'
                  AND DATE(load_date) = DATE('now')
            """, (company, year, period))
            if not existing.empty and int(existing["cnt"].iloc[0]) > 0:
                result.add_warning(
                    "Duplicate load",
                    f"{company} period {year}/{period:02d} already loaded today. "
                    f"Will delete and reload."
                )

    # ── Check 7: Unmapped BU codes (BLOCKING) ─────────────────────────────────
    bu_col = ("Business Unit code" if "Business Unit code" in df.columns
              else "Business Unit Code")

    if bu_col in df.columns:
        # Get BU codes from file (non-null, non-balance-sheet)
        # First classify accounts to identify BS accounts
        acct_col = "Account"
        df_pl = df.copy()
        df_pl["_first_digit"] = df_pl[acct_col].astype(str).str.strip().str[0]
        df_pl["_is_bs"] = df_pl["_first_digit"].isin(["1", "2", "3", "4", "5"])

        # P&L accounts need BU mapping
        df_pl_only = df_pl[~df_pl["_is_bs"]]
        bus_in_file = set(
            df_pl_only[bu_col].astype(str).str.strip()
            .replace(["nan", "", "None", "0"], pd.NA)
            .dropna().unique()
        )

        # Get BUs in hierarchy
        hier = query("""
            SELECT DISTINCT bu_code FROM org_hierarchy
            WHERE effective_to IS NULL
        """)
        bus_in_db = set(hier["bu_code"].tolist()) if not hier.empty else set()

        # Also include company-level BUs that are acceptable
        from scripts.config import COMPANY_LEVEL_BUS
        acceptable = bus_in_db | set(COMPANY_LEVEL_BUS)

        unmapped = sorted(buses for buses in bus_in_file
                          if buses not in acceptable)

        if unmapped:
            # Get row counts per unmapped BU
            unmapped_detail = []
            for bu in unmapped:
                count = len(df_pl_only[
                    df_pl_only[bu_col].astype(str).str.strip() == bu
                ])
                # Get sample account
                sample_accts = df_pl_only[
                    df_pl_only[bu_col].astype(str).str.strip() == bu
                ][acct_col].head(3).tolist()
                unmapped_detail.append(
                    f"      {bu:15} ({count:4} rows) "
                    f"sample accounts: {sample_accts}"
                )
                # Log to unmapped_bus table
                _log_unmapped_bu(bu, Path(file_path).name, count)

            result.add_error(
                "Unmapped BU codes",
                f"{len(unmapped)} BU(s) not in hierarchy:\n" +
                "\n".join(unmapped_detail) +
                "\n\n     → Add to hierarchy Excel and reload, "
                "or run: python run.py --add-bu"
            )
        else:
            result.add_info("BU mapping",
                            f"All {len(bus_in_file)} P&L BUs mapped ✅")

    result.print_report(file_name)
    return result


# ── AD50 pre-checks ───────────────────────────────────────────────────────────

def check_ad50_file(file_path: str) -> CheckResult:
    """
    Run pre-ingestion checks on a SAC AD50 export file.
    """
    from scripts.config import CANADA_BU_MIGRATION
    from scripts.db import query

    result   = CheckResult()
    file_name = Path(file_path).name

    # ── Check 1: File exists ──────────────────────────────────────────────────
    if not Path(file_path).exists():
        result.add_error("File exists", f"Not found: {file_path}")
        result.print_report(file_name)
        return result

    result.add_info("File exists", file_name)

    # ── Check 2: Read and validate format ─────────────────────────────────────
    try:
        df = pd.read_excel(file_path, header=None)
    except Exception as e:
        result.add_error("File readable", str(e))
        result.print_report(file_name)
        return result

    if df.shape[1] < 5:
        result.add_error(
            "File format",
            f"Expected 5 columns, found {df.shape[1]}. "
            f"File may be wrong format or wrong export."
        )
        result.print_report(file_name)
        return result

    result.add_info("File format", f"{df.shape[0]} rows × {df.shape[1]} cols")

    # ── Check 3: Period detection ─────────────────────────────────────────────
    col3_numeric = pd.to_numeric(df.iloc[:, 3], errors="coerce")
    data_mask    = col3_numeric.notna() & (col3_numeric > 200000)
    data_rows    = df[data_mask]

    if data_rows.empty:
        result.add_error(
            "Period detection",
            "Cannot find data rows with YYYYMM period in column 3. "
            "Check SAC export format."
        )
        result.print_report(file_name)
        return result

    periods = col3_numeric[data_mask].unique()
    if len(periods) != 1:
        result.add_error(
            "Period consistency",
            f"Expected 1 period, found {len(periods)}: {periods.tolist()}. "
            f"Export should contain current month only."
        )
    else:
        year   = int(str(int(periods[0]))[:4])
        period = int(str(int(periods[0]))[4:])
        result.add_info("Period", f"{year}/{period:02d} ({int(periods[0])})")
        result.add_info("Data rows", f"{len(data_rows):,}")

    # ── Check 4: C suffix BUs ─────────────────────────────────────────────────
    orgs = data_rows.iloc[:, 2].astype(str).str.split(" ").str[0]
    c_orgs = orgs[orgs.str.endswith("C")]
    s_orgs = orgs[orgs.str.endswith("S")]
    result.add_info("C-suffix rows", f"{len(c_orgs):,} (will load)")
    result.add_info("S-suffix rows", f"{len(s_orgs):,} (will skip — statistical)")

    # ── Check 5: Unmapped BU codes (BLOCKING) ─────────────────────────────────
    # Extract and clean BU codes from C-suffix orgs
    bus_in_file = set()
    for org in c_orgs.unique():
        bu = org.rstrip("C")
        bu = CANADA_BU_MIGRATION.get(bu, bu)
        if bu:
            bus_in_file.add(bu)

    hier = query("""
        SELECT DISTINCT bu_code FROM org_hierarchy
        WHERE effective_to IS NULL
    """)
    bus_in_db = set(hier["bu_code"].tolist()) if not hier.empty else set()

    from scripts.config import COMPANY_LEVEL_BUS
    acceptable = bus_in_db | set(COMPANY_LEVEL_BUS)

    unmapped = sorted(bu for bu in bus_in_file if bu not in acceptable)

    if unmapped:
        unmapped_details = []
        for bu in unmapped:
            # Count rows
            count = sum(1 for org in c_orgs if org.rstrip("C") == bu or
                        CANADA_BU_MIGRATION.get(org.rstrip("C")) == bu)
            unmapped_details.append(f"      {bu:15} ({count} rows)")
            _log_unmapped_bu(bu, file_name, count)

        result.add_error(
            "Unmapped BU codes",
            f"{len(unmapped)} BU(s) not in hierarchy:\n" +
            "\n".join(unmapped_details) +
            "\n\n     → Add to hierarchy Excel and reload"
        )
    else:
        result.add_info("BU mapping",
                        f"All {len(bus_in_file)} BUs mapped ✅")

    result.print_report(file_name)
    return result


# ── Helpers ───────────────────────────────────────────────────────────────────

def _log_unmapped_bu(bu_code: str, source_file: str, row_count: int):
    """Log unmapped BU to DB for tracking."""
    try:
        from scripts.db import get_conn
        with get_conn() as conn:
            conn.execute("""
                INSERT INTO unmapped_bus
                    (bu_code, source_file, row_count, last_seen)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(bu_code) DO UPDATE SET
                    last_seen  = CURRENT_TIMESTAMP,
                    row_count  = excluded.row_count,
                    source_file = excluded.source_file
            """, (bu_code, source_file, row_count))
    except Exception as e:
        log.warning(f"Could not log unmapped BU {bu_code}: {e}")


def run_all_gl_checks(file_path: str) -> bool:
    """
    Convenience function — run checks and return True if load can proceed.
    Prints full report.
    """
    result = check_gl_file(file_path)
    return not result.has_errors


def run_all_ad50_checks(file_path: str) -> bool:
    """
    Convenience function — run AD50 checks and return True if load can proceed.
    """
    result = check_ad50_file(file_path)
    return not result.has_errors
