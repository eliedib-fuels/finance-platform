"""
Finance Platform — Pipeline Orchestrator
Main entry point for all data loading operations.

Usage:
  from scripts.pipeline import run_ad50, run_gl, run_all
"""

import logging
import time
from pathlib import Path
from datetime import datetime

log = logging.getLogger(__name__)


def setup_logging():
    """Configure logging to file and console."""
    from scripts.config import LOG_PATH
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
        ],
    )


def init_platform():
    """
    Initialise database and seed reference data.
    Safe to run multiple times — idempotent.
    """
    from scripts.db import init_db, seed_entities
    from scripts.config import DATA_DIR

    log.info("Initialising Finance Platform...")
    init_db()
    seed_entities()
    log.info("Platform initialised ✅")


def run_hierarchy(hierarchy_file: str = None,
                  ad50_details_file: str = None) -> dict:
    """
    Load BU hierarchy and AD50 line master.
    Run once initially, then whenever structure changes.
    """
    from scripts.config import DATA_DIR
    from scripts.loaders.hierarchy import load_hierarchy, load_ad50_lines

    results = {}

    hier_path = hierarchy_file or str(
        DATA_DIR / "BU Hierchy Project - March 26 Trade_OCM (final).xlsx"
    )
    if Path(hier_path).exists():
        log.info(f"Loading hierarchy: {hier_path}")
        results["hierarchy"] = load_hierarchy(hier_path)
    else:
        log.warning(f"Hierarchy file not found: {hier_path}")

    ad50_path = ad50_details_file or str(DATA_DIR / "AD50_details.xlsx")
    if Path(ad50_path).exists():
        log.info(f"Loading AD50 lines: {ad50_path}")
        results["ad50_lines"] = load_ad50_lines(ad50_path)
    else:
        log.warning(f"AD50 details file not found: {ad50_path}")

    return results


def run_rates(rates_file: str = None,
              fiscal_year: int = 2026) -> dict:
    """Load budget FX rates."""
    from scripts.config import DATA_DIR
    from scripts.loaders.rates import load_rates

    path = rates_file or str(DATA_DIR / "rates.xlsx")
    if not Path(path).exists():
        log.error(f"Rates file not found: {path}")
        return {"error": "Rates file not found"}

    return load_rates(path, fiscal_year=fiscal_year)


def run_ad50(file_path: str, dry_run: bool = False,
             skip_checks: bool = False) -> dict:
    """
    Load a SAC AD50 export file.

    Args:
        file_path:   Path to AD50 Excel file
        dry_run:     Parse and validate only — no DB write
        skip_checks: Skip pre-ingestion checks (not recommended)

    Returns:
        Result dict with counts and status
    """
    from scripts.checks.pre_ingestion import check_ad50_file
    from scripts.loaders.ad50 import load_ad50

    start = time.time()
    file_name = Path(file_path).name

    log.info(f"{'='*55}")
    log.info(f"AD50 PIPELINE START — {file_name}")
    log.info(f"{'='*55}")

    # Pre-ingestion checks
    if not skip_checks:
        result = check_ad50_file(file_path)
        if result.has_errors:
            log.error(f"Pre-ingestion checks FAILED — load blocked")
            return {
                "status":  "BLOCKED",
                "errors":  result.errors,
                "file":    file_name,
            }
        if result.has_warnings:
            log.warning(f"Pre-ingestion warnings present — proceeding")

    # Load data
    try:
        summary = load_ad50(file_path, dry_run=dry_run)
        elapsed = time.time() - start
        summary["status"]   = "DRY_RUN" if dry_run else "SUCCESS"
        summary["duration"] = round(elapsed, 1)
        log.info(f"AD50 pipeline complete in {elapsed:.1f}s ✅")
        return summary
    except Exception as e:
        log.error(f"AD50 pipeline failed: {e}", exc_info=True)
        return {"status": "ERROR", "error": str(e), "file": file_name}


def run_gl(file_path: str, dry_run: bool = False,
           skip_checks: bool = False) -> dict:
    """
    Load a GL transaction export file.

    Args:
        file_path:   Path to GL Excel file
        dry_run:     Parse and validate only — no DB write
        skip_checks: Skip pre-ingestion checks (not recommended)

    Returns:
        Result dict with counts and status
    """
    from scripts.checks.pre_ingestion import check_gl_file
    from scripts.loaders.gl import load_gl

    start = time.time()
    file_name = Path(file_path).name

    log.info(f"{'='*55}")
    log.info(f"GL PIPELINE START — {file_name}")
    log.info(f"{'='*55}")

    # Pre-ingestion checks
    if not skip_checks:
        result = check_gl_file(file_path)
        if result.has_errors:
            log.error("Pre-ingestion checks FAILED — load blocked")
            return {
                "status": "BLOCKED",
                "errors": result.errors,
                "file":   file_name,
            }

    # Load data
    try:
        summary = load_gl(file_path, dry_run=dry_run)
        elapsed = time.time() - start
        summary["status"]   = "DRY_RUN" if dry_run else "SUCCESS"
        summary["duration"] = round(elapsed, 1)
        log.info(f"GL pipeline complete in {elapsed:.1f}s ✅")
        return summary
    except Exception as e:
        log.error(f"GL pipeline failed: {e}", exc_info=True)
        return {"status": "ERROR", "error": str(e), "file": file_name}


def run_all_gl(gl_dir: str = None,
               dry_run: bool = False) -> dict:
    """
    Load all GL files found in the jan_feb directory.
    Processes each entity file independently.
    """
    from scripts.config import GL_DIR

    gl_path = Path(gl_dir) if gl_dir else GL_DIR
    if not gl_path.exists():
        log.error(f"GL directory not found: {gl_path}")
        return {"error": "GL directory not found"}

    gl_files = sorted(gl_path.glob("*.xlsx"))
    log.info(f"Found {len(gl_files)} GL files in {gl_path}")

    results = {}
    for f in gl_files:
        log.info(f"Processing {f.name}...")
        results[f.name] = run_gl(str(f), dry_run=dry_run)

    # Summary
    success = sum(1 for r in results.values() if r.get("status") == "SUCCESS")
    blocked = sum(1 for r in results.values() if r.get("status") == "BLOCKED")
    errors  = sum(1 for r in results.values() if r.get("status") == "ERROR")

    print(f"\n{'='*55}")
    print(f"ALL GL FILES SUMMARY")
    print(f"{'='*55}")
    for fname, res in results.items():
        icon = {"SUCCESS": "✅", "BLOCKED": "❌",
                "ERROR": "💥", "DRY_RUN": "🔍"}.get(res.get("status"), "?")
        rows = res.get("rows_loaded", 0)
        print(f"  {icon} {fname:<45} {rows:>8,} rows")
    print(f"{'='*55}")
    print(f"  ✅ {success} success | ❌ {blocked} blocked | 💥 {errors} errors")
    print(f"{'='*55}\n")

    return {
        "results": results,
        "success": success,
        "blocked": blocked,
        "errors":  errors,
    }


def run_validation(fiscal_year: int = None,
                   fiscal_period: int = None) -> dict:
    """
    Run post-load validation checks.
    Checks double-entry integrity and BS=PL.
    """
    from scripts.db import query, get_conn

    log.info("Running post-load validation...")
    results = []

    # Check 1 — Sum of all movements = 0 per company per period
    df = query("""
        SELECT
            company,
            fiscal_year,
            fiscal_period,
            SUM(amount_local) as total,
            COUNT(*) as row_count
        FROM gl_transactions
        WHERE fiscal_year = COALESCE(?, fiscal_year)
          AND fiscal_period = COALESCE(?, fiscal_period)
        GROUP BY company, fiscal_year, fiscal_period
        ORDER BY company, fiscal_year, fiscal_period
    """, (fiscal_year, fiscal_period))

    print(f"\n{'='*65}")
    print("VALIDATION — Double Entry Integrity (sum = 0)")
    print(f"{'='*65}")

    for _, row in df.iterrows():
        total  = float(row["total"])
        status = "✅ PASS" if abs(total) < 1.0 else "⚠️  WARN"
        print(f"  {status}  {row['company']}  "
              f"{int(row['fiscal_year'])}/{int(row['fiscal_period']):02d}  "
              f"sum={total:>12,.2f}  rows={int(row['row_count']):,}")

        # Log to validation_log
        with get_conn() as conn:
            conn.execute("""
                INSERT INTO validation_log
                (check_name, check_level, dimension, dimension_value,
                 db_total, difference, status)
                VALUES ('double_entry', 'ERROR', 'company_period',
                        ?, ?, ?, ?)
            """, (
                f"{row['company']}_{row['fiscal_year']}_{row['fiscal_period']}",
                total, abs(total),
                "PASS" if abs(total) < 1.0 else "WARN"
            ))
        results.append({
            "company":  row["company"],
            "year":     row["fiscal_year"],
            "period":   row["fiscal_period"],
            "total":    total,
            "status":   "PASS" if abs(total) < 1.0 else "WARN"
        })

    # Check 2 — BS movements + PL movements = 0
    df2 = query("""
        SELECT
            company,
            fiscal_year,
            fiscal_period,
            account_type,
            SUM(amount_local) as total
        FROM gl_transactions
        WHERE fiscal_year = COALESCE(?, fiscal_year)
          AND fiscal_period = COALESCE(?, fiscal_period)
          AND account_type IN ('PL', 'BS')
        GROUP BY company, fiscal_year, fiscal_period, account_type
        ORDER BY company, fiscal_year, fiscal_period, account_type
    """, (fiscal_year, fiscal_period))

    print(f"\n{'='*65}")
    print("VALIDATION — BS + PL = 0 (Accounting Equation)")
    print(f"{'='*65}")

    # Pivot to compare BS vs PL
    if not df2.empty:
        pivot = df2.pivot_table(
            index=["company", "fiscal_year", "fiscal_period"],
            columns="account_type",
            values="total",
            aggfunc="sum"
        ).reset_index()

        for _, row in pivot.iterrows():
            pl  = float(row.get("PL",  0) or 0)
            bs  = float(row.get("BS",  0) or 0)
            net = pl + bs
            status = "✅ PASS" if abs(net) < 1.0 else "⚠️  WARN"
            print(f"  {status}  {row['company']}  "
                  f"{int(row['fiscal_year'])}/{int(row['fiscal_period']):02d}  "
                  f"PL={pl:>12,.0f}  BS={bs:>12,.0f}  "
                  f"net={net:>10,.2f}")

    print(f"{'='*65}\n")
    return {"checks": results}


def health_check() -> dict:
    """Quick health check — show row counts for all tables."""
    from scripts.db import table_counts

    counts = table_counts()

    print(f"\n{'='*45}")
    print("PLATFORM HEALTH CHECK")
    print(f"{'='*45}")
    for table, count in counts.items():
        status = "✅" if count > 0 else "⚪"
        print(f"  {status} {table:<30} {count:>8,}")
    print(f"{'='*45}\n")

    return counts
