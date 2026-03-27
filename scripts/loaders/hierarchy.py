"""
Finance Platform — Hierarchy Loader
Loads BU hierarchy from Excel into org_hierarchy table.
Handles: leading spaces in BU codes, effective dating,
         Canada migration (0879/0865 → 1033).
"""

import pandas as pd
import logging
from datetime import date

log = logging.getLogger(__name__)


def compute_sort_key(line: str) -> str:
    """
    Zero-pad AD50 line numbers for correct sorting.
    Max line = 36, so 2-digit padding is sufficient.

    Examples:
      "1"   → "01"
      "01"  → "01"
      "9"   → "09"
      "9a"  → "09a"
      "9A"  → "09a"  (case normalized)
      "07a" → "07a"
      "10"  → "10"   (correctly after 9b)
      "36"  → "36"
    """
    import re
    s = str(line).strip().lower()
    match = re.match(r'^(\d+)([a-z]*)$', s)
    if match:
        num   = int(match.group(1))
        alpha = match.group(2)
        return f"{num:02d}{alpha}"
    return s


def load_hierarchy(file_path: str,
                   effective_from: str = "2020-01-01",
                   sheet_name: str = "Trade_OCM") -> dict:
    """
    Load org hierarchy from Excel.

    Expected columns (Trade_OCM sheet):
      Business Unit | BU Name | Branch | Region | Business

    Returns summary dict with counts.
    """
    from scripts.config import CANADA_BU_MIGRATION
    from scripts.db import get_conn

    log.info(f"Loading hierarchy from {file_path} sheet={sheet_name}")

    # --- Read file ---
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # Keep only the 5 useful columns
    col_map = {
        "Business Unit": "bu_code",
        "BU Name":       "bu_name",
        "Branch":        "branch",
        "Region":        "region",
        "Business":      "business",
    }
    df = df.rename(columns=col_map)
    df = df[["bu_code", "bu_name", "branch", "region", "business"]].copy()

    # --- Clean BU codes ---
    # Strip spaces and zero-pad to 7 digits
    # e.g. "     0577002" → "0577002"
    # e.g. "568609" (Mexico, stored as int) → "0568609"
    def clean_bu_code(val):
        s = str(val).strip()
        # Remove any decimal (e.g. "568609.0" from Excel int)
        if "." in s:
            s = s.split(".")[0]
        # Zero-pad to 7 digits if purely numeric and < 7 chars
        if s.isdigit() and len(s) < 7:
            s = s.zfill(7)
        return s

    df["bu_code"] = df["bu_code"].apply(clean_bu_code)

    # Drop rows with no BU code
    df = df[df["bu_code"].notna() & (df["bu_code"] != "") & (df["bu_code"] != "nan")]

    # Drop header-like rows
    df = df[~df["bu_code"].str.contains("Business Unit", na=False)]

    # --- Apply Canada migration ---
    # 0879/0865 old codes → remap to 1033 equivalents
    df["bu_code"] = df["bu_code"].apply(
        lambda x: CANADA_BU_MIGRATION.get(x, x)
    )

    # --- Clean text fields ---
    for col in ["bu_name", "branch", "region", "business"]:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace("nan", None)

    # --- Load to DB ---
    loaded = 0
    skipped = 0

    with get_conn() as conn:
        # Deactivate all current records first
        conn.execute("""
            UPDATE org_hierarchy
            SET effective_to = ?
            WHERE effective_to IS NULL
        """, (date.today().isoformat(),))

        for _, row in df.iterrows():
            bu = row["bu_code"]
            if not bu or bu == "nan":
                skipped += 1
                continue
            try:
                conn.execute("""
                    INSERT INTO org_hierarchy
                    (bu_code, bu_name, branch, region, business,
                     effective_from, effective_to)
                    VALUES (?, ?, ?, ?, ?, ?, NULL)
                    ON CONFLICT(bu_code, effective_from) DO UPDATE SET
                        bu_name      = excluded.bu_name,
                        branch       = excluded.branch,
                        region       = excluded.region,
                        business     = excluded.business,
                        effective_to = NULL
                """, (
                    bu,
                    row.get("bu_name"),
                    row.get("branch"),
                    row.get("region"),
                    row.get("business"),
                    effective_from,
                ))
                loaded += 1
            except Exception as e:
                log.warning(f"Skipped BU {bu}: {e}")
                skipped += 1

    log.info(f"Hierarchy loaded: {loaded} BUs, {skipped} skipped")

    return {
        "loaded":   loaded,
        "skipped":  skipped,
        "total_in": len(df),
    }


def load_ad50_lines(file_path: str) -> dict:
    """
    Load AD50 line master from AD50_details.xlsx.

    Expected columns:
      AD line | AD50 line name

    Derives parent/sort automatically.
    """
    import re
    from scripts.db import get_conn

    log.info(f"Loading AD50 lines from {file_path}")

    df = pd.read_excel(file_path)

    # Normalize column names
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={
        "AD line":       "ad50_line",
        "AD50 line name": "ad50_label",
    })

    df["ad50_line"]  = df["ad50_line"].astype(str).str.strip()
    df["ad50_label"] = df["ad50_label"].astype(str).str.strip()

    # Drop empty rows
    df = df[df["ad50_line"].notna() & (df["ad50_line"] != "nan")]

    # Compute sort key
    df["ad50_sort_key"] = df["ad50_line"].apply(compute_sort_key)

    # Derive parent line
    # Rule: parent = numeric prefix only
    # e.g. "07A1" → parent "07", "09C" → parent "09", "13A" → parent "13"
    def get_parent(line: str) -> str:
        line = str(line).strip().lower()
        # If it's purely numeric (e.g. "1", "10") → no parent (IS a parent)
        if re.match(r'^\d+$', line):
            return None
        # If it has letters → parent is the numeric prefix
        match = re.match(r'^(\d+)', line)
        if match:
            return f"{int(match.group(1)):02d}"
        return None

    df["ad50_parent"]      = df["ad50_line"].apply(get_parent)
    df["ad50_parent_sort"] = df["ad50_parent"].apply(
        lambda x: compute_sort_key(x) if x else None
    )

    # Classify line type
    fte_lines  = ["35", "36"]
    cash_lines = ["22","23","24","25","26","27","28","29","30","31","32","33","34"]
    subtotals  = ["03", "22", "23"]  # derived lines not posting lines

    def get_line_type(line: str) -> str:
        base = re.match(r'^(\d+)', str(line).strip())
        num  = base.group(1) if base else ""
        if line.strip() in fte_lines:
            return "fte"
        if num in cash_lines:
            return "cash"
        return "financial"

    df["line_type"]   = df["ad50_line"].apply(get_line_type)
    df["is_ig"]       = df["ad50_label"].str.contains(
        "Intragroup|interco|IG sub|IG rev", case=False, na=False
    ).astype(int)
    df["is_subtotal"] = df["ad50_line"].apply(
        lambda x: 1 if str(x).strip() in subtotals else 0
    )

    # Get parent labels
    parent_map = dict(zip(df["ad50_line"].str.strip(),
                          df["ad50_label"].str.strip()))

    def get_parent_label(line: str) -> str:
        parent = get_parent(line)
        if not parent:
            return None
        # Try with and without leading zero
        return (parent_map.get(parent) or
                parent_map.get(str(int(parent))) or
                None)

    df["ad50_parent_label"] = df["ad50_line"].apply(get_parent_label)

    # --- Load to DB ---
    loaded = 0
    with get_conn() as conn:
        for _, row in df.iterrows():
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO ad50_lines
                    (ad50_line, ad50_label, ad50_sort_key,
                     ad50_parent, ad50_parent_label, ad50_parent_sort,
                     line_type, is_ig, is_subtotal)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    row["ad50_line"].strip(),
                    row["ad50_label"].strip(),
                    row["ad50_sort_key"],
                    row["ad50_parent"],
                    row["ad50_parent_label"],
                    row["ad50_parent_sort"],
                    row["line_type"],
                    int(row["is_ig"]),
                    int(row["is_subtotal"]),
                ))
                loaded += 1
            except Exception as e:
                log.warning(f"Skipped AD50 line {row['ad50_line']}: {e}")

    log.info(f"AD50 lines loaded: {loaded}")
    return {"loaded": loaded}
