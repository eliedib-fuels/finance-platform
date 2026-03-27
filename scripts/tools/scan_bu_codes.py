"""
Finance Platform — BU Code Scanner
Scans all GL files and produces a complete list of BU codes
found in P&L accounts that are not yet in the hierarchy.

Usage:
  python scripts/tools/scan_bu_codes.py
  python scripts/tools/scan_bu_codes.py --gl-dir data/jan_feb
  python scripts/tools/scan_bu_codes.py --output data/missing_bus.xlsx
"""

import pandas as pd
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def scan_all_gl_files(gl_dir: str, output_file: str = None):
    """
    Scan all GL Excel files and find unmapped BU codes.
    """
    from scripts.config import ACCOUNT_CLASSIFICATION, COMPANY_LEVEL_BUS
    from scripts.db import query

    gl_path = Path(gl_dir)
    gl_files = sorted(gl_path.glob("*.xlsx"))

    if not gl_files:
        print(f"No Excel files found in {gl_dir}")
        return

    print(f"\nScanning {len(gl_files)} GL files in {gl_dir}...")
    print("="*60)

    # Collect all BU codes across all files
    all_bus = {}  # bu_code → {name, accounts, files, rows}

    for gl_file in gl_files:
        print(f"  Reading {gl_file.name}...")
        try:
            # Find GL Detail sheet
            xl = pd.ExcelFile(gl_file)
            sheet = "GL Detail" if "GL Detail" in xl.sheet_names else 0
            df_raw = pd.read_excel(gl_file, header=None,
                                   sheet_name=sheet)

            # Find header row
            header_row = None
            for i, row in df_raw.iterrows():
                vals = [str(v).strip().lower() for v in row.values]
                if "account" in vals and "period" in vals:
                    header_row = i
                    break

            if header_row is None:
                print(f"    ⚠️  Cannot find header row — skipping")
                continue

            df_raw.columns = df_raw.iloc[header_row].astype(str).str.strip()
            df = df_raw.iloc[header_row + 1:].reset_index(drop=True)

            # Get relevant columns
            col_map = {
                "Ledger type": "ledger", "Ledger Type": "ledger",
                "#NAME?": "ledger",
                "Account": "account",
                "Account description": "account_desc",
                "Business Unit code": "bu_code",
                "Business Unit Code": "bu_code",
            }
            df = df.rename(columns={k: v for k, v in col_map.items()
                                    if k in df.columns})

            # Check required columns exist
            if "bu_code" not in df.columns or "account" not in df.columns:
                print(f"    ⚠️  Missing bu_code or account column — skipping")
                continue

            # Filter valid ledgers
            if "ledger" in df.columns:
                df = df[df["ledger"].astype(str).str.strip()
                        .isin(["AA", "GP", "UE"])]

            # Filter P&L accounts only (6-9xxx)
            df["account"] = df["account"].astype(str).str.strip()
            df["first_digit"] = df["account"].str[0]
            df_pl = df[df["first_digit"].isin(["6", "7", "8", "9"])]

            # Clean BU codes
            df_pl = df_pl.copy()
            df_pl["bu_code"] = (df_pl["bu_code"].astype(str)
                                .str.strip()
                                .replace(["nan", "", "None", "0"], None))
            df_pl = df_pl.dropna(subset=["bu_code"])

            # Collect BU info
            for bu, grp in df_pl.groupby("bu_code"):
                if bu not in all_bus:
                    all_bus[bu] = {
                        "bu_code": bu,
                        "bu_name": "",
                        "row_count": 0,
                        "files": set(),
                        "sample_accounts": set(),
                        "sample_desc": set(),
                    }
                all_bus[bu]["row_count"] += len(grp)
                all_bus[bu]["files"].add(gl_file.name)

                # Sample account descriptions for context
                if "account_desc" in grp.columns:
                    descs = grp["account_desc"].dropna().unique()[:3]
                    all_bus[bu]["sample_desc"].update(descs)

                sample_accts = grp["account"].unique()[:3]
                all_bus[bu]["sample_accounts"].update(sample_accts)

        except Exception as e:
            print(f"    ❌ Error reading {gl_file.name}: {e}")
            continue

    print(f"\nTotal unique P&L BU codes found: {len(all_bus)}")

    # Compare against hierarchy
    hier = query("""
        SELECT DISTINCT bu_code, bu_name, branch, region, business
        FROM org_hierarchy
        WHERE effective_to IS NULL
    """)
    bus_in_db = set(hier["bu_code"].tolist()) if not hier.empty else set()
    acceptable = bus_in_db | set(COMPANY_LEVEL_BUS)

    # Split into mapped and unmapped
    mapped   = {bu: info for bu, info in all_bus.items()
                if bu in acceptable}
    unmapped = {bu: info for bu, info in all_bus.items()
                if bu not in acceptable}

    print(f"  ✅ Mapped:   {len(mapped)}")
    print(f"  ❌ Unmapped: {len(unmapped)}")

    if not unmapped:
        print("\n✅ All BU codes are mapped — hierarchy is complete!")
        return

    # Sort by row count descending (most important first)
    unmapped_sorted = sorted(
        unmapped.items(),
        key=lambda x: x[1]["row_count"],
        reverse=True
    )

    # Print console report
    print(f"\n{'='*80}")
    print("UNMAPPED BU CODES — Add to hierarchy Excel")
    print(f"{'='*80}")
    print(f"{'BU Code':<12} {'Rows':>6}  {'Files':<30}  "
          f"{'Sample Accounts'}")
    print("-"*80)
    for bu, info in unmapped_sorted:
        files_str = ", ".join(sorted(info["files"]))[:28]
        accts_str = ", ".join(sorted(info["sample_accounts"]))[:20]
        print(f"{bu:<12} {info['row_count']:>6}  {files_str:<30}  {accts_str}")

    # Build Excel output
    rows = []
    for bu, info in unmapped_sorted:
        # Auto-detect business from BU code pattern
        bu_num = bu.replace("0577", "").replace("1033", "")
        if any(bu.startswith(p) for p in
               ["0577901", "0577903", "0577905", "0577906",
                "0577907", "0577908", "0577909", "0577910",
                "0577914", "0577864", "0577865", "0577887",
                "0577888", "0577MTG", "0577920", "0577915"]):
            suggested_business = "Upstream & SAM"
            suggested_region   = "Upstream"
        else:
            suggested_business = "Trade & OCM"
            suggested_region   = ""

        rows.append({
            "Business Unit":      bu,
            "BU Name":            info.get("bu_name", ""),
            "Branch":             "",           # ← fill in
            "Region":             suggested_region,
            "Business":           suggested_business,
            "Row Count":          info["row_count"],
            "Files":              ", ".join(sorted(info["files"])),
            "Sample Accounts":    ", ".join(sorted(
                                  info["sample_accounts"]))[:50],
            "Sample Descriptions": ", ".join(sorted(
                                   info["sample_desc"]))[:80],
        })

    df_out = pd.DataFrame(rows)

    # Save to Excel
    output = output_file or "data/missing_bu_codes.xlsx"
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="Missing BUs")

        # Also write full BU list (mapped + unmapped)
        all_rows = []
        for bu, info in sorted(all_bus.items()):
            status = "✅ Mapped" if bu in acceptable else "❌ Missing"
            hier_row = hier[hier["bu_code"] == bu]
            all_rows.append({
                "Business Unit":   bu,
                "Status":          status,
                "Row Count":       info["row_count"],
                "Branch (in DB)":  hier_row["branch"].iloc[0]
                                   if not hier_row.empty else "",
                "Region (in DB)":  hier_row["region"].iloc[0]
                                   if not hier_row.empty else "",
                "Business (in DB)":hier_row["business"].iloc[0]
                                   if not hier_row.empty else "",
                "Files":           ", ".join(sorted(info["files"])),
                "Sample Accounts": ", ".join(sorted(
                                   info["sample_accounts"]))[:50],
            })
        pd.DataFrame(all_rows).to_excel(
            writer, index=False, sheet_name="All BUs"
        )

    print(f"\n✅ Output saved to: {output}")
    print(f"   Sheet 'Missing BUs' → {len(rows)} BUs to add to hierarchy")
    print(f"   Sheet 'All BUs'     → complete picture\n")
    print("Next steps:")
    print("  1. Open the Excel file")
    print("  2. Fill in Branch and BU Name columns")
    print("  3. Copy into hierarchy Excel")
    print("  4. Run: python run.py --hierarchy")
    print("  5. Run: python run.py --gl-all --dry-run\n")

    return df_out


def main():
    parser = argparse.ArgumentParser(
        description="Scan GL files for unmapped BU codes"
    )
    parser.add_argument("--gl-dir", default="data/jan_feb",
                        help="Directory containing GL Excel files")
    parser.add_argument("--output", default="data/missing_bu_codes.xlsx",
                        help="Output Excel file path")
    args = parser.parse_args()

    scan_all_gl_files(args.gl_dir, args.output)


if __name__ == "__main__":
    main()
