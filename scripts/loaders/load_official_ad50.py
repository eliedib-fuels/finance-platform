"""
Finance Platform — Official AD50 Loader
Loads the official AD50 monthly report into official_ad50 table.

Format:
  Multiple tabs: Total, US, CentralAm, MGT, OCM (+ others)
  Header rows 14-20: RateSet | Periods | Organisation | Version
  Data row 20: Account | Label | 202401 | 202402 | ...
  Data rows 21+: AD50 line | Label | amounts (USD at budget rate)

Usage:
  python scripts/loaders/load_official_ad50.py --file data/official_ad50.xlsx
  python scripts/loaders/load_official_ad50.py --file data/official_ad50.xlsx --dry-run
"""

import pandas as pd
import re
import sys
import argparse
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
log = logging.getLogger(__name__)

# AD50 lines that are subtotals (computed, not loaded from source)
SUBTOTAL_LINES = {'03', '06', '11', '12', '14', '15'}

# Valid AD50 lines to load
VALID_LINES = {
    '01','02','04','05','07','07A','07B',
    '08','09','09A','09B','09C','09D','09E',
    '09F','09G','09H','09I','09J','09K','09L','09M',
    '10','13','13A','13B','13C','13D','13E','13F','13G',
}


def to_period_str(v) -> str:
    """Convert 202401 or 202401.0 to '202401'."""
    try:
        f = float(v)
        if f == int(f):
            return str(int(f))
    except (ValueError, TypeError):
        pass
    return str(v).strip()


def parse_tab(df_raw: pd.DataFrame, tab_name: str,
              target_periods: list = None) -> pd.DataFrame:

    rows       = []
    period_cols = {}
    header_row  = None
    acct_col    = None

    for i, row in df_raw.iterrows():
        vals = [to_period_str(v) for v in row.values]

        # Period row: 3+ YYYYMM values anywhere in the row
        periods_found = [v for v in vals if re.match(r'^20\d{4}$', v)]
        if len(periods_found) >= 3 and not period_cols:
            for j, val in enumerate(row.values):
                s = to_period_str(val)
                if re.match(r'^20\d{4}$', s):
                    yyyymm = int(s)
                    if target_periods is None or yyyymm in target_periods:
                        period_cols[j] = yyyymm
            continue

        # Header row: contains 'Account' anywhere in row
        if period_cols and not header_row:
            for j, val in enumerate(row.values):
                if str(val).strip() == 'Account':
                    header_row = i
                    acct_col   = j
                    break
            if header_row is not None:
                break

    if header_row is None or not period_cols:
        log.warning(f"Tab '{tab_name}': cannot find header row "
                    f"(period_cols={len(period_cols)}, "
                    f"header_row={header_row})")
        return pd.DataFrame()

    # Label column = acct_col + 1
    label_col = acct_col + 1

    # Data starts after header row
    data = df_raw.iloc[header_row + 1:].reset_index(drop=True)

    for _, row in data.iterrows():
        line_raw = str(row.iloc[acct_col]).strip()
        label    = str(row.iloc[label_col]).strip() \
                   if label_col < len(row) else ''

        if line_raw in ('nan', '', 'None'):
            continue
        if line_raw in SUBTOTAL_LINES:
            continue
        if line_raw not in VALID_LINES:
            continue

        for col_idx, yyyymm in period_cols.items():
            try:
                raw_val = row.iloc[col_idx]
                if str(raw_val).strip() in ('nan','','None','-'):
                    amount = 0.0
                else:
                    amount = float(raw_val)
            except (ValueError, TypeError):
                amount = 0.0

            fiscal_year   = int(str(yyyymm)[:4])
            fiscal_period = int(str(yyyymm)[4:])

            rows.append({
                'tab':           tab_name,
                'ad50_line':     line_raw,
                'ad50_label':    label,
                'fiscal_year':   fiscal_year,
                'fiscal_period': fiscal_period,
                'amount_usd':    amount,
                'plan_type':     'ACTUAL',
            })

    return pd.DataFrame(rows)


def load_official_ad50(file_path: str,
                       target_periods: list = None,
                       dry_run: bool = False) -> dict:
    """Load official AD50 file into official_ad50 table."""
    from scripts.db import get_conn

    xl     = pd.ExcelFile(file_path)
    sheets = xl.sheet_names
    print(f"\n{'='*60}")
    print(f"OFFICIAL AD50 LOADER")
    print(f"File:   {Path(file_path).name}")
    print(f"Tabs:   {sheets}")
    if target_periods:
        print(f"Periods: {target_periods}")
    else:
        print(f"Periods: ALL")
    print(f"{'='*60}")

    all_dfs = []

    for sheet in sheets:
        df_raw = pd.read_excel(file_path, sheet_name=sheet,
                               header=None)
        df = parse_tab(df_raw, tab_name=sheet,
                       target_periods=target_periods)
        if df.empty:
            print(f"  {sheet:<15} 0 rows")
            continue

        periods = df['fiscal_year'].astype(str) + '/' + \
                  df['fiscal_period'].apply(lambda x: f'{int(x):02d}')
        print(f"  {sheet:<15} {len(df):>6} rows  "
              f"({periods.nunique()} periods, "
              f"{df['ad50_line'].nunique()} lines)")
        all_dfs.append(df)

    if not all_dfs:
        print("No data parsed")
        return {'rows': 0}

    df_all = pd.concat(all_dfs, ignore_index=True)

    # Summary by period
    print(f"\nTotal rows: {len(df_all):,}")
    print(f"\nSample — Billing (01) by tab, latest period:")
    billing = df_all[df_all['ad50_line'] == '01']
    if not billing.empty:
        latest = billing['fiscal_year'] * 100 + billing['fiscal_period']
        latest_period = latest.max()
        sample = billing[
            billing['fiscal_year'] * 100 + billing['fiscal_period']
            == latest_period
        ]
        for _, r in sample.iterrows():
            v = float(r['amount_usd'])/1000
            d = f'({abs(v):,.1f})' if v < 0 else f'{v:,.1f}'
            print(f"  {r.tab:<15} {d:>10} kUSD")

    if dry_run:
        print(f"\n[DRY RUN — no DB write]")
        return {'rows': len(df_all), 'dry_run': True}

    # Write to DB
    with get_conn() as conn:
        # Delete existing periods being loaded
        periods = df_all[['fiscal_year','fiscal_period']].drop_duplicates()
        deleted = 0
        for _, row in periods.iterrows():
            cur = conn.execute("""
                DELETE FROM official_ad50
                WHERE fiscal_year=? AND fiscal_period=?
            """, (int(row.fiscal_year), int(row.fiscal_period)))
            deleted += cur.rowcount

        print(f"\nDeleted {deleted:,} existing rows")

        # Insert
        CHUNK = 500
        inserted = 0
        for i in range(0, len(df_all), CHUNK):
            chunk = df_all.iloc[i:i+CHUNK]
            chunk.to_sql('official_ad50', conn,
                         if_exists='append', index=False)
            inserted += len(chunk)

        print(f"Inserted {inserted:,} rows")

    print(f"\n✅ Official AD50 load complete")
    return {'rows': len(df_all)}


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description='Load official AD50 monthly report'
    )
    parser.add_argument('--file', required=True,
                        help='Path to official AD50 Excel file')
    parser.add_argument('--periods', type=str, default=None,
                        help='Comma-separated YYYYMM e.g. 202601,202602')
    parser.add_argument('--year', type=int, default=None,
                        help='Load all periods for one year')
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    target = None
    if args.periods:
        target = [int(p.strip()) for p in args.periods.split(',')]
    elif args.year:
        target = [args.year * 100 + m for m in range(1, 13)]

    if not Path(args.file).exists():
        print(f"File not found: {args.file}")
        sys.exit(1)

    load_official_ad50(args.file, target_periods=target,
                       dry_run=args.dry_run)
