"""
Finance Platform — SAC Detail Loader
Reads NAM trade detail per account file.
Applies correct suffix + FX rules per entity.
Loads aggregated amounts into sac_detail table.
Handles Guyana as direct plug (no GL file available).

Usage:
  python scripts/tools/load_sac_detail.py
  python scripts/tools/load_sac_detail.py --file data/NAM_-_trade_detail_per_account.xlsx
  python scripts/tools/load_sac_detail.py --dry-run
"""

import pandas as pd
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

SHEETS = {
    '1- Billing':               '01',
    '2 - Wip':                  '02',
    '4 - IG revenue':           '04',
    '5 - IG subcon':            '05',
    '8 - Ext sub':              '08',
    '7 - Pers costs':           '07',
    '9 - Other costs':          '09',
    '10 -funct neutralization': '10',
}

# Entity rules: suffix + currency rate (→ USD)
# Rate = budget rate to EUR / USD→EUR rate
USD_EUR  = 0.869565
CAD_USD  = 0.628931 / USD_EUR
MXN_USD  = 0.044106 / USD_EUR
GYD_USD  = 0.0041   / USD_EUR

ENTITY_CONFIG = {
    '0577': ('C', 'USD', 1.0),
    '1033': ('C', 'CAD', CAD_USD),
    '0879': ('C', 'CAD', CAD_USD),
    '0865': ('C', 'CAD', CAD_USD),
    '0568': ('S', 'MXN', MXN_USD),
    '0569': ('S', 'USD', 1.0),
    '0682': ('S', 'USD', 1.0),
    '0684': ('S', 'USD', 1.0),
    '0755': ('C', 'GYD', GYD_USD),  # Guyana — plug from SAC
}


def load_sac_detail(file_path: str, dry_run: bool = False,
                    fiscal_year: int = 2026,
                    fiscal_period: int = 2) -> dict:
    """
    Load SAC detail per account into sac_detail table.
    """
    from scripts.db import get_conn

    print(f"\nLoading SAC detail from {file_path}...")
    print(f"Period: {fiscal_year}/{fiscal_period:02d}")

    all_rows = []

    for sheet, ad50_line in SHEETS.items():
        df = pd.read_excel(file_path, sheet_name=sheet, header=None)

        # Find data rows — period in col 2, amount in col 3
        df.columns = ['account_raw', 'org_raw', 'period_raw', 'amount_raw']
        df['amount'] = pd.to_numeric(df['amount_raw'], errors='coerce')
        df = df[df['amount'].notna()].copy()

        # Forward fill account
        df['account_raw'] = df['account_raw'].ffill()

        # Parse org → bu_code, entity, suffix
        df['org_str']   = df['org_raw'].astype(str).str.strip()
        df['bu_raw']    = df['org_str'].str.split(' ').str[0]
        df['suffix']    = df['bu_raw'].str[-1]
        df['bu_code']   = df['bu_raw'].str[:-1]
        df['entity']    = df['bu_code'].str[:4]

        # Parse account code
        def parse_acct(raw):
            raw = str(raw).strip()
            if '.PROD.' in raw or '.NPBO.' in raw:
                return raw.split('.')[0]
            return raw.split(' ')[0][:6]

        df['account_code'] = df['account_raw'].apply(parse_acct)

        # Filter to valid entities with correct suffix
        def is_valid(row):
            cfg = ENTITY_CONFIG.get(row['entity'])
            return cfg is not None and row['suffix'] == cfg[0]

        df = df[df.apply(is_valid, axis=1)].copy()

        if df.empty:
            continue

        # Apply FX conversion
        def convert(row):
            cfg = ENTITY_CONFIG.get(row['entity'], ('C', 'USD', 1.0))
            return row['amount'] * cfg[2]

        df['amount_usd'] = df.apply(convert, axis=1)
        df['currency']   = df['entity'].map(
            lambda e: ENTITY_CONFIG.get(e, ('C','USD',1.0))[1]
        )
        df['amount_lc']  = df['amount']

        # Tag Guyana as plug
        df['data_source'] = df['entity'].apply(
            lambda e: 'SAC_PLUG_GYD' if e == '0755' else 'SAC_DETAIL'
        )

        df['ad50_line']     = ad50_line
        df['fiscal_year']   = fiscal_year
        df['fiscal_period'] = fiscal_period

        all_rows.append(df[[
            'bu_code', 'entity', 'account_code', 'ad50_line',
            'fiscal_year', 'fiscal_period',
            'amount_lc', 'currency', 'amount_usd', 'data_source'
        ]])

    if not all_rows:
        print("No data rows found")
        return {'rows': 0}

    df_out = pd.concat(all_rows, ignore_index=True)

    # Summary
    print(f"\n{'='*60}")
    print(f"SAC DETAIL SUMMARY — {fiscal_year}/{fiscal_period:02d}")
    print(f"{'='*60}")
    print(f"{'AD50':<6} {'Description':<25} {'kUSD':>10}")
    print('-'*45)

    ad50_labels = {
        '01':'Billing', '02':'WIP and UI', '04':'IG Revenue',
        '05':'IG Subcon', '06':'Production', '07':'Personnel',
        '08':'Ext Subcon', '09':'Other Costs',
        '10':'Func Neutralization', '11':'Gross Profit',
    }

    totals = df_out.groupby('ad50_line')['amount_usd'].sum()
    prod_lines = ['01','02','04','05']
    cost_lines = ['07','08','09','10']

    for line in ['01','02','04','05']:
        v = totals.get(line, 0)
        k = v/1000
        d = f'({abs(k):,.1f})' if k < 0 else f'{k:,.1f}'
        print(f"  {line:<5} {ad50_labels.get(line,''):<25} {d:>10}")

    prod = sum(totals.get(l,0) for l in prod_lines)
    print('-'*45)
    print(f"  {'06':<5} {'Production':<25} {prod/1000:>10,.1f}")
    print('-'*45)

    for line in ['07','08','09','10']:
        v = totals.get(line, 0)
        k = v/1000
        d = f'({abs(k):,.1f})' if k < 0 else f'{k:,.1f}'
        print(f"  {line:<5} {ad50_labels.get(line,''):<25} {d:>10}")

    gp = prod + sum(totals.get(l,0) for l in cost_lines)
    print('='*45)
    print(f"  {'11':<5} {'Gross Profit':<25} {gp/1000:>10,.1f}")
    gm = gp/prod*100 if prod else 0
    print(f"  {'12':<5} {'Gross Margin %':<25} {gm:>9.1f}%")
    print(f"{'='*60}")
    print(f"\nTotal rows: {len(df_out):,}")

    # Guyana plug summary
    guyana = df_out[df_out['data_source']=='SAC_PLUG_GYD']
    if not guyana.empty:
        print(f"\nGuyana plug (0755 GYD): "
              f"{guyana['amount_usd'].sum()/1000:,.1f} kUSD "
              f"({len(guyana)} rows)")

    if dry_run:
        print("\n[DRY RUN — no DB write]")
        return {'rows': len(df_out), 'dry_run': True}

    # Write to DB
    with get_conn() as conn:
        # Delete existing for this period
        conn.execute("""
            DELETE FROM sac_detail
            WHERE fiscal_year = ? AND fiscal_period = ?
        """, (fiscal_year, fiscal_period))

        df_out.to_sql('sac_detail', conn,
                      if_exists='append', index=False)

    print(f"\n✅ Loaded {len(df_out):,} rows to sac_detail")
    return {'rows': len(df_out)}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', default=
        'data/NAM_-_trade_detail_per_account.xlsx')
    parser.add_argument('--year',   type=int, default=2026)
    parser.add_argument('--period', type=int, default=2)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()

    if not Path(args.file).exists():
        print(f"File not found: {args.file}")
        sys.exit(1)

    load_sac_detail(args.file, dry_run=args.dry_run,
                    fiscal_year=args.year,
                    fiscal_period=args.period)
