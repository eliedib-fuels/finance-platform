"""
Finance Platform — Account Master Loader
Reads NAM trade detail file and loads account → AD50 mapping.

Usage:
  python scripts/tools/load_account_master.py
  python scripts/tools/load_account_master.py --file data/NAM_-_trade_detail_per_account.xlsx
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


def parse_account_master(file_path: str) -> pd.DataFrame:
    all_accounts = []
    for sheet, ad50 in SHEETS.items():
        df   = pd.read_excel(file_path, sheet_name=sheet, header=None)
        col0 = df.iloc[:,0].dropna().astype(str).str.strip()
        for raw in col0.unique():
            if raw in ('nan', 'FPA_GROUP', 'Account', ''):
                continue
            if not (raw[0].isdigit() or '.' in raw[:7]):
                continue
            r = raw.strip()
            if '.PROD.' in r or '.NPBO.' in r:
                parts     = r.split('.')
                acct      = parts[0].strip()
                subtype   = parts[1]
                subline   = '07A' if subtype == 'PROD' else '07B'
                desc      = (r.split(' ',1)[1].replace('(FLEX)','').strip()
                             if ' ' in r else r)
                all_accounts.append({
                    'account_pattern': acct,
                    'account_desc':    desc,
                    'ad50_line':       ad50,
                    'ad50_subline':    subline,
                    'source':          'FLEX',
                    'match_type':      'exact',
                })
            elif '_' in r:
                acct = r.split(' ')[0].replace('_', '%')
                all_accounts.append({
                    'account_pattern': acct,
                    'account_desc':    r,
                    'ad50_line':       ad50,
                    'ad50_subline':    None,
                    'source':          'JDE',
                    'match_type':      'pattern',
                })
            elif r[0].isdigit():
                acct = r.split(' ')[0][:6]
                all_accounts.append({
                    'account_pattern': acct,
                    'account_desc':    r,
                    'ad50_line':       ad50,
                    'ad50_subline':    None,
                    'source':          'JDE',
                    'match_type':      'exact',
                })
    return pd.DataFrame(all_accounts).drop_duplicates(
        subset=['account_pattern','ad50_line','ad50_subline'])


def load_account_master(file_path: str) -> dict:
    from scripts.db import get_conn
    print(f"\nLoading account master from {file_path}...")
    df = parse_account_master(file_path)
    print(f"Parsed {len(df)} account mappings")
    for line, grp in df.groupby('ad50_line'):
        print(f"  {line}: {len(grp):3d} accounts  "
              f"{grp['source'].value_counts().to_dict()}")
    with get_conn() as conn:
        conn.execute("DELETE FROM account_master")
        df.to_sql('account_master', conn, if_exists='append', index=False)
    print(f"\n✅ Loaded {len(df)} rows to account_master")
    return {'rows_loaded': len(df)}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', default=
        'data/NAM_-_trade_detail_per_account.xlsx')
    args = parser.parse_args()
    if not Path(args.file).exists():
        print(f"File not found: {args.file}")
        sys.exit(1)
    load_account_master(args.file)
