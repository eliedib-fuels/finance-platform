"""
Load AD50 COA mapping into account_master table.
Maps account codes to AD50 sub-lines.

Usage:
  python scripts/tools/load_coa.py --file data/AD50_COA.xlsx
"""

import pandas as pd
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def load_coa(file_path: str) -> dict:
    from scripts.db import get_conn

    df = pd.read_excel(file_path)
    df.columns = ['account', 'description', 'ad50_line', 'alloc_base']
    df['account']   = df['account'].astype(str).str.strip()
    df['ad50_line'] = df['ad50_line'].astype(str).str.strip()

    # Derive parent line from sub-line
    # 9A,9B,9C... → 09
    # 7A1,7B1...  → 07
    # 13A,13B...  → 13
    def get_parent(line):
        if line.startswith('9'):   return '09'
        if line.startswith('7'):   return '07'
        if line.startswith('8'):   return '08'
        if line.startswith('13'):  return '13'
        if line.startswith('17'):  return '17'
        if line.startswith('19'):  return '19'
        if line.startswith('20'):  return '20'
        if line in ('1','2','4','5','10','35','36'): return line.zfill(2)
        return line

    df['ad50_parent'] = df['ad50_line'].apply(get_parent)

    # Build account_master rows
    rows = []
    for _, r in df.iterrows():
        acct = str(r['account']).strip()
        if not acct or acct == 'nan':
            continue

        # Determine match type
        if '_' in acct:
            match_type = 'pattern'
            pattern    = acct  # keep _ as SQL wildcard
        else:
            match_type = 'exact'
            pattern    = acct

        rows.append({
            'account_pattern': pattern,
            'account_desc':    str(r['description']).strip(),
            'ad50_line':       r['ad50_parent'],
            'ad50_subline':    r['ad50_line'],
            'source':          'COA',
            'match_type':      match_type,
        })

    df_out = pd.DataFrame(rows).drop_duplicates(
        subset=['account_pattern','ad50_subline'])

    print(f'\nLoading COA account master...')
    print(f'Total mappings: {len(df_out)}')
    print(f'\nBy AD50 line:')
    for line, grp in df_out.groupby('ad50_line'):
        sublines = sorted(grp['ad50_subline'].unique())
        print(f'  {line}: {len(grp):3d} accounts  '
              f'sub-lines: {sublines}')

    with get_conn() as conn:
        # Remove existing COA entries, keep SAC-derived ones
        conn.execute("DELETE FROM account_master WHERE source='COA'")
        df_out.to_sql('account_master', conn,
                      if_exists='append', index=False)

    print(f'\n✅ Loaded {len(df_out)} COA mappings to account_master')
    return {'rows': len(df_out)}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--file', default='data/AD50_COA.xlsx')
    args = parser.parse_args()
    load_coa(args.file)
