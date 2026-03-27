"""
Finance Platform — Reconciliation Tool
Compares SAC detail (account level) vs GL transactions.
Line by line, BU by BU.

Usage:
  python scripts/tools/reconcile.py
  python scripts/tools/reconcile.py --line 01
  python scripts/tools/reconcile.py --line all
  python scripts/tools/reconcile.py --output data/recon_feb26.xlsx
"""

import pandas as pd
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

CAD_USD = 0.628931 / 0.869565
MXN_USD = 0.044106 / 0.869565
GYD_USD = 0.0041   / 0.869565

ENTITY_CONFIG = {
    '0577': ('C', 'USD', 1.0),
    '1033': ('C', 'CAD', CAD_USD),
    '0879': ('C', 'CAD', CAD_USD),
    '0865': ('C', 'CAD', CAD_USD),
    '0568': ('S', 'MXN', MXN_USD),
    '0569': ('S', 'USD', 1.0),
    '0682': ('S', 'USD', 1.0),
    '0684': ('S', 'USD', 1.0),
    '0755': ('C', 'GYD', GYD_USD),
}

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

AD50_LABELS = {
    '01':'Billing', '02':'WIP and UI', '04':'IG Revenue',
    '05':'IG Subcon', '07':'Personnel', '08':'Ext Subcon',
    '09':'Other Costs', '10':'Func Neutralization',
}

SHEET_MAP = {v: k for k, v in SHEETS.items()}

def get_out_of_scope_bus(file_path: str,
                          fiscal_year: int = 2026,
                          fiscal_period: int = 2) -> set:
    """
    Discover BUs that have GL data but SAC = 0 across ALL AD50 lines.
    These are out of scope for the reconciliation.
    Rule: if SAC=0 AND GL>0 for a BU → exclude from GL entirely.
    If both SAC=0 AND GL=0 → BU stays (no activity, still in scope).
    """
    from scripts.db import query

    # Get all BUs with GL P&L activity in Trade & OCM
    trade_bus = query("""
        SELECT DISTINCT g.bu_code
        FROM gl_transactions g
        JOIN org_hierarchy h ON g.bu_code = h.bu_code
            AND h.effective_to IS NULL
            AND h.business IN ('Trade & OCM','Trade')
        WHERE g.fiscal_year  = ?
          AND g.fiscal_period = ?
          AND g.account_type  = 'PL'
    """, (fiscal_year, fiscal_period))

    if trade_bus.empty:
        return set()

    gl_bus = set(trade_bus['bu_code'].tolist())

    # Get all BUs with SAC activity across all lines
    sac_bus = set()
    for sheet, ad50 in SHEETS.items():
        df = pd.read_excel(file_path, sheet_name=sheet, header=None)
        df.columns = ['account_raw','org_raw','period_raw','amount_raw']
        df['amount'] = pd.to_numeric(df['amount_raw'], errors='coerce')
        df = df[df['amount'].notna() & (df['amount'] != 0)].copy()
        df['org_str'] = df['org_raw'].astype(str).str.strip()
        df['bu_raw']  = df['org_str'].str.split(' ').str[0]
        df['suffix']  = df['bu_raw'].str[-1]
        df['bu_code'] = df['bu_raw'].str[:-1]
        df['entity']  = df['bu_code'].str[:4]

        # Only valid entity+suffix combos
        def is_valid(row):
            cfg = ENTITY_CONFIG.get(row['entity'])
            return cfg is not None and row['suffix'] == cfg[0]

        df = df[df.apply(is_valid, axis=1)]
        sac_bus.update(df['bu_code'].unique())

    # Out of scope = in GL but NOT in SAC (with non-zero amounts)
    out_of_scope = gl_bus - sac_bus

    if out_of_scope:
        print(f"\n  Out-of-scope BUs (GL>0, SAC=0) — excluded:")
        for bu in sorted(out_of_scope):
            print(f"    {bu}")

    return out_of_scope

def get_sac_data(file_path: str, ad50_line: str) -> pd.DataFrame:
    """Get SAC amounts from detail file for one AD50 line."""
    sheet = SHEET_MAP.get(ad50_line)
    if not sheet:
        return pd.DataFrame()

    df = pd.read_excel(file_path, sheet_name=sheet, header=None)
    df.columns = ['account_raw', 'org_raw', 'period_raw', 'amount_raw']
    df['amount'] = pd.to_numeric(df['amount_raw'], errors='coerce')
    df = df[df['amount'].notna()].copy()
    df['account_raw'] = df['account_raw'].ffill()

    df['org_str'] = df['org_raw'].astype(str).str.strip()
    df['bu_raw']  = df['org_str'].str.split(' ').str[0]
    df['suffix']  = df['bu_raw'].str[-1]
    df['bu_code'] = df['bu_raw'].str[:-1]
    df['entity']  = df['bu_code'].str[:4]

    def parse_acct(raw):
        raw = str(raw).strip()
        if '.PROD.' in raw or '.NPBO.' in raw:
            return raw.split('.')[0]
        return raw.split(' ')[0][:6]

    df['account_code'] = df['account_raw'].apply(parse_acct)

    # Filter valid entities + correct suffix
    def is_valid(row):
        cfg = ENTITY_CONFIG.get(row['entity'])
        return cfg is not None and row['suffix'] == cfg[0]

    df = df[df.apply(is_valid, axis=1)].copy()

    # Convert to USD
    df['amount_usd'] = df.apply(
        lambda r: r['amount'] * ENTITY_CONFIG.get(r['entity'], ('C','USD',1.0))[2],
        axis=1
    )

    return df[['bu_code', 'entity', 'account_code',
               'amount', 'amount_usd']].copy()

_out_of_scope_cache = {}

def get_gl_data(ad50_line: str,
                fiscal_year: int = 2026,
                fiscal_period: int = 2,
                out_of_scope: set = None) -> pd.DataFrame:
    """Get GL amounts for accounts mapped to this AD50 line.
    - Filters to Trade & OCM BUs only
    - Flips sign for revenue accounts (7xxxxx) to match SAC convention
    """
    from scripts.db import query

    # Get account patterns for this line
    accts = query("""
        SELECT account_pattern, match_type
        FROM account_master
        WHERE ad50_line = ?
    """, (ad50_line,))

    if accts.empty:
        return pd.DataFrame()

    # Get Trade & OCM BUs only
    trade_bus = query("""
        SELECT DISTINCT bu_code FROM org_hierarchy
        WHERE effective_to IS NULL
        AND business IN ('Trade & OCM', 'Trade')
    """)
    if trade_bus.empty:
        return pd.DataFrame()

    bu_list = [b for b in trade_bus['bu_code'].tolist()
               if out_of_scope is None or b not in out_of_scope]
    bu_ph   = ','.join(['?' for _ in bu_list])

    # Build account WHERE clause
    exact   = accts[accts['match_type']=='exact']['account_pattern'].tolist()
    pattern = accts[accts['match_type']=='pattern']['account_pattern'].tolist()

    conditions = []
    params     = []

    exact_6digit = [a for a in exact if len(a) == 6]
    exact_5digit = [a for a in exact if len(a) == 5]
    exact_other  = [a for a in exact if len(a) not in (5, 6)]

    if exact_6digit:
        ph = ','.join(['?' for _ in exact_6digit])
        conditions.append(f"g.account_code IN ({ph})")
        params.extend(exact_6digit)

    if exact_other:
        ph = ','.join(['?' for _ in exact_other])
        conditions.append(f"g.account_code IN ({ph})")
        params.extend(exact_other)

    # 5-digit FLEX accounts → match GL 6-digit by prefix
    # e.g. 64111 → LIKE '64111_' (exactly 6 chars starting with 64111)
    for a in exact_5digit:
        conditions.append("g.account_code LIKE ? AND LENGTH(g.account_code) = 6")
        params.append(a + '_')

    for p in pattern:
        conditions.append("g.account_code LIKE ?")
        params.append(p)

    if not conditions:
        return pd.DataFrame()

    where       = " OR ".join(conditions)
    params_full = params + bu_list + [fiscal_year, fiscal_period]

    df = query(f"""
        SELECT
            g.bu_code,
            g.company,
            g.account_code,
            g.account_desc,
            g.account_type,
            SUM(g.amount_usd)    as amount_usd,
            SUM(g.amount_local)  as amount_lc,
            g.currency_local
        FROM gl_transactions g
        WHERE ({where})
          AND g.bu_code IN ({bu_ph})
          AND g.fiscal_year  = ?
          AND g.fiscal_period = ?
          AND g.account_type = 'PL'
        GROUP BY g.bu_code, g.company, g.account_code,
                 g.account_desc, g.account_type, g.currency_local
        ORDER BY ABS(SUM(g.amount_usd)) DESC
    """, params_full)

    if df.empty:
        return df

    # Sign convention — match SAC display:
    # Revenue (7xxxxx): JDE=credit(negative) → SAC=positive → flip GL
    # Costs   (6xxxxx): JDE=debit(positive)  → SAC=negative → flip GL
    # Both need flipping to match SAC sign convention
    flip_mask = df['account_code'].astype(str).str.match(r'^[67]')
    df.loc[flip_mask, 'amount_usd'] *= -1
    df.loc[flip_mask, 'amount_lc']  *= -1

    return df


def reconcile_line(file_path: str, ad50_line: str,
                   fiscal_year: int = 2026,
                   fiscal_period: int = 2,
                   verbose: bool = True,
                   out_of_scope: set = None) -> pd.DataFrame:
    """
    Reconcile one AD50 line — SAC vs GL.
    Returns DataFrame with SAC, GL, difference columns.
    """
    label = AD50_LABELS.get(ad50_line, ad50_line)

    if verbose:
        print(f"\n{'='*65}")
        print(f"RECONCILIATION — Line {ad50_line} {label} "
              f"— {fiscal_year}/{fiscal_period:02d}")
        print(f"{'='*65}")

    # Get out-of-scope BUs (discovered dynamically)
    out_of_scope = get_out_of_scope_bus(
        file_path, fiscal_year, fiscal_period
    )

    # Get SAC data
    sac = get_sac_data(file_path, ad50_line)
    if sac.empty:
        if verbose:
            print("  No SAC data found")
        return pd.DataFrame()

    sac_by_bu = sac.groupby(['bu_code','account_code'])[
        'amount_usd'].sum().reset_index()
    sac_by_bu.columns = ['bu_code','account_code','sac_usd']

    # Get GL data
    gl = get_gl_data(ad50_line, fiscal_year, fiscal_period,
                     out_of_scope=out_of_scope)
    if gl.empty:
        if verbose:
            print("  ⚠️  No GL data — account_master may not be loaded")
            print("     Run: python scripts/tools/load_account_master.py")

    # Merge SAC vs GL
    if not gl.empty:
        gl_by_bu = gl.groupby(['bu_code','account_code'])[
            'amount_usd'].sum().reset_index()
        gl_by_bu.columns = ['bu_code','account_code','gl_usd']

        merged = pd.merge(
            sac_by_bu, gl_by_bu,
            on=['bu_code','account_code'],
            how='outer'
        ).fillna(0)
    else:
        merged = sac_by_bu.copy()
        merged['gl_usd'] = 0.0

    merged['diff_usd']    = merged['gl_usd'] - merged['sac_usd']
    merged['diff_abs']    = merged['diff_usd'].abs()
    merged['diff_pct']    = merged.apply(
        lambda r: r['diff_usd'] / r['sac_usd'] * 100
        if abs(r['sac_usd']) > 0 else 0, axis=1
    )
    merged['ad50_line']   = ad50_line
    merged['tied']        = merged['diff_abs'] < 1.0  # within $1

    merged = merged.sort_values('diff_abs', ascending=False)

    if verbose:
        sac_total = merged['sac_usd'].sum()
        gl_total  = merged['gl_usd'].sum()
        diff      = gl_total - sac_total
        tied_pct  = merged['tied'].sum() / len(merged) * 100 if len(merged) > 0 else 0

        status = '✅ TIED' if abs(diff) < 1000 else '⚠️  GAP'

        print(f"  SAC total:  {sac_total/1000:>10,.1f} kUSD")
        print(f"  GL total:   {gl_total/1000:>10,.1f} kUSD")
        print(f"  Difference: {diff/1000:>10,.1f} kUSD  {status}")
        print(f"  BU/Account rows: {len(merged)}  "
              f"Tied: {merged['tied'].sum()} ({tied_pct:.0f}%)")

        # Show top gaps
        gaps = merged[~merged['tied']].head(10)
        if not gaps.empty:
            print(f"\n  Top gaps:")
            print(f"  {'BU':<12} {'Account':<10} {'SAC':>10} "
                  f"{'GL':>10} {'Diff':>10}")
            print(f"  {'-'*55}")
            for _, r in gaps.iterrows():
                sac_k = f"({abs(r['sac_usd']/1000):,.1f})" \
                        if r['sac_usd'] < 0 \
                        else f"{r['sac_usd']/1000:,.1f}"
                gl_k  = f"({abs(r['gl_usd']/1000):,.1f})" \
                        if r['gl_usd'] < 0 \
                        else f"{r['gl_usd']/1000:,.1f}"
                dif_k = f"({abs(r['diff_usd']/1000):,.1f})" \
                        if r['diff_usd'] < 0 \
                        else f"{r['diff_usd']/1000:,.1f}"
                print(f"  {r['bu_code']:<12} {r['account_code']:<10} "
                      f"{sac_k:>10} {gl_k:>10} {dif_k:>10}")

        # Entity-level summary
        if not merged.empty:
            # Add entity column
            merged['entity'] = merged['bu_code'].str[:4]
            by_entity = merged.groupby('entity').agg(
                sac=('sac_usd','sum'),
                gl=('gl_usd','sum')
            ).reset_index()
            by_entity['diff'] = by_entity['gl'] - by_entity['sac']
            by_entity = by_entity[by_entity['diff'].abs() > 100]
            by_entity = by_entity.sort_values('diff', key=abs, ascending=False)

            if not by_entity.empty:
                print(f"\n  By entity (gaps > $100):")
                print(f"  {'Entity':<8} {'SAC':>10} {'GL':>10} {'Diff':>10}")
                print(f"  {'-'*42}")
                for _, r in by_entity.iterrows():
                    sk = f'({abs(r.sac/1000):,.1f})' if r.sac<0 else f'{r.sac/1000:,.1f}'
                    gk = f'({abs(r.gl/1000):,.1f})' if r.gl<0 else f'{r.gl/1000:,.1f}'
                    dval = float(r['diff'])
                    dk = f'({abs(dval/1000):,.1f})' if dval<0 else f'{dval/1000:,.1f}'
                    print(f"  {r.entity:<8} {sk:>10} {gk:>10} {dk:>10}")

    return merged


def reconcile_all(file_path: str,
                  fiscal_year: int = 2026,
                  fiscal_period: int = 2,
                  output: str = None) -> dict:
    """Reconcile all AD50 lines and produce summary + Excel output."""

    print(f"\n{'#'*65}")
    print(f"  FULL RECONCILIATION — NAM TRADE")
    print(f"  Period: {fiscal_year}/{fiscal_period:02d}")
    print(f"{'#'*65}")

    all_results = {}
    summary_rows = []

    # Discover out-of-scope BUs once for all lines
    print("\nDiscovering out-of-scope BUs...")
    out_of_scope = get_out_of_scope_bus(
        file_path, fiscal_year, fiscal_period
    )
    print(f"  {len(out_of_scope)} BUs excluded from GL\n")

    for ad50_line in ['01','02','04','05','07','08','09','10']:
        result = reconcile_line(
            file_path, ad50_line,
            fiscal_year, fiscal_period,
            verbose=True,
            out_of_scope=out_of_scope
        )
        all_results[ad50_line] = result

        if not result.empty:
            sac_total  = result['sac_usd'].sum()
            gl_total   = result['gl_usd'].sum()
            diff       = gl_total - sac_total
            tied_count = result['tied'].sum()
            total_rows = len(result)
            summary_rows.append({
                'Line':        ad50_line,
                'Description': AD50_LABELS.get(ad50_line, ''),
                'SAC kUSD':    round(sac_total/1000, 1),
                'GL kUSD':     round(gl_total/1000, 1),
                'Diff kUSD':   round(diff/1000, 1),
                'Status':      '✅' if abs(diff) < 1000 else '⚠️ ',
                'Tied Rows':   f"{tied_count}/{total_rows}",
            })

    # Print summary table
    print(f"\n{'='*70}")
    print(f"SUMMARY")
    print(f"{'='*70}")
    print(f"  {'Line':<6} {'Description':<22} {'SAC':>10} "
          f"{'GL':>10} {'Diff':>10} {'Status'}")
    print(f"  {'-'*65}")
    for r in summary_rows:
        sac_d  = f"({abs(r['SAC kUSD']):,.1f})" \
                 if r['SAC kUSD'] < 0 else f"{r['SAC kUSD']:,.1f}"
        gl_d   = f"({abs(r['GL kUSD']):,.1f})" \
                 if r['GL kUSD'] < 0 else f"{r['GL kUSD']:,.1f}"
        diff_d = f"({abs(r['Diff kUSD']):,.1f})" \
                 if r['Diff kUSD'] < 0 else f"{r['Diff kUSD']:,.1f}"
        print(f"  {r['Line']:<6} {r['Description']:<22} "
              f"{sac_d:>10} {gl_d:>10} {diff_d:>10} {r['Status']}")
    print(f"{'='*70}\n")

    # Excel output
    if output:
        out_path = output
    else:
        out_path = f"data/recon_{fiscal_year}_{fiscal_period:02d}.xlsx"

    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        # Summary sheet
        pd.DataFrame(summary_rows).to_excel(
            writer, sheet_name='Summary', index=False)

        # One sheet per AD50 line
        for line, df in all_results.items():
            if not df.empty:
                label = AD50_LABELS.get(line, line)[:20]
                sheet = f"{line} {label}"[:31]
                df_out = df.copy()
                df_out['sac_kUSD']  = df_out['sac_usd'].apply(
                    lambda v: round(v/1000,2))
                df_out['gl_kUSD']   = df_out['gl_usd'].apply(
                    lambda v: round(v/1000,2))
                df_out['diff_kUSD'] = df_out['diff_usd'].apply(
                    lambda v: round(v/1000,2))
                df_out[[
                    'bu_code','account_code',
                    'sac_kUSD','gl_kUSD','diff_kUSD','tied'
                ]].to_excel(writer, sheet_name=sheet, index=False)

    print(f"✅ Reconciliation saved to: {out_path}")
    return all_results


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Reconcile SAC vs GL by AD50 line'
    )
    parser.add_argument('--file', default=
        'data/NAM_-_trade_detail_per_account.xlsx')
    parser.add_argument('--line', default='all',
        help='AD50 line to reconcile (01,02,04,05,07,08,09,10 or all)')
    parser.add_argument('--year',   type=int, default=2026)
    parser.add_argument('--period', type=int, default=2)
    parser.add_argument('--output', default=None)
    args = parser.parse_args()

    if not Path(args.file).exists():
        print(f"File not found: {args.file}")
        sys.exit(1)

    if args.line == 'all':
        reconcile_all(args.file, args.year, args.period, args.output)
    else:
        reconcile_line(args.file, args.line, args.year, args.period)
