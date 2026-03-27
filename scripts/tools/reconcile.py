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
import re
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
                          fiscal_period: int = 2,
                          ad50_line: str = None) -> set:
    """
    Dynamic per-period out-of-scope detection.
    Rule: if a BU appears ANYWHERE in SAC for this period
    (any line, any non-zero amount) → include in GL for ALL lines.
    If never in SAC → exclude from GL entirely.
    Recharge BUs naturally handled: they appear in SAC (line 10)
    so they stay in scope for all lines.
    """
    from scripts.db import query

    # Get all Trade & OCM BUs with GL P&L activity
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

    # Get ALL BUs with ANY non-zero SAC amount across ALL lines
    sac_bus = set()
    for line in SHEETS.values():
        try:
            df = get_sac_data(file_path, line,
                              fiscal_year, fiscal_period)
            if not df.empty:
                active = df[df['amount_usd'].abs() > 0][
                    'bu_code'].unique()
                sac_bus.update(active)
        except Exception:
            continue

    # Out of scope = in GL but NEVER in SAC for this period
    out_of_scope = gl_bus - sac_bus

    print(f"  GL BUs:          {len(gl_bus)}")
    print(f"  SAC BUs:         {len(sac_bus)}")
    print(f"  Out of scope:    {len(out_of_scope)} excluded")
    if out_of_scope:
        for bu in sorted(out_of_scope):
            print(f"    {bu}")

    return out_of_scope

def get_sac_data(file_path: str, ad50_line: str,
                 fiscal_year: int = 2026,
                 fiscal_period: int = 2) -> pd.DataFrame:
    """Handles both single-period (4 col) and multi-period SAC files."""
    sheet = SHEET_MAP.get(ad50_line)
    if not sheet:
        return pd.DataFrame()
    df_raw = pd.read_excel(file_path, sheet_name=sheet, header=None)

    def to_s(v):
        try:
            f = float(v)
            return str(int(f)) if f == int(f) else str(v).strip()
        except Exception:
            return str(v).strip()

    is_multi = False
    for i, row in df_raw.head(8).iterrows():
        vals = [to_s(v) for v in row.values]
        if len([v for v in vals if re.match(r'^20\d{4}$', v)]) >= 3:
            is_multi = True
            break

    if is_multi:
        return _get_sac_multi(df_raw, fiscal_year, fiscal_period)
    else:
        return _get_sac_single(df_raw)


def _get_sac_single(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = range(len(df.columns))
    df['amount'] = pd.to_numeric(df[3], errors='coerce')
    df = df[df['amount'].notna()].copy()
    df[0] = df[0].ffill()
    df['org_str']  = df[1].astype(str).str.strip()
    df['bu_raw']   = df['org_str'].str.split(' ').str[0]
    df['suffix']   = df['bu_raw'].str[-1]
    df['bu_code']  = df['bu_raw'].str[:-1]
    df['entity']   = df['bu_code'].str[:4]
    df['account_code'] = df[0].apply(
        lambda r: str(r).strip().split('.')[0].split(' ')[0][:6])
    def is_valid(row):
        cfg = ENTITY_CONFIG.get(row['entity'])
        return cfg is not None and row['suffix'] == cfg[0]
    df = df[df.apply(is_valid, axis=1)].copy()
    df['amount_usd'] = df.apply(
        lambda r: r['amount'] * ENTITY_CONFIG.get(
            r['entity'], ('C', 'USD', 1.0))[2], axis=1)
    return df[['bu_code','entity','account_code',
               'amount','amount_usd']].copy()


def _get_sac_multi(df_raw: pd.DataFrame,
                   fiscal_year: int,
                   fiscal_period: int) -> pd.DataFrame:
    def to_s(v):
        try:
            f = float(v)
            return str(int(f)) if f == int(f) else str(v).strip()
        except Exception:
            return str(v).strip()

    target = fiscal_year * 100 + fiscal_period
    period_row = account_row = target_col = None

    for i, row in df_raw.iterrows():
        vals = [to_s(v) for v in row.values]
        numeric = [v for v in vals if re.match(r'^20\d{4}$', v)]
        if len(numeric) >= 3:
            period_row = i
            for j, val in enumerate(row.values):
                if to_s(val) == str(target):
                    target_col = j
            continue
        if 'Account' in vals and 'Organisation' in vals:
            account_row = i
            break

    if period_row is None or account_row is None or target_col is None:
        return pd.DataFrame()

    header   = [str(v).strip() for v in df_raw.iloc[account_row].values]
    acct_col = header.index('Account')
    org_col  = header.index('Organisation')

    data = df_raw.iloc[account_row + 1:].reset_index(drop=True)
    data[acct_col] = data[acct_col].where(
        data[acct_col].astype(str).str.strip().str.match(r'^\d'),
        other=None).ffill()

    rows = []
    for _, row in data.iterrows():
        org_val = str(row.iloc[org_col]).strip()
        m = re.match(r'^(\d{7}[CS])', org_val)
        if not m:
            continue
        bu_raw  = m.group(1)
        suffix  = bu_raw[-1]
        bu_code = bu_raw[:-1]
        entity  = bu_code[:4]
        cfg     = ENTITY_CONFIG.get(entity)
        if not cfg or suffix != cfg[0]:
            continue
        acct_raw = str(row.iloc[acct_col]).strip()
        if acct_raw in ('nan', '', 'None'):
            continue
        if '.PROD.' in acct_raw or '.NPBO.' in acct_raw:
            account_code = acct_raw.split('.')[0]
        else:
            account_code = acct_raw.split(' ')[0][:6]
        try:
            amount_lc = float(row.iloc[target_col])
        except Exception:
            amount_lc = 0.0
        if amount_lc == 0.0:
            continue
        rows.append({
            'bu_code':      bu_code,
            'entity':       entity,
            'account_code': account_code,
            'amount':       amount_lc,
            'amount_usd':   amount_lc * cfg[2],
        })
    return pd.DataFrame(rows) if rows else pd.DataFrame()

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

    out_of_scope = out_of_scope or get_out_of_scope_bus(
        file_path, fiscal_year, fiscal_period
    )

    # Get SAC data
    sac = get_sac_data(file_path, ad50_line, fiscal_year, fiscal_period)
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
    print("\nDiscovering out-of-scope BUs for this period...")
    out_of_scope = get_out_of_scope_bus(
        file_path, fiscal_year, fiscal_period
    )
    print()

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
