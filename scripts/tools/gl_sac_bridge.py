"""
GL vs SAC Bridge — Journal Entry Level
For a given BU + period, shows GL transactions vs SAC amounts
side by side at account level.

Usage:
  python scripts/tools/gl_sac_bridge.py --bu 0577009 --year 2026 --period 1
  python scripts/tools/gl_sac_bridge.py --bu 0577009 --year 2026 --period 1 --line 09
"""

import pandas as pd
import sys
import argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Force UTF-8 output on Windows (emoji status chars)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

AD50_LABELS = {
    '01':'Billing', '02':'WIP', '04':'IG Revenue',
    '05':'IG Subcon', '07':'Personnel', '08':'Ext Subcon',
    '09':'Other Costs', '10':'Func Neutral',
}


def bridge(bu_code: str, fiscal_year: int, fiscal_period: int,
           ad50_line: str = None, output: str = None):
    from scripts.db import query

    print(f"\n{'='*65}")
    print(f"GL vs SAC BRIDGE — BU: {bu_code}  "
          f"Period: {fiscal_year}/{fiscal_period:02d}")
    if ad50_line:
        print(f"AD50 Line filter: {ad50_line}")
    print(f"{'='*65}")

    # Get hierarchy info
    hier = query("""
        SELECT bu_name, branch, region, business
        FROM org_hierarchy
        WHERE bu_code=? AND effective_to IS NULL
    """, (bu_code,))
    if not hier.empty:
        r = hier.iloc[0]
        print(f"BU: {r['bu_name']}  |  "
              f"{r['branch']}  |  {r['region']}")
    print()

    # ── SAC amounts by account ────────────────────────────────
    sac_cond = "AND s.ad50_line=?" if ad50_line else ""
    sac_params = [bu_code, fiscal_year, fiscal_period]
    if ad50_line:
        sac_params.append(ad50_line)

    sac = query(f"""
        SELECT s.account_code,
               s.ad50_line,
               COALESCE(am.ad50_subline, s.ad50_line) as subline,
               s.amount_lc,
               s.currency,
               s.amount_usd
        FROM sac_detail s
        LEFT JOIN account_master am
            ON s.account_code = am.account_pattern
            AND am.source = 'COA'
        WHERE SUBSTR(s.bu_code, 1, 7) = SUBSTR(?, 1, 7)
          AND s.fiscal_year=?
          AND s.fiscal_period=?
          {sac_cond}
        ORDER BY s.ad50_line, s.account_code
    """, sac_params)

    # ── GL amounts by account ─────────────────────────────────
    gl_cond = "AND am.ad50_line=?" if ad50_line else ""
    gl_params = [bu_code, fiscal_year, fiscal_period]
    if ad50_line:
        gl_params.append(ad50_line)

    gl_having = "HAVING ad50_line=?" if ad50_line else ""
    gl = query(f"""
        WITH gl_mapped AS (
            SELECT g.account_code,
                   g.currency_local,
                   g.amount_local,
                   g.amount_usd,
                   (SELECT am.ad50_line FROM account_master am
                    WHERE (am.match_type='exact'
                           AND g.account_code = am.account_pattern)
                       OR (am.match_type='pattern'
                           AND g.account_code LIKE am.account_pattern)
                    ORDER BY (am.source='COA') DESC,
                             (am.match_type='exact') DESC
                    LIMIT 1) as ad50_line,
                   (SELECT am.ad50_subline FROM account_master am
                    WHERE (am.match_type='exact'
                           AND g.account_code = am.account_pattern)
                       OR (am.match_type='pattern'
                           AND g.account_code LIKE am.account_pattern)
                    ORDER BY (am.source='COA') DESC,
                             (am.match_type='exact') DESC
                    LIMIT 1) as ad50_subline
            FROM gl_transactions g
            WHERE SUBSTR(g.bu_code, 1, 7) = SUBSTR(?, 1, 7)
              AND g.fiscal_year=?
              AND g.fiscal_period=?
              AND g.account_type='PL'
        )
        SELECT account_code,
               COALESCE(ad50_line, 'UNMAPPED') as ad50_line,
               COALESCE(ad50_subline, ad50_line, 'UNMAPPED') as subline,
               SUM(amount_local) as amount_lc,
               currency_local as currency,
               SUM(amount_usd)*-1 as amount_usd
        FROM gl_mapped
        GROUP BY account_code, ad50_line, ad50_subline, currency_local
        {gl_having}
        ORDER BY ad50_line, account_code
    """, gl_params)

    if sac.empty and gl.empty:
        print("No data found for this BU/period combination")
        return

    # ── Summary by AD50 line ──────────────────────────────────
    print(f"SUMMARY BY AD50 LINE (kUSD):")
    print(f"  {'Line':<6} {'Description':<20} "
          f"{'SAC':>10} {'GL':>10} {'Diff':>10} {'Status'}")
    print(f"  {'-'*60}")

    lines = sorted(set(
        list(sac['ad50_line'].unique()) +
        list(gl['ad50_line'].unique() if not gl.empty else [])
    ))

    def fmt(v):
        k = v/1000
        if abs(k) < 0.05: return '—'
        return f'({abs(k):,.1f})' if k < 0 else f'{k:,.1f}'

    for line in lines:
        if line == 'UNMAPPED':
            continue
        sac_v = float(sac[sac['ad50_line']==line]['amount_usd'].sum()) \
                if not sac.empty else 0
        gl_v  = float(gl[gl['ad50_line']==line]['amount_usd'].sum()) \
                if not gl.empty else 0
        diff  = gl_v - sac_v
        status = '✅' if abs(diff) < 500 else '⚠️'
        label = AD50_LABELS.get(line, line)
        print(f"  {line:<6} {label:<20} "
              f"{fmt(sac_v):>10} {fmt(gl_v):>10} "
              f"{fmt(diff):>10} {status}")

    sac_tot = float(sac['amount_usd'].sum()) if not sac.empty else 0
    gl_tot  = float(gl['amount_usd'].sum()) if not gl.empty else 0
    print(f"  {'-'*60}")
    print(f"  {'TOTAL':<6} {'':<20} "
          f"{fmt(sac_tot):>10} {fmt(gl_tot):>10} "
          f"{fmt(gl_tot-sac_tot):>10}")

    # ── Account level detail ──────────────────────────────────
    print(f"\nACCOUNT DETAIL (kUSD):")
    print(f"  {'Account':<10} {'Sub':<6} "
          f"{'SAC LC':>12} {'SAC USD':>10} "
          f"{'GL LC':>12} {'GL USD':>10} "
          f"{'Diff':>10} {'Status'}")
    print(f"  {'-'*75}")

    # Merge SAC and GL at account level
    sac_acct = sac.groupby('account_code').agg(
        ad50_line=('ad50_line','first'),
        subline=('subline','first'),
        sac_lc=('amount_lc','sum'),
        currency=('currency','first'),
        sac_usd=('amount_usd','sum')
    ).reset_index() if not sac.empty else pd.DataFrame()

    gl_acct = gl.groupby('account_code').agg(
        ad50_line=('ad50_line','first'),
        subline=('subline','first'),
        gl_lc=('amount_lc','sum'),
        currency=('currency','first'),
        gl_usd=('amount_usd','sum')
    ).reset_index() if not gl.empty else pd.DataFrame()

    # Normalize: SAC 5-digit → pad to 6 for matching
    if not sac_acct.empty:
        sac_acct['acct_key'] = sac_acct['account_code'].apply(
            lambda x: x.ljust(6,'_') if len(x)==5 else x)
    if not gl_acct.empty:
        gl_acct['acct_key'] = gl_acct['account_code']

    all_accounts = sorted(set(
        list(sac_acct['acct_key'].tolist()
             if not sac_acct.empty else []) +
        list(gl_acct['acct_key'].tolist()
             if not gl_acct.empty else [])
    ))

    detail_rows = []
    for acct in all_accounts:
        sac_match = sac_acct[sac_acct['acct_key']==acct] \
                    if not sac_acct.empty else pd.DataFrame()
        gl_match  = gl_acct[gl_acct['acct_key']==acct] \
                    if not gl_acct.empty else pd.DataFrame()
        sac_row = sac_match.iloc[0] if not sac_match.empty else None
        gl_row  = gl_match.iloc[0]  if not gl_match.empty  else None

        sac_lc  = float(sac_row['sac_lc'])  if sac_row is not None else 0
        sac_usd = float(sac_row['sac_usd']) if sac_row is not None else 0
        gl_lc   = float(gl_row['gl_lc'])    if gl_row  is not None else 0
        gl_usd  = float(gl_row['gl_usd'])   if gl_row  is not None else 0
        diff    = gl_usd - sac_usd
        subline = (sac_row['subline'] if sac_row is not None
                   else gl_row['subline'] if gl_row is not None else '')
        status  = '✅' if abs(diff) < 100 else '⚠️'

        only_in = ''
        if sac_row is None: only_in = '← GL only'
        if gl_row  is None: only_in = '← SAC only'

        print(f"  {acct:<10} {subline:<6} "
              f"{sac_lc/1000:>12,.1f} {sac_usd/1000:>10,.1f} "
              f"{gl_lc/1000:>12,.1f} {gl_usd/1000:>10,.1f} "
              f"{diff/1000:>10,.1f} {status} {only_in}")

        detail_rows.append({
            'account_code': acct,
            'subline':      subline,
            'sac_lc':       sac_lc,
            'sac_usd':      sac_usd,
            'gl_lc':        gl_lc,
            'gl_usd':       gl_usd,
            'diff_usd':     diff,
            'status':       status,
            'only_in':      only_in,
        })

    # Save to Excel if requested
    if output:
        df_detail = pd.DataFrame(detail_rows)
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_detail.to_excel(writer, sheet_name='Account Detail',
                               index=False)
            if not sac.empty:
                sac.to_excel(writer, sheet_name='SAC Raw', index=False)
            if not gl.empty:
                gl.to_excel(writer, sheet_name='GL Raw', index=False)
        print(f"\n✅ Saved to: {output}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bu',     required=True)
    parser.add_argument('--year',   type=int, required=True)
    parser.add_argument('--period', type=int, required=True)
    parser.add_argument('--line',   default=None)
    parser.add_argument('--output', default=None)
    args = parser.parse_args()

    bridge(args.bu, args.year, args.period,
           ad50_line=args.line, output=args.output)
