"""
Finance Platform — Official vs SAC Comparison Tool
Compares official AD50 report vs SAC extract month by month.
Shows gaps and helps document methodology.

Usage:
  python scripts/tools/compare_official_sac.py
  python scripts/tools/compare_official_sac.py --year 2026 --period 1
  python scripts/tools/compare_official_sac.py --tab US --line 01
  python scripts/tools/compare_official_sac.py --output data/official_vs_sac.xlsx
"""

import pandas as pd
import sys
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

def gap_status(diff_k: float, fy: int) -> str:
    """
    Classify a gap (in kUSD) for display.
    diff_k < 1     → ✅  (within $1K — tied)
    diff_k < 100   → ~FX (2024/2025 FX policy diff) or ⚠️ (2026+)
    else           → ⚠️  (real gap)
    """
    if abs(diff_k) < 1:    return '✅'
    if abs(diff_k) < 100:  return '~FX' if fy < 2026 else '⚠️'
    return '⚠️'


AD50_LABELS = {
    '01':'Billing', '02':'WIP', '04':'IG Revenue',
    '05':'IG Subcon', '07':'Personnel', '07A':'Personnel PROD',
    '07B':'Personnel NPBO', '08':'Ext Subcon',
    '09':'Other Costs', '09A':'Sundry', '09B':'Contract',
    '09C':'Lab Consumables', '09D':'Travel', '09E':'Depreciation',
    '09F':'Repairs', '09G':'Rent & Util', '09H':'IT',
    '09I':'Office', '09J':'Bad debt', '09K':'Commercial',
    '09L':'Professional', '09M':'Other',
    '10':'Func Neutral', '13':'Functional',
    '13A':'S&M', '13B':'Management', '13C':'Finance',
    '13D':'HR', '13E':'IT Func', '13F':'Fees', '13G':'Legal',
}


def get_official(tab: str = None, fiscal_year: int = None,
                 fiscal_period: int = None,
                 ad50_line: str = None) -> pd.DataFrame:
    from scripts.db import query

    conditions = ["1=1"]
    params = []
    if tab:
        conditions.append("tab=?")
        params.append(tab)
    if fiscal_year:
        conditions.append("fiscal_year=?")
        params.append(fiscal_year)
    if fiscal_period:
        conditions.append("fiscal_period=?")
        params.append(fiscal_period)
    if ad50_line:
        conditions.append("ad50_line=?")
        params.append(ad50_line)

    where = " AND ".join(conditions)
    return query(f"""
        SELECT tab, ad50_line, ad50_label,
               fiscal_year, fiscal_period, amount_usd
        FROM official_ad50
        WHERE {where}
        ORDER BY tab, fiscal_year, fiscal_period, ad50_line
    """, params)


def get_sac(fiscal_year: int = None, fiscal_period: int = None,
            ad50_line: str = None) -> pd.DataFrame:
    """Get SAC totals — all entities combined (matches Total tab)."""
    from scripts.db import query

    conditions = ["1=1"]
    params = []
    if fiscal_year:
        conditions.append("fiscal_year=?")
        params.append(fiscal_year)
    if fiscal_period:
        conditions.append("fiscal_period=?")
        params.append(fiscal_period)
    if ad50_line:
        conditions.append("ad50_line=?")
        params.append(ad50_line)

    where = " AND ".join(conditions)
    return query(f"""
        SELECT ad50_line, fiscal_year, fiscal_period,
               SUM(amount_usd) as amount_usd
        FROM sac_detail
        WHERE {where}
        GROUP BY ad50_line, fiscal_year, fiscal_period
        ORDER BY fiscal_year, fiscal_period, ad50_line
    """, params)


def compare_period(fiscal_year: int, fiscal_period: int,
                   tab: str = 'Total',
                   verbose: bool = True,
                   level: str = 'parent') -> pd.DataFrame:
    """
    Compare official vs SAC for one period.
    Returns DataFrame with official, sac, difference columns.

    level='parent' → compare at 01,02,07,08,09,10,13 only
    level='sub'    → compare at 07A,07B,09B,09C... etc
    level='all'    → compare everything
    """
    official = get_official(
        tab=tab,
        fiscal_year=fiscal_year,
        fiscal_period=fiscal_period
    )
    sac = get_sac(
        fiscal_year=fiscal_year,
        fiscal_period=fiscal_period
    )

    if official.empty:
        if verbose:
            print(f"No official data for {fiscal_year}/{fiscal_period:02d} tab={tab}")
        return pd.DataFrame()

    if sac.empty:
        if verbose:
            print(f"No SAC data for {fiscal_year}/{fiscal_period:02d}")
        return pd.DataFrame()

    # Filter to comparison level
    from scripts.loaders.load_official_ad50 import VALID_LINES, SUBTOTAL_LINES
    PARENT_LINES = {'01','02','04','05','07','08','09','10','13'}
    SUB_LINES    = {l for l in VALID_LINES if l not in PARENT_LINES
                    and l not in SUBTOTAL_LINES}

    if level == 'parent':
        official = official[official['ad50_line'].isin(PARENT_LINES)]
        sac      = sac[sac['ad50_line'].isin(PARENT_LINES)]
    elif level == 'sub':
        official = official[official['ad50_line'].isin(SUB_LINES)]
        sac      = sac[sac['ad50_line'].isin(SUB_LINES)]
    # level='all' → no filter

    # Merge
    off_pivot = official.set_index('ad50_line')['amount_usd']
    sac_pivot = sac.set_index('ad50_line')['amount_usd']

    all_lines = sorted(set(off_pivot.index) | set(sac_pivot.index))

    rows = []
    for line in all_lines:
        off_val = float(off_pivot.get(line, 0))
        sac_val = float(sac_pivot.get(line, 0))
        diff    = sac_val - off_val
        rows.append({
            'ad50_line':   line,
            'description': AD50_LABELS.get(line, line),
            'official':    off_val,
            'sac':         sac_val,
            'diff':        diff,
            'diff_pct':    diff / off_val * 100 if off_val else 0,
            'tied':        abs(diff) < 1000,  # within $1K
        })

    df = pd.DataFrame(rows)

    if verbose:
        def fmt(v):
            k = v / 1000
            if abs(k) < 0.05:
                return '—'
            return f'({abs(k):,.1f})' if k < 0 else f'{k:,.1f}'

        print(f"\n{'='*70}")
        print(f"OFFICIAL vs SAC — {fiscal_year}/{fiscal_period:02d} — Tab: {tab}")
        print(f"{'='*70}")
        print(f"  {'Line':<6} {'Description':<22} "
              f"{'Official':>10} {'SAC':>10} {'Diff':>10} {'Status'}")
        print(f"  {'-'*65}")

        for _, r in df.iterrows():
            status = '✅' if r['tied'] else '⚠️ '
            print(f"  {r['ad50_line']:<6} {r['description']:<22} "
                  f"{fmt(float(r['official'])):>10} "
                  f"{fmt(float(r['sac'])):>10} "
                  f"{fmt(float(r['diff'])):>10} {status}")

        tied    = df['tied'].sum()
        untied  = (~df['tied']).sum()
        off_tot = df['official'].sum()
        sac_tot = df['sac'].sum()
        print(f"  {'-'*65}")
        print(f"  {'TOTAL':<6} {'':<22} "
              f"{fmt(off_tot):>10} {fmt(sac_tot):>10} "
              f"{fmt(sac_tot-off_tot):>10}")
        print(f"\n  {tied}/{len(df)} lines tied  |  "
              f"Total gap: {(sac_tot-off_tot)/1000:,.1f}K")

    return df


def compare_all_periods(tab: str = 'Total',
                        output: str = None,
                        level: str = 'parent') -> pd.DataFrame:
    """Compare all periods — pivot table: lines x periods, values=diff."""
    from scripts.db import query

    PARENT_LINES = ['01','02','04','05','07','08','09','10']
    # Exclude 13 — not in SAC

    periods = query("""
        SELECT DISTINCT fiscal_year, fiscal_period
        FROM official_ad50
        ORDER BY fiscal_year, fiscal_period
    """)

    if periods.empty:
        print("No official data loaded")
        return pd.DataFrame()

    all_rows = []

    for _, row in periods.iterrows():
        fy = int(row.fiscal_year)
        fp = int(row.fiscal_period)

        off = get_official(tab=tab, fiscal_year=fy,
                           fiscal_period=fp)
        sac = get_sac(fiscal_year=fy, fiscal_period=fp)

        if off.empty:
            continue

        # Filter to parent lines only, exclude 13
        off = off[off['ad50_line'].isin(PARENT_LINES)]
        sac_f = sac[sac['ad50_line'].isin(PARENT_LINES)] \
                if not sac.empty else pd.DataFrame()

        for line in PARENT_LINES:
            off_val = float(
                off[off['ad50_line']==line]['amount_usd'].sum()
            )
            sac_val = float(
                sac_f[sac_f['ad50_line']==line]['amount_usd'].sum()
            ) if not sac_f.empty else 0.0

            diff = sac_val - off_val
            # Round to 0 if within $1K
            diff_display = 0.0 if abs(diff) < 1000 else round(diff/1000, 1)

            all_rows.append({
                'ad50_line':   line,
                'description': AD50_LABELS.get(line, line),
                'period':      f"{fy}/{fp:02d}",
                'yyyymm':      fy*100+fp,
                'official':    round(off_val/1000, 1),
                'sac':         round(sac_val/1000, 1),
                'diff':        diff_display,
            })

    if not all_rows:
        return pd.DataFrame()

    df_all = pd.DataFrame(all_rows)

    # Pivot: rows=line, cols=period, values=diff
    pivot_diff = df_all.pivot_table(
        index=['ad50_line','description'],
        columns='period',
        values='diff',
        aggfunc='sum'
    )
    # Sort by line
    pivot_diff = pivot_diff.reindex(
        [(l, AD50_LABELS.get(l,l)) for l in PARENT_LINES
         if (l, AD50_LABELS.get(l,l)) in pivot_diff.index]
    )

    # Add total row
    pivot_diff.loc[('TOTAL','Total gap'),:] = pivot_diff.sum()

    # Console output
    print(f"\nGap table: Official vs SAC — Tab: {tab} (kUSD, 0=tied)")
    print(f"Positive = SAC > Official, Negative = SAC < Official")
    print()

    cols = list(pivot_diff.columns)
    # Show abbreviated header
    print(f"  {'Line':<6} {'Description':<18}" +
          ''.join(f'{c[2:]:>8}' for c in cols))
    print('  ' + '-'*(24 + 8*len(cols)))

    for (line, desc), row in pivot_diff.iterrows():
        vals_parts = []
        for col, v in zip(cols, row.values):
            col_fy = int('20' + col[:2]) if col[:2].isdigit() else 2026
            if abs(v) < 0.05:
                vals_parts.append(f'{"✅":>8}')
            else:
                sym = gap_status(v, col_fy)
                cell = sym if sym != '⚠️' and abs(v) < 100 \
                       else f'{v:>8,.0f}'
                vals_parts.append(f'{cell:>8}')
        vals = ''.join(vals_parts)
        print(f"  {line:<6} {desc:<18}{vals}")

    print()

    # Save Excel
    out_path = output or 'data/official_vs_sac.xlsx'
    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        # Gap pivot
        pivot_diff.to_excel(writer, sheet_name='Gap (kUSD)')

        # Official pivot
        pivot_off = df_all.pivot_table(
            index=['ad50_line','description'],
            columns='period', values='official', aggfunc='sum'
        )
        pivot_off.to_excel(writer, sheet_name='Official (kUSD)')

        # SAC pivot
        pivot_sac = df_all.pivot_table(
            index=['ad50_line','description'],
            columns='period', values='sac', aggfunc='sum'
        )
        pivot_sac.to_excel(writer, sheet_name='SAC (kUSD)')

        # Full detail
        df_all.to_excel(writer, sheet_name='Detail', index=False)

    print(f"✅ Saved to: {out_path}")
    return df_all


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Compare official AD50 vs SAC extract'
    )
    parser.add_argument('--tab',    default='Total',
                        help='Tab name (Total/US/CentralAm/MGT/OCM)')
    parser.add_argument('--year',   type=int, default=None)
    parser.add_argument('--period', type=int, default=None)
    parser.add_argument('--line',   default=None,
                        help='AD50 line to focus on')
    parser.add_argument('--output', default=None)
    parser.add_argument('--level', default='parent',
                        choices=['parent','sub','all'],
                        help='Comparison level (default: parent)')
    args = parser.parse_args()

    if args.year and args.period:
        compare_period(args.year, args.period,
                       tab=args.tab, verbose=True,
                       level=args.level)
    else:
        compare_all_periods(tab=args.tab, output=args.output,
                            level=args.level)
