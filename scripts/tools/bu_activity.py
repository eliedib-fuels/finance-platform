"""
BU Activity Timeline
Shows which BUs were active in SAC for each period.
Identifies: new BUs, removed BUs, gaps, and current active set.

Usage:
  python scripts/tools/bu_activity.py
  python scripts/tools/bu_activity.py --output data/bu_activity.xlsx
"""

import pandas as pd
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def build_bu_timeline(output: str = 'data/bu_activity.xlsx'):
    from scripts.db import query

    print("Building BU activity timeline from SAC detail...")

    # Get all BU x period combinations with non-zero billing
    # Use billing (01) as the primary indicator of activity
    df = query("""
        SELECT
            bu_code,
            entity,
            fiscal_year,
            fiscal_period,
            fiscal_year * 100 + fiscal_period as yyyymm,
            SUM(CASE WHEN ad50_line='01'
                THEN amount_usd ELSE 0 END) as billing,
            SUM(amount_usd) as total
        FROM sac_detail
        GROUP BY bu_code, entity, fiscal_year, fiscal_period
        HAVING SUM(ABS(amount_usd)) > 0
        ORDER BY bu_code, yyyymm
    """)

    if df.empty:
        print("No SAC data found")
        return

    # Get all periods
    periods = sorted(df['yyyymm'].unique())
    period_labels = [f"{str(p)[:4]}/{str(p)[4:]}" for p in periods]

    print(f"  {df['bu_code'].nunique()} unique BUs")
    print(f"  {len(periods)} periods: "
          f"{period_labels[0]} → {period_labels[-1]}")

    # Get hierarchy mapping
    hier = query("""
        SELECT bu_code, bu_name, branch, region, business
        FROM org_hierarchy WHERE effective_to IS NULL
    """)
    hier_map = {}
    if not hier.empty:
        for _, r in hier.iterrows():
            hier_map[r['bu_code']] = {
                'bu_name': r['bu_name'],
                'branch':  r['branch'],
                'region':  r['region'],
                'business': r['business'],
            }

    # Build pivot: BU x Period → has activity (1/0)
    df['active'] = 1
    pivot = df.pivot_table(
        index=['bu_code','entity'],
        columns='yyyymm',
        values='active',
        aggfunc='max',
        fill_value=0
    )

    # Add hierarchy info
    pivot = pivot.reset_index()
    pivot['bu_name'] = pivot['bu_code'].map(
        lambda x: hier_map.get(x, {}).get('bu_name', ''))
    pivot['branch']  = pivot['bu_code'].map(
        lambda x: hier_map.get(x, {}).get('branch', ''))
    pivot['region']  = pivot['bu_code'].map(
        lambda x: hier_map.get(x, {}).get('region', ''))
    pivot['business'] = pivot['bu_code'].map(
        lambda x: hier_map.get(x, {}).get('business', ''))
    pivot['in_hierarchy'] = pivot['bu_code'].map(
        lambda x: '✅' if x in hier_map else '❌')

    # Calculate activity stats
    period_cols = [c for c in pivot.columns if isinstance(c, int)]
    pivot['first_period'] = pivot[period_cols].apply(
        lambda r: period_labels[next((i for i,v in enumerate(r) if v), 0)],
        axis=1)
    pivot['last_period'] = pivot[period_cols].apply(
        lambda r: period_labels[max((i for i,v in enumerate(r) if v),
                                    default=0)],
        axis=1)
    pivot['periods_active'] = pivot[period_cols].sum(axis=1)
    pivot['gaps'] = pivot[period_cols].apply(
        lambda r: _count_gaps(list(r)), axis=1)

    # Rename period columns to readable labels
    rename_map = {p: l for p, l in zip(periods, period_labels)}
    pivot = pivot.rename(columns=rename_map)

    # Classify BUs
    latest_period = period_labels[-1]
    pivot['currently_active'] = pivot[latest_period].astype(str).map(
        {'1': '✅', '0': '❌', '1.0': '✅', '0.0': '❌'}).fillna('❌')

    # Identify changes
    changes = []
    all_bus = set(pivot['bu_code'])

    for bu in sorted(all_bus):
        row = pivot[pivot['bu_code'] == bu].iloc[0]
        activity = [int(row[l]) for l in period_labels if l in pivot.columns]

        # Find transitions
        for i in range(1, len(activity)):
            if activity[i-1] == 0 and activity[i] == 1:
                changes.append({
                    'bu_code': bu,
                    'event': 'ADDED',
                    'period': period_labels[i],
                    'entity': row['entity'],
                    'branch': row['branch'],
                })
            elif activity[i-1] == 1 and activity[i] == 0:
                changes.append({
                    'bu_code': bu,
                    'event': 'REMOVED',
                    'period': period_labels[i],
                    'entity': row['entity'],
                    'branch': row['branch'],
                })

    df_changes = pd.DataFrame(changes) if changes else pd.DataFrame()

    # Summary
    currently_active = pivot[pivot[latest_period].astype(float) > 0]
    missing_hier = pivot[pivot['in_hierarchy'] == '❌']

    print(f"\n  Currently active ({latest_period}): "
          f"{len(currently_active)} BUs")
    print(f"  Not in hierarchy: {len(missing_hier)} BUs")
    if not df_changes.empty:
        added   = df_changes[df_changes['event']=='ADDED']
        removed = df_changes[df_changes['event']=='REMOVED']
        print(f"  BU additions over all periods: {len(added)}")
        print(f"  BU removals over all periods:  {len(removed)}")

    # Save to Excel
    meta_cols = ['bu_code','entity','bu_name','branch','region',
                 'business','in_hierarchy','currently_active',
                 'first_period','last_period','periods_active','gaps']
    display_cols = meta_cols + [l for l in period_labels
                                if l in pivot.columns]

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Full activity grid
        pivot[display_cols].sort_values(
            ['in_hierarchy','entity','bu_code']
        ).to_excel(writer, sheet_name='BU Activity Grid', index=False)

        # Missing from hierarchy
        missing_hier[display_cols].sort_values(
            ['entity','bu_code']
        ).to_excel(writer, sheet_name='Missing from Hierarchy',
                   index=False)

        # Currently active
        currently_active[display_cols].sort_values(
            ['in_hierarchy','entity','bu_code']
        ).to_excel(writer, sheet_name='Currently Active', index=False)

        # Changes timeline
        if not df_changes.empty:
            df_changes.sort_values(['period','event']).to_excel(
                writer, sheet_name='Additions & Removals', index=False)

    print(f"\n✅ Saved to: {output}")
    print(f"\nTabs:")
    print(f"  'BU Activity Grid'      — all BUs, all periods (1=active)")
    print(f"  'Missing from Hierarchy'— BUs in SAC not in hierarchy")
    print(f"  'Currently Active'      — active in latest period")
    print(f"  'Additions & Removals'  — BU scope changes over time")


def _count_gaps(activity: list) -> int:
    """Count periods where BU was inactive between first and last active."""
    if not any(activity):
        return 0
    first = next(i for i, v in enumerate(activity) if v)
    last  = max(i for i, v in enumerate(activity) if v)
    return activity[first:last+1].count(0)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='data/bu_activity.xlsx')
    args = parser.parse_args()
    build_bu_timeline(args.output)
