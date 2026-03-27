import sys
sys.path.insert(0, '.')
from scripts.db import query

# 9G total and by entity
g9 = query("""
    SELECT s.entity, SUM(s.amount_usd) as usd
    FROM sac_detail s
    JOIN account_master am
        ON s.account_code = am.account_pattern
        AND am.source='COA' AND am.ad50_subline='9G'
    WHERE s.fiscal_year=2026 AND s.fiscal_period=1
    GROUP BY s.entity ORDER BY s.entity
""")
print('9G by entity:')
total = 0
for _, r in g9.iterrows():
    v = float(r['usd'])/1000
    print(f"  {r.entity}: {v:,.1f}K")
    total += float(r['usd'])
print(f"  TOTAL: {total/1000:,.1f}K")
print(f"  Official: -503.0K")
print(f"  Gap: {total/1000 - (-503):,.1f}K")
print()

# Full line 09 total
t09 = query("""
    SELECT SUM(amount_usd) as usd FROM sac_detail
    WHERE fiscal_year=2026 AND fiscal_period=1 AND ad50_line='09'
""")
print(f"Line 09 total: {float(t09.iloc[0,0])/1000:,.1f}K")
print(f"Official:      -3,589.0K")
