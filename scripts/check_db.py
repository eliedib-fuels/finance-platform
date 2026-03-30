import sys, sqlite3
sys.path.insert(0, '.')
from scripts.config import DB_PATH

# Add 2025 budget rates
# Using REVISED rates (more accurate for full year comparison)
# Format: EUR per 1 unit of currency
RATES_2025 = [
    (2025, 'USD', 0.952381, 1.0,       0.952381),  # EUR/USD=1.05
    (2025, 'CAD', 0.680272, 0.714286,  0.680272),  # CAD/USD = 0.952381/0.680272
    (2025, 'MXN', 0.045960, 0.048258,  0.045960),
    (2025, 'GYD', 0.004591, 0.004821,  0.004591),
    (2025, 'EUR', 1.0,      1.05,      1.0),
]

conn = sqlite3.connect(DB_PATH)

# Check current rates
cur = conn.execute("SELECT fiscal_year, currency FROM budget_rates")
existing = [(r[0], r[1]) for r in cur.fetchall()]
print('Existing rates:', existing)

# Delete existing 2025
conn.execute("DELETE FROM budget_rates WHERE fiscal_year=2025")

# Insert 2025
for fy, ccy, to_eur, to_usd, eur_rate in RATES_2025:
    # to_usd = to_eur / usd_to_eur
    usd_to_eur = 0.952381
    to_usd_calc = to_eur / usd_to_eur
    conn.execute("""
        INSERT INTO budget_rates
        (fiscal_year, currency, rate_to_eur, rate_to_usd,
         usd_eur_rate, source)
        VALUES (?,?,?,?,?,'B25_revised')
    """, (fy, ccy, to_eur, to_usd_calc, usd_to_eur))

conn.commit()
conn.close()
print('2025 rates loaded')

# Verify
from scripts.db import query
df = query("SELECT * FROM budget_rates ORDER BY fiscal_year, currency")
print(df.to_string())
