import sys, sqlite3
sys.path.insert(0, '.')
from scripts.config import DB_PATH

conn = sqlite3.connect(DB_PATH)

# Remove JDE entries where COA already covers the account
r1 = conn.execute("""
    DELETE FROM account_master
    WHERE source='JDE'
    AND account_pattern IN (
        SELECT account_pattern FROM account_master WHERE source='COA'
    )
""")
print(f'JDE duplicates removed: {r1.rowcount}')

# Remove JDE pattern entries where COA already covers
r2 = conn.execute("""
    DELETE FROM account_master
    WHERE source='JDE' AND match_type='pattern'
    AND account_pattern IN (
        SELECT account_pattern FROM account_master WHERE source='COA'
    )
""")
print(f'JDE pattern duplicates removed: {r2.rowcount}')

# Remove COA null-subline where COA has proper subline
r3 = conn.execute("""
    DELETE FROM account_master
    WHERE source='COA' AND ad50_subline IS NULL
    AND account_pattern IN (
        SELECT account_pattern FROM account_master
        WHERE source='COA' AND ad50_subline IS NOT NULL
    )
""")
print(f'COA null-subline duplicates removed: {r3.rowcount}')

conn.commit()
conn.close()
print('Done')
