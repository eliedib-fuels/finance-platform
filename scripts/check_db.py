import sys, sqlite3
sys.path.insert(0, '.')
from scripts.config import DB_PATH

conn = sqlite3.connect(DB_PATH)

# Check current sac_detail columns
cursor = conn.execute("PRAGMA table_info(sac_detail)")
cols = cursor.fetchall()
print("Current sac_detail columns:")
for c in cols:
    print(f"  {c[1]}")

# Add missing source column if needed
col_names = [c[1] for c in cols]
if 'source' not in col_names:
    conn.execute("ALTER TABLE sac_detail ADD COLUMN source TEXT")
    conn.commit()
    print("\nAdded 'source' column")
else:
    print("\n'source' column already exists")

conn.close()
print("Done")
