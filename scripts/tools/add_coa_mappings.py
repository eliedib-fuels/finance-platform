"""Add missing COA mappings to account_master."""
import sys
sys.path.insert(0, '.')
from scripts.db import get_conn

MAPPINGS = [
    ('606304', '9I',   '09', 'exact'),
    ('613500', '9F',   '09', 'exact'),
    ('615202', '9G',   '09', 'exact'),
    ('615212', '9F',   '09', 'exact'),
    ('615220', '9H',   '09', 'exact'),
    ('615304', '9G',   '09', 'exact'),
    ('622610', '9L2',  '09', 'exact'),
    ('625101', '9D1',  '09', 'exact'),
    ('625701', '9K',   '09', 'exact'),
    ('625702', '9K',   '09', 'exact'),
    ('625853', '9D2',  '09', 'exact'),
    ('625861', '9D2',  '09', 'exact'),
    ('60620_', '9H',   '09', 'pattern'),
    ('62510_', '9D1',  '09', 'pattern'),
    ('62511_', '9D2',  '09', 'pattern'),
    ('62512_', '9D1',  '09', 'pattern'),
    ('62518_', '9D1',  '09', 'pattern'),
    ('62580_', '9D1',  '09', 'pattern'),
    ('62586_', '9D2',  '09', 'pattern'),
    ('62620_', '9I',   '09', 'pattern'),
    ('62810_', '9I',   '09', 'pattern'),
    # Additional 9G accounts
    ('613111', '9G',   '09', 'exact'),
    ('613112', '9G',   '09', 'exact'),
    ('613113', '9G',   '09', 'exact'),
    ('613114', '9G',   '09', 'exact'),
    ('613115', '9G',   '09', 'exact'),
    ('613210', '9G',   '09', 'exact'),
    ('613211', '9G',   '09', 'exact'),
    ('613212', '9G',   '09', 'exact'),
    ('613213', '9G',   '09', 'exact'),
    ('613214', '9G',   '09', 'exact'),
    ('613215', '9G',   '09', 'exact'),
    ('613220', '9G',   '09', 'exact'),
    ('614000', '9G',   '09', 'exact'),
    ('615305', '9G',   '09', 'exact'),
    ('616010', '9G',   '09', 'exact'),
    ('625510', '9G',   '09', 'exact'),
]

with get_conn() as conn:
    added = 0
    for pattern, subline, line, match_type in MAPPINGS:
        # Skip if already exists
        existing = conn.execute("""
            SELECT 1 FROM account_master
            WHERE account_pattern=? AND ad50_subline=? AND source='COA'
        """, (pattern, subline)).fetchone()
        if not existing:
            conn.execute("""
                INSERT INTO account_master
                (account_pattern, ad50_line, ad50_subline,
                 source, match_type, account_desc)
                VALUES (?,?,?,'COA',?,?)
            """, (pattern, line, subline, match_type, pattern))
            added += 1

print(f'Added {added} mappings to account_master')
