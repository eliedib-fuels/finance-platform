-- Official AD50 table — source of truth from management reports
CREATE TABLE IF NOT EXISTS official_ad50 (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    tab             TEXT,       -- US / CentralAm / MGT / OCM / Total
    ad50_line       TEXT,       -- 01, 02, 07A, 09G etc.
    ad50_label      TEXT,       -- Billing, WIP etc.
    fiscal_year     INTEGER,
    fiscal_period   INTEGER,
    amount_usd      REAL,       -- already USD at budget rate
    plan_type       TEXT DEFAULT 'ACTUAL',
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_official_ad50_period
    ON official_ad50(fiscal_year, fiscal_period);
CREATE INDEX IF NOT EXISTS idx_official_ad50_line
    ON official_ad50(ad50_line);
CREATE INDEX IF NOT EXISTS idx_official_ad50_tab
    ON official_ad50(tab);
