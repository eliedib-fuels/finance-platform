-- Add to schema.sql — run once to add new tables

CREATE TABLE IF NOT EXISTS sac_detail (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bu_code         TEXT,
    entity          TEXT,
    account_code    TEXT,
    ad50_line       TEXT,
    fiscal_year     INTEGER,
    fiscal_period   INTEGER,
    amount_lc       REAL,
    currency        TEXT,
    amount_usd      REAL,
    data_source     TEXT,   -- 'SAC_DETAIL' or 'SAC_PLUG_GYD'
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sac_detail_period
    ON sac_detail(fiscal_year, fiscal_period);
CREATE INDEX IF NOT EXISTS idx_sac_detail_line
    ON sac_detail(ad50_line);
CREATE INDEX IF NOT EXISTS idx_sac_detail_bu
    ON sac_detail(bu_code);

-- Update account_master if columns missing
CREATE TABLE IF NOT EXISTS account_master (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account_pattern TEXT,
    account_desc    TEXT,
    ad50_line       TEXT,
    ad50_subline    TEXT,
    source          TEXT,   -- 'JDE' or 'FLEX'
    match_type      TEXT,   -- 'exact' or 'pattern'
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_account_master_pattern
    ON account_master(account_pattern);
CREATE INDEX IF NOT EXISTS idx_account_master_line
    ON account_master(ad50_line);
