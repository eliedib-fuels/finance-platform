-- Drop and recreate sac_detail with full column set
DROP TABLE IF EXISTS sac_detail;

CREATE TABLE IF NOT EXISTS sac_detail (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bu_code         TEXT,
    bu_name         TEXT,
    entity          TEXT,
    account_code    TEXT,
    ad50_line       TEXT,
    ad50_label      TEXT,
    fiscal_year     INTEGER,
    fiscal_period   INTEGER,
    amount_lc       REAL,
    currency        TEXT,
    amount_usd      REAL,
    amount_eur      REAL,
    plan_type       TEXT    DEFAULT 'ACTUAL',
    data_source     TEXT,
    source          TEXT,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sac_detail_period
    ON sac_detail(fiscal_year, fiscal_period);
CREATE INDEX IF NOT EXISTS idx_sac_detail_line
    ON sac_detail(ad50_line);
CREATE INDEX IF NOT EXISTS idx_sac_detail_bu
    ON sac_detail(bu_code);
CREATE INDEX IF NOT EXISTS idx_sac_detail_entity
    ON sac_detail(entity);
