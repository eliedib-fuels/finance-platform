-- ============================================================
-- FINANCE PLATFORM — SQLite Schema
-- Portable to AWS RDS PostgreSQL (minor syntax changes only)
-- ============================================================

-- -------------------------------------------
-- 1. ENTITIES
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS entities (
    company_code      TEXT PRIMARY KEY,   -- 00577, 01033 etc
    entity_name       TEXT NOT NULL,
    currency          TEXT NOT NULL,      -- USD, CAD, MXN
    active            INTEGER DEFAULT 1,
    notes             TEXT
);

-- -------------------------------------------
-- 2. ORG HIERARCHY
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS org_hierarchy (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    bu_code           TEXT NOT NULL,
    bu_name           TEXT,
    branch            TEXT,
    region            TEXT,
    business          TEXT,
    effective_from    DATE NOT NULL DEFAULT '2020-01-01',
    effective_to      DATE,
    change_reason     TEXT,
    UNIQUE(bu_code, effective_from)
);

-- -------------------------------------------
-- 3. BUDGET RATES (to EUR — divide for USD)
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS budget_rates (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    fiscal_year       INTEGER NOT NULL,
    currency          TEXT NOT NULL,
    rate_to_eur       REAL NOT NULL,      -- local × rate = EUR amount
    rate_to_usd       REAL,              -- derived: rate_to_eur / usd_rate
    usd_eur_rate      REAL,              -- EUR/USD for the year
    source            TEXT DEFAULT 'manual',
    set_date          DATE,
    UNIQUE(fiscal_year, currency)
);

-- -------------------------------------------
-- 4. ACCOUNT MASTER (COA + AD50 mapping)
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS account_master (
    account_code       TEXT PRIMARY KEY,
    account_desc       TEXT,             -- from COA (reference only)
    account_type       TEXT,             -- PL or BS
    account_class      TEXT,             -- P&L-Revenue, BS-Inventory etc

    -- AD50 parent level (largest view)
    ad50_parent_raw    TEXT,             -- e.g. "7"
    ad50_parent_label  TEXT,             -- e.g. "Personnel Costs"
    ad50_parent_sort   TEXT,             -- e.g. "07"

    -- AD50 line level
    ad50_line_raw      TEXT,             -- e.g. "7a"
    ad50_line_label    TEXT,             -- e.g. "Production Pers Cost"
    ad50_line_sort     TEXT,             -- e.g. "07a"

    -- AD50 detail level (line 9 only)
    ad50_detail_raw    TEXT,             -- e.g. "9b"
    ad50_detail_label  TEXT,             -- e.g. "Lab Consumables"
    ad50_detail_sort   TEXT,             -- e.g. "09b"

    -- Flags
    is_ig              INTEGER DEFAULT 0,
    is_production      INTEGER DEFAULT 0,
    active             INTEGER DEFAULT 1
);

-- -------------------------------------------
-- 5. AD50 LINE MASTER (management P&L labels)
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS ad50_lines (
    ad50_line          TEXT PRIMARY KEY,  -- e.g. "07A1", "09C"
    ad50_label         TEXT NOT NULL,     -- always wins in display
    ad50_sort_key      TEXT NOT NULL,     -- zero-padded for ordering
    ad50_parent        TEXT,             -- parent line e.g. "07"
    ad50_parent_label  TEXT,
    ad50_parent_sort   TEXT,
    line_type          TEXT DEFAULT 'financial', -- financial/fte/cash
    is_ig              INTEGER DEFAULT 0,
    is_subtotal        INTEGER DEFAULT 0  -- e.g. "03 Revenue" = subtotal
);

-- -------------------------------------------
-- 6. GL TRANSACTIONS (transaction level)
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS gl_transactions (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Identity
    company               TEXT NOT NULL,
    bu_code               TEXT,
    account_code          TEXT NOT NULL,
    account_desc          TEXT,
    bu_obj_account        TEXT,

    -- Classification
    account_type          TEXT,           -- PL or BS
    account_class         TEXT,

    -- Ledger
    ledger_type           TEXT NOT NULL,  -- AA, GP, UE

    -- Time
    period                TEXT NOT NULL,  -- 2026/001
    gl_date               DATE,
    fiscal_year           INTEGER,
    fiscal_period         INTEGER,

    -- Document
    document_number       TEXT,
    document_type         TEXT,           -- PV, AE, N9 etc
    reversing_entry_code  TEXT,           -- R = reversing
    batch_number          TEXT,
    user_id               TEXT,

    -- Description
    explanation_alpha     TEXT,
    explanation_remark    TEXT,
    invoice_number        TEXT,
    po_number             TEXT,
    address_book_number   TEXT,

    -- Amounts
    amount_local          REAL NOT NULL,
    currency_local        TEXT DEFAULT 'USD',
    amount_usd            REAL,
    amount_eur            REAL,
    eur_rate_used         REAL,
    usd_rate_used         REAL,

    -- Metadata
    source_file           TEXT,
    loaded_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- -------------------------------------------
-- 7. PLAN DATA (actuals + budget + forecasts)
-- One table for all plan types
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS plan_data (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    company           TEXT NOT NULL,
    bu_code           TEXT,
    ad50_line         TEXT NOT NULL,      -- AD50 line code
    ad50_label        TEXT,
    fiscal_year       INTEGER NOT NULL,
    fiscal_period     INTEGER NOT NULL,

    -- Plan type
    plan_type         TEXT NOT NULL,      -- ACTUAL/INITIAL_BUDGET/LATEST_FORECAST/ROLLING_FORECAST
    version           TEXT NOT NULL,      -- ACTUAL_2026/BUDGET_2026/FQ2_2026/RF_2026_MAR

    -- Amounts
    amount_lc         REAL,              -- local currency
    currency_local    TEXT DEFAULT 'USD',
    amount_usd        REAL,
    amount_eur        REAL,

    -- Metadata
    entry_type        TEXT DEFAULT 'actual',  -- actual/forecast
    source            TEXT DEFAULT 'sac',     -- sac/manual/ai_generated
    loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(company, bu_code, ad50_line, fiscal_year, fiscal_period, plan_type, version)
);

-- -------------------------------------------
-- 8. FTE DATA (headcount — AVG not SUM)
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS fte_data (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    company           TEXT NOT NULL,
    bu_code           TEXT NOT NULL,
    fiscal_year       INTEGER NOT NULL,
    fiscal_period     INTEGER NOT NULL,
    fte_type          TEXT NOT NULL,      -- production / npbo / total
    fte_count         REAL NOT NULL,      -- decimal FTE
    source_file       TEXT,
    loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company, bu_code, fiscal_year, fiscal_period, fte_type)
);

-- -------------------------------------------
-- 9. TRIAL BALANCE
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS trial_balance (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    company           TEXT NOT NULL,
    account_code      TEXT NOT NULL,
    account_desc      TEXT,
    account_type      TEXT,
    fiscal_year       INTEGER NOT NULL,
    fiscal_period     INTEGER NOT NULL,
    opening_usd       REAL,
    movement_usd      REAL,
    closing_usd       REAL,
    opening_eur       REAL,
    movement_eur      REAL,
    closing_eur       REAL,
    check_status      TEXT,              -- PASS/WARN/FAIL
    difference_usd    REAL,
    loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(company, account_code, fiscal_year, fiscal_period)
);

-- -------------------------------------------
-- 10. VALIDATION LOG
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS validation_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file       TEXT,
    check_name        TEXT,
    check_level       TEXT,              -- ERROR/WARN/INFO
    dimension         TEXT,
    dimension_value   TEXT,
    source_total      REAL,
    db_total          REAL,
    difference        REAL,
    status            TEXT,             -- PASS/WARN/FAIL
    notes             TEXT
);

-- -------------------------------------------
-- 11. PRE INGESTION LOG
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS pre_ingestion_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file       TEXT,
    check_name        TEXT,
    status            TEXT,             -- PASS/WARN/FAIL/ERROR
    detail            TEXT,
    blocking          INTEGER DEFAULT 0  -- 1 = blocks load
);

-- -------------------------------------------
-- 12. LOAD HISTORY
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS load_history (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    load_date         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file       TEXT NOT NULL,
    file_type         TEXT,             -- GL/AD50/HIERARCHY/RATES
    company           TEXT,
    fiscal_year       INTEGER,
    fiscal_period     INTEGER,
    rows_deleted      INTEGER DEFAULT 0,
    rows_inserted     INTEGER DEFAULT 0,
    validation_status TEXT,
    duration_seconds  REAL,
    loaded_by         TEXT DEFAULT 'pipeline'
);

-- -------------------------------------------
-- 13. UNMAPPED BUS (caught by pre-check)
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS unmapped_bus (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    bu_code           TEXT NOT NULL,
    bu_name           TEXT,
    first_seen        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen         TIMESTAMP,
    source_file       TEXT,
    row_count         INTEGER,
    resolved          INTEGER DEFAULT 0,
    resolved_date     TIMESTAMP,
    UNIQUE(bu_code)
);

-- -------------------------------------------
-- 14. UNMAPPED ACCOUNTS (caught by pre-check)
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS unmapped_accounts (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    account_code      TEXT NOT NULL,
    account_desc      TEXT,
    first_seen        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen         TIMESTAMP,
    source_file       TEXT,
    row_count         INTEGER,
    auto_type         TEXT,             -- auto-detected from first digit
    auto_class        TEXT,
    resolved          INTEGER DEFAULT 0,
    resolved_date     TIMESTAMP,
    UNIQUE(account_code)
);

-- -------------------------------------------
-- 15. AI INSIGHTS
-- -------------------------------------------
CREATE TABLE IF NOT EXISTS ai_insights (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    generated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    insight_type      TEXT,             -- weekly_narrative/anomaly/deep_dive
    fiscal_year       INTEGER,
    fiscal_period     INTEGER,
    content           TEXT,
    model_used        TEXT,
    prompt_tokens     INTEGER,
    completion_tokens INTEGER
);

-- -------------------------------------------
-- INDEXES
-- -------------------------------------------
CREATE INDEX IF NOT EXISTS idx_gl_period
    ON gl_transactions(fiscal_year, fiscal_period);
CREATE INDEX IF NOT EXISTS idx_gl_company
    ON gl_transactions(company);
CREATE INDEX IF NOT EXISTS idx_gl_bu
    ON gl_transactions(bu_code);
CREATE INDEX IF NOT EXISTS idx_gl_account
    ON gl_transactions(account_code);
CREATE INDEX IF NOT EXISTS idx_gl_ledger
    ON gl_transactions(ledger_type);
CREATE INDEX IF NOT EXISTS idx_gl_type
    ON gl_transactions(account_type);
CREATE INDEX IF NOT EXISTS idx_plan_lookup
    ON plan_data(company, fiscal_year, fiscal_period, plan_type);
CREATE INDEX IF NOT EXISTS idx_hierarchy_bu
    ON org_hierarchy(bu_code);
CREATE INDEX IF NOT EXISTS idx_fte_lookup
    ON fte_data(company, fiscal_year, fiscal_period);
