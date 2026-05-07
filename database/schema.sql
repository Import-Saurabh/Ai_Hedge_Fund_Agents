-- SQLite schema for stock market database
-- Generated from SQLite .schema output

-- Core stocks table
CREATE TABLE IF NOT EXISTS stocks (
    symbol          TEXT PRIMARY KEY,
    name            TEXT,
    exchange        TEXT DEFAULT 'NSE',
    sector          TEXT,
    industry        TEXT,
    currency        TEXT DEFAULT 'INR',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily price data
CREATE TABLE IF NOT EXISTS price_daily (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    date            DATE NOT NULL,
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    adj_close       REAL,
    volume          INTEGER,
    UNIQUE (symbol, date)
);

-- Intraday price data
CREATE TABLE IF NOT EXISTS price_intraday (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    ts              TIMESTAMP NOT NULL,
    interval        TEXT NOT NULL DEFAULT '1m',
    open            REAL, high REAL, low REAL, close REAL, volume INTEGER,
    UNIQUE (symbol, ts, interval)
);

-- Fundamentals data
CREATE TABLE IF NOT EXISTS fundamentals (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                  TEXT NOT NULL REFERENCES stocks(symbol),
    as_of_date              DATE NOT NULL,

    -- Screener overview (most authoritative — scraped directly)
    current_price           REAL,
    face_value              REAL,
    high_52w                REAL,
    low_52w                 REAL,
    book_value              REAL,

    -- Profitability
    roe_pct                 REAL,
    roce_pct                REAL,
    roa_pct                 REAL,
    interest_coverage       REAL,

    -- Cash flow (Rs. Crores)
    gross_margin_pct        REAL,
    net_profit_margin_pct   REAL,
    ebitda_margin_pct       REAL,
    ebit_margin_pct         REAL,
    opm_pct                 REAL,

    -- Leverage & liquidity
    debt_to_equity          REAL,
    current_ratio           REAL,
    quick_ratio             REAL,

    -- Working capital efficiency
    dso_days                REAL,
    dio_days                REAL,
    dpo_days                REAL,
    cash_conversion_cycle   REAL,
    working_capital_days    REAL,

    -- Valuation
    eps_annual              REAL,
    pe_ratio                REAL,
    pb_ratio                REAL,
    graham_number           REAL,
    dividend_yield_pct      REAL,
    dividend_payout_pct     REAL,
    forward_pe              REAL,

    -- Scale (Rs. Crores)
    market_cap              REAL,
    revenue                 REAL,
    ebitda                  REAL,
    inventory               REAL,
    ev                      REAL,
    ttm_eps                 REAL,
    ttm_pe                  REAL,
    ev_ebitda               REAL,
    ev_revenue              REAL,

    -- Growth
    earnings_growth_json    TEXT,

    -- Data quality
    data_source             TEXT DEFAULT 'yfinance',
    completeness_pct        REAL,

    UNIQUE (symbol, as_of_date)
);

-- Quarterly results
CREATE TABLE IF NOT EXISTS quarterly_results (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL REFERENCES stocks(symbol),
    period_end          DATE NOT NULL,

    -- P&L (Rs. Crores)
    sales               REAL NOT NULL,
    expenses            REAL,
    operating_profit    REAL,
    opm_pct             REAL,
    other_income        REAL,
    interest            REAL,
    depreciation        REAL,
    profit_before_tax   REAL,
    tax_pct             REAL,
    net_profit          REAL NOT NULL,
    eps                 REAL,

    -- Data quality
    data_source              TEXT DEFAULT 'Screener.in',
    is_audited          INTEGER DEFAULT 0,
    completeness_pct    REAL,

    UNIQUE (symbol, period_end)
);

-- Annual results
CREATE TABLE IF NOT EXISTS annual_results (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                  TEXT NOT NULL REFERENCES stocks(symbol),
    period_end              DATE NOT NULL,

    -- P&L (Rs. Crores)
    sales                   REAL NOT NULL,
    expenses                REAL,
    operating_profit        REAL,
    opm_pct                 REAL,
    other_income            REAL,
    interest                REAL,
    depreciation            REAL,
    profit_before_tax       REAL,
    tax_pct                 REAL,
    net_profit              REAL NOT NULL,
    eps                     REAL,
    dividend_payout_pct     REAL,

    -- Data quality
    source                  TEXT DEFAULT 'Screener.in',
    completeness_pct        REAL,
    is_ttm                  INTEGER DEFAULT 0,
    data_source             TEXT DEFAULT 'screener',

    UNIQUE (symbol, period_end)
);

-- Annual cashflow derived
CREATE TABLE IF NOT EXISTS "annual_cashflow_derived" (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL REFERENCES stocks(symbol),
    annual_end         DATE NOT NULL,

    revenue             REAL,
    net_income          REAL,
    dna                 REAL,
    approx_op_cf        REAL,
    approx_capex        REAL,
    approx_fcf          REAL,
    fcf_margin_pct      REAL,

    capex_source        TEXT,
    quality_score       INTEGER DEFAULT 1,
    is_real             INTEGER DEFAULT 0,
    is_interpolated     INTEGER DEFAULT 0,
    data_note           TEXT,
    unit                TEXT DEFAULT 'Rs_Crores',

    UNIQUE (symbol, annual_end)
);

-- Corporate actions
CREATE TABLE IF NOT EXISTS corporate_actions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    action_date     DATE NOT NULL,
    action_type     TEXT NOT NULL,
    value           REAL,
    notes           TEXT,
    UNIQUE (symbol, action_date, action_type)
);

-- Technical indicators
CREATE TABLE IF NOT EXISTS technical_indicators (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    date            DATE NOT NULL,
    close           REAL,
    rsi_14          REAL,
    macd            REAL,
    macd_signal     REAL,
    macd_hist       REAL,
    sma_50          REAL,
    sma_200         REAL,
    ema_21          REAL,
    bb_mid          REAL,
    bb_upper        REAL,
    bb_lower        REAL,
    atr_14          REAL,
    adx_14          REAL,
    vwap_14         REAL,
    obv             REAL,
    supertrend      REAL,
    supertrend_dir  INTEGER,
    UNIQUE (symbol, date)
);

-- Market indices
CREATE TABLE IF NOT EXISTS market_indices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   DATE NOT NULL,
    index_name      TEXT NOT NULL,
    last_price      REAL, change_pct REAL, direction TEXT,
    UNIQUE (snapshot_date, index_name)
);

-- Forex & commodities
CREATE TABLE IF NOT EXISTS forex_commodities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   DATE NOT NULL,
    instrument      TEXT NOT NULL,
    last_price      REAL, change_pct REAL,
    UNIQUE (snapshot_date, instrument)
);

-- RBI rates
CREATE TABLE IF NOT EXISTS rbi_rates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    effective_date  DATE NOT NULL,
    repo_rate       REAL, reverse_repo REAL, sdf_rate REAL,
    msf_rate        REAL, bank_rate REAL, crr REAL, slr REAL,
    is_cached       INTEGER DEFAULT 0, source TEXT,
    UNIQUE (effective_date)
);

-- Macro indicators
CREATE TABLE IF NOT EXISTS macro_indicators (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   DATE NOT NULL,
    indicator_name  TEXT NOT NULL,
    source TEXT, value REAL, unit TEXT, year INTEGER, notes TEXT,
    UNIQUE (snapshot_date, indicator_name, year)
);

-- Ownership
CREATE TABLE IF NOT EXISTS ownership (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                  TEXT NOT NULL REFERENCES stocks(symbol),
    snapshot_date           DATE NOT NULL,
    promoter_pct            REAL,
    fii_fpi_pct             REAL,
    dii_pct                 REAL,
    public_retail_pct       REAL,
    num_shareholders        INTEGER,
    insiders_pct            REAL,
    institutions_pct        REAL,
    institutions_float_pct  REAL,
    institutions_count      INTEGER,
    total_institutional_pct REAL,
    fii_net_buy_cr          REAL,
    dii_net_buy_cr          REAL,
    fii_dii_flow_date       TEXT,
    source                  TEXT,
    UNIQUE (symbol, snapshot_date)
);

-- Ownership history
CREATE TABLE IF NOT EXISTS ownership_history (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                  TEXT NOT NULL REFERENCES stocks(symbol),
    period_end              DATE NOT NULL,
    promoter_pct            REAL NOT NULL,
    fii_pct                 REAL,
    dii_pct                 REAL,
    public_pct              REAL,
    total_institutional_pct REAL,
    num_shareholders        INTEGER,
    source                  TEXT DEFAULT 'Screener.in',
    UNIQUE (symbol, period_end)
);

-- Earnings history
CREATE TABLE IF NOT EXISTS earnings_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    quarter_end     DATE NOT NULL,
    eps_actual      REAL, eps_estimate REAL,
    eps_difference  REAL, surprise_pct REAL,
    UNIQUE (symbol, quarter_end)
);

-- Earnings estimates
CREATE TABLE IF NOT EXISTS earnings_estimates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    snapshot_date   DATE NOT NULL,
    period_code     TEXT NOT NULL,
    avg_eps REAL, low_eps REAL, high_eps REAL,
    year_ago_eps REAL, analyst_count INTEGER, growth_pct REAL,
    UNIQUE (symbol, snapshot_date, period_code)
);

-- EPS trend
CREATE TABLE IF NOT EXISTS eps_trend (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    snapshot_date   DATE NOT NULL,
    period_code     TEXT NOT NULL,
    current_est REAL, seven_days_ago REAL, thirty_days_ago REAL,
    sixty_days_ago REAL, ninety_days_ago REAL,
    UNIQUE (symbol, snapshot_date, period_code)
);

-- EPS revisions
CREATE TABLE IF NOT EXISTS eps_revisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    snapshot_date   DATE NOT NULL,
    period_code     TEXT NOT NULL,
    up_last_7d INTEGER, up_last_30d INTEGER,
    down_last_30d INTEGER, down_last_7d INTEGER,
    UNIQUE (symbol, snapshot_date, period_code)
);

-- Data quality log
CREATE TABLE IF NOT EXISTS data_quality_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    symbol              TEXT NOT NULL,
    table_name          TEXT NOT NULL,
    rows_inserted       INTEGER DEFAULT 0,
    rows_null_heavy     INTEGER DEFAULT 0,
    avg_completeness    REAL,
    critical_nulls_json TEXT,
    source              TEXT,
    notes               TEXT
);

-- Run log
CREATE TABLE IF NOT EXISTS run_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    run_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    script_version  TEXT,
    modules_ok      TEXT,
    modules_warn    TEXT,
    notes           TEXT
);

-- Growth metrics
CREATE TABLE IF NOT EXISTS "growth_metrics"(
    id INT,
    symbol TEXT,
    as_of_date NUM,
    revenue_cagr_3y REAL,
    net_profit_cagr_3y REAL,
    ebitda_cagr_3y REAL,
    eps_cagr_3y REAL,
    fcf_cagr_3y REAL,
    sales_cagr_10y REAL,
    sales_cagr_5y REAL,
    sales_cagr_3y REAL,
    sales_ttm REAL,
    profit_cagr_10y REAL,
    profit_cagr_5y REAL,
    profit_cagr_3y REAL,
    profit_ttm REAL,
    stock_cagr_10y REAL,
    stock_cagr_5y REAL,
    stock_cagr_3y REAL,
    stock_ttm REAL,
    growth_available INT,
    completeness_pct REAL,
    roe_10y REAL,
    roe_5y REAL,
    roe_3y REAL,
    roe_last REAL
);

-- Balance sheet
CREATE TABLE IF NOT EXISTS balance_sheet (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    period_end DATE NOT NULL,
    period_type TEXT NOT NULL DEFAULT 'annual',
    equity_capital REAL,
    reserves REAL,
    total_equity REAL,
    borrowings REAL,
    lt_borrowings REAL,
    st_borrowings REAL,
    lease_liabilities REAL,
    preference_capital REAL,
    other_borrowings REAL,
    other_liabilities REAL,
    minority_interest REAL,
    trade_payables REAL,
    advance_from_customers REAL,
    other_liability_items REAL,
    total_liabilities REAL,
    fixed_assets REAL,
    cwip REAL,
    investments REAL,
    other_assets REAL,
    inventories REAL,
    trade_receivables REAL,
    cash_equivalents REAL,
    loans_advances REAL,
    other_asset_items REAL,
    total_assets REAL,
    net_debt REAL,
    data_source TEXT DEFAULT 'screener',
    completeness_pct REAL,
    missing_fields_json TEXT,
    UNIQUE(symbol, period_end, period_type)
);

-- Cash flow
CREATE TABLE IF NOT EXISTS cash_flow (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                      TEXT NOT NULL REFERENCES stocks(symbol),
    period_end                  DATE NOT NULL,
    period_type                 TEXT NOT NULL DEFAULT 'annual',
    cfo                         REAL,
    cfi                         REAL,
    cff                         REAL,
    capex                       REAL,
    free_cash_flow              REAL,
    net_cash_flow               REAL,
    raw_details_json            TEXT,
    data_source                 TEXT DEFAULT 'screener',
    updated_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completeness_pct            REAL,
    missing_fields_json         TEXT,
    UNIQUE (symbol, period_end, period_type)
);

-- Profit and loss (legacy/alternative table)
CREATE TABLE IF NOT EXISTS profit_and_loss (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    period_end DATE NOT NULL,
    period_type TEXT NOT NULL DEFAULT 'annual',
    sales REAL,
    expenses REAL,
    operating_profit REAL,
    opm_pct TEXT,
    other_income REAL,
    interest REAL,
    depreciation REAL,
    profit_before_tax REAL,
    tax_pct TEXT,
    net_profit REAL,
    eps REAL,
    dividend_payout_pct REAL,
    is_interpolated INTEGER DEFAULT 0,
    data_source TEXT DEFAULT 'screener',
    completeness_pct REAL,
    missing_fields_json TEXT
);

-- System table (optional, created automatically by SQLite)

-- Indexes
CREATE INDEX IF NOT EXISTS idx_price_sym_date ON price_daily(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_price_intra_sym ON price_intraday(symbol, ts DESC);
CREATE INDEX IF NOT EXISTS idx_qr_sym ON quarterly_results(symbol, period_end DESC);
CREATE INDEX IF NOT EXISTS idx_ar_sym ON annual_results(symbol, period_end DESC);
CREATE INDEX IF NOT EXISTS idx_qcd_sym ON "annual_cashflow_derived"(symbol, annual_end DESC);
CREATE INDEX IF NOT EXISTS idx_ca_sym ON corporate_actions(symbol, action_date DESC);
CREATE INDEX IF NOT EXISTS idx_ti_sym ON technical_indicators(symbol, date DESC);
CREATE INDEX IF NOT EXISTS idx_own_hist ON ownership_history(symbol, period_end DESC);
CREATE INDEX IF NOT EXISTS idx_dql_sym ON data_quality_log(symbol, run_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_bs_sym ON balance_sheet(symbol, period_type, period_end DESC);
CREATE INDEX IF NOT EXISTS idx_cf_sym_date ON cash_flow(symbol, period_end DESC);
CREATE INDEX IF NOT EXISTS idx_cf_sym ON cash_flow(symbol, period_type, period_end DESC);

-- Unique indexes
CREATE UNIQUE INDEX IF NOT EXISTS growth_metrics_symbol_date
ON growth_metrics(symbol, as_of_date);

CREATE UNIQUE INDEX IF NOT EXISTS idx_pnl_unique
ON profit_and_loss(symbol, period_end, period_type);