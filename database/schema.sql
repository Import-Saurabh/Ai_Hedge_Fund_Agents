-- ============================================================
--  BUFFETT-GRADE STOCK INTELLIGENCE SYSTEM — SQLite Schema
--  Generated for: ADANIPORTS (extensible to any symbol)
--  Modules: Price · Fundamentals · Statements · Actions
--           Technicals · Macro · Ownership · Earnings · Growth
--  News excluded (stored separately)
-- ============================================================

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ────────────────────────────────────────────────────────────
--  CORE REFERENCE
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS stocks (
    symbol          TEXT PRIMARY KEY,          -- e.g. 'ADANIPORTS'
    name            TEXT,                      -- full company name
    exchange        TEXT,                      -- 'NSE' | 'BSE'
    sector          TEXT,
    industry        TEXT,
    currency        TEXT DEFAULT 'INR',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ────────────────────────────────────────────────────────────
--  A. PRICE & MARKET DATA
-- ────────────────────────────────────────────────────────────

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
CREATE INDEX IF NOT EXISTS idx_price_daily_sym_date ON price_daily(symbol, date DESC);

CREATE TABLE IF NOT EXISTS price_intraday (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    ts              TIMESTAMP NOT NULL,        -- e.g. 1-min bars
    interval        TEXT NOT NULL DEFAULT '1m',
    open            REAL,
    high            REAL,
    low             REAL,
    close           REAL,
    volume          INTEGER,
    UNIQUE (symbol, ts, interval)
);
CREATE INDEX IF NOT EXISTS idx_price_intraday_sym ON price_intraday(symbol, ts DESC);

-- ────────────────────────────────────────────────────────────
--  B. FUNDAMENTALS  (point-in-time snapshot per run)
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fundamentals (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                  TEXT NOT NULL REFERENCES stocks(symbol),
    as_of_date              DATE NOT NULL,
    
    -- Profitability
    roe_pct                 REAL,              -- %
    roce_pct                REAL,
    roa_pct                 REAL,
    interest_coverage       REAL,              -- x
    
    -- Cash flow
    free_cash_flow          REAL,              -- Rs B
    operating_cf            REAL,
    capex                   REAL,
    
    -- Margins
    gross_margin_pct        REAL,
    net_profit_margin_pct   REAL,
    ebitda_margin_pct       REAL,
    ebit_margin_pct         REAL,
    
    -- Leverage & liquidity
    debt_to_equity          REAL,
    current_ratio           REAL,
    quick_ratio             REAL,
    
    -- Working capital efficiency
    dso_days                REAL,              -- debtor days
    dio_days                REAL,              -- inventory days
    dpo_days                REAL,              -- creditor days
    cash_conversion_cycle   REAL,
    
    -- Valuation
    eps_annual              REAL,
    pe_ratio                REAL,
    pb_ratio                REAL,
    graham_number           REAL,
    dividend_yield_pct      REAL,
    forward_pe              REAL,              -- Added to match Python code
    
    -- Scale / Absolute Values
    market_cap              REAL,              -- Rs B
    revenue                 REAL,
    net_income              REAL,
    ebitda                  REAL,
    inventory               REAL,
    ev                      REAL,              -- Added (Enterprise Value)
    
    -- TTM (Trailing Twelve Months)
    ttm_eps                 REAL,
    ttm_pe                  REAL,
    
    -- Enterprise Multiples
    ev_ebitda               REAL,              -- Added
    ev_revenue              REAL,              -- Added
    
    -- Growth & Metadata
    earnings_growth_json    TEXT,              -- Added (Storing JSON as TEXT)
    
    UNIQUE (symbol, as_of_date)
);
-- ────────────────────────────────────────────────────────────
--  C. FINANCIAL STATEMENTS
-- ────────────────────────────────────────────────────────────

-- period_type: 'annual' | 'quarterly'
-- is_interpolated: 1 = Q-BS filled by the interpolation engine

CREATE TABLE IF NOT EXISTS income_statement (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                      TEXT NOT NULL REFERENCES stocks(symbol),
    period_end                  DATE NOT NULL,
    period_type                 TEXT NOT NULL DEFAULT 'annual',  -- 'annual'|'quarterly'
    -- Top line
    total_revenue               REAL,
    cost_of_revenue             REAL,
    gross_profit                REAL,
    -- Operating
    selling_general_admin       REAL,
    operating_expense           REAL,
    operating_income            REAL,
    -- EBIT / EBITDA
    ebit                        REAL,
    ebitda                      REAL,
    normalized_ebitda           REAL,
    depreciation_amortization   REAL,
    -- Interest
    interest_expense            REAL,
    interest_income             REAL,
    net_interest_expense        REAL,
    -- Bottom line
    pretax_income               REAL,
    tax_provision               REAL,
    net_income                  REAL,
    net_income_common           REAL,
    normalized_income           REAL,
    minority_interests          REAL,
    -- Per share
    diluted_eps                 REAL,
    basic_eps                   REAL,
    diluted_shares              REAL,
    basic_shares                REAL,
    -- Special items
    special_income_charges      REAL,
    total_unusual_items         REAL,
    tax_rate                    REAL,
    is_interpolated             INTEGER DEFAULT 0,
    UNIQUE (symbol, period_end, period_type)
);
CREATE INDEX IF NOT EXISTS idx_is_sym_period ON income_statement(symbol, period_type, period_end DESC);

CREATE TABLE IF NOT EXISTS balance_sheet (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                      TEXT NOT NULL REFERENCES stocks(symbol),
    period_end                  DATE NOT NULL,
    period_type                 TEXT NOT NULL DEFAULT 'annual',
    -- Assets
    total_assets                REAL,
    current_assets              REAL,
    cash_and_equivalents        REAL,
    cash_equivalents            REAL,
    short_term_investments      REAL,
    accounts_receivable         REAL,
    allowance_doubtful          REAL,
    inventory                   REAL,
    prepaid_assets              REAL,
    restricted_cash             REAL,
    other_current_assets        REAL,
    total_non_current_assets    REAL,
    net_ppe                     REAL,
    gross_ppe                   REAL,
    accumulated_depreciation    REAL,
    land_improvements           REAL,
    buildings_improvements      REAL,
    machinery_equipment         REAL,
    construction_in_progress    REAL,
    goodwill                    REAL,
    other_intangibles           REAL,
    long_term_equity_investment REAL,
    investment_in_fin_assets    REAL,
    investment_properties       REAL,
    non_current_deferred_tax_a  REAL,
    other_non_current_assets    REAL,
    -- Liabilities
    total_liabilities           REAL,
    current_liabilities         REAL,
    accounts_payable            REAL,
    current_debt                REAL,
    current_capital_lease       REAL,
    current_provisions          REAL,
    dividends_payable           REAL,
    other_current_liabilities   REAL,
    total_non_current_liab      REAL,
    long_term_debt              REAL,
    long_term_capital_lease     REAL,
    non_current_deferred_tax_l  REAL,
    non_current_deferred_rev    REAL,
    long_term_provisions        REAL,
    other_non_current_liab      REAL,
    -- Equity
    total_equity                REAL,
    stockholders_equity         REAL,
    common_stock                REAL,
    additional_paid_in_capital  REAL,
    retained_earnings           REAL,
    other_equity_interest       REAL,
    minority_interest           REAL,
    -- Summary metrics
    total_debt                  REAL,
    net_debt                    REAL,
    working_capital             REAL,
    invested_capital            REAL,
    tangible_book_value         REAL,
    capital_lease_obligations   REAL,
    shares_issued               REAL,
    is_interpolated             INTEGER DEFAULT 0,
    UNIQUE (symbol, period_end, period_type)
);
CREATE INDEX IF NOT EXISTS idx_bs_sym_period ON balance_sheet(symbol, period_type, period_end DESC);

CREATE TABLE IF NOT EXISTS cash_flow (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                      TEXT NOT NULL REFERENCES stocks(symbol),
    period_end                  DATE NOT NULL,
    period_type                 TEXT NOT NULL DEFAULT 'annual',
    -- Operating
    operating_cash_flow         REAL,
    net_income_ops              REAL,
    depreciation                REAL,
    change_in_working_capital   REAL,
    change_in_receivables       REAL,
    change_in_inventory         REAL,
    change_in_payables          REAL,
    change_in_other_assets      REAL,
    change_in_other_liab        REAL,
    other_non_cash_items        REAL,
    taxes_refund_paid           REAL,
    -- Investing
    investing_cash_flow         REAL,
    capex                       REAL,
    purchase_of_ppe             REAL,
    sale_of_ppe                 REAL,
    purchase_of_business        REAL,
    sale_of_business            REAL,
    purchase_of_investments     REAL,
    sale_of_investments         REAL,
    interest_received           REAL,
    dividends_received          REAL,
    other_investing             REAL,
    -- Financing
    financing_cash_flow         REAL,
    net_debt_issuance           REAL,
    long_term_debt_issuance     REAL,
    long_term_debt_payments     REAL,
    short_term_debt_net         REAL,
    dividends_paid              REAL,
    interest_paid               REAL,
    stock_issuance              REAL,
    other_financing             REAL,
    -- Summary
    free_cash_flow              REAL,
    beginning_cash              REAL,
    end_cash                    REAL,
    changes_in_cash             REAL,
    is_interpolated             INTEGER DEFAULT 0,
    UNIQUE (symbol, period_end, period_type)
);
CREATE INDEX IF NOT EXISTS idx_cf_sym_period ON cash_flow(symbol, period_type, period_end DESC);

-- Quarterly FCF derived from Q-IS + interpolated Q-BS (engine output)
CREATE TABLE IF NOT EXISTS quarterly_cashflow_derived (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL REFERENCES stocks(symbol),
    quarter_end         DATE NOT NULL,
    revenue             REAL,
    net_income          REAL,
    dna                 REAL,                  -- D&A used in approximation
    approx_op_cf        REAL,                  -- net_income + D&A
    approx_capex        REAL,
    approx_fcf          REAL,
    fcf_margin_pct      REAL,
    capex_source        TEXT,                  -- 'ΔPPE+D&A' | 'Ann×share' | 'Ann÷4'
    is_interpolated     INTEGER DEFAULT 0,
    UNIQUE (symbol, quarter_end)
);

-- ────────────────────────────────────────────────────────────
--  D. CORPORATE ACTIONS  (Dividends · Splits · Bonus · Rights)
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS corporate_actions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    action_date     DATE NOT NULL,
    action_type     TEXT NOT NULL,  -- 'dividend'|'split'|'bonus'|'rights'|'buyback'
    value           REAL,           -- dividend Rs/share; split ratio; bonus ratio
    notes           TEXT,
    UNIQUE (symbol, action_date, action_type)
);
CREATE INDEX IF NOT EXISTS idx_ca_sym ON corporate_actions(symbol, action_date DESC);

-- ────────────────────────────────────────────────────────────
--  E. TECHNICAL INDICATORS
-- ────────────────────────────────────────────────────────────

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
    UNIQUE (symbol, date)
);
CREATE INDEX IF NOT EXISTS idx_ti_sym_date ON technical_indicators(symbol, date DESC);

-- ────────────────────────────────────────────────────────────
--  G. SECTOR & MACRO DATA
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS market_indices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   DATE NOT NULL,
    index_name      TEXT NOT NULL,  -- 'Nifty 50'|'Sensex'|'Nifty Bank' etc.
    last_price      REAL,
    change_pct      REAL,
    direction       TEXT,           -- '^' | 'v'
    UNIQUE (snapshot_date, index_name)
);

CREATE TABLE IF NOT EXISTS forex_commodities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   DATE NOT NULL,
    instrument      TEXT NOT NULL,  -- 'USD/INR'|'Crude Oil WTI'|'Gold Futures'
    last_price      REAL,
    change_pct      REAL,
    UNIQUE (snapshot_date, instrument)
);

CREATE TABLE IF NOT EXISTS rbi_rates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    effective_date  DATE NOT NULL,
    repo_rate       REAL,           -- %
    reverse_repo    REAL,
    sdf_rate        REAL,
    msf_rate        REAL,
    bank_rate       REAL,
    crr             REAL,
    slr             REAL,
    is_cached       INTEGER DEFAULT 0,   -- 1 = fallback cached value
    source          TEXT,
    UNIQUE (effective_date)
);

CREATE TABLE IF NOT EXISTS macro_indicators (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   DATE NOT NULL,
    indicator_name  TEXT NOT NULL,  -- 'India CPI Inflation (%)'
    source          TEXT,           -- 'World Bank' | 'RBI' etc.
    value           REAL,
    unit            TEXT,
    year            INTEGER,        -- reference year for annual indicators
    notes           TEXT,
    UNIQUE (snapshot_date, indicator_name, year)
);

-- ────────────────────────────────────────────────────────────
--  H. OWNERSHIP DATA
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ownership (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                  TEXT NOT NULL REFERENCES stocks(symbol),
    snapshot_date           DATE NOT NULL,
    -- Scraped categories
    promoter_pct            REAL,
    fii_fpi_pct             REAL,
    dii_pct                 REAL,
    public_retail_pct       REAL,
    -- yfinance breakdown
    insiders_pct            REAL,
    institutions_pct        REAL,
    institutions_float_pct  REAL,
    institutions_count      INTEGER,
    -- Derived
    total_institutional_pct REAL,    -- fii + dii
    source                  TEXT,    -- 'Screener.in'|'NSE'|'yfinance'
    UNIQUE (symbol, snapshot_date)
);

-- ────────────────────────────────────────────────────────────
--  I. EARNINGS & QUARTERLY RESULTS
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS earnings_history (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL REFERENCES stocks(symbol),
    quarter_end         DATE NOT NULL,
    eps_actual          REAL,
    eps_estimate        REAL,
    eps_difference      REAL,
    surprise_pct        REAL,
    UNIQUE (symbol, quarter_end)
);

CREATE TABLE IF NOT EXISTS earnings_estimates (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL REFERENCES stocks(symbol),
    snapshot_date       DATE NOT NULL,
    period_code         TEXT NOT NULL,  -- '0q'|'+1q'|'0y'|'+1y'
    avg_eps             REAL,
    low_eps             REAL,
    high_eps            REAL,
    year_ago_eps        REAL,
    analyst_count       INTEGER,
    growth_pct          REAL,
    UNIQUE (symbol, snapshot_date, period_code)
);

CREATE TABLE IF NOT EXISTS eps_trend (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL REFERENCES stocks(symbol),
    snapshot_date       DATE NOT NULL,
    period_code         TEXT NOT NULL,
    current_est         REAL,
    seven_days_ago      REAL,
    thirty_days_ago     REAL,
    sixty_days_ago      REAL,
    ninety_days_ago     REAL,
    UNIQUE (symbol, snapshot_date, period_code)
);

CREATE TABLE IF NOT EXISTS eps_revisions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL REFERENCES stocks(symbol),
    snapshot_date       DATE NOT NULL,
    period_code         TEXT NOT NULL,
    up_last_7d          INTEGER,
    up_last_30d         INTEGER,
    down_last_30d       INTEGER,
    down_last_7d        INTEGER,
    UNIQUE (symbol, snapshot_date, period_code)
);

-- ────────────────────────────────────────────────────────────
--  J. GROWTH METRICS
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS growth_metrics (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                  TEXT NOT NULL REFERENCES stocks(symbol),
    as_of_date              DATE NOT NULL,
    revenue_cagr_3y         REAL,
    net_profit_cagr_3y      REAL,
    ebitda_cagr_3y          REAL,
    eps_cagr_3y             REAL,
    fcf_cagr_3y             REAL,
    -- YoY snapshots (JSON arrays: [{"year":"2025-03-31","value":X,"yoy_pct":Y}, ...])
    revenue_yoy_json        TEXT,
    net_income_yoy_json     TEXT,
    ebitda_yoy_json         TEXT,
    fcf_yoy_json            TEXT,
    -- Gross margin trend (JSON: [{"year":"...","gross_margin_pct":X}, ...])
    gross_margin_trend_json TEXT,
    UNIQUE (symbol, as_of_date)
);

-- ────────────────────────────────────────────────────────────
--  AUDIT / RUN LOG
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS run_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    run_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    script_version  TEXT,                -- 'v5'
    modules_ok      TEXT,                -- CSV: 'price,fundamentals,...'
    modules_warn    TEXT,                -- CSV: 'quarterly_cashflow,...'
    notes           TEXT
);