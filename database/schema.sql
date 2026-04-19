-- ============================================================
--  BUFFETT-GRADE STOCK INTELLIGENCE SYSTEM — SQLite Schema v2
--  All Screener.in data merged into existing tables.
--  No separate screener_* tables.
--  Monetary values: Rs. Crores throughout.
-- ============================================================

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ────────────────────────────────────────────────────────────
--  CORE REFERENCE
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS stocks (
    symbol          TEXT PRIMARY KEY,
    name            TEXT,
    exchange        TEXT,
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
    ts              TIMESTAMP NOT NULL,
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
--  B. FUNDAMENTALS
--  Screener adds: working_capital_days, source_screener flag
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fundamentals (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                  TEXT NOT NULL REFERENCES stocks(symbol),
    as_of_date              DATE NOT NULL,

    -- Profitability
    roe_pct                 REAL,
    roce_pct                REAL,       -- also from Screener Ratios sheet
    roa_pct                 REAL,
    interest_coverage       REAL,

    -- Cash flow
    free_cash_flow          REAL,
    operating_cf            REAL,
    capex                   REAL,

    -- Margins
    gross_margin_pct        REAL,
    net_profit_margin_pct   REAL,
    ebitda_margin_pct       REAL,
    ebit_margin_pct         REAL,
    opm_pct                 REAL,       -- ← Screener: Operating Profit Margin %

    -- Leverage & liquidity
    debt_to_equity          REAL,
    current_ratio           REAL,
    quick_ratio             REAL,

    -- Working capital efficiency
    dso_days                REAL,
    dio_days                REAL,
    dpo_days                REAL,
    cash_conversion_cycle   REAL,
    working_capital_days    REAL,       -- ← Screener: Working Capital Days

    -- Valuation
    eps_annual              REAL,
    pe_ratio                REAL,
    pb_ratio                REAL,
    graham_number           REAL,
    dividend_yield_pct      REAL,
    dividend_payout_pct     REAL,       -- ← Screener: Dividend Payout %
    forward_pe              REAL,

    -- Scale / Absolute Values (Rs. Crores)
    market_cap              REAL,
    revenue                 REAL,
    net_income              REAL,
    ebitda                  REAL,
    inventory               REAL,
    ev                      REAL,

    -- TTM
    ttm_eps                 REAL,
    ttm_pe                  REAL,
    ttm_sales               REAL,       -- ← Screener: TTM Sales (Rs. Crores)
    ttm_net_profit          REAL,       -- ← Screener: TTM Net Profit

    -- Enterprise Multiples
    ev_ebitda               REAL,
    ev_revenue              REAL,

    -- Growth & Metadata
    earnings_growth_json    TEXT,
    data_source             TEXT,       -- 'yfinance' | 'screener' | 'both'

    UNIQUE (symbol, as_of_date)
);

-- ────────────────────────────────────────────────────────────
--  C. FINANCIAL STATEMENTS
--  income_statement: Screener adds expenses, opm_pct,
--    other_income, profit_before_tax, dividend_payout_pct
--  balance_sheet: Screener uses a simplified structure —
--    equity_capital, reserves, borrowings, cwip added
--  cash_flow: Screener adds cash_from_financing,
--    net_cash_flow, cfo_op_pct columns
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS income_statement (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                      TEXT NOT NULL REFERENCES stocks(symbol),
    period_end                  DATE NOT NULL,
    period_type                 TEXT NOT NULL DEFAULT 'annual',  -- 'annual'|'quarterly'

    -- yfinance line items (Rs. Crores)
    total_revenue               REAL,
    cost_of_revenue             REAL,
    gross_profit                REAL,
    selling_general_admin       REAL,
    operating_expense           REAL,
    operating_income            REAL,
    ebit                        REAL,
    ebitda                      REAL,
    normalized_ebitda           REAL,
    depreciation_amortization   REAL,
    interest_expense            REAL,
    interest_income             REAL,
    net_interest_expense        REAL,
    pretax_income               REAL,
    tax_provision               REAL,
    net_income                  REAL,
    net_income_common           REAL,
    normalized_income           REAL,
    minority_interests          REAL,
    diluted_eps                 REAL,
    basic_eps                   REAL,
    diluted_shares              REAL,
    basic_shares                REAL,
    special_income_charges      REAL,
    total_unusual_items         REAL,
    tax_rate                    REAL,

    -- ← Screener columns (Rs. Crores unless noted)
    scr_sales                   REAL,   -- Screener "Sales" (= revenue)
    scr_expenses                REAL,   -- Screener "Expenses"
    scr_operating_profit        REAL,   -- Screener "Operating Profit"
    scr_opm_pct                 REAL,   -- OPM % (ratio, e.g. 59.0)
    scr_other_income            REAL,   -- Other Income
    scr_interest                REAL,   -- Interest expense
    scr_depreciation            REAL,   -- Depreciation
    scr_profit_before_tax       REAL,   -- Profit before tax
    scr_tax_pct                 REAL,   -- Tax % (ratio)
    scr_net_profit              REAL,   -- Net Profit
    scr_eps                     REAL,   -- EPS in Rs
    scr_dividend_payout_pct     REAL,   -- Dividend Payout % (annual only)

    is_interpolated             INTEGER DEFAULT 0,
    data_source                 TEXT DEFAULT 'yfinance',  -- 'yfinance'|'screener'|'both'

    UNIQUE (symbol, period_end, period_type)
);
CREATE INDEX IF NOT EXISTS idx_is_sym_period ON income_statement(symbol, period_type, period_end DESC);

CREATE TABLE IF NOT EXISTS balance_sheet (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                      TEXT NOT NULL REFERENCES stocks(symbol),
    period_end                  DATE NOT NULL,
    period_type                 TEXT NOT NULL DEFAULT 'annual',

    -- yfinance detailed line items (Rs. Crores)
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
    total_equity                REAL,
    stockholders_equity         REAL,
    common_stock                REAL,
    additional_paid_in_capital  REAL,
    retained_earnings           REAL,
    other_equity_interest       REAL,
    minority_interest           REAL,
    total_debt                  REAL,
    net_debt                    REAL,
    working_capital             REAL,
    invested_capital            REAL,
    tangible_book_value         REAL,
    capital_lease_obligations   REAL,
    shares_issued               REAL,

    -- ← Screener columns (Rs. Crores)
    scr_equity_capital          REAL,   -- Paid-up equity capital
    scr_reserves                REAL,   -- Reserves & surplus
    scr_borrowings              REAL,   -- Total borrowings
    scr_other_liabilities       REAL,   -- Other liabilities
    scr_total_liabilities       REAL,   -- Screener total liabilities
    scr_fixed_assets            REAL,   -- Net fixed assets
    scr_cwip                    REAL,   -- Capital Work In Progress
    scr_investments             REAL,   -- Investments
    scr_other_assets            REAL,   -- Other assets
    scr_total_assets            REAL,   -- Screener total assets

    is_interpolated             INTEGER DEFAULT 0,
    data_source                 TEXT DEFAULT 'yfinance',

    UNIQUE (symbol, period_end, period_type)
);
CREATE INDEX IF NOT EXISTS idx_bs_sym_period ON balance_sheet(symbol, period_type, period_end DESC);

CREATE TABLE IF NOT EXISTS cash_flow (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                      TEXT NOT NULL REFERENCES stocks(symbol),
    period_end                  DATE NOT NULL,
    period_type                 TEXT NOT NULL DEFAULT 'annual',

    -- yfinance detailed line items (Rs. Crores)
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
    financing_cash_flow         REAL,
    net_debt_issuance           REAL,
    long_term_debt_issuance     REAL,
    long_term_debt_payments     REAL,
    short_term_debt_net         REAL,
    dividends_paid              REAL,
    interest_paid               REAL,
    stock_issuance              REAL,
    other_financing             REAL,
    free_cash_flow              REAL,
    beginning_cash              REAL,
    end_cash                    REAL,
    changes_in_cash             REAL,

    -- ← Screener columns (Rs. Crores)
    scr_cash_from_operating     REAL,   -- Cash from Operating Activity
    scr_cash_from_investing     REAL,   -- Cash from Investing Activity
    scr_cash_from_financing     REAL,   -- Cash from Financing Activity
    scr_net_cash_flow           REAL,   -- Net Cash Flow
    scr_free_cash_flow          REAL,   -- Free Cash Flow (Screener)
    scr_cfo_op_pct              REAL,   -- CFO / Operating Profit %

    is_interpolated             INTEGER DEFAULT 0,
    data_source                 TEXT DEFAULT 'yfinance',

    UNIQUE (symbol, period_end, period_type)
);
CREATE INDEX IF NOT EXISTS idx_cf_sym_period ON cash_flow(symbol, period_type, period_end DESC);

-- Quarterly FCF derived
CREATE TABLE IF NOT EXISTS quarterly_cashflow_derived (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL REFERENCES stocks(symbol),
    quarter_end         DATE NOT NULL,
    revenue             REAL,
    net_income          REAL,
    dna                 REAL,
    approx_op_cf        REAL,
    approx_capex        REAL,
    approx_fcf          REAL,
    fcf_margin_pct      REAL,
    capex_source        TEXT,
    is_interpolated     INTEGER DEFAULT 0,
    UNIQUE (symbol, quarter_end)
);

-- ────────────────────────────────────────────────────────────
--  D. CORPORATE ACTIONS
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS corporate_actions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    action_date     DATE NOT NULL,
    action_type     TEXT NOT NULL,
    value           REAL,
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
    adx_14          REAL,
    vwap_14         REAL,
    obv             REAL,
    supertrend      REAL,
    supertrend_dir  INTEGER,
    UNIQUE (symbol, date)
);
CREATE INDEX IF NOT EXISTS idx_ti_sym_date ON technical_indicators(symbol, date DESC);

-- ────────────────────────────────────────────────────────────
--  G. MACRO DATA
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS market_indices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   DATE NOT NULL,
    index_name      TEXT NOT NULL,
    last_price      REAL,
    change_pct      REAL,
    direction       TEXT,
    UNIQUE (snapshot_date, index_name)
);

CREATE TABLE IF NOT EXISTS forex_commodities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   DATE NOT NULL,
    instrument      TEXT NOT NULL,
    last_price      REAL,
    change_pct      REAL,
    UNIQUE (snapshot_date, instrument)
);

CREATE TABLE IF NOT EXISTS rbi_rates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    effective_date  DATE NOT NULL,
    repo_rate       REAL,
    reverse_repo    REAL,
    sdf_rate        REAL,
    msf_rate        REAL,
    bank_rate       REAL,
    crr             REAL,
    slr             REAL,
    is_cached       INTEGER DEFAULT 0,
    source          TEXT,
    UNIQUE (effective_date)
);

CREATE TABLE IF NOT EXISTS macro_indicators (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   DATE NOT NULL,
    indicator_name  TEXT NOT NULL,
    source          TEXT,
    value           REAL,
    unit            TEXT,
    year            INTEGER,
    notes           TEXT,
    UNIQUE (snapshot_date, indicator_name, year)
);

-- ────────────────────────────────────────────────────────────
--  H. OWNERSHIP
--  Screener adds: num_shareholders, scr_promoter_pct history
--  (snapshot per quarter via screener; daily snapshot via yf)
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ownership (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                  TEXT NOT NULL REFERENCES stocks(symbol),
    snapshot_date           DATE NOT NULL,

    -- Screener quarterly pattern (most recent quarter)
    promoter_pct            REAL,
    fii_fpi_pct             REAL,
    dii_pct                 REAL,
    public_retail_pct       REAL,
    num_shareholders        INTEGER,        -- ← Screener: No. of Shareholders

    -- yfinance breakdown
    insiders_pct            REAL,
    institutions_pct        REAL,
    institutions_float_pct  REAL,
    institutions_count      INTEGER,

    -- Derived
    total_institutional_pct REAL,
    fii_net_buy_cr          REAL,           -- daily flow (nselib)
    dii_net_buy_cr          REAL,
    fii_dii_flow_date       TEXT,
    source                  TEXT,
    UNIQUE (symbol, snapshot_date)
);

-- Quarterly shareholding history (one row per quarter from Screener)
-- Stored inline in ownership for latest; full history here:
CREATE TABLE IF NOT EXISTS ownership_history (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                  TEXT NOT NULL REFERENCES stocks(symbol),
    period_end              DATE NOT NULL,  -- quarter-end
    promoter_pct            REAL,
    fii_pct                 REAL,
    dii_pct                 REAL,
    public_pct              REAL,
    total_institutional_pct REAL,
    num_shareholders        INTEGER,
    source                  TEXT DEFAULT 'Screener.in',
    UNIQUE (symbol, period_end)
);
CREATE INDEX IF NOT EXISTS idx_own_hist_sym ON ownership_history(symbol, period_end DESC);

-- ────────────────────────────────────────────────────────────
--  I. EARNINGS
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
    period_code         TEXT NOT NULL,
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
--  Screener growth sheet populates scr_* CAGR columns
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS growth_metrics (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                      TEXT NOT NULL REFERENCES stocks(symbol),
    as_of_date                  DATE NOT NULL,

    -- yfinance-derived CAGRs (%)
    revenue_cagr_3y             REAL,
    net_profit_cagr_3y          REAL,
    ebitda_cagr_3y              REAL,
    eps_cagr_3y                 REAL,
    fcf_cagr_3y                 REAL,

    -- YoY JSON arrays
    revenue_yoy_json            TEXT,
    net_income_yoy_json         TEXT,
    ebitda_yoy_json             TEXT,
    fcf_yoy_json                TEXT,
    gross_margin_trend_json     TEXT,

    -- ← Screener compounded growth (% CAGR, period in label)
    scr_sales_cagr_10y          REAL,   -- Sales Growth 10 Years
    scr_sales_cagr_5y           REAL,   -- Sales Growth 5 Years
    scr_sales_cagr_3y           REAL,   -- Sales Growth 3 Years
    scr_sales_ttm               REAL,   -- Sales Growth TTM
    scr_profit_cagr_10y         REAL,   -- Profit Growth 10 Years
    scr_profit_cagr_5y          REAL,
    scr_profit_cagr_3y          REAL,
    scr_profit_ttm              REAL,
    scr_stock_cagr_10y          REAL,   -- Stock Price CAGR 10 Years
    scr_stock_cagr_5y           REAL,
    scr_stock_cagr_3y           REAL,
    scr_stock_ttm               REAL,
    scr_roe_10y                 REAL,   -- ROE 10 Year average
    scr_roe_5y                  REAL,
    scr_roe_3y                  REAL,
    scr_roe_last                REAL,

    UNIQUE (symbol, as_of_date)
);

-- ────────────────────────────────────────────────────────────
--  AUDIT / RUN LOG
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS run_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    run_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    script_version  TEXT,
    modules_ok      TEXT,
    modules_warn    TEXT,
    notes           TEXT
);