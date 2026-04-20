-- ============================================================
--  BUFFETT-GRADE STOCK INTELLIGENCE SYSTEM — SQLite Schema v3
--  Key changes vs v2:
--   • quarterly_results table added (Screener quarterly P&L)
--   • data_completeness_pct + missing_fields JSON on every
--     statement table — enforced at insert time
--   • source_priority columns: scr_* = Screener (authoritative
--     for Indian stocks), yf_* prefix for yfinance alternatives
--   • quarterly_cashflow_derived: is_real flag, quality_score
--   • cash_flow: historical_only flag for screener-only rows
--   • All scr_* values are authoritative (Screener > yfinance
--     for Indian listed companies)
-- ============================================================

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- ────────────────────────────────────────────────────────────
--  CORE REFERENCE
-- ────────────────────────────────────────────────────────────

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

-- ────────────────────────────────────────────────────────────
--  A. PRICE
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
CREATE INDEX IF NOT EXISTS idx_price_sym_date ON price_daily(symbol, date DESC);

CREATE TABLE IF NOT EXISTS price_intraday (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    ts              TIMESTAMP NOT NULL,
    interval        TEXT NOT NULL DEFAULT '1m',
    open            REAL, high REAL, low REAL, close REAL, volume INTEGER,
    UNIQUE (symbol, ts, interval)
);
CREATE INDEX IF NOT EXISTS idx_price_intra_sym ON price_intraday(symbol, ts DESC);

-- ────────────────────────────────────────────────────────────
--  B. FUNDAMENTALS  (point-in-time snapshot)
--  Source priority: Screener ratios → yfinance fallback
--  opm_pct, working_capital_days, dividend_payout_pct → Screener
--  ttm_sales, ttm_net_profit → Screener profit_loss TTM col
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fundamentals (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                  TEXT NOT NULL REFERENCES stocks(symbol),
    as_of_date              DATE NOT NULL,

    -- Profitability
    roe_pct                 REAL,
    roce_pct                REAL,
    roa_pct                 REAL,
    interest_coverage       REAL,

    -- Cash flow (Rs. Crores)
    free_cash_flow          REAL,
    operating_cf            REAL,
    capex                   REAL,

    -- Margins
    gross_margin_pct        REAL,
    net_profit_margin_pct   REAL,
    ebitda_margin_pct       REAL,
    ebit_margin_pct         REAL,
    opm_pct                 REAL,           -- from Screener: OPM % (latest Q)

    -- Leverage & liquidity
    debt_to_equity          REAL,
    current_ratio           REAL,
    quick_ratio             REAL,

    -- Working capital efficiency
    dso_days                REAL,
    dio_days                REAL,
    dpo_days                REAL,
    cash_conversion_cycle   REAL,
    working_capital_days    REAL,           -- from Screener Ratios

    -- Valuation
    eps_annual              REAL,
    pe_ratio                REAL,
    pb_ratio                REAL,
    graham_number           REAL,
    dividend_yield_pct      REAL,
    dividend_payout_pct     REAL,           -- from Screener P&L (latest annual)
    forward_pe              REAL,

    -- Scale (Rs. Crores)
    market_cap              REAL,
    revenue                 REAL,
    net_income              REAL,
    ebitda                  REAL,
    inventory               REAL,
    ev                      REAL,
    ttm_eps                 REAL,
    ttm_pe                  REAL,
    ttm_sales               REAL,           -- from Screener P&L TTM column
    ttm_net_profit          REAL,           -- from Screener P&L TTM column

    -- Enterprise multiples
    ev_ebitda               REAL,
    ev_revenue              REAL,

    -- Growth
    earnings_growth_json    TEXT,

    -- Data quality
    data_source             TEXT DEFAULT 'yfinance',   -- 'yfinance'|'screener'|'both'
    completeness_pct        REAL,           -- % of non-NULL key fields

    UNIQUE (symbol, as_of_date)
);

-- ────────────────────────────────────────────────────────────
--  C1. QUARTERLY RESULTS  (Screener "Quarters" sheet)
--  This is the authoritative quarterly P&L for Indian stocks.
--  Source: Screener.in  Unit: Rs. Crores  No yfinance fallback.
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS quarterly_results (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL REFERENCES stocks(symbol),
    period_end          DATE NOT NULL,      -- e.g. 2025-03-31

    -- P&L (Rs. Crores)
    sales               REAL NOT NULL,      -- Revenue
    expenses            REAL,
    operating_profit    REAL,
    opm_pct             REAL,               -- Operating margin %
    other_income        REAL,
    interest            REAL,
    depreciation        REAL,
    profit_before_tax   REAL,
    tax_pct             REAL,
    net_profit          REAL NOT NULL,      -- PAT
    eps                 REAL,               -- Rs per share

    -- Data quality
    source              TEXT DEFAULT 'Screener.in',
    is_audited          INTEGER DEFAULT 0,  -- 0=unaudited Q, 1=audited annual
    completeness_pct    REAL,

    UNIQUE (symbol, period_end)
);
CREATE INDEX IF NOT EXISTS idx_qr_sym ON quarterly_results(symbol, period_end DESC);

-- ────────────────────────────────────────────────────────────
--  C2. ANNUAL P&L  (Screener "Profit_Loss" sheet)
--  Authoritative annual income statement for Indian stocks.
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS annual_results (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                  TEXT NOT NULL REFERENCES stocks(symbol),
    period_end              DATE NOT NULL,      -- Mar YYYY → YYYY-03-31

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

    UNIQUE (symbol, period_end)
);
CREATE INDEX IF NOT EXISTS idx_ar_sym ON annual_results(symbol, period_end DESC);

-- ────────────────────────────────────────────────────────────
--  C3. INCOME STATEMENT  (yfinance detailed line items)
--  Detailed breakdown — yfinance primary, Screener summary in
--  scr_* columns for cross-validation only.
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS income_statement (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                      TEXT NOT NULL REFERENCES stocks(symbol),
    period_end                  DATE NOT NULL,
    period_type                 TEXT NOT NULL DEFAULT 'annual',

    -- yfinance detail (Rs. Crores)
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

    -- Screener cross-validation (Rs. Crores — these are AUTHORITATIVE)
    scr_sales                   REAL,
    scr_expenses                REAL,
    scr_operating_profit        REAL,
    scr_opm_pct                 REAL,
    scr_other_income            REAL,
    scr_interest                REAL,
    scr_depreciation            REAL,
    scr_profit_before_tax       REAL,
    scr_tax_pct                 REAL,
    scr_net_profit              REAL,
    scr_eps                     REAL,
    scr_dividend_payout_pct     REAL,       -- annual only

    -- Data quality
    is_interpolated             INTEGER DEFAULT 0,
    data_source                 TEXT DEFAULT 'yfinance',
    completeness_pct            REAL,       -- % of non-NULL yfinance fields
    missing_fields_json         TEXT,       -- JSON array of NULL field names

    UNIQUE (symbol, period_end, period_type)
);
CREATE INDEX IF NOT EXISTS idx_is_sym ON income_statement(symbol, period_type, period_end DESC);

-- ────────────────────────────────────────────────────────────
--  C4. BALANCE SHEET
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS balance_sheet (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                      TEXT NOT NULL REFERENCES stocks(symbol),
    period_end                  DATE NOT NULL,
    period_type                 TEXT NOT NULL DEFAULT 'annual',

    -- yfinance detail (Rs. Crores)
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

    -- Screener summary (Rs. Crores — AUTHORITATIVE for Indian stocks)
    scr_equity_capital          REAL,
    scr_reserves                REAL,
    scr_borrowings              REAL,
    scr_other_liabilities       REAL,
    scr_total_liabilities       REAL,
    scr_fixed_assets            REAL,
    scr_cwip                    REAL,
    scr_investments             REAL,
    scr_other_assets            REAL,
    scr_total_assets            REAL,

    -- Data quality
    is_interpolated             INTEGER DEFAULT 0,
    data_source                 TEXT DEFAULT 'yfinance',
    completeness_pct            REAL,
    missing_fields_json         TEXT,

    UNIQUE (symbol, period_end, period_type)
);
CREATE INDEX IF NOT EXISTS idx_bs_sym ON balance_sheet(symbol, period_type, period_end DESC);

-- ────────────────────────────────────────────────────────────
--  C5. CASH FLOW
--  Note: historical rows (pre-2022) often have NULL yfinance
--  data. scr_* columns fill the gap for annual periods.
--  Use scr_cash_from_operating for trend analysis.
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS cash_flow (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                      TEXT NOT NULL REFERENCES stocks(symbol),
    period_end                  DATE NOT NULL,
    period_type                 TEXT NOT NULL DEFAULT 'annual',

    -- yfinance detail (Rs. Crores) — may be NULL for historical periods
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

    -- Screener summary (Rs. Crores — AUTHORITATIVE, covers full history)
    scr_cash_from_operating     REAL,
    scr_cash_from_investing     REAL,
    scr_cash_from_financing     REAL,
    scr_net_cash_flow           REAL,
    scr_free_cash_flow          REAL,
    scr_cfo_op_pct              REAL,       -- CFO / Operating Profit %

    -- Resolved best-available values (filled by loader from scr_* if yf NULL)
    best_operating_cf           REAL,       -- scr_cash_from_operating ?? operating_cash_flow
    best_investing_cf           REAL,
    best_financing_cf           REAL,
    best_free_cash_flow         REAL,       -- scr_free_cash_flow ?? free_cash_flow

    -- Data quality
    is_interpolated             INTEGER DEFAULT 0,
    data_source                 TEXT DEFAULT 'yfinance',
    has_yf_detail               INTEGER DEFAULT 0,  -- 1 if yfinance cols populated
    completeness_pct            REAL,

    UNIQUE (symbol, period_end, period_type)
);
CREATE INDEX IF NOT EXISTS idx_cf_sym ON cash_flow(symbol, period_type, period_end DESC);

-- ────────────────────────────────────────────────────────────
--  C6. QUARTERLY CASHFLOW DERIVED
--  Only real data — no fabrication.
--  quality_score: 3=direct_qcf, 2=NI+DA_approx, 1=estimated
--  Do NOT use rows with quality_score < 2 for modelling.
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS quarterly_cashflow_derived (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol              TEXT NOT NULL REFERENCES stocks(symbol),
    quarter_end         DATE NOT NULL,

    -- Source data (Rs. Crores)
    revenue             REAL,
    net_income          REAL,
    dna                 REAL,           -- D&A (must not be 0 — set NULL if unavailable)
    approx_op_cf        REAL,           -- NULL if not derivable
    approx_capex        REAL,           -- NULL if not available
    approx_fcf          REAL,           -- NULL if not derivable
    fcf_margin_pct      REAL,

    -- Quality metadata
    capex_source        TEXT,           -- 'direct_qcf' | 'NI+DA_approx' | NULL
    quality_score       INTEGER DEFAULT 1,  -- 3=direct, 2=NI+DA, 1=estimated
    is_real             INTEGER DEFAULT 0,  -- 1=from real reported data
    is_interpolated     INTEGER DEFAULT 0,  -- 1=fabricated — DO NOT MODEL
    data_note           TEXT,           -- human-readable quality note
    unit                TEXT DEFAULT 'Rs_Crores',

    UNIQUE (symbol, quarter_end)
);
CREATE INDEX IF NOT EXISTS idx_qcd_sym ON quarterly_cashflow_derived(symbol, quarter_end DESC);

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
CREATE INDEX IF NOT EXISTS idx_ti_sym ON technical_indicators(symbol, date DESC);

-- ────────────────────────────────────────────────────────────
--  F. MACRO
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS market_indices (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   DATE NOT NULL,
    index_name      TEXT NOT NULL,
    last_price      REAL, change_pct REAL, direction TEXT,
    UNIQUE (snapshot_date, index_name)
);

CREATE TABLE IF NOT EXISTS forex_commodities (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   DATE NOT NULL,
    instrument      TEXT NOT NULL,
    last_price      REAL, change_pct REAL,
    UNIQUE (snapshot_date, instrument)
);

CREATE TABLE IF NOT EXISTS rbi_rates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    effective_date  DATE NOT NULL,
    repo_rate       REAL, reverse_repo REAL, sdf_rate REAL,
    msf_rate        REAL, bank_rate REAL, crr REAL, slr REAL,
    is_cached       INTEGER DEFAULT 0, source TEXT,
    UNIQUE (effective_date)
);

CREATE TABLE IF NOT EXISTS macro_indicators (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   DATE NOT NULL,
    indicator_name  TEXT NOT NULL,
    source TEXT, value REAL, unit TEXT, year INTEGER, notes TEXT,
    UNIQUE (snapshot_date, indicator_name, year)
);

-- ────────────────────────────────────────────────────────────
--  G. OWNERSHIP
-- ────────────────────────────────────────────────────────────

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

-- Full quarterly history (Screener authoritative)
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
CREATE INDEX IF NOT EXISTS idx_own_hist ON ownership_history(symbol, period_end DESC);

-- ────────────────────────────────────────────────────────────
--  H. EARNINGS
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS earnings_history (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    quarter_end     DATE NOT NULL,
    eps_actual      REAL, eps_estimate REAL,
    eps_difference  REAL, surprise_pct REAL,
    UNIQUE (symbol, quarter_end)
);

CREATE TABLE IF NOT EXISTS earnings_estimates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    snapshot_date   DATE NOT NULL,
    period_code     TEXT NOT NULL,
    avg_eps REAL, low_eps REAL, high_eps REAL,
    year_ago_eps REAL, analyst_count INTEGER, growth_pct REAL,
    UNIQUE (symbol, snapshot_date, period_code)
);

CREATE TABLE IF NOT EXISTS eps_trend (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    snapshot_date   DATE NOT NULL,
    period_code     TEXT NOT NULL,
    current_est REAL, seven_days_ago REAL, thirty_days_ago REAL,
    sixty_days_ago REAL, ninety_days_ago REAL,
    UNIQUE (symbol, snapshot_date, period_code)
);

CREATE TABLE IF NOT EXISTS eps_revisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    snapshot_date   DATE NOT NULL,
    period_code     TEXT NOT NULL,
    up_last_7d INTEGER, up_last_30d INTEGER,
    down_last_30d INTEGER, down_last_7d INTEGER,
    UNIQUE (symbol, snapshot_date, period_code)
);

-- ────────────────────────────────────────────────────────────
--  I. GROWTH METRICS
--  scr_* CAGRs come from Screener growth-numbers section.
--  yf_* CAGRs computed from yfinance income stmt.
--  All % values.
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS growth_metrics (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol                      TEXT NOT NULL REFERENCES stocks(symbol),
    as_of_date                  DATE NOT NULL,

    -- yfinance-derived CAGRs (computed from IS)
    revenue_cagr_3y             REAL,
    net_profit_cagr_3y          REAL,
    ebitda_cagr_3y              REAL,
    eps_cagr_3y                 REAL,
    fcf_cagr_3y                 REAL,

    -- YoY detail JSON (value_cr + yoy_pct per year)
    revenue_yoy_json            TEXT,
    net_income_yoy_json         TEXT,
    ebitda_yoy_json             TEXT,
    fcf_yoy_json                TEXT,
    gross_margin_trend_json     TEXT,

    -- Screener compounded growth (authoritative for Indian stocks)
    -- period: 10y / 5y / 3y / ttm
    scr_sales_cagr_10y          REAL,
    scr_sales_cagr_5y           REAL,
    scr_sales_cagr_3y           REAL,
    scr_sales_ttm               REAL,
    scr_profit_cagr_10y         REAL,
    scr_profit_cagr_5y          REAL,
    scr_profit_cagr_3y          REAL,
    scr_profit_ttm              REAL,
    scr_stock_cagr_10y          REAL,
    scr_stock_cagr_5y           REAL,
    scr_stock_cagr_3y           REAL,
    scr_stock_ttm               REAL,
    scr_roe_10y                 REAL,
    scr_roe_5y                  REAL,
    scr_roe_3y                  REAL,
    scr_roe_last                REAL,

    -- Data quality: NULL if Screener growth section unavailable
    scr_growth_available        INTEGER DEFAULT 0,  -- 1 if scr_* populated
    completeness_pct            REAL,

    UNIQUE (symbol, as_of_date)
);

-- ────────────────────────────────────────────────────────────
--  J. DATA QUALITY LOG
--  One row per table per run — tracks completeness over time.
-- ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS data_quality_log (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    run_timestamp       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    symbol              TEXT NOT NULL,
    table_name          TEXT NOT NULL,
    rows_inserted       INTEGER DEFAULT 0,
    rows_null_heavy     INTEGER DEFAULT 0,  -- rows with completeness < 50%
    avg_completeness    REAL,
    critical_nulls_json TEXT,               -- JSON: {field: null_count}
    source              TEXT,
    notes               TEXT
);
CREATE INDEX IF NOT EXISTS idx_dql_sym ON data_quality_log(symbol, run_timestamp DESC);

-- ────────────────────────────────────────────────────────────
--  K. RUN LOG
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