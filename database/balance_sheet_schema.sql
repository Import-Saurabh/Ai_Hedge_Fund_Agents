-- ────────────────────────────────────────────────────────────────────────────
--  BALANCE SHEET  —  fully normalized, Screener-only
--  All monetary values in Rs. Crores (Screener native).
--  No yfinance columns. No duplicate canonical/scr_ pairs.
--  The scr_ prefix is dropped — these ARE the canonical columns now.
-- ────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS balance_sheet;

CREATE TABLE IF NOT EXISTS balance_sheet (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL REFERENCES stocks(symbol),
    period_end      DATE NOT NULL,
    period_type     TEXT NOT NULL DEFAULT 'annual',   -- 'annual' | 'half_year'

    -- ── LIABILITIES SIDE ─────────────────────────────────────────────────────

    -- Equity block
    equity_capital          REAL,   -- "Equity Capital"   (paid-up share capital)
    reserves                REAL,   -- "Reserves"         (retained + other reserves)
    -- derived:
    total_equity            REAL,   -- equity_capital + reserves  (computed on insert)

    -- Borrowings block
    borrowings              REAL,   -- "Borrowings" bold total
    lt_borrowings           REAL,   -- "Long term Borrowings"
    st_borrowings           REAL,   -- "Short term Borrowings"
    lease_liabilities       REAL,   -- "Lease Liabilities"
    preference_capital      REAL,   -- "Preference Capital"
    other_borrowings        REAL,   -- "Other Borrowings"

    -- Other Liabilities block
    other_liabilities       REAL,   -- "Other Liabilities" bold total
    minority_interest       REAL,   -- "Non controlling int"
    trade_payables          REAL,   -- "Trade Payables"
    advance_from_customers  REAL,   -- "Advance from Customers"
    other_liability_items   REAL,   -- "Other liability items"

    -- Balance check
    total_liabilities       REAL,   -- "Total Liabilities"  (Screener grand total)

    -- ── ASSETS SIDE ──────────────────────────────────────────────────────────

    -- Fixed assets (Screener shows net block as bold single number;
    --   internal sub-rows are inconsistent across companies — not stored)
    fixed_assets            REAL,   -- "Fixed Assets"  (net block)
    cwip                    REAL,   -- "CWIP"          (capital work in progress)
    investments             REAL,   -- "Investments"

    -- Other Assets block
    other_assets            REAL,   -- "Other Assets" bold total
    inventories             REAL,   -- "Inventories"
    trade_receivables       REAL,   -- "Trade receivables" gross (top of sub-block)
    receivables_over_6m     REAL,   -- "Receivables over 6m"
    receivables_under_6m    REAL,   -- "Receivables under 6m"
    prov_doubtful_debts     REAL,   -- "Prov for Doubtful"  (stored as negative)
    cash_equivalents        REAL,   -- "Cash Equivalents"
    loans_advances          REAL,   -- "Loans n Advances"
    other_asset_items       REAL,   -- "Other asset items"

    -- Balance check
    total_assets            REAL,   -- "Total Assets"  (Screener grand total)

    -- ── DERIVED / QUALITY ────────────────────────────────────────────────────
    net_debt                REAL,   -- borrowings - cash_equivalents
    data_source             TEXT DEFAULT 'screener',
    completeness_pct        REAL,
    missing_fields_json     TEXT,

    UNIQUE (symbol, period_end, period_type)
);

CREATE INDEX IF NOT EXISTS idx_bs_sym
    ON balance_sheet(symbol, period_type, period_end DESC);


-- ────────────────────────────────────────────────────────────────────────────
--  MIGRATION — run once on existing DB (all ADD COLUMN are idempotent)
--  If you are creating fresh, skip this block.
-- ────────────────────────────────────────────────────────────────────────────

-- Rename old scr_* columns that now have canonical names
-- SQLite does not support RENAME COLUMN before 3.25; use the block below
-- only if you are on SQLite >= 3.25 (Python 3.8+ ships 3.31+).

ALTER TABLE balance_sheet RENAME COLUMN scr_equity_capital        TO equity_capital;
ALTER TABLE balance_sheet RENAME COLUMN scr_reserves              TO reserves;
ALTER TABLE balance_sheet RENAME COLUMN scr_borrowings            TO borrowings;
ALTER TABLE balance_sheet RENAME COLUMN scr_other_liabilities     TO other_liabilities;
ALTER TABLE balance_sheet RENAME COLUMN scr_total_liabilities     TO total_liabilities;
ALTER TABLE balance_sheet RENAME COLUMN scr_fixed_assets          TO fixed_assets;
ALTER TABLE balance_sheet RENAME COLUMN scr_cwip                  TO cwip;
ALTER TABLE balance_sheet RENAME COLUMN scr_investments           TO investments;
ALTER TABLE balance_sheet RENAME COLUMN scr_other_assets          TO other_assets;
ALTER TABLE balance_sheet RENAME COLUMN scr_total_assets          TO total_assets;

-- Drop the now-redundant yfinance canonical columns (data gone, schema clean)
-- SQLite does not support DROP COLUMN before 3.35; if older, rebuild the table.
ALTER TABLE balance_sheet DROP COLUMN total_non_current_assets;
ALTER TABLE balance_sheet DROP COLUMN net_ppe;
ALTER TABLE balance_sheet DROP COLUMN gross_ppe;
ALTER TABLE balance_sheet DROP COLUMN accumulated_depreciation;
ALTER TABLE balance_sheet DROP COLUMN land_improvements;
ALTER TABLE balance_sheet DROP COLUMN buildings_improvements;
ALTER TABLE balance_sheet DROP COLUMN machinery_equipment;
ALTER TABLE balance_sheet DROP COLUMN construction_in_progress;
ALTER TABLE balance_sheet DROP COLUMN goodwill;
ALTER TABLE balance_sheet DROP COLUMN other_intangibles;
ALTER TABLE balance_sheet DROP COLUMN long_term_equity_investment;
ALTER TABLE balance_sheet DROP COLUMN investment_in_fin_assets;
ALTER TABLE balance_sheet DROP COLUMN investment_properties;
ALTER TABLE balance_sheet DROP COLUMN non_current_deferred_tax_a;
ALTER TABLE balance_sheet DROP COLUMN other_non_current_assets;
ALTER TABLE balance_sheet DROP COLUMN current_liabilities;
ALTER TABLE balance_sheet DROP COLUMN accounts_payable;
ALTER TABLE balance_sheet DROP COLUMN current_debt;
ALTER TABLE balance_sheet DROP COLUMN current_capital_lease;
ALTER TABLE balance_sheet DROP COLUMN current_provisions;
ALTER TABLE balance_sheet DROP COLUMN dividends_payable;
ALTER TABLE balance_sheet DROP COLUMN other_current_liabilities;
ALTER TABLE balance_sheet DROP COLUMN total_non_current_liab;
ALTER TABLE balance_sheet DROP COLUMN long_term_debt;
ALTER TABLE balance_sheet DROP COLUMN long_term_capital_lease;
ALTER TABLE balance_sheet DROP COLUMN non_current_deferred_tax_l;
ALTER TABLE balance_sheet DROP COLUMN non_current_deferred_rev;
ALTER TABLE balance_sheet DROP COLUMN long_term_provisions;
ALTER TABLE balance_sheet DROP COLUMN other_non_current_liab;
ALTER TABLE balance_sheet DROP COLUMN stockholders_equity;
ALTER TABLE balance_sheet DROP COLUMN common_stock;
ALTER TABLE balance_sheet DROP COLUMN additional_paid_in_capital;
ALTER TABLE balance_sheet DROP COLUMN retained_earnings;
ALTER TABLE balance_sheet DROP COLUMN other_equity_interest;
ALTER TABLE balance_sheet DROP COLUMN minority_interest;  -- renamed above
ALTER TABLE balance_sheet DROP COLUMN total_debt;         -- = borrowings now
ALTER TABLE balance_sheet DROP COLUMN working_capital;
ALTER TABLE balance_sheet DROP COLUMN invested_capital;
ALTER TABLE balance_sheet DROP COLUMN tangible_book_value;
ALTER TABLE balance_sheet DROP COLUMN capital_lease_obligations;
ALTER TABLE balance_sheet DROP COLUMN shares_issued;
ALTER TABLE balance_sheet DROP COLUMN current_assets;
ALTER TABLE balance_sheet DROP COLUMN cash_and_equivalents;
ALTER TABLE balance_sheet DROP COLUMN cash_equivalents;   -- renamed above
ALTER TABLE balance_sheet DROP COLUMN short_term_investments;
ALTER TABLE balance_sheet DROP COLUMN accounts_receivable;
ALTER TABLE balance_sheet DROP COLUMN allowance_doubtful;
ALTER TABLE balance_sheet DROP COLUMN inventory;
ALTER TABLE balance_sheet DROP COLUMN prepaid_assets;
ALTER TABLE balance_sheet DROP COLUMN restricted_cash;
ALTER TABLE balance_sheet DROP COLUMN other_current_assets;
ALTER TABLE balance_sheet DROP COLUMN is_interpolated;

-- Add new sub-breakdown columns (safe to run multiple times — will error on
-- duplicate but that just means they already exist; wrap in try/except in Python)
ALTER TABLE balance_sheet ADD COLUMN lt_borrowings           REAL;
ALTER TABLE balance_sheet ADD COLUMN st_borrowings           REAL;
ALTER TABLE balance_sheet ADD COLUMN lease_liabilities       REAL;
ALTER TABLE balance_sheet ADD COLUMN preference_capital      REAL;
ALTER TABLE balance_sheet ADD COLUMN other_borrowings        REAL;
ALTER TABLE balance_sheet ADD COLUMN minority_interest       REAL;
ALTER TABLE balance_sheet ADD COLUMN trade_payables          REAL;
ALTER TABLE balance_sheet ADD COLUMN advance_from_customers  REAL;
ALTER TABLE balance_sheet ADD COLUMN other_liability_items   REAL;
ALTER TABLE balance_sheet ADD COLUMN inventories             REAL;
ALTER TABLE balance_sheet ADD COLUMN trade_receivables       REAL;
ALTER TABLE balance_sheet ADD COLUMN receivables_over_6m     REAL;
ALTER TABLE balance_sheet ADD COLUMN receivables_under_6m    REAL;
ALTER TABLE balance_sheet ADD COLUMN prov_doubtful_debts     REAL;
ALTER TABLE balance_sheet ADD COLUMN loans_advances          REAL;
ALTER TABLE balance_sheet ADD COLUMN other_asset_items       REAL;
ALTER TABLE balance_sheet ADD COLUMN net_debt                REAL;