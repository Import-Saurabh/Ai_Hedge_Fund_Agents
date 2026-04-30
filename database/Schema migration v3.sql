-- schema_migration_v3.sql
-- Run once against your existing database to align schema with pipeline v5.x
-- Safe to run multiple times (all statements guarded).

-- ── 1. growth_metrics: drop JSON blob columns ─────────────────
-- SQLite ≥ 3.35 supports DROP COLUMN directly.
-- For older SQLite the growth_loader._drop_json_columns() helper
-- does the table-rebuild automatically on first pipeline run.
-- Only run these manually if you have SQLite ≥ 3.35:

-- ALTER TABLE growth_metrics DROP COLUMN revenue_yoy_json;
-- ALTER TABLE growth_metrics DROP COLUMN net_income_yoy_json;
-- ALTER TABLE growth_metrics DROP COLUMN ebitda_yoy_json;
-- ALTER TABLE growth_metrics DROP COLUMN fcf_yoy_json;
-- ALTER TABLE growth_metrics DROP COLUMN gross_margin_trend_json;

-- If you're on SQLite < 3.35, just re-run the pipeline once;
-- growth_loader._drop_json_columns() will rebuild the table safely.


-- ── 2. growth_metrics: add stock CAGR columns if missing ──────

-- (These may already exist — SQLite will error on duplicate; ignore those errors)


-- ── 3. balance_sheet: verify completeness is recomputed ───────
-- After running the new pipeline once, all yfinance annual rows
-- (2022–2025) should show completeness_pct = 100 and
-- missing_fields_json = [] because screener_loader now calls
-- _bs_completeness() after each upsert.

-- You can verify with:
-- SELECT period_end, data_source, completeness_pct, missing_fields_json
-- FROM balance_sheet WHERE symbol='ADANIPORTS' ORDER BY period_end;


-- ── 4. quarterly_cashflow_derived: no schema change needed ────
-- quality_score, dna, approx_op_cf columns already exist.
-- reconcile.py v3.0 will fill dna from quarterly_results.depreciation
-- and income_statement.scr_depreciation after next pipeline run.


-- ── 5. cash_flow: verify scr_* columns present ────────────────
-- These were added in earlier versions. Verify:
-- PRAGMA table_info(cash_flow);
-- Expected columns: scr_cash_from_operating, scr_cash_from_investing,
--   scr_cash_from_financing, scr_net_cash_flow, scr_free_cash_flow,
--   scr_cfo_op_pct, best_operating_cf, best_investing_cf,
--   best_financing_cf, best_free_cash_flow


-- ── Summary of what changes after re-running pipeline ─────────
-- balance_sheet:     completeness_pct 60→100 for 2022–2025 annual rows
--                    missing_fields_json clears out scr_* columns
-- growth_metrics:    JSON blob columns removed; scr_stock_cagr_* filled
--                    from price_daily; scr_sales/profit_cagr_* computed
--                    from annual_results when Screener section missing
-- quarterly_cashflow_derived: dna + approx_op_cf filled from
--                    quarterly_results.depreciation via reconcile
-- cash_flow:         pre-2022 annual rows now have scr_* populated
--                    and best_* resolved to Screener values