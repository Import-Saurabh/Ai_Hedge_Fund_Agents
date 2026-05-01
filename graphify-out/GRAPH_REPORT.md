# Graph Report - Fund  (2026-05-01)

## Corpus Check
- 41 files · ~33,156 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 340 nodes · 607 edges · 34 communities detected
- Extraction: 87% EXTRACTED · 13% INFERRED · 0% AMBIGUOUS · INFERRED: 81 edges (avg confidence: 0.8)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]
- [[_COMMUNITY_Community 13|Community 13]]
- [[_COMMUNITY_Community 14|Community 14]]
- [[_COMMUNITY_Community 15|Community 15]]
- [[_COMMUNITY_Community 16|Community 16]]
- [[_COMMUNITY_Community 17|Community 17]]
- [[_COMMUNITY_Community 18|Community 18]]
- [[_COMMUNITY_Community 19|Community 19]]
- [[_COMMUNITY_Community 20|Community 20]]
- [[_COMMUNITY_Community 21|Community 21]]
- [[_COMMUNITY_Community 28|Community 28]]
- [[_COMMUNITY_Community 29|Community 29]]
- [[_COMMUNITY_Community 30|Community 30]]
- [[_COMMUNITY_Community 31|Community 31]]
- [[_COMMUNITY_Community 32|Community 32]]
- [[_COMMUNITY_Community 33|Community 33]]
- [[_COMMUNITY_Community 34|Community 34]]
- [[_COMMUNITY_Community 35|Community 35]]
- [[_COMMUNITY_Community 36|Community 36]]
- [[_COMMUNITY_Community 37|Community 37]]
- [[_COMMUNITY_Community 38|Community 38]]
- [[_COMMUNITY_Community 39|Community 39]]

## God Nodes (most connected - your core abstractions)
1. `run_pipeline()` - 40 edges
2. `get_connection()` - 36 edges
3. `warn()` - 15 edges
4. `ok()` - 14 edges
5. `section()` - 13 edges
6. `fetch_ownership()` - 13 edges
7. `compute_quarterly_cashflow()` - 13 edges
8. `main()` - 13 edges
9. `fetch_financial_statements()` - 12 edges
10. `load_all_screener()` - 12 edges

## Surprising Connections (you probably didn't know these)
- `compute_quarterly_cashflow()` --calls--> `_row()`  [INFERRED]
  test.py → etl\load\screener_loader.py
- `get_connection()` --calls--> `run_all_dedup()`  [INFERRED]
  database\db.py → database\dedup.py
- `get_connection()` --calls--> `init_db()`  [INFERRED]
  database\db.py → database\init_db.py
- `get_connection()` --calls--> `load_corporate_actions()`  [INFERRED]
  database\db.py → etl\load\corporate_actions_loader.py
- `get_connection()` --calls--> `load_earnings_history()`  [INFERRED]
  database\db.py → etl\load\earnings_loader.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.05
Nodes (39): init_db(), database/init_db.py  v4.0 Reads schema.sql from same directory and creates all, Safely return a DataFrame only if non-None and non-empty.     Prevents: ValueErr, run_pipeline(), _safe_df(), fetch_corporate_actions(), Fetch dividends, splits, and all corporate actions., fetch_earnings() (+31 more)

### Community 1 - "Community 1"
Cohesion: 0.14
Nodes (46): compute_fundamentals(), _compute_gross_margin_safe(), compute_growth_metrics(), compute_quarterly_cashflow(), compute_technicals(), fail(), fetch_corporate_actions(), fetch_earnings() (+38 more)

### Community 2 - "Community 2"
Cohesion: 0.11
Nodes (37): get_connection(), audit_table(), compute_completeness(), _is_null(), log_data_quality(), database/validator.py  v2.0 ───────────────────────────────────────────────────, Count rows and NULL rates for key fields. Print summary., validate_before_insert() (+29 more)

### Community 3 - "Community 3"
Cohesion: 0.19
Nodes (13): compute_technicals(), _find_col(), load_technicals(), _obv(), technical_loader.py  —  v2 ─────────────────────────────────────────────────────, Daily rolling VWAP = sum(typical_price * volume, window), Compute ALL technical indicators on a full OHLCV DataFrame.      Input columns e, Load all technical indicators (incl. new ADX/VWAP/OBV/Supertrend).     Skips war (+5 more)

### Community 4 - "Community 4"
Cohesion: 0.29
Nodes (11): dedup_all(), fix_eps_revisions_blobs(), fix_price_daily(), get_conn(), main(), _needs_rescale(), purge_interpolated_cashflow(), purge_technical_nulls() (+3 more)

### Community 5 - "Community 5"
Cohesion: 0.3
Nodes (11): _build_earnings_growth_json(), _compute_gross_margin_safe(), _cr(), fetch_fundamentals(), _get_row(), etl/extract/fundamentals.py  v4.0 ─────────────────────────────────────────────, Build {date: net_income_cr} JSON from annual IS. Newest → oldest., Compute all fundamentals metrics.     MONETARY VALUES → Rs. Crores     RATIOS (+3 more)

### Community 6 - "Community 6"
Cohesion: 0.24
Nodes (10): _compute_gross_margin_safe(), _cr(), fetch_growth_metrics(), etl/extract/growth.py  v3.0 ───────────────────────────────────────────────────, Compute growth CAGRs + YoY trends.     All monetary JSON values in Rs. Crores., Extract a metric row as {date_str: crore_value} dict., Build [{year, value_cr, yoy_pct}, ...] JSON. Newest → oldest., _safe_float() (+2 more)

### Community 7 - "Community 7"
Cohesion: 0.24
Nodes (11): _clean_num(), _clean_num_part(), fetch_screener_data(), _get_html(), _parse_overview(), _parse_table(), etl/extract/screener.py  v3.1 ─────────────────────────────────────────────────, Robust number extractor for Screener values.     Handles:       "₹ 1,612"  → (+3 more)

### Community 8 - "Community 8"
Cohesion: 0.42
Nodes (11): _completeness(), _div(), _f(), _pct(), reconcile_balance_sheet(), reconcile_cash_flow(), reconcile_fundamentals(), reconcile_growth_metrics() (+3 more)

### Community 9 - "Community 9"
Cohesion: 0.36
Nodes (10): _add_earnings_growth_json(), _add_ev_inputs(), _add_forward_pe(), _apply_all_patches(), _bs_first(), _cr(), etl/extract/fundamentals_extract_patch.py  v3.0 ───────────────────────────────, JSON of annual net-income in Rs. Crores, newest→oldest.     Example: {"2025-03- (+2 more)

### Community 10 - "Community 10"
Cohesion: 0.29
Nodes (10): _compute_completeness(), _data_changed(), _get_today_row(), load_fundamentals(), load_fundamentals_from_screener(), _pct(), etl/load/fundamentals_loader.py  v5.0 ─────────────────────────────────────────, Merge Screener Ratios + latest quarterly opm_pct + annual dividend_payout_pct (+2 more)

### Community 11 - "Community 11"
Cohesion: 0.18
Nodes (7): load_income(), load_income_from_screener(), _pct_str(), etl/load/income_loader.py  v3.0 ───────────────────────────────────────────────, Load Screener P&L data into scr_* columns of income_statement.     Creates row, 59%' → 59.0, also handles plain floats., Load yfinance income statement rows (detailed line items).

### Community 12 - "Community 12"
Cohesion: 0.29
Nodes (9): _fetch_fii_dii_flow(), fetch_ownership(), _fetch_screener_fallback(), _fetch_yf_holders(), _from_screener_df(), etl/extract/ownership.py  v4.0 ─────────────────────────────────────────────────, Extract latest-quarter shareholding from a Screener DataFrame., Fetch shareholding pattern + FII/DII trading flow.      Priority for promoter/FI (+1 more)

### Community 13 - "Community 13"
Cohesion: 0.31
Nodes (9): _cr(), fetch_quarterly_cashflow(), etl/extract/quarterly_cashflow.py  v4.1 ────────────────────────────────────────, Exact label match first, then substring — avoids wrong sub-rows., Original substring-first row finder (kept for CF paths)., Returns real quarterly cashflow records only.     quality_score: 3=direct_qcf, 2, _row(), _row_exact() (+1 more)

### Community 14 - "Community 14"
Cohesion: 0.33
Nodes (8): _completeness(), _data_changed(), _drop_json_columns(), _latest_row(), load_growth_metrics(), etl/load/growth_loader.py  v5.0 ───────────────────────────────────────────────, One-time migration: remove JSON blob columns from growth_metrics.     SQLite do, Upsert yfinance-derived growth CAGRs (no JSON blobs).

### Community 15 - "Community 15"
Cohesion: 0.46
Nodes (7): classify_doc(), download_pdf(), extract_documents(), extract_year(), fetch_page(), main(), safe_name()

### Community 16 - "Community 16"
Cohesion: 0.43
Nodes (6): fetch_statements(), _get_row_series(), _interpolate_qbs_from_annual(), Fetch all financial statements (annual + quarterly) with Q-BS interpolation., FIX-A: Interpolate missing Q-BS periods from annual BS., _safe_float()

### Community 17 - "Community 17"
Cohesion: 0.33
Nodes (6): load_ownership(), load_ownership_history(), _parse_period(), etl/load/ownership_loader.py  v2.0 ─────────────────────────────────────────────, Upsert today's ownership snapshot., Load full quarterly shareholding history from Screener DataFrame     into owners

### Community 18 - "Community 18"
Cohesion: 0.47
Nodes (5): _dedup_table(), database/dedup.py  v2.0 ───────────────────────────────────────────────────────, Run deduplication across all configured tables., run_all_dedup(), run_one_time_cleanup()

### Community 19 - "Community 19"
Cohesion: 0.4
Nodes (5): compute_technicals(), etl/extract/technicals.py  v2.0 ────────────────────────────────────────────────, Wilder's smoothing (used by RSI, ATR, ADX)., Compute all technical indicators.      Input df columns required: date, close, h, _wilder_smooth()

### Community 20 - "Community 20"
Cohesion: 0.47
Nodes (5): load_price(), etl/load/price_loader.py  v2.0 ────────────────────────────────────────────────, Load daily OHLCV + adj_close into price_daily.     Uses INSERT OR IGNORE to saf, _safe_float(), _safe_int()

### Community 21 - "Community 21"
Cohesion: 0.5
Nodes (1): etl/load/cashflow_loader.py  v4.0 ─────────────────────────────────────────────

### Community 28 - "Community 28"
Cohesion: 1.0
Nodes (1): Look up a balance sheet row using SCREENER_BS_LABEL_MAP patterns.

### Community 29 - "Community 29"
Cohesion: 1.0
Nodes (1): Add any missing columns to balance_sheet (idempotent).

### Community 30 - "Community 30"
Cohesion: 1.0
Nodes (1): Recompute and write completeness_pct + missing_fields_json for a BS row.

### Community 31 - "Community 31"
Cohesion: 1.0
Nodes (1): Writes Screener overview ratios into fundamentals.     Computes: graham_number,

### Community 32 - "Community 32"
Cohesion: 1.0
Nodes (1): Loads Screener balance sheet into the fully normalized balance_sheet table.

### Community 33 - "Community 33"
Cohesion: 1.0
Nodes (1): Dispatcher. Load order matters:       1. quarterly_results  (overview loader ne

### Community 34 - "Community 34"
Cohesion: 1.0
Nodes (1): # NOTE: receivables_over_6m / receivables_under_6m are intentionally

### Community 35 - "Community 35"
Cohesion: 1.0
Nodes (1): Add any missing columns to balance_sheet (idempotent).

### Community 36 - "Community 36"
Cohesion: 1.0
Nodes (1): Recompute and write completeness_pct + missing_fields_json for a BS row.

### Community 37 - "Community 37"
Cohesion: 1.0
Nodes (1): Writes Screener overview ratios into fundamentals.     Computes: graham_number,

### Community 38 - "Community 38"
Cohesion: 1.0
Nodes (1): Loads Screener balance sheet into the fully normalized balance_sheet table.

### Community 39 - "Community 39"
Cohesion: 1.0
Nodes (1): Dispatcher. Load order matters:       1. quarterly_results  (overview loader ne

## Knowledge Gaps
- **110 isolated node(s):** `╔══════════════════════════════════════════════════════════════╗ ║   BUFFETT-GR`, `FIX-C: Search df.index for candidates with priority:       1. Exact case-insens`, `Return full row series for first matching candidate (strict-first).`, `Return all rows whose index contains `pattern` (case-insensitive).`, `FIX-C: Dual-method gross margin with cross-validation.      Method 1: Gross Pr` (+105 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 21`** (4 nodes): `cashflow_loader.py`, `_col_val()`, `_cr()`, `etl/load/cashflow_loader.py  v4.0 ─────────────────────────────────────────────`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 28`** (1 nodes): `Look up a balance sheet row using SCREENER_BS_LABEL_MAP patterns.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 29`** (1 nodes): `Add any missing columns to balance_sheet (idempotent).`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 30`** (1 nodes): `Recompute and write completeness_pct + missing_fields_json for a BS row.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 31`** (1 nodes): `Writes Screener overview ratios into fundamentals.     Computes: graham_number,`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 32`** (1 nodes): `Loads Screener balance sheet into the fully normalized balance_sheet table.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 33`** (1 nodes): `Dispatcher. Load order matters:       1. quarterly_results  (overview loader ne`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 34`** (1 nodes): `# NOTE: receivables_over_6m / receivables_under_6m are intentionally`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 35`** (1 nodes): `Add any missing columns to balance_sheet (idempotent).`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 36`** (1 nodes): `Recompute and write completeness_pct + missing_fields_json for a BS row.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 37`** (1 nodes): `Writes Screener overview ratios into fundamentals.     Computes: graham_number,`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 38`** (1 nodes): `Loads Screener balance sheet into the fully normalized balance_sheet table.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 39`** (1 nodes): `Dispatcher. Load order matters:       1. quarterly_results  (overview loader ne`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `run_pipeline()` connect `Community 0` to `Community 2`, `Community 3`, `Community 5`, `Community 6`, `Community 7`, `Community 8`, `Community 10`, `Community 11`, `Community 12`, `Community 13`, `Community 14`, `Community 16`, `Community 17`, `Community 18`, `Community 20`?**
  _High betweenness centrality (0.471) - this node is a cross-community bridge._
- **Why does `get_connection()` connect `Community 2` to `Community 0`, `Community 3`, `Community 8`, `Community 10`, `Community 11`, `Community 14`, `Community 17`, `Community 18`, `Community 20`?**
  _High betweenness centrality (0.195) - this node is a cross-community bridge._
- **Why does `_row()` connect `Community 2` to `Community 1`?**
  _High betweenness centrality (0.172) - this node is a cross-community bridge._
- **Are the 38 inferred relationships involving `run_pipeline()` (e.g. with `init_db()` and `insert_stock()`) actually correct?**
  _`run_pipeline()` has 38 INFERRED edges - model-reasoned connections that need verification._
- **Are the 35 inferred relationships involving `get_connection()` (e.g. with `run_all_dedup()` and `init_db()`) actually correct?**
  _`get_connection()` has 35 INFERRED edges - model-reasoned connections that need verification._
- **What connects `╔══════════════════════════════════════════════════════════════╗ ║   BUFFETT-GR`, `FIX-C: Search df.index for candidates with priority:       1. Exact case-insens`, `Return full row series for first matching candidate (strict-first).` to the rest of the system?**
  _110 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Community 0` be split into smaller, more focused modules?**
  _Cohesion score 0.05 - nodes in this community are weakly interconnected._