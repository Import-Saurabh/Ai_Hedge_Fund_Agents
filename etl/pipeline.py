"""
BUFFETT-GRADE ETL PIPELINE  v5.5
Changes vs v5.4:
  - quarterly_results and annual_results are now primary P&L tables
    (replaced by Screener — not yfinance income_statement quarterly)
  - Pre/post-insert validation via database.validator
  - Post-load audit step logs completeness to data_quality_log
  - cash_flow: best_* columns resolved (fix historical NULL problem)
  - quarterly_cashflow_derived: quality_score + is_real enforced
  - growth_metrics: scr_* NULLs diagnosed and flagged
  - Source priority enforced: Screener > yfinance for Indian stocks
"""

import sys
import os
import time
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.init_db  import init_db
from database.dedup    import run_all_dedup
from database.validator import audit_table

from etl.extract.price              import fetch_price
from etl.extract.fundamentals       import fetch_fundamentals
from etl.extract.statements         import fetch_statements
from etl.extract.technicals         import compute_technicals
from etl.extract.corporate_actions  import fetch_corporate_actions
from etl.extract.macro              import fetch_market_indices, fetch_rbi_rates, fetch_macro_indicators
from etl.extract.ownership          import fetch_ownership
from etl.extract.earnings           import fetch_earnings
from etl.extract.growth             import fetch_growth_metrics
from etl.extract.quarterly_cashflow import fetch_quarterly_cashflow
from etl.extract.screener           import fetch_screener_data

from etl.load.stock_loader              import insert_stock
from etl.load.price_loader              import load_price
from etl.load.technical_loader          import load_technicals
from etl.load.fundamentals_loader       import load_fundamentals
from etl.load.income_loader             import load_income
from etl.load.balance_loader            import load_balance
from etl.load.cashflow_loader           import load_cashflow
from etl.load.corporate_actions_loader  import load_corporate_actions
from etl.load.macro_loader              import (load_market_indices, load_forex_commodities,
                                                load_rbi_rates, load_macro_indicators)
from etl.load.ownership_loader          import load_ownership
from etl.load.earnings_loader           import (load_earnings_history, load_earnings_estimates,
                                                load_eps_trend, load_eps_revisions)
from etl.load.growth_loader             import load_growth_metrics
from etl.load.quarterly_cashflow_loader import load_quarterly_cashflow
from etl.load.run_log_loader            import log_run
from etl.load.screener_loader           import load_all_screener


def run_pipeline(symbol_yf: str = "ADANIPORTS.NS"):
    symbol_nse = symbol_yf.replace(".NS", "")
    today      = date.today().isoformat()
    ok_mods, warn_mods = [], []

    print(f"\n{'='*60}")
    print(f"  BUFFETT ETL PIPELINE  v5.5")
    print(f"  Symbol : {symbol_nse}  ({symbol_yf})")
    print(f"  Date   : {today}")
    print(f"{'='*60}\n")

    # ── 0. Init DB ─────────────────────────────────────────────
    init_db()

    # ── 1. Seed stock ──────────────────────────────────────────
    insert_stock(symbol_nse, symbol_nse)
    print(f"[1/11] Stock seeded: {symbol_nse}")

    # ── 2. Price ───────────────────────────────────────────────
    print(f"\n[2/11] Price data...")
    price_df = None
    try:
        price_df = fetch_price(symbol_yf, years=5)
        load_price(price_df, symbol_nse)
        ok_mods.append("price")
    except Exception as e:
        print(f"  error price: {e}"); warn_mods.append("price")

    # ── 3. Technicals ──────────────────────────────────────────
    print(f"\n[3/11] Technical indicators...")
    try:
        if price_df is not None and not price_df.empty:
            tech_df = compute_technicals(price_df.copy())
            tech_df = tech_df[tech_df["sma_200"].notna()].copy()
            load_technicals(tech_df, symbol_nse)
            ok_mods.append("technicals")
        else:
            raise Exception("no price data")
    except Exception as e:
        print(f"  error technicals: {e}"); warn_mods.append("technicals")

    # ── 4. Fundamentals (yfinance) ─────────────────────────────
    print(f"\n[4/11] Fundamentals (yfinance ratios/valuation)...")
    try:
        fund_data = fetch_fundamentals(symbol_yf)
        load_fundamentals(symbol_nse, fund_data)
        ok_mods.append("fundamentals_yf")
    except Exception as e:
        print(f"  error fundamentals_yf: {e}"); warn_mods.append("fundamentals_yf")

    time.sleep(0.5)

    # ── 5. yfinance statements (detailed line items) ───────────
    print(f"\n[5/11] Financial statements (yfinance detailed)...")
    stmts = {}
    try:
        stmts = fetch_statements(symbol_yf)
        load_income(stmts.get("annual_income"),  symbol_nse, "annual")
        load_balance(stmts.get("annual_bs"),     symbol_nse, "annual", 0)
        load_cashflow(stmts.get("annual_cf"),    symbol_nse, "annual")

        q_inc = stmts.get("q_income")
        if q_inc is not None and not q_inc.empty:
            load_income(q_inc, symbol_nse, "quarterly")

        q_bs = stmts.get("q_bs_extended") or stmts.get("q_bs")
        if q_bs is not None and not q_bs.empty:
            load_balance(q_bs, symbol_nse, "quarterly", 0)

        q_cf = stmts.get("q_cf")
        if q_cf is not None and not q_cf.empty:
            load_cashflow(q_cf, symbol_nse, "quarterly")

        ok_mods.append("statements_yf")
    except Exception as e:
        print(f"  error statements_yf: {e}"); warn_mods.append("statements_yf")
        import traceback; traceback.print_exc()

    time.sleep(0.5)

    # ── 6. Screener.in (PRIMARY source for Indian P&L data) ────
    print(f"\n[6/11] Screener.in (primary financial data source)...")
    screener_data = {}
    try:
        screener_data = fetch_screener_data(symbol_nse)
        if not screener_data:
            raise Exception("empty response from Screener.in")

        load_all_screener(screener_data, symbol_nse)

        # Post-load audit
        for tbl in ["quarterly_results", "annual_results",
                    "balance_sheet", "cash_flow", "growth_metrics"]:
            audit_table(symbol_nse, tbl)

        ok_mods.append("screener")
    except Exception as e:
        print(f"  error screener: {e}"); warn_mods.append("screener")
        import traceback; traceback.print_exc()

    time.sleep(0.3)

    # ── 7. Corporate actions ───────────────────────────────────
    print(f"\n[7/11] Corporate actions...")
    try:
        ca_data = fetch_corporate_actions(symbol_yf)
        load_corporate_actions(ca_data, symbol_nse)
        ok_mods.append("corporate_actions")
    except Exception as e:
        print(f"  error corporate_actions: {e}"); warn_mods.append("corporate_actions")

    time.sleep(0.3)

    # ── 8. Macro ───────────────────────────────────────────────
    print(f"\n[8/11] Macro & market data...")
    try:
        mkt = fetch_market_indices()
        load_market_indices(mkt, today)
        load_forex_commodities(mkt, today)
        load_rbi_rates(fetch_rbi_rates())
        macro_recs = fetch_macro_indicators()
        if macro_recs:
            load_macro_indicators(macro_recs)
        ok_mods.append("macro")
    except Exception as e:
        print(f"  error macro: {e}"); warn_mods.append("macro")

    time.sleep(0.3)

    # ── 9. Ownership ───────────────────────────────────────────
    print(f"\n[9/11] Ownership...")
    try:
        screener_sh = screener_data.get("shareholding") if screener_data else None
        own_data = fetch_ownership(symbol_yf, symbol_nse,
                                   screener_shareholding_df=screener_sh)
        load_ownership(own_data, symbol_nse)
        audit_table(symbol_nse, "ownership_history")
        ok_mods.append("ownership")
    except Exception as e:
        print(f"  error ownership: {e}"); warn_mods.append("ownership")

    time.sleep(0.3)

    # ── 10. Earnings ───────────────────────────────────────────
    print(f"\n[10/11] Earnings...")
    try:
        earn = fetch_earnings(symbol_yf)
        if earn.get("earnings_history"):
            load_earnings_history(earn["earnings_history"], symbol_nse)
        if earn.get("earnings_estimates"):
            load_earnings_estimates(earn["earnings_estimates"], symbol_nse)
        if earn.get("eps_trend"):
            load_eps_trend(earn["eps_trend"], symbol_nse)
        if earn.get("eps_revisions"):
            load_eps_revisions(earn["eps_revisions"], symbol_nse)
        ok_mods.append("earnings")
    except Exception as e:
        print(f"  error earnings: {e}"); warn_mods.append("earnings")

    time.sleep(0.3)

    # ── 11. Growth + Quarterly FCF ─────────────────────────────
    print(f"\n[11/11] Growth + quarterly FCF...")
    try:
        growth = fetch_growth_metrics(symbol_yf)
        load_growth_metrics(growth, symbol_nse)
        ok_mods.append("growth_metrics_yf")
    except Exception as e:
        print(f"  error growth_metrics_yf: {e}"); warn_mods.append("growth_metrics_yf")

    try:
        q_inc    = stmts.get("q_income")      if stmts else None
        q_bs_ext = stmts.get("q_bs_extended") if stmts else None
        q_bs_raw = stmts.get("q_bs")          if stmts else None

        qcf_records = fetch_quarterly_cashflow(
            symbol_yf,
            q_inc=q_inc,
            q_bs_extended=q_bs_ext,
            bs_audit=stmts.get("bs_audit", {}),
            q_bs_real=q_bs_raw,
        )
        # Loader enforces quality_score >= 2; is_interpolated=0 always
        load_quarterly_cashflow(qcf_records, symbol_nse)
        audit_table(symbol_nse, "quarterly_cashflow_derived")
        ok_mods.append("quarterly_cashflow")
    except Exception as e:
        print(f"  error quarterly_cashflow: {e}"); warn_mods.append("quarterly_cashflow")

    # Final audit on fundamentals and growth
    audit_table(symbol_nse, "fundamentals")
    audit_table(symbol_nse, "growth_metrics")

    # ── Dedup ──────────────────────────────────────────────────
    print(f"\n[DEDUP]...")
    try:
        run_all_dedup()
        print("  dedup complete")
    except Exception as e:
        print(f"  dedup error: {e}")

    log_run(symbol_nse, ok_mods, warn_mods)

    print(f"\n{'='*60}")
    print(f"  PIPELINE COMPLETE  {today}")
    print(f"  OK   : {', '.join(ok_mods)}")
    print(f"  WARN : {', '.join(warn_mods) or 'none'}")
    print(f"{'='*60}")


if __name__ == "__main__":
    sym = sys.argv[1] if len(sys.argv) > 1 else "ADANIPORTS.NS"
    run_pipeline(sym)