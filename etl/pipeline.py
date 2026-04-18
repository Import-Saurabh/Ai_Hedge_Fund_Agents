"""
╔══════════════════════════════════════════════════════════════╗
║  BUFFETT-GRADE ETL PIPELINE  v5.3                            ║
║  Fixes vs v5.2:                                              ║
║   • Deduplication: growth_metrics, ownership, fundamentals,  ║
║     rbi_rates, macro_indicators — only newest row kept       ║
║   • eps_revisions: BLOB → int (numpy int64 fix)              ║
║   • quarterly_cashflow: interpolated rows flagged/excluded   ║
║   • technical_indicators: NULLs after warmup period fixed    ║
║   • EV/forward_pe/earnings_growth_json populated             ║
║   • Table names normalised (cash_flow, income_statement)     ║
╚══════════════════════════════════════════════════════════════╝
"""

import sys
import os
import time
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database.init_db import init_db
from database.dedup   import run_all_dedup          # ← NEW

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

from etl.load.stock_loader              import insert_stock
from etl.load.price_loader              import load_price
from etl.load.technical_loader          import load_technicals
from etl.load.fundamentals_loader       import load_fundamentals
from etl.load.income_loader             import load_income
from etl.load.balance_loader            import load_balance
from etl.load.cashflow_loader           import load_cashflow
from etl.load.corporate_actions_loader  import load_corporate_actions
from etl.load.macro_loader              import (load_market_indices,
                                                load_forex_commodities,
                                                load_rbi_rates,
                                                load_macro_indicators)
from etl.load.ownership_loader          import load_ownership
from etl.load.earnings_loader           import (load_earnings_history,
                                                load_earnings_estimates,
                                                load_eps_trend,
                                                load_eps_revisions)
from etl.load.growth_loader             import load_growth_metrics
from etl.load.quarterly_cashflow_loader import load_quarterly_cashflow
from etl.load.run_log_loader            import log_run


def run_pipeline(symbol_yf: str = "ADANIPORTS.NS"):
    symbol_nse = symbol_yf.replace(".NS", "")
    today      = date.today().isoformat()
    ok_mods, warn_mods = [], []

    print(f"""
╔══════════════════════════════════════════════════════════╗
║  BUFFETT ETL PIPELINE  v5.3                              ║
║  Symbol : {symbol_nse:<10}  ({symbol_yf})
║  Date   : {today}
╚══════════════════════════════════════════════════════════╝""")

    # ── 0. Init DB ────────────────────────────────────────────
    init_db()

    # ── 1. Seed stock ─────────────────────────────────────────
    insert_stock(symbol_nse, symbol_nse)
    print(f"\n[1/10] Stock seeded: {symbol_nse}")

    # ── 2. Price data ─────────────────────────────────────────
    print(f"\n[2/10] Price data...")
    price_df = None
    try:
        price_df = fetch_price(symbol_yf, years=5)
        load_price(price_df, symbol_nse)
        ok_mods.append("price")
    except Exception as e:
        print(f"  ❌ price: {e}")
        warn_mods.append("price")

    # ── 3. Technical indicators ───────────────────────────────
    print(f"\n[3/10] Technical indicators...")
    try:
        if price_df is not None and not price_df.empty:
            tech_df = compute_technicals(price_df.copy())
            # Drop rows inside the warmup window (sma_200 not yet valid)
            tech_df = tech_df[tech_df["sma_200"].notna()].copy()
            load_technicals(tech_df, symbol_nse)
            ok_mods.append("technicals")
        else:
            raise Exception("no price data available")
    except Exception as e:
        print(f"  ❌ technicals: {e}")
        warn_mods.append("technicals")

    # ── 4. Fundamentals ───────────────────────────────────────
    print(f"\n[4/10] Fundamentals...")
    try:
        fund_data = fetch_fundamentals(symbol_yf)
        load_fundamentals(symbol_nse, fund_data)
        ok_mods.append("fundamentals")
    except Exception as e:
        print(f"  ❌ fundamentals: {e}")
        warn_mods.append("fundamentals")

    time.sleep(0.5)

    # ── 5. Financial statements ───────────────────────────────
    print(f"\n[5/10] Financial statements...")
    stmts = {}
    try:
        stmts = fetch_statements(symbol_yf)

        load_income(stmts.get("annual_income"),  symbol_nse, "annual")
        load_balance(stmts.get("annual_bs"),      symbol_nse, "annual", 0)
        load_cashflow(stmts.get("annual_cf"),     symbol_nse, "annual")

        q_inc = stmts.get("q_income")
        if q_inc is not None and not q_inc.empty:
            load_income(q_inc, symbol_nse, "quarterly")

        q_bs_ext = stmts.get("q_bs_extended")
        q_bs_raw = stmts.get("q_bs")

        q_bs_to_load = (q_bs_ext
                        if q_bs_ext is not None and not q_bs_ext.empty
                        else q_bs_raw)
        if q_bs_to_load is not None and not q_bs_to_load.empty:
            load_balance(q_bs_to_load, symbol_nse, "quarterly", 0)

        q_cf = stmts.get("q_cf")
        if q_cf is not None and not q_cf.empty:
            load_cashflow(q_cf, symbol_nse, "quarterly")

        ok_mods.append("statements")
    except Exception as e:
        print(f"  ❌ statements: {e}")
        warn_mods.append("statements")
        import traceback; traceback.print_exc()

    time.sleep(0.5)

    # ── 6. Corporate actions ──────────────────────────────────
    print(f"\n[6/10] Corporate actions...")
    try:
        ca_data = fetch_corporate_actions(symbol_yf)
        load_corporate_actions(ca_data, symbol_nse)
        ok_mods.append("corporate_actions")
    except Exception as e:
        print(f"  ❌ corporate_actions: {e}")
        warn_mods.append("corporate_actions")

    time.sleep(0.3)

    # ── 7. Macro & market data ────────────────────────────────
    print(f"\n[7/10] Macro & market data...")
    try:
        mkt = fetch_market_indices()
        load_market_indices(mkt, today)
        load_forex_commodities(mkt, today)

        rbi = fetch_rbi_rates()
        load_rbi_rates(rbi)

        macro_recs = fetch_macro_indicators()
        if macro_recs:
            load_macro_indicators(macro_recs)

        ok_mods.append("macro")
    except Exception as e:
        print(f"  ❌ macro: {e}")
        warn_mods.append("macro")

    time.sleep(0.3)

    # ── 8. Ownership ──────────────────────────────────────────
    print(f"\n[8/10] Ownership...")
    try:
        own_data = fetch_ownership(symbol_yf, symbol_nse)
        load_ownership(own_data, symbol_nse)
        ok_mods.append("ownership")
    except Exception as e:
        print(f"  ❌ ownership: {e}")
        warn_mods.append("ownership")

    time.sleep(0.3)

    # ── 9. Earnings ───────────────────────────────────────────
    print(f"\n[9/10] Earnings...")
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
        print(f"  ❌ earnings: {e}")
        warn_mods.append("earnings")

    time.sleep(0.3)

    # ── 10. Growth metrics + Quarterly FCF ───────────────────
    print(f"\n[10/10] Growth metrics & quarterly FCF...")
    try:
        growth = fetch_growth_metrics(symbol_yf)
        load_growth_metrics(growth, symbol_nse)
        ok_mods.append("growth_metrics")
    except Exception as e:
        print(f"  ❌ growth_metrics: {e}")
        warn_mods.append("growth_metrics")

    try:
        q_inc      = stmts.get("q_income")       if stmts else None
        q_bs_ext   = stmts.get("q_bs_extended")  if stmts else None
        q_bs_raw   = stmts.get("q_bs")           if stmts else None
        bs_audit_d = stmts.get("bs_audit", {})   if stmts else {}

        qcf_records = fetch_quarterly_cashflow(
            symbol_yf,
            q_inc=q_inc,
            q_bs_extended=q_bs_ext,
            bs_audit=bs_audit_d,
            q_bs_real=q_bs_raw,
        )
        # Only load real (non-interpolated) records to avoid fake training data
        real_records = [r for r in qcf_records if r.get("is_interpolated", 0) == 0]
        interp_count = len(qcf_records) - len(real_records)
        if interp_count:
            print(f"  ⚠  quarterly_cashflow: skipping {interp_count} interpolated rows")
        if real_records:
            load_quarterly_cashflow(real_records, symbol_nse)
        ok_mods.append("quarterly_cashflow")
    except Exception as e:
        print(f"  ❌ quarterly_cashflow: {e}")
        warn_mods.append("quarterly_cashflow")

    # ── Deduplication pass ────────────────────────────────────
    print(f"\n[DEDUP] Removing duplicate snapshot rows...")
    try:
        run_all_dedup()
        print("  ✅ deduplication complete")
    except Exception as e:
        print(f"  ⚠  dedup error: {e}")

    # ── Run log ───────────────────────────────────────────────
    log_run(symbol_nse, ok_mods, warn_mods)

    # ── Summary ───────────────────────────────────────────────
    print(f"""
╔══════════════════════════════════════════════════════════╗
║  PIPELINE COMPLETE  ({today})
╠══════════════════════════════════════════════════════════╣
║  ✅  {", ".join(ok_mods)}
║  ⚠   {", ".join(warn_mods) if warn_mods else "none"}
╚══════════════════════════════════════════════════════════╝""")


if __name__ == "__main__":
    sym = sys.argv[1] if len(sys.argv) > 1 else "ADANIPORTS.NS"
    run_pipeline(sym)