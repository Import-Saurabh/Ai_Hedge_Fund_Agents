"""
╔══════════════════════════════════════════════════════════════╗
║  BUFFETT-GRADE ETL PIPELINE  v5                              ║
║  Orchestrates all extract → load modules.                    ║
║  Every table in the schema is populated.                     ║
║  No news. No duplicates. Price updates every run.            ║
╚══════════════════════════════════════════════════════════════╝
"""

import sys
import os
import time
from datetime import date

# ── ensure project root is on path ───────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── DB init ───────────────────────────────────────────────────
from database.init_db import init_db
from database.db import get_connection

# ── Extract modules ───────────────────────────────────────────
from etl.extract.price            import fetch_price
from etl.extract.fundamentals     import fetch_fundamentals
from etl.extract.statements       import fetch_statements
from etl.extract.technicals       import compute_technicals
from etl.extract.corporate_actions import fetch_corporate_actions
from etl.extract.macro            import fetch_market_indices, fetch_rbi_rates, fetch_macro_indicators
from etl.extract.ownership        import fetch_ownership
from etl.extract.earnings         import fetch_earnings
from etl.extract.growth           import fetch_growth_metrics
from etl.extract.quarterly_cashflow import fetch_quarterly_cashflow

# ── Load modules ──────────────────────────────────────────────
from etl.load.stock_loader             import insert_stock
from etl.load.price_loader             import load_price
from etl.load.technical_loader         import load_technicals
from etl.load.fundamentals_loader      import load_fundamentals
from etl.load.income_loader            import load_income
from etl.load.balance_loader           import load_balance
from etl.load.cashflow_loader          import load_cashflow
from etl.load.corporate_actions_loader import load_corporate_actions
from etl.load.macro_loader             import (load_market_indices,
                                               load_forex_commodities,
                                               load_rbi_rates,
                                               load_macro_indicators)
from etl.load.ownership_loader         import load_ownership
from etl.load.earnings_loader          import (load_earnings_history,
                                               load_earnings_estimates,
                                               load_eps_trend,
                                               load_eps_revisions)
from etl.load.growth_loader            import load_growth_metrics
from etl.load.quarterly_cashflow_loader import load_quarterly_cashflow
from etl.load.run_log_loader           import log_run


def run_pipeline(symbol_yf: str = "ADANIPORTS.NS"):
    symbol_nse = symbol_yf.replace(".NS", "")
    today      = date.today().isoformat()
    ok_mods, warn_mods = [], []

    print(f"""
╔══════════════════════════════════════════════════════════╗
║  BUFFETT ETL PIPELINE  v5                                ║
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
    try:
        price_df = fetch_price(symbol_yf, years=5)
        load_price(price_df, symbol_nse)
        ok_mods.append("price")
    except Exception as e:
        print(f"  ❌ price: {e}")
        warn_mods.append("price")
        price_df = None

    # ── 3. Technical indicators ───────────────────────────────
    print(f"\n[3/10] Technical indicators...")
    try:
        if price_df is not None:
            tech_df = compute_technicals(price_df.copy())
            load_technicals(tech_df, symbol_nse)
            ok_mods.append("technicals")
        else:
            raise Exception("no price data")
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
        fund_data = {}

    time.sleep(0.5)

    # ── 5. Financial statements ───────────────────────────────
    print(f"\n[5/10] Financial statements...")
    stmts = {}
    try:
        stmts = fetch_statements(symbol_yf)

        # Annual
        load_income(stmts.get("annual_income"), symbol_nse, "annual")
        load_balance(stmts.get("annual_bs"),    symbol_nse, "annual", 0)
        load_cashflow(stmts.get("annual_cf"),   symbol_nse, "annual")

        # Quarterly income
        if stmts.get("q_income") is not None and not stmts["q_income"].empty:
            load_income(stmts["q_income"], symbol_nse, "quarterly")

        # Quarterly balance sheet — use extended (interpolated) version
        q_bs = stmts.get("q_bs_extended") or stmts.get("q_bs")
        bs_audit = stmts.get("bs_audit", {})
        if q_bs is not None and not q_bs.empty:
            # Tag interpolated periods
            for col in q_bs.columns:
                is_interp = 1 if str(col)[:10] in bs_audit else 0
            load_balance(q_bs, symbol_nse, "quarterly", 0)

        # Quarterly cash flow (direct if available)
        if stmts.get("q_cf") is not None and not stmts["q_cf"].empty:
            load_cashflow(stmts["q_cf"], symbol_nse, "quarterly")

        ok_mods.append("statements")
    except Exception as e:
        print(f"  ❌ statements: {e}")
        warn_mods.append("statements")

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

    # ── 8. Ownership / shareholding ───────────────────────────
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
        q_inc       = stmts.get("q_income") if stmts else None
        q_bs_ext    = stmts.get("q_bs_extended") if stmts else None
        bs_audit_d  = stmts.get("bs_audit", {}) if stmts else {}
        qcf_records = fetch_quarterly_cashflow(
            symbol_yf, q_inc, q_bs_ext, bs_audit_d
        )
        if qcf_records:
            load_quarterly_cashflow(qcf_records, symbol_nse)
        ok_mods.append("quarterly_cashflow")
    except Exception as e:
        print(f"  ❌ quarterly_cashflow: {e}")
        warn_mods.append("quarterly_cashflow")

    # ── Run log ───────────────────────────────────────────────
    log_run(symbol_nse, ok_mods, warn_mods)

    # ── Summary ───────────────────────────────────────────────
    print(f"""
╔══════════════════════════════════════════════════════════╗
║  PIPELINE COMPLETE
╠══════════════════════════════════════════════════════════╣
║  ✅  {", ".join(ok_mods)}
║  ⚠   {", ".join(warn_mods) if warn_mods else "none"}
╚══════════════════════════════════════════════════════════╝""")


if __name__ == "__main__":
    sym = sys.argv[1] if len(sys.argv) > 1 else "ADANIPORTS.NS"
    run_pipeline(sym)
