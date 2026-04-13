from etl.extract.price import fetch_price
from etl.extract.statements import fetch_statements
from etl.extract.fundamentals import fetch_fundamentals

from etl.transform.normalizer import normalize_price

from etl.load.price_loader import load_price
from etl.load.stock_loader import insert_stock
from etl.load.fundamentals_loader import load_fundamentals
from etl.load.income_loader import load_income
from etl.load.balance_loader import load_balance
from etl.load.cashflow_loader import load_cashflow
from etl.load.technical_loader import load_technicals


def run_price_pipeline(symbol: str):

    print(f"\n🚀 Running pipeline for {symbol}")

    clean_symbol = symbol.replace(".NS", "")

    # ========================
    # 🔹 STEP 1: STOCK SEED
    # ========================
    insert_stock(clean_symbol, clean_symbol)

    # ========================
    # 🔹 PRICE PIPELINE
    # ========================
    raw_df = fetch_price(symbol)
    clean_df = normalize_price(raw_df, symbol)
    load_price(clean_df)

    print("✅ Price data loaded")

    # ========================
    # 🔹 TECHNICALS
    # ========================
    try:
        from etl.extract.technicals import compute_technicals

        tech_df = compute_technicals(raw_df.copy())

        # ensure date column exists
        if "Date" in raw_df.columns:
            tech_df["date"] = raw_df["Date"].dt.date
        elif "date" in raw_df.columns:
            tech_df["date"] = raw_df["date"]
        else:
            raise Exception("No date column found")
        load_technicals(tech_df, clean_symbol)

        print("✅ Technical indicators loaded")

    except Exception as e:
        print("⚠ Technicals skipped:", e)

    # ========================
    # 🔹 FUNDAMENTALS
    # ========================
    try:
        fund_data = fetch_fundamentals(symbol)
        load_fundamentals(clean_symbol, fund_data["info"])

        print("✅ Fundamentals loaded")

    except Exception as e:
        print("⚠ Fundamentals skipped:", e)

    # ========================
    # 🔹 FINANCIAL STATEMENTS (FIXED)
    # ========================
    print("📊 Fetching financial statements...")

    try:
        stmts = fetch_statements(symbol)

        # ✅ DIRECT LOAD (NO MELT)
        load_income(stmts.get("annual_income"), clean_symbol)
        load_balance(stmts.get("annual_bs"), clean_symbol)
        load_cashflow(stmts.get("annual_cf"), clean_symbol)

        print("✅ Income statement loaded")
        print("✅ Balance sheet loaded")
        print("✅ Cash flow loaded")

    except Exception as e:
        print("⚠ Financial statements skipped:", e)

    print("🎯 Pipeline completed successfully\n")