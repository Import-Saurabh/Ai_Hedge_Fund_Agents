from database.db import get_connection


def load_growth_metrics(data: dict, symbol: str):
    """Load growth CAGRs and YoY JSON trend arrays."""
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO growth_metrics (
            symbol, as_of_date,
            revenue_cagr_3y, net_profit_cagr_3y, ebitda_cagr_3y,
            eps_cagr_3y, fcf_cagr_3y,
            revenue_yoy_json, net_income_yoy_json,
            ebitda_yoy_json, fcf_yoy_json, gross_margin_trend_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        symbol, data.get("as_of_date"),
        data.get("revenue_cagr_3y"),
        data.get("net_profit_cagr_3y"),
        data.get("ebitda_cagr_3y"),
        data.get("eps_cagr_3y"),
        data.get("fcf_cagr_3y"),
        data.get("revenue_yoy_json"),
        data.get("net_income_yoy_json"),
        data.get("ebitda_yoy_json"),
        data.get("fcf_yoy_json"),
        data.get("gross_margin_trend_json"),
    ))
    conn.commit()
    conn.close()
    print(f"  ✅ growth_metrics: saved for {symbol}")
