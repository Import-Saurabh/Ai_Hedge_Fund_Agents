from datetime import date
from database.db import get_connection


def load_fundamentals(symbol: str, data: dict):
    """
    Load all computed fundamentals into the fundamentals table.
    Uses INSERT OR REPLACE on (symbol, as_of_date) — no duplicates per run day.
    """
    conn = get_connection()
    today = date.today().isoformat()

    conn.execute("""
        INSERT OR REPLACE INTO fundamentals (
            symbol, as_of_date,
            roe_pct, roce_pct, roa_pct, interest_coverage,
            free_cash_flow, operating_cf, capex,
            gross_margin_pct, net_profit_margin_pct, ebitda_margin_pct, ebit_margin_pct,
            debt_to_equity, current_ratio, quick_ratio,
            dso_days, dio_days, dpo_days, cash_conversion_cycle,
            eps_annual, pe_ratio, pb_ratio, graham_number, dividend_yield_pct,
            market_cap, revenue, net_income, ebitda, inventory,
            ttm_eps, ttm_pe
        ) VALUES (
            ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?
        )
    """, (
        symbol, today,
        data.get("ROE (%)"),
        data.get("ROCE (%)"),
        data.get("ROA (%)"),
        data.get("Interest Coverage"),
        data.get("Free Cash Flow"),
        data.get("Operating CF"),
        data.get("CapEx"),
        data.get("Gross Margin (%)"),
        data.get("Net Profit Margin (%)"),
        data.get("EBITDA Margin (%)"),
        data.get("EBIT Margin (%)"),
        data.get("Debt/Equity"),
        data.get("Current Ratio"),
        data.get("Quick Ratio"),
        data.get("DSO (days)"),
        data.get("DIO (days)"),
        data.get("DPO (days)"),
        data.get("CCC (days)"),
        data.get("EPS"),
        data.get("P/E"),
        data.get("P/B"),
        data.get("Graham Number"),
        data.get("Dividend Yield (%)"),
        data.get("Market Cap"),
        data.get("Revenue"),
        data.get("Net Income"),
        data.get("EBITDA"),
        data.get("Inventory"),
        data.get("TTM EPS"),
        data.get("TTM P/E"),
    ))

    conn.commit()
    conn.close()
    print(f"  ✅ fundamentals: snapshot saved for {symbol} on {today}")
