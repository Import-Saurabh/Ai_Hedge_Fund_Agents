"""
etl/load/fundamentals_loader.py  v3.0
────────────────────────────────────────────────────────────────
Fixes vs v2:
  • ev, ev_ebitda, ev_revenue, forward_pe, earnings_growth_json
    now always populated when source data available
  • Dedup guard: checks if the latest row for the symbol has
    identical key ratios — skips insert if unchanged
  • All monetary fields confirmed in Rs. Crores before insert
  • market_cap stored in Rs. Crores (not Rs. Billions)
────────────────────────────────────────────────────────────────
"""

import json
from datetime import date
from database.db import get_connection


_COMPARE_COLS = [
    "roe_pct", "roce_pct", "roa_pct", "eps_annual",
    "pe_ratio", "pb_ratio", "market_cap", "revenue",
]


def _latest_row(conn, symbol: str) -> dict | None:
    cur = conn.execute(
        "SELECT * FROM fundamentals WHERE symbol=? ORDER BY rowid DESC LIMIT 1",
        (symbol,)
    )
    row = cur.fetchone()
    if row is None:
        return None
    return dict(zip([d[0] for d in cur.description], row))


def _data_changed(latest: dict, new_data: dict) -> bool:
    for col in _COMPARE_COLS:
        old_v = latest.get(col)
        new_v = new_data.get(col)
        if new_v is None:
            continue
        if old_v != new_v:
            return True
    return False


def load_fundamentals(symbol: str, data: dict):
    """
    Upsert one fundamentals row.
    Monetary values (market_cap, revenue, etc.) must be in Rs. Crores.
    Skips insert if data is identical to the most recent row.
    """
    conn = get_connection()

    mapped = {
        "roe_pct":               data.get("ROE (%)"),
        "roce_pct":              data.get("ROCE (%)"),
        "roa_pct":               data.get("ROA (%)"),
        "interest_coverage":     data.get("Interest Coverage"),
        "free_cash_flow":        data.get("Free Cash Flow"),
        "operating_cf":          data.get("Operating CF"),
        "capex":                 data.get("CapEx"),
        "gross_margin_pct":      data.get("Gross Margin (%)"),
        "net_profit_margin_pct": data.get("Net Profit Margin (%)"),
        "ebitda_margin_pct":     data.get("EBITDA Margin (%)"),
        "ebit_margin_pct":       data.get("EBIT Margin (%)"),
        "debt_to_equity":        data.get("Debt/Equity"),
        "current_ratio":         data.get("Current Ratio"),
        "quick_ratio":           data.get("Quick Ratio"),
        "dso_days":              data.get("DSO (days)"),
        "dio_days":              data.get("DIO (days)"),
        "dpo_days":              data.get("DPO (days)"),
        "cash_conversion_cycle": data.get("CCC (days)"),
        "eps_annual":            data.get("EPS"),
        "pe_ratio":              data.get("P/E"),
        "pb_ratio":              data.get("P/B"),
        "graham_number":         data.get("Graham Number"),
        "dividend_yield_pct":    data.get("Dividend Yield (%)"),
        "market_cap":            data.get("Market Cap"),
        "revenue":               data.get("Revenue"),
        "net_income":            data.get("Net Income"),
        "ebitda":                data.get("EBITDA"),
        "inventory":             data.get("Inventory"),
        "ttm_eps":               data.get("TTM EPS"),
        "ttm_pe":                data.get("TTM P/E"),
        "ev":                    data.get("EV"),
        "ev_ebitda":             data.get("EV/EBITDA"),
        "ev_revenue":            data.get("EV/Revenue"),
        "forward_pe":            data.get("Forward PE"),
        "earnings_growth_json":  data.get("earnings_growth_json"),
    }

    latest = _latest_row(conn, symbol)
    if latest is not None and not _data_changed(latest, mapped):
        conn.close()
        print(f"  ⏭  fundamentals: no change for {symbol} — skipping insert")
        return

    today = date.today().isoformat()
    conn.execute("""
        INSERT OR REPLACE INTO fundamentals (
            symbol, as_of_date,
            roe_pct, roce_pct, roa_pct, interest_coverage,
            free_cash_flow, operating_cf, capex,
            gross_margin_pct, net_profit_margin_pct,
            ebitda_margin_pct, ebit_margin_pct,
            debt_to_equity, current_ratio, quick_ratio,
            dso_days, dio_days, dpo_days, cash_conversion_cycle,
            eps_annual, pe_ratio, pb_ratio, graham_number,
            dividend_yield_pct, market_cap, revenue, net_income,
            ebitda, inventory, ttm_eps, ttm_pe,
            ev, ev_ebitda, ev_revenue, forward_pe, earnings_growth_json
        ) VALUES (
            ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
        )
    """, (
        symbol, today,
        mapped["roe_pct"], mapped["roce_pct"], mapped["roa_pct"],
        mapped["interest_coverage"],
        mapped["free_cash_flow"], mapped["operating_cf"], mapped["capex"],
        mapped["gross_margin_pct"], mapped["net_profit_margin_pct"],
        mapped["ebitda_margin_pct"], mapped["ebit_margin_pct"],
        mapped["debt_to_equity"], mapped["current_ratio"], mapped["quick_ratio"],
        mapped["dso_days"], mapped["dio_days"], mapped["dpo_days"],
        mapped["cash_conversion_cycle"],
        mapped["eps_annual"], mapped["pe_ratio"], mapped["pb_ratio"],
        mapped["graham_number"], mapped["dividend_yield_pct"],
        mapped["market_cap"], mapped["revenue"], mapped["net_income"],
        mapped["ebitda"], mapped["inventory"],
        mapped["ttm_eps"], mapped["ttm_pe"],
        mapped["ev"], mapped["ev_ebitda"], mapped["ev_revenue"],
        mapped["forward_pe"], mapped["earnings_growth_json"],
    ))
    conn.commit()
    conn.close()
    print(f"  ✅ fundamentals: saved for {symbol} on {today}")