"""
etl/load/growth_loader.py  v3.0
────────────────────────────────────────────────────────────────
Changes vs v2:
  • load_growth_from_screener() writes scr_* CAGR columns into
    the same growth_metrics table
  • Screener growth sheet has metrics as rows, periods as columns
    e.g. rows: "Sales Growth", "Profit Growth", "Stock Price CAGR"
         cols: "10 Years", "5 Years", "3 Years", "TTM"
────────────────────────────────────────────────────────────────
"""

from datetime import date
from database.db import get_connection


_COMPARE_COLS = ["revenue_cagr_3y", "net_profit_cagr_3y",
                 "ebitda_cagr_3y", "fcf_cagr_3y"]


def _latest_row(conn, symbol: str) -> dict | None:
    cur = conn.execute(
        "SELECT * FROM growth_metrics WHERE symbol=? ORDER BY rowid DESC LIMIT 1",
        (symbol,)
    )
    row = cur.fetchone()
    if row is None:
        return None
    return dict(zip([d[0] for d in cur.description], row))


def _data_changed(latest: dict, new_data: dict) -> bool:
    for col in _COMPARE_COLS:
        if latest.get(col) != new_data.get(col) and new_data.get(col) is not None:
            return True
    return False


def load_growth_metrics(data: dict, symbol: str):
    """Upsert yfinance-derived growth CAGRs."""
    conn  = get_connection()
    today = date.today().isoformat()

    latest = _latest_row(conn, symbol)
    if latest is not None and not _data_changed(latest, data):
        conn.close()
        print(f"  ⏭  growth_metrics: no change for {symbol} — skipping")
        return

    conn.execute("""
        INSERT INTO growth_metrics (
            symbol, as_of_date,
            revenue_cagr_3y, net_profit_cagr_3y,
            ebitda_cagr_3y, eps_cagr_3y, fcf_cagr_3y,
            revenue_yoy_json, net_income_yoy_json,
            ebitda_yoy_json, fcf_yoy_json,
            gross_margin_trend_json
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, as_of_date) DO UPDATE SET
            revenue_cagr_3y=excluded.revenue_cagr_3y,
            net_profit_cagr_3y=excluded.net_profit_cagr_3y,
            ebitda_cagr_3y=excluded.ebitda_cagr_3y,
            eps_cagr_3y=excluded.eps_cagr_3y,
            fcf_cagr_3y=excluded.fcf_cagr_3y,
            revenue_yoy_json=excluded.revenue_yoy_json,
            net_income_yoy_json=excluded.net_income_yoy_json,
            ebitda_yoy_json=excluded.ebitda_yoy_json,
            fcf_yoy_json=excluded.fcf_yoy_json,
            gross_margin_trend_json=excluded.gross_margin_trend_json
    """, (
        symbol, data.get("as_of_date", today),
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


def load_growth_from_screener(df, symbol: str):
    """
    Load Screener compounded growth numbers into scr_* columns
    of growth_metrics. Merges into today's row (creates if needed).

    Screener growth sheet structure (example):
      rows:  Sales Growth, Profit Growth, Stock Price CAGR, Return on Equity
      cols:  10 Years, 5 Years, 3 Years, TTM
    """
    if df is None or df.empty:
        print("  ⚠  growth_metrics screener: no data")
        return

    today = date.today().isoformat()

    def v(metric_substr, period_substr):
        for idx in df.index:
            if metric_substr.lower() in str(idx).lower():
                for col in df.columns:
                    if period_substr.lower() in str(col).lower():
                        raw = df.loc[idx, col]
                        if raw is None:
                            return None
                        s = str(raw).replace("%", "").replace(",", "").strip()
                        if s in ("", "-", "—", "nan", "None"):
                            return None
                        try:
                            return round(float(s), 4)
                        except ValueError:
                            return None
        return None

    conn = get_connection()

    conn.execute("""
        INSERT INTO growth_metrics (symbol, as_of_date,
            scr_sales_cagr_10y, scr_sales_cagr_5y, scr_sales_cagr_3y, scr_sales_ttm,
            scr_profit_cagr_10y, scr_profit_cagr_5y, scr_profit_cagr_3y, scr_profit_ttm,
            scr_stock_cagr_10y, scr_stock_cagr_5y, scr_stock_cagr_3y, scr_stock_ttm,
            scr_roe_10y, scr_roe_5y, scr_roe_3y, scr_roe_last
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, as_of_date) DO UPDATE SET
            scr_sales_cagr_10y=excluded.scr_sales_cagr_10y,
            scr_sales_cagr_5y=excluded.scr_sales_cagr_5y,
            scr_sales_cagr_3y=excluded.scr_sales_cagr_3y,
            scr_sales_ttm=excluded.scr_sales_ttm,
            scr_profit_cagr_10y=excluded.scr_profit_cagr_10y,
            scr_profit_cagr_5y=excluded.scr_profit_cagr_5y,
            scr_profit_cagr_3y=excluded.scr_profit_cagr_3y,
            scr_profit_ttm=excluded.scr_profit_ttm,
            scr_stock_cagr_10y=excluded.scr_stock_cagr_10y,
            scr_stock_cagr_5y=excluded.scr_stock_cagr_5y,
            scr_stock_cagr_3y=excluded.scr_stock_cagr_3y,
            scr_stock_ttm=excluded.scr_stock_ttm,
            scr_roe_10y=excluded.scr_roe_10y,
            scr_roe_5y=excluded.scr_roe_5y,
            scr_roe_3y=excluded.scr_roe_3y,
            scr_roe_last=excluded.scr_roe_last
    """, (
        symbol, today,
        v("Sales", "10 Year"), v("Sales", "5 Year"),
        v("Sales", "3 Year"), v("Sales", "TTM"),
        v("Profit", "10 Year"), v("Profit", "5 Year"),
        v("Profit", "3 Year"), v("Profit", "TTM"),
        v("Stock", "10 Year"), v("Stock", "5 Year"),
        v("Stock", "3 Year"), v("Stock", "TTM"),
        v("Return on Equity", "10 Year"), v("Return on Equity", "5 Year"),
        v("Return on Equity", "3 Year"), v("Return on Equity", "TTM"),
    ))
    conn.commit()
    conn.close()
    print(f"  ✅ growth_metrics: Screener CAGR columns saved for {symbol}")