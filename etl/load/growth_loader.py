"""
etl/load/growth_loader.py  v4.0
────────────────────────────────────────────────────────────────
Changes vs v3:
  • completeness_pct written to growth_metrics
  • scr_growth_available flag updated after screener merge
  • Null-guard: yfinance CAGR only inserted if value is not None
  • dedup guard unchanged
────────────────────────────────────────────────────────────────
"""

from datetime import date
from database.db import get_connection
from database.validator import compute_completeness, log_data_quality

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
        if new_data.get(col) is not None and latest.get(col) != new_data.get(col):
            return True
    return False


def load_growth_metrics(data: dict, symbol: str):
    """Upsert yfinance-derived growth CAGRs + YoY JSON."""
    conn  = get_connection()
    today = date.today().isoformat()

    latest = _latest_row(conn, symbol)
    if latest is not None and not _data_changed(latest, data):
        conn.close()
        print(f"  skip  growth_metrics: no change for {symbol}")
        return

    comp, _ = compute_completeness(data, "growth_metrics")

    conn.execute("""
        INSERT INTO growth_metrics (
            symbol, as_of_date,
            revenue_cagr_3y, net_profit_cagr_3y,
            ebitda_cagr_3y, eps_cagr_3y, fcf_cagr_3y,
            revenue_yoy_json, net_income_yoy_json,
            ebitda_yoy_json, fcf_yoy_json,
            gross_margin_trend_json, completeness_pct
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, as_of_date) DO UPDATE SET
            revenue_cagr_3y     = COALESCE(excluded.revenue_cagr_3y, revenue_cagr_3y),
            net_profit_cagr_3y  = COALESCE(excluded.net_profit_cagr_3y, net_profit_cagr_3y),
            ebitda_cagr_3y      = COALESCE(excluded.ebitda_cagr_3y, ebitda_cagr_3y),
            eps_cagr_3y         = COALESCE(excluded.eps_cagr_3y, eps_cagr_3y),
            fcf_cagr_3y         = COALESCE(excluded.fcf_cagr_3y, fcf_cagr_3y),
            revenue_yoy_json    = COALESCE(excluded.revenue_yoy_json, revenue_yoy_json),
            net_income_yoy_json = COALESCE(excluded.net_income_yoy_json, net_income_yoy_json),
            ebitda_yoy_json     = COALESCE(excluded.ebitda_yoy_json, ebitda_yoy_json),
            fcf_yoy_json        = COALESCE(excluded.fcf_yoy_json, fcf_yoy_json),
            gross_margin_trend_json = COALESCE(excluded.gross_margin_trend_json, gross_margin_trend_json),
            completeness_pct    = excluded.completeness_pct
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
        comp,
    ))
    conn.commit()
    conn.close()
    print(f"  ok  growth_metrics: yfinance CAGRs saved | completeness {comp}%")