"""
etl/load/growth_loader.py  v2.0
────────────────────────────────────────────────────────────────
Fixes vs v1:
  • Dedup guard: compares key CAGR fields before inserting —
    skips if identical to the most recent row for this symbol
  • JSON YoY values confirmed in Rs. Crores by extractor
  • as_of_date uses today's date consistently
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
        old_v = latest.get(col)
        new_v = new_data.get(col)
        if new_v is None:
            continue
        if old_v != new_v:
            return True
    return False


def load_growth_metrics(data: dict, symbol: str):
    """
    Upsert growth metrics.
    Skips insert if CAGR figures are identical to the most recent row.
    """
    conn   = get_connection()
    today  = date.today().isoformat()

    latest = _latest_row(conn, symbol)
    if latest is not None and not _data_changed(latest, data):
        conn.close()
        print(f"  ⏭  growth_metrics: no change for {symbol} — skipping insert")
        return

    conn.execute("""
        INSERT OR REPLACE INTO growth_metrics (
            symbol, as_of_date,
            revenue_cagr_3y, net_profit_cagr_3y,
            ebitda_cagr_3y, eps_cagr_3y, fcf_cagr_3y,
            revenue_yoy_json, net_income_yoy_json,
            ebitda_yoy_json, fcf_yoy_json,
            gross_margin_trend_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        symbol,
        data.get("as_of_date", today),
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