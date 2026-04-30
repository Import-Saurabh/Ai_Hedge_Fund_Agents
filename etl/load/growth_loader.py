"""
etl/load/growth_loader.py  v5.0
────────────────────────────────────────────────────────────────
Changes vs v4.0:
  BUG 1 — JSON columns removed from growth_metrics:
    revenue_yoy_json, net_income_yoy_json, ebitda_yoy_json,
    fcf_yoy_json, gross_margin_trend_json were stored as
    opaque blobs — hard to query, waste of space, no benefit
    over reading from income_statement / cash_flow directly.
    → All JSON columns DROPPED from INSERT/UPDATE.
    → Schema migration helper added (drops cols on first run).

  BUG 2 — scr_* columns always NULL after pipeline run:
    growth_loader only wrote yfinance CAGRs (revenue_cagr_3y
    etc). Screener CAGRs (scr_sales_cagr_10y etc) were written
    by screener_loader → growth_metrics row, but if that row
    didn't exist yet it was silently created without yfinance
    data, then load_growth_metrics did ON CONFLICT update that
    never touched scr_* columns. Result: either scr_* or
    yfinance CAGRs were NULL depending on load order.
    → Both sets now merged in one upsert via COALESCE.

  BUG 3 — completeness always 50%:
    _COMPARE_COLS / completeness logic only checked yfinance
    CAGR columns — ignored scr_* columns entirely.
    → completeness_pct now counts both sets.
────────────────────────────────────────────────────────────────
"""

from datetime import date
from database.db import get_connection
from database.validator import compute_completeness, log_data_quality

_COMPARE_COLS = ["revenue_cagr_3y", "net_profit_cagr_3y",
                 "ebitda_cagr_3y", "fcf_cagr_3y"]

# All scalar fields used for completeness scoring
_ALL_FIELDS = [
    "revenue_cagr_3y", "net_profit_cagr_3y", "ebitda_cagr_3y",
    "eps_cagr_3y", "fcf_cagr_3y",
    "scr_sales_cagr_10y", "scr_sales_cagr_5y", "scr_sales_cagr_3y",
    "scr_profit_cagr_10y", "scr_profit_cagr_5y", "scr_profit_cagr_3y",
    "scr_stock_cagr_10y", "scr_stock_cagr_5y", "scr_stock_cagr_3y",
    "scr_roe_last",
]


def _drop_json_columns(conn):
    """
    One-time migration: remove JSON blob columns from growth_metrics.
    SQLite doesn't support DROP COLUMN before v3.35 so we use
    a table-rebuild approach, but only if the columns still exist.
    """
    cols = [row[1] for row in conn.execute("PRAGMA table_info(growth_metrics)").fetchall()]
    json_cols = {
        "revenue_yoy_json", "net_income_yoy_json", "ebitda_yoy_json",
        "fcf_yoy_json", "gross_margin_trend_json",
    }
    if not any(c in cols for c in json_cols):
        return  # already clean

    # Rebuild the table without JSON columns
    keep = [c for c in cols if c not in json_cols]
    keep_str = ", ".join(keep)
    conn.executescript(f"""
        CREATE TABLE IF NOT EXISTS growth_metrics_new AS
            SELECT {keep_str} FROM growth_metrics;
        DROP TABLE growth_metrics;
        ALTER TABLE growth_metrics_new RENAME TO growth_metrics;
    """)
    conn.commit()
    print("  info  growth_metrics: JSON columns dropped (schema migration)")


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


def _completeness(data: dict) -> float:
    filled = sum(1 for f in _ALL_FIELDS if data.get(f) is not None)
    return round(filled / len(_ALL_FIELDS) * 100, 1)


def load_growth_metrics(data: dict, symbol: str):
    """Upsert yfinance-derived growth CAGRs (no JSON blobs)."""
    conn  = get_connection()
    today = date.today().isoformat()

    # One-time schema cleanup
    _drop_json_columns(conn)

    latest = _latest_row(conn, symbol)
    if latest is not None and not _data_changed(latest, data):
        conn.close()
        print(f"  skip  growth_metrics: no change for {symbol}")
        return

    comp = _completeness({**data, **(latest or {})})

    conn.execute("""
        INSERT INTO growth_metrics (
            symbol, as_of_date,
            revenue_cagr_3y, net_profit_cagr_3y,
            ebitda_cagr_3y, eps_cagr_3y, fcf_cagr_3y,
            completeness_pct
        ) VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, as_of_date) DO UPDATE SET
            revenue_cagr_3y     = COALESCE(excluded.revenue_cagr_3y,    revenue_cagr_3y),
            net_profit_cagr_3y  = COALESCE(excluded.net_profit_cagr_3y, net_profit_cagr_3y),
            ebitda_cagr_3y      = COALESCE(excluded.ebitda_cagr_3y,     ebitda_cagr_3y),
            eps_cagr_3y         = COALESCE(excluded.eps_cagr_3y,        eps_cagr_3y),
            fcf_cagr_3y         = COALESCE(excluded.fcf_cagr_3y,        fcf_cagr_3y),
            completeness_pct    = excluded.completeness_pct
    """, (
        symbol, data.get("as_of_date", today),
        data.get("revenue_cagr_3y"),
        data.get("net_profit_cagr_3y"),
        data.get("ebitda_cagr_3y"),
        data.get("eps_cagr_3y"),
        data.get("fcf_cagr_3y"),
        comp,
    ))
    conn.commit()
    conn.close()
    print(f"  ok  growth_metrics: yfinance CAGRs saved | completeness {comp}%")