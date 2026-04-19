"""
etl/load/price_loader.py  v2.0
────────────────────────────────────────────────────────────────
Fixes vs v1:
  • adj_close column now correctly inserted (was always NULL)
  • Duplicate protection via INSERT OR IGNORE so re-runs are safe
  • Rounds all price columns to 4dp and volume to int
────────────────────────────────────────────────────────────────
"""

import math
import pandas as pd
from database.db import get_connection


def _safe_float(v, dp: int = 4) -> float | None:
    try:
        fv = float(v)
        if math.isnan(fv) or math.isinf(fv):
            return None
        return round(fv, dp)
    except (TypeError, ValueError):
        return None


def _safe_int(v) -> int | None:
    try:
        fv = float(v)
        if math.isnan(fv):
            return None
        return int(fv)
    except (TypeError, ValueError):
        return None


def load_price(df: pd.DataFrame, symbol: str):
    """
    Load daily OHLCV + adj_close into price_daily.
    Uses INSERT OR IGNORE to safely handle re-runs.
    """
    if df is None or df.empty:
        print(f"  ⚠  price_daily: empty dataframe — skipping")
        return

    conn  = get_connection()
    count = 0

    for _, row in df.iterrows():
        conn.execute("""
            INSERT OR IGNORE INTO price_daily
                (symbol, date, open, high, low, close, adj_close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol,
            str(row.get("date"))[:10],
            _safe_float(row.get("open")),
            _safe_float(row.get("high")),
            _safe_float(row.get("low")),
            _safe_float(row.get("close")),
            _safe_float(row.get("adj_close")),
            _safe_int(row.get("volume")),
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ price_daily: {count} rows processed for {symbol}")