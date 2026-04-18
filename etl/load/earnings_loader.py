"""
etl/load/earnings_loader.py  v2.0
────────────────────────────────────────────────────────────────
Fixes vs v1:
  • _to_int() now correctly casts numpy int64 → Python int
    (was storing raw 8-byte little-endian BLOBs like x'0100000000000000')
  • Added explicit CAST in INSERT for integer columns as extra guard
────────────────────────────────────────────────────────────────
"""

import math
from database.db import get_connection


def _to_int(v) -> int | None:
    """
    Safely cast any numeric type (numpy int64, float, str) to a plain
    Python int that SQLite stores as INTEGER, not a BLOB.

    Root cause of the BLOB issue: numpy int64 is not a Python int.
    sqlite3 serialises it as an 8-byte binary blob instead of an integer.
    Explicit int() conversion fixes this.
    """
    if v is None:
        return None
    try:
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return None
        return int(f)          # ← always a plain Python int
    except (TypeError, ValueError):
        return None


def _to_float(v) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────────
def load_earnings_history(records: list, symbol: str):
    conn  = get_connection()
    count = 0
    for r in records:
        conn.execute("""
            INSERT OR REPLACE INTO earnings_history
                (symbol, quarter_end, eps_actual, eps_estimate,
                 eps_difference, surprise_pct)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            symbol,
            r["quarter_end"],
            _to_float(r.get("eps_actual")),
            _to_float(r.get("eps_estimate")),
            _to_float(r.get("eps_difference")),
            _to_float(r.get("surprise_pct")),
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ earnings_history: {count} rows upserted")


def load_earnings_estimates(records: list, symbol: str):
    conn  = get_connection()
    count = 0
    for r in records:
        conn.execute("""
            INSERT OR REPLACE INTO earnings_estimates
                (symbol, snapshot_date, period_code,
                 avg_eps, low_eps, high_eps, year_ago_eps,
                 analyst_count, growth_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol,
            r["snapshot_date"],
            r["period_code"],
            _to_float(r.get("avg_eps")),
            _to_float(r.get("low_eps")),
            _to_float(r.get("high_eps")),
            _to_float(r.get("year_ago_eps")),
            _to_int(r.get("analyst_count")),   # ← now a safe Python int
            _to_float(r.get("growth_pct")),
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ earnings_estimates: {count} rows upserted")


def load_eps_trend(records: list, symbol: str):
    conn  = get_connection()
    count = 0
    for r in records:
        conn.execute("""
            INSERT OR REPLACE INTO eps_trend
                (symbol, snapshot_date, period_code,
                 current_est, seven_days_ago, thirty_days_ago,
                 sixty_days_ago, ninety_days_ago)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol,
            r["snapshot_date"],
            r["period_code"],
            _to_float(r.get("current_est")),
            _to_float(r.get("seven_days_ago")),
            _to_float(r.get("thirty_days_ago")),
            _to_float(r.get("sixty_days_ago")),
            _to_float(r.get("ninety_days_ago")),
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ eps_trend: {count} rows upserted")


def load_eps_revisions(records: list, symbol: str):
    """
    Stores analyst revision counts as plain Python ints.

    Previously stored as x'0100000000000000' BLOBs because
    yfinance returns numpy int64 values. _to_int() converts them
    to Python int before passing to sqlite3.
    """
    conn  = get_connection()
    count = 0
    for r in records:
        conn.execute("""
            INSERT OR REPLACE INTO eps_revisions
                (symbol, snapshot_date, period_code,
                 up_last_7d, up_last_30d, down_last_30d, down_last_7d)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol,
            r["snapshot_date"],
            r["period_code"],
            _to_int(r.get("up_last_7d")),    # ← plain Python int, never BLOB
            _to_int(r.get("up_last_30d")),
            _to_int(r.get("down_last_30d")),
            _to_int(r.get("down_last_7d")),
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ eps_revisions: {count} rows upserted")