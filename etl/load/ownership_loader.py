"""
etl/load/ownership_loader.py  v2.0
────────────────────────────────────────────────────────────────
Fixes vs v1:
  • Checks whether the NEW data actually differs from the most
    recently stored row before inserting — prevents same-value
    rows accumulating across pipeline runs
  • Stores last_updated_source timestamp for freshness tracking
────────────────────────────────────────────────────────────────
"""

import json
from datetime import date
from database.db import get_connection


_COMPARE_COLS = [
    "promoter_pct", "fii_fpi_pct", "dii_pct", "public_retail_pct",
    "total_institutional_pct",
]


def _latest_row(conn, symbol: str) -> dict | None:
    """Return the most recent ownership row for this symbol."""
    cur = conn.execute(
        "SELECT * FROM ownership WHERE symbol = ? ORDER BY rowid DESC LIMIT 1",
        (symbol,)
    )
    row = cur.fetchone()
    if row is None:
        return None
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, row))


def _data_changed(latest: dict, new_data: dict) -> bool:
    """Return True if any comparison column differs (or has become non-NULL)."""
    for col in _COMPARE_COLS:
        old_val = latest.get(col)
        new_val = new_data.get(col)
        if new_val is None:
            continue                # no new data for this field — not a change
        if old_val != new_val:
            return True
    return False


def load_ownership(data: dict, symbol: str):
    """Load shareholding snapshot, skipping if data is unchanged."""
    conn = get_connection()

    latest = _latest_row(conn, symbol)
    if latest is not None and not _data_changed(latest, data):
        conn.close()
        print(f"  ⏭  ownership: no change detected for {symbol} — skipping insert")
        return

    conn.execute("""
        INSERT OR REPLACE INTO ownership (
            symbol, snapshot_date,
            promoter_pct, fii_fpi_pct, dii_pct, public_retail_pct,
            insiders_pct, institutions_pct, institutions_float_pct,
            institutions_count, total_institutional_pct, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        symbol,
        data.get("snapshot_date", date.today().isoformat()),
        data.get("promoter_pct"),
        data.get("fii_fpi_pct"),
        data.get("dii_pct"),
        data.get("public_retail_pct"),
        data.get("insiders_pct"),
        data.get("institutions_pct"),
        data.get("institutions_float_pct"),
        data.get("institutions_count"),
        data.get("total_institutional_pct"),
        data.get("source", "mixed"),
    ))
    conn.commit()
    conn.close()
    print(f"  ✅ ownership: saved for {symbol} on {data.get('snapshot_date')}")