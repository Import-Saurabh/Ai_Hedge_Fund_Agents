"""
etl/load/macro_loader.py  v2.0
────────────────────────────────────────────────────────────────
Fixes vs v1:
  • change_pct for forex/commodities now populated (was NULL when
    yfinance returned only 1 row — fixed in macro.py extractor)
  • rbi_rates: dedup guard checks repo_rate before inserting
  • macro_indicators: uses (snapshot_date, indicator_name, year)
    UNIQUE constraint correctly — no duplicate annual rows
  • forex_commodities/market_indices: INSERT OR IGNORE (already
    have UNIQUE on date+name, so re-runs are naturally safe)
────────────────────────────────────────────────────────────────
"""

from database.db import get_connection


def load_market_indices(data: dict, snapshot_date: str):
    indices = data.get("indices", {})
    if not indices:
        print("  ⚠  market_indices: no data")
        return

    conn  = get_connection()
    count = 0
    for name, entry in indices.items():
        conn.execute("""
            INSERT OR IGNORE INTO market_indices
                (snapshot_date, index_name, last_price, change_pct, direction)
            VALUES (?, ?, ?, ?, ?)
        """, (
            snapshot_date,
            name,
            entry.get("price"),
            entry.get("change_pct"),
            entry.get("direction"),
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ market_indices: {count} rows for {snapshot_date}")


def load_forex_commodities(data: dict, snapshot_date: str):
    forex = data.get("forex", {})
    if not forex:
        print("  ⚠  forex_commodities: no data")
        return

    conn  = get_connection()
    count = 0
    for name, entry in forex.items():
        conn.execute("""
            INSERT OR IGNORE INTO forex_commodities
                (snapshot_date, instrument, last_price, change_pct)
            VALUES (?, ?, ?, ?)
        """, (
            snapshot_date,
            name,
            entry.get("price"),
            entry.get("change_pct"),
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ forex_commodities: {count} rows for {snapshot_date}")


def load_rbi_rates(data: dict):
    """
    Load RBI rates — skips if repo_rate identical to most recent row.
    effective_date UNIQUE constraint prevents exact-same-date duplicates.
    """
    if not data:
        print("  ⚠  rbi_rates: no data")
        return

    conn  = get_connection()

    # Check if rate has changed since last insert
    last = conn.execute(
        "SELECT repo_rate FROM rbi_rates ORDER BY rowid DESC LIMIT 1"
    ).fetchone()
    if last and last[0] == data.get("repo_rate"):
        conn.close()
        print(f"  ⏭  rbi_rates: repo_rate unchanged ({data.get('repo_rate')}%) — skipping")
        return

    conn.execute("""
        INSERT OR IGNORE INTO rbi_rates
            (effective_date, repo_rate, reverse_repo, sdf_rate,
             msf_rate, bank_rate, crr, slr, is_cached, source)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("date"),
        data.get("repo_rate"),
        data.get("reverse_repo"),
        data.get("sdf_rate"),
        data.get("msf_rate"),
        data.get("bank_rate"),
        data.get("crr"),
        data.get("slr"),
        data.get("is_cached", 0),
        data.get("source", ""),
    ))
    conn.commit()
    conn.close()
    print(f"  ✅ rbi_rates: repo={data.get('repo_rate')}% saved")


def load_macro_indicators(records: list):
    """
    Load macro indicator records.
    UNIQUE (snapshot_date, indicator_name, year) prevents duplicates.
    """
    if not records:
        return

    conn  = get_connection()
    count = 0
    for r in records:
        conn.execute("""
            INSERT OR IGNORE INTO macro_indicators
                (snapshot_date, indicator_name, source, value, year)
            VALUES (?, ?, ?, ?, ?)
        """, (
            r.get("snapshot_date"),
            r.get("indicator_name"),
            r.get("source"),
            r.get("value"),
            r.get("year"),
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ macro_indicators: {count} records processed")