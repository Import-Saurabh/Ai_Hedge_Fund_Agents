from database.db import get_connection


def load_market_indices(data: dict, snapshot_date: str):
    """Load NSE index snapshots."""
    conn = get_connection()
    count = 0
    for name, entry in data.get("indices", {}).items():
        conn.execute("""
            INSERT OR REPLACE INTO market_indices
                (snapshot_date, index_name, last_price, change_pct, direction)
            VALUES (?, ?, ?, ?, ?)
        """, (
            snapshot_date, name,
            entry.get("price"), entry.get("change_pct"), entry.get("direction"),
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ market_indices: {count} rows upserted")


def load_forex_commodities(data: dict, snapshot_date: str):
    """Load forex and commodity snapshots."""
    conn = get_connection()
    count = 0
    for name, entry in data.get("forex", {}).items():
        conn.execute("""
            INSERT OR REPLACE INTO forex_commodities
                (snapshot_date, instrument, last_price, change_pct)
            VALUES (?, ?, ?, ?)
        """, (
            snapshot_date, name,
            entry.get("price"), entry.get("change_pct"),
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ forex_commodities: {count} rows upserted")


def load_rbi_rates(data: dict):
    """Load RBI policy rates."""
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO rbi_rates (
            effective_date, repo_rate, reverse_repo,
            sdf_rate, msf_rate, bank_rate,
            crr, slr, is_cached, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("date"),
        data.get("repo_rate"),
        data.get("reverse_repo"),
        data.get("sdf_rate"),
        data.get("msf_rate"),
        data.get("bank_rate") or data.get("msf_rate"),
        data.get("crr"),
        data.get("slr"),
        data.get("is_cached", 1),
        data.get("source"),
    ))
    conn.commit()
    conn.close()
    print(f"  ✅ rbi_rates: saved for {data.get('date')}")


def load_macro_indicators(records: list):
    """Load World Bank macro indicators."""
    conn = get_connection()
    count = 0
    for r in records:
        conn.execute("""
            INSERT OR REPLACE INTO macro_indicators
                (snapshot_date, indicator_name, source, value, year)
            VALUES (?, ?, ?, ?, ?)
        """, (
            r["snapshot_date"], r["indicator_name"],
            r["source"], r["value"], r["year"],
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ macro_indicators: {count} rows upserted")
