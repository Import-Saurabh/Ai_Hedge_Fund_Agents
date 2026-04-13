from database.db import get_connection


def load_ownership(data: dict, symbol: str):
    """Load shareholding pattern snapshot."""
    conn = get_connection()
    conn.execute("""
        INSERT OR REPLACE INTO ownership (
            symbol, snapshot_date,
            promoter_pct, fii_fpi_pct, dii_pct, public_retail_pct,
            insiders_pct, institutions_pct, institutions_float_pct,
            institutions_count, total_institutional_pct, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        symbol,
        data.get("snapshot_date"),
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
