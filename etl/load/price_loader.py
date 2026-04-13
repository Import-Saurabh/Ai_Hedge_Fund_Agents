import pandas as pd
from database.db import get_connection


def load_price(df: pd.DataFrame, symbol: str):
    """
    Upsert daily price data. Uses INSERT OR REPLACE so re-running
    the pipeline never creates duplicate rows (UNIQUE on symbol+date).
    """
    conn = get_connection()
    rows = [
        (symbol, str(row["date"]), row["open"], row["high"],
         row["low"], row["close"], None, row["volume"])
        for _, row in df.iterrows()
    ]
    conn.executemany("""
        INSERT OR REPLACE INTO price_daily
            (symbol, date, open, high, low, close, adj_close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()
    print(f"  ✅ price_daily: {len(rows)} rows upserted")
