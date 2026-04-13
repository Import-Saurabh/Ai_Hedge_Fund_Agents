from database.db import get_connection
# etl/load/technical_loader.py

def load_technicals(df, symbol):
    conn = get_connection()
    cursor = conn.cursor()

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT OR REPLACE INTO technical_indicators
            (symbol, date, macd, sma_50, sma_200)
            VALUES (?, ?, ?, ?, ?)
        """, (
            symbol,
            str(row["date"]),
            row.get("macd"),
            row.get("sma_50"),
            row.get("sma_200")
        ))

    conn.commit()
    conn.close()