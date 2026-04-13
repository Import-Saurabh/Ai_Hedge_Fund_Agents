from database.db import get_connection

def load_price(df):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.executemany("""
        INSERT OR REPLACE INTO price_daily
        (symbol, date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, df.values.tolist())

    conn.commit()
    conn.close()