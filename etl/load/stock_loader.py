from database.db import get_connection

def insert_stock(symbol: str, name: str, exchange="NSE"):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO stocks (symbol, name, exchange)
        VALUES (?, ?, ?)
    """, (symbol, name, exchange))

    conn.commit()
    conn.close()