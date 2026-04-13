from database.db import get_connection
from datetime import date
from database.db import get_connection

def load_fundamentals(symbol, data):
    conn = get_connection()
    cursor = conn.cursor()

    today = date.today().isoformat()   # ✅ REQUIRED

    cursor.execute("""
        INSERT OR REPLACE INTO fundamentals
        (symbol, as_of_date, market_cap, pe_ratio)
        VALUES (?, ?, ?, ?)
    """, (
        symbol,
        today,                         # ✅ FIX
        data.get("marketCap"),
        data.get("trailingPE"),
    ))

    conn.commit()
    conn.close()