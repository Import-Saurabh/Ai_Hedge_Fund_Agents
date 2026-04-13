from database.db import get_connection


def load_corporate_actions(data: dict, symbol: str):
    """Load dividends and splits into corporate_actions table."""
    conn = get_connection()
    count = 0

    divs = data.get("dividends")
    if divs is not None and not divs.empty:
        for _, row in divs.iterrows():
            conn.execute("""
                INSERT OR IGNORE INTO corporate_actions
                    (symbol, action_date, action_type, value)
                VALUES (?, ?, 'dividend', ?)
            """, (symbol, str(row["date"]), float(row["value"])))
            count += 1

    splits = data.get("splits")
    if splits is not None and not splits.empty:
        for _, row in splits.iterrows():
            conn.execute("""
                INSERT OR IGNORE INTO corporate_actions
                    (symbol, action_date, action_type, value)
                VALUES (?, ?, 'split', ?)
            """, (symbol, str(row["date"]), float(row["value"])))
            count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ corporate_actions: {count} records upserted")
