from database.db import get_connection

def load_balance(df, symbol, period_type="annual"):
    conn = get_connection()
    cursor = conn.cursor()

    for date in df.columns:
        cursor.execute("""
            INSERT OR REPLACE INTO balance_sheet (
                symbol,
                period_end,
                period_type,
                total_assets,
                total_liabilities,
                total_equity
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            symbol,
            str(date)[:10],
            period_type,
            df.loc["Total Assets", date] if "Total Assets" in df.index else None,
            df.loc["Total Liabilities", date] if "Total Liabilities" in df.index else None,
            df.loc["Stockholders Equity", date] if "Stockholders Equity" in df.index else None,
        ))

    conn.commit()
    conn.close()