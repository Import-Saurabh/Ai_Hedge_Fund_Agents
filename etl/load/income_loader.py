from database.db import get_connection


def load_income(df, symbol, period_type="annual"):
    conn = get_connection()
    cursor = conn.cursor()

    if df is None or df.empty:
        return

    for date in df.columns:
        cursor.execute("""
            INSERT OR REPLACE INTO income_statement (
                symbol,
                period_end,
                period_type,
                total_revenue,
                ebitda,
                net_income,
                diluted_eps
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol,
            str(date)[:10],
            period_type,
            df.loc["Total Revenue", date] if "Total Revenue" in df.index else None,
            df.loc["EBITDA", date] if "EBITDA" in df.index else None,
            df.loc["Net Income", date] if "Net Income" in df.index else None,
            df.loc["Diluted EPS", date] if "Diluted EPS" in df.index else None,
        ))

    conn.commit()
    conn.close()