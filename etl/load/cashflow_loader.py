from database.db import get_connection

def load_cashflow(df, symbol, period_type="annual"):
    conn = get_connection()
    cursor = conn.cursor()

    for date in df.columns:
        cursor.execute("""
            INSERT OR REPLACE INTO cash_flow (
                symbol,
                period_end,
                period_type,
                operating_cash_flow,
                capex,
                free_cash_flow
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            symbol,
            str(date)[:10],
            period_type,
            df.loc["Operating Cash Flow", date] if "Operating Cash Flow" in df.index else None,
            df.loc["Capital Expenditure", date] if "Capital Expenditure" in df.index else None,
            df.loc["Free Cash Flow", date] if "Free Cash Flow" in df.index else None,
        ))

    conn.commit()
    conn.close()