import pandas as pd
from database.db import get_connection


def load_technicals(df: pd.DataFrame, symbol: str):
    """
    Load all technical indicators. close price is included so the
    table is self-contained. Uses INSERT OR REPLACE (no duplicates).
    """
    conn = get_connection()
    rows = []
    for _, row in df.iterrows():
        def g(col):
            v = row.get(col)
            try:
                import math
                f = float(v)
                return None if math.isnan(f) else f
            except:
                return None

        rows.append((
            symbol,
            str(row["date"]),
            g("close"),
            g("rsi_14"),
            g("macd"),
            g("macd_signal"),
            g("macd_hist"),
            g("sma_50"),
            g("sma_200"),
            g("ema_21"),
            g("bb_mid"),
            g("bb_upper"),
            g("bb_lower"),
            g("atr_14"),
        ))

    conn.executemany("""
        INSERT OR REPLACE INTO technical_indicators
            (symbol, date, close, rsi_14, macd, macd_signal, macd_hist,
             sma_50, sma_200, ema_21, bb_mid, bb_upper, bb_lower, atr_14)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()
    print(f"  ✅ technical_indicators: {len(rows)} rows upserted")
