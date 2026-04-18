"""
etl/extract/price.py  v2.0
────────────────────────────────────────────────────────────────
Fixes vs v1:
  • adj_close is now populated: yfinance auto_adjust=True already
    provides split/dividend-adjusted closes — we just expose the
    raw (unadjusted) close as 'close' and the adjusted value as
    'adj_close' so downstream can use either.
  • Returns 7 columns: date, open, high, low, close, adj_close, volume
────────────────────────────────────────────────────────────────
"""

import yfinance as yf
import pandas as pd


def fetch_price(symbol: str, years: int = 5) -> pd.DataFrame:
    """
    Fetch OHLCV daily price history with both raw and adjusted closes.

    yfinance auto_adjust=True returns split+dividend-adjusted prices.
    We also fetch auto_adjust=False to get the raw close, then merge.
    """
    ticker = yf.Ticker(symbol)

    # Adjusted (split + dividend)
    df_adj = ticker.history(period=f"{years}y", auto_adjust=True)
    if df_adj is None or df_adj.empty:
        raise Exception(f"No price data for {symbol}")

    # Unadjusted (raw close)
    df_raw = ticker.history(period=f"{years}y", auto_adjust=False)

    df_adj = df_adj.reset_index()
    df_adj["Date"] = pd.to_datetime(df_adj["Date"]).dt.date
    df_adj = df_adj.rename(columns={
        "Date":   "date",
        "Open":   "open",
        "High":   "high",
        "Low":    "low",
        "Close":  "adj_close",
        "Volume": "volume",
    })

    if df_raw is not None and not df_raw.empty:
        df_raw = df_raw.reset_index()
        df_raw["Date"] = pd.to_datetime(df_raw["Date"]).dt.date
        df_raw = df_raw.rename(columns={"Date": "date", "Close": "close"})
        df = pd.merge(
            df_adj[["date", "open", "high", "low", "adj_close", "volume"]],
            df_raw[["date", "close"]],
            on="date", how="left"
        )
    else:
        # Fallback: use adjusted close as both
        df = df_adj.copy()
        df["close"] = df["adj_close"]

    return df[["date", "open", "high", "low", "close", "adj_close", "volume"]]