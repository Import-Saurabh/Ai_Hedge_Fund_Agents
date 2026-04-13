import yfinance as yf
import pandas as pd

def fetch_price(symbol: str, years: int = 5) -> pd.DataFrame:
    """Fetch OHLCV daily price history."""
    df = yf.Ticker(symbol).history(period=f"{years}y", auto_adjust=True)
    if df is None or df.empty:
        raise Exception(f"No price data for {symbol}")
    df = df.reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    df = df.rename(columns={"Date": "date", "Open": "open", "High": "high",
                             "Low": "low", "Close": "close", "Volume": "volume"})
    return df[["date", "open", "high", "low", "close", "volume"]]
