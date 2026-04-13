import yfinance as yf
import pandas as pd

def fetch_price(symbol: str, years: int = 3) -> pd.DataFrame:
    df = yf.Ticker(symbol).history(period=f"{years}y")

    if df is None or df.empty:
        raise Exception("No price data")

    df = df.reset_index()
    df = df[["Date", "Open", "High", "Low", "Close", "Volume"]]

    df.columns = ["date", "open", "high", "low", "close", "volume"]
    df["date"] = pd.to_datetime(df["date"]).dt.date

    return df