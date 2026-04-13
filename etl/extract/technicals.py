import pandas as pd

def compute_technicals(df: pd.DataFrame):
    df = df.copy()

    # Simple Moving Averages
    df["SMA_50"] = df["close"].rolling(50).mean()
    df["SMA_200"] = df["close"].rolling(200).mean()

    # RSI (basic)
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = -delta.clip(upper=0).rolling(14).mean()
    rs = gain / loss
    df["RSI_14"] = 100 - (100 / (1 + rs))

    # MACD
    ema12 = df["close"].ewm(span=12).mean()
    ema26 = df["close"].ewm(span=26).mean()
    df["MACD"] = ema12 - ema26

    return df