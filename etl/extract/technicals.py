import numpy as np
import pandas as pd


def compute_technicals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all technical indicators used in test.py Section E.
    Input df must have columns: date, close, high, low, volume
    """
    df = df.copy().sort_values("date").reset_index(drop=True)

    close = df["close"]
    high  = df["high"]
    low   = df["low"]

    # RSI 14
    delta = close.diff()
    gain  = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=13, adjust=False).mean()
    df["rsi_14"] = 100 - 100 / (1 + gain / loss.replace(0, np.nan))

    # MACD
    e12 = close.ewm(span=12, adjust=False).mean()
    e26 = close.ewm(span=26, adjust=False).mean()
    df["macd"]        = e12 - e26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"]   = df["macd"] - df["macd_signal"]

    # Moving Averages
    df["sma_50"]  = close.rolling(50).mean()
    df["sma_200"] = close.rolling(200).mean()
    df["ema_21"]  = close.ewm(span=21, adjust=False).mean()

    # Bollinger Bands
    r20 = close.rolling(20)
    df["bb_mid"]   = r20.mean()
    df["bb_upper"] = df["bb_mid"] + 2 * r20.std()
    df["bb_lower"] = df["bb_mid"] - 2 * r20.std()

    # ATR 14
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs(),
    ], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(14).mean()

    return df
