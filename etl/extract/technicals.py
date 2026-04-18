"""
etl/extract/technicals.py  v2.0
────────────────────────────────────────────────────────────────
Fixes vs v1:
  • Added ADX-14, VWAP-14, OBV, Supertrend (were always NULL)
  • All indicators use min_periods where applicable so early rows
    contain NaN — caller (pipeline.py) drops rows where sma_200
    is NaN, which cleanly trims the 200-day warmup window
  • Supertrend uses ATR-based bands (standard 3×ATR multiplier)
────────────────────────────────────────────────────────────────
"""

import numpy as np
import pandas as pd


def _wilder_smooth(series: pd.Series, period: int) -> pd.Series:
    """Wilder's smoothing (used by RSI, ATR, ADX)."""
    result = series.copy() * np.nan
    valid  = series.dropna()
    if len(valid) < period:
        return result
    # Seed with simple average
    first_idx = valid.index[period - 1]
    result.loc[first_idx] = valid.iloc[:period].mean()
    for i in range(period, len(valid)):
        prev = result.loc[valid.index[i - 1]]
        curr = valid.iloc[i]
        result.loc[valid.index[i]] = (prev * (period - 1) + curr) / period
    return result


def compute_technicals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all technical indicators.

    Input df columns required: date, close, high, low, volume
    Returns df with additional indicator columns.
    NaN rows (inside the 200-day warmup window) should be dropped
    by the caller:
        tech_df = tech_df[tech_df["sma_200"].notna()].copy()
    """
    df = df.copy().sort_values("date").reset_index(drop=True)

    close  = df["close"]
    high   = df["high"]
    low    = df["low"]
    volume = df["volume"]

    # ── RSI-14 ────────────────────────────────────────────────
    delta = close.diff()
    gain  = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=13, adjust=False).mean()
    df["rsi_14"] = 100 - 100 / (1 + gain / loss.replace(0, np.nan))

    # ── MACD ──────────────────────────────────────────────────
    e12 = close.ewm(span=12, adjust=False).mean()
    e26 = close.ewm(span=26, adjust=False).mean()
    df["macd"]        = e12 - e26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_hist"]   = df["macd"] - df["macd_signal"]

    # ── Moving averages ───────────────────────────────────────
    df["sma_50"]  = close.rolling(50,  min_periods=50).mean()
    df["sma_200"] = close.rolling(200, min_periods=200).mean()
    df["ema_21"]  = close.ewm(span=21, adjust=False).mean()

    # ── Bollinger Bands (20, 2σ) ──────────────────────────────
    r20 = close.rolling(20, min_periods=20)
    df["bb_mid"]   = r20.mean()
    df["bb_upper"] = df["bb_mid"] + 2 * r20.std()
    df["bb_lower"] = df["bb_mid"] - 2 * r20.std()

    # ── ATR-14 ────────────────────────────────────────────────
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low  - prev_close).abs(),
    ], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(14, min_periods=14).mean()

    # ── ADX-14 ────────────────────────────────────────────────
    # Directional movement
    up_move   = high.diff()
    down_move = -low.diff()

    plus_dm  = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0),
                         index=df.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0),
                         index=df.index)

    atr14_s    = _wilder_smooth(tr,       14)
    plus_dm14  = _wilder_smooth(plus_dm,  14)
    minus_dm14 = _wilder_smooth(minus_dm, 14)

    plus_di  = 100 * plus_dm14  / atr14_s.replace(0, np.nan)
    minus_di = 100 * minus_dm14 / atr14_s.replace(0, np.nan)

    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    df["adx_14"] = _wilder_smooth(dx, 14)

    # ── VWAP-14 (rolling 14-day) ──────────────────────────────
    # Typical price × volume / rolling volume
    typical = (high + low + close) / 3
    tp_vol  = typical * volume
    df["vwap_14"] = (
        tp_vol.rolling(14, min_periods=14).sum()
        / volume.rolling(14, min_periods=14).sum()
    )

    # ── OBV ───────────────────────────────────────────────────
    direction = np.sign(close.diff().fillna(0))
    df["obv"]  = (direction * volume).cumsum()

    # ── Supertrend (10-period, 3×ATR) ─────────────────────────
    period     = 10
    multiplier = 3.0

    atr10 = tr.rolling(period, min_periods=period).mean()
    hl2   = (high + low) / 2

    upper_band = hl2 + multiplier * atr10
    lower_band = hl2 - multiplier * atr10

    supertrend     = [np.nan] * len(df)
    supertrend_dir = [np.nan] * len(df)

    for i in range(1, len(df)):
        if pd.isna(atr10.iloc[i]):
            continue

        ub = upper_band.iloc[i]
        lb = lower_band.iloc[i]

        prev_ub = upper_band.iloc[i - 1] if not pd.isna(atr10.iloc[i - 1]) else ub
        prev_lb = lower_band.iloc[i - 1] if not pd.isna(atr10.iloc[i - 1]) else lb

        # Adjust bands
        if lb > prev_lb or close.iloc[i - 1] < prev_lb:
            lb = lb
        else:
            lb = prev_lb

        if ub < prev_ub or close.iloc[i - 1] > prev_ub:
            ub = ub
        else:
            ub = prev_ub

        prev_st = supertrend[i - 1]
        prev_cl = close.iloc[i - 1]
        cl      = close.iloc[i]

        if pd.isna(prev_st) or prev_st == prev_ub:
            st  = ub if cl <= ub else lb
            d   = -1 if cl <= ub else 1
        else:
            st  = lb if cl >= lb else ub
            d   = 1  if cl >= lb else -1

        supertrend[i]     = st
        supertrend_dir[i] = d

    df["supertrend"]     = supertrend
    df["supertrend_dir"] = supertrend_dir

    return df