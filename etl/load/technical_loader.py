"""
technical_loader.py  —  v2
─────────────────────────────────────────────────────────────────
Fixes & additions vs v1:

  FIX-1  NULL warmup rows
         compute_technicals() now receives a FULL history df
         (5 years) so SMA-200 / BB have enough bars.
         The loader trims rows where close IS NULL before inserting.

  FIX-2  Column-name normalisation
         pandas_ta emits e.g. "BBM_20_2.0" / "BBU_20_2.0".
         A best-effort resolver maps those to our canonical names
         (bb_mid, bb_upper, bb_lower, etc.) so nothing goes NULL
         because of a column-naming mismatch.

  NEW-1  ADX (Average Directional Index, period=14)
  NEW-2  VWAP (rolling 14-day proxy; real intraday VWAP needs 1m data)
  NEW-3  OBV  (On-Balance Volume)
  NEW-4  Supertrend (period=10, multiplier=3 — most common defaults)

DB schema change required (run once):
  ALTER TABLE technical_indicators ADD COLUMN adx_14      REAL;
  ALTER TABLE technical_indicators ADD COLUMN vwap_14     REAL;
  ALTER TABLE technical_indicators ADD COLUMN obv         REAL;
  ALTER TABLE technical_indicators ADD COLUMN supertrend  REAL;
  ALTER TABLE technical_indicators ADD COLUMN supertrend_dir INTEGER;
  -- supertrend_dir: +1 = bullish (price above ST), -1 = bearish
"""

import math
import numpy as np
import pandas as pd
from database.db import get_connection


# ─────────────────────────────────────────────────────────────────
#  helpers
# ─────────────────────────────────────────────────────────────────

def _safe(v) -> float | None:
    try:
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    except Exception:
        return None


def _find_col(df: pd.DataFrame, *candidates: str) -> str | None:
    """Case-insensitive partial match — returns first hit."""
    cols_lower = {c.lower(): c for c in df.columns}
    for cand in candidates:
        cl = cand.lower()
        # exact first
        if cl in cols_lower:
            return cols_lower[cl]
        # partial
        for k, original in cols_lower.items():
            if cl in k:
                return original
    return None


# ─────────────────────────────────────────────────────────────────
#  Supertrend  (pure-numpy, no extra dep)
# ─────────────────────────────────────────────────────────────────

def _supertrend(high: pd.Series, low: pd.Series, close: pd.Series,
                period: int = 10, multiplier: float = 3.0
                ) -> tuple[pd.Series, pd.Series]:
    """
    Returns (supertrend_line, direction_series).
    direction: +1 = price above ST (bullish), -1 = bearish.
    """
    hl2 = (high + low) / 2

    # ATR via Wilder smoothing
    tr = pd.concat([
        high - low,
        (high - close.shift(1)).abs(),
        (low  - close.shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1 / period, adjust=False).mean()

    upper_band = hl2 + multiplier * atr
    lower_band = hl2 - multiplier * atr

    n = len(close)
    st        = np.full(n, np.nan)
    direction = np.zeros(n, dtype=int)

    for i in range(1, n):
        if math.isnan(upper_band.iloc[i]) or math.isnan(lower_band.iloc[i]):
            continue

        prev_upper = upper_band.iloc[i - 1] if not math.isnan(upper_band.iloc[i - 1]) else upper_band.iloc[i]
        prev_lower = lower_band.iloc[i - 1] if not math.isnan(lower_band.iloc[i - 1]) else lower_band.iloc[i]

        # Finalise bands (only tighten)
        final_upper = upper_band.iloc[i] if (
            upper_band.iloc[i] < prev_upper
            or close.iloc[i - 1] > prev_upper
        ) else prev_upper

        final_lower = lower_band.iloc[i] if (
            lower_band.iloc[i] > prev_lower
            or close.iloc[i - 1] < prev_lower
        ) else prev_lower

        # Determine trend
        if math.isnan(st[i - 1]):
            st[i] = final_upper if close.iloc[i] <= final_upper else final_lower
            direction[i] = -1 if close.iloc[i] <= final_upper else 1
        elif st[i - 1] == prev_upper:
            if close.iloc[i] <= final_upper:
                st[i]        = final_upper
                direction[i] = -1
            else:
                st[i]        = final_lower
                direction[i] =  1
        else:  # was on lower band (bullish)
            if close.iloc[i] >= final_lower:
                st[i]        = final_lower
                direction[i] =  1
            else:
                st[i]        = final_upper
                direction[i] = -1

    return pd.Series(st, index=close.index), pd.Series(direction, index=close.index)


# ─────────────────────────────────────────────────────────────────
#  VWAP proxy  (rolling N-day)
# ─────────────────────────────────────────────────────────────────

def _rolling_vwap(high: pd.Series, low: pd.Series,
                  close: pd.Series, volume: pd.Series,
                  window: int = 14) -> pd.Series:
    """
    Daily rolling VWAP = sum(typical_price * volume, window)
                        / sum(volume, window)
    This is a *proxy* — true intra-day VWAP needs tick/minute data.
    """
    tp = (high + low + close) / 3
    return (tp * volume).rolling(window).sum() / volume.rolling(window).sum()


# ─────────────────────────────────────────────────────────────────
#  OBV  (On-Balance Volume)
# ─────────────────────────────────────────────────────────────────

def _obv(close: pd.Series, volume: pd.Series) -> pd.Series:
    direction = np.sign(close.diff().fillna(0))
    return (direction * volume).cumsum()


# ─────────────────────────────────────────────────────────────────
#  compute_technicals  — called from pipeline EXTRACT step
# ─────────────────────────────────────────────────────────────────

def compute_technicals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute ALL technical indicators on a full OHLCV DataFrame.

    Input columns expected (case-insensitive):  Open, High, Low, Close, Volume
    Input index: DatetimeIndex  OR  'date' column.

    Returns a flat DataFrame with columns:
        date, close,
        rsi_14, macd, macd_signal, macd_hist,
        sma_50, sma_200, ema_21,
        bb_mid, bb_upper, bb_lower,
        atr_14,
        adx_14, vwap_14, obv,
        supertrend, supertrend_dir
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # ── normalise column names ────────────────────────────────
    df.columns = [c.strip() for c in df.columns]
    col_map = {c.lower(): c for c in df.columns}

    def _col(name: str) -> pd.Series:
        return df[col_map[name]]

    close  = _col("close")
    high   = _col("high")
    low    = _col("low")
    volume = _col("volume") if "volume" in col_map else pd.Series(0, index=df.index)

    # ── date column ───────────────────────────────────────────
    if "date" in col_map:
        dates = df[col_map["date"]].astype(str)
    else:
        dates = pd.Series(df.index.astype(str), index=df.index)

    out = pd.DataFrame(index=df.index)
    out["date"]  = dates
    out["close"] = close.values

    # ── try pandas_ta first ───────────────────────────────────
    try:
        import pandas_ta as ta  # type: ignore
        _df = df.copy()
        _df.columns = [c.capitalize() for c in _df.columns]   # pandas_ta wants Title-case
        if "Date" in _df.columns:
            _df = _df.set_index("Date")

        _df.ta.rsi(length=14, append=True)
        _df.ta.macd(fast=12, slow=26, signal=9, append=True)
        _df.ta.bbands(length=20, std=2, append=True)
        _df.ta.atr(length=14, append=True)
        _df.ta.sma(length=50,  append=True)
        _df.ta.sma(length=200, append=True)
        _df.ta.ema(length=21,  append=True)
        _df.ta.adx(length=14,  append=True)
        _df.ta.obv(append=True)

        def _ta(col): return _find_col(_df, col)

        out["rsi_14"]     = _df[_ta("RSI_14")].values     if _ta("RSI_14")     else np.nan
        out["macd"]       = _df[_ta("MACD_12")].values    if _ta("MACD_12")    else np.nan
        out["macd_signal"]= _df[_ta("MACDs_12")].values   if _ta("MACDs_12")   else np.nan
        out["macd_hist"]  = _df[_ta("MACDh_12")].values   if _ta("MACDh_12")   else np.nan
        out["sma_50"]     = _df[_ta("SMA_50")].values      if _ta("SMA_50")     else np.nan
        out["sma_200"]    = _df[_ta("SMA_200")].values     if _ta("SMA_200")    else np.nan
        out["ema_21"]     = _df[_ta("EMA_21")].values      if _ta("EMA_21")     else np.nan
        out["bb_mid"]     = _df[_ta("BBM_20")].values      if _ta("BBM_20")     else np.nan
        out["bb_upper"]   = _df[_ta("BBU_20")].values      if _ta("BBU_20")     else np.nan
        out["bb_lower"]   = _df[_ta("BBL_20")].values      if _ta("BBL_20")     else np.nan
        out["atr_14"]     = _df[_ta("ATRr_14")].values     if _ta("ATRr_14")    else _df[_ta("ATR_14")].values if _ta("ATR_14") else np.nan
        out["adx_14"]     = _df[_ta("ADX_14")].values      if _ta("ADX_14")     else np.nan
        out["obv"]        = _df[_ta("OBV")].values         if _ta("OBV")        else np.nan

    except Exception:
        # ── fallback: pure-pandas manual calculation ──────────
        delta = close.diff()
        gain  = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
        loss  = (-delta.clip(upper=0)).ewm(com=13, adjust=False).mean()
        out["rsi_14"] = (100 - 100 / (1 + gain / loss.replace(0, np.nan))).values

        e12 = close.ewm(span=12, adjust=False).mean()
        e26 = close.ewm(span=26, adjust=False).mean()
        macd_line         = e12 - e26
        macd_sig          = macd_line.ewm(span=9, adjust=False).mean()
        out["macd"]       = macd_line.values
        out["macd_signal"]= macd_sig.values
        out["macd_hist"]  = (macd_line - macd_sig).values

        out["sma_50"]  = close.rolling(50).mean().values
        out["sma_200"] = close.rolling(200).mean().values
        out["ema_21"]  = close.ewm(span=21, adjust=False).mean().values

        r20 = close.rolling(20)
        bb_m = r20.mean()
        bb_s = r20.std()
        out["bb_mid"]   = bb_m.values
        out["bb_upper"] = (bb_m + 2 * bb_s).values
        out["bb_lower"] = (bb_m - 2 * bb_s).values

        tr = pd.concat([
            high - low,
            (high - close.shift(1)).abs(),
            (low  - close.shift(1)).abs(),
        ], axis=1).max(axis=1)
        out["atr_14"] = tr.rolling(14).mean().values

        # ADX manual
        up_move   = high.diff()
        down_move = -low.diff()
        plus_dm   = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
        minus_dm  = down_move.where((down_move > up_move) & (down_move > 0), 0.0)
        atr14     = tr.ewm(alpha=1/14, adjust=False).mean()
        plus_di   = 100 * plus_dm.ewm(alpha=1/14, adjust=False).mean()  / atr14.replace(0, np.nan)
        minus_di  = 100 * minus_dm.ewm(alpha=1/14, adjust=False).mean() / atr14.replace(0, np.nan)
        dx        = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
        out["adx_14"] = dx.ewm(alpha=1/14, adjust=False).mean().values

        # OBV manual
        out["obv"] = _obv(close, volume).values

    # ── VWAP (rolling 14-day, always manual) ─────────────────
    out["vwap_14"] = _rolling_vwap(high, low, close, volume, window=14).values

    # ── Supertrend (always manual for precision) ──────────────
    st_line, st_dir = _supertrend(high, low, close, period=10, multiplier=3.0)
    out["supertrend"]     = st_line.values
    out["supertrend_dir"] = st_dir.values.astype(float)
    # Store 0 as NULL-equivalent for warmup rows where ST not computed
    out["supertrend_dir"] = out["supertrend_dir"].where(out["supertrend_dir"] != 0, other=np.nan)

    return out


# ─────────────────────────────────────────────────────────────────
#  load_technicals  — loader called from pipeline
# ─────────────────────────────────────────────────────────────────

def load_technicals(df: pd.DataFrame, symbol: str):
    """
    Load all technical indicators (incl. new ADX/VWAP/OBV/Supertrend).
    Skips warmup rows where close IS NULL.

    Requires schema migration before first use — see module docstring.
    """
    if df is None or df.empty:
        print("  ⚠  technical_indicators: empty DataFrame, skipping")
        return

    conn = get_connection()

    # ── ensure new columns exist (idempotent migrations) ─────
    _migrations = [
        "ALTER TABLE technical_indicators ADD COLUMN adx_14      REAL",
        "ALTER TABLE technical_indicators ADD COLUMN vwap_14     REAL",
        "ALTER TABLE technical_indicators ADD COLUMN obv         REAL",
        "ALTER TABLE technical_indicators ADD COLUMN supertrend  REAL",
        "ALTER TABLE technical_indicators ADD COLUMN supertrend_dir INTEGER",
    ]
    for sql in _migrations:
        try:
            conn.execute(sql)
        except Exception:
            pass   # column already exists → ignore
    conn.commit()

    rows = []
    for _, row in df.iterrows():
        close_val = _safe(row.get("close"))
        if close_val is None:          # skip warmup / bad rows
            continue

        def g(col: str) -> float | None:
            return _safe(row.get(col))

        rows.append((
            symbol,
            str(row["date"]),
            close_val,
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
            g("adx_14"),
            g("vwap_14"),
            g("obv"),
            g("supertrend"),
            g("supertrend_dir"),
        ))

    conn.executemany("""
        INSERT OR REPLACE INTO technical_indicators
            (symbol, date, close,
             rsi_14, macd, macd_signal, macd_hist,
             sma_50, sma_200, ema_21,
             bb_mid, bb_upper, bb_lower,
             atr_14,
             adx_14, vwap_14, obv,
             supertrend, supertrend_dir)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()

    null_count = sum(
        1 for r in rows
        if r[7] is None  # sma_50
    )
    print(
        f"  ✅ technical_indicators: {len(rows)} rows upserted"
        f"  (warmup NULLs in sma_50: {null_count})"
    )