"""
etl/extract/growth.py  v3.0
────────────────────────────────────────────────────────────────
Fixes vs v2:
  • YoY JSON uses key "value_cr" (not "value") consistently
    so downstream consumers can distinguish Crore values
  • _yoy_series_cr always divides by _CR — no double-conversion
  • gross_margin_trend computed per column (not just col 0)
────────────────────────────────────────────────────────────────
"""

import math
import json
import yfinance as yf
import pandas as pd
from typing import Optional, Dict
from datetime import date

_CR = 1e7   # 1 Crore


def _safe_float(v) -> Optional[float]:
    try:
        fv = float(v)
        return None if math.isnan(fv) else fv
    except Exception:
        return None


def _cr(v) -> Optional[float]:
    f = _safe_float(v)
    return round(f / _CR, 2) if f is not None else None


def _cagr(end, start, years) -> Optional[float]:
    if not end or not start or years <= 0:
        return None
    if start < 0 or end < 0:
        return None
    try:
        return round(((end / start) ** (1 / years) - 1) * 100, 2)
    except Exception:
        return None


def _yoy_series_cr(df, *candidates) -> Dict[str, float]:
    """Extract a metric row as {date_str: crore_value} dict."""
    if df is None or df.empty:
        return {}
    for df_idx in df.index:
        for c in candidates:
            if c.lower() in str(df_idx).lower():
                row = df.loc[df_idx].dropna()
                if not row.empty:
                    result = {}
                    for k, v in row.items():
                        cv = _cr(v)
                        if cv is not None:
                            result[str(k)[:10]] = cv
                    return result
    return {}


def _yoy_growth_json(series: Dict[str, float]) -> str:
    """Build [{year, value_cr, yoy_pct}, ...] JSON. Newest → oldest."""
    vals = list(series.values())
    keys = list(series.keys())
    records = []
    for i, (k, v) in enumerate(zip(keys, vals)):
        yoy = None
        if i + 1 < len(vals) and vals[i + 1] and vals[i + 1] != 0:
            yoy = round((v / vals[i + 1] - 1) * 100, 2)
        records.append({"year": k, "value_cr": v, "yoy_pct": yoy})
    return json.dumps(records)


def _compute_gross_margin_safe(inc, col_idx=0):
    if inc is None or inc.empty:
        return None
    rev = None
    for idx in inc.index:
        if str(idx).lower().strip() == "total revenue":
            try:
                v = float(inc.loc[idx].iloc[col_idx])
                if not math.isnan(v) and v > 0:
                    rev = v; break
            except Exception:
                pass
    if not rev:
        for idx in inc.index:
            if "total revenue" in str(idx).lower() or "revenue" == str(idx).lower().strip():
                try:
                    v = float(inc.loc[idx].iloc[col_idx])
                    if not math.isnan(v) and v > 0:
                        rev = v; break
                except Exception:
                    pass
    if not rev:
        return None
    for idx in inc.index:
        if "gross profit" in str(idx).lower() and "margin" not in str(idx).lower():
            try:
                gp = float(inc.loc[idx].iloc[col_idx])
                if not math.isnan(gp):
                    gm = gp / rev * 100
                    if 0 <= gm <= 100:
                        return round(gm, 2)
            except Exception:
                pass
    for idx in inc.index:
        if "cost of revenue" in str(idx).lower() or "reconciled cost" in str(idx).lower():
            try:
                cogs = float(inc.loc[idx].iloc[col_idx])
                if not math.isnan(cogs) and cogs > 0:
                    gm = (1 - cogs / rev) * 100
                    if 0 <= gm <= 100:
                        return round(gm, 2)
            except Exception:
                pass
    return None


def fetch_growth_metrics(symbol: str) -> dict:
    """
    Compute growth CAGRs + YoY trends.
    All monetary JSON values in Rs. Crores.
    CAGR % values are unit-agnostic (ratios).
    """
    t = yf.Ticker(symbol)
    try:
        inc  = t.income_stmt
        cf   = t.cash_flow
        info = t.info or {}
    except Exception:
        return {}

    today = date.today().isoformat()

    rev  = _yoy_series_cr(inc, "Total Revenue", "Revenue")
    ni   = _yoy_series_cr(inc, "Net Income", "Net Income Common Stockholders")
    eb   = _yoy_series_cr(inc, "EBITDA", "Normalized EBITDA")
    op_r = _yoy_series_cr(cf, "Operating Cash Flow",
                          "Net Cash Provided By Operating Activities")
    cx_r = _yoy_series_cr(cf, "Capital Expenditure",
                          "Purchase Of Property Plant And Equipment",
                          "Purchases Of Property Plant And Equipment")

    def series_cagr(s):
        vals = list(s.values())
        n = len(vals) - 1
        if n <= 0:
            return None
        return _cagr(vals[0], vals[-1], n)

    fcf_series = {}
    for yr in op_r:
        ocf = op_r[yr]
        cap = abs(cx_r.get(yr, 0)) if yr in cx_r else 0
        fcf_series[yr] = round(ocf - cap, 2)

    # Gross margin trend (per column)
    gm_trend = []
    if inc is not None and not inc.empty:
        for i in range(len(inc.columns)):
            col = inc.columns[i]
            gm  = _compute_gross_margin_safe(inc, col_idx=i)
            if gm is not None:
                gm_trend.append({"year": str(col)[:10], "gross_margin_pct": gm})

    def safe_round(v):
        return round(v, 2) if v is not None else None

    return {
        "as_of_date":              today,
        "revenue_cagr_3y":         safe_round(series_cagr(rev)),
        "net_profit_cagr_3y":      safe_round(series_cagr(ni)),
        "ebitda_cagr_3y":          safe_round(series_cagr(eb)),
        "eps_cagr_3y":             safe_round(series_cagr(ni)),
        "fcf_cagr_3y":             safe_round(series_cagr(fcf_series)) if fcf_series else None,
        "revenue_yoy_json":        _yoy_growth_json(rev)        if rev        else None,
        "net_income_yoy_json":     _yoy_growth_json(ni)         if ni         else None,
        "ebitda_yoy_json":         _yoy_growth_json(eb)         if eb         else None,
        "fcf_yoy_json":            _yoy_growth_json(fcf_series) if fcf_series else None,
        "gross_margin_trend_json": json.dumps(gm_trend)         if gm_trend   else None,
        "unit":                    "Rs_Crores",
    }