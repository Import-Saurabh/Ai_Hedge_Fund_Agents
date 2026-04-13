import math
import json
import yfinance as yf
import pandas as pd
from typing import Optional, Dict
from datetime import date


def _safe_float(v) -> Optional[float]:
    try:
        fv = float(v)
        return None if math.isnan(fv) else fv
    except:
        return None


def _cagr(end, start, years) -> Optional[float]:
    if not end or not start or years <= 0:
        return None
    if start < 0 or end < 0:
        return None
    try:
        return ((end / start) ** (1 / years) - 1) * 100
    except:
        return None


def _yoy_series(df, *candidates) -> Dict[str, float]:
    if df is None or df.empty:
        return {}
    for df_idx in df.index:
        for c in candidates:
            if c.lower() in str(df_idx).lower():
                row = df.loc[df_idx].dropna()
                if not row.empty:
                    return {str(k)[:10]: float(v) for k, v in row.items()}
    return {}


def _yoy_growth_json(series: Dict[str, float]) -> str:
    """Build [{year, value, yoy_pct}, ...] JSON array."""
    vals = list(series.values())
    keys = list(series.keys())
    records = []
    for i, (k, v) in enumerate(zip(keys, vals)):
        yoy = None
        if i + 1 < len(vals) and vals[i + 1] and vals[i + 1] != 0:
            yoy = round((v / vals[i + 1] - 1) * 100, 2)
        records.append({"year": k, "value": v, "yoy_pct": yoy})
    return json.dumps(records)


def _compute_gross_margin_safe(inc, col_idx=0):
    """Dual-method gross margin (simplified import-free version)."""
    if inc is None or inc.empty:
        return None
    rev = None
    for idx in inc.index:
        if str(idx).lower().strip() == "total revenue":
            try:
                v = float(inc.loc[idx].iloc[col_idx])
                if not math.isnan(v) and v > 0:
                    rev = v; break
            except:
                pass
    if not rev:
        for idx in inc.index:
            if "total revenue" in str(idx).lower() or "revenue" == str(idx).lower().strip():
                try:
                    v = float(inc.loc[idx].iloc[col_idx])
                    if not math.isnan(v) and v > 0:
                        rev = v; break
                except:
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
            except:
                pass
    for idx in inc.index:
        if "cost of revenue" in str(idx).lower() or "reconciled cost" in str(idx).lower():
            try:
                cogs = float(inc.loc[idx].iloc[col_idx])
                if not math.isnan(cogs) and cogs > 0:
                    gm = (1 - cogs / rev) * 100
                    if 0 <= gm <= 100:
                        return round(gm, 2)
            except:
                pass
    return None


def fetch_growth_metrics(symbol: str) -> dict:
    """Compute growth CAGRs + YoY trends (mirrors test.py Section J)."""
    t = yf.Ticker(symbol)
    try:
        inc  = t.income_stmt
        cf   = t.cash_flow
        info = t.info or {}
    except:
        return {}

    today  = date.today().isoformat()
    shares = info.get("sharesOutstanding")

    rev  = _yoy_series(inc, "Total Revenue", "Revenue")
    ni   = _yoy_series(inc, "Net Income", "Net Income Common Stockholders")
    eb   = _yoy_series(inc, "EBITDA", "Normalized EBITDA")
    op_r = _yoy_series(cf, "Operating Cash Flow",
                       "Net Cash Provided By Operating Activities")
    cx_r = _yoy_series(cf, "Capital Expenditure",
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
        fcf_series[yr] = ocf - cap

    # Gross margin trend
    gm_trend = []
    if inc is not None and not inc.empty:
        for i, col in enumerate(inc.columns):
            gm = _compute_gross_margin_safe(inc, col_idx=i)
            if gm is not None:
                gm_trend.append({"year": str(col)[:10], "gross_margin_pct": gm})

    return {
        "as_of_date":               today,
        "revenue_cagr_3y":          round(series_cagr(rev), 2) if series_cagr(rev) else None,
        "net_profit_cagr_3y":       round(series_cagr(ni), 2)  if series_cagr(ni)  else None,
        "ebitda_cagr_3y":           round(series_cagr(eb), 2)  if series_cagr(eb)  else None,
        "eps_cagr_3y":              round(series_cagr(ni), 2)  if series_cagr(ni)  else None,
        "fcf_cagr_3y":              round(series_cagr(fcf_series), 2)
                                    if fcf_series and series_cagr(fcf_series) else None,
        "revenue_yoy_json":         _yoy_growth_json(rev)        if rev        else None,
        "net_income_yoy_json":      _yoy_growth_json(ni)         if ni         else None,
        "ebitda_yoy_json":          _yoy_growth_json(eb)         if eb         else None,
        "fcf_yoy_json":             _yoy_growth_json(fcf_series) if fcf_series else None,
        "gross_margin_trend_json":  json.dumps(gm_trend)         if gm_trend   else None,
    }
