"""
etl/extract/fundamentals_extract_patch.py  v3.0
────────────────────────────────────────────────────────────────
Fixes vs v2:
  • EV inputs already converted to Crores by fundamentals.py,
    so patch just wires them together cleanly
  • earnings_growth_json stores values in Rs. Crores
  • All None-guards tightened so EV/EBITDA/EV/Revenue never NULL
    when market cap + debt + cash are all present
────────────────────────────────────────────────────────────────
"""

import json
import math
from typing import Optional

_CR = 1e7


def _safe_float(v) -> Optional[float]:
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return None


def _cr(v) -> Optional[float]:
    f = _safe_float(v)
    return round(f / _CR, 2) if f is not None else None


# ── EV inputs (Crores) ───────────────────────────────────────
_DEBT_LABELS = {
    "total debt", "long term debt", "long_term_debt",
    "current debt", "short long term debt",
}
_CASH_LABELS = {
    "cash and cash equivalents", "cash equivalents",
    "cash cash equivalents and short term investments",
    "cash and short term investments",
}


def _bs_first(bs, label_set: set) -> Optional[float]:
    if bs is None or bs.empty:
        return None
    for idx in bs.index:
        if str(idx).lower().strip() in label_set:
            col = bs.loc[idx].dropna()
            if not col.empty:
                return _cr(col.iloc[0])
    return None


def _add_ev_inputs(out: dict, bs, info: dict) -> None:
    """
    Ensure Total Debt, Cash, EV, EV/EBITDA, EV/Revenue are set.
    All values in Rs. Crores.
    fundamentals.py already computes these; this is a safety net.
    """
    if out.get("Total Debt") is None:
        v = _bs_first(bs, _DEBT_LABELS)
        if v is None:
            v = _cr(info.get("totalDebt"))
        if v is not None:
            out["Total Debt"] = v

    if out.get("Cash") is None:
        v = _bs_first(bs, _CASH_LABELS)
        if v is None:
            v = _cr(info.get("totalCash"))
        if v is not None:
            out["Cash"] = v

    mc      = out.get("Market Cap")
    debt    = out.get("Total Debt")
    cash    = out.get("Cash")
    ebitda  = out.get("EBITDA")
    revenue = out.get("Revenue")

    # Fallback: get from info if still missing
    if mc is None:
        mc = _cr(info.get("marketCap"))
        if mc is not None:
            out["Market Cap"] = mc

    if mc is not None and debt is not None and cash is not None:
        ev = round(mc + debt - cash, 2)
        out["EV"] = ev
        if ebitda and ebitda > 0:
            out["EV/EBITDA"] = round(ev / ebitda, 2)
        if revenue and revenue > 0:
            out["EV/Revenue"] = round(ev / revenue, 2)


# ── Forward PE ───────────────────────────────────────────────
def _add_forward_pe(out: dict, info: dict) -> None:
    if out.get("Forward PE") is not None:
        return
    fwd = _safe_float(info.get("forwardPE"))
    if fwd is not None and 0 < fwd < 500:
        out["Forward PE"] = round(fwd, 2)


# ── Earnings growth JSON (Rs. Crores) ────────────────────────
def _add_earnings_growth_json(out: dict, inc) -> None:
    """
    JSON of annual net-income in Rs. Crores, newest→oldest.
    Example: {"2025-03-31": 11092.31, "2024-03-31": 8110.64}
    """
    if out.get("earnings_growth_json") is not None:
        return    # already set by fundamentals.py
    if inc is None or inc.empty:
        return

    ni_row = None
    for idx in inc.index:
        if str(idx).lower().strip() in ("net income",
                                        "net income common stockholders"):
            ni_row = inc.loc[idx]
            break
    if ni_row is None:
        for idx in inc.index:
            if "net income" in str(idx).lower():
                ni_row = inc.loc[idx]
                break
    if ni_row is None:
        return

    trend = {}
    for col in inc.columns:
        v = _cr(_safe_float(ni_row.get(col)))
        if v is not None:
            trend[str(col)[:10]] = v

    if trend:
        out["earnings_growth_json"] = json.dumps(trend)
        out["earnings_growth"]      = trend


# ── Master patch caller ───────────────────────────────────────
def _apply_all_patches(out: dict, bs, cf, inc, info: dict) -> dict:
    _add_ev_inputs(out, bs, info)
    _add_forward_pe(out, info)
    _add_earnings_growth_json(out, inc)
    return out