"""
fundamentals_extract_patch.py
─────────────────────────────────────────────────────────────────
This is a PATCH for your existing  etl/extract/fundamentals.py
(i.e. the file that contains  fetch_fundamentals()).

Paste the three blocks marked ── PASTE INTO fetch_fundamentals ──
into the matching positions inside your existing function.
They supply the new dict keys the v2 loader expects:

    Total Debt, Cash        →  used to derive EV
    Forward PE              →  stored as forward_pe
    earnings_growth_json    →  JSON multi-year net income trend
─────────────────────────────────────────────────────────────────
"""

import json
import math
from typing import Optional


def _safe_float(v) -> Optional[float]:
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return None


# ── PASTE INTO fetch_fundamentals  (after existing BS rows) ──────
def _add_ev_inputs(out: dict, bs, info: dict) -> None:
    """
    Add Total Debt and Cash to the output dict so the loader
    can compute EV = Market Cap + Total Debt − Cash.
    """
    # Total Debt — try balance sheet first, then .info
    total_debt = None
    if bs is not None and not bs.empty:
        for idx in bs.index:
            if str(idx).lower().strip() == "total debt":
                v = _safe_float(bs.loc[idx].dropna().iloc[0]
                                if not bs.loc[idx].dropna().empty else None)
                if v is not None:
                    total_debt = v
                    break
    if total_debt is None:
        total_debt = _safe_float(info.get("totalDebt"))

    # Cash — try balance sheet first, then .info
    cash = None
    if bs is not None and not bs.empty:
        for idx in bs.index:
            s = str(idx).lower().strip()
            if s in ("cash and cash equivalents", "cash equivalents",
                     "cash cash equivalents and short term investments"):
                v = _safe_float(bs.loc[idx].dropna().iloc[0]
                                if not bs.loc[idx].dropna().empty else None)
                if v is not None:
                    cash = v
                    break
    if cash is None:
        cash = _safe_float(info.get("totalCash"))

    if total_debt is not None:
        out["Total Debt"] = total_debt
    if cash is not None:
        out["Cash"] = cash


# ── PASTE INTO fetch_fundamentals  (after P/E block) ─────────────
def _add_forward_pe(out: dict, info: dict) -> None:
    """Add forward PE from yfinance .info."""
    fwd = _safe_float(info.get("forwardPE"))
    if fwd is not None and 0 < fwd < 500:   # sanity-guard
        out["Forward PE"] = round(fwd, 2)


# ── PASTE INTO fetch_fundamentals  (after TTM EPS block) ─────────
def _add_earnings_growth_json(out: dict, inc) -> None:
    """
    Build a JSON string of annual net-income values (newest → oldest)
    so the loader can persist a multi-period earnings trend without
    needing extra DB tables.

    Example output:
        '{"2024-03-31": 51234000000, "2023-03-31": 44321000000, ...}'
    """
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
        v = _safe_float(ni_row.get(col))
        if v is not None:
            trend[str(col)[:10]] = v   # "YYYY-MM-DD"

    if trend:
        out["earnings_growth_json"] = json.dumps(trend)
        out["earnings_growth"]      = trend          # raw dict for convenience


# ─────────────────────────────────────────────────────────────────
#  Minimal integration example
#  Replace the tail of your existing fetch_fundamentals() with:
# ─────────────────────────────────────────────────────────────────

def _apply_all_patches(out: dict, bs, cf, inc, info: dict) -> dict:
    """
    Call this once at the END of fetch_fundamentals(), passing the
    same objects you already have in scope.

    Example (inside fetch_fundamentals):

        # ... existing code ...
        _apply_all_patches(out, bs, cf, inc, info)
        return out
    """
    _add_ev_inputs(out, bs, info)
    _add_forward_pe(out, info)
    _add_earnings_growth_json(out, inc)
    return out