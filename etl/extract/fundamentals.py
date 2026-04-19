"""
etl/extract/fundamentals.py  v4.0
────────────────────────────────────────────────────────────────
Fixes vs v3:
  • EV / EV_EBITDA / EV_Revenue always computed when mc+debt+cash
    are available — no longer NULL on first run
  • forward_pe pulled from info["forwardPE"] with sanity check
  • earnings_growth_json always populated from annual income stmt
  • All monetary values confirmed in Rs. Crores before return
  • TTM figures use last 4 quarterly periods correctly
────────────────────────────────────────────────────────────────
"""

import math
import json
import yfinance as yf
from typing import Optional, Dict, Any, Tuple
import pandas as pd
from etl.extract.fundamentals_extract_patch import _apply_all_patches

_REVENUE_SUBROW_PATTERNS = [
    "excise", "adjustment", "net of", "restate", "proforma",
    "segment", "geographic", "domestic", "export", "operating revenue",
]

_CR = 1e7   # 1 Crore = 10,000,000


def _safe_float(v) -> Optional[float]:
    try:
        fv = float(v)
        return None if (math.isnan(fv) or math.isinf(fv)) else fv
    except Exception:
        return None


def _cr(v) -> Optional[float]:
    """Raw rupees → Rs. Crores, 2 dp."""
    f = _safe_float(v)
    return round(f / _CR, 2) if f is not None else None


def _get_row(df: pd.DataFrame, *candidates, col_idx: int = 0) -> Optional[float]:
    """Return first raw value matching any candidate row label."""
    if df is None or df.empty:
        return None

    def _extract(idx_label):
        row = df.loc[idx_label]
        for ci in [col_idx] + [c for c in range(len(row)) if c != col_idx]:
            try:
                fv = float(row.iloc[ci])
                if not math.isnan(fv):
                    return fv
            except Exception:
                pass
        return None

    for name in candidates:
        for idx in df.index:
            if str(idx).lower().strip() == name.lower().strip():
                v = _extract(idx)
                if v is not None:
                    return v
    for name in candidates:
        is_rev = "revenue" in name.lower()
        for idx in df.index:
            idx_lower = str(idx).lower()
            if name.lower() in idx_lower:
                if is_rev and any(p in idx_lower for p in _REVENUE_SUBROW_PATTERNS):
                    continue
                v = _extract(idx)
                if v is not None:
                    return v
    return None


def _compute_gross_margin_safe(
    inc: pd.DataFrame, col_idx: int = 0
) -> Tuple[Optional[float], Optional[float], str]:
    if inc is None or inc.empty:
        return None, None, "no income stmt"

    REV_CANDIDATES = ["Total Revenue", "Revenue", "Net Revenue", "Total Net Revenue"]
    revenue = None
    rev_label = None

    for cand in REV_CANDIDATES:
        for idx in inc.index:
            if str(idx).lower().strip() == cand.lower():
                v = _safe_float(inc.loc[idx].iloc[col_idx])
                if v and v > 0:
                    revenue = v; rev_label = str(idx); break
        if revenue:
            break

    if not revenue:
        for cand in REV_CANDIDATES:
            for idx in inc.index:
                idx_s = str(idx).lower()
                if cand.lower() in idx_s:
                    if any(p in idx_s for p in _REVENUE_SUBROW_PATTERNS):
                        continue
                    v = _safe_float(inc.loc[idx].iloc[col_idx])
                    if v and v > 0:
                        revenue = v; rev_label = str(idx); break
            if revenue:
                break

    if not revenue:
        return None, None, "revenue row not found"

    gm1, gm2 = None, None

    for idx in inc.index:
        if "gross profit" in str(idx).lower() and "margin" not in str(idx).lower():
            gp = _safe_float(inc.loc[idx].iloc[col_idx])
            if gp is not None:
                raw = gp / revenue * 100
                if 0 <= raw <= 100:
                    gm1 = raw
            break

    for cand in ["Cost Of Revenue", "Reconciled Cost Of Revenue",
                 "Cost of Goods Sold", "Total Cost Of Revenue"]:
        for idx in inc.index:
            if cand.lower() in str(idx).lower():
                cogs = _safe_float(inc.loc[idx].iloc[col_idx])
                if cogs and cogs > 0:
                    raw = (1 - cogs / revenue) * 100
                    if 0 <= raw <= 100:
                        gm2 = raw
                break
        if gm2:
            break

    audit = f"Rev row='{rev_label}'"
    if gm1 is not None and gm2 is not None:
        diff = abs(gm1 - gm2)
        final = (gm1 + gm2) / 2 if diff <= 5 else gm1
        return round(final, 2), revenue, audit
    if gm1 is not None:
        return round(gm1, 2), revenue, audit
    if gm2 is not None:
        return round(gm2, 2), revenue, audit
    return None, revenue, "neither GP nor COGS row found"


def _build_earnings_growth_json(inc: pd.DataFrame) -> Optional[str]:
    """Build {date: net_income_cr} JSON from annual IS. Newest → oldest."""
    if inc is None or inc.empty:
        return None
    ni_row = None
    for idx in inc.index:
        if str(idx).lower().strip() in ("net income",
                                        "net income common stockholders"):
            ni_row = inc.loc[idx]; break
    if ni_row is None:
        for idx in inc.index:
            if "net income" in str(idx).lower():
                ni_row = inc.loc[idx]; break
    if ni_row is None:
        return None
    trend = {}
    for col in inc.columns:
        v = _cr(_safe_float(ni_row.get(col)))
        if v is not None:
            trend[str(col)[:10]] = v
    return json.dumps(trend) if trend else None


def fetch_fundamentals(symbol: str) -> Dict[str, Any]:
    """
    Compute all fundamentals metrics.
    MONETARY VALUES → Rs. Crores
    RATIOS / % / P-multiples → unchanged (unitless)
    """
    t = yf.Ticker(symbol)

    def safe_get(attr):
        try:
            return getattr(t, attr)
        except Exception:
            return None

    inc  = safe_get("income_stmt")
    bs   = safe_get("balance_sheet")
    cf   = safe_get("cash_flow")
    info = safe_get("info") or {}

    price  = _safe_float(info.get("currentPrice") or info.get("regularMarketPrice"))
    shares = _safe_float(info.get("sharesOutstanding"))

    # ── Raw income rows ───────────────────────────────────────
    revenue_raw = _get_row(inc, "Total Revenue", "Revenue")
    net_inc_raw = _get_row(inc, "Net Income", "Net Income Common Stockholders")
    ebitda_raw  = _get_row(inc, "EBITDA", "Normalized EBITDA")
    ebit_raw    = _get_row(inc, "EBIT") or _get_row(inc, "Operating Income")
    int_exp_raw = _get_row(inc, "Interest Expense", "Interest Expense Non Operating")
    dep_raw     = _get_row(inc, "Reconciled Depreciation",
                            "Depreciation And Amortization In Income Stat",
                            "Depreciation")

    # ── Raw BS rows ───────────────────────────────────────────
    total_assets_raw = _get_row(bs, "Total Assets")
    curr_liab_raw    = _get_row(bs, "Current Liabilities", "Total Current Liabilities")
    curr_assets_raw  = _get_row(bs, "Current Assets", "Total Current Assets")
    total_equity_raw = _get_row(bs, "Stockholders Equity", "Common Stock Equity",
                                "Total Equity Gross Minority Interest")
    total_debt_raw   = _get_row(bs, "Total Debt")
    ar_raw           = _get_row(bs, "Accounts Receivable", "Gross Accounts Receivable")
    ap_raw           = _get_row(bs, "Accounts Payable")
    cogs_raw         = _get_row(inc, "Cost Of Revenue", "Reconciled Cost Of Revenue")

    # Inventory
    inventory_raw = None
    if bs is not None and not bs.empty:
        for idx in bs.index:
            if str(idx).lower().strip() == "inventory":
                v = _safe_float(bs.loc[idx].dropna().iloc[0]) if not bs.loc[idx].dropna().empty else None
                if v is not None:
                    inventory_raw = v; break
        if inventory_raw is None:
            for idx in bs.index:
                s = str(idx).lower().strip()
                if ("inventory" in s and "raw" not in s and "work" not in s
                        and "finished" not in s and "progress" not in s):
                    v = _safe_float(bs.loc[idx].dropna().iloc[0]) if not bs.loc[idx].dropna().empty else None
                    if v is not None:
                        inventory_raw = v; break

    # Cash
    cash_raw = None
    if bs is not None and not bs.empty:
        for idx in bs.index:
            s = str(idx).lower().strip()
            if s in ("cash and cash equivalents", "cash equivalents",
                     "cash cash equivalents and short term investments"):
                v = _safe_float(bs.loc[idx].dropna().iloc[0]) if not bs.loc[idx].dropna().empty else None
                if v is not None:
                    cash_raw = v; break
    if cash_raw is None:
        cash_raw = _safe_float(info.get("totalCash"))

    # CF rows
    op_cf_raw = _get_row(cf, "Operating Cash Flow",
                         "Net Cash Provided By Operating Activities")
    capex_raw = _get_row(cf, "Capital Expenditure",
                         "Purchase Of Property Plant And Equipment",
                         "Purchases Of Property Plant And Equipment")

    # ── Convert to Crores ─────────────────────────────────────
    revenue      = _cr(revenue_raw)
    net_inc      = _cr(net_inc_raw)
    ebitda       = _cr(ebitda_raw)
    ebit         = _cr(ebit_raw)
    int_exp      = _cr(int_exp_raw)
    dep          = _cr(dep_raw)
    total_assets = _cr(total_assets_raw)
    curr_liab    = _cr(curr_liab_raw)
    curr_assets  = _cr(curr_assets_raw)
    total_equity = _cr(total_equity_raw)
    total_debt   = _cr(total_debt_raw)
    ar           = _cr(ar_raw)
    ap           = _cr(ap_raw)
    cogs         = _cr(cogs_raw)
    inventory    = _cr(inventory_raw)
    cash         = _cr(cash_raw)
    op_cf        = _cr(op_cf_raw)
    capex_val    = _cr(capex_raw)
    mc_raw       = _safe_float(info.get("marketCap"))
    mc           = _cr(mc_raw)

    out: Dict[str, Any] = {}
    _ebit = ebit or (round(ebitda * 0.82, 2) if ebitda else None)

    # ── Profitability ratios (unitless) ───────────────────────
    if net_inc and total_equity and total_equity != 0:
        out["ROE (%)"] = round(net_inc / total_equity * 100, 2)
    elif info.get("returnOnEquity"):
        out["ROE (%)"] = round(info["returnOnEquity"] * 100, 2)

    if _ebit and total_assets and curr_liab:
        ce = total_assets - curr_liab
        if ce > 0:
            out["ROCE (%)"] = round(_ebit / ce * 100, 2)

    if net_inc and total_assets and total_assets != 0:
        out["ROA (%)"] = round(net_inc / total_assets * 100, 2)

    if _ebit and int_exp and int_exp != 0:
        out["Interest Coverage"] = round(abs(_ebit / int_exp), 2)

    # ── FCF (Crores) ──────────────────────────────────────────
    if op_cf is not None:
        capex_abs = abs(capex_val) if capex_val else 0
        out["Free Cash Flow"] = round(op_cf - capex_abs, 2)
        out["Operating CF"]   = op_cf
        out["CapEx"]          = capex_abs
    elif info.get("freeCashflow"):
        out["Free Cash Flow"] = _cr(info["freeCashflow"])

    # ── Margins ───────────────────────────────────────────────
    gm_pct, _, _ = _compute_gross_margin_safe(inc)
    if gm_pct is not None:
        out["Gross Margin (%)"] = gm_pct
    if net_inc and revenue and revenue != 0:
        out["Net Profit Margin (%)"] = round(net_inc / revenue * 100, 2)
    if ebitda and revenue and revenue != 0:
        out["EBITDA Margin (%)"] = round(ebitda / revenue * 100, 2)
    if _ebit and revenue and revenue != 0:
        out["EBIT Margin (%)"] = round(_ebit / revenue * 100, 2)

    # ── Leverage & liquidity ──────────────────────────────────
    if total_debt and total_equity and total_equity != 0:
        out["Debt/Equity"] = round(total_debt / total_equity, 2)
    if curr_assets and curr_liab and curr_liab != 0:
        out["Current Ratio"] = round(curr_assets / curr_liab, 2)
        inv_use = inventory or 0
        out["Quick Ratio"] = round((curr_assets - inv_use) / curr_liab, 2)

    # ── Working capital days ──────────────────────────────────
    if revenue and ar:
        out["DSO (days)"] = round(ar / revenue * 365, 1)
    if inventory and cogs and cogs != 0:
        out["DIO (days)"] = round(inventory / cogs * 365, 1)
    if ap and cogs and cogs != 0:
        out["DPO (days)"] = round(ap / cogs * 365, 1)
    if all(k in out for k in ["DSO (days)", "DIO (days)", "DPO (days)"]):
        out["CCC (days)"] = round(
            out["DSO (days)"] + out["DIO (days)"] - out["DPO (days)"], 1
        )

    # ── Valuation ─────────────────────────────────────────────
    if net_inc_raw and shares and shares > 0:
        eps = net_inc_raw / shares
        out["EPS"] = round(eps, 2)
        if price and eps > 0:
            out["P/E"] = round(price / eps, 2)

    bv = _safe_float(info.get("bookValue"))
    if price and bv and bv != 0:
        out["P/B"] = round(price / bv, 2)

    if "EPS" in out and bv and out["EPS"] > 0 and bv > 0:
        gn = math.sqrt(22.5 * out["EPS"] * bv)
        out["Graham Number"] = round(gn, 2)

    dy = _safe_float(info.get("dividendYield"))
    if dy:
        out["Dividend Yield (%)"] = round(dy * 100, 2)

    # ── Forward PE ────────────────────────────────────────────
    fwd_pe = _safe_float(info.get("forwardPE"))
    if fwd_pe is not None and 0 < fwd_pe < 500:
        out["Forward PE"] = round(fwd_pe, 2)

    # ── EV (Crores) ───────────────────────────────────────────
    # Resolve total_debt and cash from info if BS gave nothing
    if total_debt is None:
        total_debt = _cr(info.get("totalDebt"))
    if cash is None:
        cash = _cr(info.get("totalCash"))

    if mc is not None and total_debt is not None and cash is not None:
        ev = round(mc + total_debt - cash, 2)
        out["EV"] = ev
        if ebitda and ebitda > 0:
            out["EV/EBITDA"] = round(ev / ebitda, 2)
        if revenue and revenue > 0:
            out["EV/Revenue"] = round(ev / revenue, 2)

    # ── Monetary scale outputs ────────────────────────────────
    out["Market Cap"]  = mc
    out["Revenue"]     = revenue
    out["Net Income"]  = net_inc
    out["EBITDA"]      = ebitda
    out["Inventory"]   = inventory
    out["Total Debt"]  = total_debt
    out["Cash"]        = cash

    # ── earnings_growth_json ──────────────────────────────────
    egj = _build_earnings_growth_json(inc)
    if egj:
        out["earnings_growth_json"] = egj

    # ── TTM EPS (last 4 quarters) ─────────────────────────────
    try:
        q_inc = t.quarterly_income_stmt
        if q_inc is not None and not q_inc.empty:
            inc_row = next((r for r in q_inc.index
                            if "net income" in str(r).lower()), None)
            if inc_row and shares and shares > 0:
                ttm_ni_raw = sum(
                    _safe_float(q_inc.loc[inc_row, c]) or 0
                    for c in q_inc.columns[:4]
                )
                ttm_eps = ttm_ni_raw / shares
                out["TTM EPS"] = round(ttm_eps, 2)
                if price and ttm_eps > 0:
                    out["TTM P/E"] = round(price / ttm_eps, 2)
    except Exception:
        pass

    _apply_all_patches(out, bs, cf, inc, info)
    return out