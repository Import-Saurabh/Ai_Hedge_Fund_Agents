import math
import yfinance as yf
from typing import Optional, Dict, Any, Tuple
import pandas as pd


# ── Revenue sub-row patterns to skip ─────────────────────────
_REVENUE_SUBROW_PATTERNS = [
    "excise", "adjustment", "net of", "restate", "proforma",
    "segment", "geographic", "domestic", "export", "operating revenue",
]


def _safe_float(v) -> Optional[float]:
    try:
        fv = float(v)
        return None if math.isnan(fv) else fv
    except:
        return None


def _get_row(df: pd.DataFrame, *candidates, col_idx: int = 0) -> Optional[float]:
    if df is None or df.empty:
        return None

    def _extract(idx_label):
        row = df.loc[idx_label]
        for ci in [col_idx] + [c for c in range(len(row)) if c != col_idx]:
            try:
                fv = float(row.iloc[ci])
                if not math.isnan(fv):
                    return fv
            except:
                pass
        return None

    # Exact match first
    for name in candidates:
        for idx in df.index:
            if str(idx).lower().strip() == name.lower().strip():
                v = _extract(idx)
                if v is not None:
                    return v

    # Partial match
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
    """Dual-method gross margin validation (FIX-C)."""
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

    # Method 1: GP / Revenue
    for idx in inc.index:
        if "gross profit" in str(idx).lower() and "margin" not in str(idx).lower():
            gp = _safe_float(inc.loc[idx].iloc[col_idx])
            if gp is not None:
                raw = gp / revenue * 100
                if 0 <= raw <= 100:
                    gm1 = raw
                break

    # Method 2: 1 - COGS/Revenue
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
        if diff <= 5:
            final = (gm1 + gm2) / 2
            audit += f" | avg(M1={gm1:.2f}%, M2={gm2:.2f}%, diff={diff:.1f}pp)"
        else:
            final = gm1
            audit += f" | M1 primary (M2={gm2:.2f}%, diff={diff:.1f}pp>5pp)"
        return round(final, 2), revenue, audit
    if gm1 is not None:
        return round(gm1, 2), revenue, audit + " | GP/Rev only"
    if gm2 is not None:
        return round(gm2, 2), revenue, audit + " | COGS-derived only"
    return None, revenue, "neither GP nor COGS row found"


def fetch_fundamentals(symbol: str) -> Dict[str, Any]:
    """Compute all fundamentals metrics (mirrors test.py Section B)."""
    t = yf.Ticker(symbol)

    def safe_get(attr):
        try:
            v = getattr(t, attr)
            return v
        except:
            return None

    inc  = safe_get("income_stmt")
    bs   = safe_get("balance_sheet")
    cf   = safe_get("cash_flow")
    info = safe_get("info") or {}

    price  = info.get("currentPrice") or info.get("regularMarketPrice")
    shares = info.get("sharesOutstanding")

    # Income rows
    revenue      = _get_row(inc, "Total Revenue", "Revenue")
    net_inc      = _get_row(inc, "Net Income", "Net Income Common Stockholders")
    ebitda       = _get_row(inc, "EBITDA", "Normalized EBITDA")
    ebit         = _get_row(inc, "EBIT") or _get_row(inc, "Operating Income")
    int_exp      = _get_row(inc, "Interest Expense", "Interest Expense Non Operating")
    dep          = _get_row(inc, "Reconciled Depreciation",
                             "Depreciation And Amortization In Income Stat",
                             "Depreciation")

    # Balance sheet rows
    total_assets  = _get_row(bs, "Total Assets")
    curr_liab     = _get_row(bs, "Current Liabilities", "Total Current Liabilities")
    curr_assets   = _get_row(bs, "Current Assets", "Total Current Assets")
    total_equity  = _get_row(bs, "Stockholders Equity", "Common Stock Equity",
                              "Total Equity Gross Minority Interest")
    total_debt    = _get_row(bs, "Total Debt")
    ar            = _get_row(bs, "Accounts Receivable", "Gross Accounts Receivable")
    ap            = _get_row(bs, "Accounts Payable")
    cogs          = _get_row(inc, "Cost Of Revenue", "Reconciled Cost Of Revenue")

    # Inventory
    inventory = None
    if bs is not None and not bs.empty:
        for idx in bs.index:
            if str(idx).lower().strip() == "inventory":
                v = _safe_float(bs.loc[idx].dropna().iloc[0]) if not bs.loc[idx].dropna().empty else None
                if v is not None:
                    inventory = v; break
        if inventory is None:
            for idx in bs.index:
                s = str(idx).lower().strip()
                if ("inventory" in s and "raw" not in s and "work" not in s
                        and "finished" not in s and "progress" not in s):
                    v = _safe_float(bs.loc[idx].dropna().iloc[0]) if not bs.loc[idx].dropna().empty else None
                    if v is not None:
                        inventory = v; break

    # Cash flow rows
    op_cf = _get_row(cf, "Operating Cash Flow", "Net Cash Provided By Operating Activities")
    capex = _get_row(cf, "Capital Expenditure",
                     "Purchase Of Property Plant And Equipment",
                     "Purchases Of Property Plant And Equipment")

    out: Dict[str, Any] = {}
    _ebit = ebit or (ebitda * 0.82 if ebitda else None)

    # ROE
    if net_inc and total_equity and total_equity != 0:
        out["ROE (%)"] = round(net_inc / total_equity * 100, 2)
    elif info.get("returnOnEquity"):
        out["ROE (%)"] = round(info["returnOnEquity"] * 100, 2)

    # ROCE
    if _ebit and total_assets and curr_liab:
        ce = total_assets - curr_liab
        if ce > 0:
            out["ROCE (%)"] = round(_ebit / ce * 100, 2)

    # ROA
    if net_inc and total_assets and total_assets != 0:
        out["ROA (%)"] = round(net_inc / total_assets * 100, 2)

    # Interest coverage
    if _ebit and int_exp and int_exp != 0:
        out["Interest Coverage"] = round(abs(_ebit / int_exp), 2)

    # FCF
    if op_cf is not None:
        capex_abs = abs(capex) if capex else 0
        out["Free Cash Flow"] = op_cf - capex_abs
        out["Operating CF"]   = op_cf
        out["CapEx"]          = capex_abs
    elif info.get("freeCashflow"):
        out["Free Cash Flow"] = info["freeCashflow"]

    # Gross margin (dual-method FIX-C)
    gm_pct, _, _ = _compute_gross_margin_safe(inc)
    if gm_pct is not None:
        out["Gross Margin (%)"] = gm_pct

    # Margins
    if net_inc and revenue and revenue != 0:
        out["Net Profit Margin (%)"] = round(net_inc / revenue * 100, 2)
    if ebitda and revenue and revenue != 0:
        out["EBITDA Margin (%)"] = round(ebitda / revenue * 100, 2)
    if _ebit and revenue and revenue != 0:
        out["EBIT Margin (%)"] = round(_ebit / revenue * 100, 2)

    # Leverage & liquidity
    if total_debt and total_equity and total_equity != 0:
        out["Debt/Equity"] = round(total_debt / total_equity, 2)
    if curr_assets and curr_liab and curr_liab != 0:
        out["Current Ratio"] = round(curr_assets / curr_liab, 2)
        inv_use = inventory or 0
        out["Quick Ratio"] = round((curr_assets - inv_use) / curr_liab, 2)

    # Working capital days
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

    # Valuation
    if price and shares and net_inc:
        eps = net_inc / shares
        out["EPS"] = round(eps, 2)
        if eps > 0:
            out["P/E"] = round(price / eps, 2)

    bv = info.get("bookValue")
    if price and bv and bv != 0:
        out["P/B"] = round(price / bv, 2)

    if "EPS" in out and bv and out["EPS"] > 0 and bv > 0:
        gn = math.sqrt(22.5 * out["EPS"] * bv)
        out["Graham Number"] = round(gn, 2)

    dy = info.get("dividendYield")
    if dy:
        out["Dividend Yield (%)"] = round(dy * 100, 2)

    mc = info.get("marketCap")
    if mc:
        out["Market Cap"] = mc

    out["Revenue"]    = revenue
    out["Net Income"] = net_inc
    out["EBITDA"]     = ebitda
    out["Inventory"]  = inventory

    # TTM EPS from quarterly data
    try:
        q_inc = t.quarterly_income_stmt
        if q_inc is not None and not q_inc.empty:
            inc_row = next((r for r in q_inc.index
                            if "net income" in str(r).lower()), None)
            if inc_row and shares:
                ttm_ni = sum(
                    _safe_float(q_inc.loc[inc_row, c]) or 0
                    for c in q_inc.columns[:4]
                )
                ttm_eps = ttm_ni / shares
                out["TTM EPS"] = round(ttm_eps, 2)
                if price and ttm_eps > 0:
                    out["TTM P/E"] = round(price / ttm_eps, 2)
    except:
        pass

    return out
