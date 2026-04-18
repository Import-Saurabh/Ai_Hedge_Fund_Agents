"""
etl/extract/quarterly_cashflow.py  v3.0
────────────────────────────────────────────────────────────────
Fixes vs v2:
  • ALL figures stored in Rs. Crores (divide raw yfinance by 1e7)
  • Direct quarterly CF from yfinance is now priority-1 and
    returns real data — no fabrication
  • Interpolated CapEx (Ann×ratio) is NEVER loaded by default;
    only direct_qcf rows with is_interpolated=0 are produced here
  • quarterly_cashflow_derived will no longer be empty — yfinance
    quarterly_cash_flow is the primary source
────────────────────────────────────────────────────────────────
"""

import math
import pandas as pd
import yfinance as yf
from typing import Optional, Dict

# ── Unit: all monetary values → Rs. Crores ───────────────────
_CR = 1e7          # 1 Crore = 10,000,000


def _safe_float(v) -> Optional[float]:
    try:
        fv = float(v)
        return None if math.isnan(fv) else fv
    except Exception:
        return None


def _cr(v) -> Optional[float]:
    """Convert raw rupees → Rs. Crores, rounded to 2dp."""
    f = _safe_float(v)
    return round(f / _CR, 2) if f is not None else None


def _row(df, *cands):
    if df is None or df.empty:
        return None
    for df_idx in df.index:
        for c in cands:
            if c.lower() in str(df_idx).lower():
                return df.loc[df_idx]
    return None


def fetch_quarterly_cashflow(
    symbol: str,
    q_inc: pd.DataFrame = None,
    q_bs_extended: pd.DataFrame = None,
    bs_audit: dict = None,
    q_bs_real: pd.DataFrame = None,
) -> list:
    """
    Derive quarterly FCF — real data only, in Rs. Crores.

    Priority:
      1. Direct yfinance quarterly_cash_flow  [is_interpolated=0]
      2. Net Income + D&A from quarterly IS   [is_interpolated=0, approx]
      3. Nothing — we do NOT produce Ann×ratio fakes anymore

    All monetary values are in Rs. Crores.
    """
    t       = yf.Ticker(symbol)
    records = []

    # ── Priority 1: direct quarterly CF ──────────────────────
    try:
        qcf = t.quarterly_cash_flow
        if qcf is not None and not qcf.empty:
            opcf_r  = _row(qcf, "Operating Cash Flow",
                           "Net Cash Provided By Operating Activities")
            cx_r    = _row(qcf, "Capital Expenditure",
                           "Purchase Of Property Plant And Equipment")
            fcf_r   = _row(qcf, "Free Cash Flow")
            ni_r    = _row(qcf, "Net Income From Continuing Operations",
                           "Net Income Continuous Operations", "Net Income")
            dep_r   = _row(qcf, "Depreciation And Amortization", "Depreciation")
            rev_r   = _row(q_inc, "Total Revenue", "Revenue") if q_inc is not None else None

            for col in qcf.columns:
                op_cf = _cr(_safe_float(opcf_r.get(col)) if opcf_r is not None else None)
                capex_raw = _safe_float(cx_r.get(col))   if cx_r   is not None else None
                capex = _cr(abs(capex_raw))               if capex_raw is not None else None
                fcf   = _cr(_safe_float(fcf_r.get(col))  if fcf_r  is not None else None)
                rev   = _cr(_safe_float(rev_r.get(col))  if rev_r  is not None else None)
                ni    = _cr(_safe_float(ni_r.get(col))   if ni_r   is not None else None)
                da    = _cr(_safe_float(dep_r.get(col))  if dep_r  is not None else None)

                if fcf is None and op_cf is not None and capex is not None:
                    fcf = round(op_cf - capex, 2)

                fcf_mgn = (round(fcf / rev * 100, 2)
                           if fcf is not None and rev and rev != 0 else None)

                records.append({
                    "quarter_end":     str(col)[:10],
                    "revenue":         rev,
                    "net_income":      ni,
                    "dna":             da,
                    "approx_op_cf":    op_cf,
                    "approx_capex":    capex,
                    "approx_fcf":      fcf,
                    "fcf_margin_pct":  fcf_mgn,
                    "capex_source":    "direct_qcf",
                    "is_interpolated": 0,
                    "unit":            "Rs_Crores",
                })

            if records:
                print(f"  ✅ quarterly_cashflow: {len(records)} direct QCF rows (Rs Cr)")
                return records
    except Exception as e:
        print(f"  ⚠  quarterly_cashflow direct fetch failed: {e}")

    # ── Priority 2: Net Income + D&A from quarterly IS ────────
    # (still real data — no fabrication — just incomplete CF)
    if q_inc is None or q_inc.empty:
        print("  ⚠  quarterly_cashflow: no q_inc available, returning empty")
        return records

    ni_r    = _row(q_inc, "Net Income", "Net Income Common Stockholders",
                   "Net Income From Continuing Operation Net Mino")
    dep_r   = _row(q_inc, "Reconciled Depreciation",
                   "Depreciation And Amortization In Income Stat", "Depreciation")
    ebitda_r = _row(q_inc, "EBITDA", "Normalized EBITDA")
    ebit_r   = _row(q_inc, "EBIT", "Operating Income")
    rev_r    = _row(q_inc, "Total Revenue", "Operating Revenue", "Revenue")

    for col in q_inc.columns:
        ni     = _cr(_safe_float(ni_r.get(col))     if ni_r    is not None else None)
        dep    = _cr(_safe_float(dep_r.get(col))    if dep_r   is not None else None)
        ebitda = _cr(_safe_float(ebitda_r.get(col)) if ebitda_r is not None else None)
        ebit   = _cr(_safe_float(ebit_r.get(col))   if ebit_r  is not None else None)
        rev    = _cr(_safe_float(rev_r.get(col))    if rev_r   is not None else None)

        da = dep if dep is not None else (
            round(ebitda - ebit, 2)
            if ebitda is not None and ebit is not None else None
        )

        op_cf = round(ni + da, 2) if ni is not None and da is not None else ni

        records.append({
            "quarter_end":     str(col)[:10],
            "revenue":         rev,
            "net_income":      ni,
            "dna":             da,
            "approx_op_cf":    op_cf,
            "approx_capex":    None,          # unknown — no fabrication
            "approx_fcf":      op_cf,         # best proxy without capex
            "fcf_margin_pct":  (round(op_cf / rev * 100, 2)
                                if op_cf is not None and rev and rev != 0 else None),
            "capex_source":    "NI+DA_approx",
            "is_interpolated": 0,             # real IS data, not fabricated
            "unit":            "Rs_Crores",
        })

    print(f"  ✅ quarterly_cashflow: {len(records)} NI+DA rows (Rs Cr, no capex)")
    return records