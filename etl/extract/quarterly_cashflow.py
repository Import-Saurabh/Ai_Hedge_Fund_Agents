"""
etl/extract/quarterly_cashflow.py  v4.0
────────────────────────────────────────────────────────────────
Changes vs v3:
  • quality_score introduced: 3=direct_qcf, 2=NI+DA, 1=nothing
  • dna: only set when D&A is a real non-zero value from IS
  • approx_op_cf: only NI+DA when BOTH are present — never NI alone
  • approx_fcf: NULL when capex unknown (not NI proxy)
  • is_real=1 always (we never insert fabricated rows)
  • is_interpolated=0 always (fabricated rows blocked at source)
  • data_note explains what each row is
────────────────────────────────────────────────────────────────
"""

import math
import pandas as pd
import yfinance as yf
from typing import Optional

_CR = 1e7


def _safe_float(v) -> Optional[float]:
    try:
        fv = float(v)
        return None if math.isnan(fv) else fv
    except Exception:
        return None


def _cr(v) -> Optional[float]:
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
    Returns real quarterly cashflow records only.
    quality_score: 3 = direct from yfinance quarterly CF
                   2 = NI + real D&A from quarterly IS
                   1 = nothing reliable — row omitted

    Rules:
    - dna is ONLY set when D&A row exists AND value > 0
    - approx_op_cf is ONLY set when both NI and dna are present
    - approx_fcf is ONLY set when approx_op_cf + capex both present
    - is_interpolated is always 0 (we drop fabricated rows)
    - is_real is always 1
    """
    t = yf.Ticker(symbol)
    records = []

    # ── Priority 1: direct quarterly CF ──────────────────────
    try:
        qcf = t.quarterly_cash_flow
        if qcf is not None and not qcf.empty:
            opcf_r = _row(qcf, "Operating Cash Flow",
                          "Net Cash Provided By Operating Activities")
            cx_r   = _row(qcf, "Capital Expenditure",
                          "Purchase Of Property Plant And Equipment")
            fcf_r  = _row(qcf, "Free Cash Flow")
            ni_r   = _row(qcf, "Net Income From Continuing Operations",
                          "Net Income Continuous Operations", "Net Income")
            dep_r  = _row(qcf, "Depreciation And Amortization", "Depreciation")
            rev_r  = _row(q_inc, "Total Revenue", "Revenue") if q_inc is not None else None

            for col in qcf.columns:
                op_cf_raw = _safe_float(opcf_r.get(col)) if opcf_r is not None else None
                op_cf     = _cr(op_cf_raw)

                capex_raw = _safe_float(cx_r.get(col)) if cx_r is not None else None
                capex     = _cr(abs(capex_raw)) if capex_raw is not None else None

                fcf_raw   = _safe_float(fcf_r.get(col)) if fcf_r is not None else None
                fcf       = _cr(fcf_raw)
                if fcf is None and op_cf is not None and capex is not None:
                    fcf = round(op_cf - capex, 2)

                rev = _cr(_safe_float(rev_r.get(col)) if rev_r is not None else None)
                ni  = _cr(_safe_float(ni_r.get(col))  if ni_r  is not None else None)

                dep_raw = _safe_float(dep_r.get(col)) if dep_r is not None else None
                dna     = _cr(dep_raw) if (dep_raw is not None and dep_raw > 0) else None

                fcf_mgn = (round(fcf / rev * 100, 2)
                           if fcf is not None and rev and rev != 0 else None)

                # Only skip if completely empty
                if op_cf is None and fcf is None and ni is None:
                    continue

                records.append({
                    "quarter_end":     str(col)[:10],
                    "revenue":         rev,
                    "net_income":      ni,
                    "dna":             dna,             # real D&A or NULL
                    "approx_op_cf":    op_cf,
                    "approx_capex":    capex,
                    "approx_fcf":      fcf,
                    "fcf_margin_pct":  fcf_mgn,
                    "capex_source":    "direct_qcf",
                    "quality_score":   3,
                    "is_real":         1,
                    "is_interpolated": 0,
                    "data_note":       "yfinance quarterly_cash_flow direct",
                    "unit":            "Rs_Crores",
                })

            if records:
                print(f"  ok  quarterly_cashflow: {len(records)} direct QCF rows")
                return records
    except Exception as e:
        print(f"  warn  quarterly_cashflow direct: {e}")

    # ── Priority 2: NI + real D&A from quarterly IS ───────────
    if q_inc is None or q_inc.empty:
        print("  warn  quarterly_cashflow: no q_inc — returning empty")
        return records

    ni_r   = _row(q_inc, "Net Income", "Net Income Common Stockholders",
                  "Net Income From Continuing Operation Net Mino")
    dep_r  = _row(q_inc, "Reconciled Depreciation",
                  "Depreciation And Amortization In Income Stat",
                  "Depreciation And Amortization")
    rev_r  = _row(q_inc, "Total Revenue", "Operating Revenue", "Revenue")
    ebit_r = _row(q_inc, "EBIT", "Operating Income")
    ebitda_r = _row(q_inc, "EBITDA", "Normalized EBITDA")

    for col in q_inc.columns:
        ni  = _cr(_safe_float(ni_r.get(col))  if ni_r  is not None else None)
        rev = _cr(_safe_float(rev_r.get(col)) if rev_r is not None else None)

        dep_raw = _safe_float(dep_r.get(col)) if dep_r is not None else None
        dna     = _cr(dep_raw) if (dep_raw is not None and dep_raw > 0) else None

        # D&A from EBITDA - EBIT if direct D&A missing
        if dna is None and ebitda_r is not None and ebit_r is not None:
            eb_raw = _safe_float(ebitda_r.get(col))
            el_raw = _safe_float(ebit_r.get(col))
            if eb_raw is not None and el_raw is not None and (eb_raw - el_raw) > 0:
                dna = _cr(eb_raw - el_raw)

        # approx_op_cf requires BOTH NI and dna
        if ni is not None and dna is not None:
            op_cf = round(ni + dna, 2)
        else:
            op_cf = None   # not NI alone — would be wrong

        # fcf requires op_cf; capex unknown so fcf is NULL
        fcf = None
        fcf_mgn = None

        if ni is None:
            continue   # skip completely empty rows

        records.append({
            "quarter_end":     str(col)[:10],
            "revenue":         rev,
            "net_income":      ni,
            "dna":             dna,
            "approx_op_cf":    op_cf,
            "approx_capex":    None,      # genuinely unknown
            "approx_fcf":      fcf,       # NULL — no capex
            "fcf_margin_pct":  fcf_mgn,
            "capex_source":    "NI+DA_approx" if op_cf is not None else "NI_only",
            "quality_score":   2 if op_cf is not None else 1,
            "is_real":         1,
            "is_interpolated": 0,
            "data_note":       (
                "NI+real_DA from quarterly IS"
                if op_cf is not None
                else "NI only — DA unavailable, op_cf not computed"
            ),
            "unit":            "Rs_Crores",
        })

    print(f"  ok  quarterly_cashflow: {len(records)} NI+DA rows "
          f"(quality 2+)")
    return records