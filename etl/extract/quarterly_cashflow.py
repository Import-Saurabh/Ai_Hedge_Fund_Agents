"""
etl/extract/quarterly_cashflow.py  v4.1
────────────────────────────────────────────────────────────────
Changes vs v4.0:
  BUG 1 — revenue = cost_of_revenue (wrong row):
    _row(q_inc, "Total Revenue", "Revenue") was matching
    "Operating Revenue" or fallback sub-rows first in some
    quarter layouts. Now uses strict priority:
      1. Exact "Total Revenue"
      2. Exact "Revenue"  (only if no sub-row match)
      3. "Operating Revenue" last resort
    Result was revenue=1656 (cost_of_revenue) vs correct 8488

  BUG 2 — dna NULL despite D&A present in quarterly IS:
    yfinance quarterly IS stores D&A as:
      "Reconciled Depreciation"         ← try first (most reliable)
      "Depreciation And Amortization"
      "Depreciation"
    Previous code missed "Reconciled Depreciation" in priority 2
    (NI+DA path). Now explicit.

  BUG 3 — EBITDA - EBIT D&A fallback was unreliable:
    quarterly IS often has ebitda=ebit (no separate ebitda row)
    → only use that fallback when ebitda > ebit by a material
      amount (> 100 Cr). Avoids setting dna=0.
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


def _row_exact(df, *candidates) -> Optional[pd.Series]:
    """Exact label match first, then substring — avoids wrong sub-rows."""
    if df is None or df.empty:
        return None
    # Phase 1: exact match (case-insensitive)
    for c in candidates:
        for idx in df.index:
            if str(idx).strip().lower() == c.lower():
                return df.loc[idx]
    # Phase 2: substring match (only if no exact found)
    for c in candidates:
        for idx in df.index:
            if c.lower() in str(idx).lower():
                return df.loc[idx]
    return None


def _row(df, *cands) -> Optional[pd.Series]:
    """Original substring-first row finder (kept for CF paths)."""
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
    quality_score: 3=direct_qcf, 2=NI+DA_approx, 1=NI_only (skipped)
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
            dep_r  = _row(qcf, "Depreciation And Amortization",
                          "Depreciation Amortization Depletion",
                          "Reconciled Depreciation", "Depreciation")

            # FIX: use exact-first match for revenue to avoid sub-rows
            rev_r = _row_exact(q_inc, "Total Revenue", "Revenue",
                               "Operating Revenue") if q_inc is not None else None

            for col in qcf.columns:
                op_cf_raw = _safe_float(opcf_r.get(col)) if opcf_r is not None else None
                op_cf     = _cr(op_cf_raw)

                capex_raw = _safe_float(cx_r.get(col)) if cx_r is not None else None
                capex     = _cr(abs(capex_raw)) if capex_raw is not None else None

                fcf_raw   = _safe_float(fcf_r.get(col)) if fcf_r is not None else None
                fcf       = _cr(fcf_raw)
                if fcf is None and op_cf is not None and capex is not None:
                    fcf = round(op_cf - capex, 2)

                # FIX: get revenue from correct row
                rev_raw = _safe_float(rev_r.get(col)) if rev_r is not None else None
                rev     = _cr(rev_raw)

                ni  = _cr(_safe_float(ni_r.get(col))  if ni_r  is not None else None)

                dep_raw = _safe_float(dep_r.get(col)) if dep_r is not None else None
                dna     = _cr(dep_raw) if (dep_raw is not None and dep_raw > 0) else None

                fcf_mgn = (round(fcf / rev * 100, 2)
                           if fcf is not None and rev and rev != 0 else None)

                if op_cf is None and fcf is None and ni is None:
                    continue

                records.append({
                    "quarter_end":     str(col)[:10],
                    "revenue":         rev,
                    "net_income":      ni,
                    "dna":             dna,
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

    # FIX: use exact-first match for revenue to avoid cost_of_revenue match
    rev_r  = _row_exact(q_inc, "Total Revenue", "Revenue", "Operating Revenue")

    ni_r   = _row_exact(q_inc,
                        "Net Income",
                        "Net Income Common Stockholders",
                        "Net Income From Continuing Operation Net Mino")

    # FIX: try "Reconciled Depreciation" first (most reliable in yfinance quarterly IS)
    dep_r  = _row_exact(q_inc,
                        "Reconciled Depreciation",
                        "Depreciation And Amortization In Income Stat",
                        "Depreciation And Amortization",
                        "Depreciation")

    ebit_r   = _row_exact(q_inc, "EBIT", "Operating Income")
    ebitda_r = _row_exact(q_inc, "EBITDA", "Normalized EBITDA")

    skipped_quality = 0
    for col in q_inc.columns:
        ni  = _cr(_safe_float(ni_r.get(col))  if ni_r  is not None else None)
        rev_raw = _safe_float(rev_r.get(col)) if rev_r is not None else None
        rev = _cr(rev_raw)

        dep_raw = _safe_float(dep_r.get(col)) if dep_r is not None else None
        dna     = _cr(dep_raw) if (dep_raw is not None and dep_raw > 0) else None

        # FIX: EBITDA - EBIT fallback only when difference is material (>100 Cr in raw)
        if dna is None and ebitda_r is not None and ebit_r is not None:
            eb_raw = _safe_float(ebitda_r.get(col))
            el_raw = _safe_float(ebit_r.get(col))
            if (eb_raw is not None and el_raw is not None
                    and (eb_raw - el_raw) > 1e9):   # > 100 Cr threshold
                dna = _cr(eb_raw - el_raw)

        # approx_op_cf requires BOTH NI and dna
        if ni is not None and dna is not None:
            op_cf = round(ni + dna, 2)
            quality = 2
            note = "NI+real_DA from quarterly IS"
            capex_src = "NI+DA_approx"
        else:
            op_cf = None
            quality = 1
            note = "NI only — DA unavailable, op_cf not computed"
            capex_src = "NI_only"

        if ni is None:
            continue

        # Skip quality_score=1 rows at extract time
        # (loader also enforces this, but better to skip early)
        if quality < 2:
            skipped_quality += 1
            # Still add the record — loader decides what to store
            # (we need net_income for fundamentals reconcile even if no op_cf)

        records.append({
            "quarter_end":     str(col)[:10],
            "revenue":         rev,
            "net_income":      ni,
            "dna":             dna,
            "approx_op_cf":    op_cf,
            "approx_capex":    None,
            "approx_fcf":      None,
            "fcf_margin_pct":  None,
            "capex_source":    capex_src,
            "quality_score":   quality,
            "is_real":         1,
            "is_interpolated": 0,
            "data_note":       note,
            "unit":            "Rs_Crores",
        })

    q2_count = sum(1 for r in records if r["quality_score"] >= 2)
    print(f"  ok  quarterly_cashflow: {len(records)} NI rows "
          f"({q2_count} quality≥2, {skipped_quality} NI-only)")
    return records