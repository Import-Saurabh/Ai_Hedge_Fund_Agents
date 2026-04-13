import math
import pandas as pd
import yfinance as yf
from typing import Optional, Dict


def _safe_float(v) -> Optional[float]:
    try:
        fv = float(v)
        return None if math.isnan(fv) else fv
    except:
        return None


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
) -> list:
    """
    Derive quarterly FCF (FIX-A) from Q-IS + interpolated Q-BS + annual CF.
    Returns list of dicts ready for DB insertion.
    """
    t = yf.Ticker(symbol)
    records = []

    # Try direct quarterly CF first
    try:
        qcf = t.quarterly_cash_flow
        if qcf is not None and not qcf.empty:
            ni_r   = _row(qcf, "Net Income From Continuing Operations",
                          "Net Income Continuous Operations", "Net Income")
            opcf_r = _row(qcf, "Operating Cash Flow",
                          "Net Cash Provided By Operating Activities")
            cx_r   = _row(qcf, "Capital Expenditure",
                          "Purchase Of Property Plant And Equipment")
            fcf_r  = _row(qcf, "Free Cash Flow")
            rev_r  = _row(q_inc, "Total Revenue", "Revenue") if q_inc is not None else None

            for col in qcf.columns:
                op_cf = _safe_float(opcf_r.get(col)) if opcf_r is not None else None
                capex = _safe_float(cx_r.get(col))   if cx_r   is not None else None
                fcf   = _safe_float(fcf_r.get(col))  if fcf_r  is not None else None
                rev   = _safe_float(rev_r.get(col))  if rev_r  is not None else None
                ni    = _safe_float(ni_r.get(col))   if ni_r   is not None else None

                if fcf is None and op_cf is not None and capex is not None:
                    fcf = op_cf - abs(capex)
                fcf_mgn = round(fcf / rev * 100, 2) if fcf and rev and rev != 0 else None

                records.append({
                    "quarter_end":     str(col)[:10],
                    "revenue":         rev,
                    "net_income":      ni,
                    "dna":             None,
                    "approx_op_cf":    op_cf,
                    "approx_capex":    abs(capex) if capex else None,
                    "approx_fcf":      fcf,
                    "fcf_margin_pct":  fcf_mgn,
                    "capex_source":    "direct",
                    "is_interpolated": 0,
                })
            return records
    except:
        pass

    # Derived from Q-IS + extended Q-BS
    if q_inc is None or q_inc.empty:
        return records

    if bs_audit is None:
        bs_audit = {}

    ann_cf = None
    try:
        ann_cf = t.cash_flow
    except:
        pass

    ni_r     = _row(q_inc, "Net Income", "Net Income Common Stockholders",
                    "Net Income From Continuing Operation Net Mino")
    dep_r    = _row(q_inc, "Reconciled Depreciation",
                    "Depreciation And Amortization In Income Stat", "Depreciation")
    ebitda_r = _row(q_inc, "EBITDA", "Normalized EBITDA")
    ebit_r   = _row(q_inc, "EBIT", "Operating Income")
    rev_r    = _row(q_inc, "Total Revenue", "Operating Revenue", "Revenue")
    ppe_r    = _row(q_bs_extended, "Net PPE", "Net Property Plant And Equipment") \
               if q_bs_extended is not None and not q_bs_extended.empty else None

    # Annual CapEx by FY
    ann_capex_by_fy: Dict[int, float] = {}
    if ann_cf is not None and not ann_cf.empty:
        cx_row = _row(ann_cf, "Capital Expenditure",
                      "Purchase Of Property Plant And Equipment",
                      "Purchases Of Property Plant And Equipment")
        if cx_row is not None:
            for col in ann_cf.columns:
                v = _safe_float(cx_row.get(col))
                if v is not None:
                    try:
                        ct = pd.Timestamp(col)
                        fy = ct.year if ct.month <= 3 else ct.year + 1
                        ann_capex_by_fy[fy] = abs(v)
                    except:
                        pass

    # Quarterly revenue by FY for prorating
    qrev_by_fy: Dict[int, Dict] = {}
    if rev_r is not None:
        for col in q_inc.columns:
            v = _safe_float(rev_r.get(col))
            if v:
                try:
                    ct = pd.Timestamp(col)
                    fy = ct.year if ct.month <= 3 else ct.year + 1
                    qrev_by_fy.setdefault(fy, {})[col] = v
                except:
                    pass

    q_bs_cols = list(q_bs_extended.columns) \
        if q_bs_extended is not None and not q_bs_extended.empty else []

    for col in q_inc.columns:
        def gv(row):
            if row is None: return None
            return _safe_float(row.get(col))

        ni     = gv(ni_r)
        dep    = gv(dep_r)
        ebitda = gv(ebitda_r)
        ebit   = gv(ebit_r)
        rev    = gv(rev_r)

        da = dep if dep is not None else (
            (ebitda - ebit) if ebitda is not None and ebit is not None else None
        )
        op_cf = (ni + da) if (ni is not None and da is not None) else ni

        # CapEx derivation (FIX-A priority)
        capex       = None
        capex_src   = "N/A"
        is_interp   = 0

        if ppe_r is not None and col in q_bs_cols:
            pos = q_bs_cols.index(col)
            if pos + 1 < len(q_bs_cols):
                prev_col = q_bs_cols[pos + 1]
                try:
                    ppe_curr = float(ppe_r[col])
                    ppe_prev = float(ppe_r[prev_col])
                    capex    = (ppe_curr - ppe_prev) + (da or 0)
                    col_key  = str(col)[:10]
                    is_interp = 1 if col_key in bs_audit else 0
                    capex_src = f"ΔPPE+D&A{' [interp]' if is_interp else ''}"
                except:
                    pass

        if capex is None and ann_capex_by_fy:
            try:
                ct = pd.Timestamp(col)
                fy = ct.year if ct.month <= 3 else ct.year + 1
                ann_cx = ann_capex_by_fy.get(fy) or list(ann_capex_by_fy.values())[0]
                fy_revs = qrev_by_fy.get(fy, {})
                total_fy_rev = sum(fy_revs.values())
                if total_fy_rev > 0 and (rev or 0) > 0:
                    share = (rev or 0) / total_fy_rev
                    capex = ann_cx * share
                    capex_src = f"Ann×{share:.2f}"
                    is_interp = 1
                else:
                    capex = ann_cx / 4
                    capex_src = "Ann÷4"
                    is_interp = 1
            except:
                pass

        fcf = (op_cf - abs(capex)
               if op_cf is not None and capex is not None else op_cf)
        fcf_mgn = round(fcf / rev * 100, 2) if fcf and rev and rev != 0 else None

        records.append({
            "quarter_end":    str(col)[:10],
            "revenue":        rev,
            "net_income":     ni,
            "dna":            da,
            "approx_op_cf":   op_cf,
            "approx_capex":   abs(capex) if capex is not None else None,
            "approx_fcf":     fcf,
            "fcf_margin_pct": fcf_mgn,
            "capex_source":   capex_src,
            "is_interpolated": is_interp,
        })

    return records
