import math
import pandas as pd
import yfinance as yf
from typing import Optional, Dict, Tuple
import time


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


def _get_row_series(df, *candidates) -> Optional[pd.Series]:
    if df is None or df.empty:
        return None
    for name in candidates:
        for idx in df.index:
            if str(idx).lower().strip() == name.lower().strip():
                return df.loc[idx]
    for name in candidates:
        for idx in df.index:
            if name.lower() in str(idx).lower():
                return df.loc[idx]
    return None


def _interpolate_qbs_from_annual(
    q_inc: pd.DataFrame,
    ann_bs: pd.DataFrame,
    q_bs: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """FIX-A: Interpolate missing Q-BS periods from annual BS."""
    if q_inc is None or q_inc.empty or ann_bs is None or ann_bs.empty:
        return q_bs, {}

    q_bs_cols = set(q_bs.columns) if q_bs is not None and not q_bs.empty else set()
    q_is_cols = list(q_inc.columns)
    missing   = [c for c in q_is_cols if c not in q_bs_cols]

    if not missing:
        return q_bs, {}

    ann_bs_by_year = {}
    for col in ann_bs.columns:
        try:
            yr = pd.Timestamp(col).year
            ann_bs_by_year[yr] = col
        except:
            pass

    rev_row_q = _get_row_series(q_inc, "Total Revenue", "Revenue")

    # Build quarterly revenue by FY
    qrev_by_fy: Dict[int, Dict] = {}
    if rev_row_q is not None:
        for col in q_inc.columns:
            v = _safe_float(rev_row_q.get(col))
            if v:
                try:
                    ct = pd.Timestamp(col)
                    fy = ct.year if ct.month <= 3 else ct.year + 1
                    qrev_by_fy.setdefault(fy, {})[col] = v
                except:
                    pass

    audit: Dict[str, str] = {}
    extended = q_bs.copy() if q_bs is not None and not q_bs.empty else pd.DataFrame()

    for qcol in missing:
        try:
            qt = pd.Timestamp(qcol)
        except:
            audit[str(qcol)[:10]] = "skip"
            continue

        fy_year = qt.year if qt.month <= 3 else qt.year + 1
        ann_col = ann_bs_by_year.get(fy_year) or ann_bs_by_year.get(fy_year - 1)
        if ann_col is None:
            audit[str(qcol)[:10]] = "no matching annual BS"
            continue

        # Revenue-proportioned scale
        fy_revs = qrev_by_fy.get(fy_year, {})
        total_fy_rev = sum(fy_revs.values())
        q_rev_this = rev_row_q.get(qcol) if rev_row_q is not None else None
        q_rev_this = _safe_float(q_rev_this) or 0

        scale = (q_rev_this / total_fy_rev) if total_fy_rev > 0 and q_rev_this > 0 else 0.25
        extended[qcol] = ann_bs[ann_col] * scale
        audit[str(qcol)[:10]] = f"Ann({str(ann_col)[:7]})×{scale:.2f}"

    if not extended.empty:
        common = [c for c in q_is_cols if c in extended.columns]
        extended = extended.reindex(columns=common)

    return extended, audit


def fetch_statements(symbol: str) -> dict:
    """Fetch all financial statements (annual + quarterly) with Q-BS interpolation."""
    t = yf.Ticker(symbol)

    def safe_get(attr):
        try:
            return getattr(t, attr)
        except:
            return None

    annual_income = safe_get("income_stmt")
    annual_bs     = safe_get("balance_sheet")
    annual_cf     = safe_get("cash_flow")
    q_income      = safe_get("quarterly_income_stmt")
    q_bs          = safe_get("quarterly_balance_sheet")
    q_cf          = safe_get("quarterly_cash_flow")

    time.sleep(0.4)

    # FIX-A: interpolate Q-BS if needed
    q_bs_extended = q_bs
    bs_audit = {}
    if q_income is not None and not q_income.empty:
        n_is = q_income.shape[1]
        n_bs = q_bs.shape[1] if q_bs is not None and not q_bs.empty else 0
        if n_bs < n_is:
            q_bs_extended, bs_audit = _interpolate_qbs_from_annual(
                q_income, annual_bs, q_bs
            )

    return {
        "annual_income":    annual_income,
        "annual_bs":        annual_bs,
        "annual_cf":        annual_cf,
        "q_income":         q_income,
        "q_bs":             q_bs,
        "q_bs_extended":    q_bs_extended,
        "bs_audit":         bs_audit,
        "q_cf":             q_cf,
    }
