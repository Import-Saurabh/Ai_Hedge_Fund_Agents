"""
etl/load/reconcile.py  v4.2
────────────────────────────────────────────────────────────────
Changes vs v4.1:
  • Removed gross_npa_pct and net_npa_pct from reconcile_profit_and_loss()
    entirely — these columns are not present in the profit_and_loss schema.
  • FINANCIAL_FIELDS now contains only financing_profit and
    financing_margin_pct; the is_financial_row check uses just these two.
────────────────────────────────────────────────────────────────
"""

import json
import math
from typing import Optional
from database.db import get_connection


# ─────────────────────────────────────────────
# Utils
# ─────────────────────────────────────────────
def _f(v) -> Optional[float]:
    if v is None:
        return None
    try:
        fv = float(v)
        return None if (math.isnan(fv) or math.isinf(fv)) else fv
    except:
        return None


def _div(a, b) -> Optional[float]:
    a, b = _f(a), _f(b)
    if a is None or b is None or b == 0:
        return None
    return round(a / b, 4)


def _pct(a, b) -> Optional[float]:
    v = _div(a, b)
    return round(v * 100, 2) if v is not None else None


def _completeness(fields: dict) -> tuple[float, list]:
    missing = [k for k, v in fields.items() if v is None]
    pct = round((1 - len(missing) / len(fields)) * 100, 1) if fields else 100.0
    return pct, missing


# ─────────────────────────────────────────────
# Schema migration helpers
# ─────────────────────────────────────────────
def _ensure_cols(conn, table: str, col_defs: list[tuple[str, str]]):
    """Idempotently add columns to *table* if they don't exist."""
    for col_name, col_type in col_defs:
        try:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_name} {col_type}")
            print(f"  db-migrate {table}: added column '{col_name}'")
        except Exception:
            pass  # already exists


# ─────────────────────────────────────────────
# 1. PROFIT & LOSS  (replaces income_statement)
# ─────────────────────────────────────────────
def reconcile_profit_and_loss(symbol: str, conn):
    """
    For every row in profit_and_loss:
      • Derive opm_pct from operating_profit / sales when the stored
        value is NULL (some Screener pages omit it).
      • Compute completeness_pct and missing_fields_json over the
        core numeric columns.
      • NBFC/Bank columns (financing_profit, financing_margin_pct) are
        excluded from the completeness penalty when the company is
        non-financial (i.e. both columns are 0 or NULL for that row).
    """
    _ensure_cols(conn, "profit_and_loss", [
        ("completeness_pct",     "REAL"),
        ("missing_fields_json",  "TEXT"),
        ("financing_profit",     "REAL"),
        ("financing_margin_pct", "REAL"),
    ])
    conn.commit()

    # Core columns we always score for completeness
    CORE_FIELDS = [
        "sales", "expenses", "operating_profit", "opm_pct",
        "other_income", "interest", "depreciation",
        "profit_before_tax", "tax_pct", "net_profit", "eps",
    ]
    # Additional columns scored only for financial companies
    FINANCIAL_FIELDS = [
        "financing_profit", "financing_margin_pct",
    ]
    ALL_FIELDS = CORE_FIELDS + FINANCIAL_FIELDS

    rows = conn.execute(f"""
        SELECT rowid, {', '.join(ALL_FIELDS)}
        FROM profit_and_loss
        WHERE symbol = ?
    """, (symbol,)).fetchall()

    for r in rows:
        rowid    = r[0]
        vals     = dict(zip(ALL_FIELDS, r[1:]))
        floats   = {k: _f(v) for k, v in vals.items()}

        # Derive opm_pct if missing
        derived_opm = None
        if floats.get("opm_pct") is None:
            sales = floats.get("sales")
            op    = floats.get("operating_profit")
            if sales and op and sales != 0:
                derived_opm = round((op / sales) * 100, 2)
            floats["opm_pct"] = derived_opm

        # Determine if this row belongs to a financial company:
        # either financing field is non-zero / non-null → financial
        is_financial_row = any(
            floats.get(f) not in (None, 0.0)
            for f in FINANCIAL_FIELDS
        )

        scored_fields = {k: floats[k] for k in (CORE_FIELDS + (FINANCIAL_FIELDS if is_financial_row else []))}
        comp, missing = _completeness(scored_fields)

        conn.execute("""
            UPDATE profit_and_loss SET
                opm_pct             = COALESCE(opm_pct, ?),
                completeness_pct    = ?,
                missing_fields_json = ?
            WHERE rowid = ?
        """, (derived_opm, comp, json.dumps(missing), rowid))

    conn.commit()
    print(f"  ✅ reconcile profit_and_loss: {len(rows)} rows for {symbol}")


# ─────────────────────────────────────────────
# 2. BALANCE SHEET
# ─────────────────────────────────────────────
def reconcile_balance_sheet(symbol: str, conn):
    _ensure_cols(conn, "balance_sheet", [
        ("completeness_pct",    "REAL"),
        ("missing_fields_json", "TEXT"),
    ])
    conn.commit()

    rows = conn.execute("""
        SELECT rowid,
               total_assets, total_liabilities, total_equity,
               borrowings, cash_equivalents, net_debt
        FROM balance_sheet
        WHERE symbol = ?
    """, (symbol,)).fetchall()

    for r in rows:
        rowid, ta, tl, te, debt, cash, net_d = r
        ta    = _f(ta)
        tl    = _f(tl)
        te    = _f(te)
        debt  = _f(debt)
        cash  = _f(cash)
        net_d = _f(net_d)

        if net_d is None and debt is not None and cash is not None:
            net_d = round(debt - cash, 2)

        fields = {
            "total_assets":      ta,
            "total_liabilities": tl,
            "total_equity":      te,
            "borrowings":        debt,
            "cash_equivalents":  cash,
            "net_debt":          net_d,
        }
        comp, missing = _completeness(fields)

        conn.execute("""
            UPDATE balance_sheet SET
                net_debt            = CASE WHEN ? IS NOT NULL THEN ? ELSE net_debt END,
                completeness_pct    = ?,
                missing_fields_json = ?
            WHERE rowid = ?
        """, (net_d, net_d, comp, json.dumps(missing), rowid))

    conn.commit()
    print(f"  ✅ reconcile balance_sheet: {len(rows)} rows for {symbol}")


# ─────────────────────────────────────────────
# 3. CASH FLOW
# ─────────────────────────────────────────────
def reconcile_cash_flow(symbol: str, conn):
    _ensure_cols(conn, "cash_flow", [
        ("completeness_pct",    "REAL"),
        ("missing_fields_json", "TEXT"),
    ])
    conn.commit()

    rows = conn.execute("""
        SELECT rowid, cfo, free_cash_flow
        FROM cash_flow
        WHERE symbol = ?
    """, (symbol,)).fetchall()

    for rowid, cfo, fcf in rows:
        fields = {"cfo": _f(cfo), "free_cash_flow": _f(fcf)}
        comp, missing = _completeness(fields)
        conn.execute("""
            UPDATE cash_flow SET
                completeness_pct    = ?,
                missing_fields_json = ?
            WHERE rowid = ?
        """, (comp, json.dumps(missing), rowid))

    conn.commit()
    print(f"  ✅ reconcile cash_flow: {len(rows)} rows for {symbol}")


# ─────────────────────────────────────────────
# 4. ANNUAL CASHFLOW DERIVED
# ─────────────────────────────────────────────
def reconcile_annual_cashflow_derived(symbol: str, conn):
    """Delegates to cashflow_loader.rebuild_annual_cashflow_derived()."""
    conn.commit()
    from etl.load.cashflow_loader import rebuild_annual_cashflow_derived
    rebuild_annual_cashflow_derived(symbol)
    print(f"  ✅ reconcile annual_cashflow_derived: complete for {symbol}")


# ─────────────────────────────────────────────
# 5. GROWTH METRICS
# ─────────────────────────────────────────────
def reconcile_growth_metrics(symbol: str, conn):
    """
    Re-computes 3-year CAGRs from profit_and_loss + cash_flow.
    Falls back to annual_results for sales / net_profit if needed.
    """
    # ── Revenue & Net Profit CAGR from profit_and_loss ────────
    pl_rows = conn.execute("""
        SELECT period_end, sales, net_profit
        FROM profit_and_loss
        WHERE symbol = ? AND period_type = 'annual'
        ORDER BY period_end DESC
    """, (symbol,)).fetchall()

    # Fallback: annual_results
    if len(pl_rows) < 4:
        pl_rows = conn.execute("""
            SELECT period_end, sales, net_profit
            FROM annual_results
            WHERE symbol = ?
            ORDER BY period_end DESC
        """, (symbol,)).fetchall()

    if len(pl_rows) < 4:
        print(f"  ⚠️  reconcile growth_metrics: not enough data for {symbol}")
        return

    def cagr(end, start, years):
        e, s = _f(end), _f(start)
        if e is None or s is None or s <= 0:
            return None
        return round(((e / s) ** (1 / years) - 1) * 100, 2)

    sales  = [r[1] for r in pl_rows]
    profit = [r[2] for r in pl_rows]

    rev_cagr  = cagr(sales[0],  sales[3],  3)
    prof_cagr = cagr(profit[0], profit[3], 3)

    # ── EBITDA & EPS CAGR — derived from profit_and_loss ──────
    # EBITDA ≈ operating_profit + other_income + depreciation
    pl_full = conn.execute("""
        SELECT operating_profit, other_income, depreciation, eps
        FROM profit_and_loss
        WHERE symbol = ? AND period_type = 'annual'
        ORDER BY period_end DESC
        LIMIT 4
    """, (symbol,)).fetchall()

    ebitda_vals, eps_vals = [], []
    for r in pl_full:
        op, oi, dep, eps_v = map(_f, r)
        if op is not None:
            ebitda = (op or 0) + (oi or 0) + (dep or 0)
            ebitda_vals.append(ebitda)
        if eps_v is not None:
            eps_vals.append(eps_v)

    ebitda_cagr = cagr(ebitda_vals[0], ebitda_vals[3], 3) if len(ebitda_vals) >= 4 else None
    eps_cagr    = cagr(eps_vals[0],    eps_vals[3],    3) if len(eps_vals)    >= 4 else None

    # ── FCF CAGR from cash_flow ────────────────────────────────
    cf_rows = conn.execute("""
        SELECT free_cash_flow
        FROM cash_flow
        WHERE symbol = ? AND period_type = 'annual'
        ORDER BY period_end DESC
        LIMIT 4
    """, (symbol,)).fetchall()

    fcf_vals = [_f(r[0]) for r in cf_rows if _f(r[0]) is not None]
    fcf_cagr  = cagr(fcf_vals[0], fcf_vals[3], 3) if len(fcf_vals) >= 4 else None

    # ── Update growth_metrics ──────────────────────────────────
    conn.execute("""
        UPDATE growth_metrics SET
            revenue_cagr_3y    = COALESCE(revenue_cagr_3y, ?),
            net_profit_cagr_3y = COALESCE(net_profit_cagr_3y, ?),
            ebitda_cagr_3y     = COALESCE(ebitda_cagr_3y, ?),
            eps_cagr_3y        = COALESCE(eps_cagr_3y, ?),
            fcf_cagr_3y        = COALESCE(fcf_cagr_3y, ?)
        WHERE symbol = ?
          AND as_of_date = (SELECT MAX(as_of_date) FROM growth_metrics WHERE symbol = ?)
    """, (rev_cagr, prof_cagr, ebitda_cagr, eps_cagr, fcf_cagr, symbol, symbol))

    conn.commit()
    print(
        f"  ✅ reconcile growth_metrics: {symbol} "
        f"(rev={rev_cagr}, prof={prof_cagr}, ebitda={ebitda_cagr}, "
        f"eps={eps_cagr}, fcf={fcf_cagr})"
    )


# ─────────────────────────────────────────────
# 6. FUNDAMENTALS
# ─────────────────────────────────────────────
def reconcile_fundamentals(symbol: str, conn):
    bs = conn.execute("""
        SELECT total_assets, total_equity, borrowings, cash_equivalents
        FROM balance_sheet
        WHERE symbol = ?
        ORDER BY period_end DESC LIMIT 1
    """, (symbol,)).fetchone()

    if not bs:
        return

    ta, te, debt, cash = map(_f, bs)
    de_ratio = _div(debt, te)

    conn.execute("""
        UPDATE fundamentals SET
            debt_to_equity = COALESCE(debt_to_equity, ?)
        WHERE symbol = ?
    """, (de_ratio, symbol))

    conn.commit()
    print(f"  ✅ reconcile fundamentals: {symbol}")


# ─────────────────────────────────────────────
# RUN ALL
# ─────────────────────────────────────────────
def run_reconciliation(symbol: str):
    conn = get_connection()
    try:
        reconcile_profit_and_loss(symbol, conn)       # ← replaces income_statement
        reconcile_balance_sheet(symbol, conn)
        reconcile_cash_flow(symbol, conn)
        reconcile_annual_cashflow_derived(symbol, conn)
        reconcile_growth_metrics(symbol, conn)
        reconcile_fundamentals(symbol, conn)
    finally:
        conn.close()

    print(f"[RECONCILE] Complete for {symbol}")