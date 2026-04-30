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


def _completeness(row: dict, fields: list):
    missing = [k for k, v in row.items() if k in fields and v is None]
    pct = round((1 - len(missing) / len(fields)) * 100, 1) if fields else 100
    return pct, missing


# ─────────────────────────────────────────────
# 1. BALANCE SHEET (Screener-only)
# ─────────────────────────────────────────────
def reconcile_balance_sheet(symbol: str, conn):

    rows = conn.execute("""
        SELECT rowid,
               total_assets,
               total_liabilities,
               total_equity,
               borrowings,
               cash_equivalents,
               net_debt
        FROM balance_sheet
        WHERE symbol = ?
    """, (symbol,)).fetchall()

    for r in rows:
        rowid, ta, tl, te, debt, cash, net_d = r

        ta = _f(ta)
        tl = _f(tl)
        te = _f(te)
        debt = _f(debt)
        cash = _f(cash)
        net_d = _f(net_d)

        if net_d is None and debt is not None and cash is not None:
            net_d = round(debt - cash, 2)

        fields = {
            "total_assets": ta,
            "total_liabilities": tl,
            "total_equity": te,
            "borrowings": debt,
            "cash_equivalents": cash,
            "net_debt": net_d,
        }

        comp, missing = _completeness(fields, list(fields.keys()))

        conn.execute("""
            UPDATE balance_sheet SET
                net_debt = COALESCE(net_debt, ?),
                completeness_pct = ?,
                missing_fields_json = ?
            WHERE rowid = ?
        """, (net_d, comp, json.dumps(missing), rowid))

    conn.commit()
    print(f"  ✅ reconcile balance_sheet: {len(rows)} rows for {symbol}")


# ─────────────────────────────────────────────
# 2. CASH FLOW
# ─────────────────────────────────────────────
def reconcile_cash_flow(symbol: str, conn):

    rows = conn.execute("""
        SELECT rowid,
               operating_cash_flow,
               investing_cash_flow,
               financing_cash_flow,
               free_cash_flow
        FROM cash_flow
        WHERE symbol = ?
    """, (symbol,)).fetchall()

    for rowid, ocf, icf, fcf_stmt, fcf in rows:
        ocf = _f(ocf)
        icf = _f(icf)
        fcf_stmt = _f(fcf_stmt)
        fcf = _f(fcf)

        best_ocf = ocf
        best_fcf = fcf

        fields = {
            "operating_cash_flow": ocf,
            "free_cash_flow": fcf,
        }

        comp, _ = _completeness(fields, list(fields.keys()))

        conn.execute("""
            UPDATE cash_flow SET
                best_operating_cf = ?,
                best_free_cash_flow = ?,
                completeness_pct = ?
            WHERE rowid = ?
        """, (best_ocf, best_fcf, comp, rowid))

    conn.commit()
    print(f"  ✅ reconcile cash_flow: {len(rows)} rows for {symbol}")


# ─────────────────────────────────────────────
# 3. INCOME STATEMENT
# ─────────────────────────────────────────────
def reconcile_income_statement(symbol: str, conn):

    rows = conn.execute("""
        SELECT rowid,
               total_revenue,
               ebitda,
               net_income,
               depreciation_amortization,
               interest_expense,
               diluted_eps
        FROM income_statement
        WHERE symbol = ?
    """, (symbol,)).fetchall()

    for r in rows:
        rowid, rev, ebitda, ni, dep, interest, eps = r

        fields = {
            "revenue": _f(rev),
            "ebitda": _f(ebitda),
            "net_income": _f(ni),
            "depreciation": _f(dep),
            "interest": _f(interest),
            "eps": _f(eps),
        }

        comp, missing = _completeness(fields, list(fields.keys()))

        conn.execute("""
            UPDATE income_statement SET
                completeness_pct = ?,
                missing_fields_json = ?
            WHERE rowid = ?
        """, (comp, json.dumps(missing), rowid))

    conn.commit()
    print(f"  ✅ reconcile income_statement: {len(rows)} rows for {symbol}")


# ─────────────────────────────────────────────
# 4. QUARTERLY CASHFLOW DERIVED
# ─────────────────────────────────────────────
def reconcile_quarterly_cashflow(symbol: str, conn):

    rows = conn.execute("""
        SELECT rowid, net_income, dna
        FROM quarterly_cashflow_derived
        WHERE symbol = ?
    """, (symbol,)).fetchall()

    for rowid, ni, dna in rows:
        ni = _f(ni)
        dna = _f(dna)

        op_cf = None
        if ni is not None and dna is not None:
            op_cf = round(ni + dna, 2)

        conn.execute("""
            UPDATE quarterly_cashflow_derived SET
                approx_op_cf = COALESCE(approx_op_cf, ?)
            WHERE rowid = ?
        """, (op_cf, rowid))

    conn.commit()
    print(f"  ✅ reconcile quarterly_cashflow: {len(rows)} rows for {symbol}")


# ─────────────────────────────────────────────
# 5. GROWTH METRICS
# ─────────────────────────────────────────────
def reconcile_growth_metrics(symbol: str, conn):

    rows = conn.execute("""
        SELECT period_end, sales, net_profit
        FROM annual_results
        WHERE symbol = ?
        ORDER BY period_end DESC
    """, (symbol,)).fetchall()

    if len(rows) < 3:
        return

    def cagr(end, start, years):
        if not end or not start or start <= 0:
            return None
        return round(((end / start) ** (1/years) - 1) * 100, 2)

    sales = [_f(r[1]) for r in rows]
    profit = [_f(r[2]) for r in rows]

    s3 = cagr(sales[0], sales[3], 3) if len(sales) > 3 else None
    p3 = cagr(profit[0], profit[3], 3) if len(profit) > 3 else None

    conn.execute("""
        UPDATE growth_metrics SET
            revenue_cagr_3y = ?,
            net_profit_cagr_3y = ?
        WHERE symbol = ?
    """, (s3, p3, symbol))

    conn.commit()
    print(f"  ✅ reconcile growth_metrics: {symbol}")


# ─────────────────────────────────────────────
# 6. FUNDAMENTALS (FIXED — NO current_assets)
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
        reconcile_balance_sheet(symbol, conn)
        reconcile_cash_flow(symbol, conn)
        reconcile_income_statement(symbol, conn)
        reconcile_quarterly_cashflow(symbol, conn)
        reconcile_growth_metrics(symbol, conn)
        reconcile_fundamentals(symbol, conn)
    finally:
        conn.close()

    print(f"[RECONCILE] Complete for {symbol}")