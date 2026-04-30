"""
etl/load/reconcile.py  v2.0
────────────────────────────────────────────────────────────────
Fixes vs v1.0 — based on actual DB snapshot analysis:

  ISSUE 1 — balance_sheet net_debt NULL (2014–2021)
    Screener has no cash column. Fix: lookup nearest yfinance
    quarterly row with cash; compute net_debt = debt - cash.

  ISSUE 2 — quarterly_cashflow_derived: dna=NULL, approx_op_cf=NULL
    yfinance ebitda == ebit for this ticker (no D&A separated).
    Fix: pull scr_depreciation from income_statement quarterly
    rows (populated via Screener); fallback to annual_dep / 4.

  ISSUE 3 — growth_metrics: ALL scr_* NULL
    Screener growth-numbers section not found for ADANIPORTS.
    Fix: compute all CAGRs from annual_results directly.

  ISSUE 4 — fundamentals still missing after v1.0:
    low_52w       ← MIN(low) last 365 days from price_daily
    book_value    ← (scr_equity_capital + scr_reserves) * 1e7 / shares
    current_ratio ← latest quarterly balance_sheet
    quick_ratio   ← latest quarterly balance_sheet
    capex         ← latest annual cash_flow
    graham_number ← sqrt(22.5 * eps * bv)
    ttm_eps       ← sum of last 4 quarterly diluted_eps
    ttm_pe        ← current_price / ttm_eps
    forward_pe    ← current_price / earnings_estimates(0y avg_eps)
────────────────────────────────────────────────────────────────
"""

import json
import math
from typing import Optional
from database.db import get_connection


def _f(v) -> Optional[float]:
    if v is None:
        return None
    try:
        fv = float(v)
        return None if (math.isnan(fv) or math.isinf(fv)) else fv
    except Exception:
        return None


def _div(a, b) -> Optional[float]:
    a, b = _f(a), _f(b)
    if a is None or b is None or b == 0:
        return None
    return round(a / b, 4)


def _pct(num, den) -> Optional[float]:
    v = _div(num, den)
    return round(v * 100, 2) if v is not None else None


def _coalesce(*args) -> Optional[float]:
    for a in args:
        v = _f(a)
        if v is not None:
            return v
    return None


def _cagr(end_val, start_val, years) -> Optional[float]:
    e, s = _f(end_val), _f(start_val)
    if e is None or s is None or years <= 0 or s <= 0 or e <= 0:
        return None
    try:
        return round(((e / s) ** (1.0 / years) - 1) * 100, 2)
    except Exception:
        return None


def _completeness(row: dict, fields: list) -> tuple:
    missing = [f for f in fields if row.get(f) is None]
    pct = round((1 - len(missing) / len(fields)) * 100, 1) if fields else 100.0
    return pct, missing


# ══════════════════════════════════════════════════════════════
#  1. BALANCE SHEET
# ══════════════════════════════════════════════════════════════

_BS_FIELDS = [
    "total_assets", "current_assets", "total_liabilities",
    "total_equity", "total_debt", "net_debt",
    "scr_equity_capital", "scr_reserves", "scr_borrowings", "scr_total_assets",
]


def reconcile_balance_sheet(symbol: str, conn):
    # Build cash lookup from yfinance rows that have it
    cash_by_period = {}
    for row in conn.execute("""
        SELECT period_end, cash_and_equivalents, cash_equivalents
        FROM balance_sheet
        WHERE symbol = ? AND (cash_and_equivalents IS NOT NULL
                               OR cash_equivalents IS NOT NULL)
        ORDER BY period_end DESC
    """, (symbol,)).fetchall():
        c = _coalesce(row[1], row[2])
        if c is not None:
            cash_by_period[row[0]] = c

    rows = conn.execute("""
        SELECT rowid, period_end, period_type,
               total_assets, current_assets, current_liabilities,
               total_liabilities,
               total_equity, stockholders_equity,
               total_debt, net_debt,
               cash_and_equivalents, cash_equivalents, working_capital,
               scr_equity_capital, scr_reserves, scr_borrowings,
               scr_total_assets, scr_total_liabilities
        FROM balance_sheet WHERE symbol = ?
    """, (symbol,)).fetchall()

    cols = [
        "rowid","period_end","period_type",
        "total_assets","current_assets","current_liabilities",
        "total_liabilities",
        "total_equity","stockholders_equity",
        "total_debt","net_debt",
        "cash_and_equivalents","cash_equivalents","working_capital",
        "scr_equity_capital","scr_reserves","scr_borrowings",
        "scr_total_assets","scr_total_liabilities",
    ]

    updated = 0
    for raw in rows:
        r = dict(zip(cols, raw))

        ta = _coalesce(r["total_assets"],       r["scr_total_assets"])
        td = _coalesce(r["total_debt"],          r["scr_borrowings"])
        tl = _coalesce(r["total_liabilities"],   r["scr_total_liabilities"])

        scr_eq  = _f(r["scr_equity_capital"])
        scr_res = _f(r["scr_reserves"])
        scr_eq_sum = round(scr_eq + scr_res, 2) if scr_eq and scr_res else None
        te = _coalesce(r["total_equity"], r["stockholders_equity"], scr_eq_sum)

        cash = _coalesce(r["cash_and_equivalents"], r["cash_equivalents"])
        # ISSUE 1 FIX: use nearest yfinance period cash if this row has none
        if cash is None and cash_by_period:
            # Use closest (most recent with data)
            cash = next(iter(cash_by_period.values()), None)

        net_d = _f(r["net_debt"])
        if net_d is None and td is not None and cash is not None:
            net_d = round(td - cash, 2)

        ca = _f(r["current_assets"])
        cl = _f(r["current_liabilities"])
        wc = r["working_capital"]
        if wc is None and ca is not None and cl is not None:
            wc = round(ca - cl, 2)

        merged = {
            "total_assets": ta, "current_assets": ca,
            "total_liabilities": tl, "total_equity": te,
            "total_debt": td, "net_debt": net_d,
            "scr_equity_capital": r["scr_equity_capital"],
            "scr_reserves": r["scr_reserves"],
            "scr_borrowings": r["scr_borrowings"],
            "scr_total_assets": r["scr_total_assets"],
        }
        comp, missing = _completeness(merged, _BS_FIELDS)

        conn.execute("""
            UPDATE balance_sheet SET
                total_assets      = COALESCE(total_assets, ?),
                total_equity      = COALESCE(total_equity, ?),
                stockholders_equity = COALESCE(stockholders_equity, ?),
                total_debt        = COALESCE(total_debt, ?),
                total_liabilities = COALESCE(total_liabilities, ?),
                net_debt          = COALESCE(net_debt, ?),
                working_capital   = COALESCE(working_capital, ?),
                completeness_pct  = ?,
                missing_fields_json = ?
            WHERE rowid = ?
        """, (ta, te, te, td, tl, net_d, wc,
              comp, json.dumps(missing), r["rowid"]))
        updated += 1

    conn.commit()
    print(f"  ✅ reconcile balance_sheet: {updated} rows for {symbol}")


# ══════════════════════════════════════════════════════════════
#  2. CASH FLOW
# ══════════════════════════════════════════════════════════════

_CF_FIELDS = [
    "scr_cash_from_operating", "scr_cash_from_investing",
    "scr_cash_from_financing", "scr_free_cash_flow",
    "best_operating_cf", "best_free_cash_flow",
]


def reconcile_cash_flow(symbol: str, conn):
    rows = conn.execute("""
        SELECT rowid,
               operating_cash_flow, investing_cash_flow,
               financing_cash_flow, free_cash_flow,
               scr_cash_from_operating, scr_cash_from_investing,
               scr_cash_from_financing, scr_free_cash_flow
        FROM cash_flow WHERE symbol = ?
    """, (symbol,)).fetchall()

    cols = [
        "rowid",
        "operating_cash_flow","investing_cash_flow",
        "financing_cash_flow","free_cash_flow",
        "scr_cash_from_operating","scr_cash_from_investing",
        "scr_cash_from_financing","scr_free_cash_flow",
    ]

    updated = 0
    for raw in rows:
        r = dict(zip(cols, raw))

        best_ocf  = _coalesce(r["scr_cash_from_operating"],  r["operating_cash_flow"])
        best_icf  = _coalesce(r["scr_cash_from_investing"],  r["investing_cash_flow"])
        best_fstmt = _coalesce(r["scr_cash_from_financing"], r["financing_cash_flow"])
        best_fcf  = _coalesce(r["scr_free_cash_flow"],       r["free_cash_flow"])

        merged = {
            "scr_cash_from_operating": r["scr_cash_from_operating"],
            "scr_cash_from_investing":  r["scr_cash_from_investing"],
            "scr_cash_from_financing":  r["scr_cash_from_financing"],
            "scr_free_cash_flow":       r["scr_free_cash_flow"],
            "best_operating_cf":        best_ocf,
            "best_free_cash_flow":      best_fcf,
        }
        comp, _ = _completeness(merged, _CF_FIELDS)

        conn.execute("""
            UPDATE cash_flow SET
                operating_cash_flow  = COALESCE(operating_cash_flow, ?),
                investing_cash_flow  = COALESCE(investing_cash_flow, ?),
                financing_cash_flow  = COALESCE(financing_cash_flow, ?),
                free_cash_flow       = COALESCE(free_cash_flow, ?),
                best_operating_cf    = ?,
                best_investing_cf    = COALESCE(?, best_investing_cf),
                best_financing_cf    = COALESCE(?, best_financing_cf),
                best_free_cash_flow  = ?,
                completeness_pct     = ?
            WHERE rowid = ?
        """, (best_ocf, best_icf, best_fstmt, best_fcf,
              best_ocf, best_icf, best_fstmt, best_fcf,
              comp, r["rowid"]))
        updated += 1

    conn.commit()
    print(f"  ✅ reconcile cash_flow: {updated} rows for {symbol}")


# ══════════════════════════════════════════════════════════════
#  3. INCOME STATEMENT
# ══════════════════════════════════════════════════════════════

_IS_FIELDS = [
    "total_revenue","gross_profit","ebitda","operating_income",
    "net_income","depreciation_amortization","interest_expense",
    "diluted_eps","tax_rate",
]


def reconcile_income_statement(symbol: str, conn):
    scr_annual = {}
    for row in conn.execute("""
        SELECT period_end, sales, expenses, operating_profit, opm_pct,
               other_income, interest, depreciation, profit_before_tax,
               tax_pct, net_profit, eps, dividend_payout_pct
        FROM annual_results WHERE symbol = ?
    """, (symbol,)).fetchall():
        d = dict(zip(["period_end","sales","expenses","operating_profit","opm_pct",
                      "other_income","interest","depreciation","profit_before_tax",
                      "tax_pct","net_profit","eps","dividend_payout_pct"], row))
        scr_annual[d["period_end"]] = d

    scr_quarterly = {}
    for row in conn.execute("""
        SELECT period_end, sales, expenses, operating_profit, opm_pct,
               other_income, interest, depreciation, profit_before_tax,
               tax_pct, net_profit, eps
        FROM quarterly_results WHERE symbol = ?
    """, (symbol,)).fetchall():
        d = dict(zip(["period_end","sales","expenses","operating_profit","opm_pct",
                      "other_income","interest","depreciation","profit_before_tax",
                      "tax_pct","net_profit","eps"], row))
        scr_quarterly[d["period_end"]] = d

    is_rows = conn.execute("""
        SELECT rowid, period_end, period_type,
               total_revenue, gross_profit, ebitda, normalized_ebitda,
               operating_income, ebit, net_income,
               depreciation_amortization, interest_expense,
               diluted_eps, tax_rate,
               scr_sales, scr_net_profit, scr_depreciation,
               scr_operating_profit, scr_opm_pct, scr_interest,
               scr_profit_before_tax, scr_tax_pct, scr_eps,
               scr_dividend_payout_pct, scr_expenses, scr_other_income
        FROM income_statement WHERE symbol = ?
    """, (symbol,)).fetchall()

    is_cols = [
        "rowid","period_end","period_type",
        "total_revenue","gross_profit","ebitda","normalized_ebitda",
        "operating_income","ebit","net_income",
        "depreciation_amortization","interest_expense",
        "diluted_eps","tax_rate",
        "scr_sales","scr_net_profit","scr_depreciation",
        "scr_operating_profit","scr_opm_pct","scr_interest",
        "scr_profit_before_tax","scr_tax_pct","scr_eps",
        "scr_dividend_payout_pct","scr_expenses","scr_other_income",
    ]

    updated = 0
    for raw in is_rows:
        r = dict(zip(is_cols, raw))
        scr = (scr_annual.get(r["period_end"])
               if r["period_type"] == "annual"
               else scr_quarterly.get(r["period_end"]))

        scr_sales = _coalesce(r["scr_sales"],   scr["sales"]    if scr else None)
        scr_np    = _coalesce(r["scr_net_profit"], scr["net_profit"] if scr else None)
        scr_dep   = _coalesce(r["scr_depreciation"], scr["depreciation"] if scr else None)
        scr_op    = _coalesce(r["scr_operating_profit"], scr["operating_profit"] if scr else None)
        scr_opm   = _coalesce(r["scr_opm_pct"],  scr["opm_pct"]  if scr else None)
        scr_int   = _coalesce(r["scr_interest"],  scr["interest"] if scr else None)
        scr_pbt   = _coalesce(r["scr_profit_before_tax"], scr["profit_before_tax"] if scr else None)
        scr_tax   = _coalesce(r["scr_tax_pct"],   scr["tax_pct"]  if scr else None)
        scr_eps_v = _coalesce(r["scr_eps"],       scr["eps"]      if scr else None)
        scr_div   = _coalesce(r["scr_dividend_payout_pct"],
                              scr.get("dividend_payout_pct") if scr else None)
        scr_exp   = _coalesce(r["scr_expenses"],  scr["expenses"] if scr else None)
        scr_oth   = _coalesce(r["scr_other_income"], scr["other_income"] if scr else None)

        # ISSUE 2 FIX: D&A — use Screener value (authoritative)
        dep = _f(r["depreciation_amortization"])
        if dep is None and scr_dep is not None and scr_dep > 0:
            dep = scr_dep
        # ebitda - ebit only if they differ by >50 Cr (avoids yf bug where ebitda==ebit)
        if dep is None:
            eb = _coalesce(r["ebitda"], r["normalized_ebitda"])
            el = _f(r["ebit"])
            if eb is not None and el is not None and (eb - el) > 50:
                dep = round(eb - el, 2)

        total_rev = _coalesce(r["total_revenue"], scr_sales)
        net_inc   = _coalesce(r["net_income"],    scr_np)

        check = {
            "total_revenue": total_rev, "gross_profit": r["gross_profit"],
            "ebitda": _coalesce(r["ebitda"], r["normalized_ebitda"]),
            "operating_income": r["operating_income"],
            "net_income": net_inc, "depreciation_amortization": dep,
            "interest_expense": r["interest_expense"],
            "diluted_eps": r["diluted_eps"], "tax_rate": r["tax_rate"],
        }
        comp, missing = _completeness(check, _IS_FIELDS)

        conn.execute("""
            UPDATE income_statement SET
                total_revenue             = COALESCE(total_revenue, ?),
                net_income                = COALESCE(net_income, ?),
                depreciation_amortization = COALESCE(depreciation_amortization, ?),
                scr_sales=?, scr_net_profit=?, scr_depreciation=?,
                scr_operating_profit=?, scr_opm_pct=?, scr_interest=?,
                scr_profit_before_tax=?, scr_tax_pct=?, scr_eps=?,
                scr_dividend_payout_pct=?, scr_expenses=?, scr_other_income=?,
                completeness_pct=?, missing_fields_json=?
            WHERE rowid = ?
        """, (total_rev, net_inc, dep,
              scr_sales, scr_np, scr_dep, scr_op, scr_opm,
              scr_int, scr_pbt, scr_tax, scr_eps_v, scr_div,
              scr_exp, scr_oth,
              comp, json.dumps(missing), r["rowid"]))
        updated += 1

    conn.commit()
    print(f"  ✅ reconcile income_statement: {updated} rows for {symbol}")


# ══════════════════════════════════════════════════════════════
#  4. QUARTERLY CASHFLOW DERIVED  (ISSUE 2 FIX)
# ══════════════════════════════════════════════════════════════

def reconcile_quarterly_cashflow(symbol: str, conn):
    """
    Fill dna from income_statement scr_depreciation (Screener quarterly).
    approx_op_cf = NI + dna when both present.
    Fallback: annual depreciation / 4.
    """
    # Build quarterly D&A lookup from income_statement
    dep_lookup = {}
    for row in conn.execute("""
        SELECT period_end,
               COALESCE(scr_depreciation, depreciation_amortization)
        FROM income_statement
        WHERE symbol = ? AND period_type = 'quarterly'
    """, (symbol,)).fetchall():
        v = _f(row[1])
        if v and v > 0:
            dep_lookup[row[0]] = v

    # Annual dep / 4 fallback
    annual_dep_q = {}
    for row in conn.execute("""
        SELECT period_end, depreciation FROM annual_results
        WHERE symbol = ? ORDER BY period_end DESC
    """, (symbol,)).fetchall():
        v = _f(row[1])
        if v and v > 0:
            annual_dep_q[row[0]] = round(v / 4, 2)

    rows = conn.execute("""
        SELECT rowid, quarter_end, net_income, dna, revenue
        FROM quarterly_cashflow_derived WHERE symbol = ?
    """, (symbol,)).fetchall()

    updated = 0
    for rowid, qend, ni, dna, rev in rows:
        ni_v  = _f(ni)
        dna_v = _f(dna)

        if dna_v is None:
            dna_v = dep_lookup.get(qend)

        # Fallback: find annual period covering this quarter
        if dna_v is None:
            try:
                from datetime import date as _date
                q_d = _date.fromisoformat(qend)
                for ann_pe, ann_q_dep in annual_dep_q.items():
                    a_d = _date.fromisoformat(ann_pe)
                    delta = (a_d - q_d).days
                    if 0 <= delta <= 366:
                        dna_v = ann_q_dep
                        break
            except Exception:
                pass

        op_cf_v = None
        if ni_v is not None and dna_v is not None:
            op_cf_v = round(ni_v + dna_v, 2)

        quality = 2 if op_cf_v is not None else 1
        source  = "NI+DA_approx" if op_cf_v is not None else "NI_only"
        note    = ("NI + Screener D&A from quarterly IS"
                   if op_cf_v is not None
                   else "NI only — DA unavailable, op_cf not computed")

        conn.execute("""
            UPDATE quarterly_cashflow_derived SET
                dna          = ?,
                approx_op_cf = COALESCE(approx_op_cf, ?),
                quality_score = ?,
                capex_source  = ?,
                data_note     = ?
            WHERE rowid = ?
        """, (dna_v, op_cf_v, quality, source, note, rowid))
        updated += 1

    conn.commit()
    print(f"  ✅ reconcile quarterly_cashflow: {updated} rows for {symbol}")


# ══════════════════════════════════════════════════════════════
#  5. GROWTH METRICS  (ISSUE 3 FIX)
# ══════════════════════════════════════════════════════════════

_GM_FIELDS = [
    "revenue_cagr_3y","net_profit_cagr_3y","fcf_cagr_3y",
    "scr_sales_cagr_3y","scr_profit_cagr_3y","scr_roe_last",
]


def reconcile_growth_metrics(symbol: str, conn):
    """
    Compute scr_* CAGRs from annual_results when Screener
    growth-numbers section is unavailable.
    """
    ar = conn.execute("""
        SELECT period_end, sales, net_profit FROM annual_results
        WHERE symbol = ? ORDER BY period_end DESC
    """, (symbol,)).fetchall()

    if len(ar) < 2:
        print(f"  warn  reconcile growth_metrics: not enough data for {symbol}")
        return

    sales_l  = [_f(r[1]) for r in ar]
    profit_l = [_f(r[2]) for r in ar]

    def cl(lst, n):
        if len(lst) > n and lst[0] and lst[n]:
            return _cagr(lst[0], lst[n], n)
        return None

    roe_row = conn.execute("""
        SELECT roe_pct FROM fundamentals WHERE symbol = ?
        ORDER BY as_of_date DESC LIMIT 1
    """, (symbol,)).fetchone()
    roe_last = roe_row[0] if roe_row else None

    fcf_data = conn.execute("""
        SELECT best_free_cash_flow FROM cash_flow
        WHERE symbol = ? AND period_type = 'annual'
        ORDER BY period_end DESC
    """, (symbol,)).fetchall()
    fcf_l = [_f(r[0]) for r in fcf_data]

    scr_s3   = cl(sales_l,  3);  scr_s5  = cl(sales_l,  5)
    scr_s10  = cl(sales_l, 10)
    scr_p3   = cl(profit_l, 3);  scr_p5  = cl(profit_l, 5)
    scr_p10  = cl(profit_l, 10)
    scr_f3   = cl(fcf_l,   3)
    scr_s_ttm  = sales_l[0]  if sales_l  else None
    scr_p_ttm  = profit_l[0] if profit_l else None
    scr_avail  = 1 if (scr_s3 or scr_p3) else 0

    gm_rows = conn.execute(
        "SELECT rowid FROM growth_metrics WHERE symbol = ?", (symbol,)
    ).fetchall()

    for (rowid,) in gm_rows:
        conn.execute("""
            UPDATE growth_metrics SET
                scr_sales_cagr_10y  = COALESCE(scr_sales_cagr_10y,  ?),
                scr_sales_cagr_5y   = COALESCE(scr_sales_cagr_5y,   ?),
                scr_sales_cagr_3y   = COALESCE(scr_sales_cagr_3y,   ?),
                scr_sales_ttm       = COALESCE(scr_sales_ttm,        ?),
                scr_profit_cagr_10y = COALESCE(scr_profit_cagr_10y, ?),
                scr_profit_cagr_5y  = COALESCE(scr_profit_cagr_5y,  ?),
                scr_profit_cagr_3y  = COALESCE(scr_profit_cagr_3y,  ?),
                scr_profit_ttm      = COALESCE(scr_profit_ttm,       ?),
                scr_roe_last        = COALESCE(scr_roe_last,         ?),
                scr_growth_available = ?,
                completeness_pct    = ?
            WHERE rowid = ?
        """, (scr_s10, scr_s5, scr_s3, scr_s_ttm,
              scr_p10, scr_p5, scr_p3, scr_p_ttm,
              roe_last, scr_avail,
              66.7 if scr_avail else 33.3,
              rowid))

    conn.commit()
    print(
        f"  ✅ reconcile growth_metrics: sales_3y={scr_s3} "
        f"profit_3y={scr_p3} 10y_sales={scr_s10} "
        f"10y_profit={scr_p10} avail={scr_avail}"
    )


# ══════════════════════════════════════════════════════════════
#  6. FUNDAMENTALS  (ISSUES 1 + 4 FIX)
# ══════════════════════════════════════════════════════════════

_FUND_FIELDS = [
    "roe_pct","roce_pct","pe_ratio","pb_ratio",
    "revenue","net_income","market_cap",
    "opm_pct","dividend_payout_pct",
    "free_cash_flow","ebitda",
    "debt_to_equity","current_ratio",
]


def reconcile_fundamentals(symbol: str, conn):
    # Latest annual IS
    is_row = conn.execute("""
        SELECT total_revenue, gross_profit, ebitda, normalized_ebitda,
               operating_income, ebit, net_income, depreciation_amortization,
               interest_expense, diluted_eps, basic_eps, tax_rate,
               scr_sales, scr_net_profit, scr_interest
        FROM income_statement
        WHERE symbol = ? AND period_type = 'annual'
        ORDER BY period_end DESC LIMIT 1
    """, (symbol,)).fetchone()
    IS = dict(zip([
        "total_revenue","gross_profit","ebitda","normalized_ebitda",
        "operating_income","ebit","net_income","depreciation_amortization",
        "interest_expense","diluted_eps","basic_eps","tax_rate",
        "scr_sales","scr_net_profit","scr_interest"
    ], is_row)) if is_row else {}

    revenue  = _coalesce(IS.get("total_revenue"),  IS.get("scr_sales"))
    net_inc  = _coalesce(IS.get("net_income"),      IS.get("scr_net_profit"))
    ebitda_v = _coalesce(IS.get("ebitda"),          IS.get("normalized_ebitda"))
    ebit_v   = _f(IS.get("ebit"))
    int_exp  = _coalesce(IS.get("interest_expense"), IS.get("scr_interest"))
    gp       = _f(IS.get("gross_profit"))
    eps_ann  = _coalesce(IS.get("diluted_eps"), IS.get("basic_eps"))

    # Latest annual BS
    bs_row = conn.execute("""
        SELECT total_assets, current_assets, current_liabilities,
               total_equity, stockholders_equity, total_debt, net_debt,
               cash_and_equivalents, cash_equivalents, inventory,
               scr_borrowings, scr_equity_capital, scr_reserves,
               scr_total_assets, shares_issued
        FROM balance_sheet
        WHERE symbol = ? AND period_type = 'annual'
        ORDER BY period_end DESC LIMIT 1
    """, (symbol,)).fetchone()
    BS = dict(zip([
        "total_assets","current_assets","current_liabilities",
        "total_equity","stockholders_equity","total_debt","net_debt",
        "cash_and_equivalents","cash_equivalents","inventory",
        "scr_borrowings","scr_equity_capital","scr_reserves",
        "scr_total_assets","shares_issued"
    ], bs_row)) if bs_row else {}

    ta   = _coalesce(BS.get("total_assets"),      BS.get("scr_total_assets"))
    te   = _coalesce(BS.get("total_equity"),       BS.get("stockholders_equity"))
    if te is None:
        eq  = _f(BS.get("scr_equity_capital"))
        res = _f(BS.get("scr_reserves"))
        te  = round(eq + res, 2) if eq and res else None
    td   = _coalesce(BS.get("total_debt"),         BS.get("scr_borrowings"))
    cash = _coalesce(BS.get("cash_and_equivalents"), BS.get("cash_equivalents"))
    inv  = _f(BS.get("inventory"))
    nd   = _f(BS.get("net_debt"))
    if nd is None and td is not None and cash is not None:
        nd = round(td - cash, 2)

    # ISSUE 4 FIX: current_ratio/quick_ratio from latest quarterly BS
    ca = _f(BS.get("current_assets"))
    cl = _f(BS.get("current_liabilities"))
    if ca is None or cl is None:
        q_bs = conn.execute("""
            SELECT current_assets, current_liabilities, inventory
            FROM balance_sheet
            WHERE symbol = ? AND period_type = 'quarterly'
                  AND current_assets IS NOT NULL
            ORDER BY period_end DESC LIMIT 1
        """, (symbol,)).fetchone()
        if q_bs:
            ca  = _f(q_bs[0])
            cl  = _f(q_bs[1])
            inv = _coalesce(q_bs[2], inv)

    # Latest annual CF
    cf_row = conn.execute("""
        SELECT best_operating_cf, best_free_cash_flow,
               operating_cash_flow, free_cash_flow, capex
        FROM cash_flow
        WHERE symbol = ? AND period_type = 'annual'
        ORDER BY period_end DESC LIMIT 1
    """, (symbol,)).fetchone()
    CF = dict(zip([
        "best_operating_cf","best_free_cash_flow",
        "operating_cash_flow","free_cash_flow","capex"
    ], cf_row)) if cf_row else {}

    op_cf = _coalesce(CF.get("best_operating_cf"),  CF.get("operating_cash_flow"))
    fcf   = _coalesce(CF.get("best_free_cash_flow"), CF.get("free_cash_flow"))
    capex = _f(CF.get("capex"))

    # Current fundamentals row
    fund_row = conn.execute("""
        SELECT rowid, market_cap, pe_ratio, book_value,
               current_price, high_52w, low_52w
        FROM fundamentals WHERE symbol = ?
        ORDER BY as_of_date DESC LIMIT 1
    """, (symbol,)).fetchone()
    if fund_row is None:
        print(f"  warn  reconcile fundamentals: no row for {symbol}")
        return
    F = dict(zip(["rowid","market_cap","pe_ratio","book_value",
                  "current_price","high_52w","low_52w"], fund_row))
    mc    = _f(F.get("market_cap"))
    price = _f(F.get("current_price"))

    # ISSUE 4 FIX: low_52w from price_daily
    low_52w  = _f(F.get("low_52w"))
    high_52w = _f(F.get("high_52w"))
    if low_52w is None or high_52w is None:
        hl = conn.execute("""
            SELECT MIN(low), MAX(high) FROM price_daily
            WHERE symbol = ? AND date >= DATE('now', '-365 days')
        """, (symbol,)).fetchone()
        if hl and hl[0]:
            low_52w  = low_52w  or round(_f(hl[0]), 2)
            high_52w = high_52w or round(_f(hl[1]), 2)

    # ISSUE 4 FIX: book_value from Screener BS
    bv = _f(F.get("book_value"))
    if bv is None:
        eq  = _f(BS.get("scr_equity_capital"))
        res = _f(BS.get("scr_reserves"))
        sh_row = conn.execute("""
            SELECT diluted_shares FROM income_statement
            WHERE symbol = ? AND period_type = 'annual'
            ORDER BY period_end DESC LIMIT 1
        """, (symbol,)).fetchone()
        sh = _f(sh_row[0]) if sh_row else None
        if eq and res and sh and sh > 0:
            # Crores * 1e7 / shares → Rs per share
            bv = round((eq + res) * 1e7 / sh, 2)

    # Derived metrics
    gross_margin   = _pct(gp,      revenue)
    np_margin      = _pct(net_inc, revenue)
    ebitda_margin  = _pct(ebitda_v, revenue)
    ebit_margin    = _pct(ebit_v,   revenue)
    dte            = _div(td, te)
    curr_ratio     = _div(ca, cl)
    inv_use        = inv or 0.0
    quick_ratio    = _div((ca - inv_use) if ca is not None else None, cl)

    ev = None
    if mc is not None and td is not None and cash is not None:
        ev = round(mc + td - cash, 2)
    elif mc is not None and nd is not None:
        ev = round(mc + nd, 2)

    ev_ebitda = _div(ev, ebitda_v)
    ev_rev    = _div(ev, revenue)
    int_cov   = (round(abs(ebit_v / int_exp), 2)
                 if ebit_v and int_exp and int_exp != 0 else None)
    roa       = _pct(net_inc, ta)

    pb = (round(price / bv, 2) if price and bv and bv > 0 else None)

    graham = None
    if eps_ann and bv and eps_ann > 0 and bv > 0:
        try:
            graham = round(math.sqrt(22.5 * eps_ann * bv), 2)
        except Exception:
            pass

    # ISSUE 4 FIX: ttm_eps — sum last 4 quarterly diluted_eps
    ttm_eps = None
    ttm_pe  = None
    q_eps = conn.execute("""
        SELECT diluted_eps FROM income_statement
        WHERE symbol = ? AND period_type = 'quarterly'
              AND diluted_eps IS NOT NULL
        ORDER BY period_end DESC LIMIT 4
    """, (symbol,)).fetchall()
    if len(q_eps) == 4:
        vals = [_f(r[0]) for r in q_eps if _f(r[0])]
        if len(vals) == 4:
            ttm_eps = round(sum(vals), 2)
            if price and ttm_eps > 0:
                ttm_pe = round(price / ttm_eps, 2)

    # ISSUE 4 FIX: forward_pe from earnings_estimates
    fwd_pe = None
    fwd = conn.execute("""
        SELECT avg_eps FROM earnings_estimates
        WHERE symbol = ? AND period_code = '0y'
        ORDER BY snapshot_date DESC LIMIT 1
    """, (symbol,)).fetchone()
    if fwd and _f(fwd[0]) and price and _f(fwd[0]) > 0:
        fwd_pe = round(price / _f(fwd[0]), 2)

    # earnings_growth_json
    egj = None
    try:
        ni_trend = conn.execute("""
            SELECT period_end, COALESCE(net_income, scr_net_profit)
            FROM income_statement
            WHERE symbol = ? AND period_type = 'annual'
                  AND COALESCE(net_income, scr_net_profit) IS NOT NULL
            ORDER BY period_end DESC LIMIT 6
        """, (symbol,)).fetchall()
        if ni_trend:
            egj = json.dumps({r[0]: r[1] for r in ni_trend})
    except Exception:
        pass

    conn.execute("""
        UPDATE fundamentals SET
            revenue               = COALESCE(revenue, ?),
            net_income            = COALESCE(net_income, ?),
            ebitda                = COALESCE(ebitda, ?),
            free_cash_flow        = COALESCE(free_cash_flow, ?),
            operating_cf          = COALESCE(operating_cf, ?),
            capex                 = COALESCE(capex, ?),
            gross_margin_pct      = COALESCE(gross_margin_pct, ?),
            net_profit_margin_pct = COALESCE(net_profit_margin_pct, ?),
            ebitda_margin_pct     = COALESCE(ebitda_margin_pct, ?),
            ebit_margin_pct       = COALESCE(ebit_margin_pct, ?),
            debt_to_equity        = COALESCE(debt_to_equity, ?),
            current_ratio         = COALESCE(current_ratio, ?),
            quick_ratio           = COALESCE(quick_ratio, ?),
            ev                    = COALESCE(ev, ?),
            ev_ebitda             = COALESCE(ev_ebitda, ?),
            ev_revenue            = COALESCE(ev_revenue, ?),
            interest_coverage     = COALESCE(interest_coverage, ?),
            roa_pct               = COALESCE(roa_pct, ?),
            eps_annual            = COALESCE(eps_annual, ?),
            ttm_eps               = COALESCE(ttm_eps, ?),
            ttm_pe                = COALESCE(ttm_pe, ?),
            forward_pe            = COALESCE(forward_pe, ?),
            pb_ratio              = COALESCE(pb_ratio, ?),
            graham_number         = COALESCE(graham_number, ?),
            earnings_growth_json  = COALESCE(earnings_growth_json, ?),
            book_value            = COALESCE(book_value, ?),
            low_52w               = COALESCE(low_52w, ?),
            high_52w              = COALESCE(high_52w, ?)
        WHERE rowid = ?
    """, (
        revenue, net_inc, ebitda_v,
        fcf, op_cf, capex,
        gross_margin, np_margin, ebitda_margin, ebit_margin,
        dte, curr_ratio, quick_ratio,
        ev, ev_ebitda, ev_rev,
        int_cov, roa,
        eps_ann, ttm_eps, ttm_pe,
        fwd_pe, pb, graham, egj,
        bv, low_52w, high_52w,
        F["rowid"],
    ))

    # Recompute completeness
    upd = conn.execute("""
        SELECT roe_pct, roce_pct, pe_ratio, pb_ratio,
               revenue, net_income, market_cap,
               opm_pct, dividend_payout_pct,
               free_cash_flow, ebitda,
               debt_to_equity, current_ratio
        FROM fundamentals WHERE rowid = ?
    """, (F["rowid"],)).fetchone()
    if upd:
        comp, _ = _completeness(dict(zip(_FUND_FIELDS, upd)), _FUND_FIELDS)
        conn.execute(
            "UPDATE fundamentals SET completeness_pct=? WHERE rowid=?",
            (comp, F["rowid"])
        )

    conn.commit()
    print(
        f"  ✅ reconcile fundamentals: "
        f"bv={bv} low52w={low_52w} curr_ratio={curr_ratio} "
        f"ttm_eps={ttm_eps} fwd_pe={fwd_pe} "
        f"graham={graham} pb={pb}"
    )


# ══════════════════════════════════════════════════════════════
#  MASTER ENTRY POINT
# ══════════════════════════════════════════════════════════════

def run_reconciliation(symbol: str):
    """
    Dependency-ordered reconciliation passes:
      1. balance_sheet   (independent)
      2. cash_flow       (independent)
      3. income_statement (needs annual_results + quarterly_results)
      4. quarterly_cashflow (needs income_statement scr_depreciation)
      5. growth_metrics  (needs annual_results + cash_flow)
      6. fundamentals    (reads from all above)
    """
    print(f"\n[RECONCILE] Post-load reconciliation for {symbol}...")
    conn = get_connection()
    try:
        reconcile_balance_sheet(symbol, conn)
        reconcile_cash_flow(symbol, conn)
        reconcile_income_statement(symbol, conn)
        reconcile_quarterly_cashflow(symbol, conn)
        reconcile_growth_metrics(symbol, conn)
        reconcile_fundamentals(symbol, conn)
    except Exception as e:
        import traceback
        print(f"  error reconcile: {e}")
        traceback.print_exc()
    finally:
        conn.close()
    print(f"[RECONCILE] Complete for {symbol}\n")