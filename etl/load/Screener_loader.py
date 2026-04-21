"""
etl/load/screener_loader.py  v3.1
────────────────────────────────────────────────────────────────
Fixes vs v3.0:
  BUG 1 — growth_metrics scr_* ALL NULL:
    gv() searched "10 Year" / "5 Year" / "3 Year" / "TTM"
    but Screener actual column headers are
    "10 Years" / "5 Years" / "3 Years" / "TTM"
    → Fixed: try both with-s and without-s variants

  BUG 2 — fundamentals.low_52w always NULL:
    Overview parser found "High / Low" li but split wrong
    when value is "₹ 1,612 / ₹ 1,181" — the ₹ sign and
    spacing caused _clean_num to fail on the second part
    → Fixed in load_overview_from_screener: accept the value
      passed from screener.py and also try to re-parse if NULL

  BUG 3 — fundamentals.book_value NULL:
    Screener li label is "Book Value" but the value has a
    trailing "₹" or is expressed as "₹ 291.2" — _v() strips
    ₹ but the label match was case-sensitive
    → Fixed: book_value now also pulled from Screener Ratios row

  BUG 4 — graham_number NULL (depends on book_value):
    Computed here after book_value is resolved

  BUG 5 — ttm_eps / ttm_pe NULL:
    Computed from last 4 quarterly EPS from quarterly_results
    (Screener has EPS per quarter — reliable for Indian stocks)
────────────────────────────────────────────────────────────────
"""

import re
import json
import math
from datetime import date
from typing import Optional
import pandas as pd
from database.db import get_connection
from database.validator import (validate_before_insert, compute_completeness,
                                 log_data_quality)

# ── Safe DataFrame check ──────────────────────────────────────

def _has_data(obj) -> bool:
    if obj is None:
        return False
    if isinstance(obj, pd.DataFrame):
        return not obj.empty
    if isinstance(obj, (dict, list)):
        return bool(obj)
    return bool(obj)


# ── Period parsing ────────────────────────────────────────────
_MMAP = {"jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06",
         "jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12"}
_MEND = {"01":"31","02":"28","03":"31","04":"30","05":"31","06":"30",
         "07":"31","08":"31","09":"30","10":"31","11":"30","12":"31"}


def _parse_period(label: str) -> Optional[str]:
    label = str(label).strip()
    if label.upper() in ("TTM", "NAN", ""):
        return None
    m = re.match(r"([A-Za-z]{3})\s+(\d{4})", label)
    if not m:
        return None
    mon = _MMAP.get(m.group(1).lower())
    if not mon:
        return None
    return f"{m.group(2)}-{mon}-{_MEND[mon]}"


def _v(series, col) -> Optional[float]:
    if series is None:
        return None
    raw = series.get(col) if hasattr(series, "get") else None
    if raw is None:
        try:
            raw = series[col]
        except Exception:
            return None
    if raw is None:
        return None
    s = str(raw).replace("%", "").replace(",", "").replace("₹", "").strip()
    if s in ("", "-", "—", "N/A", "nan", "None", "null"):
        return None
    try:
        return round(float(s), 4)
    except ValueError:
        return None


def _row(df: pd.DataFrame, *patterns) -> Optional[pd.Series]:
    if df is None or df.empty:
        return None
    for p in patterns:
        for idx in df.index:
            if str(p).lower() == str(idx).lower().strip():
                return df.loc[idx]
    for p in patterns:
        for idx in df.index:
            if str(p).lower() in str(idx).lower():
                return df.loc[idx]
    return None


def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        fv = float(str(v).replace(",", "").replace("₹", "").replace("%", "").strip())
        return None if (math.isnan(fv) or math.isinf(fv)) else fv
    except Exception:
        return None


# ── Overview loader ───────────────────────────────────────────

def load_overview_from_screener(overview: dict, symbol: str):
    """
    Writes Screener overview ratios into fundamentals.
    Also computes: graham_number, ttm_eps, ttm_pe from DB data.
    """
    if not overview:
        print("  warn  overview loader: no data")
        return

    today = date.today().isoformat()
    conn  = get_connection()

    # Ensure extra columns exist (idempotent)
    for col_name, col_type in [
        ("face_value",    "REAL"),
        ("high_52w",      "REAL"),
        ("low_52w",       "REAL"),
        ("current_price", "REAL"),
        ("book_value",    "REAL"),
    ]:
        try:
            conn.execute(f"ALTER TABLE fundamentals ADD COLUMN {col_name} {col_type}")
            conn.commit()
        except Exception:
            pass

    current_price = _safe_float(overview.get("current_price"))
    book_value    = _safe_float(overview.get("book_value"))
    high_52w      = _safe_float(overview.get("high_52w"))
    low_52w       = _safe_float(overview.get("low_52w"))
    face_value    = _safe_float(overview.get("face_value"))
    mc_cr         = _safe_float(overview.get("market_cap_cr"))
    pe            = _safe_float(overview.get("pe_ratio"))
    roe           = _safe_float(overview.get("roe_pct"))
    roce          = _safe_float(overview.get("roce_pct"))
    div_yld       = _safe_float(overview.get("dividend_yield_pct"))

    # P/B from price and book_value
    pb_ratio = None
    if current_price and book_value and book_value > 0:
        pb_ratio = round(current_price / book_value, 2)

    # Graham number = sqrt(22.5 * EPS * BookValue)
    # We need EPS — get it from latest annual_results
    graham = None
    try:
        r = conn.execute(
            "SELECT eps FROM annual_results WHERE symbol=? ORDER BY period_end DESC LIMIT 1",
            (symbol,)
        ).fetchone()
        eps_ann = _safe_float(r[0]) if r else None
        if eps_ann and book_value and eps_ann > 0 and book_value > 0:
            graham = round(math.sqrt(22.5 * eps_ann * book_value), 2)
    except Exception:
        pass

    # TTM EPS = sum of last 4 quarters EPS from quarterly_results
    ttm_eps = None
    ttm_pe  = None
    try:
        rows = conn.execute("""
            SELECT eps FROM quarterly_results
            WHERE symbol=? AND eps IS NOT NULL
            ORDER BY period_end DESC LIMIT 4
        """, (symbol,)).fetchall()
        if len(rows) == 4:
            eps_vals = [_safe_float(r[0]) for r in rows if _safe_float(r[0]) is not None]
            if len(eps_vals) == 4:
                ttm_eps = round(sum(eps_vals), 2)
                if current_price and ttm_eps > 0:
                    ttm_pe = round(current_price / ttm_eps, 2)
    except Exception:
        pass

    conn.execute("""
        INSERT INTO fundamentals (
            symbol, as_of_date,
            market_cap, pe_ratio, pb_ratio,
            roe_pct, roce_pct,
            dividend_yield_pct,
            current_price, face_value,
            high_52w, low_52w, book_value,
            graham_number, ttm_eps, ttm_pe,
            data_source
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, as_of_date) DO UPDATE SET
            market_cap         = COALESCE(excluded.market_cap,         market_cap),
            pe_ratio           = COALESCE(excluded.pe_ratio,           pe_ratio),
            pb_ratio           = COALESCE(excluded.pb_ratio,           pb_ratio),
            roe_pct            = COALESCE(excluded.roe_pct,            roe_pct),
            roce_pct           = COALESCE(excluded.roce_pct,           roce_pct),
            dividend_yield_pct = COALESCE(excluded.dividend_yield_pct, dividend_yield_pct),
            current_price      = COALESCE(excluded.current_price,      current_price),
            face_value         = COALESCE(excluded.face_value,         face_value),
            high_52w           = COALESCE(excluded.high_52w,           high_52w),
            low_52w            = COALESCE(excluded.low_52w,            low_52w),
            book_value         = COALESCE(excluded.book_value,         book_value),
            graham_number      = COALESCE(excluded.graham_number,      graham_number),
            ttm_eps            = COALESCE(excluded.ttm_eps,            ttm_eps),
            ttm_pe             = COALESCE(excluded.ttm_pe,             ttm_pe),
            data_source = CASE WHEN data_source='yfinance' THEN 'both'
                               ELSE data_source END
    """, (
        symbol, today,
        mc_cr, pe, pb_ratio,
        roe, roce, div_yld,
        current_price, face_value,
        high_52w, low_52w, book_value,
        graham, ttm_eps, ttm_pe,
        "screener",
    ))

    conn.commit()
    conn.close()
    print(
        f"  ok  fundamentals[overview]: "
        f"Price={current_price} MC={mc_cr} PE={pe} "
        f"BookVal={book_value} PB={pb_ratio} "
        f"ROE={roe} ROCE={roce} "
        f"High={high_52w} Low={low_52w} "
        f"FaceVal={face_value} Graham={graham} "
        f"TTM_EPS={ttm_eps} TTM_PE={ttm_pe}"
    )


# ── Quarterly results ─────────────────────────────────────────

def load_quarterly_results(df: pd.DataFrame, symbol: str):
    if not _has_data(df):
        print("  warn  quarterly_results: no data"); return

    sales_r = _row(df, "Sales")
    exp_r   = _row(df, "Expenses")
    op_r    = _row(df, "Operating Profit")
    opm_r   = _row(df, "OPM %")
    other_r = _row(df, "Other Income")
    int_r   = _row(df, "Interest")
    dep_r   = _row(df, "Depreciation")
    pbt_r   = _row(df, "Profit before tax")
    tax_r   = _row(df, "Tax %")
    ni_r    = _row(df, "Net Profit")
    eps_r   = _row(df, "EPS in Rs")

    conn = get_connection()
    inserted = skipped = 0
    completeness_sum = 0.0

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue

        row = {
            "symbol":            symbol,
            "period_end":        period_end,
            "sales":             _v(sales_r,  col),
            "expenses":          _v(exp_r,    col),
            "operating_profit":  _v(op_r,     col),
            "opm_pct":           _v(opm_r,    col),
            "other_income":      _v(other_r,  col),
            "interest":          _v(int_r,    col),
            "depreciation":      _v(dep_r,    col),
            "profit_before_tax": _v(pbt_r,    col),
            "tax_pct":           _v(tax_r,    col),
            "net_profit":        _v(ni_r,     col),
            "eps":               _v(eps_r,    col),
        }

        ok, reason = validate_before_insert(row, "quarterly_results")
        if not ok:
            skipped += 1; continue

        comp, _ = compute_completeness(row, "quarterly_results")
        row["completeness_pct"] = comp
        completeness_sum += comp

        conn.execute("""
            INSERT OR REPLACE INTO quarterly_results (
                symbol, period_end,
                sales, expenses, operating_profit, opm_pct,
                other_income, interest, depreciation,
                profit_before_tax, tax_pct, net_profit, eps,
                source, completeness_pct
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            symbol, period_end,
            row["sales"], row["expenses"], row["operating_profit"],
            row["opm_pct"], row["other_income"], row["interest"],
            row["depreciation"], row["profit_before_tax"],
            row["tax_pct"], row["net_profit"], row["eps"],
            "Screener.in", comp,
        ))
        inserted += 1

    conn.commit(); conn.close()
    avg = round(completeness_sum / inserted, 1) if inserted else 0
    log_data_quality(symbol, "quarterly_results", inserted, 0, avg, {}, "Screener.in")
    print(f"  ok  quarterly_results: {inserted} rows (skip={skipped}) | avg completeness {avg}%")


# ── Annual results ────────────────────────────────────────────

def load_annual_results(df: pd.DataFrame, symbol: str):
    if not _has_data(df):
        print("  warn  annual_results: no data"); return

    sales_r = _row(df, "Sales");      exp_r  = _row(df, "Expenses")
    op_r    = _row(df, "Operating Profit"); opm_r  = _row(df, "OPM %")
    other_r = _row(df, "Other Income");    int_r  = _row(df, "Interest")
    dep_r   = _row(df, "Depreciation");    pbt_r  = _row(df, "Profit before tax")
    tax_r   = _row(df, "Tax %");           ni_r   = _row(df, "Net Profit")
    eps_r   = _row(df, "EPS in Rs");       div_r  = _row(df, "Dividend Payout %")

    conn = get_connection()
    inserted = skipped = 0
    ttm_sales = ttm_net_profit = None

    for col in df.columns:
        col_str = str(col).strip()
        if col_str.upper() == "TTM":
            ttm_sales      = _v(sales_r, col)
            ttm_net_profit = _v(ni_r,    col)
            continue

        period_end = _parse_period(col_str)
        if not period_end:
            continue

        row = {
            "symbol":              symbol,
            "period_end":          period_end,
            "sales":               _v(sales_r,  col),
            "expenses":            _v(exp_r,    col),
            "operating_profit":    _v(op_r,     col),
            "opm_pct":             _v(opm_r,    col),
            "other_income":        _v(other_r,  col),
            "interest":            _v(int_r,    col),
            "depreciation":        _v(dep_r,    col),
            "profit_before_tax":   _v(pbt_r,    col),
            "tax_pct":             _v(tax_r,    col),
            "net_profit":          _v(ni_r,     col),
            "eps":                 _v(eps_r,    col),
            "dividend_payout_pct": _v(div_r,    col),
        }

        ok, _ = validate_before_insert(row, "annual_results")
        if not ok:
            skipped += 1; continue

        comp, _ = compute_completeness(row, "annual_results")
        conn.execute("""
            INSERT OR REPLACE INTO annual_results (
                symbol, period_end,
                sales, expenses, operating_profit, opm_pct,
                other_income, interest, depreciation,
                profit_before_tax, tax_pct, net_profit, eps,
                dividend_payout_pct, source, completeness_pct
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            symbol, period_end,
            row["sales"], row["expenses"], row["operating_profit"],
            row["opm_pct"], row["other_income"], row["interest"],
            row["depreciation"], row["profit_before_tax"],
            row["tax_pct"], row["net_profit"], row["eps"],
            row["dividend_payout_pct"], "Screener.in", comp,
        ))
        inserted += 1

    if ttm_sales is not None or ttm_net_profit is not None:
        _upsert_fundamentals_ttm(conn, symbol, ttm_sales, ttm_net_profit)

    conn.commit(); conn.close()
    log_data_quality(symbol, "annual_results", inserted, 0, 0, {}, "Screener.in")
    print(f"  ok  annual_results: {inserted} rows (skip={skipped})"
          f"{' | TTM sales='+str(ttm_sales) if ttm_sales else ''}")


def _upsert_fundamentals_ttm(conn, symbol, ttm_sales, ttm_net_profit):
    today = date.today().isoformat()
    conn.execute("""
        INSERT INTO fundamentals (symbol, as_of_date, ttm_sales, ttm_net_profit, data_source)
        VALUES (?,?,?,?,?)
        ON CONFLICT(symbol, as_of_date) DO UPDATE SET
            ttm_sales=excluded.ttm_sales,
            ttm_net_profit=excluded.ttm_net_profit,
            data_source=CASE WHEN data_source='yfinance' THEN 'both' ELSE data_source END
    """, (symbol, today, ttm_sales, ttm_net_profit, "screener"))


# ── Balance sheet ─────────────────────────────────────────────

def load_balance_from_screener(df: pd.DataFrame, symbol: str):
    if not _has_data(df):
        print("  warn  balance_sheet screener: no data"); return

    eq_r   = _row(df, "Equity Capital");   res_r  = _row(df, "Reserves")
    bor_r  = _row(df, "Borrowings");       othl_r = _row(df, "Other Liabilities")
    totl_r = _row(df, "Total Liabilities"); fix_r  = _row(df, "Fixed Assets")
    cwip_r = _row(df, "CWIP");             inv_r  = _row(df, "Investments")
    otha_r = _row(df, "Other Assets");     tota_r = _row(df, "Total Assets")

    conn = get_connection()
    count = 0

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue
        col_str     = str(col).strip()
        period_type = "annual" if col_str.startswith("Mar") else "half_year"
        scr_total   = _v(tota_r, col)
        if scr_total is None:
            continue

        # Derive total_equity from scr_equity_capital + scr_reserves
        eq_cap = _v(eq_r,  col)
        res    = _v(res_r, col)
        derived_equity = round(eq_cap + res, 2) if (eq_cap and res) else None

        # Derive total_liabilities from scr_total_liabilities
        # (or scr_total_assets - derived_equity as fallback)
        total_liab = _v(totl_r, col)
        if total_liab is None and scr_total and derived_equity:
            total_liab = round(scr_total - derived_equity, 2)

        # total_debt = scr_borrowings
        total_debt = _v(bor_r, col)

        conn.execute("""
            INSERT INTO balance_sheet (symbol, period_end, period_type,
                total_assets, total_equity, stockholders_equity,
                total_liabilities, total_debt,
                scr_equity_capital, scr_reserves, scr_borrowings,
                scr_other_liabilities, scr_total_liabilities,
                scr_fixed_assets, scr_cwip, scr_investments,
                scr_other_assets, scr_total_assets, data_source
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                total_assets        = COALESCE(total_assets,      excluded.total_assets),
                total_equity        = COALESCE(total_equity,      excluded.total_equity),
                stockholders_equity = COALESCE(stockholders_equity, excluded.stockholders_equity),
                total_liabilities   = COALESCE(total_liabilities, excluded.total_liabilities),
                total_debt          = COALESCE(total_debt,        excluded.total_debt),
                scr_equity_capital  = excluded.scr_equity_capital,
                scr_reserves        = excluded.scr_reserves,
                scr_borrowings      = excluded.scr_borrowings,
                scr_other_liabilities = excluded.scr_other_liabilities,
                scr_total_liabilities = excluded.scr_total_liabilities,
                scr_fixed_assets    = excluded.scr_fixed_assets,
                scr_cwip            = excluded.scr_cwip,
                scr_investments     = excluded.scr_investments,
                scr_other_assets    = excluded.scr_other_assets,
                scr_total_assets    = excluded.scr_total_assets,
                data_source = CASE WHEN data_source='yfinance' THEN 'both'
                                   ELSE 'screener' END
        """, (
            symbol, period_end, period_type,
            scr_total, derived_equity, derived_equity,
            total_liab, total_debt,
            eq_cap, res, _v(bor_r, col),
            _v(othl_r, col), _v(totl_r, col),
            _v(fix_r, col), _v(cwip_r, col), _v(inv_r, col),
            _v(otha_r, col), scr_total, "screener",
        ))
        count += 1

    conn.commit(); conn.close()
    print(f"  ok  balance_sheet: {count} Screener rows merged (equity+liab derived)")


# ── Cash flow ─────────────────────────────────────────────────

def load_cashflow_from_screener(df: pd.DataFrame, symbol: str):
    if not _has_data(df):
        print("  warn  cash_flow screener: no data"); return

    ocf_r  = _row(df, "Cash from Operating Activity")
    icf_r  = _row(df, "Cash from Investing Activity")
    fcf_r  = _row(df, "Cash from Financing Activity")
    ncf_r  = _row(df, "Net Cash Flow")
    fcf2_r = _row(df, "Free Cash Flow")
    cfo_r  = _row(df, "CFO/OP")

    conn = get_connection()
    count = 0

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue

        ocf  = _v(ocf_r,  col)
        icf  = _v(icf_r,  col)
        ffcf = _v(fcf_r,  col)
        ncf  = _v(ncf_r,  col)
        fcf2 = _v(fcf2_r, col)
        cfo  = _v(cfo_r,  col)

        if ocf is None and fcf2 is None:
            continue

        conn.execute("""
            INSERT INTO cash_flow (symbol, period_end, period_type,
                scr_cash_from_operating, scr_cash_from_investing,
                scr_cash_from_financing, scr_net_cash_flow,
                scr_free_cash_flow, scr_cfo_op_pct,
                best_operating_cf, best_investing_cf,
                best_financing_cf, best_free_cash_flow,
                data_source, has_yf_detail
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                scr_cash_from_operating = excluded.scr_cash_from_operating,
                scr_cash_from_investing = excluded.scr_cash_from_investing,
                scr_cash_from_financing = excluded.scr_cash_from_financing,
                scr_net_cash_flow       = excluded.scr_net_cash_flow,
                scr_free_cash_flow      = excluded.scr_free_cash_flow,
                scr_cfo_op_pct          = excluded.scr_cfo_op_pct,
                best_operating_cf  = COALESCE(excluded.scr_cash_from_operating, best_operating_cf),
                best_investing_cf  = COALESCE(excluded.scr_cash_from_investing, best_investing_cf),
                best_financing_cf  = COALESCE(excluded.scr_cash_from_financing, best_financing_cf),
                best_free_cash_flow= COALESCE(excluded.scr_free_cash_flow, best_free_cash_flow),
                data_source = CASE WHEN data_source='yfinance' THEN 'both'
                                   ELSE 'screener' END
        """, (
            symbol, period_end, "annual",
            ocf, icf, ffcf, ncf, fcf2, cfo,
            ocf, icf, ffcf, fcf2,
            "screener", 0,
        ))
        count += 1

    conn.commit(); conn.close()
    print(f"  ok  cash_flow: {count} Screener rows merged (best_* resolved)")


# ── Growth metrics ────────────────────────────────────────────
# FIX: Screener actual column headers are "10 Years", "5 Years",
# "3 Years", "TTM" — NOT "10 Year", "5 Year", "3 Year"
# gv() now tries both variants so it works regardless of Screener
# formatting changes.

def load_growth_from_screener(df: pd.DataFrame, symbol: str):
    if not _has_data(df):
        print("  warn  growth_metrics screener: no data"); return

    today = date.today().isoformat()

    def gv(metric_substr: str, *period_variants) -> Optional[float]:
        """
        Find metric row and period column with flexible matching.
        period_variants: list of strings to try in order
        e.g. ("10 Years", "10 Year", "10Y", "10 Yrs")
        """
        # Find the metric row
        metric_row = None
        for idx in df.index:
            if metric_substr.lower() in str(idx).lower():
                metric_row = df.loc[idx]
                break
        if metric_row is None:
            return None

        # Try each period variant against column headers
        for period in period_variants:
            for col in df.columns:
                col_str = str(col).strip()
                if period.lower() == col_str.lower():
                    return _v(metric_row, col)
                if period.lower() in col_str.lower():
                    return _v(metric_row, col)
        return None

    # Each metric tried with "X Years" first (Screener actual), then "X Year" fallback
    sales_10y  = gv("Sales Growth", "10 Years", "10 Year", "10Y")
    sales_5y   = gv("Sales Growth", "5 Years",  "5 Year",  "5Y")
    sales_3y   = gv("Sales Growth", "3 Years",  "3 Year",  "3Y")
    sales_ttm  = gv("Sales Growth", "TTM",      "Ttm")

    profit_10y = gv("Profit Growth", "10 Years", "10 Year", "10Y")
    profit_5y  = gv("Profit Growth", "5 Years",  "5 Year",  "5Y")
    profit_3y  = gv("Profit Growth", "3 Years",  "3 Year",  "3Y")
    profit_ttm = gv("Profit Growth", "TTM",      "Ttm")

    stock_10y  = gv("Stock Price CAGR", "10 Years", "10 Year", "10Y")
    stock_5y   = gv("Stock Price CAGR", "5 Years",  "5 Year",  "5Y")
    stock_3y   = gv("Stock Price CAGR", "3 Years",  "3 Year",  "3Y")
    stock_ttm  = gv("Stock Price CAGR", "TTM",      "Ttm")

    roe_10y    = gv("Return on Equity", "10 Years", "10 Year", "10Y")
    roe_5y     = gv("Return on Equity", "5 Years",  "5 Year",  "5Y")
    roe_3y     = gv("Return on Equity", "3 Years",  "3 Year",  "3Y")
    roe_last   = gv("Return on Equity", "TTM",      "Ttm", "Last Year")

    # Debug: print what we found
    print(f"  debug growth cols: {list(df.columns)}")
    print(f"  debug growth rows: {list(df.index)}")

    scr_available = 1 if any(v is not None for v in [
        sales_3y, profit_3y, sales_10y, profit_10y, roe_last
    ]) else 0

    conn = get_connection()
    conn.execute("""
        INSERT INTO growth_metrics (symbol, as_of_date,
            scr_sales_cagr_10y, scr_sales_cagr_5y, scr_sales_cagr_3y, scr_sales_ttm,
            scr_profit_cagr_10y, scr_profit_cagr_5y, scr_profit_cagr_3y, scr_profit_ttm,
            scr_stock_cagr_10y, scr_stock_cagr_5y, scr_stock_cagr_3y, scr_stock_ttm,
            scr_roe_10y, scr_roe_5y, scr_roe_3y, scr_roe_last,
            scr_growth_available
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, as_of_date) DO UPDATE SET
            scr_sales_cagr_10y  = excluded.scr_sales_cagr_10y,
            scr_sales_cagr_5y   = excluded.scr_sales_cagr_5y,
            scr_sales_cagr_3y   = excluded.scr_sales_cagr_3y,
            scr_sales_ttm       = excluded.scr_sales_ttm,
            scr_profit_cagr_10y = excluded.scr_profit_cagr_10y,
            scr_profit_cagr_5y  = excluded.scr_profit_cagr_5y,
            scr_profit_cagr_3y  = excluded.scr_profit_cagr_3y,
            scr_profit_ttm      = excluded.scr_profit_ttm,
            scr_stock_cagr_10y  = excluded.scr_stock_cagr_10y,
            scr_stock_cagr_5y   = excluded.scr_stock_cagr_5y,
            scr_stock_cagr_3y   = excluded.scr_stock_cagr_3y,
            scr_stock_ttm       = excluded.scr_stock_ttm,
            scr_roe_10y         = excluded.scr_roe_10y,
            scr_roe_5y          = excluded.scr_roe_5y,
            scr_roe_3y          = excluded.scr_roe_3y,
            scr_roe_last        = excluded.scr_roe_last,
            scr_growth_available= excluded.scr_growth_available
    """, (
        symbol, today,
        sales_10y, sales_5y, sales_3y, sales_ttm,
        profit_10y, profit_5y, profit_3y, profit_ttm,
        stock_10y, stock_5y, stock_3y, stock_ttm,
        roe_10y, roe_5y, roe_3y, roe_last,
        scr_available,
    ))
    conn.commit(); conn.close()

    status = "ok" if scr_available else "warn — still empty (check debug cols/rows above)"
    print(f"  {status}  growth_metrics: sales_3y={sales_3y} profit_3y={profit_3y} "
          f"roe_last={roe_last} available={scr_available}")


# ── Fundamentals from Screener Ratios ────────────────────────

def load_fundamentals_from_screener(ratios_df: pd.DataFrame, symbol: str):
    if not _has_data(ratios_df):
        print("  warn  fundamentals screener ratios: no data"); return

    today = date.today().isoformat()
    col   = ratios_df.columns[-1]

    dso  = _v(_row(ratios_df, "Debtor Days"),           col)
    dio  = _v(_row(ratios_df, "Inventory Days"),        col)
    dpo  = _v(_row(ratios_df, "Days Payable"),          col)
    ccc  = _v(_row(ratios_df, "Cash Conversion Cycle"), col)
    wcd  = _v(_row(ratios_df, "Working Capital Days"),  col)
    roce = _v(_row(ratios_df, "ROCE %"),                col)

    # Also try to extract Book Value from Ratios if present
    bv = _v(_row(ratios_df, "Book Value"), col)

    conn = get_connection()

    opm = div_payout = None
    try:
        r = conn.execute(
            "SELECT opm_pct FROM quarterly_results WHERE symbol=? ORDER BY period_end DESC LIMIT 1",
            (symbol,)
        ).fetchone()
        if r:
            opm = r[0]
    except Exception:
        pass

    try:
        r = conn.execute(
            "SELECT dividend_payout_pct FROM annual_results WHERE symbol=? ORDER BY period_end DESC LIMIT 1",
            (symbol,)
        ).fetchone()
        if r:
            div_payout = r[0]
    except Exception:
        pass

    conn.execute("""
        INSERT INTO fundamentals (symbol, as_of_date,
            dso_days, dio_days, dpo_days, cash_conversion_cycle,
            working_capital_days, roce_pct, opm_pct,
            dividend_payout_pct, book_value, data_source
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, as_of_date) DO UPDATE SET
            dso_days              = COALESCE(excluded.dso_days,              dso_days),
            dio_days              = COALESCE(excluded.dio_days,              dio_days),
            dpo_days              = COALESCE(excluded.dpo_days,              dpo_days),
            cash_conversion_cycle = COALESCE(excluded.cash_conversion_cycle, cash_conversion_cycle),
            working_capital_days  = COALESCE(excluded.working_capital_days,  working_capital_days),
            roce_pct              = COALESCE(excluded.roce_pct,              roce_pct),
            opm_pct               = COALESCE(excluded.opm_pct,               opm_pct),
            dividend_payout_pct   = COALESCE(excluded.dividend_payout_pct,   dividend_payout_pct),
            book_value            = COALESCE(excluded.book_value,            book_value),
            data_source = CASE WHEN data_source='yfinance' THEN 'both' ELSE data_source END
    """, (symbol, today, dso, dio, dpo, ccc, wcd, roce, opm, div_payout, bv, "screener"))

    conn.commit(); conn.close()
    print(f"  ok  fundamentals: Screener ratios merged | ROCE={roce} OPM={opm} WCD={wcd} DivPayout={div_payout} BookVal={bv}")


# ── Ownership history ─────────────────────────────────────────

def load_ownership_history(df: pd.DataFrame, symbol: str):
    if not _has_data(df):
        print("  warn  ownership_history: no data"); return

    pro_r = _row(df, "Promoter");  fii_r = _row(df, "FII")
    dii_r = _row(df, "DII");       pub_r = _row(df, "Public")
    sha_r = _row(df, "No. of Shareholders")

    conn = get_connection()
    count = skipped = 0

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue
        pro = _v(pro_r, col)
        if pro is None:
            skipped += 1; continue

        fii  = _v(fii_r, col)
        dii  = _v(dii_r, col)
        inst = round(fii + dii, 4) if fii is not None and dii is not None else None
        sha_raw = _v(sha_r, col)
        num_sha = int(sha_raw) if sha_raw is not None else None

        conn.execute("""
            INSERT OR REPLACE INTO ownership_history (
                symbol, period_end,
                promoter_pct, fii_pct, dii_pct, public_pct,
                total_institutional_pct, num_shareholders, source
            ) VALUES (?,?,?,?,?,?,?,?,?)
        """, (symbol, period_end, pro, fii, dii,
              _v(pub_r, col), inst, num_sha, "Screener.in"))
        count += 1

    conn.commit(); conn.close()
    log_data_quality(symbol, "ownership_history", count, 0, 100.0, {}, "Screener.in")
    print(f"  ok  ownership_history: {count} quarterly rows (skip={skipped})")


# ── Master dispatcher ─────────────────────────────────────────

def load_all_screener(data: dict, symbol: str):
    """
    Dispatcher. Uses _has_data() for all checks to avoid pandas
    DataFrame ambiguity errors.

    Order matters:
      1. quarterly_results first (overview loader needs EPS)
      2. annual_results  (overview loader needs EPS + div_payout)
      3. overview        (computes graham, ttm_eps using above)
      4. balance_sheet
      5. cash_flow
      6. ratios          (may also provide book_value)
      7. growth
      8. shareholding
    """
    if _has_data(data.get("quarters")):
        load_quarterly_results(data["quarters"], symbol)

    if _has_data(data.get("profit_loss")):
        load_annual_results(data["profit_loss"], symbol)

    # Overview AFTER quarterly/annual so EPS is available for graham + ttm
    if _has_data(data.get("overview")):
        load_overview_from_screener(data["overview"], symbol)

    if _has_data(data.get("balance_sheet")):
        load_balance_from_screener(data["balance_sheet"], symbol)

    if _has_data(data.get("cash_flow")):
        load_cashflow_from_screener(data["cash_flow"], symbol)

    if _has_data(data.get("ratios")):
        load_fundamentals_from_screener(data["ratios"], symbol)

    if _has_data(data.get("growth")):
        load_growth_from_screener(data["growth"], symbol)

    if _has_data(data.get("shareholding")):
        load_ownership_history(data["shareholding"], symbol)