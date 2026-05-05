"""
etl/load/screener_loader.py  v5.6
────────────────────────────────────────────────────────────────
Changes vs v5.5:
  FIX – raw_details_json no longer stores total/derived fields
        (\"Operating Activity > Total\", \"Net Cash Flow\", \"Capex\", etc.).
        Only the actual sub‑category line items are saved.
  FIX – sub‑labels that match known total patterns are skipped.
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

# ── HTTP helpers for schedules API (cashflow sub-items) ──────────────────────
import time as _time
try:
    import httpx as _httpx
    _USE_HTTPX = True
except ImportError:
    import requests as _requests
    _USE_HTTPX = False

_SCR_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept":          "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}


def _scr_get_json(url: str, referer: str = "", retries: int = 3) -> Optional[dict]:
    """GET a Screener API URL, return parsed JSON dict or None."""
    headers = dict(_SCR_HEADERS)
    if referer:
        headers["Referer"] = referer
    for attempt in range(retries):
        try:
            if _USE_HTTPX:
                r = _httpx.get(url, headers=headers, follow_redirects=True, timeout=20)
            else:
                r = _requests.get(url, headers=headers, timeout=20)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict) and data:
                    return data
                print(f"  warn  cf_schedules: empty response (dict but no keys) — {url}")
                return None
            print(f"  warn  cf_schedules: HTTP {r.status_code} — {url}")
        except Exception as e:
            print(f"  warn  cf_schedules attempt {attempt+1}: {e}")
        _time.sleep(1.5 * (attempt + 1))
    return None


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


# ── BS completeness fields ────────────────────────────────────
_BS_COMPLETENESS_FIELDS = [
    "equity_capital", "reserves", "total_equity",
    "borrowings", "lt_borrowings", "st_borrowings",
    "total_liabilities",
    "fixed_assets", "cwip",
    "inventories", "trade_receivables", "cash_equivalents",
    "total_assets",
    "net_debt",
]

# ── ALL expected Screener BS row labels → DB column name ─────
_BS_EXPECTED_ROWS = {
    # Liabilities
    "Equity Capital":          "equity_capital",
    "Reserves":                "reserves",
    "Borrowings":              "borrowings",
    "Long term Borrowings":    "lt_borrowings",
    "Short term Borrowings":   "st_borrowings",
    "Lease Liabilities":       "lease_liabilities",
    "Preference Capital":      "preference_capital",
    "Other Borrowings":        "other_borrowings",
    "Other Liabilities":       "other_liabilities",
    "Non controlling int":     "minority_interest",
    "Trade Payables":          "trade_payables",
    "Advance from Customers":  "advance_from_customers",
    "Other liability items":   "other_liability_items",
    "Total Liabilities":       "total_liabilities",
    # Assets
    "Fixed Assets":            "fixed_assets",
    "CWIP":                    "cwip",
    "Investments":             "investments",
    "Other Assets":            "other_assets",
    "Inventories":             "inventories",
    "Trade receivables":       "trade_receivables",
    "Cash Equivalents":        "cash_equivalents",
    "Loans n Advances":        "loans_advances",
    "Other asset items":       "other_asset_items",
    "Total Assets":            "total_assets",
}

# ── Balance sheet schema migration ───────────────────────────

def _ensure_bs_columns(conn):
    """Add any missing columns to balance_sheet (idempotent)."""
    new_cols = [
        ("lt_borrowings",          "REAL"),
        ("st_borrowings",          "REAL"),
        ("lease_liabilities",      "REAL"),
        ("preference_capital",     "REAL"),
        ("other_borrowings",       "REAL"),
        ("minority_interest",      "REAL"),
        ("trade_payables",         "REAL"),
        ("advance_from_customers", "REAL"),
        ("other_liability_items",  "REAL"),
        ("inventories",            "REAL"),
        ("trade_receivables",      "REAL"),
        ("loans_advances",         "REAL"),
        ("other_asset_items",      "REAL"),
        ("net_debt",               "REAL"),
        ("equity_capital",         "REAL"),
        ("reserves",               "REAL"),
        ("borrowings",             "REAL"),
        ("other_liabilities",      "REAL"),
        ("total_liabilities",      "REAL"),
        ("fixed_assets",           "REAL"),
        ("cwip",                   "REAL"),
        ("investments",            "REAL"),
        ("other_assets",           "REAL"),
        ("cash_equivalents",       "REAL"),
        ("total_equity",           "REAL"),
        ("total_assets",           "REAL"),
        ("completeness_pct",       "REAL"),
        ("missing_fields_json",    "TEXT"),
    ]
    added = []
    for col_name, col_type in new_cols:
        try:
            conn.execute(f"ALTER TABLE balance_sheet ADD COLUMN {col_name} {col_type}")
            added.append(col_name)
        except Exception:
            pass
    if added:
        print(f"  db-migrate balance_sheet: added columns → {', '.join(added)}")


def _bs_completeness(conn, symbol: str, period_end: str, period_type: str):
    """Recompute completeness for a BS row."""
    for col_name, col_type in [("completeness_pct", "REAL"), ("missing_fields_json", "TEXT")]:
        try:
            conn.execute(f"ALTER TABLE balance_sheet ADD COLUMN {col_name} {col_type}")
        except Exception:
            pass

    cur = conn.execute(
        f"SELECT {','.join(_BS_COMPLETENESS_FIELDS)} "
        f"FROM balance_sheet WHERE symbol=? AND period_end=? AND period_type=?",
        (symbol, period_end, period_type)
    )
    row = cur.fetchone()
    if row is None:
        return
    vals = dict(zip(_BS_COMPLETENESS_FIELDS, row))
    missing = [f for f, v in vals.items() if v is None]
    pct = round((1 - len(missing) / len(_BS_COMPLETENESS_FIELDS)) * 100, 1)
    conn.execute(
        "UPDATE balance_sheet SET completeness_pct=?, missing_fields_json=? "
        "WHERE symbol=? AND period_end=? AND period_type=?",
        (pct, json.dumps(missing), symbol, period_end, period_type)
    )


# ── Overview loader ───────────────────────────────────────────

def load_overview_from_screener(overview: dict, symbol: str):
    if not overview:
        print("  warn  overview loader: no data")
        return

    today = date.today().isoformat()
    conn  = get_connection()

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

    pb_ratio = None
    if current_price and book_value and book_value > 0:
        pb_ratio = round(current_price / book_value, 2)

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
            current_price, face_value, high_52w, low_52w,
            book_value, graham_number,
            ttm_eps, ttm_pe,
            data_source
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, as_of_date) DO UPDATE SET
            market_cap         = COALESCE(excluded.market_cap,        market_cap),
            pe_ratio           = COALESCE(excluded.pe_ratio,          pe_ratio),
            pb_ratio           = COALESCE(excluded.pb_ratio,          pb_ratio),
            roe_pct            = COALESCE(excluded.roe_pct,           roe_pct),
            roce_pct           = COALESCE(excluded.roce_pct,          roce_pct),
            dividend_yield_pct = COALESCE(excluded.dividend_yield_pct, dividend_yield_pct),
            current_price      = COALESCE(excluded.current_price,     current_price),
            face_value         = COALESCE(excluded.face_value,        face_value),
            high_52w           = COALESCE(excluded.high_52w,          high_52w),
            low_52w            = COALESCE(excluded.low_52w,           low_52w),
            book_value         = COALESCE(excluded.book_value,        book_value),
            graham_number      = COALESCE(excluded.graham_number,     graham_number),
            ttm_eps            = COALESCE(excluded.ttm_eps,           ttm_eps),
            ttm_pe             = COALESCE(excluded.ttm_pe,            ttm_pe),
            data_source = CASE WHEN data_source='yfinance' THEN 'both' ELSE 'screener' END
    """, (
        symbol, today,
        mc_cr, pe, pb_ratio,
        roe, roce,
        div_yld,
        current_price, face_value, high_52w, low_52w,
        book_value, graham,
        ttm_eps, ttm_pe,
        "screener",
    ))

    conn.commit()
    conn.close()
    print(f"  ok  overview: price={current_price} bv={book_value} "
          f"high={high_52w} low={low_52w} graham={graham} "
          f"ttm_eps={ttm_eps} ttm_pe={ttm_pe}")


# ── Quarterly results ─────────────────────────────────────────

def load_quarterly_results(df: pd.DataFrame, symbol: str):
    if not _has_data(df):
        print("  warn  quarterly_results: no data"); return

    sales_r   = _row(df, "Sales")
    exp_r     = _row(df, "Expenses")
    op_r      = _row(df, "Operating Profit")
    opm_r     = _row(df, "OPM %")
    oth_r     = _row(df, "Other Income")
    int_r     = _row(df, "Interest")
    dep_r     = _row(df, "Depreciation")
    pbt_r     = _row(df, "Profit before tax")
    tax_r     = _row(df, "Tax %")
    np_r      = _row(df, "Net Profit")
    eps_r     = _row(df, "EPS in Rs")

    conn = get_connection()
    count = 0

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue

        sales = _v(sales_r, col)
        if sales is None:
            continue

        conn.execute("""
            INSERT INTO quarterly_results (
                symbol, period_end,
                sales, expenses, operating_profit, opm_pct,
                other_income, interest, depreciation,
                profit_before_tax, tax_pct, net_profit, eps,
                data_source
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, period_end) DO UPDATE SET
                sales             = excluded.sales,
                expenses          = excluded.expenses,
                operating_profit  = excluded.operating_profit,
                opm_pct           = excluded.opm_pct,
                other_income      = excluded.other_income,
                interest          = excluded.interest,
                depreciation      = excluded.depreciation,
                profit_before_tax = excluded.profit_before_tax,
                tax_pct           = excluded.tax_pct,
                net_profit        = excluded.net_profit,
                eps               = excluded.eps,
                data_source       = 'screener'
        """, (
            symbol, period_end,
            sales,
            _v(exp_r, col), _v(op_r, col), _v(opm_r, col),
            _v(oth_r, col), _v(int_r, col), _v(dep_r, col),
            _v(pbt_r, col), _v(tax_r, col), _v(np_r,  col),
            _v(eps_r, col),
            "screener",
        ))
        count += 1

    conn.commit(); conn.close()
    print(f"  ok  quarterly_results: {count} rows")


# ── Annual results ────────────────────────────────────────────

def load_annual_results(df: pd.DataFrame, symbol: str):
    if not _has_data(df):
        print("  warn  annual_results: no data"); return

    sales_r   = _row(df, "Sales")
    exp_r     = _row(df, "Expenses")
    op_r      = _row(df, "Operating Profit")
    opm_r     = _row(df, "OPM %")
    oth_r     = _row(df, "Other Income")
    int_r     = _row(df, "Interest")
    dep_r     = _row(df, "Depreciation")
    pbt_r     = _row(df, "Profit before tax")
    tax_r     = _row(df, "Tax %")
    np_r      = _row(df, "Net Profit")
    eps_r     = _row(df, "EPS in Rs")
    div_r     = _row(df, "Dividend Payout %")

    conn = get_connection()
    count = 0

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue

        sales = _v(sales_r, col)
        if sales is None:
            continue

        conn.execute("""
            INSERT INTO annual_results (
                symbol, period_end,
                sales, expenses, operating_profit, opm_pct,
                other_income, interest, depreciation,
                profit_before_tax, tax_pct, net_profit, eps,
                dividend_payout_pct, data_source
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, period_end) DO UPDATE SET
                sales               = excluded.sales,
                expenses            = excluded.expenses,
                operating_profit    = excluded.operating_profit,
                opm_pct             = excluded.opm_pct,
                other_income        = excluded.other_income,
                interest            = excluded.interest,
                depreciation        = excluded.depreciation,
                profit_before_tax   = excluded.profit_before_tax,
                tax_pct             = excluded.tax_pct,
                net_profit          = excluded.net_profit,
                eps                 = excluded.eps,
                dividend_payout_pct = excluded.dividend_payout_pct,
                data_source         = 'screener'
        """, (
            symbol, period_end,
            sales,
            _v(exp_r, col), _v(op_r, col), _v(opm_r, col),
            _v(oth_r, col), _v(int_r, col), _v(dep_r, col),
            _v(pbt_r, col), _v(tax_r, col), _v(np_r,  col),
            _v(eps_r, col), _v(div_r, col),
            "screener",
        ))
        count += 1

    conn.commit(); conn.close()
    ttm_flag = " (incl TTM if present)" if count > 10 else ""
    print(f"  ok  annual_results: {count} rows{ttm_flag}")


# ── BS diagnostic ─────────────────────────────────────────────

def _print_bs_row_diagnostic(df: pd.DataFrame):
    scraped_index_lower = [str(idx).lower().strip() for idx in df.index]

    print(f"\n  ── Balance Sheet Row Diagnostic ──────────────────────────")
    print(f"  {'Screener Label':<30} {'DB Column':<28} {'Found?'}")
    print(f"  {'-'*30} {'-'*28} {'-'*6}")

    missing_rows = []

    for label, col in _BS_EXPECTED_ROWS.items():
        found = any(label.lower() == s for s in scraped_index_lower)
        if not found:
            found = any(label.lower() in s for s in scraped_index_lower)
        status = "✅ yes" if found else "❌ MISSING"
        print(f"  {label:<30} {col:<28} {status}")
        if not found:
            missing_rows.append(label)

    print(f"  {'─'*68}")
    if missing_rows:
        print(f"  ⚠️  {len(missing_rows)} row(s) not found in scraped data:")
        for r in missing_rows:
            print(f"       • '{r}'")
        print(f"  Tip: Run df.index.tolist() to see actual scraped row names.")
    else:
        print(f"  ✅ All {len(_BS_EXPECTED_ROWS)} expected rows found in scraped data.")
    print(f"  ── End Diagnostic ────────────────────────────────────────\n")

    print(f"  Actual scraped row labels ({len(df.index)}):")
    for idx in df.index:
        print(f"    • '{idx}'")
    print()


# ── Balance sheet loader ──────────────────────────────────────

def load_balance_from_screener(df: pd.DataFrame, symbol: str):
    if not _has_data(df):
        print("  warn  balance_sheet screener: no data"); return

    conn = get_connection()
    _ensure_bs_columns(conn)
    conn.commit()

    _print_bs_row_diagnostic(df)

    # Liabilities side
    eq_r      = _row(df, "Equity Capital")
    res_r     = _row(df, "Reserves")
    bor_r     = _row(df, "Borrowings")
    lt_bor_r  = _row(df, "Long term Borrowings")
    st_bor_r  = _row(df, "Short term Borrowings")
    lease_r   = _row(df, "Lease Liabilities")
    pref_r    = _row(df, "Preference Capital")
    obor_r    = _row(df, "Other Borrowings")
    othl_r    = _row(df, "Other Liabilities")
    minint_r  = _row(df, "Non controlling int", "Non-controlling int", "Minority Interest", "Non Controlling Interest")
    tp_r      = _row(df, "Trade Payables")
    adv_r     = _row(df, "Advance from Customers", "Advances from Customers")
    oliab_r   = _row(df, "Other liability items", "Other Liability Items")
    totl_r    = _row(df, "Total Liabilities")

    # Assets side
    fix_r     = _row(df, "Fixed Assets", "Net Block")
    cwip_r    = _row(df, "CWIP", "Capital Work in Progress")
    inv_r     = _row(df, "Investments")
    otha_r    = _row(df, "Other Assets")
    invtry_r  = _row(df, "Inventories")
    trec_r    = _row(df, "Trade receivables", "Trade Receivables", "Debtors", "Sundry Debtors")
    cash_r    = _row(df, "Cash Equivalents", "Cash & Equivalents", "Cash and Equivalents")
    loans_r   = _row(df, "Loans n Advances", "Loans and Advances", "Loans & Advances")
    oasset_r  = _row(df, "Other asset items", "Other Asset Items")
    tota_r    = _row(df, "Total Assets")

    print(f"  ── Balance Sheet Row Resolution ──────────────────────────")
    critical = {
        "equity_capital":   eq_r,
        "reserves":         res_r,
        "borrowings":       bor_r,
        "total_liabilities":totl_r,
        "fixed_assets":     fix_r,
        "cash_equivalents": cash_r,
        "total_assets":     tota_r,
        "minority_interest":minint_r,
        "trade_payables":   tp_r,
        "inventories":      invtry_r,
        "trade_receivables":trec_r,
    }
    for db_col, series in critical.items():
        status = "✅ resolved" if series is not None else "❌ NULL — will be missing"
        print(f"  {db_col:<25} {status}")
    print(f"  ──────────────────────────────────────────────────────────\n")

    count = 0
    for col in df.columns:
        col_str    = str(col).strip()
        period_end = _parse_period(col_str)
        if not period_end:
            continue

        mon = col_str[:3].lower()
        period_type = "annual" if mon == "mar" else (
            "half_year" if mon in ("sep","oct","nov","dec","jan","feb") else "annual"
        )

        total_assets = _v(tota_r, col)
        if total_assets is None:
            continue

        eq_cap = _v(eq_r, col)
        res    = _v(res_r, col)
        total_equity = round(eq_cap + res, 2) if (eq_cap is not None and res is not None) else None

        borrowings    = _v(bor_r, col)
        cash_eq       = _v(cash_r, col)
        net_debt = (round(borrowings - cash_eq, 2)
                    if borrowings is not None and cash_eq is not None else None)

        if count < 3:
            row_nulls = {k: v for k, v in {
                "equity_capital": _v(eq_r, col),
                "reserves":       _v(res_r, col),
                "borrowings":     borrowings,
                "lt_borrowings":  _v(lt_bor_r, col),
                "st_borrowings":  _v(st_bor_r, col),
                "total_liabilities": _v(totl_r, col),
                "fixed_assets":   _v(fix_r, col),
                "cash_equivalents": cash_eq,
                "total_assets":   total_assets,
            }.items() if v is None}
            if row_nulls:
                print(f"  warn  bs[{col_str}] NULL fields: {list(row_nulls.keys())}")

        conn.execute("""
            INSERT INTO balance_sheet (
                symbol, period_end, period_type,
                equity_capital, reserves, total_equity,
                borrowings, lt_borrowings, st_borrowings,
                lease_liabilities, preference_capital, other_borrowings,
                other_liabilities, minority_interest, trade_payables,
                advance_from_customers, other_liability_items,
                total_liabilities,
                fixed_assets, cwip, investments,
                other_assets, inventories, trade_receivables,
                cash_equivalents, loans_advances, other_asset_items,
                total_assets,
                net_debt,
                data_source
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                equity_capital          = excluded.equity_capital,
                reserves                = excluded.reserves,
                total_equity            = excluded.total_equity,
                borrowings              = excluded.borrowings,
                lt_borrowings           = excluded.lt_borrowings,
                st_borrowings           = excluded.st_borrowings,
                lease_liabilities       = excluded.lease_liabilities,
                preference_capital      = excluded.preference_capital,
                other_borrowings        = excluded.other_borrowings,
                other_liabilities       = excluded.other_liabilities,
                minority_interest       = excluded.minority_interest,
                trade_payables          = excluded.trade_payables,
                advance_from_customers  = excluded.advance_from_customers,
                other_liability_items   = excluded.other_liability_items,
                total_liabilities       = excluded.total_liabilities,
                fixed_assets            = excluded.fixed_assets,
                cwip                    = excluded.cwip,
                investments             = excluded.investments,
                other_assets            = excluded.other_assets,
                inventories             = excluded.inventories,
                trade_receivables       = excluded.trade_receivables,
                cash_equivalents        = excluded.cash_equivalents,
                loans_advances          = excluded.loans_advances,
                other_asset_items       = excluded.other_asset_items,
                total_assets            = excluded.total_assets,
                net_debt                = excluded.net_debt,
                data_source             = 'screener'
        """, (
            symbol, period_end, period_type,
            eq_cap, res, total_equity,
            borrowings, _v(lt_bor_r, col), _v(st_bor_r, col),
            _v(lease_r, col), _v(pref_r, col), _v(obor_r, col),
            _v(othl_r, col), _v(minint_r, col), _v(tp_r, col),
            _v(adv_r, col), _v(oliab_r, col),
            _v(totl_r, col),
            _v(fix_r, col), _v(cwip_r, col), _v(inv_r, col),
            _v(otha_r, col), _v(invtry_r, col), _v(trec_r, col),
            cash_eq, _v(loans_r, col), _v(oasset_r, col),
            total_assets,
            net_debt,
            "screener",
        ))

        _bs_completeness(conn, symbol, period_end, period_type)
        count += 1

    conn.commit(); conn.close()
    print(f"  ok  balance_sheet: {count} Screener rows upserted")


# ── Cash flow schedule constants ─────────────────────────────────────────────

_CF_SCHEDULE_PARENTS = [
    ("Operating Activity", "Cash+from+Operating+Activity"),
    ("Investing Activity", "Cash+from+Investing+Activity"),
    ("Financing Activity", "Cash+from+Financing+Activity"),
]

_CF_TOTAL_LABELS = {
    "Operating Activity": [
        "cash from operating activity",
        "net cash from operating activities",
        "net cash provided by operating activities",
        "total operating",
        "operating activity",
        "cash from operations",
    ],
    "Investing Activity": [
        "cash from investing activity",
        "net cash from investing activities",
        "net cash used in investing activities",
        "total investing",
        "investing activity",
        "cash from investing",
    ],
    "Financing Activity": [
        "cash from financing activity",
        "net cash from financing activities",
        "net cash used in financing activities",
        "total financing",
        "financing activity",
        "cash from financing",
    ],
}

_ALL_CF_TOTAL_LABELS = {lbl for lbls in _CF_TOTAL_LABELS.values() for lbl in lbls}

_CAPEX_LABELS = [
    "fixed assets purchased", "purchase of fixed assets",
    "purchase of property plant and equipment",
    "capital expenditure", "capex", "additions to fixed assets",
]

_CF_EXPECTED_SUB_LABELS = {
    "Operating Activity": [
        "Profit from operations",
        "Receivables",
        "Inventory",
        "Payables",
        "Loans Advances",
        "Other WC items",
        "Working capital changes",
        "Direct taxes",
    ],
    "Investing Activity": [
        "Fixed assets purchased",
        "Fixed assets sold",
        "Investments purchased",
        "Investments sold",
        "Interest received",
        "Dividends received",
        "Investment in group cos",
        "Issue of shares on acq",
        "Redemp n Canc of Shares",
        "Acquisition of companies",
        "Inter corporate deposits",
        "Other investing items",
    ],
    "Financing Activity": [
        "Proceeds from shares",
        "Redemption of debentures",
        "Proceeds from borrowings",
        "Repayment of borrowings",
        "Interest paid fin",
        "Dividends paid",
        "Financial liabilities",
        "Other financing items",
    ],
}


def _cf_clean(text) -> Optional[float]:
    if text is None:
        return None
    s = (str(text).strip()
         .replace("₹","").replace("Cr.","").replace("Cr","")
         .replace("%","").replace(",","").strip())
    if s in ("","-","—","N/A","nan","None"):
        return None
    try:
        return round(float(s), 2)
    except ValueError:
        return None


def _cf_lk(label) -> str:
    return " ".join(str(label).lower().split())


def _cf_is_total_label(label: str) -> bool:
    """Return True if this label is a section-total row (not a component)."""
    lk = _cf_lk(label)
    if lk in _ALL_CF_TOTAL_LABELS:
        return True
    for tl in _ALL_CF_TOTAL_LABELS:
        if tl in lk:
            return True
    return False


def _cf_find_total(sub_items: dict, section: str) -> Optional[float]:
    """
    Find section total from sub-items dict.
    Priority: exact match → soft match → sum of non-total items.
    Never sums the total row itself.
    """
    # 1. Exact match
    for cand in _CF_TOTAL_LABELS.get(section, []):
        for lbl, val in sub_items.items():
            if _cf_lk(lbl) == cand:
                v = _cf_clean(val)
                if v is not None:
                    return v

    # 2. Soft match
    for cand in _CF_TOTAL_LABELS.get(section, []):
        for lbl, val in sub_items.items():
            if cand in _cf_lk(lbl):
                v = _cf_clean(val)
                if v is not None:
                    return v

    # 3. Sum components only (exclude any total-row labels)
    components = [
        _cf_clean(v) for lbl, v in sub_items.items()
        if not _cf_is_total_label(lbl) and _cf_clean(v) is not None
    ]
    if components:
        return round(sum(components), 2)

    return None


def _cf_find_capex(inv_sub: dict) -> Optional[float]:
    for cand in _CAPEX_LABELS:
        for lbl, val in inv_sub.items():
            if cand in _cf_lk(lbl):
                return _cf_clean(val)
    return None


def _fetch_cf_schedules(company_id: int, consolidated: bool, symbol_nse: str) -> dict:
    """
    Fetch Operating/Investing/Financing sub-items from Screener schedules API.
    Returns { section_name: { period_label: { sub_label: float } } }
    """
    result = {}
    cons = "" if consolidated else "false"
    referer = (
        f"https://www.screener.in/company/{symbol_nse}/"
        f"{'consolidated' if consolidated else ''}/"
    )

    for section_name, parent_param in _CF_SCHEDULE_PARENTS:
        url = (
            f"https://www.screener.in/api/company/{company_id}/schedules/"
            f"?parent={parent_param}&section=cash-flow&consolidated={cons}"
        )
        print(f"  cf_schedule [{section_name}] → {url}")
        raw = _scr_get_json(url, referer=referer)
        _time.sleep(0.4)

        if not raw or not isinstance(raw, dict):
            result[section_name] = {}
            print(f"  warn  cf_schedule [{section_name}]: no data from API")
            continue

        parsed: dict = {}
        raw_sub_labels = []

        for sub_label, period_values in raw.items():
            sub_key = str(sub_label).strip()
            if not isinstance(period_values, dict):
                print(f"  warn  cf_schedule [{section_name}]: "
                      f"sub_label '{sub_key}' has non-dict value — skipped")
                continue
            raw_sub_labels.append(sub_key)

            for period_label, value in period_values.items():
                period_key = str(period_label).strip()
                if not _parse_period(period_key):
                    continue
                if period_key not in parsed:
                    parsed[period_key] = {}
                cleaned = _cf_clean(value)
                parsed[period_key][sub_key] = cleaned

        result[section_name] = parsed

        n_periods   = len(parsed)
        sample_subs = raw_sub_labels[:20]
        print(f"  ok  cf_schedule [{section_name}]: "
              f"{n_periods} periods, {len(raw_sub_labels)} sub-labels")
        print(f"       sub-labels found: {sample_subs}")

        expected = _CF_EXPECTED_SUB_LABELS.get(section_name, [])
        missing_subs = []
        for exp_lbl in expected:
            found = any(exp_lbl.lower() in sl.lower() for sl in raw_sub_labels)
            if not found:
                missing_subs.append(exp_lbl)
        if missing_subs:
            print(f"  warn  cf_schedule [{section_name}] — "
                  f"{len(missing_subs)} expected sub-labels not found: {missing_subs}")
        else:
            print(f"  ✅  cf_schedule [{section_name}] — all {len(expected)} expected sub-labels found")

    return result


# ── Cash flow from Screener (main loader) ─────────────────────

def load_cashflow_from_screener(df: pd.DataFrame, symbol: str,
                                 company_id: Optional[int] = None,
                                 consolidated: bool = True):
    """
    Load Screener cash flow data into cash_flow table.

    Phase 1 — top-level HTML table (df): cfo / cfi / cff / ncf totals.
    Phase 2 — schedules API: sub-category line items stored in
              raw_details_json as "Section > Sub Label" keys.
              capex from Investing > Fixed assets purchased.
              free_cash_flow derived as cfo + capex.
              net_cash_flow derived as cfo + cfi + cff when missing.

    v5.6: raw_details_json only contains the actual sub-items,
          no totals or derived fields.
    """
    if not _has_data(df):
        print("  warn  cash_flow screener: no data")
        return

    ocf_row  = _row(df, "Cash from Operating Activity")
    icf_row  = _row(df, "Cash from Investing Activity")
    fcf_row  = _row(df, "Cash from Financing Activity")
    ncf_row  = _row(df, "Net Cash Flow")
    fcf2_row = _row(df, "Free Cash Flow")

    print(f"\n  ── CF Phase 1: top-level HTML rows ──────────────────────")
    print(f"  Cash from Operating Activity  {'✅ found' if ocf_row is not None else '❌ NOT FOUND'}")
    print(f"  Cash from Investing Activity  {'✅ found' if icf_row is not None else '❌ NOT FOUND'}")
    print(f"  Cash from Financing Activity  {'✅ found' if fcf_row is not None else '❌ NOT FOUND'}")
    print(f"  Net Cash Flow                 {'✅ found' if ncf_row is not None else '❌ NOT FOUND'}")
    print(f"  Free Cash Flow                {'✅ found' if fcf2_row is not None else '❌ NOT FOUND'}")
    print(f"  HTML table index labels: {list(df.index)}")
    print(f"  HTML table period cols:  {list(df.columns)}")

    schedules: dict = {}
    if company_id:
        print(f"\n  ── CF Phase 2: schedules API (company_id={company_id}) ─────────")
        schedules = _fetch_cf_schedules(company_id, consolidated, symbol)
    else:
        print("  warn  cf_schedules: no company_id — sub-items will be empty")

    for sec_name, sec_data in schedules.items():
        sample_period = next(iter(sec_data), None)
        if sample_period:
            sub_labels = list(sec_data[sample_period].keys())
            print(f"  cf_schedule [{sec_name}] sample period '{sample_period}': "
                  f"{len(sub_labels)} sub-labels → {sub_labels[:8]}{'...' if len(sub_labels) > 8 else ''}")
        else:
            print(f"  warn  cf_schedule [{sec_name}]: 0 periods parsed")

    conn  = get_connection()
    count = 0
    total_sub_items_written = 0

    for col in df.columns:
        col_str    = str(col).strip()
        period_end = _parse_period(col_str)
        if not period_end:
            continue

        cfo = _v(ocf_row,  col)
        cfi = _v(icf_row,  col)
        cff = _v(fcf_row,  col)
        ncf = _v(ncf_row,  col)
        fcf = _v(fcf2_row, col)

        ops_items = schedules.get("Operating Activity", {}).get(col_str, {})
        inv_items = schedules.get("Investing Activity",  {}).get(col_str, {})
        fin_items = schedules.get("Financing Activity",  {}).get(col_str, {})

        if count < 3:
            print(f"  cf_period '{col_str}': "
                  f"ops={len(ops_items)} inv={len(inv_items)} fin={len(fin_items)} sub-items")
            if not ops_items and not inv_items and not fin_items:
                all_period_keys = set()
                for sec_data in schedules.values():
                    all_period_keys.update(sec_data.keys())
                print(f"  debug  available period keys in schedules: "
                      f"{sorted(all_period_keys)[:10]}")

        if cfo is None:
            cfo = _cf_find_total(ops_items, "Operating Activity")
        if cfi is None:
            cfi = _cf_find_total(inv_items, "Investing Activity")
        if cff is None:
            cff = _cf_find_total(fin_items, "Financing Activity")

        capex = _cf_find_capex(inv_items)

        screener_fcf = round(cfo + capex, 2) if (cfo is not None and capex is not None) else fcf
        if ncf is None and cfo is not None and cfi is not None and cff is not None:
            ncf = round(cfo + cfi + cff, 2)

        if cfo is None and screener_fcf is None:
            print(f"  skip  cf[{col_str}]: cfo and fcf both None — no usable data")
            continue

        # ── Build raw_details_json (ONLY sub‑items) ────────────────────────
        raw_detail: dict = {}

        for sec_name, sub_items in [
            ("Operating Activity", ops_items),
            ("Investing Activity",  inv_items),
            ("Financing Activity",  fin_items),
        ]:
            for sub_label, val in sub_items.items():
                # skip section‑total rows, we don't want them in raw details
                if _cf_is_total_label(sub_label):
                    continue
                raw_detail[f"{sec_name} > {sub_label}"] = val

        sub_item_count = len(raw_detail)
        total_sub_items_written += sub_item_count

        raw_json = json.dumps(raw_detail, default=str) if raw_detail else None

        conn.execute("""
            INSERT INTO cash_flow (
                symbol, period_end, period_type,
                cfo, cfi, cff, capex, free_cash_flow, net_cash_flow,
                raw_details_json, data_source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                cfo              = COALESCE(excluded.cfo,            cash_flow.cfo),
                cfi              = COALESCE(excluded.cfi,            cash_flow.cfi),
                cff              = COALESCE(excluded.cff,            cash_flow.cff),
                capex            = COALESCE(excluded.capex,          cash_flow.capex),
                free_cash_flow   = COALESCE(excluded.free_cash_flow, cash_flow.free_cash_flow),
                net_cash_flow    = COALESCE(excluded.net_cash_flow,  cash_flow.net_cash_flow),
                raw_details_json = excluded.raw_details_json,
                data_source      = excluded.data_source,
                updated_at       = CURRENT_TIMESTAMP
        """, (
            symbol, period_end, "annual",
            cfo, cfi, cff, capex, screener_fcf, ncf,
            raw_json, "screener",
        ))
        count += 1

    conn.commit()

    # ── Diagnostic (unchanged, shows sub‑items only) ─────────────────
    latest = conn.execute("""
        SELECT cfo, cfi, cff, capex, free_cash_flow, net_cash_flow, raw_details_json
        FROM cash_flow WHERE symbol = ?
        ORDER BY period_end DESC LIMIT 1
    """, (symbol,)).fetchone()

    print()
    print("  ── Cash Flow Row Diagnostic ──────────────────────────────────────────────")
    print(f"  {'Screener Label':<35} {'DB Column':<28} {'Found?'}")
    print(f"  {'-'*35} {'-'*28} {'-'*6}")

    def _diag(label: str, db_col: str, val) -> None:
        status = "✅ yes" if val is not None else "❌ NULL"
        print(f"  {label:<35} {db_col:<28} {status}")

    if latest:
        s_cfo, s_cfi, s_cff, s_capex, s_fcf, s_ncf, s_raw = latest

        _diag("Cash from Operating Activity", "cfo",            s_cfo)
        _diag("Cash from Investing Activity", "cfi",            s_cfi)
        _diag("Cash from Financing Activity", "cff",            s_cff)
        _diag("Fixed assets purchased (cap)", "capex",          s_capex)
        _diag("Free Cash Flow (cfo+capex)",   "free_cash_flow", s_fcf)
        _diag("Net Cash Flow",                "net_cash_flow",  s_ncf)

        sub_count = 0
        all_raw_keys = []
        if s_raw:
            try:
                raw = json.loads(s_raw)
                all_raw_keys = list(raw.keys())

                print(f"\n  ── Sub-item checks (raw_details_json) ──────────────────────────────────")
                for section_name, expected_subs in _CF_EXPECTED_SUB_LABELS.items():
                    for exp_sub in expected_subs:
                        full_key = f"{section_name} > {exp_sub}"
                        matched_key = None
                        matched_val = None
                        for k, v in raw.items():
                            if k == full_key or (
                                k.startswith(section_name + " > ") and
                                exp_sub.lower() in k.lower()
                            ):
                                matched_key = k
                                matched_val = v
                                break
                        status = f"✅ {matched_val}" if matched_val is not None else "❌ NULL"
                        print(f"  {full_key[:65]:<65} {status}")

                sub_count = len(all_raw_keys)
            except Exception as e:
                print(f"  warn  cf diagnostic JSON parse error: {e}")

        print(f"\n  ── Summary ──────────────────────────────────────────────────────────────")
        core_null = [f for f, v in [
            ("cfo", s_cfo), ("cfi", s_cfi), ("cff", s_cff),
            ("capex", s_capex), ("free_cash_flow", s_fcf), ("net_cash_flow", s_ncf),
        ] if v is None]
        if core_null:
            print(f"  ⚠️  Missing core fields (latest period): {core_null}")
        else:
            print(f"  ✅ All 6 core cash flow fields populated (latest period)")
        print(f"  📦 Sub-items stored in raw_details_json: {sub_count}")
        print(f"  📝 All raw_details keys ({len(all_raw_keys)}): {all_raw_keys}")
    else:
        print("  ⚠️  No cash_flow rows found for diagnostic")

    print("  ── End Cash Flow Diagnostic ──────────────────────────────────────────────")
    print()

    conn.close()
    print(f"  ok  cash_flow: {count} Screener rows upserted (periods with data) | "
          f"total sub-items written: {total_sub_items_written}")


# ── Balance sheet schedules backfill (unchanged) ──────────────────────────
# (rest of the file remains identical)
# ── Balance sheet schedules backfill ──────────────────────────

_SCHEDULE_COL_MAP = {
    "Long term Borrowings":    "lt_borrowings",
    "Short term Borrowings":   "st_borrowings",
    "Lease Liabilities":       "lease_liabilities",
    "Preference Capital":      "preference_capital",
    "Other Borrowings":        "other_borrowings",
    "Non controlling int":     "minority_interest",
    "Trade Payables":          "trade_payables",
    "Advance from Customers":  "advance_from_customers",
    "Other liability items":   "other_liability_items",
    "Inventories":             "inventories",
    "Trade receivables":       "trade_receivables",
    "Cash Equivalents":        "cash_equivalents",
    "Loans n Advances":        "loans_advances",
}

_SCHED_LABEL_MAP = [
    (["long term borrowing"],          "Long term Borrowings"),
    (["short term borrowing"],         "Short term Borrowings"),
    (["lease liabilit"],               "Lease Liabilities"),
    (["preference capital"],           "Preference Capital"),
    (["other borrowing"],              "Other Borrowings"),
    (["non controlling", "minority interest", "non-controlling"], "Non controlling int"),
    (["trade payable"],                "Trade Payables"),
    (["advance from customer"],        "Advance from Customers"),
    (["inventor"],                     "Inventories"),
    (["trade receivable", "debtors", "sundry debtor"], "Trade receivables"),
    (["cash equivalent", "cash & equiv", "cash and bank", "cash and equiv",
      "cash in hand", "bank balance", "cash & bank", "cash at bank", "balance with bank"], "Cash Equivalents"),
    (["loan", "advance"],              "Loans n Advances"),
]

def _sched_canonical(sub_label: str) -> Optional[str]:
    n = str(sub_label).lower().strip()
    for patterns, canonical in _SCHED_LABEL_MAP:
        if any(p in n for p in patterns):
            return canonical
    return None

def load_balance_schedules_backfill(schedules: dict, symbol: str):
    if not schedules:
        print("  skip  bs_backfill: no schedules data")
        return

    conn = get_connection()
    _ensure_bs_columns(conn)
    conn.commit()

    backfilled = 0
    for parent_name, period_data in schedules.items():
        for period_label, sub_dict in period_data.items():
            period_end = _parse_period(str(period_label).strip())
            if not period_end:
                continue

            mon = str(period_label).strip()[:3].lower()
            period_type = "annual" if mon == "mar" else (
                "half_year" if mon in ("sep", "oct", "nov", "dec", "jan", "feb") else "annual"
            )

            exists = conn.execute(
                "SELECT rowid FROM balance_sheet WHERE symbol=? AND period_end=? AND period_type=?",
                (symbol, period_end, period_type)
            ).fetchone()
            if not exists:
                continue

            col_vals = {}
            for sub_label, value in sub_dict.items():
                canonical = _sched_canonical(sub_label)
                if canonical is None:
                    continue
                db_col = _SCHEDULE_COL_MAP.get(canonical)
                if db_col is None:
                    continue
                fv = _safe_float(value)
                if fv is None:
                    continue
                col_vals[db_col] = round(col_vals.get(db_col, 0.0) + fv, 4)

            if not col_vals:
                continue

            set_parts = [f"{col} = COALESCE({col}, ?)" for col in col_vals]
            vals = list(col_vals.values())
            vals.extend([symbol, period_end, period_type])
            conn.execute(
                f"UPDATE balance_sheet SET {', '.join(set_parts)} "
                f"WHERE symbol=? AND period_end=? AND period_type=?",
                vals
            )
            backfilled += len(col_vals)

    rows = conn.execute(
        "SELECT period_end, period_type FROM balance_sheet WHERE symbol=?",
        (symbol,)
    ).fetchall()
    for period_end, period_type in rows:
        _bs_completeness(conn, symbol, period_end, period_type)

    conn.commit()
    conn.close()

    _backfill_net_debt(symbol)
    print(f"  ok  bs_backfill: {backfilled} sub-cells backfilled for {symbol} "
          f"(COALESCE — existing data preserved)")

def _backfill_net_debt(symbol: str):
    conn = get_connection()
    rows = conn.execute("""
        SELECT rowid, borrowings, cash_equivalents
        FROM balance_sheet
        WHERE symbol=? AND net_debt IS NULL
    """, (symbol,)).fetchall()

    updated = 0
    for rowid, bor, cash in rows:
        b = _safe_float(bor)
        c = _safe_float(cash)
        if b is not None and c is not None:
            conn.execute(
                "UPDATE balance_sheet SET net_debt=? WHERE rowid=?",
                (round(b - c, 2), rowid)
            )
            updated += 1

    conn.commit()
    conn.close()
    if updated:
        print(f"  ok  bs_backfill: net_debt recomputed for {updated} row(s)")


# ── Growth metrics ────────────────────────────────────────────

def _ensure_growth_metrics_unique(conn) -> None:
    """
    Create UNIQUE INDEX on growth_metrics(symbol, as_of_date) if it doesn't
    exist yet. Safe to call repeatedly — idempotent.
    """
    try:
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
                growth_metrics_symbol_date
            ON growth_metrics(symbol, as_of_date)
        """)
        conn.commit()
    except Exception as e:
        print(f"  warn  _ensure_growth_metrics_unique: {e}")


def load_growth_from_screener(df: pd.DataFrame, symbol: str):
    if not _has_data(df):
        print("  warn  growth screener: no data"); return

    def gv(row_name, *col_names):
        r = _row(df, row_name)
        if r is None:
            return None
        for c in col_names:
            for actual_col in df.columns:
                if str(c).lower() in str(actual_col).lower():
                    v = _v(r, actual_col)
                    if v is not None:
                        return v
        return None

    sales_10y  = gv("Sales Growth",   "10 Years", "10Y",  "10Yr")
    sales_5y   = gv("Sales Growth",   "5 Years",  "5Y",   "5Yr")
    sales_3y   = gv("Sales Growth",   "3 Years",  "3Y",   "3Yr")
    sales_ttm  = gv("Sales Growth",   "TTM")
    profit_10y = gv("Profit Growth",  "10 Years", "10Y",  "10Yr")
    profit_5y  = gv("Profit Growth",  "5 Years",  "5Y",   "5Yr")
    profit_3y  = gv("Profit Growth",  "3 Years",  "3Y",   "3Yr")
    profit_ttm = gv("Profit Growth",  "TTM")
    stock_10y  = gv("Stock Price CAGR","10 Years","10Y",  "10Yr")
    stock_5y   = gv("Stock Price CAGR","5 Years", "5Y",   "5Yr")
    stock_3y   = gv("Stock Price CAGR","3 Years", "3Y",   "3Yr")
    stock_ttm  = gv("Stock Price CAGR","TTM",     "1 Year","1Y")
    roe_10y    = gv("Return on Equity","10 Years","10Y",  "10Yr")
    roe_5y     = gv("Return on Equity","5 Years", "5Y",   "5Yr")
    roe_3y     = gv("Return on Equity","3 Years", "3Y",   "3Yr")
    roe_last   = gv("Return on Equity","TTM",     "Ttm",  "Last Year")

    print(f"  debug growth cols: {list(df.columns)}")
    print(f"  debug growth rows: {list(df.index)}")

    scr_available = 1 if any(v is not None for v in [
        sales_3y, profit_3y, sales_10y, profit_10y, roe_last
    ]) else 0

    today = date.today().isoformat()
    conn  = get_connection()
    _ensure_growth_metrics_unique(conn)
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
          f"stock_10y={stock_10y} roe_last={roe_last} available={scr_available}")


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
    bv   = _v(_row(ratios_df, "Book Value"),            col)

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
    print(f"  ok  fundamentals: Screener ratios merged | ROCE={roce} OPM={opm} WCD={wcd} "
          f"DivPayout={div_payout} BookVal={bv}")


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
    if _has_data(data.get("quarters")):
        load_quarterly_results(data["quarters"], symbol)

    if _has_data(data.get("profit_loss")):
        load_annual_results(data["profit_loss"], symbol)

    if _has_data(data.get("overview")):
        load_overview_from_screener(data["overview"], symbol)

    if _has_data(data.get("balance_sheet")):
        load_balance_from_screener(data["balance_sheet"], symbol)

    bs_schedules = data.get("bs_schedules")
    if bs_schedules:
        load_balance_schedules_backfill(bs_schedules, symbol)

    if _has_data(data.get("cash_flow")):
        _company_id  = data.get("company_id")
        _consolidated = bool(data.get("consolidated", True))
        load_cashflow_from_screener(
            data["cash_flow"], symbol,
            company_id=_company_id,
            consolidated=_consolidated,
        )

    if _has_data(data.get("ratios")):
        load_fundamentals_from_screener(data["ratios"], symbol)

    if _has_data(data.get("growth")):
        load_growth_from_screener(data["growth"], symbol)

    if _has_data(data.get("shareholding")):
        load_ownership_history(data["shareholding"], symbol)