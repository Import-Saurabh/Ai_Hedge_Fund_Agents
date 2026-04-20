"""
etl/load/screener_loader.py  v2.0
────────────────────────────────────────────────────────────────
Loads Screener.in DataFrames into the correct tables:

  quarterly_results  ← Screener "quarters" sheet   (NEW table)
  annual_results     ← Screener "profit_loss" sheet (NEW table)
  balance_sheet      ← scr_* columns merged in
  cash_flow          ← scr_* cols + best_* resolved
  growth_metrics     ← scr_* CAGR columns
  ownership_history  ← shareholding history
  fundamentals       ← ratios + TTM cols

Source priority rule (enforced here):
  • Screener values are AUTHORITATIVE for Indian listed stocks
  • yfinance values fill scr_-prefixed alternatives only
  • When both present, Screener wins for the "best_" resolved cols

All monetary values: Rs. Crores (Screener native — no conversion).
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
    """Extract and clean a value from a Screener DataFrame series."""
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
    """Find first matching row in DataFrame by substring patterns."""
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


# ── Quarterly results (Screener "Quarters") ──────────────────

def load_quarterly_results(df: pd.DataFrame, symbol: str):
    """
    Load Screener quarterly P&L into quarterly_results table.
    This is the authoritative quarterly data source.
    """
    if df is None or df.empty:
        print("  warn  quarterly_results: no data")
        return

    sales_r  = _row(df, "Sales")
    exp_r    = _row(df, "Expenses")
    op_r     = _row(df, "Operating Profit")
    opm_r    = _row(df, "OPM %")
    other_r  = _row(df, "Other Income")
    int_r    = _row(df, "Interest")
    dep_r    = _row(df, "Depreciation")
    pbt_r    = _row(df, "Profit before tax")
    tax_r    = _row(df, "Tax %")
    ni_r     = _row(df, "Net Profit")
    eps_r    = _row(df, "EPS in Rs")

    conn = get_connection()
    inserted = skipped = 0
    completeness_sum = 0.0

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue

        row = {
            "symbol":           symbol,
            "period_end":       period_end,
            "sales":            _v(sales_r,  col),
            "expenses":         _v(exp_r,    col),
            "operating_profit": _v(op_r,     col),
            "opm_pct":          _v(opm_r,    col),
            "other_income":     _v(other_r,  col),
            "interest":         _v(int_r,    col),
            "depreciation":     _v(dep_r,    col),
            "profit_before_tax":_v(pbt_r,    col),
            "tax_pct":          _v(tax_r,    col),
            "net_profit":       _v(ni_r,     col),
            "eps":              _v(eps_r,    col),
        }

        ok, reason = validate_before_insert(row, "quarterly_results")
        if not ok:
            skipped += 1
            continue

        comp, missing = compute_completeness(row, "quarterly_results")
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

    conn.commit()
    conn.close()
    avg_comp = round(completeness_sum / inserted, 1) if inserted else 0
    log_data_quality(symbol, "quarterly_results", inserted, 0, avg_comp, {}, "Screener.in")
    print(f"  ok  quarterly_results: {inserted} rows (skip={skipped}) | avg completeness {avg_comp}%")


# ── Annual results (Screener "Profit_Loss") ───────────────────

def load_annual_results(df: pd.DataFrame, symbol: str):
    """
    Load Screener annual P&L into annual_results table.
    Includes TTM column if present → stored in fundamentals.ttm_*.
    """
    if df is None or df.empty:
        print("  warn  annual_results: no data")
        return

    sales_r  = _row(df, "Sales")
    exp_r    = _row(df, "Expenses")
    op_r     = _row(df, "Operating Profit")
    opm_r    = _row(df, "OPM %")
    other_r  = _row(df, "Other Income")
    int_r    = _row(df, "Interest")
    dep_r    = _row(df, "Depreciation")
    pbt_r    = _row(df, "Profit before tax")
    tax_r    = _row(df, "Tax %")
    ni_r     = _row(df, "Net Profit")
    eps_r    = _row(df, "EPS in Rs")
    div_r    = _row(df, "Dividend Payout %")

    conn = get_connection()
    inserted = skipped = 0
    ttm_sales = ttm_net_profit = None

    for col in df.columns:
        col_str = str(col).strip()

        # Handle TTM column separately — goes to fundamentals
        if col_str.upper() == "TTM":
            ttm_sales      = _v(sales_r, col)
            ttm_net_profit = _v(ni_r,    col)
            continue

        period_end = _parse_period(col_str)
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
            "dividend_payout_pct": _v(div_r,  col),
        }

        ok, reason = validate_before_insert(row, "annual_results")
        if not ok:
            skipped += 1
            continue

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

    # Write TTM values into today's fundamentals row
    if ttm_sales is not None or ttm_net_profit is not None:
        _upsert_fundamentals_ttm(conn, symbol, ttm_sales, ttm_net_profit)

    conn.commit()
    conn.close()
    log_data_quality(symbol, "annual_results", inserted, 0, 0, {}, "Screener.in")
    print(f"  ok  annual_results: {inserted} rows (skip={skipped})"
          f"{' | TTM sales='+str(ttm_sales) if ttm_sales else ''}")


def _upsert_fundamentals_ttm(conn, symbol: str, ttm_sales, ttm_net_profit):
    today = date.today().isoformat()
    conn.execute("""
        INSERT INTO fundamentals (symbol, as_of_date, ttm_sales, ttm_net_profit, data_source)
        VALUES (?,?,?,?,?)
        ON CONFLICT(symbol, as_of_date) DO UPDATE SET
            ttm_sales=excluded.ttm_sales,
            ttm_net_profit=excluded.ttm_net_profit,
            data_source=CASE WHEN data_source='yfinance' THEN 'both' ELSE data_source END
    """, (symbol, today, ttm_sales, ttm_net_profit, "screener"))


# ── Balance sheet (scr_* merge) ───────────────────────────────

def load_balance_from_screener(df: pd.DataFrame, symbol: str):
    if df is None or df.empty:
        print("  warn  balance_sheet screener: no data"); return

    eq_r   = _row(df, "Equity Capital")
    res_r  = _row(df, "Reserves")
    bor_r  = _row(df, "Borrowings")
    othl_r = _row(df, "Other Liabilities")
    totl_r = _row(df, "Total Liabilities")
    fix_r  = _row(df, "Fixed Assets")
    cwip_r = _row(df, "CWIP")
    inv_r  = _row(df, "Investments")
    otha_r = _row(df, "Other Assets")
    tota_r = _row(df, "Total Assets")

    conn = get_connection()
    count = 0

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue
        col_str    = str(col).strip()
        period_type = "annual" if col_str.startswith("Mar") else "half_year"

        scr_total = _v(tota_r, col)
        if scr_total is None:
            continue   # skip entirely null rows

        conn.execute("""
            INSERT INTO balance_sheet (symbol, period_end, period_type,
                scr_equity_capital, scr_reserves, scr_borrowings,
                scr_other_liabilities, scr_total_liabilities,
                scr_fixed_assets, scr_cwip, scr_investments,
                scr_other_assets, scr_total_assets, data_source
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                scr_equity_capital=excluded.scr_equity_capital,
                scr_reserves=excluded.scr_reserves,
                scr_borrowings=excluded.scr_borrowings,
                scr_other_liabilities=excluded.scr_other_liabilities,
                scr_total_liabilities=excluded.scr_total_liabilities,
                scr_fixed_assets=excluded.scr_fixed_assets,
                scr_cwip=excluded.scr_cwip,
                scr_investments=excluded.scr_investments,
                scr_other_assets=excluded.scr_other_assets,
                scr_total_assets=excluded.scr_total_assets,
                data_source=CASE WHEN data_source='yfinance' THEN 'both'
                                 ELSE 'screener' END
        """, (
            symbol, period_end, period_type,
            _v(eq_r,col), _v(res_r,col), _v(bor_r,col),
            _v(othl_r,col), _v(totl_r,col),
            _v(fix_r,col), _v(cwip_r,col), _v(inv_r,col),
            _v(otha_r,col), scr_total, "screener",
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ok  balance_sheet: {count} Screener rows merged")


# ── Cash flow (scr_* merge + best_* resolution) ───────────────

def load_cashflow_from_screener(df: pd.DataFrame, symbol: str):
    """
    Merge Screener CF into cash_flow table.
    Also resolves best_* columns:
      best_operating_cf  = scr_cash_from_operating (always available)
      best_free_cash_flow = scr_free_cash_flow (always available)
    This fixes the historical NULL problem for 2014–2021.
    """
    if df is None or df.empty:
        print("  warn  cash_flow screener: no data"); return

    ocf_r  = _row(df, "Cash from Operating Activity")
    icf_r  = _row(df, "Cash from Investing Activity")
    fcf_r  = _row(df, "Cash from Financing Activity")
    ncf_r  = _row(df, "Net Cash Flow")
    fcf2_r = _row(df, "Free Cash Flow")
    cfo_r  = _row(df, "CFO/OP")

    conn  = get_connection()
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
            continue   # completely empty row — skip

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
                scr_cash_from_operating=excluded.scr_cash_from_operating,
                scr_cash_from_investing=excluded.scr_cash_from_investing,
                scr_cash_from_financing=excluded.scr_cash_from_financing,
                scr_net_cash_flow=excluded.scr_net_cash_flow,
                scr_free_cash_flow=excluded.scr_free_cash_flow,
                scr_cfo_op_pct=excluded.scr_cfo_op_pct,
                -- best_* always takes Screener value (authoritative)
                best_operating_cf=COALESCE(excluded.scr_cash_from_operating, best_operating_cf),
                best_investing_cf=COALESCE(excluded.scr_cash_from_investing, best_investing_cf),
                best_financing_cf=COALESCE(excluded.scr_cash_from_financing, best_financing_cf),
                best_free_cash_flow=COALESCE(excluded.scr_free_cash_flow, best_free_cash_flow),
                data_source=CASE WHEN data_source='yfinance' THEN 'both'
                                 ELSE 'screener' END
        """, (
            symbol, period_end, "annual",
            ocf, icf, ffcf, ncf, fcf2, cfo,
            ocf, icf, ffcf, fcf2,       # best_* = screener values
            "screener", 0,
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ok  cash_flow: {count} Screener rows merged (best_* resolved)")


# ── Growth metrics (scr_* CAGRs) ─────────────────────────────

def load_growth_from_screener(df: pd.DataFrame, symbol: str):
    """
    Load Screener compounded growth numbers into scr_* columns.
    Screener growth sheet rows: Sales Growth, Profit Growth,
    Stock Price CAGR, Return on Equity
    cols: 10 Years, 5 Years, 3 Years, TTM
    """
    if df is None or df.empty:
        print("  warn  growth_metrics screener: no data"); return

    today = date.today().isoformat()

    def gv(metric_substr: str, period_substr: str) -> Optional[float]:
        for idx in df.index:
            if metric_substr.lower() in str(idx).lower():
                for col in df.columns:
                    if period_substr.lower() in str(col).lower():
                        return _v(df.loc[idx], col)
        return None

    # Check if we have any data at all
    sales_3y = gv("Sales", "3 Year")
    profit_3y = gv("Profit", "3 Year")
    scr_available = 1 if (sales_3y is not None or profit_3y is not None) else 0

    if not scr_available:
        print("  warn  growth_metrics: Screener growth section empty/unavailable")

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
            scr_sales_cagr_10y=excluded.scr_sales_cagr_10y,
            scr_sales_cagr_5y=excluded.scr_sales_cagr_5y,
            scr_sales_cagr_3y=excluded.scr_sales_cagr_3y,
            scr_sales_ttm=excluded.scr_sales_ttm,
            scr_profit_cagr_10y=excluded.scr_profit_cagr_10y,
            scr_profit_cagr_5y=excluded.scr_profit_cagr_5y,
            scr_profit_cagr_3y=excluded.scr_profit_cagr_3y,
            scr_profit_ttm=excluded.scr_profit_ttm,
            scr_stock_cagr_10y=excluded.scr_stock_cagr_10y,
            scr_stock_cagr_5y=excluded.scr_stock_cagr_5y,
            scr_stock_cagr_3y=excluded.scr_stock_cagr_3y,
            scr_stock_ttm=excluded.scr_stock_ttm,
            scr_roe_10y=excluded.scr_roe_10y,
            scr_roe_5y=excluded.scr_roe_5y,
            scr_roe_3y=excluded.scr_roe_3y,
            scr_roe_last=excluded.scr_roe_last,
            scr_growth_available=excluded.scr_growth_available
    """, (
        symbol, today,
        gv("Sales",            "10 Year"), gv("Sales",   "5 Year"),
        gv("Sales",            "3 Year"),  gv("Sales",   "TTM"),
        gv("Profit",           "10 Year"), gv("Profit",  "5 Year"),
        gv("Profit",           "3 Year"),  gv("Profit",  "TTM"),
        gv("Stock",            "10 Year"), gv("Stock",   "5 Year"),
        gv("Stock",            "3 Year"),  gv("Stock",   "TTM"),
        gv("Return on Equity", "10 Year"), gv("Return on Equity", "5 Year"),
        gv("Return on Equity", "3 Year"),  gv("Return on Equity", "TTM"),
        scr_available,
    ))
    conn.commit()
    conn.close()
    status = "ok" if scr_available else "warn (empty)"
    print(f"  {status}  growth_metrics: Screener CAGRs saved | sales_3y={sales_3y} profit_3y={profit_3y}")


# ── Fundamentals from Screener Ratios sheet ───────────────────

def load_fundamentals_from_screener(ratios_df: pd.DataFrame, symbol: str):
    """
    Merge Screener Ratios into fundamentals for today.
    Also pulls opm_pct from latest quarterly_results row.
    """
    if ratios_df is None or ratios_df.empty:
        print("  warn  fundamentals screener ratios: no data"); return

    today = date.today().isoformat()
    col   = ratios_df.columns[-1]   # most recent period

    dso  = _v(_row(ratios_df, "Debtor Days"),            col)
    dio  = _v(_row(ratios_df, "Inventory Days"),         col)
    dpo  = _v(_row(ratios_df, "Days Payable"),           col)
    ccc  = _v(_row(ratios_df, "Cash Conversion Cycle"),  col)
    wcd  = _v(_row(ratios_df, "Working Capital Days"),   col)
    roce = _v(_row(ratios_df, "ROCE %"),                 col)

    # Fetch latest opm_pct and dividend_payout_pct from the clean tables
    conn = get_connection()

    opm = None
    div_payout = None
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
            dividend_payout_pct, data_source
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(symbol, as_of_date) DO UPDATE SET
            dso_days              = COALESCE(excluded.dso_days, dso_days),
            dio_days              = COALESCE(excluded.dio_days, dio_days),
            dpo_days              = COALESCE(excluded.dpo_days, dpo_days),
            cash_conversion_cycle = COALESCE(excluded.cash_conversion_cycle, cash_conversion_cycle),
            working_capital_days  = COALESCE(excluded.working_capital_days, working_capital_days),
            roce_pct              = COALESCE(excluded.roce_pct, roce_pct),
            opm_pct               = COALESCE(excluded.opm_pct, opm_pct),
            dividend_payout_pct   = COALESCE(excluded.dividend_payout_pct, dividend_payout_pct),
            data_source = CASE WHEN data_source='yfinance' THEN 'both' ELSE data_source END
    """, (symbol, today, dso, dio, dpo, ccc, wcd, roce, opm, div_payout, "screener"))

    conn.commit()
    conn.close()
    print(f"  ok  fundamentals: Screener ratios merged | ROCE={roce} OPM={opm} WCD={wcd}")


# ── Ownership history (shareholding) ─────────────────────────

def load_ownership_history(df: pd.DataFrame, symbol: str):
    if df is None or df.empty:
        print("  warn  ownership_history: no data"); return

    pro_r = _row(df, "Promoter")
    fii_r = _row(df, "FII")
    dii_r = _row(df, "DII")
    pub_r = _row(df, "Public")
    sha_r = _row(df, "No. of Shareholders")

    conn  = get_connection()
    count = skipped = 0

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue

        pro = _v(pro_r, col)
        if pro is None:
            skipped += 1
            continue   # promoter_pct is NOT NULL required

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
        """, (
            symbol, period_end, pro, fii, dii,
            _v(pub_r, col), inst, num_sha, "Screener.in",
        ))
        count += 1

    conn.commit()
    conn.close()
    log_data_quality(symbol, "ownership_history", count, 0, 100.0, {}, "Screener.in")
    print(f"  ok  ownership_history: {count} quarterly rows (skip={skipped})")


# ── Master dispatcher ─────────────────────────────────────────

def load_all_screener(data: dict, symbol: str):
    """
    data = dict returned by fetch_screener_data().
    Dispatches each section to the correct loader.
    """
    if data.get("quarters"):
        load_quarterly_results(data["quarters"], symbol)

    if data.get("profit_loss"):
        load_annual_results(data["profit_loss"], symbol)

    if data.get("balance_sheet"):
        load_balance_from_screener(data["balance_sheet"], symbol)

    if data.get("cash_flow"):
        load_cashflow_from_screener(data["cash_flow"], symbol)

    if data.get("ratios"):
        load_fundamentals_from_screener(data["ratios"], symbol)

    if data.get("growth"):
        load_growth_from_screener(data["growth"], symbol)

    if data.get("shareholding"):
        load_ownership_history(data["shareholding"], symbol)