"""
etl/load/screener_loader.py  v1.0
────────────────────────────────────────────────────────────────
Loads Screener.in data into the following tables:
  • screener_quarters        (quarterly P&L)
  • screener_profit_loss     (annual P&L)
  • screener_balance_sheet   (annual + half-year BS)
  • screener_cash_flow       (annual CF)
  • screener_ratios          (annual efficiency ratios)
  • screener_shareholding    (quarterly ownership history)
  • screener_growth          (compounded growth rates)

All monetary values stored as-is in Rs. Crores (Screener native).
────────────────────────────────────────────────────────────────
"""

import math
import re
from typing import Optional
import pandas as pd
from database.db import get_connection


def _safe(v) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else round(f, 4)
    except Exception:
        return None


def _pct(v) -> Optional[float]:
    """Parse percentage string '65.89%' → 65.89, or pass-through float."""
    if v is None:
        return None
    s = str(v).replace("%", "").strip()
    try:
        return round(float(s), 4)
    except Exception:
        return None


MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}
MONTH_END = {
    "01": "31", "02": "28", "03": "31", "04": "30",
    "05": "31", "06": "30", "07": "31", "08": "31",
    "09": "30", "10": "31", "11": "30", "12": "31",
}


def _parse_period(label: str) -> Optional[str]:
    """'Mar 2025' → '2025-03-31', 'Sep 2025' → '2025-09-30'. TTM → None."""
    label = str(label).strip()
    if label.upper() in ("TTM", "NAN", ""):
        return None
    m = re.match(r"([A-Za-z]{3})\s+(\d{4})", label)
    if not m:
        return None
    mon = MONTH_MAP.get(m.group(1).lower())
    if not mon:
        return None
    return f"{m.group(2)}-{mon}-{MONTH_END[mon]}"


def _row(df: pd.DataFrame, *candidates) -> Optional[pd.Series]:
    if df is None or df.empty:
        return None
    for c in candidates:
        for idx in df.index:
            if str(idx).lower().strip() == c.lower().strip():
                return df.loc[idx]
    for c in candidates:
        for idx in df.index:
            if c.lower() in str(idx).lower():
                return df.loc[idx]
    return None


def _val(row, col):
    if row is None:
        return None
    v = row.get(col) if hasattr(row, "get") else (row[col] if col in row.index else None)
    if v is None:
        return None
    s = str(v).replace(",", "").replace("%", "").replace("₹", "").strip()
    if s in ("", "-", "—", "N/A", "nan", "None"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


# ── Quarters ────────────────────────────────────────────────

def load_screener_quarters(df: pd.DataFrame, symbol: str):
    if df is None or df.empty:
        print("  ⚠  screener_quarters: no data")
        return
    conn = get_connection()
    count = 0

    sales_r    = _row(df, "Sales")
    exp_r      = _row(df, "Expenses")
    op_r       = _row(df, "Operating Profit")
    opm_r      = _row(df, "OPM %")
    other_r    = _row(df, "Other Income")
    int_r      = _row(df, "Interest")
    dep_r      = _row(df, "Depreciation")
    pbt_r      = _row(df, "Profit before tax")
    tax_r      = _row(df, "Tax %")
    ni_r       = _row(df, "Net Profit")
    eps_r      = _row(df, "EPS in Rs")

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue

        conn.execute("""
            INSERT OR REPLACE INTO screener_quarters (
                symbol, period_end,
                sales, expenses, operating_profit, opm_pct,
                other_income, interest, depreciation,
                profit_before_tax, tax_pct, net_profit, eps
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            symbol, period_end,
            _safe(_val(sales_r, col)),
            _safe(_val(exp_r,   col)),
            _safe(_val(op_r,    col)),
            _safe(_val(opm_r,   col)),
            _safe(_val(other_r, col)),
            _safe(_val(int_r,   col)),
            _safe(_val(dep_r,   col)),
            _safe(_val(pbt_r,   col)),
            _safe(_val(tax_r,   col)),
            _safe(_val(ni_r,    col)),
            _safe(_val(eps_r,   col)),
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ screener_quarters: {count} rows upserted")


# ── Profit & Loss (Annual) ───────────────────────────────────

def load_screener_profit_loss(df: pd.DataFrame, symbol: str):
    if df is None or df.empty:
        print("  ⚠  screener_profit_loss: no data")
        return
    conn = get_connection()
    count = 0

    sales_r    = _row(df, "Sales")
    exp_r      = _row(df, "Expenses")
    op_r       = _row(df, "Operating Profit")
    opm_r      = _row(df, "OPM %")
    other_r    = _row(df, "Other Income")
    int_r      = _row(df, "Interest")
    dep_r      = _row(df, "Depreciation")
    pbt_r      = _row(df, "Profit before tax")
    tax_r      = _row(df, "Tax %")
    ni_r       = _row(df, "Net Profit")
    eps_r      = _row(df, "EPS in Rs")
    div_r      = _row(df, "Dividend Payout %")

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue

        conn.execute("""
            INSERT OR REPLACE INTO screener_profit_loss (
                symbol, period_end,
                sales, expenses, operating_profit, opm_pct,
                other_income, interest, depreciation,
                profit_before_tax, tax_pct, net_profit, eps,
                dividend_payout_pct
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            symbol, period_end,
            _safe(_val(sales_r,  col)),
            _safe(_val(exp_r,    col)),
            _safe(_val(op_r,     col)),
            _safe(_val(opm_r,    col)),
            _safe(_val(other_r,  col)),
            _safe(_val(int_r,    col)),
            _safe(_val(dep_r,    col)),
            _safe(_val(pbt_r,    col)),
            _safe(_val(tax_r,    col)),
            _safe(_val(ni_r,     col)),
            _safe(_val(eps_r,    col)),
            _safe(_val(div_r,    col)),
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ screener_profit_loss: {count} rows upserted")


# ── Balance Sheet ────────────────────────────────────────────

def load_screener_balance_sheet(df: pd.DataFrame, symbol: str):
    if df is None or df.empty:
        print("  ⚠  screener_balance_sheet: no data")
        return
    conn = get_connection()
    count = 0

    eq_r      = _row(df, "Equity Capital")
    res_r     = _row(df, "Reserves")
    borrow_r  = _row(df, "Borrowings")
    othl_r    = _row(df, "Other Liabilities")
    totl_r    = _row(df, "Total Liabilities")
    fixed_r   = _row(df, "Fixed Assets")
    cwip_r    = _row(df, "CWIP")
    inv_r     = _row(df, "Investments")
    otha_r    = _row(df, "Other Assets")
    tota_r    = _row(df, "Total Assets")

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue

        conn.execute("""
            INSERT OR REPLACE INTO screener_balance_sheet (
                symbol, period_end,
                equity_capital, reserves, borrowings,
                other_liabilities, total_liabilities,
                fixed_assets, cwip, investments,
                other_assets, total_assets
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            symbol, period_end,
            _safe(_val(eq_r,     col)),
            _safe(_val(res_r,    col)),
            _safe(_val(borrow_r, col)),
            _safe(_val(othl_r,   col)),
            _safe(_val(totl_r,   col)),
            _safe(_val(fixed_r,  col)),
            _safe(_val(cwip_r,   col)),
            _safe(_val(inv_r,    col)),
            _safe(_val(otha_r,   col)),
            _safe(_val(tota_r,   col)),
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ screener_balance_sheet: {count} rows upserted")


# ── Cash Flow ────────────────────────────────────────────────

def load_screener_cash_flow(df: pd.DataFrame, symbol: str):
    if df is None or df.empty:
        print("  ⚠  screener_cash_flow: no data")
        return
    conn = get_connection()
    count = 0

    ocf_r   = _row(df, "Cash from Operating Activity")
    icf_r   = _row(df, "Cash from Investing Activity")
    fcf_r_s = _row(df, "Cash from Financing Activity")
    ncf_r   = _row(df, "Net Cash Flow")
    fcf_r   = _row(df, "Free Cash Flow")
    cfo_r   = _row(df, "CFO/OP")

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue

        conn.execute("""
            INSERT OR REPLACE INTO screener_cash_flow (
                symbol, period_end,
                cash_from_operating, cash_from_investing,
                cash_from_financing, net_cash_flow,
                free_cash_flow, cfo_op_pct
            ) VALUES (?,?,?,?,?,?,?,?)
        """, (
            symbol, period_end,
            _safe(_val(ocf_r,   col)),
            _safe(_val(icf_r,   col)),
            _safe(_val(fcf_r_s, col)),
            _safe(_val(ncf_r,   col)),
            _safe(_val(fcf_r,   col)),
            _safe(_val(cfo_r,   col)),
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ screener_cash_flow: {count} rows upserted")


# ── Ratios ───────────────────────────────────────────────────

def load_screener_ratios(df: pd.DataFrame, symbol: str):
    if df is None or df.empty:
        print("  ⚠  screener_ratios: no data")
        return
    conn = get_connection()
    count = 0

    deb_r  = _row(df, "Debtor Days")
    inv_r  = _row(df, "Inventory Days")
    pay_r  = _row(df, "Days Payable")
    ccc_r  = _row(df, "Cash Conversion Cycle")
    wcd_r  = _row(df, "Working Capital Days")
    roc_r  = _row(df, "ROCE %")

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue

        conn.execute("""
            INSERT OR REPLACE INTO screener_ratios (
                symbol, period_end,
                debtor_days, inventory_days, days_payable,
                cash_conversion_cycle, working_capital_days,
                roce_pct
            ) VALUES (?,?,?,?,?,?,?,?)
        """, (
            symbol, period_end,
            _safe(_val(deb_r, col)),
            _safe(_val(inv_r, col)),
            _safe(_val(pay_r, col)),
            _safe(_val(ccc_r, col)),
            _safe(_val(wcd_r, col)),
            _safe(_val(roc_r, col)),
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ screener_ratios: {count} rows upserted")


# ── Shareholding ─────────────────────────────────────────────

def load_screener_shareholding(df: pd.DataFrame, symbol: str):
    if df is None or df.empty:
        print("  ⚠  screener_shareholding: no data")
        return
    conn = get_connection()
    count = 0

    pro_r   = _row(df, "Promoters")
    fii_r   = _row(df, "FIIs")
    dii_r   = _row(df, "DIIs")
    pub_r   = _row(df, "Public")
    sha_r   = _row(df, "No. of Shareholders")

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue

        pro = _safe(_val(pro_r, col))
        fii = _safe(_val(fii_r, col))
        dii = _safe(_val(dii_r, col))
        institutional = round(fii + dii, 4) if fii is not None and dii is not None else None
        sha_raw = _val(sha_r, col)
        num_sha = int(sha_raw) if sha_raw is not None else None

        conn.execute("""
            INSERT OR REPLACE INTO screener_shareholding (
                symbol, period_end,
                promoter_pct, fii_pct, dii_pct,
                public_pct, total_institutional_pct,
                num_shareholders
            ) VALUES (?,?,?,?,?,?,?,?)
        """, (
            symbol, period_end,
            pro, fii, dii,
            _safe(_val(pub_r, col)),
            institutional,
            num_sha,
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ screener_shareholding: {count} rows upserted")


# ── Growth (Compounded CAGR numbers) ────────────────────────

def load_screener_growth(df: pd.DataFrame, symbol: str):
    """
    Screener's growth-numbers section has metrics as rows and
    time periods (3 Years, 5 Years, 10 Years, TTM) as columns.
    We store each metric + period as a row.
    """
    if df is None or df.empty:
        print("  ⚠  screener_growth: no data")
        return
    conn = get_connection()
    count = 0

    from datetime import date
    today = date.today().isoformat()

    for metric in df.index:
        for col in df.columns:
            period_label = str(col).strip()
            raw = _val(df.loc[metric], col)
            conn.execute("""
                INSERT OR REPLACE INTO screener_growth (
                    symbol, snapshot_date, metric, period_label, value
                ) VALUES (?,?,?,?,?)
            """, (symbol, today, str(metric).strip(), period_label, _safe(raw)))
            count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ screener_growth: {count} rows upserted")


# ── Master loader ─────────────────────────────────────────────

def load_all_screener(data: dict, symbol: str):
    """
    data is the dict returned by fetch_screener_data().
    Loads all available sections.
    """
    if data.get("quarters")    is not None: load_screener_quarters(data["quarters"],       symbol)
    if data.get("profit_loss") is not None: load_screener_profit_loss(data["profit_loss"], symbol)
    if data.get("balance_sheet") is not None: load_screener_balance_sheet(data["balance_sheet"], symbol)
    if data.get("cash_flow")   is not None: load_screener_cash_flow(data["cash_flow"],     symbol)
    if data.get("ratios")      is not None: load_screener_ratios(data["ratios"],           symbol)
    if data.get("shareholding") is not None: load_screener_shareholding(data["shareholding"], symbol)
    if data.get("growth")      is not None: load_screener_growth(data["growth"],           symbol)