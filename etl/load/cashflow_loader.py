"""
etl/load/cashflow_loader.py  v3.0
────────────────────────────────────────────────────────────────
Changes vs v2:
  • load_cashflow_from_screener() writes scr_* columns into the
    same cash_flow table using INSERT ... ON CONFLICT merge
  • data_source column tracks 'yfinance' | 'screener' | 'both'
────────────────────────────────────────────────────────────────
"""

import re
import math
import pandas as pd
from database.db import get_connection

_CR = 1e7


def _cr(v) -> float | None:
    if v is None:
        return None
    try:
        fv = float(v)
        if math.isnan(fv) or math.isinf(fv):
            return None
        return round(fv / _CR, 2)
    except (TypeError, ValueError):
        return None


def _col_val(s, *candidates):
    for cand in candidates:
        for idx in s.index:
            if str(idx).lower().strip() == cand.lower().strip():
                try:
                    return float(s[idx])
                except Exception:
                    pass
    for cand in candidates:
        for idx in s.index:
            if cand.lower() in str(idx).lower():
                try:
                    return float(s[idx])
                except Exception:
                    pass
    return None


def load_cashflow(df: pd.DataFrame, symbol: str, period_type: str):
    """Load yfinance cash flow rows (detailed line items)."""
    if df is None or df.empty:
        print(f"  ⚠  cash_flow ({period_type}): empty — skipping")
        return

    conn  = get_connection()
    count = 0

    for col in df.columns:
        period_end = str(col)[:10]
        s = df[col]

        def gcr(*names):
            return _cr(_col_val(s, *names))

        conn.execute("""
            INSERT INTO cash_flow (
                symbol, period_end, period_type,
                operating_cash_flow, net_income_ops, depreciation,
                change_in_working_capital, change_in_receivables,
                change_in_inventory, change_in_payables,
                change_in_other_assets, change_in_other_liab,
                other_non_cash_items, taxes_refund_paid,
                investing_cash_flow, capex, purchase_of_ppe, sale_of_ppe,
                purchase_of_business, sale_of_business,
                purchase_of_investments, sale_of_investments,
                interest_received, dividends_received, other_investing,
                financing_cash_flow, net_debt_issuance,
                long_term_debt_issuance, long_term_debt_payments,
                short_term_debt_net, dividends_paid, interest_paid,
                stock_issuance, other_financing,
                free_cash_flow, beginning_cash, end_cash,
                changes_in_cash, is_interpolated, data_source
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                operating_cash_flow=excluded.operating_cash_flow,
                investing_cash_flow=excluded.investing_cash_flow,
                financing_cash_flow=excluded.financing_cash_flow,
                free_cash_flow=excluded.free_cash_flow,
                data_source=CASE WHEN data_source='screener' THEN 'both'
                                 ELSE 'yfinance' END
        """, (
            symbol, period_end, period_type,
            gcr("Operating Cash Flow", "Net Cash Provided By Operating Activities"),
            gcr("Net Income From Continuing Operations", "Net Income Continuous Operations"),
            gcr("Depreciation And Amortization", "Depreciation Amortization Depletion"),
            gcr("Change In Working Capital"),
            gcr("Change In Receivables"),
            gcr("Change In Inventory"),
            gcr("Change In Payable", "Change In Payables And Accrued Expense"),
            gcr("Change In Other Current Assets"),
            gcr("Change In Other Current Liabilities"),
            gcr("Other Non Cash Items"),
            gcr("Taxes Refunds Paid"),
            gcr("Investing Cash Flow", "Net Cash Used For Investing Activities"),
            gcr("Capital Expenditure"),
            gcr("Purchase Of Ppe", "Purchases Of Property Plant And Equipment"),
            gcr("Sale Of Ppe"),
            gcr("Acquisition Of Business", "Purchase Of Business"),
            gcr("Sale Of Business"),
            gcr("Purchase Of Investment"),
            gcr("Sale Of Investment"),
            gcr("Interest Received Cfi"),
            gcr("Dividends Received Cfi"),
            gcr("Other Investing Activities"),
            gcr("Financing Cash Flow", "Net Cash Used Provided By Financing Activities"),
            gcr("Net Issuance Payments Of Debt", "Net Debt Issuance"),
            gcr("Long Term Debt Issuance"),
            gcr("Long Term Debt Payments", "Repayment Of Debt"),
            gcr("Short Term Debt Net"),
            gcr("Payment Of Dividends", "Common Stock Dividend Paid"),
            gcr("Interest Paid Cff"),
            gcr("Net Common Stock Issuance"),
            gcr("Other Financing Activities"),
            gcr("Free Cash Flow"),
            gcr("Beginning Cash Position"),
            gcr("End Cash Position"),
            gcr("Changes In Cash"),
            0,
            "yfinance",
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ cash_flow ({period_type}): {count} rows upserted [yfinance]")


MONTH_MAP = {"jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06",
             "jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12"}
MONTH_END = {"01":"31","02":"28","03":"31","04":"30","05":"31","06":"30",
             "07":"31","08":"31","09":"30","10":"31","11":"30","12":"31"}


def _parse_period(label: str):
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


def load_cashflow_from_screener(df: pd.DataFrame, symbol: str):
    """
    Load Screener cash flow into scr_* columns of cash_flow table.
    Values already in Rs. Crores — no conversion needed.
    All Screener CF data is annual (Mar YYYY periods).
    """
    if df is None or df.empty:
        print("  ⚠  cash_flow screener: empty — skipping")
        return

    def row(metric):
        for idx in df.index:
            if metric.lower() in str(idx).lower():
                return df.loc[idx]
        return None

    ocf_r  = row("Cash from Operating Activity")
    icf_r  = row("Cash from Investing Activity")
    fcf_r  = row("Cash from Financing Activity")
    ncf_r  = row("Net Cash Flow")
    fcf2_r = row("Free Cash Flow")
    cfo_r  = row("CFO/OP")

    def v(series, col):
        if series is None:
            return None
        raw = series.get(col)
        if raw is None:
            return None
        s = str(raw).replace("%", "").replace(",", "").strip()
        if s in ("", "-", "—", "N/A", "nan", "None"):
            return None
        try:
            return round(float(s), 4)
        except ValueError:
            return None

    conn  = get_connection()
    count = 0

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue

        conn.execute("""
            INSERT INTO cash_flow (
                symbol, period_end, period_type,
                scr_cash_from_operating, scr_cash_from_investing,
                scr_cash_from_financing, scr_net_cash_flow,
                scr_free_cash_flow, scr_cfo_op_pct,
                data_source
            ) VALUES (?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                scr_cash_from_operating=excluded.scr_cash_from_operating,
                scr_cash_from_investing=excluded.scr_cash_from_investing,
                scr_cash_from_financing=excluded.scr_cash_from_financing,
                scr_net_cash_flow=excluded.scr_net_cash_flow,
                scr_free_cash_flow=excluded.scr_free_cash_flow,
                scr_cfo_op_pct=excluded.scr_cfo_op_pct,
                data_source=CASE WHEN data_source='yfinance' THEN 'both'
                                 ELSE 'screener' END
        """, (
            symbol, period_end, "annual",
            v(ocf_r, col), v(icf_r, col), v(fcf_r, col),
            v(ncf_r, col), v(fcf2_r, col), v(cfo_r, col),
            "screener",
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ cash_flow: {count} Screener rows upserted [screener]")