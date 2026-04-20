"""
etl/load/cashflow_loader.py  v4.0
────────────────────────────────────────────────────────────────
Changes vs v3:
  • Resolves best_operating_cf / best_free_cash_flow immediately
    after yfinance data is written — does NOT wait for Screener
  • completeness_pct computed and stored per row
  • has_yf_detail flag set to 1 when yfinance columns populated
  • Rows with zero useful yfinance data still inserted (scr_*
    will fill them via screener_loader)
────────────────────────────────────────────────────────────────
"""

import math
import pandas as pd
from database.db import get_connection
from database.validator import compute_completeness, log_data_quality

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
    """Load yfinance cash flow. Sets best_* = yfinance values initially;
    screener_loader will override with Screener values (authoritative)."""
    if df is None or df.empty:
        print(f"  warn  cash_flow ({period_type}): empty — skipping")
        return

    conn  = get_connection()
    count = 0
    completeness_sum = 0.0

    for col in df.columns:
        period_end = str(col)[:10]
        s = df[col]

        def gcr(*names):
            return _cr(_col_val(s, *names))

        ocf  = gcr("Operating Cash Flow", "Net Cash Provided By Operating Activities")
        icf  = gcr("Investing Cash Flow", "Net Cash Used For Investing Activities")
        fcf_r = gcr("Financing Cash Flow", "Net Cash Used Provided By Financing Activities")
        fcf  = gcr("Free Cash Flow")

        # has_yf_detail = 1 if at least OCF is present
        has_detail = 1 if ocf is not None else 0

        row_dict = {
            "best_operating_cf":  ocf,
            "best_free_cash_flow": fcf,
        }
        comp, _ = compute_completeness(row_dict, "cash_flow")
        completeness_sum += comp

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
                changes_in_cash,
                best_operating_cf, best_investing_cf,
                best_financing_cf, best_free_cash_flow,
                is_interpolated, data_source, has_yf_detail, completeness_pct
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                operating_cash_flow=excluded.operating_cash_flow,
                investing_cash_flow=excluded.investing_cash_flow,
                financing_cash_flow=excluded.financing_cash_flow,
                free_cash_flow=excluded.free_cash_flow,
                best_operating_cf=COALESCE(best_operating_cf, excluded.best_operating_cf),
                best_free_cash_flow=COALESCE(best_free_cash_flow, excluded.best_free_cash_flow),
                has_yf_detail=excluded.has_yf_detail,
                data_source=CASE WHEN data_source='screener' THEN 'both'
                                 ELSE 'yfinance' END
        """, (
            symbol, period_end, period_type,
            ocf,
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
            icf,
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
            fcf_r,
            gcr("Net Issuance Payments Of Debt", "Net Debt Issuance"),
            gcr("Long Term Debt Issuance"),
            gcr("Long Term Debt Payments", "Repayment Of Debt"),
            gcr("Short Term Debt Net"),
            gcr("Payment Of Dividends", "Common Stock Dividend Paid"),
            gcr("Interest Paid Cff"),
            gcr("Net Common Stock Issuance"),
            gcr("Other Financing Activities"),
            fcf,
            gcr("Beginning Cash Position"),
            gcr("End Cash Position"),
            gcr("Changes In Cash"),
            ocf, icf, fcf_r, fcf,   # best_* initial = yfinance
            0, "yfinance", has_detail, round(comp, 1),
        ))
        count += 1

    conn.commit()
    conn.close()
    avg = round(completeness_sum / count, 1) if count else 0
    log_data_quality(symbol, "cash_flow", count, 0, avg, {}, "yfinance")
    print(f"  ok  cash_flow ({period_type}): {count} rows | avg completeness {avg}%")