import math
import pandas as pd
from database.db import get_connection


def _g(df, row_name, col):
    for idx in df.index:
        if str(idx).lower().strip() == row_name.lower():
            try:
                v = float(df.loc[idx, col])
                return None if math.isnan(v) else v
            except:
                return None
    for idx in df.index:
        if row_name.lower() in str(idx).lower():
            try:
                v = float(df.loc[idx, col])
                return None if math.isnan(v) else v
            except:
                return None
    return None


def load_cashflow(df: pd.DataFrame, symbol: str, period_type: str = "annual"):
    """Load full cash flow statement."""
    if df is None or df.empty:
        print(f"  ⚠  cash_flow ({period_type}): empty, skipping")
        return

    conn = get_connection()
    count = 0

    for col in df.columns:
        date_str = str(col)[:10]
        conn.execute("""
            INSERT OR REPLACE INTO cash_flow (
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
                free_cash_flow, beginning_cash, end_cash, changes_in_cash
            ) VALUES (
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?
            )
        """, (
            symbol, date_str, period_type,
            _g(df, "Operating Cash Flow", col),
            _g(df, "Net Income From Continuing Operations", col),
            _g(df, "Depreciation", col) or _g(df, "Depreciation And Amortization", col),
            _g(df, "Change In Working Capital", col),
            _g(df, "Change In Receivables", col),
            _g(df, "Change In Inventory", col),
            _g(df, "Change In Payable", col),
            _g(df, "Change In Other Current Assets", col),
            _g(df, "Change In Other Current Liabilities", col),
            _g(df, "Other Non Cash Items", col),
            _g(df, "Taxes Refund Paid", col),
            _g(df, "Investing Cash Flow", col),
            _g(df, "Capital Expenditure", col),
            _g(df, "Purchase Of PPE", col) or _g(df, "Purchase Of Property Plant And Equipment", col),
            _g(df, "Sale Of PPE", col) or _g(df, "Sale Of Property Plant And Equipment", col),
            _g(df, "Purchase Of Business", col),
            _g(df, "Sale Of Business", col),
            _g(df, "Purchase Of Investment", col),
            _g(df, "Sale Of Investment", col),
            _g(df, "Interest Received Cfi", col),
            _g(df, "Dividends Received Cfi", col),
            _g(df, "Net Other Investing Changes", col),
            _g(df, "Financing Cash Flow", col),
            _g(df, "Net Issuance Payments Of Debt", col),
            _g(df, "Long Term Debt Issuance", col),
            _g(df, "Long Term Debt Payments", col),
            _g(df, "Net Short Term Debt Issuance", col),
            _g(df, "Cash Dividends Paid", col),
            _g(df, "Interest Paid Cff", col),
            _g(df, "Common Stock Issuance", col),
            _g(df, "Net Other Financing Charges", col),
            _g(df, "Free Cash Flow", col),
            _g(df, "Beginning Cash Position", col),
            _g(df, "End Cash Position", col),
            _g(df, "Changes In Cash", col),
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ cash_flow ({period_type}): {count} periods saved")
