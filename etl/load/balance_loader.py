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


def load_balance(df: pd.DataFrame, symbol: str, period_type: str = "annual",
                 is_interpolated: int = 0):
    """Load full balance sheet. is_interpolated=1 for Q-BS filled by engine."""
    if df is None or df.empty:
        print(f"  ⚠  balance_sheet ({period_type}): empty, skipping")
        return

    conn = get_connection()
    count = 0

    for col in df.columns:
        date_str = str(col)[:10]
        conn.execute("""
            INSERT OR REPLACE INTO balance_sheet (
                symbol, period_end, period_type,
                total_assets, current_assets,
                cash_and_equivalents, cash_equivalents, short_term_investments,
                accounts_receivable, allowance_doubtful, inventory,
                prepaid_assets, restricted_cash, other_current_assets,
                total_non_current_assets, net_ppe, gross_ppe, accumulated_depreciation,
                land_improvements, buildings_improvements, machinery_equipment,
                construction_in_progress, goodwill, other_intangibles,
                long_term_equity_investment, investment_in_fin_assets,
                investment_properties, non_current_deferred_tax_a,
                other_non_current_assets,
                total_liabilities, current_liabilities,
                accounts_payable, current_debt, current_capital_lease,
                current_provisions, dividends_payable, other_current_liabilities,
                total_non_current_liab, long_term_debt, long_term_capital_lease,
                non_current_deferred_tax_l, non_current_deferred_rev,
                long_term_provisions, other_non_current_liab,
                total_equity, stockholders_equity, common_stock,
                additional_paid_in_capital, retained_earnings,
                other_equity_interest, minority_interest,
                total_debt, net_debt, working_capital,
                invested_capital, tangible_book_value, capital_lease_obligations,
                shares_issued, is_interpolated
            ) VALUES (
                ?, ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?,
                ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?
            )
        """, (
            symbol, date_str, period_type,
            _g(df, "Total Assets", col),
            _g(df, "Current Assets", col),
            _g(df, "Cash And Cash Equivalents", col),
            _g(df, "Cash Equivalents", col),
            _g(df, "Other Short Term Investments", col),
            _g(df, "Accounts Receivable", col),
            _g(df, "Allowance For Doubtful Accounts Receivable", col),
            _g(df, "Inventory", col),
            _g(df, "Prepaid Assets", col),
            _g(df, "Restricted Cash", col),
            _g(df, "Other Current Assets", col),
            _g(df, "Total Non Current Assets", col),
            _g(df, "Net PPE", col),
            _g(df, "Gross PPE", col),
            _g(df, "Accumulated Depreciation", col),
            _g(df, "Land And Improvements", col),
            _g(df, "Buildings And Improvements", col),
            _g(df, "Machinery Furniture Equipment", col),
            _g(df, "Construction In Progress", col),
            _g(df, "Goodwill", col),
            _g(df, "Other Intangible Assets", col),
            _g(df, "Long Term Equity Investment", col),
            _g(df, "Investmentin Financial Assets", col),
            _g(df, "Investment Properties", col),
            _g(df, "Non Current Deferred Taxes Assets", col),
            _g(df, "Other Non Current Assets", col),
            _g(df, "Total Liabilities Net Minority Interest", col),
            _g(df, "Current Liabilities", col),
            _g(df, "Accounts Payable", col),
            _g(df, "Current Debt", col),
            _g(df, "Current Capital Lease Obligation", col),
            _g(df, "Current Provisions", col),
            _g(df, "Dividends Payable", col),
            _g(df, "Other Current Liabilities", col),
            _g(df, "Total Non Current Liabilities Net Minority In", col),
            _g(df, "Long Term Debt", col),
            _g(df, "Long Term Capital Lease Obligation", col),
            _g(df, "Non Current Deferred Taxes Liabilities", col),
            _g(df, "Non Current Deferred Revenue", col),
            _g(df, "Long Term Provisions", col),
            _g(df, "Other Non Current Liabilities", col),
            _g(df, "Total Equity Gross Minority Interest", col),
            _g(df, "Stockholders Equity", col),
            _g(df, "Common Stock", col),
            _g(df, "Additional Paid In Capital", col),
            _g(df, "Retained Earnings", col),
            _g(df, "Other Equity Interest", col),
            _g(df, "Minority Interest", col),
            _g(df, "Total Debt", col),
            _g(df, "Net Debt", col),
            _g(df, "Working Capital", col),
            _g(df, "Invested Capital", col),
            _g(df, "Tangible Book Value", col),
            _g(df, "Capital Lease Obligations", col),
            _g(df, "Share Issued", col),
            is_interpolated,
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ balance_sheet ({period_type}): {count} periods saved")
