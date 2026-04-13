import math
import pandas as pd
from database.db import get_connection


def _g(df, row_name, col):
    """Safe get from DataFrame by row label and column."""
    for idx in df.index:
        if str(idx).lower().strip() == row_name.lower():
            try:
                v = float(df.loc[idx, col])
                return None if math.isnan(v) else v
            except:
                return None
    # Partial fallback
    for idx in df.index:
        if row_name.lower() in str(idx).lower():
            try:
                v = float(df.loc[idx, col])
                return None if math.isnan(v) else v
            except:
                return None
    return None


def load_income(df: pd.DataFrame, symbol: str, period_type: str = "annual"):
    """Load full income statement into DB. Handles both annual and quarterly."""
    if df is None or df.empty:
        print(f"  ⚠  income_statement ({period_type}): empty, skipping")
        return

    conn = get_connection()
    count = 0

    for col in df.columns:
        date_str = str(col)[:10]
        conn.execute("""
            INSERT OR REPLACE INTO income_statement (
                symbol, period_end, period_type,
                total_revenue, cost_of_revenue, gross_profit,
                selling_general_admin, operating_expense, operating_income,
                ebit, ebitda, normalized_ebitda, depreciation_amortization,
                interest_expense, interest_income, net_interest_expense,
                pretax_income, tax_provision, net_income, net_income_common,
                normalized_income, minority_interests,
                diluted_eps, basic_eps, diluted_shares, basic_shares,
                special_income_charges, total_unusual_items, tax_rate
            ) VALUES (
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?
            )
        """, (
            symbol, date_str, period_type,
            _g(df, "Total Revenue", col),
            _g(df, "Cost Of Revenue", col) or _g(df, "Reconciled Cost Of Revenue", col),
            _g(df, "Gross Profit", col),
            _g(df, "Selling General And Administration", col),
            _g(df, "Operating Expense", col),
            _g(df, "Operating Income", col),
            _g(df, "EBIT", col),
            _g(df, "EBITDA", col),
            _g(df, "Normalized EBITDA", col),
            _g(df, "Reconciled Depreciation", col) or _g(df, "Depreciation", col),
            _g(df, "Interest Expense", col) or _g(df, "Interest Expense Non Operating", col),
            _g(df, "Interest Income", col) or _g(df, "Interest Income Non Operating", col),
            _g(df, "Net Interest Income", col),
            _g(df, "Pretax Income", col),
            _g(df, "Tax Provision", col),
            _g(df, "Net Income", col),
            _g(df, "Net Income Common Stockholders", col),
            _g(df, "Normalized Income", col),
            _g(df, "Minority Interests", col),
            _g(df, "Diluted EPS", col),
            _g(df, "Basic EPS", col),
            _g(df, "Diluted Average Shares", col),
            _g(df, "Basic Average Shares", col),
            _g(df, "Special Income Charges", col),
            _g(df, "Total Unusual Items", col),
            _g(df, "Tax Rate For Calcs", col),
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ income_statement ({period_type}): {count} periods saved")
