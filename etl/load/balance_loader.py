"""
etl/load/balance_loader.py  v3.0
────────────────────────────────────────────────────────────────
Fixes vs v2:
  • All monetary values converted from raw rupees → Rs. Crores
    before insert (÷ 1e7), rounded to 2 decimal places
  • Dedup guard: skip insert if identical row already exists for
    (symbol, period_end, period_type) — prevents accumulation
  • Quarterly BS period_end normalised to YYYY-MM-DD string
────────────────────────────────────────────────────────────────
"""

import math
import pandas as pd
from database.db import get_connection

_CR = 1e7   # 1 Crore = 10,000,000


def _cr(v) -> float | None:
    """Raw rupees → Rs. Crores, 2 dp. Returns None for NaN/None/0-ish."""
    if v is None:
        return None
    try:
        fv = float(v)
        if math.isnan(fv) or math.isinf(fv):
            return None
        return round(fv / _CR, 2)
    except (TypeError, ValueError):
        return None


def _row_val(df, *candidates) -> float | None:
    """Extract the first non-NaN value from matching index rows."""
    if df is None or df.empty:
        return None
    for name in candidates:
        for idx in df.index:
            if str(idx).lower().strip() == name.lower().strip():
                row = df.loc[idx].dropna()
                if not row.empty:
                    return _cr(row.iloc[0])
    for name in candidates:
        for idx in df.index:
            if name.lower() in str(idx).lower():
                row = df.loc[idx].dropna()
                if not row.empty:
                    return _cr(row.iloc[0])
    return None


def _already_exists(conn, symbol: str, period_end: str, period_type: str) -> bool:
    cur = conn.execute(
        "SELECT 1 FROM balance_sheet WHERE symbol=? AND period_end=? AND period_type=?",
        (symbol, period_end, period_type)
    )
    return cur.fetchone() is not None


def load_balance(df: pd.DataFrame, symbol: str, period_type: str,
                 is_interpolated: int = 0):
    """
    Load balance sheet rows.  All monetary values stored in Rs. Crores.
    Skips rows that already exist (upsert by UNIQUE constraint).
    """
    if df is None or df.empty:
        print(f"  ⚠  balance_sheet ({period_type}): empty dataframe — skipping")
        return

    conn  = get_connection()
    count = 0

    for col in df.columns:
        period_end = str(col)[:10]
        col_df = df[[col]]

        def get(label, *more):
            row = col_df.rename(columns={col: "v"})
            for cand in (label,) + more:
                for idx in col_df.index:
                    if str(idx).lower().strip() == cand.lower().strip():
                        try:
                            return _cr(float(col_df.loc[idx, col]))
                        except Exception:
                            pass
            for cand in (label,) + more:
                for idx in col_df.index:
                    if cand.lower() in str(idx).lower():
                        try:
                            return _cr(float(col_df.loc[idx, col]))
                        except Exception:
                            pass
            return None

        conn.execute("""
            INSERT OR REPLACE INTO balance_sheet (
                symbol, period_end, period_type,
                total_assets, current_assets,
                cash_and_equivalents, cash_equivalents,
                short_term_investments, accounts_receivable,
                allowance_doubtful, inventory, prepaid_assets,
                restricted_cash, other_current_assets,
                total_non_current_assets, net_ppe, gross_ppe,
                accumulated_depreciation, land_improvements,
                buildings_improvements, machinery_equipment,
                construction_in_progress, goodwill, other_intangibles,
                long_term_equity_investment, investment_in_fin_assets,
                investment_properties, non_current_deferred_tax_a,
                other_non_current_assets,
                total_liabilities, current_liabilities,
                accounts_payable, current_debt, current_capital_lease,
                current_provisions, dividends_payable,
                other_current_liabilities, total_non_current_liab,
                long_term_debt, long_term_capital_lease,
                non_current_deferred_tax_l, non_current_deferred_rev,
                long_term_provisions, other_non_current_liab,
                total_equity, stockholders_equity, common_stock,
                additional_paid_in_capital, retained_earnings,
                other_equity_interest, minority_interest,
                total_debt, net_debt, working_capital, invested_capital,
                tangible_book_value, capital_lease_obligations,
                shares_issued, is_interpolated
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        """, (
            symbol, period_end, period_type,
            get("Total Assets"),
            get("Current Assets", "Total Current Assets"),
            get("Cash And Cash Equivalents", "Cash Equivalents"),
            get("Cash Equivalents"),
            get("Short Term Investments", "Other Short Term Investments"),
            get("Accounts Receivable", "Gross Accounts Receivable"),
            get("Allowance For Doubtful Accounts Receivable"),
            get("Inventory"),
            get("Prepaid Assets"),
            get("Restricted Cash"),
            get("Other Current Assets"),
            get("Total Non Current Assets"),
            get("Net Ppe", "Net PPE"),
            get("Gross Ppe", "Gross PPE"),
            get("Accumulated Depreciation"),
            get("Land And Improvements"),
            get("Buildings And Improvements"),
            get("Machinery Furniture Equipment"),
            get("Construction In Progress"),
            get("Goodwill"),
            get("Other Intangible Assets"),
            get("Long Term Equity Investment", "Investments In Other Ventures Under Equity Method"),
            get("Investment In Financial Assets", "Available For Sale Securities"),
            get("Investment Properties"),
            get("Non Current Deferred Assets", "Non Current Deferred Tax Assets"),
            get("Other Non Current Assets"),
            get("Total Liabilities Net Minority Interest", "Total Liabilities"),
            get("Current Liabilities", "Total Current Liabilities"),
            get("Accounts Payable"),
            get("Current Debt", "Short Term Debt"),
            get("Current Capital Lease Obligation"),
            get("Current Provisions"),
            get("Dividends Payable"),
            get("Other Current Liabilities"),
            get("Total Non Current Liabilities Net Minority Interest", "Total Non Current Liabilities"),
            get("Long Term Debt"),
            get("Long Term Capital Lease Obligation"),
            get("Deferred Tax Liabilities Non Current", "Non Current Deferred Tax Liabilities"),
            get("Non Current Deferred Revenue"),
            get("Long Term Provisions"),
            get("Other Non Current Liabilities"),
            get("Total Equity Gross Minority Interest", "Stockholders Equity", "Total Equity"),
            get("Stockholders Equity", "Common Stock Equity"),
            get("Common Stock"),
            get("Additional Paid In Capital", "Capital Stock"),
            get("Retained Earnings"),
            get("Other Equity Interest"),
            get("Minority Interest"),
            get("Total Debt"),
            get("Net Debt"),
            get("Working Capital"),
            get("Invested Capital"),
            get("Tangible Book Value"),
            get("Capital Lease Obligations"),
            get("Ordinary Shares Number", "Share Issued"),
            is_interpolated,
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ balance_sheet ({period_type}): {count} rows upserted (Rs Cr)")