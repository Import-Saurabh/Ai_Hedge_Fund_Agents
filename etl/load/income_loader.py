"""
etl/load/income_loader.py  v2.0
────────────────────────────────────────────────────────────────
Fixes vs v1:
  • All monetary columns converted raw rupees → Rs. Crores (÷ 1e7)
    before insert, rounded to 2 dp
  • Per-share (EPS) and ratio (tax_rate) columns left as-is
  • Uses INSERT OR REPLACE for safe upsert
────────────────────────────────────────────────────────────────
"""

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


def _plain(v) -> float | None:
    """For EPS, ratios — no unit conversion."""
    if v is None:
        return None
    try:
        fv = float(v)
        return None if (math.isnan(fv) or math.isinf(fv)) else round(fv, 6)
    except (TypeError, ValueError):
        return None


def _col_val(df_col, *candidates):
    """Pull a value from a single-column DataFrame slice."""
    for cand in candidates:
        for idx in df_col.index:
            if str(idx).lower().strip() == cand.lower().strip():
                try:
                    return float(df_col.iloc[df_col.index.get_loc(idx)])
                except Exception:
                    pass
    for cand in candidates:
        for idx in df_col.index:
            if cand.lower() in str(idx).lower():
                try:
                    return float(df_col.iloc[df_col.index.get_loc(idx)])
                except Exception:
                    pass
    return None


def load_income(df: pd.DataFrame, symbol: str, period_type: str):
    """Load income statement rows. Monetary values stored in Rs. Crores."""
    if df is None or df.empty:
        print(f"  ⚠  income_statement ({period_type}): empty — skipping")
        return

    conn  = get_connection()
    count = 0

    for col in df.columns:
        period_end = str(col)[:10]
        s = df[col]  # Series indexed by metric name

        def get_cr(*names):
            v = _col_val(s, *names)
            return _cr(v)

        def get_plain(*names):
            v = _col_val(s, *names)
            return _plain(v)

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
                special_income_charges, total_unusual_items, tax_rate,
                is_interpolated
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        """, (
            symbol, period_end, period_type,
            get_cr("Total Revenue", "Revenue"),
            get_cr("Cost Of Revenue", "Reconciled Cost Of Revenue"),
            get_cr("Gross Profit"),
            get_cr("Selling General And Administration", "General And Administrative Expense"),
            get_cr("Operating Expense", "Total Operating Expenses"),
            get_cr("Operating Income", "EBIT"),
            get_cr("EBIT", "Operating Income"),
            get_cr("EBITDA", "Normalized EBITDA"),
            get_cr("Normalized EBITDA"),
            get_cr("Reconciled Depreciation", "Depreciation And Amortization In Income Stat",
                   "Depreciation And Amortization"),
            get_cr("Interest Expense", "Interest Expense Non Operating"),
            get_cr("Interest Income", "Interest Income Non Operating"),
            get_cr("Net Interest Income"),
            get_cr("Pretax Income"),
            get_cr("Tax Provision"),
            get_cr("Net Income"),
            get_cr("Net Income Common Stockholders"),
            get_cr("Normalized Income"),
            get_cr("Minority Interests"),
            get_plain("Diluted EPS"),
            get_plain("Basic EPS"),
            get_plain("Diluted Average Shares"),
            get_plain("Basic Average Shares"),
            get_cr("Special Income Charges"),
            get_cr("Total Unusual Items"),
            get_plain("Tax Rate For Calcs", "Tax Rate"),
            0,
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ income_statement ({period_type}): {count} rows upserted (Rs Cr)")