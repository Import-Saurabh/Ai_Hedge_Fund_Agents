"""
etl/load/income_loader.py  v3.0
────────────────────────────────────────────────────────────────
Changes vs v2:
  • Writes Screener scr_* columns when source='screener'
  • data_source column set to 'yfinance' | 'screener' | 'both'
  • When source='screener', maps Screener metric names to scr_*
  • When source='yfinance' (default), fills original columns
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
    if v is None:
        return None
    try:
        fv = float(v)
        return None if (math.isnan(fv) or math.isinf(fv)) else round(fv, 6)
    except (TypeError, ValueError):
        return None


def _pct_str(v) -> float | None:
    """'59%' → 59.0, also handles plain floats."""
    if v is None:
        return None
    s = str(v).replace("%", "").strip()
    try:
        return round(float(s), 4)
    except ValueError:
        return None


def _col_val(df_col, *candidates):
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
    """Load yfinance income statement rows (detailed line items)."""
    if df is None or df.empty:
        print(f"  ⚠  income_statement ({period_type}): empty — skipping")
        return

    conn  = get_connection()
    count = 0

    for col in df.columns:
        period_end = str(col)[:10]
        s = df[col]

        def get_cr(*names):
            return _cr(_col_val(s, *names))

        def get_plain(*names):
            return _plain(_col_val(s, *names))

        conn.execute("""
            INSERT INTO income_statement (
                symbol, period_end, period_type,
                total_revenue, cost_of_revenue, gross_profit,
                selling_general_admin, operating_expense, operating_income,
                ebit, ebitda, normalized_ebitda, depreciation_amortization,
                interest_expense, interest_income, net_interest_expense,
                pretax_income, tax_provision, net_income, net_income_common,
                normalized_income, minority_interests,
                diluted_eps, basic_eps, diluted_shares, basic_shares,
                special_income_charges, total_unusual_items, tax_rate,
                is_interpolated, data_source
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                total_revenue=excluded.total_revenue,
                cost_of_revenue=excluded.cost_of_revenue,
                gross_profit=excluded.gross_profit,
                selling_general_admin=excluded.selling_general_admin,
                operating_expense=excluded.operating_expense,
                operating_income=excluded.operating_income,
                ebit=excluded.ebit, ebitda=excluded.ebitda,
                normalized_ebitda=excluded.normalized_ebitda,
                depreciation_amortization=excluded.depreciation_amortization,
                interest_expense=excluded.interest_expense,
                interest_income=excluded.interest_income,
                net_interest_expense=excluded.net_interest_expense,
                pretax_income=excluded.pretax_income,
                tax_provision=excluded.tax_provision,
                net_income=excluded.net_income,
                net_income_common=excluded.net_income_common,
                normalized_income=excluded.normalized_income,
                minority_interests=excluded.minority_interests,
                diluted_eps=excluded.diluted_eps, basic_eps=excluded.basic_eps,
                diluted_shares=excluded.diluted_shares, basic_shares=excluded.basic_shares,
                special_income_charges=excluded.special_income_charges,
                total_unusual_items=excluded.total_unusual_items,
                tax_rate=excluded.tax_rate,
                data_source=CASE WHEN data_source='screener' THEN 'both'
                                 ELSE 'yfinance' END
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
            "yfinance",
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ income_statement ({period_type}): {count} rows upserted [yfinance]")


def load_income_from_screener(df: pd.DataFrame, symbol: str, period_type: str):
    """
    Load Screener P&L data into scr_* columns of income_statement.
    Creates row if not present, otherwise merges into existing row.
    Screener values are already in Rs. Crores — no conversion needed.
    """
    if df is None or df.empty:
        print(f"  ⚠  income_statement screener ({period_type}): empty — skipping")
        return

    import re

    MONTH_MAP = {"jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06",
                 "jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12"}
    MONTH_END = {"01":"31","02":"28","03":"31","04":"30","05":"31","06":"30",
                 "07":"31","08":"31","09":"30","10":"31","11":"30","12":"31"}

    def parse_period(label):
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

    def row(metric, *more):
        for name in (metric,) + more:
            for idx in df.index:
                if name.lower() in str(idx).lower():
                    return df.loc[idx]
        return None

    sales_r  = row("Sales")
    exp_r    = row("Expenses")
    op_r     = row("Operating Profit")
    opm_r    = row("OPM %")
    other_r  = row("Other Income")
    int_r    = row("Interest")
    dep_r    = row("Depreciation")
    pbt_r    = row("Profit before tax")
    tax_r    = row("Tax %")
    ni_r     = row("Net Profit")
    eps_r    = row("EPS in Rs")
    div_r    = row("Dividend Payout %")

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
        period_end = parse_period(str(col))
        if not period_end:
            continue

        conn.execute("""
            INSERT INTO income_statement (
                symbol, period_end, period_type,
                scr_sales, scr_expenses, scr_operating_profit,
                scr_opm_pct, scr_other_income, scr_interest,
                scr_depreciation, scr_profit_before_tax,
                scr_tax_pct, scr_net_profit, scr_eps,
                scr_dividend_payout_pct, data_source
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                scr_sales=excluded.scr_sales,
                scr_expenses=excluded.scr_expenses,
                scr_operating_profit=excluded.scr_operating_profit,
                scr_opm_pct=excluded.scr_opm_pct,
                scr_other_income=excluded.scr_other_income,
                scr_interest=excluded.scr_interest,
                scr_depreciation=excluded.scr_depreciation,
                scr_profit_before_tax=excluded.scr_profit_before_tax,
                scr_tax_pct=excluded.scr_tax_pct,
                scr_net_profit=excluded.scr_net_profit,
                scr_eps=excluded.scr_eps,
                scr_dividend_payout_pct=excluded.scr_dividend_payout_pct,
                data_source=CASE WHEN data_source='yfinance' THEN 'both'
                                 ELSE 'screener' END
        """, (
            symbol, period_end, period_type,
            v(sales_r, col), v(exp_r, col), v(op_r, col),
            v(opm_r, col), v(other_r, col), v(int_r, col),
            v(dep_r, col), v(pbt_r, col),
            v(tax_r, col), v(ni_r, col), v(eps_r, col),
            v(div_r, col),
            "screener",
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ income_statement ({period_type}): {count} rows upserted [screener]")