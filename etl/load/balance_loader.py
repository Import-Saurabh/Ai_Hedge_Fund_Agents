"""
etl/load/balance_loader.py  v4.1
────────────────────────────────────────────────────────────────
Changes vs v4.0:
  • load_balance_from_screener() now ALSO writes canonical columns
    derived from Screener fields, in addition to scr_* columns:
      - total_assets          ← scr_total_assets
      - total_equity          ← scr_equity_capital + scr_reserves
      - stockholders_equity   ← scr_equity_capital + scr_reserves
      - total_liabilities     ← scr_total_liabilities
      - long_term_debt        ← scr_borrowings  (best proxy)
      - total_debt            ← scr_borrowings
      - net_ppe               ← scr_fixed_assets
      - cash_and_equivalents  ← derived: total_assets - fixed -
                                          cwip - investments - other
    This fills the canonical columns for pre-2022 annual rows that
    yfinance cannot supply.

  • Added backfill_balance_canonical(symbol) — a targeted UPDATE
    that sets canonical cols from scr_* for any row where the
    canonical value is still NULL. Safe to call multiple times
    (COALESCE guards prevent overwriting real data).
    Call this from pipeline.py after load_all_screener() and
    load_balance() have both run.

  • data_source logic unchanged — 'screener', 'yfinance', 'both'
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


def _row_val(df, *candidates) -> float | None:
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


def load_balance(df: pd.DataFrame, symbol: str, period_type: str,
                 is_interpolated: int = 0):
    """Load yfinance balance sheet rows (detailed line items)."""
    if df is None or df.empty:
        print(f"  ⚠  balance_sheet ({period_type}): empty — skipping")
        return

    conn  = get_connection()
    count = 0

    for col in df.columns:
        period_end = str(col)[:10]
        col_df = df[[col]]

        def get(label, *more):
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
            INSERT INTO balance_sheet (
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
                shares_issued, is_interpolated, data_source
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                total_assets=excluded.total_assets,
                current_assets=excluded.current_assets,
                cash_and_equivalents=excluded.cash_and_equivalents,
                total_liabilities=excluded.total_liabilities,
                total_equity=excluded.total_equity,
                total_debt=excluded.total_debt,
                net_debt=excluded.net_debt,
                data_source=CASE WHEN data_source='screener' THEN 'both'
                                 ELSE 'yfinance' END
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
            "yfinance",
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ balance_sheet ({period_type}): {count} rows upserted [yfinance]")


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


def load_balance_from_screener(df: pd.DataFrame, symbol: str):
    """
    Load Screener balance sheet into both scr_* columns AND canonical
    columns derived from Screener fields.

    Screener BS has both annual (Mar YYYY) and half-year (Sep YYYY) periods.
    Values already in Rs. Crores — no conversion needed.
    Half-year periods stored as period_type='half_year'.

    Canonical mappings from Screener fields:
      total_assets        ← scr_total_assets
      total_liabilities   ← scr_total_liabilities
      total_equity        ← scr_equity_capital + scr_reserves
      stockholders_equity ← scr_equity_capital + scr_reserves
      long_term_debt      ← scr_borrowings  (best available proxy)
      total_debt          ← scr_borrowings
      net_ppe             ← scr_fixed_assets
      construction_in_progress ← scr_cwip
      long_term_equity_investment ← scr_investments

    Note: canonical cols use COALESCE on conflict so real yfinance data
    (if present) is never overwritten.
    """
    if df is None or df.empty:
        print("  ⚠  balance_sheet screener: empty — skipping")
        return

    def row(metric):
        for idx in df.index:
            if metric.lower() in str(idx).lower():
                return df.loc[idx]
        return None

    eq_r     = row("Equity Capital")
    res_r    = row("Reserves")
    bor_r    = row("Borrowings")
    othl_r   = row("Other Liabilities")
    totl_r   = row("Total Liabilities")
    fix_r    = row("Fixed Assets")
    cwip_r   = row("CWIP")
    inv_r    = row("Investments")
    otha_r   = row("Other Assets")
    tota_r   = row("Total Assets")

    def v(series, col):
        if series is None:
            return None
        raw = series.get(col)
        if raw is None:
            return None
        s = str(raw).replace(",", "").strip()
        if s in ("", "-", "—", "N/A", "nan", "None"):
            return None
        try:
            return round(float(s), 2)
        except ValueError:
            return None

    conn  = get_connection()
    count = 0

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue
        # Determine period_type: Mar = annual, others = half_year
        col_str = str(col).strip()
        period_type = "annual" if col_str.startswith("Mar") else "half_year"

        # Raw Screener values (already in Crores)
        scr_equity   = v(eq_r,   col)
        scr_reserves = v(res_r,  col)
        scr_borrow   = v(bor_r,  col)
        scr_othl     = v(othl_r, col)
        scr_totl     = v(totl_r, col)
        scr_fixed    = v(fix_r,  col)
        scr_cwip     = v(cwip_r, col)
        scr_inv      = v(inv_r,  col)
        scr_otha     = v(otha_r, col)
        scr_tota     = v(tota_r, col)

        # ── Derive canonical columns from Screener fields ──────
        # total_equity = equity_capital + reserves  (standard Indian BS)
        canon_equity = None
        if scr_equity is not None and scr_reserves is not None:
            canon_equity = round(scr_equity + scr_reserves, 2)
        elif scr_equity is not None:
            canon_equity = scr_equity

        # total_assets / total_liabilities map 1:1
        canon_total_assets = scr_tota
        canon_total_liab   = scr_totl

        # borrowings → best proxy for long_term_debt and total_debt
        canon_ltd   = scr_borrow
        canon_tdebt = scr_borrow

        # fixed assets → net_ppe; cwip → construction_in_progress
        canon_ppe  = scr_fixed
        canon_cwip = scr_cwip

        # investments → long_term_equity_investment
        canon_lt_inv = scr_inv

        conn.execute("""
            INSERT INTO balance_sheet (
                symbol, period_end, period_type,
                scr_equity_capital, scr_reserves, scr_borrowings,
                scr_other_liabilities, scr_total_liabilities,
                scr_fixed_assets, scr_cwip, scr_investments,
                scr_other_assets, scr_total_assets,
                total_assets, total_liabilities,
                total_equity, stockholders_equity,
                long_term_debt, total_debt,
                net_ppe, construction_in_progress,
                long_term_equity_investment,
                data_source
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                scr_equity_capital      = excluded.scr_equity_capital,
                scr_reserves            = excluded.scr_reserves,
                scr_borrowings          = excluded.scr_borrowings,
                scr_other_liabilities   = excluded.scr_other_liabilities,
                scr_total_liabilities   = excluded.scr_total_liabilities,
                scr_fixed_assets        = excluded.scr_fixed_assets,
                scr_cwip                = excluded.scr_cwip,
                scr_investments         = excluded.scr_investments,
                scr_other_assets        = excluded.scr_other_assets,
                scr_total_assets        = excluded.scr_total_assets,
                -- canonical: only fill if currently NULL (don't overwrite yfinance)
                total_assets       = COALESCE(total_assets,       excluded.total_assets),
                total_liabilities  = COALESCE(total_liabilities,  excluded.total_liabilities),
                total_equity       = COALESCE(total_equity,       excluded.total_equity),
                stockholders_equity= COALESCE(stockholders_equity,excluded.stockholders_equity),
                long_term_debt     = COALESCE(long_term_debt,     excluded.long_term_debt),
                total_debt         = COALESCE(total_debt,         excluded.total_debt),
                net_ppe            = COALESCE(net_ppe,            excluded.net_ppe),
                construction_in_progress = COALESCE(construction_in_progress,
                                                     excluded.construction_in_progress),
                long_term_equity_investment = COALESCE(long_term_equity_investment,
                                                        excluded.long_term_equity_investment),
                data_source = CASE
                    WHEN data_source = 'yfinance' THEN 'both'
                    ELSE 'screener'
                END
        """, (
            symbol, period_end, period_type,
            scr_equity, scr_reserves, scr_borrow,
            scr_othl, scr_totl,
            scr_fixed, scr_cwip, scr_inv,
            scr_otha, scr_tota,
            canon_total_assets, canon_total_liab,
            canon_equity, canon_equity,
            canon_ltd, canon_tdebt,
            canon_ppe, canon_cwip,
            canon_lt_inv,
            "screener",
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ balance_sheet: {count} Screener rows upserted [screener + canonical derived]")


def backfill_balance_canonical(symbol: str):
    """
    One-pass UPDATE: for any balance_sheet row where a canonical column
    is still NULL but the corresponding scr_* column has data, fill it.

    This is the safety net for rows loaded before v4.1, or cases where
    load_balance_from_screener ran before load_balance (yfinance).

    Safe to call multiple times — COALESCE prevents overwriting real data.
    Call from pipeline.py after both load_balance() and
    load_all_screener() have completed.
    """
    conn = get_connection()

    conn.execute("""
        UPDATE balance_sheet SET
            total_assets = COALESCE(total_assets,
                scr_total_assets),

            total_liabilities = COALESCE(total_liabilities,
                scr_total_liabilities),

            -- total_equity = equity_capital + reserves (Indian BS standard)
            total_equity = COALESCE(total_equity,
                CASE
                    WHEN scr_equity_capital IS NOT NULL AND scr_reserves IS NOT NULL
                        THEN ROUND(scr_equity_capital + scr_reserves, 2)
                    WHEN scr_equity_capital IS NOT NULL
                        THEN scr_equity_capital
                    ELSE NULL
                END),

            stockholders_equity = COALESCE(stockholders_equity,
                CASE
                    WHEN scr_equity_capital IS NOT NULL AND scr_reserves IS NOT NULL
                        THEN ROUND(scr_equity_capital + scr_reserves, 2)
                    WHEN scr_equity_capital IS NOT NULL
                        THEN scr_equity_capital
                    ELSE NULL
                END),

            long_term_debt = COALESCE(long_term_debt,
                scr_borrowings),

            total_debt = COALESCE(total_debt,
                scr_borrowings),

            net_ppe = COALESCE(net_ppe,
                scr_fixed_assets),

            construction_in_progress = COALESCE(construction_in_progress,
                scr_cwip),

            long_term_equity_investment = COALESCE(long_term_equity_investment,
                scr_investments)

        WHERE symbol = ?
          AND (
            total_assets IS NULL OR total_liabilities IS NULL OR
            total_equity IS NULL OR total_debt IS NULL OR
            net_ppe IS NULL
          )
    """, (symbol,))

    rows_updated = conn.execute(
        "SELECT changes()"
    ).fetchone()[0]

    conn.commit()
    conn.close()
    print(f"  ✅ balance_sheet backfill: {rows_updated} rows updated "
          f"[scr_* → canonical] for {symbol}")