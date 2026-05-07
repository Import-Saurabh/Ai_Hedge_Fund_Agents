"""
etl/load/fundamentals_loader.py  v6.1
────────────────────────────────────────────────────────────────
Changes vs v6.0:
  • BUG FIX — pe_ratio and ttm_pe removed from _CARRY_FORWARD_COLS.
    They are price-dependent and silently produce wrong values when
    carried forward as price moves daily.
  • Added Pass 3 in _backfill_nulls_from_db(): recomputes pe_ratio
    and ttm_pe for EVERY row using that row's own current_price and
    eps_annual / ttm_eps respectively.
  • ttm_sales and ttm_net_profit columns intentionally absent
    (dropped via: ALTER TABLE fundamentals DROP COLUMN ttm_sales;
                  ALTER TABLE fundamentals DROP COLUMN ttm_net_profit;)
────────────────────────────────────────────────────────────────
"""

import json
import math
from datetime import date
from database.db import get_connection


# ── Key fields used for completeness scoring ─────────────────
# (free_cash_flow / operating_cf / capex / net_income removed)
_KEY_FIELDS = [
    "roe_pct", "roce_pct", "roa_pct", "pe_ratio", "pb_ratio",
    "revenue", "market_cap", "opm_pct",
    "dividend_payout_pct", "ev", "ev_ebitda",
    "debt_to_equity", "ebitda",
]

_COMPARE_COLS = ["roe_pct", "roce_pct", "roa_pct", "eps_annual", "pe_ratio",
                 "pb_ratio", "market_cap", "revenue"]


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
def _pct(filled, total):
    if not total:
        return 0.0
    return round(filled / total * 100, 1)


def _compute_completeness(conn, symbol: str, as_of_date: str) -> float:
    """Count non-NULL key fields in today's fundamentals row."""
    cur = conn.execute(
        f"SELECT {','.join(_KEY_FIELDS)} FROM fundamentals "
        f"WHERE symbol=? AND as_of_date=?",
        (symbol, as_of_date)
    )
    row = cur.fetchone()
    if not row:
        return 0.0
    filled = sum(1 for v in row if v is not None)
    return _pct(filled, len(_KEY_FIELDS))


def _get_today_row(conn, symbol: str, today: str):
    cur = conn.execute(
        "SELECT * FROM fundamentals WHERE symbol=? AND as_of_date=? LIMIT 1",
        (symbol, today)
    )
    row = cur.fetchone()
    if row is None:
        return None
    return dict(zip([d[0] for d in cur.description], row))


def _data_changed(latest: dict, new_data: dict) -> bool:
    for col in _COMPARE_COLS:
        if new_data.get(col) is not None and latest.get(col) != new_data.get(col):
            return True
    return False


# ─────────────────────────────────────────────────────────────
# Backfill NULLs — carry-forward + sibling tables
# ─────────────────────────────────────────────────────────────

# BUG FIX: pe_ratio and ttm_pe are intentionally EXCLUDED from this list.
# Both are derived from current_price which changes every day. Carrying a
# stale pe_ratio/ttm_pe from an older row produces silently wrong values
# (e.g. pe_ratio=22.23 frozen while price moves from 1657→1748).
# They are instead recomputed per-row in Pass 3 of _backfill_nulls_from_db().
_CARRY_FORWARD_COLS = [
    "roe_pct", "roa_pct", "interest_coverage",
    "gross_margin_pct", "net_profit_margin_pct",
    "ebitda_margin_pct", "ebit_margin_pct",
    "current_ratio", "quick_ratio",
    "dio_days", "dpo_days",
    "eps_annual", "ttm_eps", "graham_number",
    "dividend_yield_pct", "forward_pe",
    "inventory", "ev", "ev_ebitda", "ev_revenue",
    "earnings_growth_json",
    # monetary carry-forwards
    "market_cap", "ebitda", "revenue",
    # screener header fields
    "low_52w", "high_52w", "face_value", "book_value",
]

# Columns where sibling tables are MORE authoritative than carry-forward.
# Applied AFTER carry-forward so they win.
_SIBLING_COLS = [
    "revenue", "ebitda",          # income_statement (annual)
    "debt_to_equity",             # balance_sheet (computed)
    "opm_pct",                    # quarterly_results / annual_results
    "dividend_payout_pct",        # annual_results
]


def _backfill_nulls_from_db(conn, symbol: str, as_of_date: str):
    """
    Three-pass backfill for every fundamentals row of this symbol:

    Pass 1 — Carry-forward:
        For each column in _CARRY_FORWARD_COLS, find the most recent
        non-NULL value across all fundamentals rows and propagate it
        to every row that is still NULL for that column.
        pe_ratio and ttm_pe are intentionally excluded (see Pass 3).

    Pass 2 — Sibling tables (authoritative override):
        Pull revenue/ebitda from income_statement, debt_to_equity
        from balance_sheet, opm_pct from quarterly/annual_results,
        dividend_payout_pct from annual_results. These overwrite
        carry-forward values because they are more precisely dated.

    Pass 3 — Price-dependent ratio recompute (BUG FIX):
        pe_ratio  = current_price / eps_annual   (every row, always)
        ttm_pe    = current_price / ttm_eps       (every row, always)
        These must use each row's own current_price — never carried.

    Finally recomputes completeness_pct for every row.
    """
    # ── Fetch all rows for this symbol ───────────────────────
    all_cols = ", ".join(_CARRY_FORWARD_COLS)
    rows = conn.execute(
        f"SELECT rowid, as_of_date, {all_cols} FROM fundamentals WHERE symbol=?",
        (symbol,)
    ).fetchall()

    if not rows:
        return

    col_names = _CARRY_FORWARD_COLS  # positional alignment

    # ── Pass 1: carry-forward from most recent non-NULL ───────
    rows_sorted = sorted(rows, key=lambda r: r[1], reverse=True)

    best = {}   # col_name -> best known value
    for row in rows_sorted:
        for i, col in enumerate(col_names):
            v = row[i + 2]
            if v is not None and col not in best:
                best[col] = v

    # Apply best values to every row that is NULL for that col
    for row in rows:
        rowid = row[0]
        updates = {}
        for i, col in enumerate(col_names):
            v = row[i + 2]
            if v is None and col in best:
                updates[col] = best[col]
        if updates:
            set_clause = ", ".join(f"{k}=?" for k in updates)
            conn.execute(
                f"UPDATE fundamentals SET {set_clause} WHERE rowid=?",
                (*updates.values(), rowid)
            )

    conn.commit()

    # ── Pass 2: sibling tables (override carry-forward) ───────

    # income_statement: revenue, ebitda (annual, most recent)
    is_row = conn.execute("""
        SELECT total_revenue, ebitda
        FROM income_statement
        WHERE symbol=? AND period_type='annual'
        ORDER BY period_end DESC LIMIT 1
    """, (symbol,)).fetchone()
    is_revenue = is_row[0] if is_row else None
    is_ebitda  = is_row[1] if is_row else None

    # annual_results fallback for revenue/ebitda if income_statement empty
    if is_revenue is None or is_ebitda is None:
        ar2 = conn.execute("""
            SELECT sales, operating_profit FROM annual_results
            WHERE symbol=? ORDER BY period_end DESC LIMIT 1
        """, (symbol,)).fetchone()
        if ar2:
            if is_revenue is None: is_revenue = ar2[0]
            if is_ebitda  is None: is_ebitda  = ar2[1]

    # balance_sheet: debt_to_equity
    bs_row = conn.execute("""
        SELECT borrowings, total_equity FROM balance_sheet
        WHERE symbol=? AND period_type='annual'
        ORDER BY period_end DESC LIMIT 1
    """, (symbol,)).fetchone()
    bs_de = None
    if bs_row and bs_row[0] is not None and bs_row[1] and float(bs_row[1]) != 0:
        bs_de = round(float(bs_row[0]) / float(bs_row[1]), 4)

    # quarterly_results: opm_pct (most recent quarter)
    qr_row = conn.execute("""
        SELECT opm_pct FROM quarterly_results
        WHERE symbol=? ORDER BY period_end DESC LIMIT 1
    """, (symbol,)).fetchone()
    qr_opm = qr_row[0] if qr_row else None

    # annual_results: dividend_payout_pct, opm_pct fallback
    ar_row = conn.execute("""
        SELECT dividend_payout_pct, opm_pct FROM annual_results
        WHERE symbol=? ORDER BY period_end DESC LIMIT 1
    """, (symbol,)).fetchone()
    ar_div_payout = ar_row[0] if ar_row else None
    ar_opm        = ar_row[1] if ar_row else None

    opm_fill = qr_opm or ar_opm

    # Re-fetch rows after pass-1 updates
    rows2 = conn.execute(
        "SELECT rowid, revenue, ebitda, debt_to_equity, opm_pct, dividend_payout_pct "
        "FROM fundamentals WHERE symbol=?",
        (symbol,)
    ).fetchall()

    for row in rows2:
        rowid, revenue, ebitda, de, opm, div_payout = row
        updates = {}
        if is_revenue    is not None: updates["revenue"]             = is_revenue
        if is_ebitda     is not None: updates["ebitda"]              = is_ebitda
        if bs_de         is not None: updates["debt_to_equity"]      = bs_de
        if opm_fill      is not None: updates["opm_pct"]             = opm_fill
        if ar_div_payout is not None: updates["dividend_payout_pct"] = ar_div_payout

        if updates:
            set_clause = ", ".join(f"{k}=?" for k in updates)
            conn.execute(
                f"UPDATE fundamentals SET {set_clause} WHERE rowid=?",
                (*updates.values(), rowid)
            )

    conn.commit()

    # ── Recompute EV/EBITDA and EV/Revenue where NULL ─────────
    ev_rows = conn.execute(
        "SELECT rowid, ev, ebitda, revenue, ev_ebitda, ev_revenue "
        "FROM fundamentals WHERE symbol=?",
        (symbol,)
    ).fetchall()
    for row in ev_rows:
        rowid, ev, ebitda, revenue, ev_ebitda, ev_revenue = row
        updates = {}
        if ev and ebitda and ebitda > 0 and ev_ebitda is None:
            updates["ev_ebitda"] = round(ev / ebitda, 2)
        if ev and revenue and revenue > 0 and ev_revenue is None:
            updates["ev_revenue"] = round(ev / revenue, 2)
        if updates:
            set_clause = ", ".join(f"{k}=?" for k in updates)
            conn.execute(
                f"UPDATE fundamentals SET {set_clause} WHERE rowid=?",
                (*updates.values(), rowid)
            )

    conn.commit()

    # ── Pass 3: recompute price-dependent ratios (BUG FIX) ────
    # pe_ratio and ttm_pe depend on current_price which is different
    # for every row. They must NEVER be carry-forwarded — always derived
    # fresh from each row's own price.
    price_rows = conn.execute(
        "SELECT rowid, current_price, eps_annual, ttm_eps "
        "FROM fundamentals WHERE symbol=?",
        (symbol,)
    ).fetchall()
    for row in price_rows:
        rowid, price, eps_annual, ttm_eps = row
        updates = {}
        if price and eps_annual and float(eps_annual) > 0:
            updates["pe_ratio"] = round(float(price) / float(eps_annual), 2)
        if price and ttm_eps and float(ttm_eps) > 0:
            updates["ttm_pe"] = round(float(price) / float(ttm_eps), 2)
        if updates:
            set_clause = ", ".join(f"{k}=?" for k in updates)
            conn.execute(
                f"UPDATE fundamentals SET {set_clause} WHERE rowid=?",
                (*updates.values(), rowid)
            )

    conn.commit()

    # ── Recompute completeness for every row ──────────────────
    key_sel = ", ".join(_KEY_FIELDS)
    all_fund = conn.execute(
        f"SELECT rowid, {key_sel} FROM fundamentals WHERE symbol=?",
        (symbol,)
    ).fetchall()
    for row in all_fund:
        rowid  = row[0]
        values = row[1:]
        filled = sum(1 for v in values if v is not None)
        comp   = round(filled / len(_KEY_FIELDS) * 100, 1)
        conn.execute(
            "UPDATE fundamentals SET completeness_pct=? WHERE rowid=?",
            (comp, rowid)
        )
    conn.commit()

    final_comp = _compute_completeness(conn, symbol, as_of_date)
    print(f"  ok  fundamentals: backfill complete for {symbol} | completeness {final_comp}%")


# ─────────────────────────────────────────────────────────────
# Schema migration — idempotently drop retired columns
# ─────────────────────────────────────────────────────────────
def _migrate_drop_retired_cols(conn):
    """
    SQLite doesn't support DROP COLUMN before 3.35.0.
    We use a safe recreate-and-copy pattern only when needed.
    For SQLite >= 3.35 we use ALTER TABLE ... DROP COLUMN.
    """
    import sqlite3
    ver = tuple(int(x) for x in sqlite3.sqlite_version.split("."))

    retired = ["free_cash_flow", "operating_cf", "capex", "net_income"]

    if ver >= (3, 35, 0):
        for col in retired:
            try:
                conn.execute(f"ALTER TABLE fundamentals DROP COLUMN {col}")
                print(f"  db-migrate fundamentals: dropped column '{col}'")
            except Exception:
                pass  # already absent
        conn.commit()
    else:
        pragma = conn.execute("PRAGMA table_info(fundamentals)").fetchall()
        existing_cols = {row[1] for row in pragma}
        cols_to_drop = [c for c in retired if c in existing_cols]
        if not cols_to_drop:
            return  # nothing to do

        keep_cols = [row[1] for row in pragma if row[1] not in cols_to_drop]
        col_list  = ", ".join(keep_cols)

        print(f"  db-migrate fundamentals: recreating table to drop {cols_to_drop}")
        conn.execute("BEGIN")
        try:
            conn.execute(f"""
                CREATE TABLE fundamentals_new AS
                SELECT {col_list} FROM fundamentals
            """)
            conn.execute("DROP TABLE fundamentals")
            conn.execute("ALTER TABLE fundamentals_new RENAME TO fundamentals")
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_fund_sym_date "
                "ON fundamentals(symbol, as_of_date)"
            )
            conn.execute("COMMIT")
            print(f"  db-migrate fundamentals: done")
        except Exception as e:
            conn.execute("ROLLBACK")
            print(f"  db-migrate fundamentals: FAILED — {e}")


# ─────────────────────────────────────────────────────────────
# Public loaders
# ─────────────────────────────────────────────────────────────
def load_fundamentals(symbol: str, data: dict):
    """
    Upsert yfinance-derived fundamentals into today's row.
    Dropped keys: free_cash_flow, operating_cf, capex, net_income,
                  ttm_sales, ttm_net_profit.
    """
    conn  = get_connection()
    today = date.today().isoformat()

    # One-time migration on first call
    _migrate_drop_retired_cols(conn)

    existing = _get_today_row(conn, symbol, today)

    if existing is not None and not _data_changed(existing, data):
        comp = _compute_completeness(conn, symbol, today)
        conn.execute(
            "UPDATE fundamentals SET completeness_pct=? WHERE symbol=? AND as_of_date=?",
            (comp, symbol, today)
        )
        conn.commit()
        _backfill_nulls_from_db(conn, symbol, today)
        conn.close()
        print(f"  skip  fundamentals: no change for {symbol} | completeness {comp}%")
        return

    if existing is None:
        conn.execute("""
            INSERT INTO fundamentals (
                symbol, as_of_date,
                roe_pct, roce_pct, roa_pct, interest_coverage,
                gross_margin_pct, net_profit_margin_pct,
                ebitda_margin_pct, ebit_margin_pct,
                debt_to_equity, current_ratio, quick_ratio,
                dso_days, dio_days, dpo_days, cash_conversion_cycle,
                eps_annual, pe_ratio, pb_ratio, graham_number,
                dividend_yield_pct, market_cap, revenue,
                ebitda, inventory, ttm_eps, ttm_pe,
                ev, ev_ebitda, ev_revenue, forward_pe,
                earnings_growth_json, data_source
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        """, (
            symbol, today,
            data.get("ROE (%)"),        data.get("ROCE (%)"),
            data.get("ROA (%)"),        data.get("Interest Coverage"),
            data.get("Gross Margin (%)"),
            data.get("Net Profit Margin (%)"),
            data.get("EBITDA Margin (%)"),
            data.get("EBIT Margin (%)"),
            data.get("Debt/Equity"),    data.get("Current Ratio"),
            data.get("Quick Ratio"),
            data.get("DSO (days)"),     data.get("DIO (days)"),
            data.get("DPO (days)"),     data.get("CCC (days)"),
            data.get("EPS"),            data.get("P/E"),
            data.get("P/B"),            data.get("Graham Number"),
            data.get("Dividend Yield (%)"),
            data.get("Market Cap"),     data.get("Revenue"),
            data.get("EBITDA"),         data.get("Inventory"),
            data.get("TTM EPS"),        data.get("TTM P/E"),
            data.get("EV"),             data.get("EV/EBITDA"),
            data.get("EV/Revenue"),     data.get("Forward PE"),
            data.get("earnings_growth_json"),
            "yfinance",
        ))
    else:
        conn.execute("""
            UPDATE fundamentals SET
                roe_pct               = COALESCE(?, roe_pct),
                roce_pct              = COALESCE(?, roce_pct),
                roa_pct               = COALESCE(?, roa_pct),
                interest_coverage     = COALESCE(?, interest_coverage),
                gross_margin_pct      = COALESCE(?, gross_margin_pct),
                net_profit_margin_pct = COALESCE(?, net_profit_margin_pct),
                ebitda_margin_pct     = COALESCE(?, ebitda_margin_pct),
                ebit_margin_pct       = COALESCE(?, ebit_margin_pct),
                debt_to_equity        = COALESCE(?, debt_to_equity),
                current_ratio         = COALESCE(?, current_ratio),
                quick_ratio           = COALESCE(?, quick_ratio),
                dso_days              = COALESCE(?, dso_days),
                dio_days              = COALESCE(?, dio_days),
                dpo_days              = COALESCE(?, dpo_days),
                cash_conversion_cycle = COALESCE(?, cash_conversion_cycle),
                eps_annual            = COALESCE(?, eps_annual),
                pe_ratio              = COALESCE(?, pe_ratio),
                pb_ratio              = COALESCE(?, pb_ratio),
                graham_number         = COALESCE(?, graham_number),
                dividend_yield_pct    = COALESCE(?, dividend_yield_pct),
                market_cap            = COALESCE(?, market_cap),
                revenue               = COALESCE(?, revenue),
                ebitda                = COALESCE(?, ebitda),
                inventory             = COALESCE(?, inventory),
                ttm_eps               = COALESCE(?, ttm_eps),
                ttm_pe                = COALESCE(?, ttm_pe),
                ev                    = COALESCE(?, ev),
                ev_ebitda             = COALESCE(?, ev_ebitda),
                ev_revenue            = COALESCE(?, ev_revenue),
                forward_pe            = COALESCE(?, forward_pe),
                earnings_growth_json  = COALESCE(?, earnings_growth_json),
                data_source = CASE WHEN data_source='screener' THEN 'both'
                                   ELSE 'yfinance' END
            WHERE symbol=? AND as_of_date=?
        """, (
            data.get("ROE (%)"),        data.get("ROCE (%)"),
            data.get("ROA (%)"),        data.get("Interest Coverage"),
            data.get("Gross Margin (%)"),
            data.get("Net Profit Margin (%)"),
            data.get("EBITDA Margin (%)"),
            data.get("EBIT Margin (%)"),
            data.get("Debt/Equity"),    data.get("Current Ratio"),
            data.get("Quick Ratio"),
            data.get("DSO (days)"),     data.get("DIO (days)"),
            data.get("DPO (days)"),     data.get("CCC (days)"),
            data.get("EPS"),            data.get("P/E"),
            data.get("P/B"),            data.get("Graham Number"),
            data.get("Dividend Yield (%)"),
            data.get("Market Cap"),     data.get("Revenue"),
            data.get("EBITDA"),         data.get("Inventory"),
            data.get("TTM EPS"),        data.get("TTM P/E"),
            data.get("EV"),             data.get("EV/EBITDA"),
            data.get("EV/Revenue"),     data.get("Forward PE"),
            data.get("earnings_growth_json"),
            symbol, today,
        ))

    comp = _compute_completeness(conn, symbol, today)
    conn.execute(
        "UPDATE fundamentals SET completeness_pct=? WHERE symbol=? AND as_of_date=?",
        (comp, symbol, today)
    )
    conn.commit()
    _backfill_nulls_from_db(conn, symbol, today)
    conn.close()
    print(f"  ok  fundamentals: yfinance saved for {symbol} | completeness {comp}%")


def load_fundamentals_from_screener(ratios_df, symbol: str):
    """
    Merge Screener Ratios + latest quarterly opm_pct + annual
    dividend_payout_pct into today's fundamentals row.
    Always merges into ONE row per day.
    """
    today = date.today().isoformat()
    conn  = get_connection()

    dso = dio = dpo = ccc = wcd = roce = None

    if ratios_df is not None and not ratios_df.empty:
        col = ratios_df.columns[-1]  # most recent period

        def rv(metric):
            for idx in ratios_df.index:
                if metric.lower() in str(idx).lower():
                    raw = ratios_df.loc[idx, col]
                    s = str(raw).replace("%", "").replace(",", "").strip()
                    if s not in ("", "-", "nan", "None"):
                        try:
                            return round(float(s), 4)
                        except ValueError:
                            pass
            return None

        dso  = rv("Debtor Days")
        dio  = rv("Inventory Days")
        dpo  = rv("Days Payable")
        ccc  = rv("Cash Conversion Cycle")
        wcd  = rv("Working Capital Days")
        roce = rv("ROCE %")

    # ── opm_pct from latest quarterly_results ─────────────────
    opm = None
    try:
        r = conn.execute(
            "SELECT opm_pct FROM quarterly_results WHERE symbol=? "
            "ORDER BY period_end DESC LIMIT 1", (symbol,)
        ).fetchone()
        if r:
            opm = r[0]
    except Exception:
        pass

    # ── dividend_payout_pct from latest annual_results ────────
    div_payout = None
    try:
        r = conn.execute(
            "SELECT dividend_payout_pct FROM annual_results WHERE symbol=? "
            "ORDER BY period_end DESC LIMIT 1", (symbol,)
        ).fetchone()
        if r:
            div_payout = r[0]
    except Exception:
        pass

    # ── Ensure row exists ─────────────────────────────────────
    existing = _get_today_row(conn, symbol, today)
    if existing is None:
        conn.execute("""
            INSERT INTO fundamentals (symbol, as_of_date, data_source)
            VALUES (?, ?, 'screener')
        """, (symbol, today))

    conn.execute("""
        UPDATE fundamentals SET
            dso_days              = COALESCE(?, dso_days),
            dio_days              = COALESCE(?, dio_days),
            dpo_days              = COALESCE(?, dpo_days),
            cash_conversion_cycle = COALESCE(?, cash_conversion_cycle),
            working_capital_days  = COALESCE(?, working_capital_days),
            roce_pct              = COALESCE(?, roce_pct),
            opm_pct               = COALESCE(?, opm_pct),
            dividend_payout_pct   = COALESCE(?, dividend_payout_pct),
            data_source = CASE WHEN data_source='yfinance' THEN 'both'
                               WHEN data_source IS NULL    THEN 'screener'
                               ELSE data_source END
        WHERE symbol=? AND as_of_date=?
    """, (dso, dio, dpo, ccc, wcd, roce, opm, div_payout, symbol, today))

    comp = _compute_completeness(conn, symbol, today)
    conn.execute(
        "UPDATE fundamentals SET completeness_pct=? WHERE symbol=? AND as_of_date=?",
        (comp, symbol, today)
    )
    conn.commit()
    _backfill_nulls_from_db(conn, symbol, today)
    conn.close()
    print(f"  ok  fundamentals: Screener ratios merged | ROCE={roce} OPM={opm} "
          f"WCD={wcd} DivPayout={div_payout} | completeness {comp}%")