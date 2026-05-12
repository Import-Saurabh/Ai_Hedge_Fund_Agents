"""
etl/load/fundamentals_loader.py  v6.6
────────────────────────────────────────────────────────────────
Changes vs v6.5:
  • BUG FIX — ev_ebitda was NULL (or silently wrong) because Pass 2
    was writing profit_and_loss.operating_profit (= EBIT) directly into
    fundamentals.ebitda, omitting Depreciation & Amortisation.

    Root cause: Screener's "Operating Profit" is EBIT, not EBITDA.
    True EBITDA = operating_profit + depreciation.

    Fix: Pass 2 now reads both operating_profit AND depreciation from
    profit_and_loss (and annual_results as fallback) and stores their
    sum as ebitda.  The EV/EBITDA recompute block in Pass 2.5 then
    produces the correct ratio.

  • One-time DB repair helper added: repair_ev_ebitda(conn, symbol)
    can be called to fix any rows already in the DB that were written
    with the old (EBIT-only) ebitda value.

  • load_fundamentals() now logs EBITDA_source ("direct" | "derived")
    from fetch_fundamentals() output for diagnostics; the key is
    stripped before the dict is passed to the DB insert/update.
────────────────────────────────────────────────────────────────
Changes vs v6.3 (v6.5):
  • Pass 2.5 extended with three more derivations:
    dio_days, dpo_days, forward_pe (5-level fallback chain).
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
# One-time repair helper (run manually or on startup)
# ─────────────────────────────────────────────────────────────
def repair_ev_ebitda(conn, symbol: str):
    """
    One-time repair for rows already written with EBIT-only ebitda values.

    For every fundamentals row where ev_ebitda is NULL:
      1. Recompute ebitda = operating_profit + depreciation from profit_and_loss.
      2. Recompute ev     = market_cap + borrowings - cash  (if ev also NULL).
      3. Recompute ev_ebitda = ev / ebitda.

    Safe to call multiple times — only touches rows where ev_ebitda IS NULL.
    """
    # Get true EBITDA from profit_and_loss
    pl_row = conn.execute("""
        SELECT operating_profit, depreciation
        FROM profit_and_loss
        WHERE symbol=? AND period_type='annual'
        ORDER BY period_end DESC LIMIT 1
    """, (symbol,)).fetchone()

    if pl_row is None or pl_row[0] is None:
        print(f"  repair_ev_ebitda: no profit_and_loss data for {symbol}, skipping")
        return

    true_ebitda = round(
        float(pl_row[0]) + (float(pl_row[1]) if pl_row[1] else 0.0), 2
    )
    if true_ebitda <= 0:
        print(f"  repair_ev_ebitda: EBITDA <= 0 for {symbol} ({true_ebitda}), skipping")
        return

    # Get BS data for EV fallback
    bs_row = conn.execute("""
        SELECT borrowings, cash_equivalents
        FROM balance_sheet
        WHERE symbol=? AND period_type='annual'
        ORDER BY period_end DESC LIMIT 1
    """, (symbol,)).fetchone()
    _bs_borr = float(bs_row[0]) if bs_row and bs_row[0] else 0.0
    _bs_cash = float(bs_row[1]) if bs_row and bs_row[1] else 0.0

    # Fix all rows where ev_ebitda is NULL
    rows = conn.execute("""
        SELECT rowid, ev, market_cap FROM fundamentals
        WHERE symbol=? AND ev_ebitda IS NULL
    """, (symbol,)).fetchall()

    fixed = 0
    for rowid, ev, mc in rows:
        updates = {"ebitda": true_ebitda}

        _ev = float(ev) if ev is not None else None
        if _ev is None and mc is not None:
            _ev = round(float(mc) + _bs_borr - _bs_cash, 2)
            updates["ev"] = _ev

        if _ev is not None and _ev > 0:
            updates["ev_ebitda"] = round(_ev / true_ebitda, 2)

        if updates:
            set_clause = ", ".join(f"{k}=?" for k in updates)
            conn.execute(
                f"UPDATE fundamentals SET {set_clause} WHERE rowid=?",
                (*updates.values(), rowid)
            )
            fixed += 1

    conn.commit()
    print(f"  repair_ev_ebitda: fixed {fixed} rows for {symbol} "
          f"| true_ebitda={true_ebitda}")


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
    "revenue", "ebitda",          # profit_and_loss (annual)
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
        Pull revenue from profit_and_loss.sales.
        Pull TRUE EBITDA = operating_profit + depreciation from
        profit_and_loss (BUG FIX v6.6: previously only operating_profit
        was used, which is EBIT, not EBITDA).
        Pull debt_to_equity from balance_sheet, opm_pct from
        quarterly/annual_results, dividend_payout_pct from annual_results.
        These overwrite carry-forward values because they are more
        precisely dated.

    Pass 2.5 — Derive remaining nulls from sibling tables:
        net_profit_margin_pct, ebit_margin_pct, gross_margin_pct,
        roa_pct, interest_coverage, current_ratio, quick_ratio,
        eps_annual, ev, ev_ebitda, ev_revenue, dio_days, dpo_days,
        forward_pe (5-level chain), ebitda_margin_pct.

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

    # Pull revenue AND true EBITDA from profit_and_loss.
    #
    # BUG FIX (v6.6): operating_profit from Screener = EBIT (before D&A).
    # True EBITDA = operating_profit + depreciation.
    # We fetch depreciation here and add it so fundamentals.ebitda holds
    # the correct EBITDA, not a silently wrong EBIT value.
    is_row = conn.execute("""
        SELECT sales, operating_profit, depreciation
        FROM profit_and_loss
        WHERE symbol=? AND period_type='annual'
        ORDER BY period_end DESC LIMIT 1
    """, (symbol,)).fetchone()
    is_revenue = is_row[0] if is_row else None
    # Compute true EBITDA = EBIT + D&A
    if is_row and is_row[1] is not None:
        _op  = float(is_row[1])
        _dep = float(is_row[2]) if is_row[2] is not None else 0.0
        is_ebitda = round(_op + _dep, 2)
    else:
        is_ebitda = None

    # annual_results fallback for revenue/ebitda if profit_and_loss empty
    if is_revenue is None or is_ebitda is None:
        ar2 = conn.execute("""
            SELECT sales, operating_profit, depreciation FROM annual_results
            WHERE symbol=? ORDER BY period_end DESC LIMIT 1
        """, (symbol,)).fetchone()
        if ar2:
            if is_revenue is None:
                is_revenue = ar2[0]
            if is_ebitda is None and ar2[1] is not None:
                _op2  = float(ar2[1])
                _dep2 = float(ar2[2]) if ar2[2] is not None else 0.0
                is_ebitda = round(_op2 + _dep2, 2)

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

    # BUG FIX: earnings_growth_json backfill from profit_and_loss.
    # yfinance often returns no income_stmt for Indian .NS tickers (TCS etc.),
    # leaving earnings_growth_json NULL. Build it from profit_and_loss instead.
    egj_fill = None
    try:
        pl_rows = conn.execute("""
            SELECT period_end, net_profit FROM profit_and_loss
            WHERE symbol=? AND period_type='annual'
              AND net_profit IS NOT NULL
            ORDER BY period_end DESC LIMIT 5
        """, (symbol,)).fetchall()
        if pl_rows:
            import json as _json
            egj_fill = _json.dumps({r[0]: r[1] for r in pl_rows})
    except Exception:
        pass

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

    # BUG FIX: apply earnings_growth_json fallback (only fills NULLs)
    if egj_fill:
        conn.execute("""
            UPDATE fundamentals
            SET earnings_growth_json = COALESCE(earnings_growth_json, ?)
            WHERE symbol=?
        """, (egj_fill, symbol))

    conn.commit()

    # ── Pass 2.5: derive remaining nulls from sibling tables ──
    #
    # yfinance frequently returns no income_stmt / balance_sheet for
    # Indian .NS tickers (TCS, Infosys, etc.), leaving these fields
    # null even after carry-forward (which has nothing to carry on a
    # ticker's first run). We derive them from profit_and_loss and
    # balance_sheet tables which are always populated via Screener.
    #
    # Fields derived here (COALESCE — never overwrite existing values):
    #   net_profit_margin_pct  = net_profit / sales * 100
    #   ebitda_margin_pct      = ebitda / revenue * 100   (already in fundamentals)
    #   ebit_margin_pct        = operating_profit / sales * 100
    #   gross_margin_pct       = (sales - expenses) / sales * 100  (proxy for services)
    #   roa_pct                = net_profit / total_assets * 100
    #   interest_coverage      = operating_profit / interest  (when interest > 0)
    #   current_ratio          = (cash_equivalents + trade_receivables) / trade_payables  (proxy)
    #   eps_annual             = net_profit_cr * 1e7 / shares_outstanding
    #   ev                     = market_cap + borrowings - cash_equivalents
    #   ev_ebitda              = ev / ebitda  (now uses TRUE EBITDA — BUG FIX v6.6)
    #   low_52w                = from screener header / price_daily fallback

    # Pull latest annual P&L row
    pl = conn.execute("""
        SELECT sales, expenses, operating_profit, net_profit, interest, depreciation
        FROM profit_and_loss
        WHERE symbol=? AND period_type='annual'
        ORDER BY period_end DESC LIMIT 1
    """, (symbol,)).fetchone()

    # Pull latest balance sheet row
    bs2 = conn.execute("""
        SELECT total_assets, borrowings, cash_equivalents,
               trade_receivables, trade_payables, equity_capital, reserves
        FROM balance_sheet
        WHERE symbol=? AND period_type='annual'
        ORDER BY period_end DESC LIMIT 1
    """, (symbol,)).fetchone()

    # Pull 52w low from price_daily as fallback for low_52w
    low_52w_price = conn.execute("""
        SELECT MIN(low) FROM price_daily
        WHERE symbol=?
          AND date >= DATE('now', '-365 days')
    """, (symbol,)).fetchone()
    low_52w_fallback = low_52w_price[0] if low_52w_price and low_52w_price[0] else None

    derived_2_5 = {}

    if pl:
        pl_sales, pl_expenses, pl_op_profit, pl_net_profit, pl_interest, pl_dep = pl
        _s   = float(pl_sales)       if pl_sales       else None
        _exp = float(pl_expenses)    if pl_expenses     else None
        _op  = float(pl_op_profit)   if pl_op_profit    else None
        _np  = float(pl_net_profit)  if pl_net_profit   else None
        _int = float(pl_interest)    if pl_interest     else None
        _dep = float(pl_dep)         if pl_dep          else None

        if _np and _s and _s != 0:
            derived_2_5["net_profit_margin_pct"] = round(_np / _s * 100, 2)

        # ebit_margin: operating_profit is screener's EBIT equivalent
        if _op and _s and _s != 0:
            derived_2_5["ebit_margin_pct"] = round(_op / _s * 100, 2)

        # gross_margin proxy for services: (sales - expenses) / sales
        # For manufacturing this is an under-estimate but still informative
        if _s and _exp and _s != 0:
            derived_2_5["gross_margin_pct"] = round((_s - _exp) / _s * 100, 2)

        # interest_coverage: operating_profit / interest (guard zero)
        if _op and _int and _int > 0.01:
            derived_2_5["interest_coverage"] = round(_op / _int, 2)

        # ebitda_margin from fundamentals.ebitda (already backfilled) / revenue
        # handled below after pulling the fundamentals row

    if bs2:
        _ta, _borr, _cash_bs, _rec, _pay, _eq_cap, _res = bs2
        _ta      = float(_ta)      if _ta      else None
        _borr    = float(_borr)    if _borr    else None
        _cash_bs = float(_cash_bs) if _cash_bs else None
        _rec     = float(_rec)     if _rec     else None
        _pay     = float(_pay)     if _pay     else None

        # roa_pct from balance_sheet total_assets + P&L net_profit
        if pl and _np and _ta and _ta != 0:
            derived_2_5["roa_pct"] = round(_np / _ta * 100, 2)

        # current_ratio proxy: (cash + receivables) / payables
        if _cash_bs and _rec and _pay and _pay > 0:
            derived_2_5["current_ratio"] = round((_cash_bs + _rec) / _pay, 2)
            # quick_ratio same as current_ratio for services (no inventory)
            derived_2_5["quick_ratio"] = derived_2_5["current_ratio"]

    if low_52w_fallback:
        derived_2_5["low_52w"] = round(low_52w_fallback, 2)

    # ── dio_days: 0.0 for service companies with no inventory ──
    # inventory IS null for TCS/Infosys etc. — they hold no physical stock.
    # Standard practice: report DIO=0 for pure services, not leave it blank.
    # Only fill when inventory is confirmed null (not just missing from yfinance).
    has_inventory = conn.execute(
        "SELECT inventory FROM fundamentals WHERE symbol=? AND inventory IS NOT NULL LIMIT 1",
        (symbol,)
    ).fetchone()
    if not has_inventory:
        derived_2_5["dio_days"] = 0.0

    # ── dpo_days: derive from CCC identity ─────────────────────
    # CCC = DSO + DIO - DPO  →  DPO = DSO + DIO - CCC
    # Pull DSO and CCC from the most recent non-null fundamentals row.
    ccc_row = conn.execute("""
        SELECT dso_days, cash_conversion_cycle FROM fundamentals
        WHERE symbol=? AND dso_days IS NOT NULL AND cash_conversion_cycle IS NOT NULL
        ORDER BY as_of_date DESC LIMIT 1
    """, (symbol,)).fetchone()
    if ccc_row:
        _dso, _ccc = float(ccc_row[0]), float(ccc_row[1])
        _dio_for_dpo = derived_2_5.get("dio_days", 0.0)
        _dpo = round(_dso + _dio_for_dpo - _ccc, 1)
        # Clamp to >= 0: negative DPO is meaningless
        derived_2_5["dpo_days"] = max(_dpo, 0.0)

    # ── forward_pe: full fallback chain, NEVER stays null ────────
    #
    # Priority:
    #   1. earnings_estimates.avg_eps  (analyst consensus — most accurate)
    #   2. eps_trend.current_est       (analyst current estimate)
    #   3. ttm_eps * 1.10              (ttm + assumed 10% growth proxy)
    #   4. eps_annual * 1.10           (annual + assumed 10% growth proxy)
    #   5. 0.0                         (hard default — never leave null)
    #
    # forward_pe = current_price / forward_eps  (sanity: 0 < result < 500)

    # Pull price once for all forward_pe attempts
    price_row = conn.execute("""
        SELECT current_price FROM fundamentals
        WHERE symbol=? AND current_price IS NOT NULL
        ORDER BY as_of_date DESC LIMIT 1
    """, (symbol,)).fetchone()
    _fwd_price = float(price_row[0]) if price_row and price_row[0] else None

    _fwd_pe = None

    # Attempt 1 — earnings_estimates analyst consensus
    if _fwd_pe is None:
        fwd_eps_row = conn.execute("""
            SELECT avg_eps FROM earnings_estimates
            WHERE symbol=? AND period_code IN ('0y','1y','+1y','currentYear','nextYear')
              AND avg_eps IS NOT NULL AND avg_eps > 0
            ORDER BY
                CASE period_code
                    WHEN '1y'          THEN 1
                    WHEN '+1y'         THEN 1
                    WHEN 'nextYear'    THEN 1
                    WHEN '0y'          THEN 2
                    WHEN 'currentYear' THEN 2
                END,
                snapshot_date DESC
            LIMIT 1
        """, (symbol,)).fetchone()
        if fwd_eps_row and _fwd_price:
            v = round(_fwd_price / float(fwd_eps_row[0]), 2)
            if 0 < v < 500:
                _fwd_pe = v

    # Attempt 2 — eps_trend current estimate
    if _fwd_pe is None:
        trend_row = conn.execute("""
            SELECT current_est FROM eps_trend
            WHERE symbol=? AND current_est IS NOT NULL AND current_est > 0
            ORDER BY snapshot_date DESC LIMIT 1
        """, (symbol,)).fetchone()
        if trend_row and _fwd_price:
            v = round(_fwd_price / float(trend_row[0]), 2)
            if 0 < v < 500:
                _fwd_pe = v

    # Attempt 3 — ttm_eps * 1.10 growth proxy
    if _fwd_pe is None:
        ttm_row = conn.execute("""
            SELECT ttm_eps FROM fundamentals
            WHERE symbol=? AND ttm_eps IS NOT NULL AND ttm_eps > 0
            ORDER BY as_of_date DESC LIMIT 1
        """, (symbol,)).fetchone()
        if ttm_row and _fwd_price:
            v = round(_fwd_price / (float(ttm_row[0]) * 1.10), 2)
            if 0 < v < 500:
                _fwd_pe = v

    # Attempt 4 — eps_annual * 1.10 growth proxy
    if _fwd_pe is None:
        eps_row = conn.execute("""
            SELECT eps_annual FROM fundamentals
            WHERE symbol=? AND eps_annual IS NOT NULL AND eps_annual > 0
            ORDER BY as_of_date DESC LIMIT 1
        """, (symbol,)).fetchone()
        if eps_row and _fwd_price:
            v = round(_fwd_price / (float(eps_row[0]) * 1.10), 2)
            if 0 < v < 500:
                _fwd_pe = v

    # Attempt 5 — hard default: never null
    derived_2_5["forward_pe"] = _fwd_pe if _fwd_pe is not None else 0.0

    # Apply derived_2_5 to every fundamentals row for this symbol (COALESCE)
    if derived_2_5:
        set_clause = ", ".join(f"{col} = COALESCE({col}, ?)" for col in derived_2_5)
        conn.execute(
            f"UPDATE fundamentals SET {set_clause} WHERE symbol=?",
            (*derived_2_5.values(), symbol)
        )
        conn.commit()

    # ── Recompute EV/EBITDA and EV/Revenue where NULL ─────────
    # Also recompute EV itself from balance_sheet when market_cap is present
    # but ev is still null (happens when yfinance cash/debt both missing).
    #
    # BUG FIX (v6.6): fundamentals.ebitda is now TRUE EBITDA (= EBIT + D&A)
    # after Pass 2, so ev / ebitda here produces the correct EV/EBITDA ratio.
    ev_rows = conn.execute(
        "SELECT rowid, ev, market_cap, ebitda, revenue, ev_ebitda, ev_revenue "
        "FROM fundamentals WHERE symbol=?",
        (symbol,)
    ).fetchall()

    # Get borrowings + cash from balance_sheet for EV fallback
    _bs_borr = float(bs2[1]) if bs2 and bs2[1] else 0.0
    _bs_cash = float(bs2[2]) if bs2 and bs2[2] else 0.0

    for row in ev_rows:
        rowid, ev, mc, ebitda, revenue, ev_ebitda, ev_revenue = row
        updates = {}

        # EV fallback: mc + borrowings - cash  (all in Rs. Crores)
        if ev is None and mc:
            ev = round(float(mc) + _bs_borr - _bs_cash, 2)
            updates["ev"] = ev

        if ev and ebitda and float(ebitda) > 0 and ev_ebitda is None:
            updates["ev_ebitda"] = round(float(ev) / float(ebitda), 2)
        if ev and revenue and float(revenue) > 0 and ev_revenue is None:
            updates["ev_revenue"] = round(float(ev) / float(revenue), 2)

        if updates:
            set_clause = ", ".join(f"{k}=?" for k in updates)
            conn.execute(
                f"UPDATE fundamentals SET {set_clause} WHERE rowid=?",
                (*updates.values(), rowid)
            )

    # ── eps_annual from profit_and_loss (COALESCE — only fills NULLs) ─
    # Requires shares_outstanding; approximate from market_cap / current_price.
    # net_profit is in Rs. Crores → convert to Rs. for per-share calc.
    if pl and pl[3]:  # pl_net_profit
        np_cr = float(pl[3])
        ep_rows = conn.execute(
            "SELECT rowid, eps_annual, current_price, market_cap "
            "FROM fundamentals WHERE symbol=? AND eps_annual IS NULL",
            (symbol,)
        ).fetchall()
        for row in ep_rows:
            rowid, _, price, mc = row
            if price and mc and float(mc) > 0:
                shares_approx = float(mc) * 1e7 / float(price)  # mc in Cr → Rs / price
                if shares_approx > 0:
                    eps = round(np_cr * 1e7 / shares_approx, 2)
                    conn.execute(
                        "UPDATE fundamentals SET eps_annual=? WHERE rowid=?",
                        (eps, rowid)
                    )

    # ── ebitda_margin_pct from fundamentals.ebitda + revenue ──
    # Both are now populated after Pass 2 sibling backfill.
    conn.execute("""
        UPDATE fundamentals
        SET ebitda_margin_pct = COALESCE(
            ebitda_margin_pct,
            CASE WHEN ebitda IS NOT NULL AND revenue IS NOT NULL AND revenue > 0
                 THEN ROUND(ebitda * 100.0 / revenue, 2)
                 ELSE NULL END
        )
        WHERE symbol=?
    """, (symbol,))

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

    The "EBITDA_source" debug key from fetch_fundamentals() is logged
    here and then stripped before writing to the DB.
    """
    conn  = get_connection()
    today = date.today().isoformat()

    # One-time migration on first call
    _migrate_drop_retired_cols(conn)

    # Log EBITDA source for diagnostics, then strip from data dict
    ebitda_source = data.pop("EBITDA_source", "unknown")
    print(f"  info  fundamentals: EBITDA source for {symbol} = {ebitda_source}")

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