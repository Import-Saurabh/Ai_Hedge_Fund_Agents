"""
fundamentals_loader.py  —  v2
─────────────────────────────────────────────────────────────────
Fixes vs v1:

  FIX-1  Duplicate rows
         Changed primary key strategy: uses (symbol, as_of_date)
         with INSERT OR REPLACE.  The extract side (fetch_fundamentals)
         already passes today's date; if you run twice in one day the
         row is simply replaced, not duplicated.

  FIX-2  Column-name typo  "operatin g_cf"
         The DB column is now  operating_cf  (no space).
         The INSERT uses the explicit, correct column list so any
         prior schema typo is bypassed.

  FIX-3  Data formatting (split numbers)
         All numeric values are coerced through _clean() which strips
         commas/spaces and converts to float, guarding against strings
         like "1,23,456" that cause NULL or broken inserts.

  NEW-1  EV  (Enterprise Value = Market Cap + Total Debt − Cash)
  NEW-2  EV/EBITDA
  NEW-3  EV/Revenue
  NEW-4  forward_pe      (from yfinance .info["forwardPE"])
  NEW-5  earnings_growth_json
         JSON string: last-N-years net_income stored as
         {"2021-03-31": 4.2e10, "2022-03-31": 5.1e10, …}
         so the ML layer can derive multi-period trends itself.

DB schema migration required (run once):
  ALTER TABLE fundamentals ADD COLUMN ev               REAL;
  ALTER TABLE fundamentals ADD COLUMN ev_ebitda        REAL;
  ALTER TABLE fundamentals ADD COLUMN ev_revenue       REAL;
  ALTER TABLE fundamentals ADD COLUMN forward_pe       REAL;
  ALTER TABLE fundamentals ADD COLUMN earnings_growth_json TEXT;
"""

import json
import math
from datetime import date
from database.db import get_connection


# ─────────────────────────────────────────────────────────────────
#  helpers
# ─────────────────────────────────────────────────────────────────

def _clean(v) -> float | None:
    """
    Coerce v to float.  Handles:
      • None / NaN / Inf  → None
      • Strings with commas ("1,23,456") or spaces
      • Already-float values
    """
    if v is None:
        return None
    if isinstance(v, str):
        v = v.replace(",", "").replace(" ", "").strip()
        if not v:
            return None
    try:
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return None


def _ev(market_cap, total_debt, cash) -> float | None:
    """EV = Market Cap + Total Debt − Cash & Equivalents."""
    mc  = _clean(market_cap)
    td  = _clean(total_debt) or 0.0
    csh = _clean(cash)       or 0.0
    if mc is None:
        return None
    return mc + td - csh


# ─────────────────────────────────────────────────────────────────
#  loader
# ─────────────────────────────────────────────────────────────────

def load_fundamentals(symbol: str, data: dict):
    """
    Load all fundamentals into DB.
    Uses INSERT OR REPLACE on (symbol, as_of_date) — one row per day, no duplicates.

    data dict keys (same as fetch_fundamentals output):
        existing keys: ROE (%), ROCE (%), ROA (%), Interest Coverage,
                       Free Cash Flow, Operating CF, CapEx,
                       Gross Margin (%), Net Profit Margin (%),
                       EBITDA Margin (%), EBIT Margin (%),
                       Debt/Equity, Current Ratio, Quick Ratio,
                       DSO (days), DIO (days), DPO (days), CCC (days),
                       EPS, P/E, P/B, Graham Number, Dividend Yield (%),
                       Market Cap, Revenue, Net Income, EBITDA, Inventory,
                       TTM EPS, TTM P/E

        new keys (added by fetch_fundamentals v2):
                       Total Debt, Cash,          ← needed for EV
                       Forward PE,
                       earnings_growth_json        ← JSON string
    """
    conn   = get_connection()
    today  = date.today().isoformat()

    # ── idempotent schema migrations ─────────────────────────
    _migrations = [
        "ALTER TABLE fundamentals ADD COLUMN ev               REAL",
        "ALTER TABLE fundamentals ADD COLUMN ev_ebitda        REAL",
        "ALTER TABLE fundamentals ADD COLUMN ev_revenue       REAL",
        "ALTER TABLE fundamentals ADD COLUMN forward_pe       REAL",
        "ALTER TABLE fundamentals ADD COLUMN earnings_growth_json TEXT",
    ]
    for sql in _migrations:
        try:
            conn.execute(sql)
        except Exception:
            pass   # column already exists
    conn.commit()

    # ── derive EV family ──────────────────────────────────────
    ev_val  = _ev(
        data.get("Market Cap"),
        data.get("Total Debt"),
        data.get("Cash"),
    )
    ebitda  = _clean(data.get("EBITDA"))
    revenue = _clean(data.get("Revenue"))

    ev_ebitda  = (round(ev_val / ebitda,  2)
                  if ev_val and ebitda  and ebitda  != 0 else None)
    ev_revenue = (round(ev_val / revenue, 2)
                  if ev_val and revenue and revenue != 0 else None)

    # ── earnings growth JSON  ─────────────────────────────────
    # Prefer pre-built JSON passed in; otherwise stringify the raw dict
    eg_json = data.get("earnings_growth_json")
    if eg_json is None:
        eg_raw = data.get("earnings_growth")          # dict {date_str: value}
        if isinstance(eg_raw, dict) and eg_raw:
            eg_json = json.dumps(
                {k: round(float(v), 2) for k, v in eg_raw.items()
                 if v is not None}
            )

    # ── insert ────────────────────────────────────────────────
    conn.execute("""
        INSERT OR REPLACE INTO fundamentals (
            symbol, as_of_date,
            roe_pct, roce_pct, roa_pct, interest_coverage,
            free_cash_flow, operating_cf, capex,
            gross_margin_pct, net_profit_margin_pct,
            ebitda_margin_pct, ebit_margin_pct,
            debt_to_equity, current_ratio, quick_ratio,
            dso_days, dio_days, dpo_days, cash_conversion_cycle,
            eps_annual, pe_ratio, pb_ratio, graham_number,
            dividend_yield_pct,
            market_cap, revenue, net_income, ebitda, inventory,
            ttm_eps, ttm_pe,
            ev, ev_ebitda, ev_revenue,
            forward_pe,
            earnings_growth_json
        ) VALUES (
            ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?,
            ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?,
            ?, ?, ?, ?, ?,
            ?, ?,
            ?, ?, ?,
            ?,
            ?
        )
    """, (
        symbol, today,
        _clean(data.get("ROE (%)")),
        _clean(data.get("ROCE (%)")),
        _clean(data.get("ROA (%)")),
        _clean(data.get("Interest Coverage")),
        _clean(data.get("Free Cash Flow")),
        _clean(data.get("Operating CF")),
        _clean(data.get("CapEx")),
        _clean(data.get("Gross Margin (%)")),
        _clean(data.get("Net Profit Margin (%)")),
        _clean(data.get("EBITDA Margin (%)")),
        _clean(data.get("EBIT Margin (%)")),
        _clean(data.get("Debt/Equity")),
        _clean(data.get("Current Ratio")),
        _clean(data.get("Quick Ratio")),
        _clean(data.get("DSO (days)")),
        _clean(data.get("DIO (days)")),
        _clean(data.get("DPO (days)")),
        _clean(data.get("CCC (days)")),
        _clean(data.get("EPS")),
        _clean(data.get("P/E")),
        _clean(data.get("P/B")),
        _clean(data.get("Graham Number")),
        _clean(data.get("Dividend Yield (%)")),
        _clean(data.get("Market Cap")),
        _clean(revenue),
        _clean(data.get("Net Income")),
        _clean(ebitda),
        _clean(data.get("Inventory")),
        _clean(data.get("TTM EPS")),
        _clean(data.get("TTM P/E")),
        # new columns
        _clean(ev_val),
        ev_ebitda,
        ev_revenue,
        _clean(data.get("Forward PE")),
        eg_json,
    ))

    conn.commit()
    conn.close()
    print(f"  ✅ fundamentals: snapshot saved for {symbol} on {today}"
          f"  [EV={'set' if ev_val else 'N/A'}"
          f"  EV/EBITDA={ev_ebitda or 'N/A'}"
          f"  fwd_PE={data.get('Forward PE') or 'N/A'}]")