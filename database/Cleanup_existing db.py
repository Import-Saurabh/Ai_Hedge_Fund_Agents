"""
cleanup_existing_db.py  v3.0
────────────────────────────────────────────────────────────────
ONE-TIME script — run against your existing Ai_Hedge_Fund.db

Actions:
  1. Deduplicate snapshot tables
  2. Fix eps_revisions BLOB → integer
  3. Purge technical_indicators warmup rows (sma_200 IS NULL)
  4. Remove fabricated quarterly cashflow (is_interpolated=1)
  5. Rescale ALL monetary columns that are still in raw rupees
     → Rs. Crores (÷ 1e7, round to 2dp) in:
       balance_sheet, income_statement, cash_flow,
       fundamentals, quarterly_cashflow_derived, growth_metrics
  6. Fix adj_close NULL in price_daily (set = close as fallback)
  7. Normalize decimal precision in price_daily (4 dp)

Usage:
    python cleanup_existing_db.py --db "C:/path/to/Ai_Hedge_Fund.db"
    python cleanup_existing_db.py --db "C:/path/to/Ai_Hedge_Fund.db" --dry-run
    python cleanup_existing_db.py --db "C:/path/to/Ai_Hedge_Fund.db" --skip-rescale
────────────────────────────────────────────────────────────────
"""

import sys
import math
import struct
import sqlite3
import argparse

_CR = 1e7   # 1 Crore = 10,000,000
# Values above this threshold in a monetary column → still in raw rupees
_RAW_THRESHOLD = 1e8


def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ── 1. Deduplicate ────────────────────────────────────────────
DEDUP_SPECS = [
    ("growth_metrics",             ["symbol", "as_of_date",
                                    "revenue_cagr_3y", "net_profit_cagr_3y"]),
    ("ownership",                  ["symbol", "promoter_pct", "fii_fpi_pct",
                                    "dii_pct", "public_retail_pct"]),
    ("fundamentals",               ["symbol", "as_of_date",
                                    "roe_pct", "eps_annual"]),
    ("rbi_rates",                  ["effective_date", "repo_rate"]),
    ("macro_indicators",           ["indicator_name", "year", "value"]),
    ("market_indices",             ["index_name", "snapshot_date", "last_price"]),
    ("forex_commodities",          ["instrument", "snapshot_date", "last_price"]),
    ("earnings_estimates",         ["symbol", "period_code", "snapshot_date", "avg_eps"]),
    ("eps_trend",                  ["symbol", "period_code", "snapshot_date", "current_est"]),
    ("eps_revisions",              ["symbol", "period_code", "snapshot_date"]),
    ("quarterly_cashflow_derived", ["symbol", "quarter_end", "approx_fcf"]),
    ("technical_indicators",       ["symbol", "date", "close"]),
]


def dedup_all(conn):
    results = {}
    for table, cols in DEDUP_SPECS:
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if not exists:
            results[table] = "SKIP (table missing)"
            continue
        pragma     = conn.execute(f"PRAGMA table_info({table})").fetchall()
        table_cols = {row[1] for row in pragma}
        valid_cols = [c for c in cols if c in table_cols]
        if not valid_cols:
            results[table] = "SKIP (no matching cols)"
            continue
        sql = f"""
            DELETE FROM {table}
            WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM {table}
                GROUP BY {', '.join(valid_cols)}
            )
        """
        cur = conn.execute(sql)
        results[table] = cur.rowcount
    conn.commit()
    return results


# ── 2. Fix BLOB eps_revisions ─────────────────────────────────
def fix_eps_revisions_blobs(conn):
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='eps_revisions'"
    ).fetchone()
    if not exists:
        return 0
    rows = conn.execute(
        "SELECT rowid, up_last_7d, up_last_30d, down_last_30d, down_last_7d "
        "FROM eps_revisions"
    ).fetchall()
    fixed = 0
    for row in rows:
        rowid, *vals = row
        new_vals = []
        changed = False
        for v in vals:
            if isinstance(v, bytes) and len(v) == 8:
                new_vals.append(struct.unpack("<q", v)[0])
                changed = True
            elif v is not None:
                try:
                    new_vals.append(int(float(v)))
                except Exception:
                    new_vals.append(None)
            else:
                new_vals.append(None)
        if changed:
            conn.execute(
                "UPDATE eps_revisions SET up_last_7d=?, up_last_30d=?, "
                "down_last_30d=?, down_last_7d=? WHERE rowid=?",
                (*new_vals, rowid)
            )
            fixed += 1
    conn.commit()
    return fixed


# ── 3. Purge technical warmup NULLs ──────────────────────────
def purge_technical_nulls(conn):
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name='technical_indicators'"
    ).fetchone()
    if not exists:
        return 0
    pragma = conn.execute("PRAGMA table_info(technical_indicators)").fetchall()
    if "sma_200" not in {r[1] for r in pragma}:
        return 0
    cur = conn.execute("DELETE FROM technical_indicators WHERE sma_200 IS NULL")
    conn.commit()
    return cur.rowcount


# ── 4. Remove interpolated cashflow ──────────────────────────
def purge_interpolated_cashflow(conn):
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name='quarterly_cashflow_derived'"
    ).fetchone()
    if not exists:
        return 0
    cur = conn.execute(
        "DELETE FROM quarterly_cashflow_derived WHERE is_interpolated = 1"
    )
    conn.commit()
    return cur.rowcount


# ── 5. Rescale monetary columns → Rs. Crores ─────────────────
#
# All monetary columns in these tables should be in Rs. Crores.
# Detection: if AVG(abs(col)) > _RAW_THRESHOLD → still in raw rupees.
#
RESCALE_SPECS = {
    "balance_sheet": [
        "total_assets", "current_assets", "cash_and_equivalents",
        "cash_equivalents", "short_term_investments", "accounts_receivable",
        "inventory", "prepaid_assets", "restricted_cash",
        "other_current_assets", "total_non_current_assets",
        "net_ppe", "gross_ppe", "accumulated_depreciation",
        "goodwill", "other_intangibles", "long_term_equity_investment",
        "investment_in_fin_assets", "investment_properties",
        "total_liabilities", "current_liabilities", "accounts_payable",
        "current_debt", "long_term_debt", "total_equity",
        "stockholders_equity", "retained_earnings",
        "total_debt", "net_debt", "working_capital", "invested_capital",
        "tangible_book_value",
    ],
    "income_statement": [
        "total_revenue", "cost_of_revenue", "gross_profit",
        "selling_general_admin", "operating_expense", "operating_income",
        "ebit", "ebitda", "normalized_ebitda", "depreciation_amortization",
        "interest_expense", "interest_income", "net_interest_expense",
        "pretax_income", "tax_provision", "net_income", "net_income_common",
        "normalized_income", "minority_interests",
        "special_income_charges", "total_unusual_items",
    ],
    "cash_flow": [
        "operating_cash_flow", "net_income_ops", "depreciation",
        "change_in_working_capital", "change_in_receivables",
        "change_in_inventory", "change_in_payables",
        "other_non_cash_items", "taxes_refund_paid",
        "investing_cash_flow", "capex", "purchase_of_ppe", "sale_of_ppe",
        "purchase_of_business", "sale_of_business",
        "purchase_of_investments", "sale_of_investments",
        "financing_cash_flow", "net_debt_issuance",
        "long_term_debt_issuance", "long_term_debt_payments",
        "dividends_paid", "interest_paid",
        "free_cash_flow", "beginning_cash", "end_cash", "changes_in_cash",
    ],
    "fundamentals": [
        "free_cash_flow", "operating_cf", "capex",
        "market_cap", "revenue", "net_income", "ebitda",
        "ev",
    ],
    "quarterly_cashflow_derived": [
        "revenue", "net_income", "dna",
        "approx_op_cf", "approx_capex", "approx_fcf",
    ],
}


def _needs_rescale(conn, table: str, col: str) -> bool:
    try:
        row = conn.execute(
            f"SELECT AVG(ABS({col})) FROM {table} WHERE {col} IS NOT NULL"
        ).fetchone()
        if row and row[0] and abs(row[0]) > _RAW_THRESHOLD:
            return True
    except Exception:
        pass
    return False


def rescale_monetary_columns(conn):
    results = {}
    for table, cols in RESCALE_SPECS.items():
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if not exists:
            results[table] = "SKIP (missing)"
            continue
        pragma     = conn.execute(f"PRAGMA table_info({table})").fetchall()
        table_cols = {row[1] for row in pragma}
        rescaled   = []
        for col in cols:
            if col not in table_cols:
                continue
            if _needs_rescale(conn, table, col):
                conn.execute(
                    f"UPDATE {table} SET {col} = ROUND({col} / {_CR}, 2) "
                    f"WHERE {col} IS NOT NULL"
                )
                rescaled.append(col)
        conn.commit()
        results[table] = rescaled if rescaled else "already in Crores"
    return results


# ── 6 & 7. Fix price_daily ─────────────────────────────────────
def fix_price_daily(conn):
    """
    a) Fill adj_close = close where adj_close IS NULL (fallback)
    b) Round price columns to 4 dp, volume to integer
    """
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='price_daily'"
    ).fetchone()
    if not exists:
        return 0, 0

    # Fill NULL adj_close
    cur_adj = conn.execute(
        "UPDATE price_daily SET adj_close = ROUND(close, 4) WHERE adj_close IS NULL"
    )
    adj_fixed = cur_adj.rowcount

    # Round price columns
    conn.execute("""
        UPDATE price_daily SET
            open      = ROUND(open,  4),
            high      = ROUND(high,  4),
            low       = ROUND(low,   4),
            close     = ROUND(close, 4),
            adj_close = ROUND(adj_close, 4)
        WHERE open IS NOT NULL
    """)
    conn.commit()
    return adj_fixed, "rounded"


# ── Main ──────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="One-time DB cleanup v3")
    parser.add_argument("--db",           required=True, help="Path to SQLite DB")
    parser.add_argument("--dry-run",      action="store_true")
    parser.add_argument("--skip-rescale", action="store_true",
                        help="Skip monetary rescaling (already in Crores)")
    args = parser.parse_args()

    print(f"\n{'═'*62}")
    print(f"  DB CLEANUP v3  →  {args.db}")
    print(f"{'═'*62}\n")

    if args.dry_run:
        print("DRY RUN — no changes will be made.\n")
        return

    conn = get_conn(args.db)

    print("Step 1/7 — Deduplicating snapshot tables...")
    dedup_res  = dedup_all(conn)
    total_dup  = 0
    for t, d in dedup_res.items():
        n = d if isinstance(d, int) else 0
        total_dup += n
        label = f"removed {n}" if isinstance(d, int) and n > 0 else str(d)
        print(f"  {t:<35} {label}")
    print(f"  → Total: {total_dup} duplicates removed\n")

    print("Step 2/7 — Fixing eps_revisions BLOBs → integer...")
    n = fix_eps_revisions_blobs(conn)
    print(f"  → Fixed {n} rows\n")

    print("Step 3/7 — Purging technical_indicators warmup rows (sma_200 NULL)...")
    n = purge_technical_nulls(conn)
    print(f"  → Deleted {n} rows\n")

    print("Step 4/7 — Removing interpolated quarterly cashflow rows...")
    n = purge_interpolated_cashflow(conn)
    print(f"  → Deleted {n} rows\n")

    if not args.skip_rescale:
        print("Step 5/7 — Rescaling monetary columns to Rs. Crores...")
        res = rescale_monetary_columns(conn)
        for t, cols in res.items():
            if isinstance(cols, list) and cols:
                print(f"  {t}: rescaled {cols}")
            else:
                print(f"  {t}: {cols}")
        print()
    else:
        print("Step 5/7 — Skipped (--skip-rescale)\n")

    print("Step 6/7 — Fixing adj_close NULLs in price_daily...")
    adj_n, _ = fix_price_daily(conn)
    print(f"  → adj_close filled for {adj_n} rows\n")

    print("Step 7/7 — Rounding price_daily columns to 4 dp...")
    print("  → Done\n")

    conn.close()

    print(f"{'═'*62}")
    print("  CLEANUP COMPLETE")
    print("  Re-run the pipeline to repopulate fresh data.")
    print(f"{'═'*62}\n")


if __name__ == "__main__":
    main()