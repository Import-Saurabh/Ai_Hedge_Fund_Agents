"""
cleanup_existing_db.py  v2.0
────────────────────────────────────────────────────────────────
ONE-TIME script — run this against your existing database.

Actions:
  1. Deduplicate snapshot tables (growth_metrics, ownership, etc.)
  2. Fix eps_revisions BLOB → integer
  3. Purge technical_indicators warmup rows (sma_200=NULL)
  4. Remove fabricated quarterly cashflow (is_interpolated=1)
  5. Rescale monetary columns from raw rupees → Rs. Crores
     in: fundamentals, quarterly_cashflow_derived, growth_metrics

Usage:
    python cleanup_existing_db.py --db "C:/path/to/Ai_Hedge_Fund.db"
    python cleanup_existing_db.py --db "C:/path/to/Ai_Hedge_Fund.db" --skip-rescale
────────────────────────────────────────────────────────────────
"""

import sys
import math
import struct
import sqlite3
import argparse

_CR = 1e7   # 1 Crore = 10,000,000


def get_conn(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


# ── 1. Deduplicate ───────────────────────────────────────────
DEDUP_SPECS = [
    ("growth_metrics",    ["symbol", "revenue_cagr_3y", "net_profit_cagr_3y",
                           "ebitda_cagr_3y", "fcf_cagr_3y"]),
    ("ownership",         ["symbol", "promoter_pct", "fii_fpi_pct",
                           "dii_pct", "public_retail_pct"]),
    ("fundamentals",      ["symbol", "roe_pct", "roce_pct", "roa_pct",
                           "free_cash_flow", "eps_annual"]),
    ("rbi_rates",         ["repo_rate", "reverse_repo", "crr", "slr"]),
    ("macro_indicators",  ["indicator_name", "year", "value"]),
    ("market_indices",    ["index_name", "snapshot_date", "last_price"]),
    ("forex_commodities", ["instrument",  "snapshot_date", "last_price"]),
    ("earnings_estimates",["symbol", "period_code", "snapshot_date", "avg_eps"]),
    ("eps_trend",         ["symbol", "period_code", "snapshot_date", "current_est"]),
    ("eps_revisions",     ["symbol", "period_code", "snapshot_date"]),
]


def dedup_all(conn):
    results = {}
    for table, cols in DEDUP_SPECS:
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        if not exists:
            results[table] = "SKIP"
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


# ── 2. Fix BLOB eps_revisions ────────────────────────────────
def fix_eps_revisions_blobs(conn):
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='eps_revisions'"
    ).fetchone()
    if not exists:
        return 0
    rows  = conn.execute(
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
            elif isinstance(v, (int, float)) and v is not None:
                new_vals.append(int(v))
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


# ── 3. Purge technical warmup NULLs ─────────────────────────
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


# ── 4. Remove interpolated cashflow ─────────────────────────
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


# ── 5. Rescale monetary columns raw → Rs. Crores ─────────────
#
# Detection heuristic: if a monetary value > 1e9 it's likely in
# raw rupees (e.g. 1,10,923,10,00,00 = ₹1.1 lakh crore). We
# divide by 1e7 to get Crores. Values already in Crores are < 1e5
# for most Indian large-caps.
#
RESCALE_SPECS = {
    # table: [columns to rescale]
    "fundamentals": [
        "free_cash_flow", "operating_cf", "capex",
        "market_cap", "revenue", "net_income", "ebitda",
        "ev", "total_debt",
    ],
    "quarterly_cashflow_derived": [
        "revenue", "net_income", "dna",
        "approx_op_cf", "approx_capex", "approx_fcf",
    ],
}

_CRORE_THRESHOLD = 1e8    # values above this in the column → still in raw rupees


def _needs_rescale(conn, table: str, col: str) -> bool:
    """Return True if the column median is clearly in raw rupees (> 1e8)."""
    try:
        row = conn.execute(
            f"SELECT AVG({col}) FROM {table} WHERE {col} IS NOT NULL"
        ).fetchone()
        if row and row[0] and abs(row[0]) > _CRORE_THRESHOLD:
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
            results[table] = "SKIP"
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
        results[table] = rescaled
    return results


# ── Main ─────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-rescale", action="store_true",
                        help="Skip monetary rescaling (if already in Crores)")
    args = parser.parse_args()

    print(f"\n{'═'*62}")
    print(f"  DB CLEANUP v2  →  {args.db}")
    print(f"{'═'*62}\n")

    if args.dry_run:
        print("DRY RUN — no changes made.\n")
        return

    conn = get_conn(args.db)

    print("Step 1/5 — Deduplicating snapshot tables...")
    dedup_res = dedup_all(conn)
    total_dup = 0
    for t, d in dedup_res.items():
        n = d if isinstance(d, int) else 0
        total_dup += n
        print(f"  {t:<30} {'removed ' + str(n) if n else str(d)}")
    print(f"  → Total: {total_dup} duplicates removed\n")

    print("Step 2/5 — Fixing eps_revisions BLOBs...")
    n = fix_eps_revisions_blobs(conn)
    print(f"  → Fixed {n} rows\n")

    print("Step 3/5 — Purging technical_indicators warmup rows...")
    n = purge_technical_nulls(conn)
    print(f"  → Deleted {n} rows\n")

    print("Step 4/5 — Removing interpolated quarterly cashflow...")
    n = purge_interpolated_cashflow(conn)
    print(f"  → Deleted {n} rows\n")

    if not args.skip_rescale:
        print("Step 5/5 — Rescaling monetary columns to Rs. Crores...")
        res = rescale_monetary_columns(conn)
        for t, cols in res.items():
            print(f"  {t}: {cols if cols != 'SKIP' else 'skipped'}")
        print()
    else:
        print("Step 5/5 — Skipped rescaling (--skip-rescale)\n")

    conn.close()

    print(f"{'═'*62}")
    print("  CLEANUP COMPLETE — re-run pipeline to repopulate fresh data")
    print(f"{'═'*62}\n")


if __name__ == "__main__":
    main()