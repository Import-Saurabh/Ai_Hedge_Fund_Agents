"""
database/dedup.py  v2.0
─────────────────────────────────────────────────────────────────
Fixes vs v1:
  • growth_metrics: dedup now compares as_of_date + cagr values
    (not just symbol) so re-runs on same day with same data skip
  • fundamentals: dedup keyed on (symbol, as_of_date) + roe/eps
  • market_indices / forex_commodities: already have UNIQUE on
    (snapshot_date, name) — dedup config updated to match
  • Added quarterly_cashflow_derived to dedup config
─────────────────────────────────────────────────────────────────
"""

from database.db import get_connection


_DEDUP_CONFIG = [
    {
        "table":     "growth_metrics",
        "key_cols":  ["symbol", "as_of_date"],
        "data_cols": ["revenue_cagr_3y", "net_profit_cagr_3y",
                      "ebitda_cagr_3y", "fcf_cagr_3y"],
        "keep": "first",
    },
    {
        "table":     "ownership",
        "key_cols":  ["symbol"],
        "data_cols": ["promoter_pct", "fii_fpi_pct", "dii_pct",
                      "public_retail_pct", "total_institutional_pct"],
        "keep": "first",
    },
    {
        "table":     "fundamentals",
        "key_cols":  ["symbol", "as_of_date"],
        "data_cols": ["roe_pct", "roce_pct", "roa_pct",
                      "eps_annual", "pe_ratio"],
        "keep": "first",
    },
    {
        "table":     "rbi_rates",
        "key_cols":  ["effective_date"],
        "data_cols": ["repo_rate", "reverse_repo", "crr", "slr"],
        "keep": "first",
    },
    {
        "table":     "macro_indicators",
        "key_cols":  ["indicator_name", "year"],
        "data_cols": ["value"],
        "keep": "first",
    },
    {
        "table":     "market_indices",
        "key_cols":  ["index_name", "snapshot_date"],
        "data_cols": ["last_price"],
        "keep": "first",
    },
    {
        "table":     "forex_commodities",
        "key_cols":  ["instrument", "snapshot_date"],
        "data_cols": ["last_price"],
        "keep": "first",
    },
    {
        "table":     "earnings_estimates",
        "key_cols":  ["symbol", "period_code", "snapshot_date"],
        "data_cols": ["avg_eps", "analyst_count"],
        "keep": "first",
    },
    {
        "table":     "eps_trend",
        "key_cols":  ["symbol", "period_code", "snapshot_date"],
        "data_cols": ["current_est"],
        "keep": "first",
    },
    {
        "table":     "eps_revisions",
        "key_cols":  ["symbol", "period_code", "snapshot_date"],
        "data_cols": ["up_last_7d", "down_last_7d"],
        "keep": "first",
    },
    {
        "table":     "quarterly_cashflow_derived",
        "key_cols":  ["symbol", "quarter_end"],
        "data_cols": ["approx_fcf", "net_income"],
        "keep": "first",
    },
    {
        "table":     "technical_indicators",
        "key_cols":  ["symbol", "date"],
        "data_cols": ["close", "rsi_14"],
        "keep": "first",
    },
]


def _dedup_table(conn, table: str, key_cols: list,
                 data_cols: list, keep: str = "first") -> int:
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if not exists:
        return 0

    group_cols = key_cols + data_cols
    if not group_cols:
        return 0

    # Only include columns that actually exist in the table
    pragma     = conn.execute(f"PRAGMA table_info({table})").fetchall()
    table_cols = {r[1] for r in pragma}
    valid_cols = [c for c in group_cols if c in table_cols]
    if not valid_cols:
        return 0

    group_expr = ", ".join(valid_cols)
    agg        = "MIN(rowid)" if keep == "first" else "MAX(rowid)"

    sql = f"""
        DELETE FROM {table}
        WHERE rowid NOT IN (
            SELECT {agg}
            FROM   {table}
            GROUP  BY {group_expr}
        )
    """
    cur = conn.execute(sql)
    return cur.rowcount


def run_all_dedup():
    """Run deduplication across all configured tables."""
    conn    = get_connection()
    summary = {}

    for cfg in _DEDUP_CONFIG:
        table     = cfg["table"]
        key_cols  = cfg["key_cols"]
        data_cols = cfg["data_cols"]
        keep      = cfg.get("keep", "first")

        try:
            deleted = _dedup_table(conn, table, key_cols, data_cols, keep)
            summary[table] = deleted
            if deleted:
                print(f"  🧹 dedup {table}: removed {deleted} duplicate rows")
        except Exception as e:
            summary[table] = f"ERROR: {e}"
            print(f"  ⚠  dedup {table}: {e}")

    conn.commit()
    conn.close()
    return summary


def run_one_time_cleanup():
    print("═" * 60)
    print("ONE-TIME DEDUP CLEANUP")
    print("═" * 60)
    summary = run_all_dedup()
    total   = sum(v for v in summary.values() if isinstance(v, int))
    print(f"\nTotal rows removed: {total}")
    print("═" * 60)
    return summary


if __name__ == "__main__":
    run_one_time_cleanup()