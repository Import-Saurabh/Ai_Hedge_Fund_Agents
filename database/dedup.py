"""
database/dedup.py
─────────────────────────────────────────────────────────────────
Removes duplicate snapshot rows that accumulate when the pipeline
runs multiple days in a row but the underlying source data hasn't
changed (e.g. growth_metrics, ownership, rbi_rates, macro_indicators,
fundamentals).

Strategy: for each logical "entity + data fingerprint" keep only
the EARLIEST row (lowest rowid). Later runs with identical values
are deleted. Rows where the data genuinely changed are kept.

Call run_all_dedup() once at the end of every pipeline run.
─────────────────────────────────────────────────────────────────
"""

from database.db import get_connection


# ── Per-table dedup config ────────────────────────────────────
# key_cols    : columns that define "same entity"
# data_cols   : columns that define "same data content"
#               (if ALL are equal across rows for the same key → duplicate)
# keep        : "first" keeps oldest (MIN rowid), "last" keeps newest
_DEDUP_CONFIG = [
    {
        "table":     "growth_metrics",
        "key_cols":  ["symbol"],
        "data_cols": [
            "revenue_cagr_3y", "net_profit_cagr_3y", "ebitda_cagr_3y",
            "eps_cagr_3y", "fcf_cagr_3y",
            "revenue_yoy_json", "net_income_yoy_json",
        ],
        "keep": "first",
    },
    {
        "table":     "ownership",
        "key_cols":  ["symbol"],
        "data_cols": [
            "promoter_pct", "fii_fpi_pct", "dii_pct", "public_retail_pct",
            "total_institutional_pct",
        ],
        "keep": "first",
    },
    {
        "table":     "fundamentals",
        "key_cols":  ["symbol"],
        "data_cols": [
            "roe_pct", "roce_pct", "roa_pct",
            "free_cash_flow", "operating_cf", "capex",
            "gross_margin_pct", "net_profit_margin_pct",
            "eps_annual", "pe_ratio",
        ],
        "keep": "first",
    },
    {
        "table":     "rbi_rates",
        "key_cols":  [],          # no symbol — only one RBI
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
]


def _dedup_table(conn, table: str, key_cols: list, data_cols: list, keep: str = "first"):
    """
    Delete duplicate rows from `table`.

    Two rows are duplicates when all key_cols AND all data_cols match.
    We keep either the MIN(rowid) [keep='first'] or MAX(rowid) [keep='last'].
    """
    # Check table exists
    exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    if not exists:
        return 0

    group_cols = key_cols + data_cols
    if not group_cols:
        return 0

    group_expr = ", ".join(group_cols)
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
    """Run deduplication across all configured tables. Returns summary dict."""
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
    """
    Run once against an already-corrupted database to purge all
    accumulated duplicate snapshot rows.

    Usage:
        python -c "from database.dedup import run_one_time_cleanup; run_one_time_cleanup()"
    """
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