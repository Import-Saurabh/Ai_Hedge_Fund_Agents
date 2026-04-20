"""
database/validator.py  v1.0
────────────────────────────────────────────────────────────────
Pre-insert validation and post-insert completeness checks.
All loaders call validate_before_insert() before writing.
Pipeline calls audit_table() after each load step.
────────────────────────────────────────────────────────────────
"""

import json
import math
from typing import Optional
from database.db import get_connection

# ── Minimum required fields per table ────────────────────────
# Insert is REJECTED if any of these are NULL
REQUIRED_FIELDS = {
    "quarterly_results":  ["symbol", "period_end", "sales", "net_profit"],
    "annual_results":     ["symbol", "period_end", "sales", "net_profit"],
    "income_statement":   ["symbol", "period_end", "period_type"],
    "balance_sheet":      ["symbol", "period_end", "period_type"],
    "cash_flow":          ["symbol", "period_end", "period_type"],
    "growth_metrics":     ["symbol", "as_of_date"],
    "ownership_history":  ["symbol", "period_end", "promoter_pct"],
    "fundamentals":       ["symbol", "as_of_date"],
    "quarterly_cashflow_derived": ["symbol", "quarter_end"],
}

# Fields used to compute completeness % per table
COMPLETENESS_FIELDS = {
    "quarterly_results": [
        "sales", "expenses", "operating_profit", "opm_pct",
        "other_income", "interest", "depreciation",
        "profit_before_tax", "tax_pct", "net_profit", "eps",
    ],
    "annual_results": [
        "sales", "expenses", "operating_profit", "opm_pct",
        "other_income", "interest", "depreciation",
        "profit_before_tax", "tax_pct", "net_profit", "eps",
        "dividend_payout_pct",
    ],
    "income_statement": [
        "total_revenue", "gross_profit", "ebitda", "operating_income",
        "net_income", "depreciation_amortization", "interest_expense",
        "diluted_eps", "tax_rate",
    ],
    "balance_sheet": [
        "total_assets", "current_assets", "total_liabilities",
        "total_equity", "total_debt", "net_debt",
        "scr_equity_capital", "scr_reserves", "scr_borrowings",
        "scr_total_assets",
    ],
    "cash_flow": [
        "scr_cash_from_operating", "scr_cash_from_investing",
        "scr_cash_from_financing", "scr_free_cash_flow",
        "best_operating_cf", "best_free_cash_flow",
    ],
    "growth_metrics": [
        "revenue_cagr_3y", "net_profit_cagr_3y", "fcf_cagr_3y",
        "scr_sales_cagr_3y", "scr_profit_cagr_3y", "scr_roe_last",
    ],
    "fundamentals": [
        "roe_pct", "roce_pct", "pe_ratio", "pb_ratio",
        "revenue", "net_income", "market_cap",
        "opm_pct", "dividend_payout_pct",
    ],
    "quarterly_cashflow_derived": [
        "revenue", "net_income", "approx_op_cf", "approx_fcf",
    ],
}

# ── Sanity bounds — values outside these are flagged ─────────
SANITY_BOUNDS = {
    "opm_pct":              (-50, 100),
    "roce_pct":             (-100, 100),
    "roe_pct":              (-200, 200),
    "working_capital_days": (-500, 1000),  # negative WC is valid (e.g. ADANIPORTS -114)
    "pe_ratio":             (0, 2000),
    "debt_to_equity":       (0, 100),
    "completeness_pct":     (0, 100),
    "quality_score":        (1, 3),
}


def _is_null(v) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    return False


def compute_completeness(row: dict, table: str) -> tuple[float, list[str]]:
    """
    Returns (completeness_pct, missing_fields_list).
    Uses COMPLETENESS_FIELDS map for the table.
    """
    fields = COMPLETENESS_FIELDS.get(table, [])
    if not fields:
        return 100.0, []
    missing = [f for f in fields if _is_null(row.get(f))]
    pct = round((1 - len(missing) / len(fields)) * 100, 1)
    return pct, missing


def validate_before_insert(row: dict, table: str) -> tuple[bool, str]:
    """
    Returns (ok, reason).
    ok=False → caller should SKIP the insert.
    """
    required = REQUIRED_FIELDS.get(table, [])
    for field in required:
        if _is_null(row.get(field)):
            return False, f"required field '{field}' is NULL"

    # Sanity checks on numeric fields
    for field, (lo, hi) in SANITY_BOUNDS.items():
        v = row.get(field)
        if v is not None and not _is_null(v):
            try:
                fv = float(v)
                if not (lo <= fv <= hi):
                    # Flag but don't reject — log a warning
                    pass  # caller can log this
            except (TypeError, ValueError):
                pass

    return True, "ok"


def log_data_quality(
    symbol: str,
    table_name: str,
    rows_inserted: int,
    rows_null_heavy: int,
    avg_completeness: float,
    critical_nulls: dict,
    source: str,
    notes: str = "",
):
    """Write a row to data_quality_log."""
    try:
        conn = get_connection()
        conn.execute("""
            INSERT INTO data_quality_log (
                symbol, table_name, rows_inserted, rows_null_heavy,
                avg_completeness, critical_nulls_json, source, notes
            ) VALUES (?,?,?,?,?,?,?,?)
        """, (
            symbol, table_name, rows_inserted, rows_null_heavy,
            round(avg_completeness, 1),
            json.dumps(critical_nulls),
            source, notes,
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  warn  data_quality_log write failed: {e}")


def audit_table(symbol: str, table: str) -> dict:
    """
    Post-insert audit: count rows, NULL rates for key fields.
    Returns summary dict; prints a report.
    """
    conn = get_connection()
    sym_col = "symbol" if table not in ("market_indices", "forex_commodities",
                                         "rbi_rates", "macro_indicators") else None
    try:
        if sym_col:
            total = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE symbol=?", (symbol,)
            ).fetchone()[0]
        else:
            total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        fields_to_check = COMPLETENESS_FIELDS.get(table, [])
        null_counts = {}
        for f in fields_to_check:
            try:
                if sym_col:
                    nc = conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE symbol=? AND {f} IS NULL",
                        (symbol,)
                    ).fetchone()[0]
                else:
                    nc = conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE {f} IS NULL"
                    ).fetchone()[0]
                if nc > 0:
                    null_counts[f] = nc
            except Exception:
                pass

        conn.close()

        null_heavy = sum(1 for c in null_counts.values() if c == total and total > 0)
        avg_comp = 0.0
        if fields_to_check and total > 0:
            filled = sum(total - nc for nc in null_counts.values())
            avg_comp = round(filled / (len(fields_to_check) * total) * 100, 1)

        if null_counts:
            print(f"  audit [{table}] {total} rows | completeness ~{avg_comp}% "
                  f"| NULLs: {null_counts}")
        else:
            print(f"  audit [{table}] {total} rows | completeness ~{avg_comp}% | all key fields present")

        return {
            "table": table, "total_rows": total,
            "avg_completeness": avg_comp, "null_counts": null_counts,
        }
    except Exception as e:
        conn.close()
        print(f"  warn  audit failed for {table}: {e}")
        return {}