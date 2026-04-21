"""
database/validator.py  v2.0
────────────────────────────────────────────────────────────────
Key fixes vs v1:
  • COMPLETENESS_FIELDS match actual DB column names exactly
  • audit_table uses correct WHERE clause for symbol-keyed tables
  • quarterly_results / annual_results completeness fields correct
  • fundamentals completeness uses key financial fields
────────────────────────────────────────────────────────────────
"""

import json
import math
from typing import Optional
from database.db import get_connection

REQUIRED_FIELDS = {
    "quarterly_results":          ["symbol", "period_end", "sales", "net_profit"],
    "annual_results":             ["symbol", "period_end", "sales", "net_profit"],
    "income_statement":           ["symbol", "period_end", "period_type"],
    "balance_sheet":              ["symbol", "period_end", "period_type"],
    "cash_flow":                  ["symbol", "period_end", "period_type"],
    "growth_metrics":             ["symbol", "as_of_date"],
    "ownership_history":          ["symbol", "period_end", "promoter_pct"],
    "fundamentals":               ["symbol", "as_of_date"],
    "quarterly_cashflow_derived": ["symbol", "quarter_end"],
}

# These must match EXACT column names in the DB schema
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
        "total_revenue", "gross_profit", "ebitda",
        "operating_income", "net_income", "depreciation_amortization",
        "interest_expense", "diluted_eps",
        "scr_sales", "scr_net_profit", "scr_depreciation",
    ],
    "balance_sheet": [
        "scr_equity_capital", "scr_reserves", "scr_borrowings",
        "scr_fixed_assets", "scr_cwip", "scr_investments",
        "scr_other_assets", "scr_total_assets",
        "total_assets", "total_equity", "total_debt",
    ],
    "cash_flow": [
        "best_operating_cf", "best_investing_cf",
        "best_financing_cf", "best_free_cash_flow",
        "scr_cash_from_operating", "scr_cash_from_investing",
        "scr_free_cash_flow",
    ],
    "growth_metrics": [
        "revenue_cagr_3y", "net_profit_cagr_3y", "fcf_cagr_3y",
        "scr_sales_cagr_3y", "scr_profit_cagr_3y", "scr_roe_last",
    ],
    "fundamentals": [
        "roe_pct", "roce_pct", "pe_ratio", "pb_ratio",
        "revenue", "net_income", "market_cap",
        "opm_pct", "dividend_payout_pct",
        "ev", "ev_ebitda", "free_cash_flow", "debt_to_equity",
    ],
    "quarterly_cashflow_derived": [
        "revenue", "net_income", "dna",
        "approx_op_cf", "approx_fcf",
    ],
    "ownership_history": [
        "promoter_pct", "fii_pct", "dii_pct", "public_pct",
        "total_institutional_pct",
    ],
}

# Symbol column name per table (for WHERE clause in audit)
_SYMBOL_COL = {
    "quarterly_results":          "symbol",
    "annual_results":             "symbol",
    "income_statement":           "symbol",
    "balance_sheet":              "symbol",
    "cash_flow":                  "symbol",
    "growth_metrics":             "symbol",
    "ownership_history":          "symbol",
    "fundamentals":               "symbol",
    "quarterly_cashflow_derived": "symbol",
    "ownership":                  "symbol",
    "earnings_history":           "symbol",
    "technical_indicators":       "symbol",
    "corporate_actions":          "symbol",
}


def _is_null(v) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    return False


def compute_completeness(row: dict, table: str):
    fields  = COMPLETENESS_FIELDS.get(table, [])
    if not fields:
        return 100.0, []
    missing = [f for f in fields if _is_null(row.get(f))]
    pct     = round((1 - len(missing) / len(fields)) * 100, 1)
    return pct, missing


def validate_before_insert(row: dict, table: str):
    required = REQUIRED_FIELDS.get(table, [])
    for field in required:
        if _is_null(row.get(field)):
            return False, f"required field '{field}' is NULL"
    return True, "ok"


def log_data_quality(symbol, table_name, rows_inserted,
                     rows_null_heavy, avg_completeness,
                     critical_nulls, source, notes=""):
    try:
        conn = get_connection()
        conn.execute("""
            INSERT INTO data_quality_log (
                symbol, table_name, rows_inserted, rows_null_heavy,
                avg_completeness, critical_nulls_json, source, notes
            ) VALUES (?,?,?,?,?,?,?,?)
        """, (symbol, table_name, rows_inserted, rows_null_heavy,
              round(avg_completeness, 1),
              json.dumps(critical_nulls), source, notes))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"  warn  data_quality_log: {e}")


def audit_table(symbol: str, table: str) -> dict:
    """Count rows and NULL rates for key fields. Print summary."""
    conn       = get_connection()
    sym_col    = _SYMBOL_COL.get(table)
    fields     = COMPLETENESS_FIELDS.get(table, [])

    try:
        if sym_col:
            total = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {sym_col}=?", (symbol,)
            ).fetchone()[0]
        else:
            total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

        null_counts = {}
        for f in fields:
            try:
                if sym_col:
                    nc = conn.execute(
                        f"SELECT COUNT(*) FROM {table} "
                        f"WHERE {sym_col}=? AND {f} IS NULL", (symbol,)
                    ).fetchone()[0]
                else:
                    nc = conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE {f} IS NULL"
                    ).fetchone()[0]
                if nc > 0:
                    null_counts[f] = nc
            except Exception:
                pass

        avg_comp = 0.0
        if fields and total > 0:
            filled = sum(total - nc for nc in null_counts.values())
            avg_comp = round(filled / (len(fields) * total) * 100, 1)

        # Only show NULL fields that have SOME rows populated (not all null)
        partial_nulls = {k: v for k, v in null_counts.items() if v < total}
        all_null      = {k: v for k, v in null_counts.items() if v == total}

        status = f"{total} rows | {avg_comp}% complete"
        if partial_nulls:
            status += f" | partial NULLs: {partial_nulls}"
        if all_null:
            status += f" | all-NULL cols: {list(all_null.keys())}"

        print(f"  audit [{table}] {status}")
        conn.close()
        return {"table": table, "total": total,
                "avg_comp": avg_comp, "nulls": null_counts}
    except Exception as e:
        conn.close()
        print(f"  warn  audit failed {table}: {e}")
        return {}