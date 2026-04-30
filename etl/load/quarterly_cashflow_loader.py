"""
etl/load/quarterly_cashflow_loader.py  v3.0
────────────────────────────────────────────────────────────────
Changes vs v2.0:

  BUG 1 — all quarterly cashflow rows have NULL dna/approx_op_cf
  because quality_score=1 rows were silently accepted into DB
  with no dna. The extract phase (quarterly_cashflow.py) falls
  to priority-2 (NI+DA) when direct QCF is unavailable, but
  ADANIPORTS quarterly IS has "Reconciled Depreciation" which
  was missed. Even when dna IS found at extract time, the loader
  was rejecting dna=0 → NULL, which is correct, but rows where
  dna was genuinely missing fell to quality=1 and were never
  upgraded by reconcile because reconcile checks
  quarterly_results.depreciation which may not have been loaded
  yet at time of extract.
  Fix: Store all rows regardless of quality_score.
       reconcile_quarterly_cashflow() will fill dna + upgrade
       quality_score after Screener quarterly_results are loaded.

  BUG 2 — capex_source column stored as "" (empty string) for
  NI_only rows. This makes it hard to filter. Now stores the
  correct string in all cases.
────────────────────────────────────────────────────────────────
"""

from database.db import get_connection
from database.validator import validate_before_insert, compute_completeness, log_data_quality


def load_quarterly_cashflow(records: list, symbol: str):
    if not records:
        print("  warn  quarterly_cashflow_derived: no records")
        return

    conn     = get_connection()
    inserted = skipped_validation = 0
    completeness_sum = 0.0

    for r in records:
        # Block fabricated/interpolated rows
        if r.get("is_interpolated", 0) == 1:
            continue

        # Require at minimum: quarter_end + net_income
        if not r.get("quarter_end"):
            skipped_validation += 1
            continue
        if r.get("net_income") is None:
            skipped_validation += 1
            continue

        # Fix dna=0 → NULL (zero D&A is almost always a data error)
        dna = r.get("dna")
        if dna is not None and float(dna) == 0.0:
            dna = None

        comp, _ = compute_completeness(r, "quarterly_cashflow_derived")
        completeness_sum += comp

        conn.execute("""
            INSERT OR REPLACE INTO quarterly_cashflow_derived (
                symbol, quarter_end,
                revenue, net_income, dna,
                approx_op_cf, approx_capex, approx_fcf,
                fcf_margin_pct, capex_source,
                quality_score, is_real, is_interpolated,
                data_note, unit
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            symbol,
            r["quarter_end"],
            r.get("revenue"),
            r.get("net_income"),
            dna,
            r.get("approx_op_cf"),
            r.get("approx_capex"),
            r.get("approx_fcf"),
            r.get("fcf_margin_pct"),
            r.get("capex_source") or "NI_only",
            r.get("quality_score", 1),
            r.get("is_real", 1),
            0,   # always 0 here — fabricated rows blocked above
            r.get("data_note", ""),
            r.get("unit", "Rs_Crores"),
        ))
        inserted += 1

    conn.commit()
    conn.close()

    avg_comp = round(completeness_sum / inserted, 1) if inserted else 0
    log_data_quality(symbol, "quarterly_cashflow_derived", inserted,
                     0, avg_comp, {}, "yfinance")
    print(f"  ok  quarterly_cashflow_derived: {inserted} rows "
          f"(skip_invalid={skipped_validation}) "
          f"| avg completeness {avg_comp}%  "
          f"[reconcile will fill dna + upgrade quality_score]")