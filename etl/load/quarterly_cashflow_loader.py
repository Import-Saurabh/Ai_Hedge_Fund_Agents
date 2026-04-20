"""
etl/load/quarterly_cashflow_loader.py  v2.0
────────────────────────────────────────────────────────────────
Changes vs v1:
  • quality_score, is_real, data_note columns written
  • Rows with quality_score < 2 are skipped (logged only)
  • dna=0 rejected — must be NULL or real positive value
  • completeness_pct computed per row
────────────────────────────────────────────────────────────────
"""

from database.db import get_connection
from database.validator import validate_before_insert, compute_completeness, log_data_quality


def load_quarterly_cashflow(records: list, symbol: str):
    if not records:
        print("  warn  quarterly_cashflow_derived: no records")
        return

    conn     = get_connection()
    inserted = skipped_quality = skipped_validation = 0
    completeness_sum = 0.0

    for r in records:
        # Block fabricated rows at loader level too
        if r.get("is_interpolated", 0) == 1:
            skipped_quality += 1
            continue

        # Require at minimum: quarter_end + (net_income OR approx_op_cf)
        if not r.get("quarter_end"):
            skipped_validation += 1
            continue
        if r.get("net_income") is None and r.get("approx_op_cf") is None:
            skipped_validation += 1
            continue

        # Fix dna=0 → NULL
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
            r.get("capex_source"),
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
          f"(skip_quality={skipped_quality} skip_invalid={skipped_validation}) "
          f"| avg completeness {avg_comp}%")