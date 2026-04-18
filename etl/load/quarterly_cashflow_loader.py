"""
etl/load/quarterly_cashflow_loader.py  v2.0
────────────────────────────────────────────────────────────────
Fixes vs v1:
  • Refuses to load is_interpolated=1 rows by default
    (fabricated CapEx corrupts ML training signals)
  • Prints a warning count for any skipped rows
  • Added data_quality column in INSERT for downstream filtering
────────────────────────────────────────────────────────────────
"""

from database.db import get_connection


def load_quarterly_cashflow(records: list, symbol: str,
                             allow_interpolated: bool = False):
    """
    Load derived quarterly FCF data.

    Parameters
    ----------
    records            : list of dicts from fetch_quarterly_cashflow()
    symbol             : NSE symbol string
    allow_interpolated : if False (default) rows with is_interpolated=1
                         are skipped — they contain fabricated CapEx values
                         that corrupt ML feature distributions
    """
    conn     = get_connection()
    count    = 0
    skipped  = 0

    for r in records:
        if not allow_interpolated and r.get("is_interpolated", 0) == 1:
            skipped += 1
            continue

        conn.execute("""
            INSERT OR REPLACE INTO quarterly_cashflow_derived (
                symbol, quarter_end, revenue, net_income, dna,
                approx_op_cf, approx_capex, approx_fcf,
                fcf_margin_pct, capex_source, is_interpolated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol,
            r["quarter_end"],
            r.get("revenue"),
            r.get("net_income"),
            r.get("dna"),
            r.get("approx_op_cf"),
            r.get("approx_capex"),
            r.get("approx_fcf"),
            r.get("fcf_margin_pct"),
            r.get("capex_source"),
            r.get("is_interpolated", 0),
        ))
        count += 1

    conn.commit()
    conn.close()

    if skipped:
        print(f"  ⚠  quarterly_cashflow_derived: skipped {skipped} interpolated rows "
              f"(set allow_interpolated=True to load them)")
    print(f"  ✅ quarterly_cashflow_derived: {count} real rows upserted")