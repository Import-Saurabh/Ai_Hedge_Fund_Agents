from database.db import get_connection


def load_quarterly_cashflow(records: list, symbol: str):
    """Load derived quarterly FCF data."""
    conn = get_connection()
    count = 0
    for r in records:
        conn.execute("""
            INSERT OR REPLACE INTO quarterly_cashflow_derived (
                symbol, quarter_end, revenue, net_income, dna,
                approx_op_cf, approx_capex, approx_fcf,
                fcf_margin_pct, capex_source, is_interpolated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol, r["quarter_end"],
            r.get("revenue"), r.get("net_income"), r.get("dna"),
            r.get("approx_op_cf"), r.get("approx_capex"), r.get("approx_fcf"),
            r.get("fcf_margin_pct"), r.get("capex_source"),
            r.get("is_interpolated", 0),
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ quarterly_cashflow_derived: {count} rows upserted")
