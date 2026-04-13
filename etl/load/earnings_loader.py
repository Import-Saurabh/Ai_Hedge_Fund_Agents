from database.db import get_connection


def load_earnings_history(records: list, symbol: str):
    conn = get_connection()
    count = 0
    for r in records:
        conn.execute("""
            INSERT OR REPLACE INTO earnings_history
                (symbol, quarter_end, eps_actual, eps_estimate,
                 eps_difference, surprise_pct)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            symbol, r["quarter_end"],
            r.get("eps_actual"), r.get("eps_estimate"),
            r.get("eps_difference"), r.get("surprise_pct"),
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ earnings_history: {count} rows upserted")


def load_earnings_estimates(records: list, symbol: str):
    conn = get_connection()
    count = 0
    for r in records:
        conn.execute("""
            INSERT OR REPLACE INTO earnings_estimates
                (symbol, snapshot_date, period_code,
                 avg_eps, low_eps, high_eps, year_ago_eps,
                 analyst_count, growth_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol, r["snapshot_date"], r["period_code"],
            r.get("avg_eps"), r.get("low_eps"), r.get("high_eps"),
            r.get("year_ago_eps"), r.get("analyst_count"), r.get("growth_pct"),
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ earnings_estimates: {count} rows upserted")


def load_eps_trend(records: list, symbol: str):
    conn = get_connection()
    count = 0
    for r in records:
        conn.execute("""
            INSERT OR REPLACE INTO eps_trend
                (symbol, snapshot_date, period_code,
                 current_est, seven_days_ago, thirty_days_ago,
                 sixty_days_ago, ninety_days_ago)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol, r["snapshot_date"], r["period_code"],
            r.get("current_est"), r.get("seven_days_ago"),
            r.get("thirty_days_ago"), r.get("sixty_days_ago"),
            r.get("ninety_days_ago"),
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ eps_trend: {count} rows upserted")


def load_eps_revisions(records: list, symbol: str):
    conn = get_connection()
    count = 0
    for r in records:
        conn.execute("""
            INSERT OR REPLACE INTO eps_revisions
                (symbol, snapshot_date, period_code,
                 up_last_7d, up_last_30d, down_last_30d, down_last_7d)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            symbol, r["snapshot_date"], r["period_code"],
            r.get("up_last_7d"), r.get("up_last_30d"),
            r.get("down_last_30d"), r.get("down_last_7d"),
        ))
        count += 1
    conn.commit()
    conn.close()
    print(f"  ✅ eps_revisions: {count} rows upserted")
