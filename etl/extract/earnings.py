import yfinance as yf
import pandas as pd
from datetime import date


def fetch_earnings(symbol: str) -> dict:
    """Fetch earnings history, estimates, EPS trend, revisions, calendar."""
    t   = yf.Ticker(symbol)
    out = {}

    # Earnings history (actual vs estimate)
    try:
        eh = t.earnings_history
        if eh is not None and not eh.empty:
            records = []
            for idx, row in eh.iterrows():
                records.append({
                    "quarter_end":    str(idx)[:10],
                    "eps_actual":     row.get("epsActual"),
                    "eps_estimate":   row.get("epsEstimate"),
                    "eps_difference": row.get("epsDifference"),
                    "surprise_pct":   row.get("surprisePercent"),
                })
            out["earnings_history"] = records
    except:
        pass

    # EPS estimates
    try:
        ee = t.earnings_estimate
        if ee is not None and not ee.empty:
            today = date.today().isoformat()
            records = []
            for idx, row in ee.iterrows():
                records.append({
                    "snapshot_date":  today,
                    "period_code":    str(idx),
                    "avg_eps":        row.get("avg"),
                    "low_eps":        row.get("low"),
                    "high_eps":       row.get("high"),
                    "year_ago_eps":   row.get("yearAgoEps"),
                    "analyst_count":  row.get("numberOfAnalysts"),
                    "growth_pct":     row.get("growth"),
                })
            out["earnings_estimates"] = records
    except:
        pass

    # EPS trend
    try:
        et = t.eps_trend
        if et is not None and not et.empty:
            today = date.today().isoformat()
            records = []
            for idx, row in et.iterrows():
                records.append({
                    "snapshot_date":   today,
                    "period_code":     str(idx),
                    "current_est":     row.get("current"),
                    "seven_days_ago":  row.get("7daysAgo"),
                    "thirty_days_ago": row.get("30daysAgo"),
                    "sixty_days_ago":  row.get("60daysAgo"),
                    "ninety_days_ago": row.get("90daysAgo"),
                })
            out["eps_trend"] = records
    except:
        pass

    # EPS revisions
    try:
        er = t.eps_revisions
        if er is not None and not er.empty:
            today = date.today().isoformat()
            records = []
            for idx, row in er.iterrows():
                records.append({
                    "snapshot_date": today,
                    "period_code":   str(idx),
                    "up_last_7d":    row.get("upLast7days"),
                    "up_last_30d":   row.get("upLast30days"),
                    "down_last_30d": row.get("downLast30days"),
                    "down_last_7d":  row.get("downLast7Days"),
                })
            out["eps_revisions"] = records
    except:
        pass

    return out
