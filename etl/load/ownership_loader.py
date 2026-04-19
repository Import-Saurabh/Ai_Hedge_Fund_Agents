"""
etl/load/ownership_loader.py  v2.0
────────────────────────────────────────────────────────────────
Changes vs v1:
  • Writes num_shareholders from Screener shareholding
  • Also loads full quarterly history into ownership_history table
────────────────────────────────────────────────────────────────
"""

import re
from datetime import date
from database.db import get_connection

MONTH_MAP = {"jan":"01","feb":"02","mar":"03","apr":"04","may":"05","jun":"06",
             "jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12"}
MONTH_END = {"01":"31","02":"28","03":"31","04":"30","05":"31","06":"30",
             "07":"31","08":"31","09":"30","10":"31","11":"30","12":"31"}


def _parse_period(label: str):
    m = re.match(r"([A-Za-z]{3})\s+(\d{4})", str(label).strip())
    if not m:
        return None
    mon = MONTH_MAP.get(m.group(1).lower())
    if not mon:
        return None
    return f"{m.group(2)}-{mon}-{MONTH_END[mon]}"


def load_ownership(data: dict, symbol: str):
    """Upsert today's ownership snapshot."""
    conn  = get_connection()
    today = date.today().isoformat()

    conn.execute("""
        INSERT OR REPLACE INTO ownership (
            symbol, snapshot_date,
            promoter_pct, fii_fpi_pct, dii_pct, public_retail_pct,
            num_shareholders,
            insiders_pct, institutions_pct,
            institutions_float_pct, institutions_count,
            total_institutional_pct,
            fii_net_buy_cr, dii_net_buy_cr, fii_dii_flow_date,
            source
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        symbol, today,
        data.get("promoter_pct"),
        data.get("fii_fpi_pct"),
        data.get("dii_pct"),
        data.get("public_retail_pct"),
        data.get("num_shareholders"),
        data.get("insiders_pct"),
        data.get("institutions_pct"),
        data.get("institutions_float_pct"),
        data.get("institutions_count"),
        data.get("total_institutional_pct"),
        data.get("fii_net_buy_cr"),
        data.get("dii_net_buy_cr"),
        data.get("fii_dii_flow_date"),
        data.get("source", "unknown"),
    ))
    conn.commit()
    conn.close()
    print(f"  ✅ ownership: snapshot saved for {symbol}")


def load_ownership_history(df, symbol: str):
    """
    Load full quarterly shareholding history from Screener DataFrame
    into ownership_history table (one row per quarter).
    """
    if df is None or df.empty:
        print("  ⚠  ownership_history: no data")
        return

    def row(metric):
        for idx in df.index:
            if metric.lower() in str(idx).lower():
                return df.loc[idx]
        return None

    pro_r = row("Promoter")
    fii_r = row("FII")
    dii_r = row("DII")
    pub_r = row("Public")
    sha_r = row("No. of Shareholders")

    def v(series, col):
        if series is None:
            return None
        raw = series.get(col)
        if raw is None:
            return None
        s = str(raw).replace("%", "").replace(",", "").strip()
        if s in ("", "-", "—", "nan", "None"):
            return None
        try:
            return round(float(s), 4)
        except ValueError:
            return None

    conn  = get_connection()
    count = 0

    for col in df.columns:
        period_end = _parse_period(str(col))
        if not period_end:
            continue

        fii = v(fii_r, col)
        dii = v(dii_r, col)
        inst = round(fii + dii, 4) if fii is not None and dii is not None else None

        sha_raw = v(sha_r, col)
        num_sha = int(sha_raw) if sha_raw is not None else None

        conn.execute("""
            INSERT OR REPLACE INTO ownership_history (
                symbol, period_end,
                promoter_pct, fii_pct, dii_pct, public_pct,
                total_institutional_pct, num_shareholders, source
            ) VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            symbol, period_end,
            v(pro_r, col), fii, dii,
            v(pub_r, col), inst, num_sha,
            "Screener.in",
        ))
        count += 1

    conn.commit()
    conn.close()
    print(f"  ✅ ownership_history: {count} quarterly rows upserted")