"""
etl/extract/ownership.py  v4.0
────────────────────────────────────────────────────────────────
Changes vs v3:
  • Screener.in shareholding DataFrame (already scraped) is now
    the PRIMARY source for promoter/FII/DII/public percentages —
    passed in by pipeline.py from screener_data["shareholding"]
  • NSE cookie-session scraping removed (fragile, often blocked)
  • Screener.in direct scrape kept as fallback if df not passed
  • yfinance major_holders kept for insiders_pct/institutions_pct
  • FII/DII daily flow from nselib / NSE API unchanged
────────────────────────────────────────────────────────────────
"""

import re
import time
import requests
import yfinance as yf
import pandas as pd
from datetime import date, timedelta
from typing import Optional, Dict

HDR = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


# ── nselib FII/DII trading flow data ─────────────────────────
def _fetch_fii_dii_flow() -> Optional[Dict]:
    try:
        from nselib import capital_market
        today_str = date.today().strftime("%d-%m-%Y")
        week_ago  = (date.today() - timedelta(days=7)).strftime("%d-%m-%Y")
        df = capital_market.fii_dii_trading_data(week_ago, today_str)
        if df is not None and not df.empty:
            latest = df.iloc[0]
            result = {"fii_dii_date": str(latest.get("Date", ""))[:10]}
            for col in df.columns:
                cl = str(col).lower()
                if "fii" in cl and "net" in cl:
                    try:
                        result["fii_net_buy_cr"] = round(float(latest[col]) / 100, 2)
                    except Exception:
                        pass
                if "dii" in cl and "net" in cl:
                    try:
                        result["dii_net_buy_cr"] = round(float(latest[col]) / 100, 2)
                    except Exception:
                        pass
            return result
    except ImportError:
        pass
    except Exception:
        pass

    # Fallback: NSE direct FII/DII endpoint
    try:
        r = requests.get(
            "https://www.nseindia.com/api/fiidiiTradeReact",
            headers=HDR, timeout=12
        )
        if r.status_code == 200 and r.text.strip():
            data = r.json()
            if isinstance(data, list) and data:
                latest = data[0]
                result = {"fii_dii_date": str(latest.get("date", ""))[:10]}
                try:
                    result["fii_net_buy_cr"] = round(
                        float(str(latest.get("fiiNet", 0)).replace(",", "")), 2)
                except Exception:
                    pass
                try:
                    result["dii_net_buy_cr"] = round(
                        float(str(latest.get("diiNet", 0)).replace(",", "")), 2)
                except Exception:
                    pass
                return result
    except Exception:
        pass

    return None


# ── yfinance institutional holders ───────────────────────────
def _fetch_yf_holders(symbol_yf: str) -> Dict:
    out = {}
    try:
        t  = yf.Ticker(symbol_yf)
        mh = t.major_holders
        if mh is not None and not mh.empty:
            for idx in mh.index:
                label = str(mh.iloc[idx, 1]).lower() if mh.shape[1] > 1 else ""
                val   = mh.iloc[idx, 0]
                try:
                    fval = float(val)
                except Exception:
                    continue
                if "insider" in label:
                    out["insiders_pct"] = round(fval * 100, 4)
                elif "institution" in label and "float" not in label:
                    out["institutions_pct"] = round(fval * 100, 4)
                elif "float" in label and "institution" in label:
                    out["institutions_float_pct"] = round(fval * 100, 4)
                elif "count" in label or "number" in label:
                    out["institutions_count"] = int(fval)
    except Exception:
        pass
    return out


# ── Parse shareholding from Screener DataFrame ───────────────
def _from_screener_df(df: pd.DataFrame) -> Dict:
    """Extract latest-quarter shareholding from a Screener DataFrame."""
    if df is None or df.empty:
        return {}
    # Use last column (most recent quarter)
    col = df.columns[-1]

    def pct(pattern: str):
        for idx in df.index:
            if pattern.lower() in str(idx).lower():
                v = df.loc[idx, col]
                s = str(v).replace("%", "").strip()
                try:
                    return round(float(s), 4)
                except Exception:
                    pass
        return None

    result = {
        "promoter_pct":      pct("Promoter"),
        "fii_fpi_pct":       pct("FII"),
        "dii_pct":           pct("DII"),
        "public_retail_pct": pct("Public"),
        "source":            "Screener.in",
    }
    return {k: v for k, v in result.items() if v is not None}


# ── Screener fallback scrape ──────────────────────────────────
def _fetch_screener_fallback(sym_nse: str) -> Optional[Dict]:
    for url in [
        f"https://www.screener.in/company/{sym_nse}/consolidated/",
        f"https://www.screener.in/company/{sym_nse}/",
    ]:
        try:
            r = requests.get(url, headers=HDR, timeout=15)
            if r.status_code != 200:
                continue
            html = r.text
            patterns = {
                "promoter_pct":      r"Promoters\s*[^%\d]*?([\d.]+)\s*%",
                "fii_fpi_pct":       r"FII[^%\d]*?([\d.]+)\s*%",
                "dii_pct":           r"DII[^%\d]*?([\d.]+)\s*%",
                "public_retail_pct": r"Public[^%\d]*?([\d.]+)\s*%",
            }
            result = {}
            for key, pat in patterns.items():
                m = re.search(pat, html, re.IGNORECASE)
                if m:
                    try:
                        result[key] = float(m.group(1))
                    except Exception:
                        pass
            if len(result) >= 2:
                result["source"] = "Screener.in"
                return result
        except Exception:
            pass
    return None


# ── Master fetch ──────────────────────────────────────────────
def fetch_ownership(
    symbol_yf: str,
    symbol_nse: str,
    screener_shareholding_df: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Fetch shareholding pattern + FII/DII trading flow.

    Priority for promoter/FII/DII/public %:
      1. Screener DataFrame passed from pipeline (already scraped)
      2. Screener.in direct scrape fallback
      3. yfinance major_holders (last resort)

    FII/DII daily flow from nselib or NSE API.
    """
    out = {"snapshot_date": date.today().isoformat()}

    # Step 1: yfinance holders (insiders / institutions)
    yf_data = _fetch_yf_holders(symbol_yf)
    out.update(yf_data)

    # Step 2: Screener DataFrame (primary source — already fetched)
    if screener_shareholding_df is not None:
        sh = _from_screener_df(screener_shareholding_df)
        if sh:
            out.update(sh)

    # Step 3: Screener fallback scrape if promoter still missing
    if not out.get("promoter_pct"):
        screener = _fetch_screener_fallback(symbol_nse)
        if screener:
            for k, v in screener.items():
                if k not in out or out[k] is None:
                    out[k] = v
            if "source" not in out:
                out["source"] = screener.get("source", "Screener.in")

    # Step 4: FII/DII trading flow
    flow = _fetch_fii_dii_flow()
    if flow:
        out["fii_net_buy_cr"]    = flow.get("fii_net_buy_cr")
        out["dii_net_buy_cr"]    = flow.get("dii_net_buy_cr")
        out["fii_dii_flow_date"] = flow.get("fii_dii_date")

    # Derived total institutional
    fii = out.get("fii_fpi_pct")
    dii = out.get("dii_pct")
    if fii is not None and dii is not None:
        out["total_institutional_pct"] = round(fii + dii, 2)

    return out