"""
etl/extract/ownership.py  v3.0
────────────────────────────────────────────────────────────────
Fixes vs v2:
  • Added nselib capital_market.fii_dii_trading_data() for
    real FII/DII buy-sell flow data (daily activity)
    Ref: https://unofficed.com/nse-python/documentation/special/#the-fii-dii-api
  • insiders_pct / institutions_pct now populated from yfinance
    major_holders with correct decimal handling
  • NSE shareholding pattern API fixed (cookie-based session)
  • All sources merged with priority: NSE API > Screener > yfinance
────────────────────────────────────────────────────────────────
"""

import re
import time
import requests
import yfinance as yf
from datetime import date, timedelta
from typing import Optional, Dict

HDR = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}


# ── NSE session with cookie priming ──────────────────────────
def _nse_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HDR)
    try:
        session.get("https://www.nseindia.com", timeout=10)
        time.sleep(0.6)
        session.get("https://www.nseindia.com/get-quotes/equity", timeout=8)
        time.sleep(0.4)
    except Exception:
        pass
    return session


# ── Parse NSE shareholding API response ──────────────────────
def _parse_shareholding_response(data) -> Dict:
    result = {}
    if isinstance(data, list) and data:
        item = data[0] if isinstance(data[0], dict) else {}
    elif isinstance(data, dict):
        item = data
    else:
        return result

    if "shareholdingPatterns" in item:
        p = item["shareholdingPatterns"]
        if isinstance(p, list) and p:
            item = p[0]
    if "data" in item and isinstance(item["data"], (dict, list)):
        inner = item["data"]
        item = inner[0] if isinstance(inner, list) and inner else inner

    KEY_MAP = {
        "promoter_pct":      ["promoterAndPromoterGroupShareHolding", "promoter",
                               "promoterHolding", "totalPromoter"],
        "fii_fpi_pct":       ["foreignPortfolioInvestorsCorporate", "fii", "FII",
                               "foreignPortfolioInvestors", "totalFII", "fpiCorporate"],
        "dii_pct":           ["dii", "DII", "domesticInstitutionalInvestors", "totalDII"],
        "public_retail_pct": ["publicShareholding", "public", "retail", "totalPublic"],
    }
    for out_key, ckeys in KEY_MAP.items():
        for ck in ckeys:
            val = item.get(ck)
            if val is not None:
                try:
                    result[out_key] = round(float(str(val).replace("%", "").strip()), 2)
                    break
                except Exception:
                    pass
    return result


# ── nselib FII/DII trading flow data ─────────────────────────
def _fetch_fii_dii_flow() -> Optional[Dict]:
    """
    Fetch FII/DII net buy/sell activity using nselib.
    Falls back to NSE direct API if nselib not installed.

    Returns dict with keys:
        fii_net_buy_cr, dii_net_buy_cr, date (latest available)
    """
    # Try nselib first
    try:
        from nselib import capital_market
        today_str = date.today().strftime("%d-%m-%Y")
        week_ago  = (date.today() - timedelta(days=7)).strftime("%d-%m-%Y")
        df = capital_market.fii_dii_trading_data(week_ago, today_str)
        if df is not None and not df.empty:
            latest = df.iloc[0]   # most recent row
            result = {"fii_dii_date": str(latest.get("Date", ""))[:10]}
            # FII columns
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
        session = _nse_session()
        r = session.get(
            "https://www.nseindia.com/api/fiidiiTradeReact",
            timeout=12
        )
        if r.status_code == 200 and r.text.strip():
            data = r.json()
            if isinstance(data, list) and data:
                latest = data[0]
                result = {"fii_dii_date": str(latest.get("date", ""))[:10]}
                try:
                    result["fii_net_buy_cr"] = round(
                        float(str(latest.get("fiiNet", 0)).replace(",", "")), 2
                    )
                except Exception:
                    pass
                try:
                    result["dii_net_buy_cr"] = round(
                        float(str(latest.get("diiNet", 0)).replace(",", "")), 2
                    )
                except Exception:
                    pass
                return result
    except Exception:
        pass

    return None


# ── Screener.in scrape fallback ───────────────────────────────
def _fetch_screener(sym_nse: str) -> Optional[Dict]:
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


# ── yfinance institutional holders ───────────────────────────
def _fetch_yf_holders(symbol_yf: str) -> Dict:
    out = {}
    try:
        t  = yf.Ticker(symbol_yf)
        mh = t.major_holders
        if mh is not None and not mh.empty:
            for idx in mh.index:
                # major_holders col 0 = value, col 1 = description
                label = str(mh.iloc[idx, 1]).lower() if mh.shape[1] > 1 else ""
                val   = mh.iloc[idx, 0]
                try:
                    fval = float(val)
                except Exception:
                    continue

                # yfinance returns these as fractions (e.g. 0.0312 = 3.12%)
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


# ── Master fetch ──────────────────────────────────────────────
def fetch_ownership(symbol_yf: str, symbol_nse: str) -> dict:
    """
    Fetch shareholding pattern + FII/DII trading flow.

    Priority for promoter/FII/DII/public %:
      1. NSE corporate shareholding API
      2. Screener.in scrape
      3. yfinance major_holders

    FII/DII daily flow from nselib (or NSE fiidiiTradeReact API).
    """
    out = {"snapshot_date": date.today().isoformat()}

    # Step 1: yfinance holders (insiders / institutions)
    yf_data = _fetch_yf_holders(symbol_yf)
    out.update(yf_data)

    # Step 2: NSE shareholding API
    try:
        session = _nse_session()
        url = (f"https://www.nseindia.com/api/corporate-share-holdings-master"
               f"?index=equities&symbol={symbol_nse}")
        r = session.get(url, timeout=12)
        if r.status_code == 200 and r.text.strip():
            parsed = _parse_shareholding_response(r.json())
            if parsed:
                out.update(parsed)
                out["source"] = "NSE API"
    except Exception:
        pass

    # Step 3: Screener fallback if promoter still missing
    if not out.get("promoter_pct"):
        screener = _fetch_screener(symbol_nse)
        if screener:
            for k, v in screener.items():
                if k not in out or out[k] is None:
                    out[k] = v
            if "source" not in out:
                out["source"] = screener.get("source", "Screener.in")

    # Step 4: FII/DII trading flow (nselib / NSE API)
    flow = _fetch_fii_dii_flow()
    if flow:
        out["fii_net_buy_cr"]  = flow.get("fii_net_buy_cr")
        out["dii_net_buy_cr"]  = flow.get("dii_net_buy_cr")
        out["fii_dii_flow_date"] = flow.get("fii_dii_date")

    # Derived total institutional
    fii = out.get("fii_fpi_pct")
    dii = out.get("dii_pct")
    if fii is not None and dii is not None:
        out["total_institutional_pct"] = round(fii + dii, 2)

    return out