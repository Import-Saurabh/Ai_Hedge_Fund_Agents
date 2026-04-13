import re
import time
import requests
import yfinance as yf
from datetime import date
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


def _nse_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HDR)
    try:
        session.get("https://www.nseindia.com", timeout=10)
        time.sleep(0.5)
        session.get("https://www.nseindia.com/get-quotes/equity", timeout=8)
        time.sleep(0.3)
    except:
        pass
    return session


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
                               "promoterHolding"],
        "fii_fpi_pct":       ["foreignPortfolioInvestorsCorporate", "fii", "FII",
                               "foreignPortfolioInvestors"],
        "dii_pct":           ["dii", "DII", "domesticInstitutionalInvestors"],
        "public_retail_pct": ["publicShareholding", "public", "retail"],
    }
    for out_key, ckeys in KEY_MAP.items():
        for ck in ckeys:
            val = item.get(ck)
            if val is not None:
                try:
                    result[out_key] = round(float(str(val).replace("%", "").strip()), 2)
                    break
                except:
                    pass
    return result


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
                    except:
                        pass
            if len(result) >= 2:
                result["source"] = "Screener.in"
                return result
        except:
            pass
    return None


def fetch_ownership(symbol_yf: str, symbol_nse: str) -> dict:
    """Fetch promoter/FII/DII shareholding + yfinance institutional data."""
    out = {"snapshot_date": date.today().isoformat()}

    # yfinance major holders
    try:
        t = yf.Ticker(symbol_yf)
        mh = t.major_holders
        if mh is not None and not mh.empty:
            for idx in mh.index:
                label = str(mh.iloc[idx, 1]).lower() if mh.shape[1] > 1 else ""
                val   = mh.iloc[idx, 0]
                if "insider" in label:
                    out["insiders_pct"] = round(float(val) * 100, 4)
                elif "institution" in label and "float" not in label:
                    out["institutions_pct"] = round(float(val) * 100, 4)
                elif "float" in label:
                    out["institutions_float_pct"] = round(float(val) * 100, 4)
                elif "count" in label or "number" in label:
                    out["institutions_count"] = int(val)
    except:
        pass

    # NSE API
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
    except:
        pass

    # Screener.in fallback
    if "promoter_pct" not in out or out.get("promoter_pct") is None:
        screener = _fetch_screener(symbol_nse)
        if screener:
            out.update(screener)

    # Derived total institutional
    fii = out.get("fii_fpi_pct")
    dii = out.get("dii_pct")
    if fii is not None and dii is not None:
        out["total_institutional_pct"] = round(fii + dii, 2)

    return out
