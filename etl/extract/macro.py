import time
import re
import requests
import yfinance as yf
from datetime import date

HDR = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

NSE_INDICES = {
    "Nifty 50":           "^NSEI",
    "Nifty Bank":         "^NSEBANK",
    "Sensex":             "^BSESN",
    "Nifty IT":           "^CNXIT",
    "Nifty FMCG":         "^CNXFMCG",
    "Nifty Auto":         "^CNXAUTO",
    "Nifty Pharma":       "^CNXPHARMA",
    "Nifty Metal":        "^CNXMETAL",
    "Nifty Realty":       "^CNXREALTY",
    "Nifty Energy":       "^CNXENERGY",
    "Nifty MidCap 50":    "^NSEMDCP50",
    "Nifty Next 50":      "^NSMIDCP",
    "USD/INR":            "USDINR=X",
    "Crude Oil WTI":      "CL=F",
    "Gold Futures":       "GC=F",
}

FOREX_COMMODITIES = {"USD/INR", "Crude Oil WTI", "Gold Futures"}


def fetch_market_indices() -> dict:
    """Fetch all NSE index prices + forex/commodities."""
    indices = {}
    forex   = {}
    today   = date.today().isoformat()

    for name, sym in NSE_INDICES.items():
        try:
            hist = yf.Ticker(sym).history(period="2d", auto_adjust=True)
            if hist is not None and not hist.empty:
                price = round(float(hist["Close"].iloc[-1]), 2)
                chg   = None
                if len(hist) >= 2:
                    chg = round(float(hist["Close"].pct_change().iloc[-1]) * 100, 2)
                direction = ("^" if chg and chg >= 0 else "v") if chg is not None else None
                entry = {"date": today, "name": name, "price": price,
                         "change_pct": chg, "direction": direction}
                if name in FOREX_COMMODITIES:
                    forex[name] = entry
                else:
                    indices[name] = entry
        except Exception as e:
            pass
        time.sleep(0.25)

    return {"indices": indices, "forex": forex}


def fetch_rbi_rates() -> dict:
    """Fetch RBI rates with cached fallback."""
    today = date.today().isoformat()

    # Cached Apr 2025 rates (live sources may be unreachable)
    cached = {
        "date":         today,
        "repo_rate":    6.25,
        "reverse_repo": 3.35,
        "sdf_rate":     6.00,
        "msf_rate":     6.50,
        "bank_rate":    6.50,
        "crr":          4.00,
        "slr":          18.00,
        "is_cached":    1,
        "source":       "Cached — verify at rbi.org.in/rates",
    }

    try:
        r = requests.get(
            "https://www.rbi.org.in/scripts/bs_viewcontent.aspx?Id=4006",
            headers=HDR, timeout=10)
        if r.status_code == 200:
            html = r.text
            patterns = {
                "repo_rate":    r"Policy\s+Repo\s+Rate[^\d]*([\d.]+)",
                "reverse_repo": r"Reverse\s+Repo[^\d]*([\d.]+)",
                "sdf_rate":     r"Standing\s+Deposit\s+Facility[^\d]*([\d.]+)",
                "msf_rate":     r"Marginal\s+Standing\s+Facility[^\d]*([\d.]+)",
                "crr":          r"Cash\s+Reserve\s+Ratio[^\d]*([\d.]+)",
                "slr":          r"Statutory\s+Liquidity\s+Ratio[^\d]*([\d.]+)",
            }
            found = 0
            live = {"date": today, "is_cached": 0, "source": "RBI website"}
            for key, pat in patterns.items():
                m = re.search(pat, html, re.IGNORECASE)
                if m:
                    live[key] = float(m.group(1))
                    found += 1
            if found >= 3:
                return live
    except:
        pass

    return cached


def fetch_macro_indicators() -> list:
    """Fetch World Bank macro indicators for India."""
    today = date.today().isoformat()
    WB = {
        "India CPI Inflation (%)": "FP.CPI.TOTL.ZG",
        "India GDP Growth (%)":    "NY.GDP.MKTP.KD.ZG",
        "Current Account (USD B)": "BN.CAB.XOKA.CD",
    }
    results = []
    for label, code in WB.items():
        try:
            r = requests.get(
                f"https://api.worldbank.org/v2/country/IN/indicator/"
                f"{code}?format=json&mrv=4",
                headers=HDR, timeout=10)
            if r.status_code == 200:
                data = r.json()
                if len(data) > 1 and data[1]:
                    for entry in data[1]:
                        yr, val = entry.get("date"), entry.get("value")
                        if val:
                            results.append({
                                "snapshot_date":  today,
                                "indicator_name": label,
                                "source":         "World Bank",
                                "value":          float(val),
                                "year":           int(yr),
                            })
                            break
        except:
            pass
    return results
