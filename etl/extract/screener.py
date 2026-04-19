"""
etl/extract/screener.py  v2.0
────────────────────────────────────────────────────────────────
Scrapes Screener.in for all financial tables.
Returns DataFrames keyed by section name.
All monetary values in Rs. Crores (Screener.in native).
────────────────────────────────────────────────────────────────
"""

import re
import time
from io import StringIO
from typing import Optional, Dict

try:
    import httpx
    _USE_HTTPX = True
except ImportError:
    import requests
    _USE_HTTPX = False

import pandas as pd
from bs4 import BeautifulSoup

HDR = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

_SECTIONS = {
    "quarters":      "quarters",
    "profit_loss":   "profit-loss",
    "growth":        "growth-numbers",
    "balance_sheet": "balance-sheet",
    "cash_flow":     "cash-flow",
    "ratios":        "ratios",
    "shareholding":  "shareholding",
}


def _get_html(symbol_nse: str, consolidated: bool = True) -> Optional[str]:
    suffix = "consolidated" if consolidated else ""
    url = f"https://www.screener.in/company/{symbol_nse}/{suffix}/"
    for attempt in range(3):
        try:
            if _USE_HTTPX:
                r = httpx.get(url, headers=HDR, follow_redirects=True, timeout=20)
            else:
                r = requests.get(url, headers=HDR, timeout=20)
            if r.status_code == 200:
                return r.text
            if r.status_code == 404 and consolidated:
                return _get_html(symbol_nse, consolidated=False)
        except Exception as e:
            print(f"  warning  screener fetch attempt {attempt + 1}: {e}")
            time.sleep(2 * (attempt + 1))
    return None


def _parse_table(section_tag) -> Optional[pd.DataFrame]:
    if section_tag is None:
        return None
    table = section_tag.find("table")
    if table is None:
        return None
    try:
        df = pd.read_html(StringIO(str(table)))[0]
        df.iloc[:, 0] = (
            df.iloc[:, 0]
            .astype(str)
            .str.replace(r"\s*\+\s*", "", regex=True)
            .str.strip()
        )
        df = df.rename(columns={df.columns[0]: "metric"})
        df = df.set_index("metric")
        return df
    except Exception as e:
        print(f"  warning  screener table parse error: {e}")
        return None


def fetch_screener_data(symbol_nse: str) -> Dict[str, Optional[pd.DataFrame]]:
    """
    Fetch all Screener.in tables for a given NSE symbol.
    Returns dict: {section_key: DataFrame (metric x period)}.
    Monetary values are Rs. Crores (Screener native).
    """
    html = _get_html(symbol_nse)
    if not html:
        print(f"  error screener: could not fetch HTML for {symbol_nse}")
        return {}

    soup   = BeautifulSoup(html, "lxml")
    result = {}

    for key, section_id in _SECTIONS.items():
        section = soup.find("section", id=section_id)
        df      = _parse_table(section)
        result[key] = df
        status  = f"{df.shape[0]}r x {df.shape[1]}c" if df is not None else "not found"
        ok = "ok" if df is not None else "warn"
        print(f"  {ok} screener[{key}]: {status}")

    return result