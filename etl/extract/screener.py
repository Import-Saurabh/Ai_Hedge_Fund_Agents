"""
etl/extract/screener.py  v3.1
────────────────────────────────────────────────────────────────
Changes vs v3.0:
  BUG 1 — low_52w NULL:
    "High / Low" li value from Screener is "₹ 1,612 / ₹ 1,181"
    Previous code split on "/" but left "₹ 1,181 " which
    _clean_num couldn't parse after stripping only the ₹ at
    the very beginning.
    → Fixed: strip ₹ from EACH part after split

  BUG 2 — book_value NULL:
    Screener label is "Book Value" but _parse_overview checked
    "book value" with exact match failing on "Book Value₹"
    → Fixed: relaxed substring match + ₹-aware strip

  BUG 3 — market_cap sometimes "₹ 3,67,472 Cr" format:
    _clean_num now handles Indian comma formatting (1,23,456)
────────────────────────────────────────────────────────────────
"""

import re
import time
from io import StringIO
from typing import Optional, Dict, Any

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


def _clean_num(text: str) -> Optional[float]:
    """
    Robust number extractor for Screener values.
    Handles:
      "₹ 1,612"  →  1612.0
      "₹1,181"   →  1181.0
      "3,67,472 Cr."  →  367472.0  (Indian lakh comma format)
      "14.0%"    →  14.0
      "1,612 / 1,181" → returns first value (low)
    """
    if not text:
        return None
    s = str(text).strip()

    # Remove currency symbols and units
    s = s.replace("₹", "").replace("Cr.", "").replace("Cr", "").replace("%", "").strip()

    # If range like "1,181 / 1,612" — caller should split before calling
    if "/" in s:
        parts = s.split("/")
        s = parts[0].strip()

    # Remove all commas (handles both 1,234 and 1,23,456 Indian format)
    s = s.replace(",", "").strip()

    if s in ("", "-", "—", "N/A", "nan", "None"):
        return None
    try:
        return round(float(s), 4)
    except ValueError:
        return None


def _clean_num_part(text: str) -> Optional[float]:
    """Like _clean_num but also strips ₹ from a single fragment."""
    if not text:
        return None
    s = str(text).replace("₹", "").replace(",", "").replace("Cr.", "").replace("Cr", "").strip()
    if s in ("", "-", "—", "N/A", "nan", "None"):
        return None
    try:
        return round(float(s), 4)
    except ValueError:
        return None


def _parse_overview(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Parse the company overview/ratios panel at the top of the Screener page.

    Extracts:
      current_price, high_52w, low_52w, market_cap_cr,
      pe_ratio, book_value, dividend_yield_pct,
      roce_pct, roe_pct, face_value
    """
    overview: Dict[str, Any] = {}

    # ── Top ratios list (#top-ratios) ──────────────────────────
    top_ratios = soup.find(id="top-ratios")
    if top_ratios is None:
        top_ratios = soup.find("ul", class_="company-ratios")

    if top_ratios:
        items = top_ratios.find_all("li")
        for li in items:
            name_tag  = li.find("span", class_="name")
            value_tag = li.find("span", class_="number") or li.find("span", class_="value")
            if not name_tag or not value_tag:
                continue

            name       = name_tag.get_text(strip=True).lower()
            value_text = value_tag.get_text(strip=True)

            # ── Market Cap ────────────────────────────────────
            if "market cap" in name:
                overview["market_cap_cr"] = _clean_num(value_text)

            # ── Current Price ─────────────────────────────────
            elif "current price" in name:
                overview["current_price"] = _clean_num(value_text)

            # ── 52-week High / Low ────────────────────────────
            # Screener renders as single "High / Low" with value "₹ 1,612 / ₹ 1,181"
            elif ("high" in name and "low" in name) or "52" in name:
                raw = value_text.strip()
                if "/" in raw:
                    parts = raw.split("/")
                    # Each part may have its own ₹ sign
                    high_v = _clean_num_part(parts[0])
                    low_v  = _clean_num_part(parts[1])
                    # Screener shows High first, Low second
                    if high_v is not None:
                        overview["high_52w"] = high_v
                    if low_v is not None:
                        overview["low_52w"] = low_v
                else:
                    overview["high_52w"] = _clean_num(raw)

            elif "52 week high" in name or "52w high" in name:
                overview["high_52w"] = _clean_num(value_text)

            elif "52 week low" in name or "52w low" in name:
                overview["low_52w"] = _clean_num(value_text)

            # ── Stock P/E ────────────────────────────────────
            elif "stock p/e" in name or name.strip() in ("p/e", "pe"):
                overview["pe_ratio"] = _clean_num(value_text)

            # ── Book Value ───────────────────────────────────
            elif "book value" in name:
                overview["book_value"] = _clean_num(value_text)

            # ── Dividend Yield ───────────────────────────────
            elif "dividend yield" in name:
                overview["dividend_yield_pct"] = _clean_num(value_text)

            # ── ROCE ─────────────────────────────────────────
            elif "roce" in name:
                overview["roce_pct"] = _clean_num(value_text)

            # ── ROE ──────────────────────────────────────────
            elif name.strip() == "roe" or ("roe" in name and "roce" not in name):
                overview["roe_pct"] = _clean_num(value_text)

            # ── Face Value ───────────────────────────────────
            elif "face value" in name:
                overview["face_value"] = _clean_num(value_text)

    # ── Current price from header if missing ──────────────────
    if "current_price" not in overview:
        price_tag = soup.find("span", class_="number")
        if price_tag:
            overview["current_price"] = _clean_num(price_tag.get_text())

    # ── Fallback: regex scan for 52-week High/Low ─────────────
    # Some Screener layouts embed these in plain text nodes
    if "high_52w" not in overview or "low_52w" not in overview:
        full_text = soup.get_text(" ")
        m_high = re.search(r"52[- ]?[Ww]eek\s+[Hh]igh[^₹\d]*([\d,\.]+)", full_text)
        m_low  = re.search(r"52[- ]?[Ww]eek\s+[Ll]ow[^₹\d]*([\d,\.]+)", full_text)
        if m_high and "high_52w" not in overview:
            overview["high_52w"] = _clean_num(m_high.group(1))
        if m_low and "low_52w" not in overview:
            overview["low_52w"]  = _clean_num(m_low.group(1))

    found = [k for k, v in overview.items() if v is not None]
    if found:
        print(f"  ok  screener[overview]: {len(found)} fields → {', '.join(found)}")
    else:
        print(f"  warn  screener[overview]: no overview fields found")

    return overview


def fetch_screener_data(symbol_nse: str) -> Dict[str, Any]:
    """
    Fetch all Screener.in tables + overview ratios.

    Returns dict:
      "overview"      → dict (current_price, high_52w, low_52w,
                               market_cap_cr, pe_ratio, book_value,
                               dividend_yield_pct, roce_pct, roe_pct,
                               face_value)
      "quarters"      → DataFrame
      "profit_loss"   → DataFrame
      "growth"        → DataFrame | None
      "balance_sheet" → DataFrame
      "cash_flow"     → DataFrame
      "ratios"        → DataFrame
      "shareholding"  → DataFrame

    All DataFrame monetary values in Rs. Crores (Screener native).
    """
    html = _get_html(symbol_nse)
    if not html:
        print(f"  error screener: could not fetch HTML for {symbol_nse}")
        return {}

    soup   = BeautifulSoup(html, "lxml")
    result: Dict[str, Any] = {}

    result["overview"] = _parse_overview(soup)

    for key, section_id in _SECTIONS.items():
        section = soup.find("section", id=section_id)
        df      = _parse_table(section)
        result[key] = df
        status  = f"{df.shape[0]}r x {df.shape[1]}c" if df is not None else "not found"
        ok = "ok" if df is not None else "warn"
        print(f"  {ok} screener[{key}]: {status}")

    return result