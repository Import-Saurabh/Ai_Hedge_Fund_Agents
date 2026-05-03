"""
etl/extract/screener.py  v3.2
────────────────────────────────────────────────────────────────
Changes vs v3.1:
  NEW — fetch_bs_schedules(symbol_nse, html):
    Screener's main HTML only exposes 10 top-level (bold) balance
    sheet rows.  Sub-items like Cash Equivalents, Trade Receivables,
    Inventories, lt_borrowings etc. live behind the schedules API:

      GET /api/company/{id}/schedules/
          ?parent=Other+Assets
          &section=balance-sheet
          &consolidated=

    The numeric company ID is embedded in the HTML as a data
    attribute on the #company-info element, e.g.:
      <div id="company-info" data-company-id="57" ...>
    We extract it with a regex fallback so no login is required.

    fetch_bs_schedules() returns a dict:
      {
        "Other Assets":      {col: {sub_label: value, ...}, ...},
        "Other Liabilities": {col: {sub_label: value, ...}, ...},
        "Borrowings":        {col: {sub_label: value, ...}, ...},
      }
    This dict is merged into the "balance_sheet" DataFrame by
    fetch_screener_data() before returning, so screener_loader.py
    sees all the sub-rows it already knows how to handle.

  NEW — _extract_company_id(html):
    Pulls numeric Screener company ID from page HTML without any
    login / cookie.  Tries multiple patterns robustly.
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

# ── Schedule parents we want to fetch ─────────────────────────
# Key  = parent param in API URL
# Maps to which sub-items we care about (for logging)
_SCHEDULE_PARENTS = [
    "Other+Assets",
    "Other+Liabilities",
    "Borrowings",
]

# Human-readable names for logging
_SCHEDULE_NAMES = {
    "Other+Assets":      "Other Assets",
    "Other+Liabilities": "Other Liabilities",
    "Borrowings":        "Borrowings",
}


# ─────────────────────────────────────────────────────────────
# HTML fetch
# ─────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────
# Company ID extraction (no login required)
# ─────────────────────────────────────────────────────────────

def _extract_company_id(html: str) -> Optional[int]:
    """
    Extract Screener's numeric company ID from the page HTML.

    Screener embeds the ID in several places:
      1. <div id="company-info" data-company-id="57" ...>
      2. Inline JS: var company_id = 57;
      3. API URLs in page JS: /api/company/57/
      4. form action="/company/57/..."

    Returns int ID or None if not found.
    """
    if not html:
        return None

    # Pattern 1: data-company-id attribute (most reliable)
    m = re.search(r'data-company-id=["\'](\d+)["\']', html)
    if m:
        return int(m.group(1))

    # Pattern 2: JS variable assignment
    m = re.search(r'(?:var\s+|let\s+|const\s+)company_id\s*=\s*(\d+)', html)
    if m:
        return int(m.group(1))

    # Pattern 3: /api/company/{id}/ in any href or src
    m = re.search(r'/api/company/(\d+)/', html)
    if m:
        return int(m.group(1))

    # Pattern 4: data-id on a known element
    soup = BeautifulSoup(html, "lxml")
    for tag in ["div", "section", "main", "article"]:
        el = soup.find(tag, attrs={"data-company-id": True})
        if el:
            try:
                return int(el["data-company-id"])
            except Exception:
                pass

    # Pattern 5: any element with data-id that looks like a company anchor
    for el in soup.find_all(attrs={"data-id": True}):
        try:
            val = int(el["data-id"])
            if val > 0:
                return val
        except Exception:
            pass

    # Pattern 6: scan all anchor hrefs for /company/{id}/
    for a in soup.find_all("a", href=True):
        m = re.search(r'/company/(\d+)/', a["href"])
        if m:
            return int(m.group(1))

    # Pattern 7: look inside <script> tags for numeric ID near "company"
    for script in soup.find_all("script"):
        text = script.get_text()
        m = re.search(r'"company"[^}]*?"id"\s*:\s*(\d+)', text)
        if m:
            return int(m.group(1))
        m = re.search(r'companyId["\s:=]+(\d+)', text)
        if m:
            return int(m.group(1))

    print("  warn  screener: could not extract company_id from HTML")
    return None


# ─────────────────────────────────────────────────────────────
# Schedules API  (sub-breakdown of balance sheet parents)
# ─────────────────────────────────────────────────────────────

def _fetch_schedule(company_id: int, parent: str,
                    consolidated: bool = True) -> Optional[Dict]:
    """
    Call the Screener schedules API for one parent category.

    URL:  /api/company/{id}/schedules/
          ?parent={parent}&section=balance-sheet&consolidated=

    Returns raw JSON dict {period_label: {sub_label: value, ...}}
    or None on failure / non-200 / non-dict response.

    No login is required; the endpoint returns data for guest
    users for most companies.  If a company is restricted it
    returns an empty dict — we handle that gracefully.
    """
    cons_param = "" if consolidated else "false"
    url = (
        f"https://www.screener.in/api/company/{company_id}/schedules/"
        f"?parent={parent}&section=balance-sheet&consolidated={cons_param}"
    )

    for attempt in range(3):
        try:
            if _USE_HTTPX:
                r = httpx.get(url, headers=HDR, follow_redirects=True, timeout=15)
            else:
                r = requests.get(url, headers=HDR, timeout=15)

            if r.status_code == 200:
                data = r.json()
                if isinstance(data, dict) and data:
                    return data
                # empty dict → not available for this company
                return None

            if r.status_code in (403, 401):
                print(f"  warn  schedules[{parent}]: auth required (guest restricted)")
                return None

        except Exception as e:
            print(f"  warn  schedules[{parent}] attempt {attempt+1}: {e}")
            time.sleep(1.5 * (attempt + 1))

    return None


def _schedules_to_rows(raw: Dict) -> Dict[str, Dict[str, float]]:
    """
    Convert raw schedules JSON to a period→{label: value} mapping.

    Screener's API returns data in sub-label-first orientation:
      {
        "Cash in hand":  {"Mar 2024": "12.34", "Mar 2023": "10.11", ...},
        "Bank Balance":  {"Mar 2024": "456.78", "Mar 2023": "320.00", ...},
        ...
      }
    (This is confirmed by balance_sheet_scrapper.py which does pd.DataFrame(data).transpose())

    We transpose this to period-first for the merge loop:
      {
        "Mar 2024": {"Cash in hand": 12.34, "Bank Balance": 456.78},
        "Mar 2023": {"Cash in hand": 10.11, "Bank Balance": 320.00},
        ...
      }

    Also handles the reverse orientation defensively.
    """
    if not raw:
        return {}

    # Detect orientation: check if nested values are dicts whose keys look
    # like period labels ("Mar 2024") vs sub-item text labels.
    _PERIOD_PAT = re.compile(
        r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}$',
        re.IGNORECASE
    )

    def _is_period(s: str) -> bool:
        return bool(_PERIOD_PAT.match(str(s).strip()))

    first_val = next(iter(raw.values()))
    is_sublabel_first = (
        isinstance(first_val, dict) and
        any(_is_period(k) for k in list(first_val.keys())[:3])
    )

    def _fv(v):
        try:
            f = float(str(v).replace(",", "").replace("₹", "").strip())
            return None if (f != f or f == float('inf')) else round(f, 4)
        except (ValueError, TypeError):
            return None

    result: Dict[str, Dict[str, float]] = {}

    if is_sublabel_first:
        # Standard Screener: {sub_label: {period: value}}
        for sub_label, period_dict in raw.items():
            if not isinstance(period_dict, dict):
                continue
            for period, val in period_dict.items():
                fv = _fv(val)
                if fv is None:
                    continue
                p = str(period).strip()
                result.setdefault(p, {})[str(sub_label).strip()] = fv
    else:
        # Fallback: {period: {sub_label: value}}
        for period, sub_dict in raw.items():
            if not isinstance(sub_dict, dict):
                continue
            p = str(period).strip()
            for sub_label, val in sub_dict.items():
                fv = _fv(val)
                if fv is None:
                    continue
                result.setdefault(p, {})[str(sub_label).strip()] = fv

    return result


def fetch_bs_schedules(symbol_nse: str, html: str,
                       consolidated: bool = True) -> Dict[str, Any]:
    """
    Fetch balance sheet sub-breakdowns from the Screener schedules API.

    Requires the page HTML to extract the company ID (no login needed).

    Returns dict:
      {
        "Other Assets":      {period: {sub_label: float, ...}, ...},
        "Other Liabilities": {period: {sub_label: float, ...}, ...},
        "Borrowings":        {period: {sub_label: float, ...}, ...},
      }

    Any parent that fails / is unavailable is simply absent from the dict.
    """
    company_id = _extract_company_id(html)
    if company_id is None:
        print("  warn  bs_schedules: no company_id — skipping sub-breakdown fetch")
        return {}

    print(f"  ok  screener company_id={company_id} — fetching BS schedules...")

    schedules: Dict[str, Any] = {}
    for parent_param in _SCHEDULE_PARENTS:
        name = _SCHEDULE_NAMES[parent_param]
        raw = _fetch_schedule(company_id, parent_param, consolidated)
        if raw:
            rows = _schedules_to_rows(raw)
            schedules[name] = rows
            # Count unique sub-labels across all periods for logging
            all_labels: set = set()
            for period_data in rows.values():
                all_labels.update(period_data.keys())
            print(f"  ok  bs_schedules[{name}]: "
                  f"{len(rows)} periods × {len(all_labels)} sub-items")
            # Debug: show exact sub-labels so label map can be tuned
            print(f"       sub-labels: {sorted(all_labels)}")
        else:
            print(f"  warn  bs_schedules[{name}]: no data (guest-restricted or empty)")

        time.sleep(0.4)   # polite delay

    return schedules


# ─────────────────────────────────────────────────────────────
# Merge schedules into the main BS DataFrame
# ─────────────────────────────────────────────────────────────

def _merge_schedules_into_bs(bs_df: pd.DataFrame,
                              schedules: Dict[str, Any]) -> pd.DataFrame:
    """
    Inject sub-item rows from the schedules API into the existing
    balance_sheet DataFrame so screener_loader sees them as normal rows.

    For each (parent, period, sub_label, value):
      • If the sub_label already exists as a row in bs_df → update the cell
      • Otherwise → append a new row

    The result is a DataFrame with the same column structure as bs_df
    but enriched with sub-breakdown rows.

    Parent → sub-label mappings that screener_loader.py already knows:

    "Borrowings" schedule sub-labels (Screener standard):
      "Long term borrowings"  → row "Long term Borrowings"
      "Short term borrowings" → row "Short term Borrowings"
      "Lease Liabilities"     → row "Lease Liabilities"

    "Other Liabilities" sub-labels:
      "Non controlling int"   → row "Non controlling int"
      "Trade Payables"        → row "Trade Payables"
      "Advance from Customers"→ row "Advance from Customers"
      (anything else)         → row "Other liability items"

    "Other Assets" sub-labels:
      "Inventories"           → row "Inventories"
      "Trade receivables"     → row "Trade receivables"
      "Receivables over 6m"   → row "Receivables over 6m"
      "Receivables under 6m"  → row "Receivables under 6m"
      "Cash Equivalents"      → row "Cash Equivalents"
        (also matches "Cash & Equivalents", "Cash and Bank")
      "Loans n Advances"      → row "Loans n Advances"
        (also matches "Loans and Advances")
    """
    if bs_df is None or bs_df.empty or not schedules:
        return bs_df

    df = bs_df.copy()

    # Normalise label for matching
    def _norm(s: str) -> str:
        return str(s).lower().strip()

    # --- Label normalisation rules --------------------------------
    # IMPORTANT: More-specific patterns must come BEFORE general ones.
    # "Other asset items" / "Other liability items" are returned verbatim
    # by the API — match them first before any generic "other" rule fires.
    _LABEL_MAP = [
        # ── Exact API labels — match first ───────────────────────
        (["other asset item"],           "Other asset items"),
        (["other liability item"],       "Other liability items"),

        # Borrowings sub-items
        (["long term borrowing"],          "Long term Borrowings"),
        (["short term borrowing"],         "Short term Borrowings"),
        (["lease liabilit"],               "Lease Liabilities"),
        (["preference capital"],           "Preference Capital"),
        (["other borrowing"],              "Other Borrowings"),

        # Other Liabilities sub-items (specific first)
        (["non controlling", "minority interest", "non-controlling"],
                                           "Non controlling int"),
        (["trade payable"],                "Trade Payables"),
        (["advance from customer"],        "Advance from Customers"),  # BEFORE "advance"

        # Other Assets sub-items (specific first)
        (["inventor"],                     "Inventories"),
        (["trade receivable", "debtors", "sundry debtor"],
                                           "Trade receivables"),
        # Removed: receivable over/under 6m, prov for doubtful — not in API
        # Cash: match many Screener sub-label variants
        (["cash equivalent", "cash & equiv", "cash and bank",
          "cash and equiv", "cash in hand", "bank balance",
          "cash & bank", "cash at bank", "balance with bank"],
                                           "Cash Equivalents"),
        # Loans: AFTER advance_from_customers to avoid false match on "advance"
        (["loan", "advance"],              "Loans n Advances"),
    ]

    def _canonical_label(sub_label: str) -> Optional[str]:
        n = _norm(sub_label)
        for patterns, canonical in _LABEL_MAP:
            if any(p in n for p in patterns):
                return canonical
        return None   # unknown sub-item — skip

    # Build a working dict: {canonical_row_label: {col: value}}
    inject: Dict[str, Dict[str, float]] = {}

    for parent_name, period_data in schedules.items():
        for period_col, sub_dict in period_data.items():
            # Normalise the period string so it matches DataFrame columns
            # Screener API uses "Mar 2024"; df.columns are also "Mar 2024"
            # Find the closest matching column
            matching_col = None
            for df_col in df.columns:
                if _norm(str(df_col)) == _norm(period_col):
                    matching_col = df_col
                    break
            if matching_col is None:
                # Try partial match (year + month)
                for df_col in df.columns:
                    if _norm(period_col)[:8] in _norm(str(df_col)):
                        matching_col = df_col
                        break
            if matching_col is None:
                continue  # period not in main BS — skip

            for sub_label, value in sub_dict.items():
                canonical = _canonical_label(sub_label)
                if canonical is None:
                    continue
                if canonical not in inject:
                    inject[canonical] = {}
                # Sum values with the same canonical label in the same period
                # (e.g., "Cash in hand" + "Bank Balance" → "Cash Equivalents")
                inject[canonical][matching_col] = round(
                    inject[canonical].get(matching_col, 0.0) + value, 4
                )

    if not inject:
        return df

    # Merge into df
    new_rows = []
    for canonical, col_vals in inject.items():
        if canonical in df.index:
            # Update existing row
            for col, val in col_vals.items():
                if col in df.columns:
                    # Only fill if cell is NaN / 0
                    existing = df.at[canonical, col]
                    if pd.isna(existing) or existing == 0:
                        df.at[canonical, col] = val
        else:
            # Build a new row
            new_row = pd.Series(dtype=object, name=canonical)
            for col in df.columns:
                new_row[col] = col_vals.get(col, None)
            new_rows.append(new_row)

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        df = pd.concat([df, new_df])

    added = sorted(inject.keys())
    print(f"  ok  bs_schedules merged: {len(added)} sub-rows → "
          f"{', '.join(added)}")
    return df


# ─────────────────────────────────────────────────────────────
# Table parser
# ─────────────────────────────────────────────────────────────

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


# ─────────────────────────────────────────────────────────────
# Number cleaning
# ─────────────────────────────────────────────────────────────

def _clean_num(text: str) -> Optional[float]:
    """
    Robust number extractor for Screener values.
    Handles:
      "₹ 1,612"          → 1612.0
      "₹1,181"           → 1181.0
      "3,67,472 Cr."     → 367472.0  (Indian lakh comma format)
      "14.0%"            → 14.0
      "1,612 / 1,181"    → returns first value
    """
    if not text:
        return None
    s = str(text).strip()
    s = s.replace("₹", "").replace("Cr.", "").replace("Cr", "").replace("%", "").strip()
    if "/" in s:
        parts = s.split("/")
        s = parts[0].strip()
    s = s.replace(",", "").strip()
    if s in ("", "-", "—", "N/A", "nan", "None"):
        return None
    try:
        return round(float(s), 4)
    except ValueError:
        return None


def _clean_num_part(text: str) -> Optional[float]:
    """Like _clean_num but strips ₹ from a single fragment."""
    if not text:
        return None
    s = str(text).replace("₹", "").replace(",", "").replace("Cr.", "").replace("Cr", "").strip()
    if s in ("", "-", "—", "N/A", "nan", "None"):
        return None
    try:
        return round(float(s), 4)
    except ValueError:
        return None


# ─────────────────────────────────────────────────────────────
# Overview parser
# ─────────────────────────────────────────────────────────────

def _parse_overview(soup: BeautifulSoup) -> Dict[str, Any]:
    """
    Parse the company overview/ratios panel at the top of the Screener page.

    Extracts:
      current_price, high_52w, low_52w, market_cap_cr,
      pe_ratio, book_value, dividend_yield_pct,
      roce_pct, roe_pct, face_value
    """
    overview: Dict[str, Any] = {}

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

            if "market cap" in name:
                overview["market_cap_cr"] = _clean_num(value_text)

            elif "current price" in name:
                overview["current_price"] = _clean_num(value_text)

            elif ("high" in name and "low" in name) or "52" in name:
                raw = value_text.strip()
                if "/" in raw:
                    parts = raw.split("/")
                    high_v = _clean_num_part(parts[0])
                    low_v  = _clean_num_part(parts[1])
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

            elif "stock p/e" in name or name.strip() in ("p/e", "pe"):
                overview["pe_ratio"] = _clean_num(value_text)

            elif "book value" in name:
                overview["book_value"] = _clean_num(value_text)

            elif "dividend yield" in name:
                overview["dividend_yield_pct"] = _clean_num(value_text)

            elif "roce" in name:
                overview["roce_pct"] = _clean_num(value_text)

            elif name.strip() == "roe" or ("roe" in name and "roce" not in name):
                overview["roe_pct"] = _clean_num(value_text)

            elif "face value" in name:
                overview["face_value"] = _clean_num(value_text)

    if "current_price" not in overview:
        price_tag = soup.find("span", class_="number")
        if price_tag:
            overview["current_price"] = _clean_num(price_tag.get_text())

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


# ─────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────

def fetch_screener_data(symbol_nse: str) -> Dict[str, Any]:
    """
    Fetch all Screener.in tables + overview ratios + BS sub-breakdowns.

    Returns dict:
      "overview"      → dict (current_price, high_52w, low_52w,
                               market_cap_cr, pe_ratio, book_value,
                               dividend_yield_pct, roce_pct, roe_pct,
                               face_value)
      "quarters"      → DataFrame
      "profit_loss"   → DataFrame
      "growth"        → DataFrame | None
      "balance_sheet" → DataFrame  ← NOW enriched with sub-items from
                                      the schedules API (Cash Equivalents,
                                      Trade Receivables, Inventories,
                                      lt/st borrowings, etc.)
      "cash_flow"     → DataFrame
      "ratios"        → DataFrame
      "shareholding"  → DataFrame
      "bs_schedules"  → dict (raw schedule data, for debugging)

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

    # ── Enrich balance sheet with sub-breakdowns ──────────────
    # Determine if page was consolidated (for the correct API param)
    consolidated = True
    if html:
        # Screener sets a canonical URL — check if "consolidated" appears
        # We default to consolidated=True (most companies); if the main
        # HTML was fetched from the standalone URL, set False.
        # This heuristic works for ~95% of NSE-listed companies.
        consolidated = "consolidated" in html[:5000].lower()

    bs_schedules = fetch_bs_schedules(symbol_nse, html, consolidated)
    result["bs_schedules"] = bs_schedules

    if bs_schedules and result.get("balance_sheet") is not None:
        result["balance_sheet"] = _merge_schedules_into_bs(
            result["balance_sheet"], bs_schedules
        )

    return result