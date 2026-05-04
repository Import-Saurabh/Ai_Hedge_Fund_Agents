"""
etl/extract/cashflow.py  v2.0
────────────────────────────────────────────────────────────────
Changes vs v1.0:
  FIX 1 — _parse_schedule(): Screener's schedule JSON is keyed as
           { "Sub Label": { "Mar 2024": "123", ... }, ... }
           (sub-label → period → value).  v1.0 assumed the inverse
           layout and produced empty sub-item dicts for every period.
           Now correctly pivots: iterates sub-labels in the outer
           loop, period-labels in the inner loop.

  FIX 2 — _HDR / _get_json(): Schedule API calls now send a dynamic
           Referer header (required by Screener to return data) and
           Accept: application/json (not text/html).  Without Referer
           the API silently returns an empty dict {}.

  FIX 3 — _get_html(): Now records WHICH URL actually succeeded so
           the consolidated flag is set from the real URL, not a
           fragile substring search on the first 5 000 chars.

  FIX 4 — _build_period_rows(): Replaced `or` boolean short-circuit
           on floats with explicit `is None` checks.  Previously a
           legitimate 0.0 total from schedules triggered the toplevel
           fallback lookup, overwriting a valid zero with None.

  FIX 5 — _find_total(): Added sum-of-sub-items as a last resort
           when no known label pattern matches, so stocks whose
           Screener pages use non-standard total row names still
           get a section total.

  FIX 6 — _fetch_toplevel_cf(): Strip commas and parse numbers
           immediately so the DataFrame contains floats, not strings.
           Previously _clean_num() was called only during lookup,
           meaning the toplevel dict sometimes held raw strings that
           _f() silently turned to None.

  FIX 7 — fetch_cashflow(): When ALL three schedule sections return
           empty but the top-level HTML table was parsed successfully,
           still build rows from the top-level table instead of
           returning [].  Screener occasionally rate-limits the API
           while the HTML page loads fine.
────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import json
import math
import re
import time
from io import StringIO
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    import httpx
    _USE_HTTPX = True
except ImportError:
    import requests
    _USE_HTTPX = False

from bs4 import BeautifulSoup


# ── Constants ────────────────────────────────────────────────────────────────

# Base headers for HTML page fetches
_HDR_HTML = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

# Headers for JSON schedule API calls — Referer is set dynamically per symbol
_HDR_JSON = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept":          "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    # Referer injected at call site: f"https://www.screener.in/company/{symbol}/..."
}

_CF_SCHEDULE_PARENTS = [
    ("Operating Activity", "Cash+from+Operating+Activity"),
    ("Investing Activity", "Cash+from+Investing+Activity"),
    ("Financing Activity", "Cash+from+Financing+Activity"),
]

# Known label patterns for the three rolled-up section totals
_TOTAL_LABELS: Dict[str, List[str]] = {
    "Operating Activity": [
        "cash from operating activity",
        "net cash from operating activities",
        "net cash provided by operating activities",
        "total operating",
        "operating activity",
        "cash flow from operations",
    ],
    "Investing Activity": [
        "cash from investing activity",
        "net cash from investing activities",
        "net cash used in investing activities",
        "total investing",
        "investing activity",
        "cash flow from investing",
    ],
    "Financing Activity": [
        "cash from financing activity",
        "net cash from financing activities",
        "net cash used in financing activities",
        "total financing",
        "financing activity",
        "cash flow from financing",
    ],
}

# Labels that identify capex inside the Investing schedule
_CAPEX_LABELS = [
    "purchase of fixed assets",
    "purchase of property plant and equipment",
    "capital expenditure",
    "capex",
    "additions to fixed assets",
    "purchase of ppe",
    "fixed assets purchased",
    "acquisition of fixed assets",
    "payment for fixed assets",
]


# ── Number helpers ────────────────────────────────────────────────────────────

def _f(v) -> Optional[float]:
    if v is None:
        return None
    try:
        fv = float(v)
        return None if (math.isnan(fv) or math.isinf(fv)) else fv
    except (TypeError, ValueError):
        return None


def _clean_num(text: Any) -> Optional[float]:
    """
    Parse a Screener numeric string → float (₹ Crores, Screener native).
    Handles: "1,234.56", "₹ 1,234", "3,67,472 Cr.", "-234", "0".
    Returns None for blanks / dashes / non-numeric.
    """
    if text is None:
        return None
    s = str(text).strip()
    s = (
        s.replace("₹", "")
         .replace("Cr.", "")
         .replace("Cr", "")
         .replace("%", "")
         .replace(",", "")
         .strip()
    )
    if s in ("", "-", "—", "N/A", "nan", "None"):
        return None
    try:
        return round(float(s), 2)
    except ValueError:
        return None


def _label_key(label: Any) -> str:
    """Normalise a label for comparison: lowercase, strip extra whitespace."""
    return " ".join(str(label).lower().split())


# ── Period label → ISO date ───────────────────────────────────────────────────

_MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}

_MONTH_END = {
    "01": "31", "02": "28", "03": "31", "04": "30",
    "05": "31", "06": "30", "07": "31", "08": "31",
    "09": "30", "10": "31", "11": "30", "12": "31",
}


def _period_to_iso(label: Any) -> Optional[str]:
    """
    Convert Screener period labels to ISO date strings (YYYY-MM-DD).

    Handles:
        "Mar 2024"       → "2024-03-31"
        "Mar 2024 TTM"   → "2024-03-31"
        "Sep 2023"       → "2023-09-30"
        "2024"           → "2024-03-31"  (assume March FY end)
        "FY2024"         → "2024-03-31"
        "2023-03-31"     → "2023-03-31"  (already ISO)
    """
    s = str(label).strip()

    # Already ISO
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s

    # Strip TTM suffix
    s = re.sub(r"\s*TTM\s*$", "", s, flags=re.I).strip()

    # "Mar 2024" or "Mar-2024"
    m = re.match(r"^([A-Za-z]{3})[\s\-](\d{4})$", s)
    if m:
        mon = _MONTH_MAP.get(m.group(1).lower())
        yr  = m.group(2)
        if mon:
            return f"{yr}-{mon}-{_MONTH_END.get(mon, '30')}"

    # "FY2024" or "FY 2024"
    m = re.match(r"^FY\s*(\d{4})$", s, re.I)
    if m:
        return f"{m.group(1)}-03-31"

    # Plain year "2024"
    m = re.match(r"^(\d{4})$", s)
    if m:
        return f"{m.group(1)}-03-31"

    return None


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _get(url: str, headers: dict, retries: int = 3, delay: float = 2.0):
    """GET with retry. Returns response object or None."""
    for attempt in range(retries):
        try:
            if _USE_HTTPX:
                r = httpx.get(url, headers=headers, follow_redirects=True, timeout=20)
            else:
                r = requests.get(url, headers=headers, timeout=20)
            if r.status_code == 200:
                return r
            print(f"  warn  cashflow fetch: HTTP {r.status_code} — {url}")
        except Exception as e:
            print(f"  warn  cashflow fetch attempt {attempt + 1}: {e}")
        time.sleep(delay * (attempt + 1))
    return None


def _get_html(symbol_nse: str) -> Tuple[Optional[str], bool]:
    """
    Fetch Screener company page HTML.
    Tries consolidated first, then standalone.

    Returns (html_text, is_consolidated).
    FIX 3: Returns the consolidated flag derived from WHICH URL loaded,
    not from a fragile substring search on page content.
    """
    for suffix, is_cons in (("consolidated", True), ("", False)):
        url = f"https://www.screener.in/company/{symbol_nse}/{suffix}/"
        r = _get(url, headers=_HDR_HTML)
        if r is not None:
            # Extra guard: if we landed on a login/redirect page there will be
            # no cash-flow section — treat as failure and try standalone.
            if 'id="cash-flow"' in r.text or "cash-flow" in r.text:
                print(f"  ok  cashflow: page loaded ({'consolidated' if is_cons else 'standalone'})")
                return r.text, is_cons
            print(f"  warn  cashflow: page loaded but no cash-flow section — trying next")
    return None, False


def _get_json(url: str, referer: str) -> Optional[Dict]:
    """
    GET a Screener schedule API URL and return parsed JSON dict, or None.
    FIX 2: Injects the correct Referer and Accept: application/json headers.
    """
    headers = {**_HDR_JSON, "Referer": referer}
    r = _get(url, headers=headers)
    if r is None:
        return None
    try:
        data = r.json()
        # Screener returns a dict keyed by sub-label (or empty dict {})
        if isinstance(data, dict) and data:
            return data
        # Empty dict means no data (rate-limited or not available)
        return None
    except Exception as e:
        print(f"  warn  cashflow JSON parse: {e}")
        return None


# ── Company ID extraction ─────────────────────────────────────────────────────

def _extract_company_id(html: str) -> Optional[int]:
    """
    Extract Screener numeric company ID from page HTML.
    Tries multiple patterns for robustness.
    """
    if not html:
        return None

    for pat in [
        r'data-company-id=["\'](\d+)["\']',
        r'(?:var\s+|let\s+|const\s+)company_id\s*=\s*(\d+)',
        r'/api/company/(\d+)/',
    ]:
        m = re.search(pat, html)
        if m:
            return int(m.group(1))

    soup = BeautifulSoup(html, "lxml")

    for tag in ["div", "section", "main", "article"]:
        el = soup.find(tag, attrs={"data-company-id": True})
        if el:
            try:
                return int(el["data-company-id"])
            except Exception:
                pass

    for el in soup.find_all(attrs={"data-id": True}):
        try:
            val = int(el["data-id"])
            if val > 0:
                return val
        except Exception:
            pass

    for a in soup.find_all("a", href=True):
        m = re.search(r'/company/(\d+)/', a["href"])
        if m:
            return int(m.group(1))

    for script in soup.find_all("script"):
        text = script.get_text()
        for pat in [
            r'"company"[^}]*?"id"\s*:\s*(\d+)',
            r'companyId["\s:=]+(\d+)',
        ]:
            m = re.search(pat, text)
            if m:
                return int(m.group(1))

    print("  warn  cashflow: could not extract company_id from HTML")
    return None


# ── Schedules API fetch ───────────────────────────────────────────────────────

def _fetch_cf_schedule(
    company_id: int,
    parent_param: str,
    section_name: str,
    consolidated: bool,
    symbol_nse: str,
) -> Optional[Dict]:
    """
    Call Screener schedules API for one CF section.

    Returns raw JSON:
        { "Sub Label": { "Mar 2024": "123.45", "Mar 2023": "67.89" }, ... }
    or None on failure.
    """
    cons    = "" if consolidated else "false"
    url     = (
        f"https://www.screener.in/api/company/{company_id}/schedules/"
        f"?parent={parent_param}&section=cash-flow&consolidated={cons}"
    )
    # FIX 2: Dynamic Referer based on actual symbol and consolidated status
    referer = (
        f"https://www.screener.in/company/{symbol_nse}/"
        f"{'consolidated' if consolidated else ''}/"
    )
    data = _get_json(url, referer=referer)
    if data:
        print(f"  ok  cashflow schedule [{section_name}]: {len(data)} sub-labels")
    else:
        print(f"  warn  cashflow schedule [{section_name}]: no data")
    return data


# ── Top-level CF table (fallback totals) ──────────────────────────────────────

def _fetch_toplevel_cf(html: str) -> Optional[pd.DataFrame]:
    """
    Parse the top-level cash flow table from the Screener HTML page.

    Returns a DataFrame indexed by metric (str) with period columns.
    FIX 6: Values are parsed to float immediately (not left as strings).
    """
    soup    = BeautifulSoup(html, "lxml")
    section = soup.find("section", id="cash-flow")
    if section is None:
        return None
    table = section.find("table")
    if table is None:
        return None
    try:
        df = pd.read_html(StringIO(str(table)))[0]
        # Clean up the metric label column
        df.iloc[:, 0] = (
            df.iloc[:, 0]
            .astype(str)
            .str.replace(r"\s*\+\s*", "", regex=True)
            .str.strip()
        )
        df = df.rename(columns={df.columns[0]: "metric"})
        df = df.set_index("metric")

        # FIX 6: Convert all period columns to float right here
        for col in df.columns:
            df[col] = df[col].apply(_clean_num)

        return df
    except Exception as e:
        print(f"  warn  cashflow top-level parse: {e}")
        return None


# ── Parse schedule JSON into per-period sub-item dicts ───────────────────────

def _parse_schedule(raw: Dict) -> Dict[str, Dict[str, Any]]:
    """
    Convert raw Screener schedule JSON into a per-period dict.

    Screener's ACTUAL wire format (FIX 1 — v1.0 had this inverted):
        {
          "Sub Label A": { "Mar 2024": "123.45", "Mar 2023": "67.89" },
          "Sub Label B": { "Mar 2024": "999.00", "Mar 2023": "888.00" },
          ...
        }

    Output:
        {
          "Mar 2024": { "Sub Label A": 123.45, "Sub Label B": 999.00, ... },
          "Mar 2023": { "Sub Label A":  67.89, "Sub Label B": 888.00, ... },
        }
    """
    result: Dict[str, Dict[str, Any]] = {}

    for sub_label, period_values in raw.items():
        if not isinstance(period_values, dict):
            continue
        sub_key = str(sub_label).strip()
        for period_label, value in period_values.items():
            period_key = str(period_label).strip()
            if period_key not in result:
                result[period_key] = {}
            result[period_key][sub_key] = _clean_num(value)

    return result


# ── Total / capex finders ─────────────────────────────────────────────────────

def _find_total(sub_items: Dict[str, Any], section_name: str) -> Optional[float]:
    """
    Find the rolled-up section total from sub-items dict.

    Priority:
      1. Exact label match against _TOTAL_LABELS
      2. Soft (substring) match
      3. FIX 5: Sum all sub-item values as last resort
    """
    candidates = _TOTAL_LABELS.get(section_name, [])

    # 1. Exact match
    for candidate in candidates:
        for label, val in sub_items.items():
            if _label_key(label) == candidate:
                v = _f(val)
                if v is not None:
                    return v

    # 2. Soft match (candidate is substring of label)
    for candidate in candidates:
        for label, val in sub_items.items():
            if candidate in _label_key(label):
                v = _f(val)
                if v is not None:
                    return v

    # 3. Sum all non-None sub-items as a last resort
    #    (works when Screener uses a stock-specific total label we don't know)
    values = [_f(v) for v in sub_items.values() if _f(v) is not None]
    if values:
        return round(sum(values), 2)

    return None


def _find_capex(investing_sub_items: Dict[str, Any]) -> Optional[float]:
    """Extract capex from investing sub-items dict."""
    for candidate in _CAPEX_LABELS:
        for label, val in investing_sub_items.items():
            if candidate in _label_key(label):
                return _f(val)
    return None


# ── Merge all schedules into per-period rows ──────────────────────────────────

def _build_period_rows(
    schedules: Dict[str, Dict[str, Dict[str, Any]]],
    toplevel_df: Optional[pd.DataFrame],
) -> List[Dict]:
    """
    Combine the three schedule dicts + top-level HTML table into a list of
    per-period row dicts ready for cashflow_loader.

    schedules = {
        "Operating Activity":  { period_label: { sub_label: float_val } },
        "Investing Activity":  { ... },
        "Financing Activity":  { ... },
    }
    """

    # Collect all period labels seen across all sections
    all_periods: set = set()
    for section_data in schedules.values():
        all_periods.update(section_data.keys())

    # Pre-build top-level fallback totals dict
    # { period_label_raw: { normalised_label: float_val } }
    toplevel: Dict[str, Dict[str, Optional[float]]] = {}
    if toplevel_df is not None:
        for period_col in toplevel_df.columns:
            period_key = str(period_col).strip()
            col_totals: Dict[str, Optional[float]] = {}
            for metric_label, val in toplevel_df[period_col].items():
                # values already float from FIX 6
                col_totals[_label_key(metric_label)] = _f(val)
            toplevel[period_key] = col_totals
            # Also store under the ISO date so both lookup keys work
            iso_key = _period_to_iso(period_key)
            if iso_key and iso_key not in toplevel:
                toplevel[iso_key] = col_totals

    def _toplevel_lookup(period_label: str, iso_date: str, keys: List[str]) -> Optional[float]:
        """
        FIX 4: Use explicit `is None` so a legitimate 0.0 from the
        schedules is never overridden by the toplevel fallback.
        """
        tl = toplevel.get(period_label) or toplevel.get(iso_date) or {}
        for k in keys:
            v = tl.get(k)
            if v is not None:
                return v
        return None

    rows: List[Dict] = []

    for period_label in sorted(all_periods):
        iso_date = _period_to_iso(period_label)
        if not iso_date:
            print(f"  warn  cashflow: cannot parse period label '{period_label}' — skipping")
            continue

        ops_items = schedules.get("Operating Activity", {}).get(period_label, {})
        inv_items = schedules.get("Investing Activity", {}).get(period_label, {})
        fin_items = schedules.get("Financing Activity", {}).get(period_label, {})

        # FIX 4: Resolve totals using explicit None checks, not `or`
        cfo = _find_total(ops_items, "Operating Activity")
        if cfo is None:
            cfo = _toplevel_lookup(period_label, iso_date, _TOTAL_LABELS["Operating Activity"])

        cfi = _find_total(inv_items, "Investing Activity")
        if cfi is None:
            cfi = _toplevel_lookup(period_label, iso_date, _TOTAL_LABELS["Investing Activity"])

        cff = _find_total(fin_items, "Financing Activity")
        if cff is None:
            cff = _toplevel_lookup(period_label, iso_date, _TOTAL_LABELS["Financing Activity"])

        capex = _find_capex(inv_items)

        # FCF = CFO + capex (capex stored as negative by Screener)
        fcf: Optional[float] = None
        if cfo is not None and capex is not None:
            fcf = round(cfo + capex, 2)

        # Net cash flow = CFO + CFI + CFF
        ncf: Optional[float] = None
        if cfo is not None and cfi is not None and cff is not None:
            ncf = round(cfo + cfi + cff, 2)

        # raw_details_json — every sub-item keyed by "Section > Sub Label"
        raw_detail: Dict[str, Any] = {}
        for section_name, sub_items in [
            ("Operating Activity", ops_items),
            ("Investing Activity", inv_items),
            ("Financing Activity", fin_items),
        ]:
            for sub_label, val in sub_items.items():
                raw_detail[f"{section_name} > {sub_label}"] = val

        rows.append({
            "period_end":       iso_date,
            "period_type":      "annual",
            "cfo":              cfo,
            "cfi":              cfi,
            "cff":              cff,
            "capex":            capex,
            "free_cash_flow":   fcf,
            "net_cash_flow":    ncf,
            "data_source":      "screener",
            "raw_details_json": json.dumps(raw_detail, default=str),
        })

    return rows


# ── Fallback: build rows from top-level HTML table only ──────────────────────

def _build_rows_from_toplevel(toplevel_df: pd.DataFrame) -> List[Dict]:
    """
    FIX 7: When schedule API calls all return empty (rate-limited etc.),
    build period rows from just the top-level HTML CF table.
    Only cfo/cfi/cff totals are available — sub-items will be empty.
    """
    rows: List[Dict] = []
    for period_col in toplevel_df.columns:
        iso_date = _period_to_iso(str(period_col).strip())
        if not iso_date:
            continue

        col = toplevel_df[period_col]

        def _find_row(candidates: List[str]) -> Optional[float]:
            for label in col.index:
                for c in candidates:
                    if c in _label_key(label):
                        return _f(col[label])
            return None

        cfo   = _find_row(_TOTAL_LABELS["Operating Activity"])
        cfi   = _find_row(_TOTAL_LABELS["Investing Activity"])
        cff   = _find_row(_TOTAL_LABELS["Financing Activity"])

        fcf: Optional[float] = None
        ncf: Optional[float] = None
        if cfo is not None and cfi is not None and cff is not None:
            ncf = round(cfo + cfi + cff, 2)

        rows.append({
            "period_end":       iso_date,
            "period_type":      "annual",
            "cfo":              cfo,
            "cfi":              cfi,
            "cff":              cff,
            "capex":            None,
            "free_cash_flow":   fcf,
            "net_cash_flow":    ncf,
            "data_source":      "screener",
            "raw_details_json": json.dumps({}, default=str),
        })
    return rows


# ── Public entry point ────────────────────────────────────────────────────────

def fetch_cashflow(symbol_nse: str) -> List[Dict]:
    """
    Fetch granular Screener cash flow data for `symbol_nse`.

    Steps:
      1. Fetch company page HTML → extract numeric company_id.
      2. Detect consolidated vs standalone from which URL loaded (FIX 3).
      3. Parse top-level CF HTML table for fallback totals (FIX 6).
      4. Call schedules API for Operating / Investing / Financing (FIX 2).
      5. Parse each schedule JSON — correct pivot (FIX 1).
      6. Merge into per-period row dicts with correct 0.0 handling (FIX 4)
         and last-resort summing (FIX 5).
      7. Fall back to top-level table rows if API returned nothing (FIX 7).

    Returns a list of dicts (one per annual period) suitable for
    cashflow_loader.load_cashflow().  Empty list on unrecoverable failure.
    """
    print(f"\n  [cashflow extract] Fetching Screener CF for {symbol_nse}...")

    # ── 1 & 3. Page HTML + consolidated flag ─────────────────────────────────
    html, consolidated = _get_html(symbol_nse)
    if not html:
        print(f"  error  cashflow: could not fetch Screener page for {symbol_nse}")
        return []

    # ── 2. Company ID ─────────────────────────────────────────────────────────
    company_id = _extract_company_id(html)
    if not company_id:
        print(f"  error  cashflow: company_id not found for {symbol_nse}")
        return []
    print(f"  ok  cashflow: company_id={company_id}, consolidated={consolidated}")

    # ── 4. Top-level CF table (FIX 6 — values parsed to float immediately) ───
    toplevel_df = _fetch_toplevel_cf(html)
    if toplevel_df is not None:
        print(f"  ok  cashflow top-level table: "
              f"{toplevel_df.shape[0]} rows × {toplevel_df.shape[1]} cols")
    else:
        print(f"  warn  cashflow: top-level CF table not found in HTML")

    # ── 5. Schedule API calls ─────────────────────────────────────────────────
    schedules: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for section_name, parent_param in _CF_SCHEDULE_PARENTS:
        raw = _fetch_cf_schedule(
            company_id, parent_param, section_name, consolidated, symbol_nse
        )
        time.sleep(0.5)   # polite delay
        if raw:
            # FIX 1: correct pivot
            schedules[section_name] = _parse_schedule(raw)
        else:
            schedules[section_name] = {}

    any_schedule_data = any(bool(v) for v in schedules.values())

    # ── FIX 7: If API returned nothing but we have the HTML table, use it ────
    if not any_schedule_data:
        print(f"  warn  cashflow: all schedule API calls returned empty")
        if toplevel_df is not None:
            print(f"  info  cashflow: falling back to top-level HTML table for {symbol_nse}")
            rows = _build_rows_from_toplevel(toplevel_df)
            print(f"  ok  cashflow: {len(rows)} annual periods from top-level table")
            return rows
        print(f"  error cashflow: no data at all for {symbol_nse}")
        return []

    # ── 6. Build per-period rows ──────────────────────────────────────────────
    rows = _build_period_rows(schedules, toplevel_df)
    print(f"  ok  cashflow: {len(rows)} annual periods extracted for {symbol_nse}")
    return rows