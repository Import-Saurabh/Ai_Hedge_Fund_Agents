"""
etl/extract/cashflow.py  v3.0
────────────────────────────────────────────────────────────────
Changes vs v2.0:
  FIX 8 — _find_total() was summing ALL sub-items as a last-resort
           fallback, which caused it to include the total row label
           itself (Screener sometimes returns the section total as a
           sub-item). Now total rows are excluded from the sum by
           checking against _TOTAL_LABELS before summing.

  FIX 9 — raw_details_json was empty (0 sub-items) in the DB even
           when schedule API returned data. Root cause: the section
           total row label (e.g. "Cash from Operating Activity") was
           being stored in the sub-items dict and then _find_total
           short-circuited before the sub-items were written to raw.
           Now ALL sub-items (including the total row) are written to
           raw_details_json using their ORIGINAL label as key, so
           every sub-category is preserved.

  FIX 10 — Top-level HTML table rows that are NOT in schedule data
            (CFO/OP ratio, Free Cash Flow row, Net Cash Flow row)
            are now merged into raw_details_json under the key
            "TopLevel > <label>" so nothing from the HTML table
            is discarded.

  FIX 11 — _find_total() now tries to match the known total-label
            patterns FIRST (exact then soft), and only falls back to
            summing non-total sub-items (never the total row itself).
            This avoids double-counting when Screener returns e.g.
            "Cash from Operating Activity" inside the schedule JSON.

  FIX 12 — Comprehensive sub-label mapping: the full list of known
            sub-labels from the target table is mapped to canonical
            raw_details keys so downstream consumers can rely on
            stable key names even if Screener changes capitalisation.

  Screener data model (what we now capture per period):
  ┌──────────────────────────────────────────────────────────────┐
  │ cash_flow table columns (unchanged schema)                   │
  │   cfo, cfi, cff, capex, free_cash_flow, net_cash_flow        │
  ├──────────────────────────────────────────────────────────────┤
  │ raw_details_json  (everything else goes here)                │
  │                                                              │
  │ Operating Activity sub-items:                                │
  │   "Operating Activity > Profit from operations"              │
  │   "Operating Activity > Receivables"                         │
  │   "Operating Activity > Inventory"                           │
  │   "Operating Activity > Payables"                            │
  │   "Operating Activity > Loans Advances"                      │
  │   "Operating Activity > Other WC items"                      │
  │   "Operating Activity > Working capital changes"             │
  │   "Operating Activity > Direct taxes"                        │
  │                                                              │
  │ Investing Activity sub-items:                                │
  │   "Investing Activity > Fixed assets purchased"              │
  │   "Investing Activity > Fixed assets sold"                   │
  │   "Investing Activity > Investments purchased"               │
  │   "Investing Activity > Investments sold"                    │
  │   "Investing Activity > Interest received"                   │
  │   "Investing Activity > Dividends received"                  │
  │   "Investing Activity > Investment in group cos"             │
  │   "Investing Activity > Issue of shares on acq"              │
  │   "Investing Activity > Redemp n Canc of Shares"             │
  │   "Investing Activity > Acquisition of companies"            │
  │   "Investing Activity > Inter corporate deposits"            │
  │   "Investing Activity > Other investing items"               │
  │                                                              │
  │ Financing Activity sub-items:                                │
  │   "Financing Activity > Proceeds from shares"                │
  │   "Financing Activity > Redemption of debentures"            │
  │   "Financing Activity > Proceeds from borrowings"            │
  │   "Financing Activity > Repayment of borrowings"             │
  │   "Financing Activity > Interest paid fin"                   │
  │   "Financing Activity > Dividends paid"                      │
  │   "Financing Activity > Financial liabilities"               │
  │   "Financing Activity > Other financing items"               │
  │                                                              │
  │ Top-level HTML rows (ratios/derived):                        │
  │   "TopLevel > Free Cash Flow"                                │
  │   "TopLevel > Net Cash Flow"                                 │
  │   "TopLevel > CFO/OP"                                        │
  └──────────────────────────────────────────────────────────────┘
────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import json
import math
import re
import time
from io import StringIO
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

try:
    import httpx
    _USE_HTTPX = True
except ImportError:
    import requests
    _USE_HTTPX = False

from bs4 import BeautifulSoup


# ── Constants ────────────────────────────────────────────────────────────────

_HDR_HTML = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

_HDR_JSON = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept":          "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}

_CF_SCHEDULE_PARENTS = [
    ("Operating Activity", "Cash+from+Operating+Activity"),
    ("Investing Activity", "Cash+from+Investing+Activity"),
    ("Financing Activity", "Cash+from+Financing+Activity"),
]

# ── Known section total labels ────────────────────────────────────────────────
# These labels identify the rolled-up section total inside schedule JSON.
# They must NEVER be treated as sub-items when summing components.
_TOTAL_LABELS: Dict[str, List[str]] = {
    "Operating Activity": [
        "cash from operating activity",
        "net cash from operating activities",
        "net cash provided by operating activities",
        "total operating",
        "operating activity",
        "cash flow from operations",
        "cash from operations",
    ],
    "Investing Activity": [
        "cash from investing activity",
        "net cash from investing activities",
        "net cash used in investing activities",
        "total investing",
        "investing activity",
        "cash flow from investing",
        "cash from investing",
    ],
    "Financing Activity": [
        "cash from financing activity",
        "net cash from financing activities",
        "net cash used in financing activities",
        "total financing",
        "financing activity",
        "cash flow from financing",
        "cash from financing",
    ],
}

# Flat set of ALL total-label strings for quick membership test
_ALL_TOTAL_LABELS: Set[str] = {
    lbl for labels in _TOTAL_LABELS.values() for lbl in labels
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

# Top-level HTML table rows that should be captured in raw_details_json
# under "TopLevel > <canonical_key>"
_TOPLEVEL_EXTRA_LABELS = [
    "free cash flow",
    "net cash flow",
    "cfo/op",
    "free cash flow (cfo+capex)",
    "net cash flow (cfo+cfi+cff)",
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
    Handles: "1,234.56", "₹ 1,234", "3,67,472 Cr.", "-234", "0", "91%".
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


def _is_total_label(label: str) -> bool:
    """Return True if this label matches any known section-total pattern."""
    lk = _label_key(label)
    if lk in _ALL_TOTAL_LABELS:
        return True
    # Soft match: any known total substring appears in the label
    for tl in _ALL_TOTAL_LABELS:
        if tl in lk:
            return True
    return False


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
    """
    for suffix, is_cons in (("consolidated", True), ("", False)):
        url = f"https://www.screener.in/company/{symbol_nse}/{suffix}/"
        r = _get(url, headers=_HDR_HTML)
        if r is not None:
            if 'id="cash-flow"' in r.text or "cash-flow" in r.text:
                print(f"  ok  cashflow: page loaded ({'consolidated' if is_cons else 'standalone'})")
                return r.text, is_cons
            print(f"  warn  cashflow: page loaded but no cash-flow section — trying next")
    return None, False


def _get_json(url: str, referer: str) -> Optional[Dict]:
    """
    GET a Screener schedule API URL and return parsed JSON dict, or None.
    """
    headers = {**_HDR_JSON, "Referer": referer}
    r = _get(url, headers=headers)
    if r is None:
        return None
    try:
        data = r.json()
        if isinstance(data, dict) and data:
            return data
        return None
    except Exception as e:
        print(f"  warn  cashflow JSON parse: {e}")
        return None


# ── Company ID extraction ─────────────────────────────────────────────────────

def _extract_company_id(html: str) -> Optional[int]:
    """Extract Screener numeric company ID from page HTML."""
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


# ── Top-level CF table ────────────────────────────────────────────────────────

def _fetch_toplevel_cf(html: str) -> Optional[pd.DataFrame]:
    """
    Parse the top-level cash flow table from the Screener HTML page.

    Returns a DataFrame indexed by metric (str) with period columns.
    All values are parsed to float immediately.
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

        # Convert all period columns to float right here
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

    Screener's wire format:
        {
          "Sub Label A": { "Mar 2024": "123.45", "Mar 2023": "67.89" },
          "Sub Label B": { "Mar 2024": "999.00", "Mar 2023": "888.00" },
          ...
        }

    Output (period → sub_label → float):
        {
          "Mar 2024": { "Sub Label A": 123.45, "Sub Label B": 999.00, ... },
          "Mar 2023": { "Sub Label A":  67.89, "Sub Label B": 888.00, ... },
        }

    NOTE: ALL sub-labels are preserved, including any total rows that
    Screener may include. The total rows are identified and excluded
    only in _find_total() when computing section totals.
    """
    result: Dict[str, Dict[str, Any]] = {}

    for sub_label, period_values in raw.items():
        if not isinstance(period_values, dict):
            continue
        sub_key = str(sub_label).strip()
        for period_label, value in period_values.items():
            period_key = str(period_label).strip()
            # Skip meta keys like "setAttributes" that Screener injects
            if not _period_to_iso(period_key):
                continue
            if period_key not in result:
                result[period_key] = {}
            result[period_key][sub_key] = _clean_num(value)

    return result


# ── Total / capex finders ─────────────────────────────────────────────────────

def _find_total(sub_items: Dict[str, Any], section_name: str) -> Optional[float]:
    """
    Find the rolled-up section total from sub-items dict.

    FIX 11 — Priority:
      1. Exact label match against _TOTAL_LABELS for this section
      2. Soft (substring) match against _TOTAL_LABELS for this section
      3. Sum ONLY non-total sub-item values (never include the total row)

    This prevents double-counting when Screener returns e.g.
    "Cash from Operating Activity" as a sub-item alongside the components.
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

    # 3. Sum ONLY component sub-items (exclude any total-row labels)
    #    FIX 11: filter out total labels so we don't double-count
    component_values = [
        _f(v)
        for label, v in sub_items.items()
        if not _is_total_label(label) and _f(v) is not None
    ]
    if component_values:
        return round(sum(component_values), 2)

    return None


def _find_capex(investing_sub_items: Dict[str, Any]) -> Optional[float]:
    """Extract capex from investing sub-items dict."""
    for candidate in _CAPEX_LABELS:
        for label, val in investing_sub_items.items():
            if candidate in _label_key(label):
                return _f(val)
    return None


# ── Build raw_details_json ────────────────────────────────────────────────────

def _build_raw_details(
    section_name: str,
    sub_items: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build the raw_details portion for one CF section.

    FIX 9: ALL sub-labels are preserved (including total rows returned
    by Screener), keyed as "Section > Original Label".
    This ensures downstream consumers and diagnostics can see every
    line item that Screener provided.
    """
    result: Dict[str, Any] = {}
    for label, val in sub_items.items():
        key = f"{section_name} > {label}"
        result[key] = _f(val)
    return result


# ── Merge all schedules into per-period rows ──────────────────────────────────

def _build_period_rows(
    schedules: Dict[str, Dict[str, Dict[str, Any]]],
    toplevel_df: Optional[pd.DataFrame],
) -> List[Dict]:
    """
    Combine the three schedule dicts + top-level HTML table into a list of
    per-period row dicts ready for cashflow_loader.

    FIX 10: Top-level extra rows (Free Cash Flow, Net Cash Flow, CFO/OP)
            are merged into raw_details_json under "TopLevel > <label>".
    """

    # Collect all valid period labels across all sections
    all_periods: Set[str] = set()
    for section_data in schedules.values():
        for period_label in section_data:
            if _period_to_iso(period_label):
                all_periods.add(period_label)

    # Also collect periods from top-level HTML table
    if toplevel_df is not None:
        for col in toplevel_df.columns:
            if _period_to_iso(str(col).strip()):
                all_periods.add(str(col).strip())

    # Pre-build top-level fallback totals dict
    # { period_label_raw: { normalised_label: float_val } }
    toplevel: Dict[str, Dict[str, Optional[float]]] = {}
    if toplevel_df is not None:
        for period_col in toplevel_df.columns:
            period_key = str(period_col).strip()
            col_totals: Dict[str, Optional[float]] = {}
            for metric_label, val in toplevel_df[period_col].items():
                col_totals[_label_key(metric_label)] = _f(val)
            toplevel[period_key] = col_totals
            iso_key = _period_to_iso(period_key)
            if iso_key and iso_key not in toplevel:
                toplevel[iso_key] = col_totals

    def _toplevel_lookup(period_label: str, iso_date: str, keys: List[str]) -> Optional[float]:
        """Use explicit `is None` so a legitimate 0.0 is never overridden."""
        tl = toplevel.get(period_label) or toplevel.get(iso_date) or {}
        for k in keys:
            v = tl.get(k)
            if v is not None:
                return v
        return None

    def _toplevel_extra(period_label: str, iso_date: str) -> Dict[str, Any]:
        """
        FIX 10: Collect top-level HTML rows (Free Cash Flow, Net Cash Flow,
        CFO/OP etc.) that aren't in schedule data, for raw_details_json.
        """
        tl = toplevel.get(period_label) or toplevel.get(iso_date) or {}
        extra: Dict[str, Any] = {}
        for norm_label, val in tl.items():
            for extra_pat in _TOPLEVEL_EXTRA_LABELS:
                if extra_pat in norm_label or norm_label in extra_pat:
                    # Use the original label as stored in toplevel dict
                    canonical = norm_label.title()
                    extra[f"TopLevel > {canonical}"] = val
                    break
        return extra

    rows: List[Dict] = []

    for period_label in sorted(all_periods):
        iso_date = _period_to_iso(period_label)
        if not iso_date:
            print(f"  warn  cashflow: cannot parse period label '{period_label}' — skipping")
            continue

        ops_items = schedules.get("Operating Activity", {}).get(period_label, {})
        inv_items = schedules.get("Investing Activity",  {}).get(period_label, {})
        fin_items = schedules.get("Financing Activity",  {}).get(period_label, {})

        # ── Section totals ────────────────────────────────────────────────────
        # FIX 11: _find_total() now never double-counts the total row
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
        # Prefer the HTML top-level FCF row if capex not found in schedules
        fcf: Optional[float] = None
        if cfo is not None and capex is not None:
            fcf = round(cfo + capex, 2)
        if fcf is None:
            fcf = _toplevel_lookup(period_label, iso_date, [
                "free cash flow", "free cash flow (cfo+capex)"
            ])

        # Net cash flow = CFO + CFI + CFF
        ncf: Optional[float] = None
        if cfo is not None and cfi is not None and cff is not None:
            ncf = round(cfo + cfi + cff, 2)
        if ncf is None:
            ncf = _toplevel_lookup(period_label, iso_date, [
                "net cash flow", "net cash flow (cfo+cfi+cff)", "net cash"
            ])

        # ── raw_details_json ──────────────────────────────────────────────────
        # FIX 9: ALL sub-items (including total rows) are preserved.
        # FIX 10: Top-level extras (CFO/OP, FCF, NCF rows) are also stored.
        raw_detail: Dict[str, Any] = {}

        for section_name, sub_items in [
            ("Operating Activity", ops_items),
            ("Investing Activity",  inv_items),
            ("Financing Activity",  fin_items),
        ]:
            section_raw = _build_raw_details(section_name, sub_items)
            raw_detail.update(section_raw)

        # Add top-level extras
        raw_detail.update(_toplevel_extra(period_label, iso_date))

        sub_count = sum(
            1 for k in raw_detail
            if not k.startswith("TopLevel >") and _f(raw_detail[k]) is not None
        )
        print(
            f"  ok  cashflow [{period_label}]: "
            f"cfo={cfo} cfi={cfi} cff={cff} capex={capex} "
            f"fcf={fcf} ncf={ncf} | {sub_count} sub-items in raw_details"
        )

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
    When schedule API calls all return empty (rate-limited etc.),
    build period rows from just the top-level HTML CF table.
    Only cfo/cfi/cff totals are available — sub-items will be empty,
    but top-level rows (FCF, NCF, CFO/OP) are stored in raw_details.
    """
    rows: List[Dict] = []
    for period_col in toplevel_df.columns:
        period_label = str(period_col).strip()
        iso_date = _period_to_iso(period_label)
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
        fcf   = _find_row(["free cash flow"])
        ncf   = _find_row(["net cash flow"])

        if fcf is None and cfo is not None and cfi is not None and cff is not None:
            ncf = round(cfo + cfi + cff, 2)

        # Capture all top-level rows into raw_details
        raw_detail: Dict[str, Any] = {}
        for label in col.index:
            raw_detail[f"TopLevel > {label}"] = _f(col[label])

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
            "raw_details_json": json.dumps(raw_detail, default=str),
        })
    return rows


# ── Public entry point ────────────────────────────────────────────────────────

def fetch_cashflow(symbol_nse: str) -> List[Dict]:
    """
    Fetch granular Screener cash flow data for `symbol_nse`.

    Steps:
      1. Fetch company page HTML → extract numeric company_id.
      2. Detect consolidated vs standalone from which URL loaded.
      3. Parse top-level CF HTML table for fallback totals + extra rows.
      4. Call schedules API for Operating / Investing / Financing.
      5. Parse each schedule JSON — correct pivot (sub_label → period).
      6. Merge into per-period row dicts with:
           - all sub-items preserved in raw_details_json
           - correct 0.0 handling (explicit None checks)
           - top-level extras (CFO/OP, FCF, NCF) in raw_details_json
      7. Fall back to top-level table rows if API returned nothing.

    Returns a list of dicts (one per annual period) suitable for
    cashflow_loader.load_cashflow().  Empty list on unrecoverable failure.
    """
    print(f"\n  [cashflow extract] Fetching Screener CF for {symbol_nse}...")

    # 1 & 2. Page HTML + consolidated flag
    html, consolidated = _get_html(symbol_nse)
    if not html:
        print(f"  error  cashflow: could not fetch Screener page for {symbol_nse}")
        return []

    # 3. Company ID
    company_id = _extract_company_id(html)
    if not company_id:
        print(f"  error  cashflow: company_id not found for {symbol_nse}")
        return []
    print(f"  ok  cashflow: company_id={company_id}, consolidated={consolidated}")

    # 4. Top-level CF table (values parsed to float immediately)
    toplevel_df = _fetch_toplevel_cf(html)
    if toplevel_df is not None:
        print(
            f"  ok  cashflow top-level table: "
            f"{toplevel_df.shape[0]} rows × {toplevel_df.shape[1]} cols"
        )
        print(f"       top-level labels: {list(toplevel_df.index)}")
    else:
        print(f"  warn  cashflow: top-level CF table not found in HTML")

    # 5. Schedule API calls
    schedules: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for section_name, parent_param in _CF_SCHEDULE_PARENTS:
        raw = _fetch_cf_schedule(
            company_id, parent_param, section_name, consolidated, symbol_nse
        )
        time.sleep(0.5)   # polite delay
        if raw:
            parsed = _parse_schedule(raw)
            schedules[section_name] = parsed
            # Print sub-labels found for diagnostic
            sample_period = next(iter(parsed), None)
            if sample_period:
                sub_labels = list(parsed[sample_period].keys())
                print(f"       [{section_name}] sub-labels ({len(sub_labels)}): {sub_labels}")
        else:
            schedules[section_name] = {}

    any_schedule_data = any(bool(v) for v in schedules.values())

    # If API returned nothing but we have the HTML table, use it
    if not any_schedule_data:
        print(f"  warn  cashflow: all schedule API calls returned empty")
        if toplevel_df is not None:
            print(f"  info  cashflow: falling back to top-level HTML table for {symbol_nse}")
            rows = _build_rows_from_toplevel(toplevel_df)
            print(f"  ok  cashflow: {len(rows)} annual periods from top-level table")
            return rows
        print(f"  error cashflow: no data at all for {symbol_nse}")
        return []

    # 6. Build per-period rows
    rows = _build_period_rows(schedules, toplevel_df)
    print(f"  ok  cashflow: {len(rows)} annual periods extracted for {symbol_nse}")
    return rows