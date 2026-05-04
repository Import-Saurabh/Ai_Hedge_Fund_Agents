"""
etl/extract/cashflow_scrapper.py  v2.0
────────────────────────────────────────────────────────────────
Changes vs v1.0:
  • Removed hardcoded company ID (was /api/company/57/).
    Now dynamically extracts company_id from the Screener HTML
    page for ANY stock symbol — same approach as cashflow.py.
  • Removed hardcoded Referer header pointing to ADANIPORTS.
  • Output CSV renamed to {symbol}_cashflow_breakdown.csv.
  • Works standalone (python cashflow_scrapper.py RELIANCE) or
    imported as a utility.
────────────────────────────────────────────────────────────────
"""

import re
import os
import time
import requests
import pandas as pd
from typing import Optional


class ScreenerCashFlowScraper:

    _HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
    }

    _CF_PARENTS = [
        {"name": "Operating Activity", "parent": "Cash+from+Operating+Activity"},
        {"name": "Investing Activity", "parent": "Cash+from+Investing+Activity"},
        {"name": "Financing Activity", "parent": "Cash+from+Financing+Activity"},
    ]

    def __init__(self, symbol_nse: str):
        """
        Parameters
        ----------
        symbol_nse : str
            NSE ticker without exchange suffix, e.g. "RELIANCE", "ADANIPORTS".
        """
        self.symbol = symbol_nse.upper().strip()
        self.session = requests.Session()
        self.session.headers.update(self._HEADERS)
        # Set a dynamic Referer based on the actual symbol
        self.session.headers["Referer"] = (
            f"https://www.screener.in/company/{self.symbol}/consolidated/"
        )

    # ── Helpers ──────────────────────────────────────────────────────────────

    @staticmethod
    def get_safe_path(filename: str) -> str:
        """Return filename unchanged if free, else append _NEW before extension."""
        if not os.path.exists(filename):
            return filename
        try:
            os.rename(filename, filename)
            return filename
        except OSError:
            name, ext = os.path.splitext(filename)
            return f"{name}_NEW{ext}"

    # ── Step 1: fetch company page HTML ──────────────────────────────────────

    def _fetch_html(self) -> Optional[str]:
        """Fetch Screener company page; tries consolidated first, then standalone."""
        for suffix in ("consolidated", ""):
            url = f"https://www.screener.in/company/{self.symbol}/{suffix}/"
            try:
                r = self.session.get(url, timeout=20)
                if r.status_code == 200:
                    print(f"  Fetched page: {url}")
                    return r.text
                print(f"  HTTP {r.status_code} for {url}")
            except Exception as e:
                print(f"  Request error: {e}")
            time.sleep(1.0)
        return None

    # ── Step 2: extract numeric company ID from HTML ──────────────────────────

    @staticmethod
    def _extract_company_id(html: str) -> Optional[int]:
        """
        Dynamically extract Screener's numeric company ID from page HTML.
        Tries several patterns for robustness (mirrors cashflow.py logic).
        """
        for pat in [
            r'data-company-id=["\'](\d+)["\']',
            r'(?:var\s+|let\s+|const\s+)company_id\s*=\s*(\d+)',
            r'/api/company/(\d+)/',
        ]:
            m = re.search(pat, html)
            if m:
                return int(m.group(1))

        # BeautifulSoup fallback (only imported if needed to keep deps minimal)
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "lxml")
            for tag in ["div", "section", "main", "article"]:
                el = soup.find(tag, attrs={"data-company-id": True})
                if el:
                    return int(el["data-company-id"])
            for el in soup.find_all(attrs={"data-id": True}):
                try:
                    val = int(el["data-id"])
                    if val > 0:
                        return val
                except Exception:
                    pass
            for script in soup.find_all("script"):
                for pat in [r'"company"[^}]*?"id"\s*:\s*(\d+)',
                            r'companyId["\s:=]+(\d+)']:
                    m = re.search(pat, script.get_text())
                    if m:
                        return int(m.group(1))
        except ImportError:
            pass

        print("  warn: could not extract company_id from HTML")
        return None

    # ── Step 3: fetch schedule for one CF section ─────────────────────────────

    def _fetch_schedule(
        self,
        company_id: int,
        parent_param: str,
        section_name: str,
        consolidated: bool,
    ) -> Optional[dict]:
        cons = "" if consolidated else "false"
        url = (
            f"https://www.screener.in/api/company/{company_id}/schedules/"
            f"?parent={parent_param}&section=cash-flow&consolidated={cons}"
        )
        print(f"  Fetching [{section_name}]: {url}")
        try:
            r = self.session.get(url, timeout=20)
            if r.status_code == 200:
                data = r.json()
                if data and isinstance(data, dict):
                    print(f"    → {len(data)} period columns")
                    return data
                print(f"    → empty response (check Screener login/session)")
            else:
                print(f"    → HTTP {r.status_code}")
        except Exception as e:
            print(f"    → error: {e}")
        return None

    # ── Main method ───────────────────────────────────────────────────────────

    def fetch_cashflow_schedules(self) -> Optional[pd.DataFrame]:
        """
        Fetch all three CF schedule sections for self.symbol, combine into
        a flat DataFrame, and save to CSV.

        Returns the combined DataFrame (or None if nothing was retrieved).
        """

        # 1. Page HTML
        html = self._fetch_html()
        if not html:
            print(f"  ERROR: could not fetch Screener page for {self.symbol}")
            return None

        # 2. Company ID
        company_id = self._extract_company_id(html)
        if not company_id:
            print(f"  ERROR: company_id not found for {self.symbol}")
            return None
        print(f"  company_id={company_id}")

        # 3. Consolidated flag
        consolidated = "consolidated" in html[:5000].lower()
        print(f"  consolidated={consolidated}")

        # 4. Fetch the three sections
        all_dfs = []

        for item in self._CF_PARENTS:
            raw = self._fetch_schedule(
                company_id, item["parent"], item["name"], consolidated
            )
            time.sleep(0.5)   # polite delay

            if not raw:
                continue

            # raw = { "Mar 2024": { "Sub Label": "123.45", ... }, ... }
            df = pd.DataFrame(raw).transpose()
            df.index.name = "Sub-Category"
            df.reset_index(inplace=True)
            df.insert(0, "Parent_Category", item["name"])
            all_dfs.append(df)
            print(f"    → extracted {len(df)} sub-rows for {item['name']}")

        if not all_dfs:
            print("  No data retrieved — check session cookies / availability.")
            return None

        final_df = pd.concat(all_dfs, ignore_index=True)

        # 5. Clean numeric columns (period columns like "Mar 2024")
        for col in final_df.columns:
            if any(x in str(col) for x in ["Mar", "Sep", "Jun", "Dec", "20"]):
                final_df[col] = (
                    final_df[col]
                    .astype(str)
                    .str.replace(",", "", regex=False)
                    .replace("nan", "0")
                )
                final_df[col] = pd.to_numeric(final_df[col], errors="coerce").fillna(0)

        # 6. Save
        out_path = self.get_safe_path(f"{self.symbol}_cashflow_breakdown.csv")
        final_df.to_csv(out_path, index=False)
        print(f"\n--- SUCCESS ---")
        print(f"  Symbol      : {self.symbol}")
        print(f"  Company ID  : {company_id}")
        print(f"  Rows        : {len(final_df)}")
        print(f"  Saved to    : {out_path}")

        return final_df


if __name__ == "__main__":
    import sys
    sym = sys.argv[1] if len(sys.argv) > 1 else "ADANIPORTS"
    scraper = ScreenerCashFlowScraper(sym)
    scraper.fetch_cashflow_schedules()