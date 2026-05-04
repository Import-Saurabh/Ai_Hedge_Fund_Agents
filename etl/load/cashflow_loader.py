"""
etl/load/cashflow_loader.py  v6.0
────────────────────────────────────────────────────────────────
Changes vs v5.0:
  • Sub-category line items from Screener schedules are now fully
    preserved in raw_details_json (every "Section > Sub Label" key).
  • _ensure_cashflow_cols() runs at startup to defensively add any
    columns that may be missing from older DB schemas (idempotent).
  • load_cashflow() accepts BOTH:
      – list[dict]  (from etl/extract/cashflow.py — existing path)
      – pd.DataFrame (from cashflow_scrapper.py / screener_loader)
    so it works regardless of caller.
  • No yfinance dependency anywhere in this file.
  • ON CONFLICT strategy unchanged:
      – Core numerics use COALESCE (Screener value never overwritten
        by NULL).
      – raw_details_json is always replaced (latest fetch wins, so
        newly scraped sub-items are always current).
      – completeness_pct / missing_fields_json are recomputed on
        every upsert.
────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import json
import math
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from database.db import get_connection


# ── Helpers ───────────────────────────────────────────────────────────────────

def _f(v) -> Optional[float]:
    """Safe float — returns None for NaN / Inf / None / unparseable."""
    if v is None:
        return None
    try:
        fv = float(v)
        return None if (math.isnan(fv) or math.isinf(fv)) else fv
    except (TypeError, ValueError):
        return None


def _json_or_none(obj: Any) -> Optional[str]:
    """Serialise to JSON string, or return None if obj is empty/None."""
    if obj is None:
        return None
    if isinstance(obj, str):
        return obj if obj.strip() not in ("", "{}", "[]", "null") else None
    try:
        s = json.dumps(obj, default=str)
        return s if s not in ("{}", "[]", "null") else None
    except Exception:
        return None


def _merge_raw_details(existing_json: Optional[str], new_obj: Any) -> Optional[str]:
    """
    Merge existing raw_details_json with new sub-item data.
    New keys overwrite old; old keys not in new are preserved.
    This way each load adds more detail rather than discarding prior data.
    """
    existing: Dict = {}
    if existing_json:
        try:
            existing = json.loads(existing_json)
        except Exception:
            existing = {}

    new: Dict = {}
    if new_obj:
        if isinstance(new_obj, str):
            try:
                new = json.loads(new_obj)
            except Exception:
                new = {}
        elif isinstance(new_obj, dict):
            new = new_obj

    merged = {**existing, **new}   # new keys win
    return _json_or_none(merged)


def _completeness(fields: Dict[str, Any]) -> tuple[float, List[str]]:
    """Return (completeness_pct, missing_field_names)."""
    if not fields:
        return 100.0, []
    missing = [k for k, v in fields.items() if v is None]
    pct = round((1 - len(missing) / len(fields)) * 100, 1)
    return pct, missing


# ── Schema migration ──────────────────────────────────────────────────────────

def _ensure_cashflow_cols(conn) -> None:
    """
    Idempotently add any columns that might be absent from older DB schemas.
    ALTER TABLE on an existing column raises an exception which we silently ignore.
    """
    extras = [
        ("completeness_pct",    "REAL"),
        ("missing_fields_json", "TEXT"),
    ]
    for col_name, col_type in extras:
        try:
            conn.execute(
                f"ALTER TABLE cash_flow ADD COLUMN {col_name} {col_type}"
            )
            print(f"  db-migrate cash_flow: added column '{col_name}'")
        except Exception:
            pass   # already exists


# ── DataFrame → list[dict] normaliser ────────────────────────────────────────

def _df_to_records(df: pd.DataFrame, symbol: str) -> List[Dict]:
    """
    Convert a long-format DataFrame produced by cashflow_scrapper.py
    (columns: Parent_Category, Sub-Category, <period cols>...)
    into the same list[dict] format expected by the core upsert loop.

    Each period column becomes one record. Sub-items for that period
    across all Parent_Category rows are gathered into raw_details_json.
    """
    # Identify period columns (everything that isn't category labels)
    non_period = {"Parent_Category", "Sub-Category"}
    period_cols = [c for c in df.columns if c not in non_period]

    if not period_cols:
        print(f"  warn  cashflow_loader ({symbol}): DataFrame has no period columns")
        return []

    from etl.extract.cashflow import _period_to_iso   # reuse period parser

    records: List[Dict] = []

    for period_col in period_cols:
        iso_date = _period_to_iso(period_col)
        if not iso_date:
            print(f"  warn  cashflow_loader: cannot parse period '{period_col}' — skip")
            continue

        # Build raw_details dict: "Section > Sub Label" → value
        raw_detail: Dict[str, Any] = {}
        for _, row in df.iterrows():
            parent   = str(row.get("Parent_Category", "")).strip()
            sub      = str(row.get("Sub-Category",    "")).strip()
            val      = _f(row.get(period_col))
            composite_key = f"{parent} > {sub}"
            raw_detail[composite_key] = val

        records.append({
            "period_end":       iso_date,
            "period_type":      "annual",
            "cfo":              None,   # will be derived below
            "cfi":              None,
            "cff":              None,
            "capex":            None,
            "free_cash_flow":   None,
            "net_cash_flow":    None,
            "data_source":      "screener",
            "raw_details_json": raw_detail,
            "_df_source":       True,   # flag so totals are computed from raw
        })

    # For DataFrame-sourced records, try to pull totals from the sub-items
    _TOTAL_LABELS = {
        "Operating Activity": [
            "cash from operating activity",
            "net cash from operating activities",
            "net cash provided by operating activities",
        ],
        "Investing Activity": [
            "cash from investing activity",
            "net cash from investing activities",
            "net cash used in investing activities",
        ],
        "Financing Activity": [
            "cash from financing activity",
            "net cash from financing activities",
            "net cash used in financing activities",
        ],
    }
    _CAPEX_LABELS = [
        "purchase of fixed assets",
        "purchase of property plant and equipment",
        "capital expenditure",
        "capex",
        "additions to fixed assets",
    ]

    for rec in records:
        rd = rec["raw_details_json"]
        if not isinstance(rd, dict):
            continue

        def _find(section: str, candidates: List[str]) -> Optional[float]:
            prefix = section + " > "
            for k, v in rd.items():
                if k.startswith(prefix):
                    label_lower = k[len(prefix):].lower().strip()
                    for cand in candidates:
                        if cand in label_lower:
                            return _f(v)
            return None

        cfo   = _find("Operating Activity", _TOTAL_LABELS["Operating Activity"])
        cfi   = _find("Investing Activity",  _TOTAL_LABELS["Investing Activity"])
        cff   = _find("Financing Activity",  _TOTAL_LABELS["Financing Activity"])
        capex = _find("Investing Activity",  _CAPEX_LABELS)

        fcf: Optional[float] = None
        if cfo is not None and capex is not None:
            fcf = round(cfo + capex, 2)

        ncf: Optional[float] = None
        if cfo is not None and cfi is not None and cff is not None:
            ncf = round(cfo + cfi + cff, 2)

        rec.update(cfo=cfo, cfi=cfi, cff=cff, capex=capex,
                   free_cash_flow=fcf, net_cash_flow=ncf)
        rec["raw_details_json"] = _json_or_none(rd)

    return records


# ── Core upsert ───────────────────────────────────────────────────────────────

def load_cashflow(
    records: Union[List[Dict], pd.DataFrame],
    symbol: str,
) -> None:
    """
    Upsert cash flow data into the cash_flow table.

    Parameters
    ----------
    records : list[dict]  OR  pd.DataFrame
        • list[dict]  — output of etl.extract.cashflow.fetch_cashflow()
          Each dict must have at minimum: period_end, period_type.
        • pd.DataFrame — long-format output of cashflow_scrapper.py
          (columns: Parent_Category, Sub-Category, <period cols>...)
          The function normalises this automatically.

    symbol : str
        NSE ticker without exchange suffix (e.g. "ADANIPORTS").
    """
    # ── Normalise DataFrame input ─────────────────────────────────────────────
    if isinstance(records, pd.DataFrame):
        records = _df_to_records(records, symbol)

    if not records:
        print(f"  warn  cashflow_loader ({symbol}): no records — skipping")
        return

    conn  = get_connection()
    _ensure_cashflow_cols(conn)

    count   = 0
    skipped = 0

    for rec in records:
        period_end  = rec.get("period_end")
        period_type = rec.get("period_type", "annual")

        if not period_end:
            skipped += 1
            continue

        cfo   = _f(rec.get("cfo"))
        cfi   = _f(rec.get("cfi"))
        cff   = _f(rec.get("cff"))
        capex = _f(rec.get("capex"))
        fcf   = _f(rec.get("free_cash_flow"))
        ncf   = _f(rec.get("net_cash_flow"))

        # Derive FCF if not supplied but CFO + capex are available
        if fcf is None and cfo is not None and capex is not None:
            fcf = round(cfo + capex, 2)

        # Derive net cash flow if all three sections are available
        if ncf is None and cfo is not None and cfi is not None and cff is not None:
            ncf = round(cfo + cfi + cff, 2)

        # ── raw_details_json: merge with any existing DB value ────────────────
        # Fetch current value so we can merge (add sub-items, never lose them)
        existing_row = conn.execute(
            "SELECT raw_details_json FROM cash_flow "
            "WHERE symbol=? AND period_end=? AND period_type=?",
            (symbol, period_end, period_type),
        ).fetchone()
        existing_raw = existing_row[0] if existing_row else None
        new_raw      = rec.get("raw_details_json")
        merged_raw   = _merge_raw_details(existing_raw, new_raw)

        data_source = rec.get("data_source", "screener")

        # ── Completeness ──────────────────────────────────────────────────────
        core_fields = {
            "cfo":            cfo,
            "cfi":            cfi,
            "cff":            cff,
            "capex":          capex,
            "free_cash_flow": fcf,
            "net_cash_flow":  ncf,
        }
        comp_pct, missing_fields = _completeness(core_fields)

        conn.execute("""
            INSERT INTO cash_flow (
                symbol, period_end, period_type,
                cfo, cfi, cff,
                capex, free_cash_flow, net_cash_flow,
                raw_details_json, data_source,
                completeness_pct, missing_fields_json
            ) VALUES (
                ?, ?, ?,
                ?, ?, ?,
                ?, ?, ?,
                ?, ?,
                ?, ?
            )
            ON CONFLICT(symbol, period_end, period_type) DO UPDATE SET
                cfo              = COALESCE(cash_flow.cfo,            excluded.cfo),
                cfi              = COALESCE(cash_flow.cfi,            excluded.cfi),
                cff              = COALESCE(cash_flow.cff,            excluded.cff),
                capex            = COALESCE(cash_flow.capex,          excluded.capex),
                free_cash_flow   = COALESCE(cash_flow.free_cash_flow, excluded.free_cash_flow),
                net_cash_flow    = COALESCE(cash_flow.net_cash_flow,  excluded.net_cash_flow),
                raw_details_json = excluded.raw_details_json,
                data_source      = excluded.data_source,
                completeness_pct    = excluded.completeness_pct,
                missing_fields_json = excluded.missing_fields_json,
                updated_at       = CURRENT_TIMESTAMP
        """, (
            symbol, period_end, period_type,
            cfo, cfi, cff,
            capex, fcf, ncf,
            merged_raw, data_source,
            comp_pct, json.dumps(missing_fields),
        ))
        count += 1

    conn.commit()
    conn.close()

    print(
        f"  ok  cashflow_loader ({symbol}): "
        f"{count} upserted"
        + (f", {skipped} skipped (no period_end)" if skipped else "")
    )