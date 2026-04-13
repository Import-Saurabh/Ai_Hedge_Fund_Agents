import yfinance as yf
import pandas as pd
from typing import Dict

def fetch_corporate_actions(symbol: str) -> dict:
    """Fetch dividends, splits, and all corporate actions."""
    t = yf.Ticker(symbol)
    out = {}

    try:
        divs = t.dividends
        if divs is not None and not divs.empty:
            divs = divs.reset_index()
            divs.columns = ["date", "value"]
            divs["date"] = pd.to_datetime(divs["date"]).dt.date
            out["dividends"] = divs
    except:
        pass

    try:
        splits = t.splits
        if splits is not None and not splits.empty:
            splits = splits.reset_index()
            splits.columns = ["date", "value"]
            splits["date"] = pd.to_datetime(splits["date"]).dt.date
            out["splits"] = splits
    except:
        pass

    return out
