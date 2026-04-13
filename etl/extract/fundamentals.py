import yfinance as yf

def fetch_fundamentals(symbol: str):
    t = yf.Ticker(symbol)

    return {
        "info": t.info,
        "income_stmt": t.income_stmt,
        "balance_sheet": t.balance_sheet,
        "cash_flow": t.cash_flow
    }