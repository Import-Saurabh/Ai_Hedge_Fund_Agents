import yfinance as yf

def fetch_statements(symbol: str):
    """
    Fetch financial statements (annual + quarterly)
    """

    ticker = yf.Ticker(symbol)

    return {
        "annual_income": ticker.income_stmt,
        "annual_bs": ticker.balance_sheet,
        "annual_cf": ticker.cash_flow,

        "quarter_income": ticker.quarterly_income_stmt,
        "quarter_bs": ticker.quarterly_balance_sheet,
        "quarter_cf": ticker.quarterly_cash_flow
    }