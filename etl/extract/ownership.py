import yfinance as yf

def fetch_ownership(symbol: str):
    t = yf.Ticker(symbol)

    return {
        "institutional": t.institutional_holders,
        "mutualfund": t.mutualfund_holders
    }