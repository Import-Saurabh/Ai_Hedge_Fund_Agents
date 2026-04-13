import yfinance as yf

def fetch_news(symbol: str):
    t = yf.Ticker(symbol)
    return t.news