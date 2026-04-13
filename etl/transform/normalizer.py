def normalize_price(df, symbol):
    df["symbol"] = symbol.replace(".NS", "")
    return df[["symbol", "date", "open", "high", "low", "close", "volume"]]