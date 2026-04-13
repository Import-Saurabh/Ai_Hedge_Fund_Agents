def melt_financials(df, symbol):
    records = []

    if df is None or df.empty:
        return records

    for metric in df.index:
        for date in df.columns:
            value = df.loc[metric, date]

            records.append((
                symbol,
                str(date)[:10],
                metric,
                float(value) if value is not None else None
            ))

    return records