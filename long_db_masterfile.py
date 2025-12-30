from stock_longdb import ingest_stock_bar_5m

import pandas as pd

def get_sp500_symbols():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    tables = pd.read_html(url)
    df = tables[0]
    return df["Symbol"].tolist()

symbols = get_sp500_symbols()


for symbol in symbols:
    ingest_stock_bar_5m(symbol)

