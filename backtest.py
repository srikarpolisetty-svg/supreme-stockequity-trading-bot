import duckdb
import pandas as pd
from backtest_functions import backtest_returns_stock_5m

# show everything
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)

with duckdb.connect(
    "/home/ubuntu/supreme-stockequity-trading-bot/stocks_data.db",
    read_only=True,
) as con:

    df = backtest_returns_stock_5m(con)

    print(len(df))
    print(df)
