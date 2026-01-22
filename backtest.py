import duckdb
from backtest_functions import backtest_returns_stock_5m
con = duckdb.connect(
    "/home/ubuntu/supreme-stockequity-trading-bot/stocks_data.db",
    read_only=True,
)



df = backtest_returns_stock_5m(
    con,
)

print(len(df))
print(df.head())
