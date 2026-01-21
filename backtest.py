import duckdb
from backtest_functions import backtest_returns_stock_5m
con = duckdb.connect(
    "/home/ubuntu/supreme-stockequity-trading-bot/stocks_data.db",
    read_only=True,
)

import duckdb
con = duckdb.connect("/home/ubuntu/supreme-stockequity-trading-bot/stocks_data.db", read_only=True)

print(con.execute("PRAGMA table_info('stock_bars_raw_5m')").df()[["cid","name","type"]])
print(con.execute("PRAGMA table_info('stock_bars_enriched_5m')").df()[["cid","name","type"]])
print(con.execute("""
    SELECT timestamp, symbol, open, high, low, close, volume, range_pct, con_id, snapshot_id
    FROM stock_bars_raw_5m
    WHERE symbol='SBAC'
    ORDER BY timestamp DESC
    LIMIT 5
""").df())

print(con.execute("""
    SELECT MIN(close) mn, MAX(close) mx, COUNT(*) n
    FROM stock_bars_raw_5m
    WHERE symbol='SBAC'
""").df())

con.close()


#df = backtest_returns_stock_5m(
    #con,
    #symbol="SBAC",
#)

#print(len(df))
#print(df.head())
