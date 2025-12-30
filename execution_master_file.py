from execution_functions import get_all_symbols
import duckdb
from execution import run_equity_bot_for_symbol

con = duckdb.connect("stocks_data.db")


symbols = get_all_symbols(con)


for symbol in symbols:
    run_equity_bot_for_symbol(symbol)




con.close()