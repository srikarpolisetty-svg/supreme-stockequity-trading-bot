from analysis import run_stock_pressure_signal
import duckdb
from execution_functions import get_all_symbols


con = duckdb.connect("stocks_data.db")


symbols = get_all_symbols(con)


for symbol in symbols:
    run_stock_pressure_signal(symbol)


con.close()

