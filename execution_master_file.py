from execution_functions import get_all_symbols
import duckdb
from IBKR_execution import main_execution

DB_PATH = "stocks_data.db"
CLIENT_ID = 2001   # pick a unique client id for this engine

# Load symbols (DB only used for symbol list)
with duckdb.connect(DB_PATH, read_only=True) as con:
    symbols = get_all_symbols(con)

# State-driven execution (single IB session)
main_execution(
    client_id=CLIENT_ID,
    symbols=symbols,
)
