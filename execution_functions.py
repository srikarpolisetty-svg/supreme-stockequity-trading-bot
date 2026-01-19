import duckdb


def to_float(x, default=0.0):
    try:
        return float(str(x).replace(",", ""))
    except (TypeError, ValueError):
        return default






con = duckdb.connect("stocks_data.db")

def get_all_symbols(con, table="stock_bars_enriched_5m"):
    df = con.execute(
        f"SELECT DISTINCT symbol FROM {table}"
    ).df()
    return df["symbol"].tolist()








