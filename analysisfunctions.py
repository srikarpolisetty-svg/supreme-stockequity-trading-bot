def get_latest_stock_snapshot(con, table: str, symbol: str):
    """
    Grab the latest snapshot row for a given table / symbol.
    Returns a 1-row DataFrame.
    """
    query = f"""
        SELECT *
        FROM {table}
        WHERE symbol = ?
        ORDER BY timestamp DESC
        LIMIT 1
    """
    return con.execute(query, [symbol]).df()








def load_all_symbols(con, symbols):
    tables = {
        "short": "stock_bars_enriched_5m_3d",
        "long":  "stock_bars_enriched_5m",
    }

    data = {}

    for sym in symbols:
        data[sym] = {
            "short": get_latest_stock_snapshot(con, tables["short"], sym),
            "long":  get_latest_stock_snapshot(con, tables["long"],  sym),
        }

    return data






def get_stock_metrics(groups, symbol: str):
    """
    groups: dict from load_all_symbols()
    symbol: e.g. "AAPL"
    """
    short_df = groups[symbol]["short"]
    long_df  = groups[symbol]["long"]

    short_row = short_df.iloc[0]
    long_row  = long_df.iloc[0]

    return {
        "short": {
            "z_price":     short_row["close_z"],
            "z_volume":    short_row["volume_z"],
            "z_volatility": short_row["range_z"],   # range_pct z-score
            "open":        short_row["open"],
            "high":        short_row["high"],
            "low":         short_row["low"],
            "close":       short_row["close"],
            "volume":      short_row["volume"],
            "range_pct":   short_row["range_pct"],
            "symbol":      short_row["symbol"],
            "timestamp":   short_row["timestamp"],
            "snapshot_id": short_row["snapshot_id"],
        },
        "long": {
            "z_price":      long_row["close_z"],
            "z_volume":     long_row["volume_z"],
            "z_volatility": long_row["range_z"],
            "timestamp":    long_row["timestamp"],
            "snapshot_id":  long_row["snapshot_id"],
        }
    }




def update_stock_signal(
    con,
    short_snapshot_id: str,
    long_snapshot_id: str,
    symbol: str,
    signal_column: str = "trade_signal",
):
    # Update short-term table (3-day)
    con.execute(f"""
        UPDATE stock_execution_signals_5m_3d
        SET {signal_column} = TRUE
        WHERE snapshot_id = ?
          AND symbol = ?;
    """, [short_snapshot_id, symbol])

    # Update long-term table
    con.execute(f"""
        UPDATE stock_execution_signals_5m
        SET {signal_column} = TRUE
        WHERE snapshot_id = ?
          AND symbol = ?;
    """, [long_snapshot_id, symbol])
