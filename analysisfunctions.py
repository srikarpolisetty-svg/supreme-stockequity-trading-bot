def get_latest_stock_snapshot(
    con,
    table: str,
    symbol: str,
):
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


def load_all_symbols(
    con,
    symbols,
    table: str = "stock_bars_enriched_5m",
):
    """
    Stock equivalent of load_all_groups() for options.
    Returns:
        data[symbol] = df_or_None
    """
    data = {}

    for sym in symbols:
        sym = str(sym).upper().strip()

        try:
            df = get_latest_stock_snapshot(con, table, sym)
        except Exception:
            df = None

        if df is not None and df.empty:
            df = None

        data[sym] = df

    return data


def get_stock_metrics(groups, symbol: str):
    """
    Stock equivalent of get_option_metrics(), but returns BOTH 3-day and 35-day z-scores.
    Expects `groups[symbol]` to be a 1-row DF from stock_bars_enriched_5m
    that contains:
      close_z_3d, volume_z_3d, range_z_3d,
      close_z_35d, volume_z_35d, range_z_35d
    """
    symbol = str(symbol).upper().strip()

    df = groups.get(symbol)
    if df is None or df.empty:
        return None

    row = df.iloc[0]

    return {
        "short": {
            "z_price":      row.get("close_z_3d"),
            "z_volume":     row.get("volume_z_3d"),
            "z_volatility": row.get("range_z_3d"),
        },
        "long": {
            "z_price":      row.get("close_z_35d"),
            "z_volume":     row.get("volume_z_35d"),
            "z_volatility": row.get("range_z_35d"),
        },

        "open":         row.get("open"),
        "high":         row.get("high"),
        "low":          row.get("low"),
        "close":        row.get("close"),
        "volume":       row.get("volume"),
        "range_pct":    row.get("range_pct"),

        "symbol":       row.get("symbol"),
        "timestamp":    row.get("timestamp"),
        "snapshot_id":  row.get("snapshot_id"),
        "con_id":       row.get("con_id"),
    }



def update_stock_signal(
    con,
    symbol: str,
    snapshot_id: str,
    signal_column: str,
    table: str = "stock_execution_signals_5m",
):
    """
    Exact analogue of update_signal() for options.
    """
    symbol = str(symbol).upper().strip()

    con.execute(f"""
        UPDATE {table}
        SET {signal_column} = TRUE
        WHERE snapshot_id = ?
          AND symbol = ?;
    """, [snapshot_id, symbol])
