def backtest_returns_stock_5m(
    con,

    # identity
    symbol: str | None = None,

    # numeric filters (min/max)
    price_min: float | None = None,
    price_max: float | None = None,

    volume_min: int | None = None,
    volume_max: int | None = None,

    range_pct_min: float | None = None,
    range_pct_max: float | None = None,

    # z-score filters (3d)
    close_z_3d_min: float | None = None,
    close_z_3d_max: float | None = None,
    volume_z_3d_min: float | None = None,
    volume_z_3d_max: float | None = None,
    range_z_3d_min: float | None = None,
    range_z_3d_max: float | None = None,

    # z-score filters (35d)
    close_z_35d_min: float | None = None,
    close_z_35d_max: float | None = None,
    volume_z_35d_min: float | None = None,
    volume_z_35d_max: float | None = None,
    range_z_35d_min: float | None = None,
    range_z_35d_max: float | None = None,

    # optional categorical-ish filters
    trade_signal: bool | None = None,

    # table override
    table: str = "stock_bars_enriched_5m",
):
    """
    Stock analogue of backtest_returns_5w().

    Returns ONLY the latest timestamp slice (optionally for a single symbol).
    Expects columns in `table` (default stock_bars_enriched_5m):
      timestamp, symbol,
      open, high, low, close, volume, range_pct,
      close_z_3d, volume_z_3d, range_z_3d,
      close_z_35d, volume_z_35d, range_z_35d,
      opt_ret_10m, opt_ret_1h, opt_ret_eod, opt_ret_next_open, opt_ret_1d, opt_ret_2d, opt_ret_3d

    If you want to filter on `trade_signal`, pass table="stock_execution_signals_5m".
    """
    sym = str(symbol).upper().strip() if symbol else None

    query = f"""
        SELECT
            timestamp,
            symbol,
            open,
            high,
            low,
            close,
            volume,
            range_pct,

            close_z_3d,
            volume_z_3d,
            range_z_3d,

            close_z_35d,
            volume_z_35d,
            range_z_35d,

            opt_ret_10m,
            opt_ret_1h,
            opt_ret_eod,
            opt_ret_next_open,
            opt_ret_1d,
            opt_ret_2d,
            opt_ret_3d
        FROM {table}
        WHERE 1=1

          {f"AND symbol = '{sym}'" if sym else ""}

          {f"AND close >= {price_min}" if price_min is not None else ""}
          {f"AND close <= {price_max}" if price_max is not None else ""}

          {f"AND volume >= {volume_min}" if volume_min is not None else ""}
          {f"AND volume <= {volume_max}" if volume_max is not None else ""}

          {f"AND range_pct >= {range_pct_min}" if range_pct_min is not None else ""}
          {f"AND range_pct <= {range_pct_max}" if range_pct_max is not None else ""}

          {f"AND close_z_3d >= {close_z_3d_min}" if close_z_3d_min is not None else ""}
          {f"AND close_z_3d <= {close_z_3d_max}" if close_z_3d_max is not None else ""}

          {f"AND volume_z_3d >= {volume_z_3d_min}" if volume_z_3d_min is not None else ""}
          {f"AND volume_z_3d <= {volume_z_3d_max}" if volume_z_3d_max is not None else ""}

          {f"AND range_z_3d >= {range_z_3d_min}" if range_z_3d_min is not None else ""}
          {f"AND range_z_3d <= {range_z_3d_max}" if range_z_3d_max is not None else ""}

          {f"AND close_z_35d >= {close_z_35d_min}" if close_z_35d_min is not None else ""}
          {f"AND close_z_35d <= {close_z_35d_max}" if close_z_35d_max is not None else ""}

          {f"AND volume_z_35d >= {volume_z_35d_min}" if volume_z_35d_min is not None else ""}
          {f"AND volume_z_35d <= {volume_z_35d_max}" if volume_z_35d_max is not None else ""}

          {f"AND range_z_35d >= {range_z_35d_min}" if range_z_35d_min is not None else ""}
          {f"AND range_z_35d <= {range_z_35d_max}" if range_z_35d_max is not None else ""}

          {f"AND trade_signal = {str(bool(trade_signal)).upper()}" if trade_signal is not None else ""}

          -- latest slice only
          AND timestamp = (
              SELECT max(timestamp)
              FROM {table}
              WHERE 1=1
                {f"AND symbol = '{sym}'" if sym else ""}
          )
    """
    return con.execute(query).df()
