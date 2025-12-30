
import duckdb



def compute_z_scores_for_stock(
    con,
    symbol: str,
    current_close: float,
    current_volume: float,
    current_range_pct: float,
    table: str = "stock_bars_raw_5m",
):
    """
    Compute z-scores for close, volume, range_pct for a given symbol
    using historical rows in the stock bars table.
    """
    df = con.execute(
        f"""
        SELECT close, volume, range_pct
        FROM {table}
        WHERE symbol = ?
        """,
        [symbol],
    ).df()

    if df.empty:
        return 0.0, 0.0, 0.0

    close_mean = df["close"].mean()
    close_std  = df["close"].std()

    vol_mean = df["volume"].mean()
    vol_std  = df["volume"].std()

    rp_mean = df["range_pct"].mean()
    rp_std  = df["range_pct"].std()

    close_z = (current_close - close_mean) / close_std if close_std else 0.0
    vol_z   = (current_volume - vol_mean) / vol_std if vol_std else 0.0
    rp_z    = (current_range_pct - rp_mean) / rp_std if rp_std else 0.0

    return close_z, vol_z, rp_z




def compute_z_scores_for_stock_3d(
    con,
    symbol: str,
    current_close: float,
    current_volume: float,
    current_range_pct: float,
    table: str = "stock_bars_raw_5m_3d",
):
    """
    Compute z-scores for close, volume, range_pct for a given symbol
    using historical rows in the stock bars table.
    """
    df = con.execute(
        f"""
        SELECT close, volume, range_pct
        FROM {table}
        WHERE symbol = ?
        """,
        [symbol],
    ).df()

    if df.empty:
        return 0.0, 0.0, 0.0

    close_mean = df["close"].mean()
    close_std  = df["close"].std()

    vol_mean = df["volume"].mean()
    vol_std  = df["volume"].std()

    rp_mean = df["range_pct"].mean()
    rp_std  = df["range_pct"].std()

    close_z = (current_close - close_mean) / close_std if close_std else 0.0
    vol_z   = (current_volume - vol_mean) / vol_std if vol_std else 0.0
    rp_z    = (current_range_pct - rp_mean) / rp_std if rp_std else 0.0

    return close_z, vol_z, rp_z