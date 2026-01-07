
import duckdb


import duckdb

import duckdb

DB_PATH = "stocks_data.db"

MIN_ROWS = 30  # minimum history required for z-scores

def compute_z_scores_for_stock(
    symbol: str,
    current_close: float,
    current_volume: float,
    current_range_pct: float,
    table: str = "stock_bars_raw_5m",
):
    try:
        with duckdb.connect(DB_PATH, read_only=True) as con:
            # --- SAFETY 1: table existence ---
            tables = {
                t[0] for t in con.execute("SHOW TABLES").fetchall()
            }
            if table not in tables:
                # cold start: table not built yet
                return 0.0, 0.0, 0.0

            df = con.execute(
                f"""
                SELECT close, volume, range_pct
                FROM {table}
                WHERE symbol = ?
                """,
                [symbol],
            ).df()

    except Exception:
        # any unexpected DB issue → fail soft
        return 0.0, 0.0, 0.0

    # --- SAFETY 2: not enough data ---
    if df.empty or len(df) < MIN_ROWS:
        return 0.0, 0.0, 0.0

    close_std = df["close"].std()
    vol_std   = df["volume"].std()
    rp_std    = df["range_pct"].std()

    # --- SAFETY 3: zero variance ---
    if not close_std or not vol_std or not rp_std:
        return 0.0, 0.0, 0.0

    close_z = (current_close - df["close"].mean()) / close_std
    vol_z   = (current_volume - df["volume"].mean()) / vol_std
    rp_z    = (current_range_pct - df["range_pct"].mean()) / rp_std

    return close_z, vol_z, rp_z






import duckdb

import duckdb

DB_PATH = "stocks_data.db"

MIN_ROWS = 30  # adjust if you want more history

def compute_z_scores_for_stock_3d(
    symbol: str,
    current_close: float,
    current_volume: float,
    current_range_pct: float,
    table: str = "stock_bars_raw_5m_3d",
):
    try:
        with duckdb.connect(DB_PATH, read_only=True) as con:
            # --- SAFETY 1: table existence ---
            tables = {
                t[0] for t in con.execute("SHOW TABLES").fetchall()
            }
            if table not in tables:
                return 0.0, 0.0, 0.0

            df = con.execute(
                f"""
                SELECT close, volume, range_pct
                FROM {table}
                WHERE symbol = ?
                """,
                [symbol],
            ).df()

    except Exception:
        # fail soft on any DB issue
        return 0.0, 0.0, 0.0

    # --- SAFETY 2: not enough data ---
    if df.empty or len(df) < MIN_ROWS:
        return 0.0, 0.0, 0.0

    close_std = df["close"].std()
    vol_std   = df["volume"].std()
    rp_std    = df["range_pct"].std()

    # --- SAFETY 3: zero variance ---
    if not close_std or not vol_std or not rp_std:
        return 0.0, 0.0, 0.0

    close_z = (current_close - df["close"].mean()) / close_std
    vol_z   = (current_volume - df["volume"].mean()) / vol_std
    rp_z    = (current_range_pct - df["range_pct"].mean()) / rp_std

    return close_z, vol_z, rp_z

