
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







import os, time, tempfile
from typing import List, Optional

import pandas as pd
import requests

SP500_URL = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
CACHE_PATH = "/home/ubuntu/supreme-stockequity-trading-bot/sp500_constituents.csv"

def _atomic_write_csv(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    d = os.path.dirname(path) or "."
    with tempfile.NamedTemporaryFile("w", delete=False, dir=d, suffix=".tmp") as f:
        tmp_path = f.name
        df.to_csv(tmp_path, index=False)
    os.replace(tmp_path, path)  # atomic on POSIX

def _normalize_symbol(sym: str) -> str:
    # yfinance compatibility: BRK.B -> BRK-B, BF.B -> BF-B
    return sym.replace(".", "-").strip().upper()

def get_sp500_symbols(retries: int = 3, backoff_sec: float = 2.0, timeout_sec: float = 10.0) -> List[str]:
    last_err: Optional[Exception] = None

    for i in range(retries):
        try:
            r = requests.get(SP500_URL, timeout=timeout_sec)
            r.raise_for_status()

            # Parse CSV from text content
            from io import StringIO
            df = pd.read_csv(StringIO(r.text))

            if "Symbol" not in df.columns:
                raise ValueError(f"Expected 'Symbol' column, got columns={list(df.columns)}")

            syms = df["Symbol"].dropna().astype(str).map(_normalize_symbol).tolist()

            # sanity check: avoid caching nonsense
            if len(syms) < 400:
                raise ValueError(f"Too few symbols ({len(syms)}). Possible bad response.")

            _atomic_write_csv(df, CACHE_PATH)
            return syms

        except Exception as e:
            last_err = e
            time.sleep(backoff_sec * (2 ** i))

    # Fallback cache
    if os.path.exists(CACHE_PATH):
        df = pd.read_csv(CACHE_PATH)
        if "Symbol" not in df.columns:
            raise RuntimeError(f"Cache exists but missing 'Symbol' column: {CACHE_PATH}")
        return df["Symbol"].dropna().astype(str).map(_normalize_symbol).tolist()

    raise RuntimeError(f"Failed to fetch S&P 500 symbols and no cache found at {CACHE_PATH}.") from last_err

