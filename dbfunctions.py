import duckdb
import pandas as pd

DB_PATH = "/home/ubuntu/supreme-options-bot-prekafka/stocks_data.db"

def compute_z_scores_for_stock(
    symbol: str,
    current_close,
    current_volume,
    current_range_pct,
    table: str = "stock_bars_raw_5m",
):
    """
    Compute BOTH 3-day and 35-day z-scores for close, volume, range_pct
    from a single raw stock table.

    Returns:
        (close_z_3d, vol_z_3d, range_z_3d, close_z_35d, vol_z_35d, range_z_35d)
        where any element can be None if we can't compute it safely.
    """

    with duckdb.connect(DB_PATH, read_only=True) as con:
        df = con.execute(
            f"""
            SELECT
                close,
                volume,
                range_pct,
                timestamp
            FROM {table}
            WHERE symbol = ?
              AND timestamp >= CURRENT_TIMESTAMP - INTERVAL 35 DAY
            """,
            [symbol],
        ).df()

    if df.empty:
        return (None, None, None, None, None, None)

    # Make sure timestamp is datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    # Helper: safe z
    def z(curr, series: pd.Series):
        if curr is None:
            return None

        s = series.dropna()
        if s.empty:
            return None

        mean = s.mean()
        std  = s.std()

        if std is None or pd.isna(std) or std <= 0:
            return None

        try:
            curr_val = float(curr)
        except Exception:
            return None

        return (curr_val - mean) / std

    # --------------------
    # 3-day window (relative to most recent timestamp we have for this symbol)
    # --------------------
    tmax = df["timestamp"].max()
    if pd.isna(tmax):
        df_3d = df.iloc[0:0]
    else:
        df_3d = df[df["timestamp"] >= (tmax - pd.Timedelta(days=3))]

    close_z_3d = z(current_close,     df_3d["close"])
    vol_z_3d   = z(current_volume,    df_3d["volume"])
    range_z_3d = z(current_range_pct, df_3d["range_pct"])

    # --------------------
    # 35-day window
    # --------------------
    close_z_35d = z(current_close,     df["close"])
    vol_z_35d   = z(current_volume,    df["volume"])
    range_z_35d = z(current_range_pct, df["range_pct"])

    return (close_z_3d, vol_z_3d, range_z_3d, close_z_35d, vol_z_35d, range_z_35d)
















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

