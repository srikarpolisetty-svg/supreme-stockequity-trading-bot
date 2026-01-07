import os
import yfinance as yf
import pandas as pd
from dbfunctions import compute_z_scores_for_stock_3d
import duckdb
from datetime import datetime
import pytz



def ingest_stock_bar_5m_3d(symbol: str, shard_id: int, run_id: str):

    # ======================
    # DATA PULL
    # ======================
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="5d", interval="5m")

    if df.empty:
        print(f"Skip {symbol}: no data")
        return None

    latest = df.iloc[-1]
    open_ = float(latest["Open"])
    high = float(latest["High"])
    low = float(latest["Low"])
    close = float(latest["Close"])
    volume = int(latest["Volume"])

    range_pct = (high - low) / close if close else None

    timestamp = datetime.now(pytz.timezone("America/New_York")).strftime(
    "%Y-%m-%d %H:%M:%S"
 )
    snapshot_id = f"{symbol}_{timestamp}"
    print(snapshot_id)


    # ======================
    # RAW BARS (3 DAY) -> parquet
    # ======================


    cols_raw = [
        "snapshot_id",
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "range_pct",
    ]

    rows_raw = [[
        snapshot_id,
        timestamp,
        symbol,
        open_,
        high,
        low,
        close,
        volume,
        range_pct,
    ]]

    df_raw = pd.DataFrame(rows_raw, columns=cols_raw)

    out_dir = f"runs/{run_id}/stock_bars_raw_5m_3d"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
    df_raw.to_parquet(out_path, index=False)

    # ======================
    # Z-SCORES (3 DAY)
    # ======================
    close_z, volume_z, range_z = compute_z_scores_for_stock_3d(
    symbol=symbol,
    current_close=close,
    current_volume=volume,
    current_range_pct=range_pct,
 )


    # ======================
    # ENRICHED BARS (3 DAY) -> parquet
    # ======================



    cols_enriched = [
        "snapshot_id",
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "range_pct",
        "close_z",
        "volume_z",
        "range_z",
        "opt_ret_10m",
        "opt_ret_1h",
        "opt_ret_eod",
        "opt_ret_next_open",
        "opt_ret_1d",
        "opt_ret_2d",
        "opt_ret_3d",
    ]

    rows_enriched = [[
        snapshot_id,
        timestamp,
        symbol,
        open_,
        high,
        low,
        close,
        volume,
        range_pct,
        close_z,
        volume_z,
        range_z,
        None, None, None, None, None, None, None,
    ]]

    df_enriched = pd.DataFrame(rows_enriched, columns=cols_enriched)

    out_dir = f"runs/{run_id}/stock_bars_enriched_5m_3d"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
    df_enriched.to_parquet(out_path, index=False)

    # ======================
    # EXECUTION SIGNALS (3 DAY) -> parquet
    # ======================



    cols_signals = [
        "snapshot_id",
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "range_pct",
        "close_z",
        "volume_z",
        "range_z",
        "opt_ret_10m",
        "opt_ret_1h",
        "opt_ret_eod",
        "opt_ret_next_open",
        "opt_ret_1d",
        "opt_ret_2d",
        "opt_ret_3d",
        "trade_signal",
    ]

    rows_signals = [[
        snapshot_id,
        timestamp,
        symbol,
        open_,
        high,
        low,
        close,
        volume,
        range_pct,
        close_z,
        volume_z,
        range_z,
        None, None, None, None, None, None, None,
        None,
    ]]

    df_signals = pd.DataFrame(rows_signals, columns=cols_signals)

    out_dir = f"runs/{run_id}/stock_execution_signals_5m_3d"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
    df_signals.to_parquet(out_path, index=False)









import duckdb


def master_ingest_3d(run_id: str, db_path: str = "stocks_data.db"):
    """
    Master ingest for a single run_id.
    Reads parquet files written by shards and inserts them into existing tables.
    """

    signals_dir = f"runs/{run_id}/stock_execution_signals_5m_3d"
    enriched_dir = f"runs/{run_id}/stock_bars_enriched_5m_3d"
    raw_dir = f"runs/{run_id}/stock_bars_raw_5m_3d"

    con = duckdb.connect(db_path)

    try:
        con.execute("BEGIN;")

        con.execute("""
            INSERT INTO stock_execution_signals_5m_3d
            SELECT * FROM read_parquet(?)
        """, [f"{signals_dir}/shard_*.parquet"])

        con.execute("""
            INSERT INTO stock_bars_enriched_5m_3d
            SELECT * FROM read_parquet(?)
        """, [f"{enriched_dir}/shard_*.parquet"])

        con.execute("""
            INSERT INTO stock_bars_raw_5m_3d
            SELECT * FROM read_parquet(?)
        """, [f"{raw_dir}/shard_*.parquet"])

        con.execute("COMMIT;")

    except Exception:
        con.execute("ROLLBACK;")
        raise

    finally:
        con.close()





