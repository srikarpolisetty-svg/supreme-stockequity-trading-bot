import os
import yfinance as yf
import pandas as pd
from dbfunctions import compute_z_scores_for_stock  # keep if you still want z's
import duckdb
  
def ingest_stock_bar_5m(symbol: str, shard_id: int, run_id: str):
    # ---- pull data ----
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period="5d", interval="5m")

    if hist.empty:
        print(f"Skip {symbol}: no data")
        return None

    latest = hist.iloc[-1]

    open_ = float(latest["Open"])
    high  = float(latest["High"])
    low   = float(latest["Low"])
    close = float(latest["Close"])
    volume = int(latest["Volume"])

    range_pct = (high - low) / close if close else None

    timestamp = (
        hist.index[-1]
        .tz_convert("America/New_York")
        .strftime("%Y-%m-%d %H:%M:%S")
    )

    snapshot_id = f"{symbol}_{timestamp}"
  # local run id (no params)
    con = duckdb.connect("stocks_data.db")

    # ======================
    # RAW BARS (3 DAY)
    # ======================
    con.execute("""
    CREATE TABLE IF NOT EXISTS stock_bars_raw_5m (
        snapshot_id TEXT,
        timestamp TIMESTAMP,
        symbol TEXT,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        range_pct DOUBLE
    );
    """)

    con.execute("""
    DELETE FROM stock_bars_raw_5m
    WHERE timestamp < NOW() - INTERVAL '3 days'
    """)
    # =========================
    # RAW -> parquet (like 5W)
    # =========================
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

    out_dir = f"runs/{run_id}/stock_bars_raw_5m"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
    df_raw.to_parquet(out_path, index=False)

    # =========================
    # ENRICHED -> parquet
    # (z-scores + placeholders)
    # =========================
    close_z, volume_z, range_z = compute_z_scores_for_stock(
    con=con,
    symbol=symbol,
    current_close=close,
    current_volume=volume,
    current_range_pct=range_pct,
 )



    con.execute("""
    CREATE TABLE IF NOT EXISTS stock_bars_enriched_5m (
        snapshot_id TEXT,
        timestamp TIMESTAMP,
        symbol TEXT,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        range_pct DOUBLE,
        close_z DOUBLE,
        volume_z DOUBLE,
        range_z DOUBLE,
        opt_ret_10m DOUBLE,
        opt_ret_1h DOUBLE,
        opt_ret_eod DOUBLE,
        opt_ret_next_open DOUBLE,
        opt_ret_1d DOUBLE,
        opt_ret_2d DOUBLE,
        opt_ret_3d DOUBLE
    );
    """)

    con.execute("""
    DELETE FROM stock_bars_enriched_5m
    WHERE timestamp < NOW() - INTERVAL '3 days'
    """)





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

    out_dir = f"runs/{run_id}/stock_bars_enriched_5m"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
    df_enriched.to_parquet(out_path, index=False)

    # =========================
    # SIGNALS -> parquet
    # =========================



    con.execute("""
    CREATE TABLE IF NOT EXISTS stock_execution_signals_5m (
        snapshot_id TEXT,
        timestamp TIMESTAMP,
        symbol TEXT,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        range_pct DOUBLE,
        close_z DOUBLE,
        volume_z DOUBLE,
        range_z DOUBLE,
        opt_ret_10m DOUBLE,
        opt_ret_1h DOUBLE,
        opt_ret_eod DOUBLE,
        opt_ret_next_open DOUBLE,
        opt_ret_1d DOUBLE,
        opt_ret_2d DOUBLE,
        opt_ret_3d DOUBLE,
        trade_signal BOOLEAN
    );
    """)

    con.execute("""
    DELETE FROM stock_execution_signals_5m
    WHERE timestamp < NOW() - INTERVAL '3 days'
    """)
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

    out_dir = f"runs/{run_id}/stock_execution_signals_5m"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/shard_{shard_id}_{symbol}.parquet"
    df_signals.to_parquet(out_path, index=False)

    print(snapshot_id)
    






import duckdb


def master_ingest_5w(run_id: str, db_path: str = "stocks_data.db"):
    """
    Master ingest for a single 5W run_id.
    Reads parquet written by shards and inserts into existing tables.
    """

    raw_dir = f"runs/{run_id}/stock_bars_raw_5m"
    enriched_dir = f"runs/{run_id}/stock_bars_enriched_5m"
    signals_dir = f"runs/{run_id}/stock_execution_signals_5m"

    con = duckdb.connect(db_path)

    try:
        con.execute("BEGIN;")

        con.execute("""
            INSERT INTO stock_execution_signals_5m
            SELECT * FROM read_parquet(?)
        """, [f"{signals_dir}/shard_*.parquet"])

        con.execute("""
            INSERT INTO stock_bars_enriched_5m
            SELECT * FROM read_parquet(?)
        """, [f"{enriched_dir}/shard_*.parquet"])

        con.execute("""
            INSERT INTO stock_bars_raw_5m
            SELECT * FROM read_parquet(?)
        """, [f"{raw_dir}/shard_*.parquet"])

        con.execute("COMMIT;")

    except Exception:
        con.execute("ROLLBACK;")
        raise

    finally:
        con.close()


