import yfinance as yf
import pandas as pd
import datetime
import pytz
import duckdb
from dbfunctions import compute_z_scores_for_stock

import yfinance as yf
import duckdb

def ingest_stock_bar_5m(symbol: str):
    # --- Pull data ---
    ticker = yf.Ticker(symbol)
    df = ticker.history(period="5d", interval="5m")
    latest = df.iloc[-1]

    open_  = float(latest["Open"])
    high   = float(latest["High"])
    low    = float(latest["Low"])
    close  = float(latest["Close"])
    volume = int(latest["Volume"])

    range_pct = (high - low) / close

    timestamp = (
        df.index[-1]
        .tz_convert("America/New_York")
        .strftime("%Y-%m-%d %H:%M:%S")
    )
    snapshot_id = f"{symbol}_{timestamp}"

    # --- DB connection ---
    con = duckdb.connect("stocks_data.db")

    # --- Raw table ---
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
    WHERE timestamp < NOW() - INTERVAL '35 days'
    """)

    con.execute(
        """
        INSERT INTO stock_bars_raw_5m
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_id,
            timestamp,
            symbol,
            open_,
            high,
            low,
            close,
            volume,
            range_pct,
        )
    )

    # --- Z-scores ---
    close_z, volume_z, range_z = compute_z_scores_for_stock(
        con=con,
        symbol=symbol,
        current_close=close,
        current_volume=volume,
        current_range_pct=range_pct,
    )

    # --- Enriched table ---
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
    WHERE timestamp < NOW() - INTERVAL '35 days'
    """)

    con.execute(
        """
        INSERT INTO stock_bars_enriched_5m
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """,
        (
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
        )
    )

    # --- Execution signals table ---
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
    WHERE timestamp < NOW() - INTERVAL '35 days'
    """)

    con.execute(
        """
        INSERT INTO stock_execution_signals_5m
        VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
            NULL, NULL, NULL, NULL, NULL, NULL, NULL,
            NULL
        )
        """,
        (
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
        )
    )

    return snapshot_id

