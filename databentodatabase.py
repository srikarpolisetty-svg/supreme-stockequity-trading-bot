from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os
import pandas as pd
import databento as db
from dbfunctions import compute_z_scores_for_stock 

NY_TZ = ZoneInfo("America/New_York")
DB_PATH = "/home/ubuntu/supreme-stockequity-trading-bot/stocks_data.db"

def ingest_stock_5m_databento(symbol: str, run_id: str, shard_id: int):
    def get_stock_ohlcv(symbol: str) -> dict | None:
        client = db.Historical()

        now = datetime.now(timezone.utc)
        start = now - timedelta(minutes=5)

        df = client.timeseries.get_range(
            dataset="EQUS.MINI",
            schema="ohlcv-1m",
            symbols=[symbol],
            start=start.isoformat(),
            end=now.isoformat(),
        ).to_df()

        if df.empty:
            return None

        row = df.sort_values("ts_event").iloc[-1]

        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        v = int(row["volume"])

        range_pct = ((h - l) / c * 100.0) if c > 0 else None

        return {
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": v,
            "range_pct": range_pct,
        }

    stock = get_stock_ohlcv(symbol)
    if stock is None:
        return None

    open_ = stock["open"]
    high = stock["high"]
    low = stock["low"]
    close = stock["close"]
    volume = stock["volume"]
    range_pct = stock["range_pct"]

    ts = datetime.now(NY_TZ).strftime("%Y-%m-%d %H:%M:%S")
    snapshot_id = f"{symbol}_{ts}"

    underlying_price = close

    # -------------------------
    # RAW -> parquet
    # -------------------------
    cols_raw = [
        "underlying_price",
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

    df_raw = pd.DataFrame(
        [[
            underlying_price,
            snapshot_id,
            ts,
            symbol,
            open_,
            high,
            low,
            close,
            volume,
            range_pct,
        ]],
        columns=cols_raw,
    )

    out_dir = f"runs/{run_id}/stock_bars_raw_5m"
    os.makedirs(out_dir, exist_ok=True)
    df_raw.to_parquet(
        f"{out_dir}/shard_{shard_id}_{symbol}.parquet",
        index=False,
    )

    # -------------------------
    # ENRICHED -> parquet
    # -------------------------
    (
        close_z_3d,
        volume_z_3d,
        range_z_3d,
        close_z_35d,
        volume_z_35d,
        range_z_35d,
    ) = compute_z_scores_for_stock(
        symbol=symbol,
        current_close=close,
        current_volume=volume,
        current_range_pct=range_pct,
    )

    cols_enriched = [
        *cols_raw,
        "close_z_3d",
        "volume_z_3d",
        "range_z_3d",
        "close_z_35d",
        "volume_z_35d",
        "range_z_35d",
        "opt_ret_10m",
        "opt_ret_1h",
        "opt_ret_eod",
        "opt_ret_next_open",
        "opt_ret_1d",
        "opt_ret_2d",
        "opt_ret_3d",
    ]

    df_enriched = pd.DataFrame(
        [[
            underlying_price,
            snapshot_id,
            ts,
            symbol,
            open_,
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
            None, None, None, None, None, None, None,
        ]],
        columns=cols_enriched,
    )

    out_dir = f"runs/{run_id}/stock_bars_enriched_5m"
    os.makedirs(out_dir, exist_ok=True)
    df_enriched.to_parquet(
        f"{out_dir}/shard_{shard_id}_{symbol}.parquet",
        index=False,
    )

    # -------------------------
    # SIGNALS -> parquet
    # -------------------------
    cols_signals = [*cols_enriched, "trade_signal"]

    df_signals = pd.DataFrame(
        [[
            underlying_price,
            snapshot_id,
            ts,
            symbol,
            open_,
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
            None, None, None, None, None, None, None,
            None,
        ]],
        columns=cols_signals,
    )

    out_dir = f"runs/{run_id}/stock_execution_signals_5m"
    os.makedirs(out_dir, exist_ok=True)
    df_signals.to_parquet(
        f"{out_dir}/shard_{shard_id}_{symbol}.parquet",
        index=False,
    )

    print(f"[STOCK] {snapshot_id}")
    return snapshot_id











def master_ingest_5m(run_id: str, db_path: str = DB_PATH):
    import glob
    import duckdb

    raw_dir = f"runs/{run_id}/stock_bars_raw_5m"
    enriched_dir = f"runs/{run_id}/stock_bars_enriched_5m"
    signals_dir = f"runs/{run_id}/stock_execution_signals_5m"

    raw_glob = f"{raw_dir}/shard_*.parquet"
    enriched_glob = f"{enriched_dir}/shard_*.parquet"
    signals_glob = f"{signals_dir}/shard_*.parquet"

    # ---- guard: skip cleanly if nothing to ingest ----
    signals_files = glob.glob(signals_glob)
    enriched_files = glob.glob(enriched_glob)
    raw_files = glob.glob(raw_glob)

    if not (signals_files or enriched_files or raw_files):
        print(f"[STOCK][INGEST] no parquet files found for run_id={run_id}", flush=True)
        return

    con = duckdb.connect(db_path)
    try:
        con.execute("BEGIN;")

        def _assert_schema_order(table: str, parquet_glob: str):
            """
            Enforce: parquet columns (names + order) exactly match table columns.
            Uses union_by_name=true so multi-file schema inference doesn't break on NULL vs DOUBLE.
            """
            if not glob.glob(parquet_glob):
                print(f"[STOCK][INGEST] skip {table}: no files", flush=True)
                return False

            pq_cols = con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?, union_by_name=true)",
                [parquet_glob],
            ).df()["column_name"].tolist()

            tbl_cols = con.execute(
                f"PRAGMA table_info('{table}')"
            ).df()["name"].tolist()

            if pq_cols != tbl_cols:
                raise RuntimeError(
                    f"[STOCK][INGEST] schema/order mismatch for {table}\n"
                    f"  parquet={pq_cols}\n"
                    f"  table ={tbl_cols}"
                )
            return True

        # ---- signals ----
        if _assert_schema_order("stock_execution_signals_5m", signals_glob):
            con.execute(
                """
                INSERT INTO stock_execution_signals_5m
                SELECT * FROM read_parquet(?, union_by_name=true)
                """,
                [signals_glob],
            )

        # ---- enriched ----
        if _assert_schema_order("stock_bars_enriched_5m", enriched_glob):
            con.execute(
                """
                INSERT INTO stock_bars_enriched_5m
                SELECT * FROM read_parquet(?, union_by_name=true)
                """,
                [enriched_glob],
            )

        # ---- raw ----
        if _assert_schema_order("stock_bars_raw_5m", raw_glob):
            con.execute(
                """
                INSERT INTO stock_bars_raw_5m
                SELECT * FROM read_parquet(?, union_by_name=true)
                """,
                [raw_glob],
            )

        con.execute("COMMIT;")
        print(f"[STOCK][INGEST] committed run_id={run_id}", flush=True)

    except Exception:
        con.execute("ROLLBACK;")
        raise
    finally:
        con.close()
