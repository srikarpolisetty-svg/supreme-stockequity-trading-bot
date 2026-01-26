# ibkr_stock_5m_ingest.py
from IBKR_database import master_ingest_5m
import duckdb
import argparse
from datetime import datetime


DB_PATH = "/home/ubuntu/supreme-stockequity-trading-bot/stocks_data.db"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    print(f"[MASTER] start {datetime.now()} run_id={args.run_id}", flush=True)

    with duckdb.connect(DB_PATH) as con:
        # =========================
        # RAW (5m)
        # =========================
        con.execute("""
            CREATE TABLE IF NOT EXISTS stock_bars_raw_5m (
                underlying_price DOUBLE,
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

        # =========================
        # ENRICHED (5m)
        # =========================
        con.execute("""
            CREATE TABLE IF NOT EXISTS stock_bars_enriched_5m (
                underlying_price DOUBLE,
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                range_pct DOUBLE,

                -- short horizon (3d)
                close_z_3d DOUBLE,
                volume_z_3d DOUBLE,
                range_z_3d DOUBLE,

                -- long horizon (35d)
                close_z_35d DOUBLE,
                volume_z_35d DOUBLE,
                range_z_35d DOUBLE,

                opt_ret_10m DOUBLE,
                opt_ret_1h DOUBLE,
                opt_ret_eod DOUBLE,
                opt_ret_next_open DOUBLE,
                opt_ret_1d DOUBLE,
                opt_ret_2d DOUBLE,
                opt_ret_3d DOUBLE
            );
        """)

        # =========================
        # EXECUTION SIGNALS (5m)
        # =========================
        con.execute("""
            CREATE TABLE IF NOT EXISTS stock_execution_signals_5m (
                underlying_price DOUBLE,
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                range_pct DOUBLE,

                -- short horizon (3d)
                close_z_3d DOUBLE,
                volume_z_3d DOUBLE,
                range_z_3d DOUBLE,

                -- long horizon (35d)
                close_z_35d DOUBLE,
                volume_z_35d DOUBLE,
                range_z_35d DOUBLE,

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

        # =========================
        # INGEST (single-writer)
        # =========================
        master_ingest_5m(run_id=args.run_id)

        # =========================
        # CLEANUP (35 days)
        # =========================
        con.execute("""
            DELETE FROM stock_bars_raw_5m
            WHERE timestamp < NOW() - INTERVAL '35 days';
        """)

        con.execute("""
            DELETE FROM stock_bars_enriched_5m
            WHERE timestamp < NOW() - INTERVAL '35 days';
        """)

        con.execute("""
            DELETE FROM stock_execution_signals_5m
            WHERE timestamp < NOW() - INTERVAL '35 days';
        """)

