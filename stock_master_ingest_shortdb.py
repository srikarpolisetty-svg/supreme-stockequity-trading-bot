from stock_shortdb import master_ingest_3d
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
        # 0) Ensure tables exist BEFORE ingest
        con.execute("""
            CREATE TABLE IF NOT EXISTS stock_bars_raw_5m_3d (
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
            CREATE TABLE IF NOT EXISTS stock_bars_enriched_5m_3d (
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
            CREATE TABLE IF NOT EXISTS stock_execution_signals_5m_3d (
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

        # 1) Ingest (single writer phase)
        master_ingest_3d(run_id=args.run_id)

        # 2) Cleanup AFTER ingest
        con.execute("""
            DELETE FROM stock_bars_raw_5m_3d
            WHERE timestamp < NOW() - INTERVAL '3 days';
        """)

        con.execute("""
            DELETE FROM stock_bars_enriched_5m_3d
            WHERE timestamp < NOW() - INTERVAL '3 days';
        """)

        con.execute("""
            DELETE FROM stock_execution_signals_5m_3d
            WHERE timestamp < NOW() - INTERVAL '3 days';
        """)
