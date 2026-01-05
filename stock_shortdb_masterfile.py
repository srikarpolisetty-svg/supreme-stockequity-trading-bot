from stock_shortdb import ingest_stock_bar_5m_3d
import pandas as pd
import time
import argparse
from stock_shortdb import master_ingest_3d # <-- match your 5W pattern (create this if you don't have it)
from datetime import datetime
import pytz




def get_sp500_symbols():
    url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
    df = pd.read_csv(url)
    return df["Symbol"].tolist()


def main():
    # ---- shard args ----
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--run_id", required=True)

    args = parser.parse_args()
    run_id = args.run_id

    # ---- load + shard symbols ----
    symbols = get_sp500_symbols()
    symbols = sorted(symbols)  # stable ordering

    my_symbols = symbols[args.shard::args.shards]

    print(f"[3D] Shard {args.shard}/{args.shards} processing {len(my_symbols)} symbols")

    # ---- process ----

    for symbol in my_symbols:
        try:
            ingest_stock_bar_5m_3d(symbol, args.shard, run_id)

        except Exception as e:
            print(f"Error ingesting {symbol}: {e}")

        time.sleep(0.3)

  # ---- master ingest (RUN ONCE)


if __name__ == "__main__":
    main()
