from __future__ import annotations

import argparse
from databentodatabase import ingest_stock_5m_databento
from dbfunctions import get_sp500_symbols


def main():
    # ---- shard args ----
    parser = argparse.ArgumentParser()
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    # ---- load + shard symbols ----
    symbols = sorted(get_sp500_symbols())  # stable ordering
    my_symbols = symbols[args.shard::args.shards]

    print(
        f"Shard {args.shard}/{args.shards} processing "
        f"{len(my_symbols)} symbols (run_id={args.run_id})",
        flush=True,
    )

    # ---- process (parquet only) ----
    try:
        for symbol in my_symbols:
            ingest_stock_5m_databento(
                symbol=symbol,
                run_id=args.run_id,
                shard_id=args.shard,
            )
    except Exception as e:
        print(f"Shard {args.shard} failed: {e}", flush=True)


if __name__ == "__main__":
    main()
