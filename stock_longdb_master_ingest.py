from stock_longdb import master_ingest_5w

import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    master_ingest_5w(run_id=args.run_id)


