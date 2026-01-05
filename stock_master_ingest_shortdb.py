from stock_shortdb import master_ingest_3d


import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    master_ingest_3d(run_id=args.run_id)
