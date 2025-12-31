from stock_longdb import ingest_stock_bar_5m
import pandas as pd
import time


def get_sp500_symbols():
    url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
    df = pd.read_csv(url)
    return df["Symbol"].tolist()


def main():
    symbols = get_sp500_symbols()

    for symbol in symbols:
        try:
            ingest_stock_bar_5m(symbol)
        except Exception as e:
            print(f"[ERROR] {symbol}: {e}")

        time.sleep(0.3)  # throttle every symbol


if __name__ == "__main__":
    main()
