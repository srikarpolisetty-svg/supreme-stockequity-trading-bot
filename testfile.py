import pandas as pd

def get_sp500_symbols():
    url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
    df = pd.read_csv(url)
    return df["Symbol"].tolist()


if __name__ == "__main__":
    symbols = get_sp500_symbols()
    print("count:", len(symbols))
    print(symbols[:10])
