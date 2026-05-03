import polars as pl
import os

RAW_PATH = "data/raw/all_stocks.csv"
SAMPLE_PATH = "data/sample/all_stocks_sample.csv"
N = 10_000

def main():
    print(f"Reading {RAW_PATH} ...")
    df = pl.read_csv(RAW_PATH)
    sample = df.head(N)
    sample.write_csv(SAMPLE_PATH)
    row_count = sample.height
    file_size = os.path.getsize(SAMPLE_PATH)
    print(f"Sample row count: {row_count}")
    print(f"Sample file size: {file_size/1024:.1f} KB")

if __name__ == "__main__":
    main()
