import os
import glob
import pandas as pd
from tqdm import tqdm
from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")
PARQUET_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "parquet")
PARTITION_DIR = os.path.join(PARQUET_DIR, "partitioned")


def write():
    os.makedirs(PARTITION_DIR, exist_ok=True)
    print("Loading CSV...")
    df = pd.read_csv(RAW_CSV)
    df.columns = df.columns.str.lower()
    print(f"Loaded {len(df)} rows from CSV")
    tickers = df["ticker"].unique().tolist()
    print(f"Running write benchmark (parquet partitioned, {len(tickers)} tickers)...")
    with measure("parquet_partitioned_write", data_path=PARTITION_DIR) as m:
        for ticker in tqdm(tickers, desc="Writing partitions", unit="ticker"):
            ticker_df = df[df["ticker"] == ticker]
            path = os.path.join(PARTITION_DIR, f"{ticker}.parquet")
            ticker_df.to_parquet(path, index=False)
    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Disk: {m.value.disk_size_mb:.1f}MB")


def read():
    print("Running read benchmark...")
    with measure("parquet_partitioned_read", data_path=PARTITION_DIR) as m:
        files = glob.glob(os.path.join(PARTITION_DIR, "*.parquet"))
        df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    print(f"Read done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return df


def query():
    print("Running query benchmark...")
    with measure("parquet_partitioned_query", data_path=PARTITION_DIR) as m:
        files = glob.glob(os.path.join(PARTITION_DIR, "*.parquet"))
        frames = [pd.read_parquet(f, columns=["ticker", "close"]) for f in files]
        df = pd.concat(frames, ignore_index=True)
        result = df.groupby("ticker")["close"].agg(["mean", "max", "min"]).reset_index()
        result.columns = ["ticker", "avg_close", "max_close", "min_close"]
    print(f"Query done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return result


if __name__ == "__main__":
    write()
    read()
    query()
    print("All benchmarks complete. Results saved.")
