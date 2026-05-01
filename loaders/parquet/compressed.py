import os
import pandas as pd
from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")
PARQUET_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "parquet")
SNAPPY_PATH = os.path.join(PARQUET_DIR, "compressed_snappy.parquet")
GZIP_PATH = os.path.join(PARQUET_DIR, "compressed_gzip.parquet")


def write():
    os.makedirs(PARQUET_DIR, exist_ok=True)
    print("Loading CSV...")
    df = pd.read_csv(RAW_CSV)
    df.columns = df.columns.str.lower()
    print(f"Loaded {len(df)} rows from CSV")

    print("Running write benchmark (parquet snappy)...")
    with measure("parquet_compressed_snappy_write", data_path=SNAPPY_PATH) as m:
        df.to_parquet(SNAPPY_PATH, index=False, compression="snappy")
    print(f"Snappy write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Disk: {m.value.disk_size_mb:.1f}MB")

    print("Running write benchmark (parquet gzip)...")
    with measure("parquet_compressed_gzip_write", data_path=GZIP_PATH) as m:
        df.to_parquet(GZIP_PATH, index=False, compression="gzip")
    print(f"Gzip write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Disk: {m.value.disk_size_mb:.1f}MB")


def read():
    print("Running read benchmark (snappy)...")
    with measure("parquet_compressed_snappy_read", data_path=SNAPPY_PATH) as m:
        df = pd.read_parquet(SNAPPY_PATH)
    print(f"Read done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return df


def query():
    print("Running query benchmark (snappy)...")
    with measure("parquet_compressed_snappy_query", data_path=SNAPPY_PATH) as m:
        df = pd.read_parquet(SNAPPY_PATH, columns=["ticker", "close"])
        result = df.groupby("ticker")["close"].agg(["mean", "max", "min"]).reset_index()
        result.columns = ["ticker", "avg_close", "max_close", "min_close"]
    print(f"Query done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return result


if __name__ == "__main__":
    write()
    read()
    query()
    print("All benchmarks complete. Results saved.")
