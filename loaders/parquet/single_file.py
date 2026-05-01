import os
import pandas as pd
from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")
PARQUET_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "parquet")
PARQUET_PATH = os.path.join(PARQUET_DIR, "single_file.parquet")


def write():
    os.makedirs(PARQUET_DIR, exist_ok=True)
    print("Loading CSV...")
    df = pd.read_csv(RAW_CSV)
    df.columns = df.columns.str.lower()
    print(f"Loaded {len(df)} rows from CSV")
    print("Running write benchmark (parquet single_file)...")
    with measure("parquet_single_file_write", data_path=PARQUET_PATH) as m:
        df.to_parquet(PARQUET_PATH, index=False)
    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Disk: {m.value.disk_size_mb:.1f}MB")


def read():
    print("Running read benchmark...")
    with measure("parquet_single_file_read", data_path=PARQUET_PATH) as m:
        df = pd.read_parquet(PARQUET_PATH)
    print(f"Read done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return df


def query():
    print("Running query benchmark...")
    with measure("parquet_single_file_query", data_path=PARQUET_PATH) as m:
        df = pd.read_parquet(PARQUET_PATH, columns=["ticker", "close"])
        result = df.groupby("ticker")["close"].agg(["mean", "max", "min"]).reset_index()
        result.columns = ["ticker", "avg_close", "max_close", "min_close"]
    print(f"Query done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return result


if __name__ == "__main__":
    write()
    read()
    query()
    print("All benchmarks complete. Results saved.")
