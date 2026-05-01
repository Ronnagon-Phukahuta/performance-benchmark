import os
import polars as pl
from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")
PARQUET_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "parquet")
PARQUET_PATH = os.path.join(PARQUET_DIR, "single_file_polars.parquet")


def write():
    os.makedirs(PARQUET_DIR, exist_ok=True)
    print("Loading CSV...")
    df = pl.read_csv(RAW_CSV)
    print(f"Loaded {len(df)} rows from CSV")
    print("Running write benchmark (parquet_lazy_polars)...")
    with measure("parquet_lazy_polars_write", data_path=PARQUET_PATH) as m:
        df.write_parquet(PARQUET_PATH)
    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Disk: {m.value.disk_size_mb:.1f}MB")


def read():
    print("Running read benchmark (lazy)...")
    with measure("parquet_lazy_polars_read", data_path=PARQUET_PATH) as m:
        df = pl.scan_parquet(PARQUET_PATH).collect()
    print(f"Read done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return df


def query():
    print("Running query benchmark (lazy)...")
    with measure("parquet_lazy_polars_query", data_path=PARQUET_PATH) as m:
        result = (
            pl.scan_parquet(PARQUET_PATH)
            .select(["ticker", "close"])
            .group_by("ticker")
            .agg([
                pl.mean("close").alias("avg_close"),
                pl.max("close").alias("max_close"),
                pl.min("close").alias("min_close"),
            ])
            .collect()
        )
    print(f"Query done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return result


if __name__ == "__main__":
    write()
    read()
    query()
    print("All benchmarks complete. Results saved.")