import os
import threading
import time

import polars as pl

from benchmark.metrics import measure

DIM_CSV      = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "dim_symbols.csv")
FACT_CSV     = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices.csv")
PARQUET_DIR  = os.path.join(os.path.dirname(__file__), "..", "..", "data", "parquet", "star_schema")
DIM_PARQUET  = os.path.join(PARQUET_DIR, "dim_symbols.parquet")
FACT_PARQUET = os.path.join(PARQUET_DIR, "fact_prices.parquet")


def write_dim() -> None:
    print(f"Running write_dim benchmark (CSV → {DIM_PARQUET})...")
    with measure("parquet_star_write_dim", data_path=PARQUET_DIR) as m:
        os.makedirs(PARQUET_DIR, exist_ok=True)
        df = pl.read_csv(DIM_CSV, infer_schema_length=1000)
        df.write_parquet(DIM_PARQUET)
        row_count = len(df)
    print(f"write_dim done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Disk: {m.value.disk_size_mb:.1f}MB | Rows: {row_count:,}")


def write_fact() -> None:
    print(f"Running write_fact benchmark (CSV → {FACT_PARQUET})...")
    with measure("parquet_star_write_fact", data_path=PARQUET_DIR) as m:
        os.makedirs(PARQUET_DIR, exist_ok=True)
        df = pl.read_csv(FACT_CSV, infer_schema_length=1000)
        df.write_parquet(FACT_PARQUET)
        row_count = len(df)
    print(f"write_fact done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Disk: {m.value.disk_size_mb:.1f}MB | Rows: {row_count:,}")


def query_join() -> pl.DataFrame:
    print("Running query_join benchmark (lazy JOIN fact_prices → dim_symbols, GROUP BY sector)...")
    with measure("parquet_star_query_join", data_path=PARQUET_DIR) as m:
        fact = pl.scan_parquet(FACT_PARQUET)
        dim  = pl.scan_parquet(DIM_PARQUET)
        result = (
            fact.join(dim, on="ticker_id", how="left")
            .group_by("sector")
            .agg(
                pl.col("close").mean().alias("avg_close"),
                pl.col("close").max().alias("max_close"),
                pl.col("close").min().alias("min_close"),
            )
            .sort("sector")
            .collect()
        )
    print(f"query_join done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Sectors: {len(result)}")
    return result


def query_oltp() -> pl.DataFrame:
    print("Running query_oltp benchmark (full-scan filter: ticker_id=1, date 2020–2023)...")
    with measure("parquet_star_query_oltp", data_path=PARQUET_DIR) as m:
        result = (
            pl.scan_parquet(FACT_PARQUET)
            .filter(
                (pl.col("ticker_id") == 1)
                & (pl.col("date") >= "2020-01-01")
                & (pl.col("date") <= "2023-12-31")
            )
            .collect()
        )
    print(f"query_oltp done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Rows: {len(result):,}")
    return result


def query_concurrent(n_threads: int = 10) -> None:
    print(f"Running query_concurrent benchmark ({n_threads} threads)...")
    errors: list[Exception] = []

    def _worker():
        try:
            query_join()
        except Exception as exc:
            errors.append(exc)

    with measure(f"parquet_star_concurrent_{n_threads}", data_path=PARQUET_DIR) as m:
        threads = [threading.Thread(target=_worker) for _ in range(n_threads)]
        wall_start = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        wall_sec = time.perf_counter() - wall_start

    if errors:
        print(f"  {len(errors)} thread(s) raised errors: {errors[0]}")
    print(f"query_concurrent({n_threads}) done: wall={wall_sec:.2f}s | "
          f"measured={m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")


if __name__ == "__main__":
    write_dim()
    write_fact()
    query_join()
    query_oltp()
    for n in [5, 10, 20]:
        query_concurrent(n)
    print("All benchmarks complete.")
