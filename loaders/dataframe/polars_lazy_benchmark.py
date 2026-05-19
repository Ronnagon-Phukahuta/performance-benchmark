import os

import polars as pl

from benchmark.metrics import BenchmarkResult, measure

ENGINE = "polars_lazy"

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")
FACT_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices_sample.csv")
DIM_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "dim_symbols.csv")


def op1_read_csv() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op1_read_csv...")
    with measure(f"{ENGINE}_op1_read_csv") as m:
        df = pl.scan_csv(RAW_CSV).collect()
    print(f"  op1 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(df):,}")
    return m.value


def op2_filter() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op2_filter (ticker == AAPL)...")
    with measure(f"{ENGINE}_op2_filter") as m:
        result = (
            pl.scan_csv(RAW_CSV)
            .filter(pl.col("ticker") == "AAPL")
            .collect()
        )
    print(f"  op2 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(result):,}")
    return m.value


def op3_groupby() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op3_groupby (GROUP BY ticker → AVG/MAX/MIN close)...")
    with measure(f"{ENGINE}_op3_groupby") as m:
        result = (
            pl.scan_csv(RAW_CSV)
            .group_by("ticker")
            .agg([
                pl.mean("close").alias("avg_close"),
                pl.max("close").alias("max_close"),
                pl.min("close").alias("min_close"),
            ])
            .collect()
        )
    print(f"  op3 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Groups: {len(result):,}")
    return m.value


def op4_sort() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op4_sort (close DESC)...")
    with measure(f"{ENGINE}_op4_sort") as m:
        result = (
            pl.scan_csv(RAW_CSV)
            .sort("close", descending=True)
            .collect()
        )
    print(f"  op4 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(result):,}")
    return m.value


def op5_join() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op5_join (fact LEFT JOIN dim on ticker_id)...")
    with measure(f"{ENGINE}_op5_join") as m:
        fact = pl.scan_csv(FACT_CSV)
        dim = pl.scan_csv(DIM_CSV)
        result = fact.join(dim, on="ticker_id", how="left").collect()
    print(f"  op5 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(result):,}")
    return m.value


def op6_window() -> BenchmarkResult:
    # rolling_mean with over() requires the full group to be in memory;
    # lazy scan + sort first, then apply the window function eagerly.
    print(f"[{ENGINE}] Running op6_window (rolling 7-row avg close per ticker)...")
    with measure(f"{ENGINE}_op6_window") as m:
        df = pl.scan_csv(RAW_CSV).sort("date").collect()
        result = df.with_columns(
            pl.col("close").rolling_mean(window_size=7).over("ticker").alias("rolling_avg_close")
        )
    print(f"  op6 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(result):,}")
    return m.value


def op7_string() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op7_string (tickers starting with 'A')...")
    with measure(f"{ENGINE}_op7_string") as m:
        result = (
            pl.scan_csv(RAW_CSV)
            .filter(pl.col("ticker").str.starts_with("A"))
            .collect()
        )
    print(f"  op7 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(result):,}")
    return m.value


def op8_typecast_null() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op8_typecast_null (date→date, fill null close)...")
    with measure(f"{ENGINE}_op8_typecast_null") as m:
        result = (
            pl.scan_csv(RAW_CSV)
            .with_columns([
                pl.col("date").str.to_date(),
                pl.col("close").fill_null(0.0),
            ])
            .collect()
        )
    print(f"  op8 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(result):,}")
    return m.value


def op9_concat() -> BenchmarkResult:
    # Collect once, split into two LazyFrames, lazy-concat and collect.
    print(f"[{ENGINE}] Running op9_concat (split in half + lazy concat)...")
    with measure(f"{ENGINE}_op9_concat") as m:
        df = pl.scan_csv(RAW_CSV).collect()
        half = len(df) // 2
        lf1 = df[:half].lazy()
        lf2 = df[half:].lazy()
        result = pl.concat([lf1, lf2]).collect()
    print(f"  op9 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(result):,}")
    return m.value


if __name__ == "__main__":
    for fn in [op1_read_csv, op2_filter, op3_groupby, op4_sort, op5_join,
               op6_window, op7_string, op8_typecast_null, op9_concat]:
        fn()
    print(f"\n[{ENGINE}] All operations complete.")
