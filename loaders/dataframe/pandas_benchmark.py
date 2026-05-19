import os

import pandas as pd

from benchmark.metrics import BenchmarkResult, measure

ENGINE = "pandas"

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")
FACT_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices_sample.csv")
DIM_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "dim_symbols.csv")


def op1_read_csv() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op1_read_csv...")
    with measure(f"{ENGINE}_op1_read_csv") as m:
        df = pd.read_csv(RAW_CSV)
    print(f"  op1 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(df):,}")
    return m.value


def op2_filter() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op2_filter (ticker == AAPL)...")
    with measure(f"{ENGINE}_op2_filter") as m:
        df = pd.read_csv(RAW_CSV)
        result = df[df["ticker"] == "AAPL"]
    print(f"  op2 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(result):,}")
    return m.value


def op3_groupby() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op3_groupby (GROUP BY ticker → AVG/MAX/MIN close)...")
    with measure(f"{ENGINE}_op3_groupby") as m:
        df = pd.read_csv(RAW_CSV)
        result = df.groupby("ticker")["close"].agg(["mean", "max", "min"])
    print(f"  op3 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Groups: {len(result):,}")
    return m.value


def op4_sort() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op4_sort (close DESC)...")
    with measure(f"{ENGINE}_op4_sort") as m:
        df = pd.read_csv(RAW_CSV)
        result = df.sort_values("close", ascending=False)
    print(f"  op4 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(result):,}")
    return m.value


def op5_join() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op5_join (fact LEFT JOIN dim on ticker_id)...")
    with measure(f"{ENGINE}_op5_join") as m:
        fact = pd.read_csv(FACT_CSV)
        dim = pd.read_csv(DIM_CSV)
        result = fact.merge(dim, on="ticker_id", how="left")
    print(f"  op5 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(result):,}")
    return m.value


def op6_window() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op6_window (rolling 7-row avg close per ticker)...")
    with measure(f"{ENGINE}_op6_window") as m:
        df = pd.read_csv(RAW_CSV)
        df = df.sort_values("date")
        df["rolling_avg_close"] = df.groupby("ticker")["close"].transform(
            lambda x: x.rolling(7).mean()
        )
    print(f"  op6 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(df):,}")
    return m.value


def op7_string() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op7_string (tickers starting with 'A')...")
    with measure(f"{ENGINE}_op7_string") as m:
        df = pd.read_csv(RAW_CSV)
        result = df[df["ticker"].str.startswith("A")]
    print(f"  op7 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(result):,}")
    return m.value


def op8_typecast_null() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op8_typecast_null (date→datetime, fill null close)...")
    with measure(f"{ENGINE}_op8_typecast_null") as m:
        df = pd.read_csv(RAW_CSV)
        df["date"] = pd.to_datetime(df["date"])
        df["close"] = df["close"].fillna(0.0)
    print(f"  op8 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(df):,}")
    return m.value


def op9_concat() -> BenchmarkResult:
    print(f"[{ENGINE}] Running op9_concat (split in half + concat)...")
    with measure(f"{ENGINE}_op9_concat") as m:
        df = pd.read_csv(RAW_CSV)
        half = len(df) // 2
        df1, df2 = df.iloc[:half], df.iloc[half:]
        result = pd.concat([df1, df2])
    print(f"  op9 done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(result):,}")
    return m.value


if __name__ == "__main__":
    for fn in [op1_read_csv, op2_filter, op3_groupby, op4_sort, op5_join,
               op6_window, op7_string, op8_typecast_null, op9_concat]:
        fn()
    print(f"\n[{ENGINE}] All operations complete.")
