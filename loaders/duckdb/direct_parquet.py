import os
import duckdb
from benchmark.metrics import measure

PARQUET_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "parquet", "single_file_polars.parquet")

def write():
    print("No write step for direct parquet query")
    return

def read():
    print("Running read benchmark (duckdb direct parquet)...")
    with measure("duckdb_direct_parquet_read", data_path=PARQUET_PATH) as m:
        con = duckdb.connect()
        df = con.execute(f"SELECT * FROM read_parquet('{PARQUET_PATH}')").fetchdf()
        con.close()
    print(f"Read done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return df

def query():
    print("Running query benchmark (duckdb direct parquet)...")
    with measure("duckdb_direct_parquet_query", data_path=PARQUET_PATH) as m:
        con = duckdb.connect()
        result = con.execute(f"SELECT ticker, AVG(close), MAX(close), MIN(close) FROM read_parquet('{PARQUET_PATH}') GROUP BY ticker").fetchdf()
        con.close()
    print(f"Query done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return result

if __name__ == "__main__":
    write()
    read()
    query()
    print("All benchmarks complete. Results saved.")