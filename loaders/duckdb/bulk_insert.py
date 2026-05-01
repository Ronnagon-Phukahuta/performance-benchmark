import os
import duckdb
import pandas as pd
from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")
DUCKDB_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "duckdb")
DUCKDB_PATH = os.path.join(DUCKDB_DIR, "bulk_insert.db")

CREATE_TABLE_SQL = """
    CREATE TABLE stocks (
        date    VARCHAR,
        ticker  VARCHAR,
        open    DOUBLE,
        high    DOUBLE,
        low     DOUBLE,
        close   DOUBLE,
        volume  BIGINT
    )
"""

QUERY_SQL = """
    SELECT ticker,
           AVG(close) AS avg_close,
           MAX(close) AS max_close,
           MIN(close) AS min_close
    FROM stocks
    GROUP BY ticker
"""


def write():
    os.makedirs(DUCKDB_DIR, exist_ok=True)
    print("Loading CSV...")
    df = pd.read_csv(RAW_CSV)
    df.columns = df.columns.str.lower()
    print(f"Loaded {len(df)} rows from CSV")
    bulk_df = df[["date", "ticker", "open", "high", "low", "close", "volume"]]
    print("Running write benchmark (bulk_insert)...")
    with measure("duckdb_bulk_insert_write", data_path=DUCKDB_DIR) as m:
        con = duckdb.connect(DUCKDB_PATH)
        con.execute("DROP TABLE IF EXISTS stocks")
        con.execute(CREATE_TABLE_SQL)
        con.register("df", bulk_df)
        con.execute("INSERT INTO stocks SELECT * FROM df")
        con.close()
    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Disk: {m.value.disk_size_mb:.1f}MB")


def read():
    print("Running read benchmark...")
    with measure("duckdb_bulk_insert_read", data_path=DUCKDB_DIR) as m:
        con = duckdb.connect(DUCKDB_PATH)
        df = con.execute("SELECT * FROM stocks").fetchdf()
        con.close()
    print(f"Read done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return df


def query():
    print("Running query benchmark...")
    with measure("duckdb_bulk_insert_query", data_path=DUCKDB_DIR) as m:
        con = duckdb.connect(DUCKDB_PATH)
        result = con.execute(QUERY_SQL).fetchdf()
        con.close()
    print(f"Query done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return result


if __name__ == "__main__":
    write()
    read()
    query()
    print("All benchmarks complete. Results saved.")
