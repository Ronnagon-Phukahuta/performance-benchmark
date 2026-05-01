import os
import duckdb
import pandas as pd
from tqdm import tqdm
from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")
DUCKDB_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "duckdb")
DUCKDB_PATH = os.path.join(DUCKDB_DIR, "row_by_row.db")

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
    rows = df[["date", "ticker", "open", "high", "low", "close", "volume"]].values.tolist()
    chunk_size = 10_000
    total = len(rows)
    chunks = [rows[start : start + chunk_size] for start in range(0, total, chunk_size)]
    print("Running write benchmark (row_by_row)...")
    with measure("duckdb_row_by_row_write", data_path=DUCKDB_DIR) as m:
        con = duckdb.connect(DUCKDB_PATH)
        con.execute("DROP TABLE IF EXISTS stocks")
        con.execute(CREATE_TABLE_SQL)
        for chunk in tqdm(chunks, desc="Inserting rows", unit="chunk",
                          bar_format="Inserting rows: {n_fmt}/{total_fmt} chunks [{elapsed}<{remaining}]"):
            con.executemany("INSERT INTO stocks VALUES (?, ?, ?, ?, ?, ?, ?)", chunk)
        con.close()
    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Disk: {m.value.disk_size_mb:.1f}MB")


def read():
    print("Running read benchmark...")
    with measure("duckdb_row_by_row_read", data_path=DUCKDB_DIR) as m:
        con = duckdb.connect(DUCKDB_PATH)
        df = con.execute("SELECT * FROM stocks").fetchdf()
        con.close()
    print(f"Read done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return df


def query():
    print("Running query benchmark...")
    with measure("duckdb_row_by_row_query", data_path=DUCKDB_DIR) as m:
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
