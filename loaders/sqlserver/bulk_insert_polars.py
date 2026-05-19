
import math
import os
import polars as pl
import pymssql
from benchmark.metrics import measure

SAMPLE_SIZE = 100_000  # Benchmarking on 100K rows only

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")

HOST = "localhost"
PORT = 1433
USER = "sa"
PASSWORD = "Benchmark123!"
DATABASE = "master"
TABLE = "stocks"

CREATE_TABLE_SQL = f"""
    CREATE TABLE {TABLE} (
        date    VARCHAR(20),
        ticker  VARCHAR(20),
        type    VARCHAR(10),
        [open]  FLOAT,
        high    FLOAT,
        low     FLOAT,
        [close] FLOAT,
        volume  FLOAT
    )
"""

QUERY_SQL = f"""
    SELECT ticker,
           AVG([close]) AS avg_close,
           MAX([close]) AS max_close,
           MIN([close]) AS min_close
    FROM {TABLE}
    GROUP BY ticker
"""

def _clean(val):
    if val is None:
        return None
    try:
        if math.isnan(float(val)):
            return None
    except (TypeError, ValueError):
        pass
    return val

def _connect():
    return pymssql.connect(HOST, USER, PASSWORD, DATABASE, port=PORT)

def write():
    print("Loading CSV with Polars...")
    df = pl.read_csv(RAW_CSV)
    df = df.select(["date", "ticker", "type", "open", "high", "low", "close", "volume"])
    df = df.head(SAMPLE_SIZE)
    print(f"Benchmarking on 100K rows — extrapolating to 28M")
    rows = [tuple(_clean(v) for v in row) for row in df.iter_rows(named=False)]
    total = len(rows)
    print(f"Loaded {total:,} rows from CSV")
    print("Running write benchmark (sqlserver_bulk_insert_polars)...")
    with measure("sqlserver_bulk_insert_polars_write", data_path="") as m:
        conn = _connect()
        conn.autocommit(False)
        cur = conn.cursor()
        cur.execute(f"IF OBJECT_ID('{TABLE}') IS NOT NULL DROP TABLE {TABLE}")
        cur.execute(CREATE_TABLE_SQL)
        insert_sql = f"INSERT INTO {TABLE} VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        print(f"Sending {len(rows):,} rows to SQL Server...")
        cur.executemany(insert_sql, rows)
        conn.commit()
        cur.close()
        conn.close()
    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    extrapolated = m.value.duration_sec * (28_151_758 / SAMPLE_SIZE)
    print(f"Extrapolated to 28M rows: {extrapolated:.0f}s (~{extrapolated/3600:.1f}h)")

def read():
    print("Running read benchmark...")
    with measure("sqlserver_bulk_insert_polars_read", data_path="") as m:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(f"SELECT TOP {SAMPLE_SIZE} * FROM {TABLE}")
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        df = pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)})
        cur.close()
        conn.close()
    print(f"Read done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    extrapolated = m.value.duration_sec * (28_151_758 / SAMPLE_SIZE)
    print(f"Extrapolated to 28M rows: {extrapolated:.0f}s (~{extrapolated/3600:.1f}h)")
    return df

def query():
    print("Running query benchmark...")
    with measure("sqlserver_bulk_insert_polars_query", data_path="") as m:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(QUERY_SQL)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        result = pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(columns)})
        cur.close()
        conn.close()
    print(f"Query done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    extrapolated = m.value.duration_sec * (28_151_758 / SAMPLE_SIZE)
    print(f"Extrapolated to 28M rows: {extrapolated:.0f}s (~{extrapolated/3600:.1f}h)")
    return result

if __name__ == "__main__":
    write()
    read()
    query()
    print("All benchmarks complete. Results saved.")
