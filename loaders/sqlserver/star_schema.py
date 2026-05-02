import csv
import math
import os
import threading
import time

import pymssql

from benchmark.metrics import measure

DIM_CSV         = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "dim_symbols.csv")
FACT_CSV        = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices.csv")
FACT_SAMPLE_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices_sample.csv")

HOST     = "localhost"
PORT     = 1433
USER     = "sa"
PASSWORD = "Benchmark123!"
DATABASE = "master"

CHUNK_SIZE = 1000

CREATE_DIM_SQL = """
    CREATE TABLE dim_symbols (
        ticker_id INT PRIMARY KEY,
        ticker    VARCHAR(20),
        type      VARCHAR(10),
        sector    VARCHAR(50),
        industry  VARCHAR(50),
        exchange  VARCHAR(20)
    )
"""

CREATE_FACT_SQL = """
    CREATE TABLE fact_prices (
        ticker_id INT,
        date      DATE,
        [open]    FLOAT,
        high      FLOAT,
        low       FLOAT,
        [close]   FLOAT,
        volume    FLOAT
    )
"""

QUERY_JOIN_SQL = """
    SELECT s.sector,
           AVG(f.[close]) AS avg_close,
           MAX(f.[close]) AS max_close,
           MIN(f.[close]) AS min_close
    FROM fact_prices f
    JOIN dim_symbols s ON f.ticker_id = s.ticker_id
    GROUP BY s.sector
    ORDER BY s.sector
"""

QUERY_OLTP_SQL = """
    SELECT * FROM fact_prices
    WHERE ticker_id = %s
      AND date BETWEEN %s AND %s
"""


def _connect():
    return pymssql.connect(HOST, USER, PASSWORD, DATABASE, port=PORT)


def _clean(val: str) -> object:
    """Convert CSV string values; return None for empty or NaN floats."""
    if val == "" or val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f):
            return None
        return f
    except ValueError:
        return val


def _load_csv_chunks(csv_path: str, numeric_cols: set[str], chunk_size: int):
    """Yield lists of row tuples from a CSV file in chunks."""
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        chunk: list[tuple] = []
        for row in reader:
            values = tuple(
                _clean(v) if col in numeric_cols else (None if v == "" else v)
                for col, v in row.items()
            )
            chunk.append(values)
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk


def write_dim() -> None:
    print(f"Running write_dim benchmark (executemany chunks → dim_symbols)...")
    numeric_cols = {"ticker_id"}
    placeholders = "%s, %s, %s, %s, %s, %s"
    with measure("sqlserver_star_write_dim", data_path="") as m:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("IF OBJECT_ID('dim_symbols') IS NOT NULL DROP TABLE dim_symbols")
        cur.execute(CREATE_DIM_SQL)
        total = 0
        for chunk in _load_csv_chunks(DIM_CSV, numeric_cols, CHUNK_SIZE):
            cur.executemany(f"INSERT INTO dim_symbols VALUES ({placeholders})", chunk)
            total += len(chunk)
        conn.commit()
        cur.close()
        conn.close()
    print(f"write_dim done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Rows: {total:,}")


def write_fact() -> None:
    print("Note: full 28M rows DNF (~315 min extrapolated). Running on 100k sample.")
    print(f"Running write_fact benchmark (executemany chunks → fact_prices, sample)...")
    numeric_cols = {"ticker_id", "open", "high", "low", "close", "volume"}
    placeholders = "%s, %s, %s, %s, %s, %s, %s"
    with measure("sqlserver_star_write_fact_sample", data_path="") as m:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("IF OBJECT_ID('fact_prices') IS NOT NULL DROP TABLE fact_prices")
        cur.execute(CREATE_FACT_SQL)
        total = 0
        for i, chunk in enumerate(_load_csv_chunks(FACT_SAMPLE_CSV, numeric_cols, CHUNK_SIZE), start=1):
            cur.executemany(f"INSERT INTO fact_prices VALUES ({placeholders})", chunk)
            total += len(chunk)
            if i % 1000 == 0:
                print(f"  Inserted {total:,} rows...")
        conn.commit()
        cur.execute("CREATE INDEX idx_fact_ticker ON fact_prices(ticker_id)")
        conn.commit()
        cur.close()
        conn.close()
    print(f"write_fact done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Rows: {total:,}")


def query_join() -> list:
    print("Running query_join benchmark (JOIN fact_prices → dim_symbols, GROUP BY sector)...")
    with measure("sqlserver_star_query_join", data_path="") as m:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(QUERY_JOIN_SQL)
        result = cur.fetchall()
        cur.close()
        conn.close()
    print(f"query_join done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Sectors: {len(result)}")
    return result


def query_oltp() -> list:
    print("Running query_oltp benchmark (ticker_id=1, date 2020–2023)...")
    with measure("sqlserver_star_query_oltp", data_path="") as m:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(QUERY_OLTP_SQL, (1, "2020-01-01", "2023-12-31"))
        result = cur.fetchall()
        cur.close()
        conn.close()
    print(f"query_oltp done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Rows: {len(result):,}")
    return result


def query_concurrent(n_threads: int = 10) -> None:
    print(f"Running query_concurrent benchmark ({n_threads} threads)...")
    errors: list[Exception] = []

    def _worker():
        try:
            conn = _connect()
            cur = conn.cursor()
            cur.execute(QUERY_JOIN_SQL)
            cur.fetchall()
            cur.close()
            conn.close()
        except Exception as exc:
            errors.append(exc)

    with measure(f"sqlserver_star_concurrent_{n_threads}", data_path="") as m:
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
