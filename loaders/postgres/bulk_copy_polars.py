import io
import os
import polars as pl
import pyarrow.csv as pa_csv
import psycopg2
from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")

DB_PARAMS = dict(
    host="localhost",
    port=5432,
    dbname="benchmark_db",
    user="benchmark",
    password="benchmark",
)

TABLE = "stocks_bulk_copy_polars"

CREATE_TABLE_SQL = f"""
    CREATE TABLE {TABLE} (
        date    VARCHAR,
        ticker  VARCHAR,
        open    DOUBLE PRECISION,
        high    DOUBLE PRECISION,
        low     DOUBLE PRECISION,
        close   DOUBLE PRECISION,
        volume  BIGINT
    )
"""

QUERY_SQL = f"""
    SELECT ticker,
           AVG(close) AS avg_close,
           MAX(close) AS max_close,
           MIN(close) AS min_close
    FROM {TABLE}
    GROUP BY ticker
"""


def write():
    print("Loading CSV...")
    df = pl.read_csv(RAW_CSV)
    df.columns = [c.lower() for c in df.columns]
    df = df.select(["date", "ticker", "open", "high", "low", "close", "volume"])
    df = df.with_columns(pl.col("volume").fill_null(0).cast(pl.Int64))
    print(f"Loaded {len(df)} rows from CSV")
    print("Running write benchmark (postgres bulk_copy_polars via COPY)...")
    with measure("postgres_bulk_copy_polars_write", data_path="") as m:
        arrow_table = df.to_arrow()
        buf = io.BytesIO()
        pa_csv.write_csv(arrow_table, buf)
        buf.seek(0)
        con = psycopg2.connect(**DB_PARAMS)
        cur = con.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cur.execute(CREATE_TABLE_SQL)
        cur.copy_expert(
            f"COPY {TABLE} (date, ticker, open, high, low, close, volume) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
            buf,
        )
        con.commit()
        cur.close()
        con.close()
    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")


def read():
    print("Running read benchmark...")
    with measure("postgres_bulk_copy_polars_read", data_path="") as m:
        con = psycopg2.connect(**DB_PARAMS)
        cur = con.cursor()
        cur.execute(f"SELECT * FROM {TABLE}")
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        df = pl.DataFrame(
            {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        )
        cur.close()
        con.close()
    print(f"Read done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return df


def query():
    print("Running query benchmark...")
    with measure("postgres_bulk_copy_polars_query", data_path="") as m:
        con = psycopg2.connect(**DB_PARAMS)
        cur = con.cursor()
        cur.execute(QUERY_SQL)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        result = pl.DataFrame(
            {col: [row[i] for row in rows] for i, col in enumerate(columns)}
        )
        cur.close()
        con.close()
    print(f"Query done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return result


if __name__ == "__main__":
    write()
    read()
    query()
    print("All benchmarks complete. Results saved.")
