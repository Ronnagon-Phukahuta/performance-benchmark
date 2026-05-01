import os
import pandas as pd
import psycopg2
from tqdm import tqdm
from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")

DB_PARAMS = dict(
    host="localhost",
    port=5432,
    dbname="benchmark_db",
    user="benchmark",
    password="benchmark",
)

TABLE = "stocks_batch_insert"
BATCH_SIZE = 10_000

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
    df = pd.read_csv(RAW_CSV)
    df.columns = df.columns.str.lower()
    df["volume"] = df["volume"].fillna(0).astype(int)
    rows = df[["date", "ticker", "open", "high", "low", "close", "volume"]].values.tolist()
    total_rows = len(rows)
    batches = [rows[s : s + BATCH_SIZE] for s in range(0, total_rows, BATCH_SIZE)]
    print(f"Loaded {total_rows} rows from CSV")
    print(f"Running write benchmark (postgres batch_insert, {len(batches)} batches)...")
    with measure("postgres_batch_insert_write", data_path="") as m:
        con = psycopg2.connect(**DB_PARAMS)
        cur = con.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        cur.execute(CREATE_TABLE_SQL)
        for batch in tqdm(batches, desc="Inserting batches", unit="batch"):
            cur.executemany(
                f"INSERT INTO {TABLE} VALUES (%s, %s, %s, %s, %s, %s, %s)", batch
            )
        con.commit()
        cur.close()
        con.close()
    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")


def read():
    print("Running read benchmark...")
    with measure("postgres_batch_insert_read", data_path="") as m:
        con = psycopg2.connect(**DB_PARAMS)
        df = pd.read_sql(f"SELECT * FROM {TABLE}", con)
        con.close()
    print(f"Read done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return df


def query():
    print("Running query benchmark...")
    with measure("postgres_batch_insert_query", data_path="") as m:
        con = psycopg2.connect(**DB_PARAMS)
        result = pd.read_sql(QUERY_SQL, con)
        con.close()
    print(f"Query done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return result


if __name__ == "__main__":
    write()
    read()
    query()
    print("All benchmarks complete. Results saved.")
