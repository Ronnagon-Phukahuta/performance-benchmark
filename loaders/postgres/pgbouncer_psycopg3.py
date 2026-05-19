"""
Phase 7D — psycopg3 binary protocol + pgBouncer connection pooling

Two connection targets:
  - DIRECT:     localhost:5432  (psycopg3, binary protocol)
  - PGBOUNCER:  localhost:6432  (psycopg3, binary protocol via pgBouncer transaction pool)

Write: Polars → PyArrow CSV buffer → COPY FROM STDIN (via psycopg3 direct)
Read:  SELECT * — binary protocol avoids text→float parsing for 5 numeric columns
Query: GROUP BY ticker, AVG/MAX/MIN close — binary protocol, both connection targets

Protocol advantage:
  psycopg2 (text):   Postgres sends "3.141590", Python parses str→float per cell
  psycopg3 (binary): Postgres sends 8-byte IEEE-754, Python reads float directly
  At 28M rows × 5 numeric columns = 140M float parses avoided on read
"""

import io
import os

import polars as pl
import psycopg
import pyarrow.csv as pa_csv

from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")

_COMMON = dict(
    dbname="benchmark_db",
    user="benchmark",
    password="benchmark",
)

DIRECT_PARAMS = dict(**_COMMON, host="localhost", port=5432)
PGBOUNCER_PARAMS = dict(**_COMMON, host="localhost", port=6432)

TABLE = "stocks_psycopg3"

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


def _make_csv_buffer(df: pl.DataFrame) -> io.BytesIO:
    """Convert Polars DataFrame to a CSV BytesIO buffer via PyArrow (fast path)."""
    arrow_table = df.to_arrow()
    buf = io.BytesIO()
    pa_csv.write_csv(arrow_table, buf)
    buf.seek(0)
    return buf


def write() -> pl.DataFrame:
    """
    Load CSV → Polars → PyArrow CSV buffer → COPY FROM STDIN via psycopg3.
    Uses direct connection (port 5432); pgBouncer adds no value for single bulk writes.
    """
    print("Loading CSV...")
    df = pl.read_csv(RAW_CSV)
    df.columns = [c.lower() for c in df.columns]
    df = df.select(["date", "ticker", "open", "high", "low", "close", "volume"])
    df = df.with_columns(pl.col("volume").fill_null(0).cast(pl.Int64))
    print(f"Loaded {len(df):,} rows from CSV")

    print("Running write benchmark (psycopg3 COPY FROM STDIN, direct)...")
    with measure("postgres_psycopg3_write", data_path="") as m:
        buf = _make_csv_buffer(df)
        with psycopg.connect(**DIRECT_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
                cur.execute(CREATE_TABLE_SQL)
                with cur.copy(
                    f"COPY {TABLE} (date, ticker, open, high, low, close, volume)"
                    " FROM STDIN (FORMAT CSV, HEADER TRUE)"
                ) as copy:
                    copy.write(buf.read())
            conn.commit()

    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB")
    return df


def _read_binary(params: dict, label: str) -> pl.DataFrame:
    """SELECT * with psycopg3 binary protocol from the given connection target."""
    print(f"Running read benchmark (psycopg3 binary, {label})...")
    with measure(f"postgres_psycopg3_read_{label}", data_path="") as m:
        with psycopg.connect(**params) as conn:
            with conn.cursor(binary=True) as cur:
                cur.execute(f"SELECT * FROM {TABLE}")
                rows = cur.fetchall()
                cols = [d.name for d in cur.description]
        df = pl.DataFrame(
            {col: [row[i] for row in rows] for i, col in enumerate(cols)}
        )
    print(
        f"Read ({label}) done: {m.value.duration_sec:.2f}s | "
        f"RAM: {m.value.peak_ram_mb:.1f} MB | Rows: {len(df):,}"
    )
    return df


def _query_binary(params: dict, label: str) -> pl.DataFrame:
    """GROUP BY query with psycopg3 binary protocol from the given connection target."""
    print(f"Running query benchmark (psycopg3 binary, {label})...")
    with measure(f"postgres_psycopg3_query_{label}", data_path="") as m:
        with psycopg.connect(**params) as conn:
            with conn.cursor(binary=True) as cur:
                cur.execute(QUERY_SQL)
                rows = cur.fetchall()
                cols = [d.name for d in cur.description]
        result = pl.DataFrame(
            {col: [row[i] for row in rows] for i, col in enumerate(cols)}
        )
    print(
        f"Query ({label}) done: {m.value.duration_sec:.2f}s | "
        f"RAM: {m.value.peak_ram_mb:.1f} MB | Rows: {len(result):,}"
    )
    return result


def read() -> None:
    """Benchmark read via direct psycopg3 AND pgBouncer psycopg3, then compare."""
    df_direct = _read_binary(DIRECT_PARAMS, "direct")
    df_pgbouncer = _read_binary(PGBOUNCER_PARAMS, "pgbouncer")
    print(f"\nRead comparison: direct={len(df_direct):,} rows | pgbouncer={len(df_pgbouncer):,} rows")


def query() -> None:
    """Benchmark GROUP BY query via direct psycopg3 AND pgBouncer psycopg3, then compare."""
    r_direct = _query_binary(DIRECT_PARAMS, "direct")
    r_pgbouncer = _query_binary(PGBOUNCER_PARAMS, "pgbouncer")
    print(f"\nQuery comparison: direct={len(r_direct):,} tickers | pgbouncer={len(r_pgbouncer):,} tickers")


if __name__ == "__main__":
    write()
    read()
    query()
    print("\nAll benchmarks complete. Results saved.")
