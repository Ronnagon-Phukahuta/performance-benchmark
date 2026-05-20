"""
Phase 9 — Postgres Indexed vs Non-Indexed Benchmark

Measures the impact of B-tree indexes on query performance across three
query patterns, each run twice: once with no index (full sequential scan)
and once after indexes are created.

Query patterns:
  1. GROUP BY ticker — AVG/MAX/MIN close (analytical, 8,049 result rows)
  2. Point lookup    — WHERE ticker = 'AAPL' (9,909 rows, high-selectivity)
  3. Date range      — WHERE date >= '2020-01-01' AND date <= '2023-12-31'

Indexes created:
  idx_stocks_indexed_ticker  — B-tree on ticker
  idx_stocks_indexed_date    — B-tree on date

Index creation time is measured and saved independently from bulk load time.

Run order:
  write()          → bulk load, NO indexes
  query_no_index() → all three queries (sequential scan baseline)
  create_index()   → CREATE INDEX on ticker + date, ANALYZE
  query_indexed()  → same three queries (index-assisted)
"""

import io
import os

import polars as pl
import psycopg
import pyarrow.csv as pa_csv

from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")

CONN_PARAMS = dict(
    dbname="benchmark_db",
    user="benchmark",
    password="benchmark",
    host="localhost",
    port=5432,
)

TABLE = "stocks_indexed"

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

GROUPBY_SQL = f"""
    SELECT ticker,
           AVG(close) AS avg_close,
           MAX(close) AS max_close,
           MIN(close) AS min_close
    FROM {TABLE}
    GROUP BY ticker
    ORDER BY ticker
"""

LOOKUP_SQL = f"SELECT * FROM {TABLE} WHERE ticker = 'AAPL'"

RANGE_SQL = f"""
    SELECT * FROM {TABLE}
    WHERE date >= '2020-01-01' AND date <= '2023-12-31'
"""


def _make_csv_buffer(df: pl.DataFrame) -> io.BytesIO:
    arrow_table = df.to_arrow()
    buf = io.BytesIO()
    pa_csv.write_csv(arrow_table, buf)
    buf.seek(0)
    return buf


def _run_query(label: str, sql: str, conn_params: dict) -> pl.DataFrame:
    with measure(f"phase9_postgres_{label}", data_path="") as m:
        with psycopg.connect(**conn_params) as conn:
            with conn.cursor(binary=True) as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                cols = [d.name for d in cur.description]
    df = pl.DataFrame({col: [row[i] for row in rows] for i, col in enumerate(cols)})
    print(
        f"  {label}: {m.value.duration_sec:.2f}s | "
        f"RAM: {m.value.peak_ram_mb:.1f} MB | Rows: {len(df):,}"
    )
    return df


def write() -> None:
    """
    Bulk-load CSV into a fresh table with NO indexes.
    Must be called before query_no_index().
    """
    print("Loading CSV...")
    df = pl.read_csv(RAW_CSV)
    df.columns = [c.lower() for c in df.columns]
    df = df.select(["date", "ticker", "open", "high", "low", "close", "volume"])
    df = df.with_columns(pl.col("volume").fill_null(0).cast(pl.Int64))
    print(f"Loaded {len(df):,} rows")

    print(f"Writing to {TABLE} (no index)...")
    with measure("phase9_postgres_write", data_path="") as m:
        buf = _make_csv_buffer(df)
        with psycopg.connect(**CONN_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
                cur.execute(CREATE_TABLE_SQL)
                with cur.copy(
                    f"COPY {TABLE} (date, ticker, open, high, low, close, volume)"
                    " FROM STDIN (FORMAT CSV, HEADER TRUE)"
                ) as copy:
                    copy.write(buf.read())
            conn.commit()

    print(
        f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB"
    )


def query_no_index() -> None:
    """
    Run all three query patterns with NO indexes (sequential scan baseline).
    Call after write() and before create_index().
    """
    print("\n--- Query: NO INDEX (sequential scan) ---")
    _run_query("no_index_groupby", GROUPBY_SQL, CONN_PARAMS)
    _run_query("no_index_lookup", LOOKUP_SQL, CONN_PARAMS)
    _run_query("no_index_range", RANGE_SQL, CONN_PARAMS)


def create_index() -> None:
    """
    Create B-tree indexes on ticker and date, then ANALYZE.
    Index creation times are measured and saved independently.
    """
    print("\n--- Creating indexes ---")
    with psycopg.connect(**CONN_PARAMS) as conn:
        conn.autocommit = True

        print(f"  CREATE INDEX idx_{TABLE}_ticker ...")
        with measure("phase9_postgres_create_index_ticker", data_path="") as m:
            conn.execute(
                f"CREATE INDEX idx_{TABLE}_ticker ON {TABLE} (ticker)"
            )
        print(f"  ticker index: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB")

        print(f"  CREATE INDEX idx_{TABLE}_date ...")
        with measure("phase9_postgres_create_index_date", data_path="") as m:
            conn.execute(
                f"CREATE INDEX idx_{TABLE}_date ON {TABLE} (date)"
            )
        print(f"  date index: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB")

        print("  ANALYZE ...")
        conn.execute(f"ANALYZE {TABLE}")
        print("  ANALYZE done — planner statistics updated")


def query_indexed() -> None:
    """
    Run all three query patterns WITH indexes.
    Call after create_index().
    """
    print("\n--- Query: WITH INDEX ---")
    _run_query("indexed_groupby", GROUPBY_SQL, CONN_PARAMS)
    _run_query("indexed_lookup", LOOKUP_SQL, CONN_PARAMS)
    _run_query("indexed_range", RANGE_SQL, CONN_PARAMS)


def show_query_plans() -> None:
    """
    Print EXPLAIN output for all three queries so index usage can be verified.
    Not benchmarked — diagnostic only.
    """
    print("\n--- Query plans (EXPLAIN) ---")
    queries = {
        "GROUP BY ticker": GROUPBY_SQL,
        "WHERE ticker = 'AAPL'": LOOKUP_SQL,
        "WHERE date range": RANGE_SQL,
    }
    with psycopg.connect(**CONN_PARAMS) as conn:
        with conn.cursor() as cur:
            for name, sql in queries.items():
                cur.execute(f"EXPLAIN {sql}")
                plan = "\n".join(row[0] for row in cur.fetchall())
                print(f"\n  [{name}]\n{plan}")


if __name__ == "__main__":
    write()
    query_no_index()
    create_index()
    query_indexed()
    show_query_plans()
    print("\nPhase 9 Postgres benchmark complete. Results saved to benchmark_results.json")
