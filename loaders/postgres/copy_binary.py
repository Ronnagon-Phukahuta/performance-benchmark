"""
Phase 7E — PostgreSQL COPY Protocol Strategies: Binary vs CSV vs SELECT

Three read strategies for 28,151,758 rows:

  Strategy 1 — SELECT * + psycopg3 binary=True          (Phase 7D baseline ~142s)
    Extended query protocol. binary=True makes the driver request binary format
    from the server. Still goes through PostgreSQL's row-level execution pipeline.

  Strategy 2 — COPY TO STDOUT (FORMAT BINARY) → Polars  (server-side binary export)
    COPY uses a separate server code path — no executor, no output functions per row.
    PostgreSQL sends raw IEEE-754 bytes for numeric columns directly. psycopg3
    copy.rows() deserializes using C extension (set_types required for binary).
    Bottleneck: Python-level iteration over 28M tuples remains.

  Strategy 3 — COPY TO STDOUT (FORMAT CSV) → Polars read_csv  (bulk + SIMD parse)
    Entire CSV stream buffered as bytes, then handed to Polars' SIMD parser in one
    pass. Zero Python-level row iteration. Trade: text on wire, vectorized parse
    on client. Hypothesis: Polars SIMD parse is faster than 28M×7 Python type calls.

write()  — Polars → PyArrow CSV buffer → COPY FROM STDIN (same as Phase 7D)
read()   — all three strategies, separate benchmark results per strategy
query()  — COPY BINARY → Polars → Arrow → DuckDB GROUP BY (full pipeline)
"""

import io
import os

import duckdb
import polars as pl
import psycopg
import pyarrow.csv as pa_csv

from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")

DB_PARAMS = dict(
    host="localhost",
    port=5432,
    dbname="benchmark_db",
    user="benchmark",
    password="benchmark",
)

TABLE = "stocks_copy_binary"
COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume"]

# PostgreSQL type names for COPY BINARY deserialization — must match column order
# psycopg3 copy.set_types() uses these to pick C-level type loaders
PG_TYPES = ["text", "text", "float8", "float8", "float8", "float8", "int8"]

# Schema for empty-result safety
_SCHEMA = {
    "date": pl.Utf8,
    "ticker": pl.Utf8,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Int64,
}

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

# DuckDB GROUP BY query — runs against in-process Arrow table registered as "t"
QUERY_SQL = """
    SELECT ticker,
           AVG(close) AS avg_close,
           MAX(close) AS max_close,
           MIN(close) AS min_close
    FROM t
    GROUP BY ticker
"""

# Phase 7D baselines for comparison output
_BASELINE_SELECT_BINARY_S = 142
_BASELINE_PGBOUNCER_S = 109


def write() -> None:
    """
    Polars → PyArrow CSV buffer → COPY FROM STDIN via psycopg3.
    Identical approach to Phase 7D. COPY is already a raw byte stream at the
    wire level — driver choice and binary mode have no effect on write speed.
    """
    print("Loading CSV...")
    df = pl.read_csv(RAW_CSV)
    df.columns = [c.lower() for c in df.columns]
    df = df.select(COLUMNS)
    df = df.with_columns(pl.col("volume").fill_null(0).cast(pl.Int64))
    print(f"Loaded {len(df):,} rows")

    print("Running write benchmark (psycopg3 COPY FROM STDIN)...")
    with measure("postgres_copy_binary_write", data_path="") as m:
        arrow_table = df.to_arrow()
        buf = io.BytesIO()
        pa_csv.write_csv(arrow_table, buf)
        buf.seek(0)
        with psycopg.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
                cur.execute(CREATE_TABLE_SQL)
                with cur.copy(
                    f"COPY {TABLE} ({', '.join(COLUMNS)})"
                    " FROM STDIN (FORMAT CSV, HEADER TRUE)"
                ) as copy:
                    copy.write(buf.read())
            conn.commit()

    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB")


def _read_select_binary() -> pl.DataFrame:
    """
    Strategy 1: SELECT * with psycopg3 cursor(binary=True).
    Extended query protocol — binary format requested per-column via protocol negotiation.
    Row-by-row deserialization: 28M×7 = 196M type conversions in Python.
    Phase 7D result: ~142s direct, ~109s via pgBouncer.
    """
    print("  [1] SELECT * + binary cursor (Phase 7D baseline)...")
    with measure("postgres_copy_binary_read_select_binary", data_path="") as m:
        with psycopg.connect(**DB_PARAMS) as conn:
            with conn.cursor(binary=True) as cur:
                cur.execute(f"SELECT * FROM {TABLE}")
                rows = cur.fetchall()
                cols = [d.name for d in cur.description]
        df = pl.DataFrame(
            {col: [row[i] for row in rows] for i, col in enumerate(cols)}
        )
    print(
        f"     {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB"
        f" | rows: {len(df):,}"
    )
    return df


def _read_copy_binary() -> pl.DataFrame:
    """
    Strategy 2: COPY TO STDOUT (FORMAT BINARY).
    Server sends raw IEEE-754 bytes via the COPY export path — no text output
    functions, no row-level executor. psycopg3 copy.rows() uses C-extension loaders
    (set_types required) to deserialize each field.
    Client-side bottleneck: Python-level iteration over 28M rows remains.
    zip(*rows) transposes via C-level iterators — faster than explicit loop.
    """
    print("  [2] COPY TO STDOUT FORMAT BINARY...")
    with measure("postgres_copy_binary_read_copy_binary", data_path="") as m:
        with psycopg.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                with cur.copy(
                    f"COPY {TABLE} TO STDOUT (FORMAT BINARY)"
                ) as copy:
                    copy.set_types(PG_TYPES)
                    rows = list(copy.rows())
        df = (
            pl.DataFrame(dict(zip(COLUMNS, zip(*rows))))
            if rows
            else pl.DataFrame(schema=_SCHEMA)
        )
    print(
        f"     {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB"
        f" | rows: {len(df):,}"
    )
    return df


def _read_copy_csv() -> pl.DataFrame:
    """
    Strategy 3: COPY TO STDOUT (FORMAT CSV) → Polars read_csv.
    Streams the full CSV output into a BytesIO buffer, then parses with
    Polars' SIMD-accelerated parser in one vectorized pass.
    Zero Python-level row iteration — entire parse runs in Rust.
    Trade-off: text transfer on wire; gain: no per-row Python deserialization.
    """
    print("  [3] COPY TO STDOUT FORMAT CSV → Polars SIMD parse...")
    with measure("postgres_copy_binary_read_copy_csv", data_path="") as m:
        buf = io.BytesIO()
        with psycopg.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                with cur.copy(
                    f"COPY {TABLE} TO STDOUT (FORMAT CSV, HEADER TRUE)"
                ) as copy:
                    for chunk in copy:
                        buf.write(chunk)
        buf.seek(0)
        df = pl.read_csv(buf, schema_overrides={"volume": pl.Int64})
    print(
        f"     {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB"
        f" | rows: {len(df):,}"
    )
    return df


def read() -> None:
    """
    Run all three strategies in sequence and print a comparison.
    Each strategy saves its own result to benchmark_results.json.
    """
    print("Running read benchmark — 3 strategies (28,151,758 rows)...")
    print()

    _read_select_binary()
    _read_copy_binary()
    _read_copy_csv()

    print()
    print("─" * 60)
    print("Read strategy comparison")
    print(f"  Phase 7D SELECT binary (direct):    ~{_BASELINE_SELECT_BINARY_S}s")
    print(f"  Phase 7D SELECT binary (pgBouncer): ~{_BASELINE_PGBOUNCER_S}s")
    print("  COPY BINARY:  see postgres_copy_binary_read_copy_binary")
    print("  COPY CSV:     see postgres_copy_binary_read_copy_csv")
    print("─" * 60)


def query() -> pl.DataFrame:
    """
    Full pipeline: COPY BINARY → Polars DataFrame → Arrow → DuckDB GROUP BY.

    Why DuckDB instead of Postgres GROUP BY:
      COPY TO STDOUT can only export raw table data, not query results.
      To aggregate, we either run SELECT GROUP BY (row-level executor, slow)
      or pull the data and aggregate in-process with DuckDB (~0.2s overhead).
      This pipeline tests the COPY BINARY read + DuckDB aggregation combined.
    """
    print("Running query benchmark (COPY BINARY → Polars → DuckDB GROUP BY)...")
    with measure("postgres_copy_binary_query", data_path="") as m:
        # 1. Pull data via COPY BINARY
        with psycopg.connect(**DB_PARAMS) as conn:
            with conn.cursor() as cur:
                with cur.copy(
                    f"COPY {TABLE} TO STDOUT (FORMAT BINARY)"
                ) as copy:
                    copy.set_types(PG_TYPES)
                    rows = list(copy.rows())

        df = (
            pl.DataFrame(dict(zip(COLUMNS, zip(*rows))))
            if rows
            else pl.DataFrame(schema=_SCHEMA)
        )

        # 2. GROUP BY in DuckDB — in-process, Arrow zero-copy
        con = duckdb.connect()
        con.register("t", df.to_arrow())
        result = con.execute(QUERY_SQL).pl()
        con.close()

    print(
        f"Query done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB"
        f" | tickers: {len(result):,}"
    )
    return result


if __name__ == "__main__":
    write()
    read()
    query()
    print("\nAll benchmarks complete. Results saved.")
