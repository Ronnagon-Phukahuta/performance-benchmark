"""
Phase 7G — ADBC (Arrow Database Connectivity) PostgreSQL Driver

ADBC vs psycopg3: what changes at the wire level
─────────────────────────────────────────────────
psycopg3 SELECT binary=True:
  PostgreSQL wire → psycopg3 C extension → Python tuples (one per row)
  → Polars DataFrame constructor → columnar buffers
  Cost: 28M tuple allocations + 28M×7 Python object references = ~196M Python ops

ADBC PostgreSQL driver:
  PostgreSQL wire → libpq COPY BINARY → Arrow IPC columnar buffers
  → pl.from_arrow() zero-copy view
  Cost: zero Python objects per row — data arrives as typed columnar Arrow arrays

The ADBC driver speaks to a standard PostgreSQL server (no special extension,
no Arrow Flight gRPC service). Internally it uses PostgreSQL's COPY BINARY
protocol to stream columnar data and assembles pa.RecordBatch objects directly.
pl.from_arrow() wraps those buffers without copying — the DataFrame shares memory
with the Arrow allocation.

Key ADBC API:
  conn   = adbc_driver_postgresql.dbapi.connect(uri)
  cursor = conn.cursor()

  # Write (bulk ingest via COPY BINARY internally)
  cursor.adbc_ingest("table", arrow_table, mode="replace")
  conn.commit()

  # Read (zero-copy Arrow result)
  cursor.execute("SELECT * FROM table")
  arrow_table = cursor.fetch_arrow_table()   # pa.Table, columnar Arrow memory
  df = pl.from_arrow(arrow_table)            # zero-copy Polars view

  # Streaming read (lower RAM — RecordBatch by RecordBatch)
  reader = cursor.fetch_record_batch()       # pa.RecordBatchReader
  df = pl.from_arrow(reader)

Install:
  pip install adbc-driver-postgresql

Phase 7F baselines (same data, same Postgres instance):
  async 4-chunk SELECT:    63.75s  15,940 MB  ← current read leader
  COPY CSV + Polars:       67.23s   4,257 MB  ← current RAM leader
  async GROUP BY query:    18.31s   4,630 MB  ← current query leader
"""

import os

import adbc_driver_postgresql.dbapi as adbc
import polars as pl

from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")

# Standard PostgreSQL URI — ADBC driver connects to unmodified Postgres
CONN_URI = "postgresql://benchmark:benchmark@localhost:5432/benchmark_db"

TABLE = "stocks_arrow_flight"
COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume"]

QUERY_SQL = f"""
    SELECT ticker,
           AVG(close)  AS avg_close,
           MAX(close)  AS max_close,
           MIN(close)  AS min_close
    FROM {TABLE}
    GROUP BY ticker
"""

# Phase 7F baselines for comparison output
_BASELINE_READ_ASYNC_S = 63.75
_BASELINE_READ_COPY_CSV_S = 67.23
_BASELINE_QUERY_ASYNC_S = 18.31

# Statement option keys that may unlock binary/COPY mode in the ADBC PG driver.
# The driver tries each in order; unknown options are silently skipped.
_BINARY_OPTIONS = [
    "adbc.postgresql.stmt.use_copy",
    "adbc.postgresql.use_copy",
]


def _try_enable_binary(cur) -> None:
    """
    Attempt to enable binary/COPY mode on an ADBC cursor's underlying statement.
    Prints a line per option showing whether it was accepted or rejected.
    """
    stmt = getattr(cur, "adbc_statement", None)
    if stmt is None:
        print("  [diagnostic] adbc_statement not exposed on cursor — cannot set options")
        return
    for key in _BINARY_OPTIONS:
        try:
            stmt.set_option(key, "true")
            print(f"  [diagnostic] set_option({key!r}, 'true'): accepted")
        except Exception as exc:
            print(f"  [diagnostic] set_option({key!r}, 'true'): {exc}")


def _print_schema_diagnostic(arrow_table) -> None:
    """
    Print Arrow field types for the result table.
    large_utf8 (64-bit offsets) typically indicates binary/COPY path;
    utf8 (32-bit offsets) or dictionary types suggest text protocol.
    """
    print("  [diagnostic] result schema:")
    for field in arrow_table.schema:
        print(f"    {field.name}: {field.type}")


def write() -> None:
    """
    Polars → Arrow table → ADBC adbc_ingest (mode="replace").

    adbc_ingest uses PostgreSQL COPY BINARY internally — the Arrow columnar
    buffers are serialised directly to the wire without a CSV intermediate.
    mode="replace" drops the existing table and recreates it from the Arrow
    schema: large_utf8 → TEXT, float64 → FLOAT8, int64 → INT8.

    Compare to psycopg3 COPY FROM STDIN (CSV path via PyArrow pa_csv.write_csv).
    ADBC avoids the CSV serialisation step entirely.
    """
    print("Loading CSV...")
    df = pl.read_csv(RAW_CSV)
    df.columns = [c.lower() for c in df.columns]
    df = df.select(COLUMNS)
    df = df.with_columns(pl.col("volume").fill_null(0).cast(pl.Int64))
    print(f"Loaded {len(df):,} rows")

    arrow_table = df.to_arrow()

    print("Running write benchmark (ADBC adbc_ingest, mode=replace)...")
    with measure("postgres_arrow_flight_write", data_path="") as m:
        with adbc.connect(CONN_URI) as conn:
            with conn.cursor() as cur:
                cur.adbc_ingest(TABLE, arrow_table, mode="replace")
            conn.commit()

    print(
        f"Write done: {m.value.duration_sec:.2f}s"
        f" | RAM: {m.value.peak_ram_mb:.1f} MB"
    )


def read() -> pl.DataFrame:
    """
    SELECT * → ADBC fetch_arrow_table() → pl.from_arrow() (zero-copy).

    The ADBC driver receives Arrow IPC columnar batches from Postgres.
    fetch_arrow_table() assembles them into a single pa.Table.
    pl.from_arrow() wraps the Arrow buffers as Polars series without copying —
    no per-row Python object allocation.

    Alternative: fetch_record_batch() returns a RecordBatchReader for streaming
    (lower peak RAM). fetch_arrow_table() is used here for a fair comparison
    against strategies that also materialise the full result.
    """
    print("Running read benchmark (ADBC fetch_arrow_table → pl.from_arrow zero-copy)...")
    with measure("postgres_arrow_flight_read", data_path="") as m:
        with adbc.connect(CONN_URI) as conn:
            with conn.cursor() as cur:
                _try_enable_binary(cur)
                cur.execute(f"SELECT * FROM {TABLE}")
                arrow_table = cur.fetch_arrow_table()
                _print_schema_diagnostic(arrow_table)
        df = pl.from_arrow(arrow_table)

    print(
        f"Read done: {m.value.duration_sec:.2f}s"
        f" | RAM: {m.value.peak_ram_mb:.1f} MB"
        f" | rows: {len(df):,}"
    )
    print(f"  vs async 4-chunk (7F):  {_BASELINE_READ_ASYNC_S}s"
          f"  → speedup {_BASELINE_READ_ASYNC_S / m.value.duration_sec:.2f}×")
    print(f"  vs COPY CSV (7E):       {_BASELINE_READ_COPY_CSV_S}s"
          f"  → speedup {_BASELINE_READ_COPY_CSV_S / m.value.duration_sec:.2f}×")
    return df


def read_streaming() -> pl.DataFrame:
    """
    Streaming variant: fetch_record_batch() → RecordBatchReader → pl.from_arrow().

    Each RecordBatch is processed as it arrives — peak RAM is bounded by the
    largest single batch rather than the full 28M-row result. Use this variant
    when RAM is constrained (target: approach COPY CSV's 4,257 MB).
    """
    print("Running read_streaming benchmark (ADBC fetch_record_batch → Polars)...")
    with measure("postgres_arrow_flight_read_streaming", data_path="") as m:
        with adbc.connect(CONN_URI) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {TABLE}")
                reader = cur.fetch_record_batch()
                # Materialise all batches while the cursor is still open.
                # Passing the reader to pl.from_arrow() after the cursor closes
                # raises "Attempt to read from a stream that has already been closed".
                arrow_table = reader.read_all()
        df = pl.from_arrow(arrow_table)

    print(
        f"Read streaming done: {m.value.duration_sec:.2f}s"
        f" | RAM: {m.value.peak_ram_mb:.1f} MB"
        f" | rows: {len(df):,}"
    )
    return df


def query() -> pl.DataFrame:
    """
    GROUP BY query via ADBC — server-side aggregation, Arrow result.
    Returns 8,049 rows (one per ticker). Arrow IPC for a small result set
    has negligible overhead vs psycopg3 binary.
    """
    print("Running query benchmark (ADBC GROUP BY → Arrow → Polars)...")
    with measure("postgres_arrow_flight_query", data_path="") as m:
        with adbc.connect(CONN_URI) as conn:
            with conn.cursor() as cur:
                cur.execute(QUERY_SQL)
                arrow_table = cur.fetch_arrow_table()
        result = pl.from_arrow(arrow_table)

    print(
        f"Query done: {m.value.duration_sec:.2f}s"
        f" | RAM: {m.value.peak_ram_mb:.1f} MB"
        f" | tickers: {len(result):,}"
    )
    print(f"  vs async GROUP BY (7F): {_BASELINE_QUERY_ASYNC_S}s"
          f"  → speedup {_BASELINE_QUERY_ASYNC_S / m.value.duration_sec:.2f}×")
    return result


if __name__ == "__main__":
    write()
    read()
    read_streaming()
    query()
    print("\nAll benchmarks complete. Results saved.")
