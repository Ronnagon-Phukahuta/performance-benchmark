"""
Phase 7F — psycopg3 Async Pipeline: Concurrent Reads via asyncio

Goal: overlap Postgres I/O across 4 concurrent connections to hide network
and server execution latency, pushing read time below the Phase 7E COPY CSV
winner (67.23s).

Strategy:
  - Split the table into 4 physical page ranges using ctid
  - Each range is fetched by an independent AsyncConnection (4 Postgres backends)
  - asyncio.gather() runs all 4 concurrently — I/O overlaps in the event loop
  - Results are concatenated into a single Polars DataFrame

Why ctid ranges (not OFFSET/LIMIT):
  OFFSET n performs a full heap scan and discards n rows before returning — O(n)
  for each chunk. 4 chunks with OFFSET would read 4× the total data.
  ctid filtering reads only the physical pages in the target range — true O(n/4)
  per chunk. Total data read = same as a single sequential scan.

Why asyncio (not threading):
  psycopg3 async uses libpq non-blocking mode under asyncio. Each
  `await cur.fetchall()` yields the event loop while waiting for Postgres to
  send the next batch of data. While one chunk waits on the network, another
  chunk's server-side query is executing. Threading would achieve similar I/O
  overlap but with OS thread overhead; asyncio is lighter.

Bottleneck this cannot fix:
  Python deserialization is still single-threaded (GIL). Four concurrent
  connections return data in overlapping windows, but building Python tuples
  and Polars DataFrames still happens on one core. The benefit is server-side
  parallelism + I/O overlap, not parallel Python execution.

Interface (same as all other loaders):
  write()  — async COPY FROM STDIN via AsyncConnection
  read()   — 4 concurrent ctid-range SELECTs via asyncio.gather()
  query()  — single async SELECT GROUP BY

Phase 7E baselines (same table structure, same data):
  SELECT binary direct:   123.77s  20,719 MB
  COPY CSV + Polars:       67.23s   4,257 MB  ← current best
"""

import asyncio
import io
import os
import selectors
import sys

import polars as pl
import psycopg
import pyarrow.csv as pa_csv

from benchmark.metrics import measure


def run_async(coro):
    """
    Run an async coroutine from sync code.
    On Windows, Python 3.8+ defaults to ProactorEventLoop which psycopg3
    does not support. Force SelectorEventLoop so libpq non-blocking I/O works.
    """
    if sys.platform == "win32":
        loop = asyncio.SelectorEventLoop(selectors.SelectSelector())
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()
    else:
        return asyncio.run(coro)


RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")

DB_PARAMS = dict(
    host="localhost",
    port=5432,
    dbname="benchmark_db",
    user="benchmark",
    password="benchmark",
)

TABLE = "stocks_async_pipeline"
COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume"]
N_CHUNKS = 4

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

QUERY_SQL = f"""
    SELECT ticker,
           AVG(close)  AS avg_close,
           MAX(close)  AS max_close,
           MIN(close)  AS min_close
    FROM {TABLE}
    GROUP BY ticker
"""


# ---------------------------------------------------------------------------
# Async helpers
# ---------------------------------------------------------------------------

async def _get_page_count() -> int:
    """
    Query pg_relation_size for exact page count — always current, no ANALYZE needed.
    Divides by 8192 (Postgres default block size) to get number of heap pages.
    """
    async with await psycopg.AsyncConnection.connect(**DB_PARAMS) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT ceil(pg_relation_size(%s::regclass) / 8192.0)::bigint",
                (TABLE,),
            )
            row = await cur.fetchone()
    return int(row[0]) if row and row[0] else 250_000


async def _fetch_chunk(
    start_page: int,
    end_page: int,
    chunk_id: int,
) -> pl.DataFrame:
    """
    Fetch one physical page range from the table.

    ctid filtering reads only the target heap pages — no full scan, no row
    discard. Each coroutine opens its own AsyncConnection so all 4 run on
    separate Postgres backend processes.

    binary=True on the cursor: IEEE-754 bytes instead of ASCII text per row.
    The deserialization cost is unavoidable here since we need typed Python
    values for Polars. (COPY FORMAT CSV variant is in copy_binary.py.)
    """
    query = (
        f"SELECT * FROM {TABLE} "
        f"WHERE ctid >= '({start_page},0)'::tid "
        f"AND ctid < '({end_page},0)'::tid"
    )
    async with await psycopg.AsyncConnection.connect(**DB_PARAMS) as conn:
        async with conn.cursor(binary=True) as cur:
            await cur.execute(query)
            rows = await cur.fetchall()
            cols = [d.name for d in cur.description] if cur.description else COLUMNS

    if not rows:
        return pl.DataFrame(schema=_SCHEMA)

    print(f"    chunk {chunk_id} done: {len(rows):,} rows (pages {start_page}–{end_page})")
    return pl.DataFrame(
        {col: [row[i] for row in rows] for i, col in enumerate(cols)}
    )


async def _async_read() -> pl.DataFrame:
    """
    Get page count, split into N_CHUNKS ranges, fetch all concurrently.
    asyncio.gather() schedules all 4 coroutines; each yields on await,
    letting the others advance while Postgres processes their queries.
    """
    total_pages = await _get_page_count()
    chunk_size = max(1, total_pages // N_CHUNKS)

    page_ranges = [
        (
            i * chunk_size,
            (i + 1) * chunk_size if i < N_CHUNKS - 1 else total_pages + 1,
        )
        for i in range(N_CHUNKS)
    ]

    print(f"    total pages: {total_pages:,} | chunk size: ~{chunk_size:,} pages each")

    dfs = await asyncio.gather(
        *[
            _fetch_chunk(start, end, idx)
            for idx, (start, end) in enumerate(page_ranges)
        ]
    )
    return pl.concat(dfs)


async def _async_write(buf: io.BytesIO) -> None:
    """
    Async COPY FROM STDIN. Functionally identical to Phase 7E write — async
    COPY does not improve single-connection bulk write throughput, but tests
    the AsyncConnection COPY API end-to-end.
    """
    async with await psycopg.AsyncConnection.connect(**DB_PARAMS) as conn:
        async with conn.cursor() as cur:
            await cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
            await cur.execute(CREATE_TABLE_SQL)
            async with cur.copy(
                f"COPY {TABLE} ({', '.join(COLUMNS)}) FROM STDIN (FORMAT CSV, HEADER TRUE)"
            ) as copy:
                await copy.write(buf.read())
        await conn.commit()


async def _async_query() -> pl.DataFrame:
    """Single async GROUP BY query — server-side aggregation, binary result."""
    async with await psycopg.AsyncConnection.connect(**DB_PARAMS) as conn:
        async with conn.cursor(binary=True) as cur:
            await cur.execute(QUERY_SQL)
            rows = await cur.fetchall()
            cols = [d.name for d in cur.description] if cur.description else []

    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(
        {col: [row[i] for row in rows] for i, col in enumerate(cols)}
    )


# ---------------------------------------------------------------------------
# Sync benchmark interface (wraps asyncio.run for metrics.py compatibility)
# ---------------------------------------------------------------------------

def write() -> None:
    """
    Load CSV → Polars → PyArrow CSV buffer → async COPY FROM STDIN.
    The async wrapper has no throughput benefit for a single bulk write —
    this benchmarks the AsyncConnection COPY API and establishes the table.
    """
    print("Loading CSV...")
    df = pl.read_csv(RAW_CSV)
    df.columns = [c.lower() for c in df.columns]
    df = df.select(COLUMNS)
    df = df.with_columns(pl.col("volume").fill_null(0).cast(pl.Int64))
    print(f"Loaded {len(df):,} rows")

    arrow_table = df.to_arrow()
    buf = io.BytesIO()
    pa_csv.write_csv(arrow_table, buf)
    buf.seek(0)

    print("Running write benchmark (psycopg3 async COPY FROM STDIN)...")
    with measure("postgres_async_pipeline_write", data_path="") as m:
        run_async(_async_write(buf))

    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB")


def read() -> pl.DataFrame:
    """
    Fetch 28M rows using 4 concurrent AsyncConnections, each reading one
    physical ctid page range. asyncio.gather() overlaps server processing
    and I/O across all 4 chunks.

    Phase 7E baselines:
      SELECT binary direct:  123.77s  20,719 MB
      COPY CSV + Polars:      67.23s   4,257 MB
    """
    print(f"Running read benchmark ({N_CHUNKS} concurrent async chunks via ctid ranges)...")
    with measure("postgres_async_pipeline_read", data_path="") as m:
        df = run_async(_async_read())

    print(
        f"Read done: {m.value.duration_sec:.2f}s"
        f" | RAM: {m.value.peak_ram_mb:.1f} MB"
        f" | rows: {len(df):,}"
    )
    print(f"  Phase 7E COPY CSV baseline: 67.23s / 4,257 MB")
    speedup = 67.23 / m.value.duration_sec
    print(f"  speedup vs COPY CSV: {speedup:.2f}×")
    return df


def query() -> pl.DataFrame:
    """
    Single async GROUP BY query — Postgres executes the aggregation server-side
    and returns 8,049 rows (one per ticker). Compare to Phase 7E COPY→DuckDB (76s).
    """
    print("Running query benchmark (async SELECT GROUP BY, server-side aggregation)...")
    with measure("postgres_async_pipeline_query", data_path="") as m:
        result = run_async(_async_query())

    print(
        f"Query done: {m.value.duration_sec:.2f}s"
        f" | RAM: {m.value.peak_ram_mb:.1f} MB"
        f" | tickers: {len(result):,}"
    )
    print(f"  Phase 7E COPY→DuckDB baseline: 76.75s  (expected improvement)")
    print(f"  Phase 1  Postgres native:       24.27s  (expected reference)")
    return result


if __name__ == "__main__":
    write()
    read()
    query()
    print("\nAll benchmarks complete. Results saved.")
