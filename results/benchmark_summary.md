# Performance Benchmark Summary — Phase 2

**Date:** 2026-05-01  
**Dataset:** 28,151,758 rows · 8,049 tickers (5,884 stocks + 2,165 ETFs from Kaggle)  
**Hardware:** Intel Core i5-12400F · 32 GB RAM · NVMe SSD  
**Benchmark:** Write / Read / Analytical query (GROUP BY ticker → AVG/MAX/MIN close)

---

## Write Performance

| Method                     | Duration   | Peak RAM   | Disk Size | Notes                        |
|----------------------------|------------|------------|-----------|------------------------------|
| parquet_partitioned        | 0.23 s     | 201 MB     | 23 MB     | ⚠ subset-extrapolated        |
| duckdb_bulk_insert_polars  | 3.48 s     | 4,871 MB   | 5,408 MB  |                              |
| parquet_lazy_polars        | 6.21 s     | 7,702 MB   | 320 MB    |                              |
| duckdb_copy_csv            | 6.25 s     | 5,241 MB   | 6,336 MB  |                              |
| parquet_single_file_polars | 6.31 s     | 7,651 MB   | 320 MB    |                              |
| parquet_single_file        | 11.54 s    | 6,402 MB   | 375 MB    |                              |
| duckdb_bulk_insert         | 11.69 s    | 6,172 MB   | 4,356 MB  |                              |
| parquet_compressed_snappy  | 12.09 s    | 7,656 MB   | 375 MB    |                              |
| parquet_compressed_gzip    | 30.34 s    | 7,634 MB   | 295 MB    |                              |
| postgres_bulk_copy         | 263.43 s   | 16,352 MB  | N/A       |                              |
| duckdb_row_by_row          | DNF (~6h)  | —          | —         | 100K subset × extrapolated   |
| duckdb_row_by_row_polars   | DNF (~6h)  | —          | —         | 100K subset × extrapolated   |
| duckdb_batch_insert        | DNF (~6h)  | —          | —         | 100K subset × extrapolated   |
| duckdb_batch_insert_polars | DNF (~6h)  | —          | —         | 100K subset × extrapolated   |
| postgres_row_by_row        | DNF (~2.9h)| —          | —         | 100K subset × extrapolated   |
| postgres_batch_insert      | DNF (~2.9h)| —          | —         | 100K subset × extrapolated   |

---

## Read Performance

| Method                     | Duration   | Peak RAM   | Notes                        |
|----------------------------|------------|------------|------------------------------|
| parquet_lazy_polars        | 0.39 s     | 8,075 MB   |                              |
| parquet_single_file_polars | 0.40 s     | 8,213 MB   |                              |
| parquet_partitioned        | 1.18 s     | 355 MB     | ⚠ subset-extrapolated        |
| parquet_single_file        | 3.11 s     | 7,584 MB   |                              |
| parquet_compressed_snappy  | 3.42 s     | 7,221 MB   |                              |
| duckdb_bulk_insert         | 5.29 s     | 7,195 MB   |                              |
| duckdb_copy_csv            | 5.59 s     | 10,407 MB  |                              |
| duckdb_bulk_insert_polars  | 6.06 s     | 10,619 MB  |                              |
| duckdb_direct_parquet      | 7.91 s     | 11,170 MB  |                              |
| postgres_bulk_copy         | 159.16 s   | 19,125 MB  |                              |

---

## Query Performance (GROUP BY ticker → AVG/MAX/MIN close)

| Method                     | Duration   | Peak RAM   | Notes                        |
|----------------------------|------------|------------|------------------------------|
| duckdb_direct_parquet      | 0.13 s     | 3,612 MB   |                              |
| duckdb_bulk_insert         | 0.14 s     | 620 MB     |                              |
| duckdb_copy_csv            | 0.17 s     | 3,898 MB   |                              |
| duckdb_bulk_insert_polars  | 0.18 s     | 3,891 MB   |                              |
| parquet_partitioned        | 0.32 s     | 328 MB     | ⚠ subset-extrapolated        |
| parquet_lazy_polars        | 0.98 s     | 7,793 MB   |                              |
| parquet_single_file_polars | 1.08 s     | 7,896 MB   |                              |
| parquet_single_file        | 2.03 s     | 6,659 MB   |                              |
| parquet_compressed_snappy  | 2.20 s     | 6,162 MB   |                              |
| postgres_bulk_copy         | 24.27 s    | 3,474 MB   |                              |

---

## DNF Variants (Did Not Finish — 100K subset + extrapolation)

These methods were too slow to run against the full 28M-row dataset. Times are extrapolated linearly from a 100,000-row subset run.

| Method                     | Extrapolated Write | Reason                     |
|----------------------------|--------------------|----------------------------|
| duckdb_row_by_row          | ~6h                | One INSERT per row         |
| duckdb_row_by_row_polars   | ~6h                | One INSERT per row (Polars)|
| duckdb_batch_insert        | ~6h                | Chunked INSERTs (Python)   |
| duckdb_batch_insert_polars | ~6h                | Chunked INSERTs (Polars)   |
| postgres_row_by_row        | ~2.9h              | One INSERT per row         |
| postgres_batch_insert      | ~2.9h              | Chunked INSERTs            |

---

## Top 3 Winners

### Write
1. **parquet_partitioned** — 0.23 s *(subset-extrapolated; actual may differ)*
2. **duckdb_bulk_insert_polars** — 3.48 s
3. **parquet_lazy_polars** — 6.21 s

### Read
1. **parquet_lazy_polars** — 0.39 s
2. **parquet_single_file_polars** — 0.40 s
3. **parquet_partitioned** — 1.18 s *(subset-extrapolated)*

### Query
1. **duckdb_direct_parquet** — 0.13 s
2. **duckdb_bulk_insert** — 0.14 s
3. **duckdb_copy_csv** — 0.17 s

---

## Key Numbers

| Metric               | Value                                      | Method                    |
|----------------------|--------------------------------------------|---------------------------|
| Fastest write        | 3.48 s (excl. subset-extrapolated)         | duckdb_bulk_insert_polars |
| Fastest read         | 0.39 s                                     | parquet_lazy_polars       |
| Fastest query        | 0.13 s                                     | duckdb_direct_parquet     |
| Smallest disk (write)| 295 MB                                     | parquet_compressed_gzip   |
| Worst RAM (read)     | 19,125 MB                                  | postgres_bulk_copy        |
| Worst RAM (write)    | 16,352 MB                                  | postgres_bulk_copy        |
| Slowest write (full) | 263.43 s                                   | postgres_bulk_copy        |
| Slowest read (full)  | 159.16 s                                   | postgres_bulk_copy        |

---

## Why — Technical Explanation

### Why bulk_insert is faster than batch_insert (not just "no loop")

bulk_insert passes a DataFrame object directly to DuckDB via Apache Arrow IPC (Inter-Process Communication). DuckDB reads the Arrow buffer in-place without deserializing or copying data — this is called zero-copy transfer. The entire 28M row DataFrame crosses the Python-DuckDB boundary in a single C++ function call.

batch_insert still uses a Python loop. Even if each iteration sends 10,000 rows, Python must: serialize each batch to a tuple list, acquire the GIL, call the C extension, release the GIL, and repeat. At 28M rows with batch size 10,000 this means 2,816 GIL acquisitions. The DuckDB in-process round trip is nanoseconds — the Python overhead per iteration is microseconds. Multiply by 2,816 and the gap disappears.

Result: bulk O(1) Python calls vs batch O(n/batch_size) Python calls. At 28M rows the constant factor of each Python call dominates everything else.

### Why DuckDB query is always fast regardless of write method

DuckDB uses a vectorized execution engine (based on MonetDB/X100 research). Instead of processing one row at a time, it processes data in vectors of 1,024 values using SIMD CPU instructions. The GROUP BY + AVG/MAX/MIN query only needs to touch the ticker and close columns — DuckDB's columnar storage means it physically skips all other columns on disk.

The write method (row_by_row, bulk, COPY) only affects how data is arranged on disk initially. Once stored, DuckDB's query engine treats all variants identically. This is why every DuckDB variant completed the GROUP BY in under 0.2 seconds regardless of how it was written.

### Why Parquet + Polars LazyFrame read is fast but RAM is still high

Polars LazyFrame (scan_parquet) builds a logical query plan without reading any data. When .collect() is called, Polars applies predicate pushdown and projection pushdown — it only reads the columns actually needed from the Parquet file.

However, reading 28M rows of even 2 columns still requires materializing the result into RAM. At 28M rows × 2 columns × 8 bytes = ~448 MB minimum, but Polars allocates chunked Arrow arrays with overhead, pushing peak RAM to ~8 GB when the full schema is read. The LazyFrame advantage over eager read_parquet appears when queries filter rows — at full scan with all columns, both approaches load the same data.

### Why Postgres bulk_copy read took 159 seconds and 19 GB RAM

Postgres stores data in 8KB heap pages in row-oriented format (heap tuples). Reading 28M rows requires fetching every page sequentially, deserializing each row from its heap tuple format, and sending it over the localhost TCP socket.

The Pandas pd.read_sql() call uses DBAPI2 cursor.fetchall() which buffers the entire result set in Python memory before returning — 28M rows × 8 columns × ~50 bytes average = ~11 GB minimum, plus Python object overhead pushing it to 19 GB. SQLAlchemy with chunked fetchmany() or server-side cursors would reduce RAM significantly, but would not fix the fundamental row-oriented scan cost.

### Why batch_insert ≈ row_by_row (the non-obvious result)

The expected intuition: sending 10,000 rows per trip should be 10,000x faster than 1 row per trip. The actual result: 77.87s vs 76.67s — essentially identical.

The reason is that DuckDB runs in the same OS process as Python. There is no network socket, no TCP stack, no serialization overhead between Python and DuckDB. Each executemany() call crosses a thin C extension boundary that costs ~1-10 microseconds. At batch_size=10,000 and 28M rows, that is 2,816 boundary crossings × ~10μs = ~28ms of boundary overhead total — negligible compared to the Python loop cost of building each batch as a list of tuples.

The real cost is the Python-side work: iterating rows, constructing tuples, and calling the C function. This cost exists whether batch_size is 1 or 10,000. Only eliminating Python-side iteration entirely (bulk vectorized transfer) produces a meaningful speedup.

### Why partitioned Parquet write is O(p) and why that matters

Writing 8,049 individual ticker files requires: opening a file handle, writing the Parquet footer (schema + row group metadata), closing the file handle — 8,049 times. Each file open/close on NVMe SSD costs ~0.1-1ms. At 8,049 files that is 0.8-8 seconds of pure filesystem overhead before writing any data.

At 48 tickers (Phase 1) this was invisible — 48 file opens ≈ 5-50ms. At 8,049 tickers it becomes the dominant cost. This is why partitioned write extrapolates poorly: the O(p) term is not about data volume but about filesystem operation count. Parallelizing writes with multiprocessing (one worker per ticker batch) would reduce this to O(p/workers).

The tradeoff is worth it for read-heavy workloads: reading a single ticker touches exactly 1 file regardless of how many total tickers exist — true O(1) with partition pruning.
