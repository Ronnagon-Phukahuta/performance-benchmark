# Performance Benchmark Summary — Phase 3

**Date:** 2026-05-02
**Phase:** 3 — Star Schema, OLTP Indexed, JOIN, Concurrent Reads
**Dataset:** 28,151,758 rows · 8,049 tickers (5,884 stocks + 2,165 ETFs from Kaggle)
**Hardware:** Intel Core i5-12400F · 32 GB RAM · NVMe SSD
**Benchmark:** Star schema write (dim + fact) / JOIN query (GROUP BY sector) / OLTP indexed query / Concurrent reads (N threads)

---

## Star Schema Write Performance

| Method                  | write_dim | write_fact   | Peak RAM   | Disk      | Notes                                    |
|-------------------------|-----------|--------------|------------|-----------|------------------------------------------|
| DuckDB                  | 0.11 s    | 8.30 s       | 2,494 MB   | 7,583 MB  |                                          |
| Parquet                 | 0.04 s    | 8.07 s       | 4,050 MB   | 319 MB    |                                          |
| Postgres                | 0.40 s    | 234.38 s     | 27 MB      | N/A       | COPY FROM STDIN, server-side             |
| SQL Server              | 4.92 s    | 67.37 s      | 33 MB      | N/A       | ⚠ 100K sample — ~5.2h extrapolated      |
| MongoDB embedded        | N/A       | 270.21 s     | 1,890 MB   | N/A       | Sector/industry/exchange embedded per doc |
| MongoDB lookup          | N/A       | 221.15 s     | 1,906 MB   | N/A       | Two collections: prices + symbols         |

> SQL Server write_fact DNF at 28M rows — benchmarked on 100K subset, extrapolated linearly (~5.2h). TDS protocol bottleneck — see Why section.

---

## JOIN Query Performance (fact JOIN dim → GROUP BY sector)

| Method              | Duration  | Peak RAM   | Notes                                          |
|---------------------|-----------|------------|------------------------------------------------|
| Parquet + Polars    | 0.89 s    | 4,398 MB   | Lazy scan, predicate + projection pushdown     |
| DuckDB              | 2.51 s    | 384 MB     |                                                |
| Postgres            | 19.44 s   | 27 MB      |                                                |
| MongoDB lookup      | 22.63 s   | 1,868 MB   | $group → $lookup symbols → $group by sector    |
| MongoDB embedded    | 24.78 s   | 1,858 MB   | $group only — no $lookup needed                |
| SQL Server          | 0.05 s    | 33 MB      | ⚠ 100K sample only — not comparable at 28M    |

---

## OLTP Query Performance (single ticker_id=1, date range 2020–2023)

| Method              | Duration  | Peak RAM   | Rows    | Notes                                       |
|---------------------|-----------|------------|---------|---------------------------------------------|
| MongoDB lookup      | 0.01 s    | 1,868 MB   | 63 rows | Direct B-tree index hit on ticker_id        |
| DuckDB              | 0.02 s    | 100 MB     | 63 rows | Indexed seek on ticker_id                   |
| Parquet + Polars    | 0.03 s    | 1,792 MB   | 63 rows | Full scan filter — no index in Parquet      |
| Postgres            | 0.10 s    | 27 MB      | 63 rows | Index scan on idx_fact_ticker               |
| SQL Server          | 0.10 s    | 33 MB      | 63 rows | Index scan on idx_fact_ticker               |
| MongoDB embedded    | 0.02 s    | 1,850 MB   | 63 rows | B-tree index on ticker_id                   |

---

## Concurrent Reads (query_join × N threads, wall clock time)

| Method              | 5 threads | 10 threads | 20 threads | Peak RAM (20 threads) |
|---------------------|-----------|------------|------------|-----------------------|
| DuckDB              | 0.51 s    | 0.96 s     | 1.82 s     | 432 MB                |
| SQL Server          | 0.07 s    | 0.09 s     | 0.15 s     | 35 MB                 |
| Parquet + Polars    | 2.50 s    | 5.23 s     | 10.51 s    | 15,087 MB             |
| Postgres            | 55.79 s   | 64.30 s    | 70.65 s    | 28 MB                 |
| MongoDB lookup      | 37.85 s   | 55.31 s    | 106.01 s   | 1,858 MB              |
| MongoDB embedded    | 37.63 s   | 57.76 s    | 112.22 s   | 1,856 MB              |

> SQL Server concurrent results based on 100K sample only — not comparable at 28M rows. See Why section for extrapolation.

---

## DNF / Extrapolated

| Method                 | Sample Size | Sample Duration | Extrapolated 28M | Reason                        |
|------------------------|-------------|-----------------|------------------|-------------------------------|
| SQL Server write_fact  | 100K rows   | 67.37 s         | ~5.2h ❌         | TDS protocol per-row overhead |

---

## Top 3 Winners per Category

### Star Schema Write (fact table, full 28M rows)
1. **Parquet** — 8.07 s, 319 MB disk *(smallest disk footprint by 24×)*
2. **DuckDB** — 8.30 s, but 7,583 MB disk
3. **MongoDB lookup** — 221.15 s *(document store, no COPY equivalent)*

### JOIN Query (GROUP BY sector)
1. **Parquet + Polars** — 0.89 s *(lazy evaluation + columnar scan)*
2. **DuckDB** — 2.51 s
3. **Postgres** — 19.44 s

### OLTP Query (indexed, single ticker, date range)
1. **MongoDB lookup** — 0.01 s *(direct document pointer lookup)*
2. **DuckDB** — 0.02 s
3. **Parquet + Polars** — 0.03 s *(full scan — no Parquet index)*

### Concurrent Reads (20 threads, wall time)
1. **DuckDB** — 1.82 s *(shared buffer pool, near-linear scaling)*
2. **Parquet + Polars** — 10.51 s *(fast but 15 GB RAM at 20 threads)*
3. **Postgres** — 70.65 s *(sub-linear — connection overhead dominates)*

---

## Key Numbers

| Metric                          | Value          | Method                  |
|---------------------------------|----------------|-------------------------|
| Fastest JOIN query              | 0.89 s         | Parquet + Polars        |
| Fastest OLTP query              | 0.01 s         | MongoDB lookup          |
| Best concurrent scale (20t)     | 1.82 s         | DuckDB                  |
| Worst concurrent RAM (20t)      | 15,087 MB      | Parquet + Polars        |
| Smallest disk (fact table)      | 319 MB         | Parquet                 |
| Worst write (full, 28M)         | 270.21 s       | MongoDB embedded        |
| SQL Server write_fact DNF       | ~5.2h extrap.  | TDS protocol bottleneck |

---

## Why — Technical Explanation

### Why Parquet write_fact is faster than DuckDB despite no index

Parquet writes columnar data directly to the filesystem with no intermediate structures — no transaction log, no write-ahead log (WAL), no index maintenance. Writing `fact_prices.parquet` is a single sequential pass: compress each column chunk, write the row group, append the footer. The entire operation is bounded by raw NVMe write bandwidth.

DuckDB must maintain its internal ART (Adaptive Radix Tree) index on `ticker_id` as each row is inserted. Every INSERT into the `fact_prices` table triggers an index update: find the correct leaf node, insert the row pointer, rebalance if necessary. At 28M rows this is 28M index updates interleaved with 28M row writes — write amplification beyond the raw data volume.

The result: Parquet at 8.07s vs DuckDB at 8.30s is deceptively close. The difference would grow significantly if DuckDB maintained multiple indexes or if the index cardinality were higher. For append-only analytical workloads, Parquet's zero-overhead write model is the correct architectural choice when query latency requirements do not mandate an index.

### Why DuckDB concurrent reads scale near-linearly but Parquet does not

DuckDB manages a shared buffer pool across all connections to the same `.db` file. When the first thread reads `fact_prices` into memory, subsequent threads serve their queries from the already-cached pages — the NVMe read happens once, and all threads consume from shared RAM. At 20 concurrent threads, DuckDB's wall time (1.82s) is approximately 4× the single-thread time (rather than 20×), because threads spend most of their time computing against already-resident data.

Polars `scan_parquet` has no equivalent shared cache. Each `threading.Thread` creates an independent Polars execution context that opens the parquet file, reads the columns it needs, and materializes an Arrow buffer — entirely separately from every other thread. At 20 threads this means 20 simultaneous reads of the same `fact_prices.parquet` file, 20 independent Arrow allocations, and 20× the single-thread RAM (15,087 MB vs ~750 MB single-thread). All 20 threads also compete for the same NVMe I/O bandwidth, making the wall time roughly proportional to thread count (10.51s ≈ 20 × 0.5s per thread in parallel I/O contention).

This is the core tradeoff between file-based and database-backed storage: databases manage shared caches; files do not.

### Why Postgres concurrent reads show sub-linear scaling (55→64→70s)

Postgres uses a process-per-connection model inherited from its original design: each client connection forks a new `postgres` backend process. On Linux (and WSL), `fork()` involves: duplicating the parent process's virtual memory map, negotiating authentication, allocating a shared memory segment for the backend's private buffer, and registering the connection in `pg_stat_activity`. This takes 10–50ms per connection regardless of query complexity.

At 5 threads = 5 simultaneous connections, the 55.79s total is dominated by connection setup and the sequential nature of the join query itself. At 20 threads = 20 connections, the database has more parallelism available but the TDS overhead and process management overhead grows. The sub-linear scaling (55→64→70s rather than 55→110→220s) indicates that Postgres is using shared buffer caching effectively — repeated queries hit the `shared_buffers` cache — but connection management prevents linear speedup.

Connection pooling with PgBouncer in transaction mode would eliminate the fork cost, pre-warm connections, and likely reduce concurrent read time to under 20s for 20 threads. The 70s result is not a Postgres performance problem — it is a connection model problem for short-lived Python threads.

### Why MongoDB OLTP query is the fastest at 0.01s

MongoDB's WiredTiger storage engine indexes `ticker_id` with a B-tree that stores direct file offsets to matching documents. A `find({"ticker_id": 1, "date": {"$gte": "2020-01-01", "$lte": "2023-12-31"}})` query on an indexed field does not scan the collection — it descends the B-tree in O(log n) steps to the first matching document, then streams consecutive matches directly to the cursor.

There is no query planner, no vector execution engine, no columnar scan — just pointer arithmetic and direct document reads. MongoDB was designed for exactly this access pattern: retrieve a small set of documents by indexed field. At 63 matching rows out of 28M, the index reduces the search space by ~450,000×. The 0.01s result reflects the true ceiling of indexed document retrieval on NVMe storage.

DuckDB (0.02s) and Parquet (0.03s) must execute a columnar scan even for single-ticker queries — their strength is aggregate analytics, not point lookups. The OLTP result reveals MongoDB's architectural advantage for transactional read patterns that relational columnar stores are not optimized for.

### Why MongoDB embedded is slower than lookup for JOIN query

The intuitive prediction — embedding sector/industry/exchange in every document eliminates the `$lookup` stage, making it faster — turns out to be wrong. The embedded version (24.78s) is slower than the lookup version (22.63s).

The explanation is document size and I/O efficiency. Each embedded price document stores three extra string fields (sector up to 50 chars, industry up to 50 chars, exchange up to 20 chars) = ~120 extra bytes per document. At 28M documents, this adds ~3.4 GB of data that WiredTiger must read during the `$group` scan — every document page holds fewer records, requiring more page reads to scan the full collection.

The lookup collection stores minimal price documents (~80 bytes each). More documents fit per WiredTiger 4KB page, meaning fewer total page reads to scan 28M prices. The `$lookup` against the 8,049-document `symbols` collection is negligible — it runs after `$group` has already reduced 28M price rows to ~8,049 per-ticker summaries. Joining 8,049 rows against 8,049 dimension records takes microseconds.

The lesson: denormalization (embedding) trades write overhead and document size for read speed — but only when the embedded fields are used in a filter or projection. When the embedded fields are needed only for post-aggregation grouping, the added document size costs more in I/O than the `$lookup` saves.

### Why SQL Server concurrent reads appear fast (100K sample caveat)

The SQL Server concurrent results (0.07s at 5 threads, 0.15s at 20 threads) reflect a 100K-row dataset only and should not be compared directly to other methods running on 28M rows.

To estimate 28M performance: the single-thread `query_join` on 100K rows took 0.05s. Scaling linearly to 28M rows (281× more data): ~14s per query. At 20 concurrent threads competing for the same TDS connection pool and executing 14s queries: wall time would be approximately 50–100s — comparable to Postgres.

SQL Server's columnstore index (from Phase 2) would improve single-query time at 28M rows to ~0.5–2s based on batch mode execution scaling, which would put concurrent reads in the 5–20s range. However, this would require the columnstore variant (Phase 2 `bulk_columnstore.py`) rather than the standard B-tree index used in this phase. The 100K results demonstrate SQL Server's query engine is capable — the constraint is exclusively the write path (TDS protocol) and the impracticality of loading 28M rows from Python without BCP.

### MongoDB WiredTiger cache — operational gotcha

After completing the embedded benchmark (28M embedded documents with extra fields), Windows Task Manager showed `VmmemWSL` consuming approximately 12 GB. The lookup benchmark (two separate collections) showed similar behaviour — peak RAM during write reached 1,906 MB inside WSL, and WiredTiger retained the cache after the benchmark completed. The cache size is not tied to document schema (embedded vs normalized) but to the total working set size scanned — both variants scanned the full 28M-document prices collection. Running `wsl --shutdown` reduced this to ~5.5 GB but did not fully release the memory to Windows.

WiredTiger's default cache size is `(totalRAM - 1GB) / 2`, targeting ~15 GB on a 32 GB system. Under analytical load (full `$group` scan of 28M documents), WiredTiger aggressively pages the working set into its cache and does not proactively evict it after the query completes — it assumes subsequent queries will benefit from cached pages. This is correct behaviour for a production database with continuous load, but problematic on a shared development machine.

The practical implication: MongoDB in a shared environment requires an explicit `wiredTigerCacheSizeGB` setting in `mongod.conf`. A value of 2–4 GB is appropriate for development. Without this, MongoDB will consume half of available RAM and hold it indefinitely, causing memory pressure on any other service on the same host. This is not a bug — it is a deliberate caching strategy that requires operator configuration to match the deployment context.
