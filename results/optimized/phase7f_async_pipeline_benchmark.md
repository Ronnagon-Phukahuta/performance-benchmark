# Performance Benchmark Summary — Phase 7F

**Date:** 2026-05-19
**Phase:** 7F — psycopg3 Async Pipeline: Concurrent Reads via asyncio + ctid Chunking
**Dataset:** 28,151,758 rows · 8,049 tickers (5,884 stocks + 2,165 ETFs from Kaggle)
**Hardware:** Intel Core i5-12400F · 32 GB RAM · NVMe SSD
**Benchmark:** Async write / 4-concurrent-chunk read / async server-side query

---

Phase 7F tests whether asyncio concurrency can push Postgres read time below the
Phase 7E COPY CSV winner (67.23s). Four `AsyncConnection`s each fetch one physical
ctid page range concurrently via `asyncio.gather()`, overlapping server execution
and I/O across 4 Postgres backend processes.

The result: a marginal read improvement (1.05×), a significant query improvement
(1.32× vs native Postgres), and a clear demonstration of where the GIL imposes
a ceiling.

---

## Write (28,151,758 rows — async COPY FROM STDIN)

| Method                      | Duration   | Peak RAM   | vs psycopg2 baseline |
|-----------------------------|------------|------------|----------------------|
| psycopg2 baseline (Phase 1) | 263.43s    | 16,352 MB  | —                    |
| psycopg3 sync (Phase 7E)    | 262.40s    | 7,905.9 MB | ~same                |
| psycopg3 async (Phase 7F)   | 245.09s    | 8,016.6 MB | -6.9%                |

**Finding:** Async COPY FROM STDIN is marginally faster than sync (245s vs 262s).
The improvement is within noise for a single-connection bulk write — async has no
structural advantage here. The table is created and data written identically; the
small gain likely reflects page cache and OS scheduler differences between runs.

---

## Read (SELECT * — 28,151,758 rows, 4 concurrent ctid chunks)

| Method                              | Duration   | Peak RAM     | vs psycopg2 (159s) | Speedup |
|-------------------------------------|------------|--------------|---------------------|---------|
| psycopg2 baseline (Phase 1)         | 159.16s    | 19,125 MB    | —                   | —       |
| COPY CSV + Polars SIMD (Phase 7E)   | 67.23s     | 4,256.6 MB   | -58%                | 2.37×   |
| **async 4-chunk ctid (Phase 7F)**   | **63.75s** | **15,940.5 MB** | **-60%**         | **2.49×** |

**Speedup vs COPY CSV (Phase 7E): 1.05×**

**Finding:** Async concurrency delivered only 1.05× improvement over COPY CSV,
despite 4 parallel Postgres backends. The GIL caps the gain — Python deserialization
(building tuples, then Polars DataFrame) is single-threaded regardless of how many
connections deliver data concurrently.

**RAM tradeoff:** 15,940 MB (async, 4× SELECT binary buffers) vs 4,257 MB (COPY CSV).
Async read is faster by 3.5s but costs 11.7 GB more RAM. On memory-constrained systems,
COPY CSV remains the better choice.

### ctid chunking vs OFFSET — why it matters

```
OFFSET-based (naive approach):
  Chunk 0: SELECT * OFFSET 0 LIMIT 7M       → reads pages 0–N, returns 7M rows
  Chunk 1: SELECT * OFFSET 7M LIMIT 7M      → reads ALL pages, discards 7M, returns 7M
  Chunk 2: SELECT * OFFSET 14M LIMIT 7M     → reads ALL pages, discards 14M, returns 7M
  Chunk 3: SELECT * OFFSET 21M LIMIT 7M     → reads ALL pages, discards 21M, returns 7M
  Total I/O: 4× full table scan

ctid-based (implemented):
  Chunk 0: WHERE ctid >= '(0,0)' AND ctid < '(62500,0)'    → reads pages 0–62499
  Chunk 1: WHERE ctid >= '(62500,0)' AND ctid < '(125000,0)' → reads pages 62500–124999
  Chunk 2: WHERE ctid >= '(125000,0)' AND ctid < '(187500,0)' → reads pages 125000–187499
  Chunk 3: WHERE ctid >= '(187500,0)'                         → reads pages 187500–end
  Total I/O: 1× full table scan, split across 4 processes
```

---

## Query (async SELECT GROUP BY — server-side aggregation)

| Method                              | Duration   | Peak RAM   | vs Postgres native | Speedup |
|-------------------------------------|------------|------------|---------------------|---------|
| psycopg2 native GROUP BY (Phase 1)  | 24.27s     | 3,474 MB   | —                   | —       |
| COPY BINARY → DuckDB (Phase 7E)     | 76.75s     | 15,563 MB  | 3.16× slower        | —       |
| **async SELECT GROUP BY (Phase 7F)**| **18.31s** | **4,630 MB** | **-25%**          | **1.32×** |

**vs Phase 7E COPY→DuckDB: 4.19× faster**

**Finding:** Async server-side GROUP BY (18.31s) is the fastest query result across
all phases — beating even the Phase 1 psycopg2 native query (24.27s) by 25%. The
improvement over Phase 1 reflects binary protocol (psycopg3) + warm page cache
from the preceding read benchmark. The 4.19× gain over COPY→DuckDB confirms the
principle from Phase 7E: push aggregation to the server.

---

## Key Insights

**Insight 1 — The GIL is the ceiling on async I/O parallelism**

asyncio.gather() runs 4 coroutines on a single OS thread. When one coroutine is
`await`-ing data from Postgres (blocked on I/O), the event loop can schedule another.
Server-side, 4 PostgreSQL backend processes execute queries in parallel on separate
CPU cores — genuine parallelism at the server. Client-side, Python builds tuples and
Polars DataFrames on one core. The I/O overlap benefit (server parallelism, network
interleaving) is real but small for this workload where deserialization dominates.
Result: 1.05× over COPY CSV, not 4×.

**Insight 2 — ctid chunking is the prerequisite for async read to be valid**

OFFSET-based chunking would make 4 async reads slower than 1 sequential read:
4 full table scans = 4× disk I/O. ctid page-range splitting reads each physical
page exactly once across all 4 chunks — total I/O identical to a single scan.
The concurrency benefit only exists because the I/O is genuinely partitioned.

**Insight 3 — Async query beats native sync query, but the reason is protocol not concurrency**

18.31s (async binary) vs 24.27s (psycopg2 text). A single `SELECT GROUP BY` has
no concurrent chunks — it is structurally identical to the Phase 1 query. The 25%
improvement comes from two compounding factors: (1) psycopg3 binary protocol for
result rows and (2) warm OS page cache from the preceding read, not from asyncio.
The lesson: benchmark in isolation before attributing gains to the technique under test.

**Insight 4 — RAM is the decisive constraint when choosing between async and COPY CSV**

| Strategy             | Read speed | Peak RAM  | Use case                         |
|----------------------|------------|-----------|----------------------------------|
| COPY CSV + Polars    | 67.23s     | 4,257 MB  | RAM-constrained, ~32 GB system   |
| Async 4-chunk SELECT | 63.75s     | 15,940 MB | Speed-first, RAM headroom ≥ 20GB |

3.5s faster at the cost of 11.7 GB additional RAM. On this 32 GB system, async
read would leave only ~10 GB free during the operation. COPY CSV remains the
correct default; async is viable only with significant RAM headroom.

**Insight 5 — Progressive optimisation summary through Phase 7F**

| Phase | Strategy                     | Read    | RAM      | Query  |
|-------|------------------------------|---------|----------|--------|
| 1     | psycopg2 + Pandas            | 159s    | 19,125MB | 24.3s  |
| 7B    | psycopg2 + Polars            | 141s    | 17,622MB | 21.1s  |
| 7D    | psycopg3 + pgBouncer         | 109s    | —        | 19s    |
| 7E    | COPY BINARY                  | 81s     | 13,732MB | —      |
| 7E    | COPY CSV + Polars *(RAM win)*| 67s     | 4,257MB  | —      |
| 7F    | async 4-chunk *(speed win)*  | **63s** | 15,940MB | **18s** |

Starting from 159s / 19 GB, the chain reached 63s / 16 GB (speed path) or
67s / 4.3 GB (RAM path). Both beat the original by 2.4–2.5×. The next barrier
is Python object creation itself — Phase 7G (ADBC Arrow IPC) targets zero-copy
Arrow transfer that eliminates tuple allocation entirely.
