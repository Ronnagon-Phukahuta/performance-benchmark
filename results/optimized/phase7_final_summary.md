# Phase 7 Final Summary — PostgreSQL Protocol Optimisation

**Date:** 2026-05-19
**Phases:** 7D · 7E · 7F · 7G
**Dataset:** 28,151,758 rows · 8,049 tickers (5,884 stocks + 2,165 ETFs from Kaggle)
**Hardware:** Intel Core i5-12400F · 32 GB RAM · NVMe SSD
**Baseline:** Phase 1 — psycopg2 + Pandas, unoptimised

---

Phase 7 applied five successive protocol improvements to a Postgres-backed benchmark
that started at 159s read / 263s write. Each phase targeted a different bottleneck
— the driver, the COPY path, connection pooling, async concurrency, and finally the
client-side Arrow representation. The progression below shows both the gains and
the diminishing returns as the bottleneck shifted from Python overhead to Postgres
executor time.

---

## Read Progression (SELECT * — 28,151,758 rows)

| Phase | Strategy                          | Duration    | Peak RAM    | vs baseline | Speedup |
|-------|-----------------------------------|-------------|-------------|-------------|---------|
| 1     | psycopg2 + Pandas (baseline)      | 159.16s     | 19,125 MB   | —           | —       |
| 7D    | psycopg3 binary cursor            | 142s        | 20,700 MB   | -11%        | 1.12×   |
| 7D    | psycopg3 + pgBouncer (pool=20)    | 109s        | 21,900 MB   | -31%        | 1.46×   |
| 7E    | COPY TO STDOUT FORMAT BINARY      | 81.68s      | 13,732 MB   | -49%        | 1.95×   |
| 7E    | COPY TO STDOUT FORMAT CSV→Polars  | 67.23s      | 4,257 MB    | -58%        | 2.37×   |
| 7F    | async 4-chunk ctid SELECT         | 63.75s      | 15,940 MB   | -60%        | 2.49×   |
| 7G    | ADBC fetch_arrow_table()          | 101.56s     | 6,506 MB    | -36%        | 1.57×   |
| **7G**| **ADBC fetch_record_batch() streaming** | **61.20s** | **6,723 MB** | **-62%** | **2.60×** |

**Read winner: ADBC streaming — 61.20s / 6.7 GB (single connection, no asyncio)**

The 7G ADBC streaming result is the fastest single-connection read of all phases.
It beats the async 4-chunk approach (63.75s) without asyncio, ctid arithmetic, or
4 concurrent connections — 2.6× faster than the Phase 1 baseline from a simple
API change.

Notable detour: `fetch_arrow_table()` (101.56s) is slower than COPY CSV because
the ADBC driver's read path uses text protocol by default (string/utf8 Arrow types
instead of binary date32), which adds per-value string parsing that COPY CSV's
Polars SIMD parser handles more efficiently.

---

## Write Progression (bulk INSERT — 28,151,758 rows)

| Phase | Strategy                          | Duration    | Peak RAM    | vs baseline |
|-------|-----------------------------------|-------------|-------------|-------------|
| 1     | psycopg2 COPY FROM STDIN          | 263.43s     | 16,352 MB   | —           |
| 7D    | psycopg3 COPY FROM STDIN          | 272s        | —           | ~same       |
| 7E    | psycopg3 COPY FROM STDIN          | 262.40s     | 7,906 MB    | ~same       |
| 7F    | psycopg3 async COPY FROM STDIN    | 245.09s     | 8,017 MB    | -7%         |
| **7G**| **ADBC adbc_ingest (Arrow COPY BINARY)** | **133.26s** | **3,193 MB** | **-49% / 2.0×** |

**Write winner: ADBC adbc_ingest — 133s / 3.2 GB (2× faster than all prior phases)**

Phases 7D–7F showed no meaningful write improvement — all used the same psycopg3
COPY FROM STDIN path with a CSV text intermediate. ADBC `adbc_ingest()` accepts a
`pa.Table` and serialises it as Arrow IPC / COPY BINARY with no Python-level row
iteration, halving both time and RAM. This is the one path where ADBC's binary
serialisation is genuinely exercised.

---

## Query Progression (GROUP BY ticker — 8,049 result rows)

| Phase | Strategy                          | Duration   | Peak RAM    | vs baseline |
|-------|-----------------------------------|------------|-------------|-------------|
| 1     | psycopg2 native GROUP BY          | 24.27s     | 3,474 MB    | —           |
| 7E    | COPY BINARY → DuckDB in-process   | 76.75s     | 15,564 MB   | **+216% regression** |
| 7F    | psycopg3 async SELECT GROUP BY    | 18.31s     | 4,630 MB    | -25%        |
| **7G**| **ADBC SELECT GROUP BY → Arrow**  | **18.56s** | **3,169 MB**| **-23%**    |

**Query winner: psycopg3 async GROUP BY — 18.31s (Phase 7F, statistically tied with 7G)**

The 7E COPY→DuckDB approach is instructive as a regression: pulling 28M rows across
the network to aggregate them in-process is always slower than pushing the GROUP BY
to the server. Phase 7F and 7G both converge to ~18.3–18.6s — the time Postgres
needs to scan and aggregate the table internally. This is the query floor without
server-side index changes.

---

## Bottleneck Evolution

```
Phase 1 (159s read):   bottleneck = Python tuple creation × 28M rows
                        (psycopg2 C extension creates one Python tuple per row)

Phase 7D (142–109s):   bottleneck = Python tuple creation (still)
                        psycopg3 binary format reduces bytes; tuple alloc unchanged

Phase 7E (81–67s):     bottleneck = Python object allocation (shifted)
                        COPY avoids executor; SIMD parser eliminates Python loop for CSV

Phase 7F (63s):        bottleneck = Python GIL
                        async concurrency helps server; client deserialization still single-threaded

Phase 7G (61s):        bottleneck = ADBC text decode + incremental batch limit
                        streaming narrows gap; true binary read path not yet exercised on reads
                        write bottleneck eliminated — ADBC ingest is genuinely zero-copy
```

---

## Decision Matrix — Best Strategy per Constraint

| Constraint             | Best strategy                | Phase | Duration | Peak RAM  | Complexity |
|------------------------|------------------------------|-------|----------|-----------|------------|
| **Read speed**         | ADBC streaming               | 7G    | 61.20s   | 6,723 MB  | Low        |
| **Read RAM**           | COPY CSV + Polars            | 7E    | 67.23s   | 4,257 MB  | Low        |
| **Write speed**        | ADBC adbc_ingest             | 7G    | 133s     | 3,193 MB  | Low        |
| **Write RAM**          | ADBC adbc_ingest             | 7G    | 133s     | 3,193 MB  | Low        |
| **Query speed**        | psycopg3 async GROUP BY      | 7F    | 18.31s   | 4,630 MB  | Medium     |
| **Query RAM**          | ADBC GROUP BY                | 7G    | 18.56s   | 3,169 MB  | Low        |
| **Simplicity**         | COPY CSV + Polars            | 7E    | 67.23s   | 4,257 MB  | Lowest     |
| **Balanced default**   | ADBC streaming               | 7G    | 61.20s   | 6,723 MB  | Low        |

---

## Overall Improvement Summary

```
                  Phase 1 baseline    Phase 7G best       Improvement
Read:             159.16s / 19.1 GB   61.20s / 6.7 GB     2.60× faster / 2.84× less RAM
Write:            263.43s / 16.4 GB   133.26s / 3.2 GB    1.98× faster / 5.12× less RAM
Query:            24.27s  / 3.5 GB    18.31s  / 4.6 GB    1.32× faster (server-bound)
```

Read and write improved 2× from simple API changes — no schema changes, no index
tuning, no hardware upgrades. The query improvement is smaller (1.32×) because
the bottleneck shifted to the server executor after the first few phases, not
the client protocol.

---

## What Phase 7 Did Not Improve

**Query below 18s without server changes:**
The 18-second floor for GROUP BY is Postgres scan time on a heap table with no
partial aggregation index. Index-only scans, TimescaleDB continuous aggregates,
or a materialised view refreshed on write would break this floor — those are
Phase 8 candidates.

**ADBC binary reads:**
The `adbc-driver-postgresql` driver's read path uses text protocol for string/date
columns. A future driver version or connection option enabling COPY TO STDOUT BINARY
for SELECT would eliminate the 40s gap between `fetch_arrow_table()` (101s) and
`fetch_record_batch()` (61s). DuckDB's native ADBC driver already returns true
Arrow IPC — the DuckDB benchmark (Phase 3) already benefits from this.

**Concurrency beyond single-machine:**
All Phase 7 improvements target single-machine, single-Postgres-instance latency.
Horizontal scaling (read replicas, Citus, partitioning across nodes) would address
throughput at scale — orthogonal to the protocol work here.
