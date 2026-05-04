# Performance Benchmark Summary — Phase 6

**Date:** 2026-05-05
**Phase:** 6 — Neo4j (Graph Database)
**Dataset:** 28,151,758 rows · 8,049 tickers (5,884 stocks + 2,165 ETFs from Kaggle)
**Hardware:** Intel Core i5-12400F · 32 GB RAM · NVMe SSD
**Benchmark:** Write (nodes + relationships) / JOIN query (GROUP BY sector) / OLTP (single ticker) / Graph traversal / Concurrent reads (N threads)

---

## Write Performance

| Method                          | Duration   | Peak RAM  | Rows       | Notes                                              |
|---------------------------------|------------|-----------|------------|----------------------------------------------------|
| write_graph (nodes + rels)      | 1.77 s     | 135 MB    | 8,049      | Ticker / Sector / Industry / Exchange nodes + rels |
| write_prices (100K sample)      | 13.11 s    | 202 MB    | 100,000    | Price nodes + HAS_PRICE relationships              |
| write_prices (28M full)         | DNF ❌     | ~31 GB    | —          | RAM spiked to 31 GB on 32 GB system                |

> **write_prices full 28M rows DNF.**
> 100K rows took 13.11s → 28M extrapolated = ~3,830s (~64 min).
> RAM at 100K = 202 MB → 28M extrapolated = ~56 GB (exceeds 32 GB system RAM).
> Fix: increase docker memory limit and set `NEO4J_dbms_memory_heap_max__size=8G` — see Why section.

---

## JOIN Query (MATCH Ticker→Sector + Price, GROUP BY sector)

| Method       | Duration | Peak RAM | Sectors | Notes                           |
|--------------|----------|----------|---------|---------------------------------|
| Neo4j Cypher | 0.14 s   | 152 MB   | 8       | Based on 100K price sample      |

---

## OLTP Query (ticker_id=1, date range 2020–2023)

| Method | Duration | Peak RAM | Rows | Notes                        |
|--------|----------|----------|------|------------------------------|
| Neo4j  | 0.07 s   | 152 MB   | 63   | B-tree index on ticker_id    |

---

## Graph Traversal — Native Strength

| Method                    | Duration | Peak RAM | Results           | Notes                                                    |
|---------------------------|----------|----------|-------------------|----------------------------------------------------------|
| 1-hop sector traversal    | 0.07 s   | 152 MB   | 910 related tickers | pointer-chain follow, no table scan                    |

> SQL equivalent: `SELECT t2.ticker FROM dim_symbols t1 JOIN dim_symbols t2 ON t1.sector = t2.sector WHERE t1.ticker_id = 1`
> SQL requires a self-join scan; Cypher follows relationship pointers directly in O(degree).

---

## Concurrent Reads (query_join × N threads, wall clock time)

| Threads | Wall Time | Peak RAM | Notes                                      |
|---------|-----------|----------|--------------------------------------------|
| 1       | 0.14 s    | 152 MB   | baseline                                   |
| 5       | 0.12 s    | 153 MB   | bolt connection pool handles parallel reads |
| 10      | 0.17 s    | 153 MB   |                                            |
| 20      | 0.23 s    | 154 MB   | 1.6× single-thread — best scaling tested   |

> **Best concurrent scaling of all paradigms tested.**
> Neo4j bolt connection pool handles parallel queries with minimal overhead.
> 20 threads = 0.23s wall — only 1.6× slower than single thread (0.14s).
> Compare: MongoDB 20 threads = 112s (~800×), Redis 20 threads = 130s (linear).

---

## Key Numbers

| Metric                          | Value               | Notes                                          |
|---------------------------------|---------------------|------------------------------------------------|
| Fastest query                   | 0.07 s              | query_oltp and query_traversal (tie)           |
| Best concurrent scale (20t)     | 0.23 s              | Lowest overhead of all server paradigms        |
| write_prices DNF                | ~31 GB RAM spike    | 28M Price nodes exceed 32 GB system RAM        |
| Graph traversal result          | 910 related tickers | 0.07s — no self-join needed                    |
| RAM footprint (idle)            | ~153 MB             | Lowest of all server-based paradigms           |
| write_prices extrapolated (28M) | ~3,830 s (~64 min)  | Linear from 13.11s / 100K                     |

---

## Why — Technical Explanation

### Why Neo4j graph traversal is faster than SQL self-join

SQL self-join: `SELECT t2.* FROM dim_symbols t1 JOIN dim_symbols t2 ON t1.sector = t2.sector WHERE t1.ticker_id = 1`
This requires scanning `dim_symbols` twice and joining on a sector string — O(n²) worst case without an index, O(n log n) with one.

Neo4j Cypher: `MATCH (t1 {ticker_id:1})-[:IN_SECTOR]->(s)<-[:IN_SECTOR]-(t2)`
This follows pointer chains directly — O(degree) where degree = relationships per node. For ticker_id=1 with 910 sector-mates, Neo4j follows exactly 912 pointer hops (1 to sector + 911 back). No table scan, no join.

This advantage grows with graph depth — 2-hop, 3-hop traversals stay O(degree^k) while SQL JOINs become exponentially expensive.

### Why Neo4j concurrent reads scale better than all other paradigms

Neo4j uses a connection pool (bolt protocol) with async query execution. Each Cypher query runs in its own transaction on the server — no global lock, no single-threaded bottleneck (unlike Redis), no connection fork overhead (unlike Postgres). The graph engine processes multiple read transactions simultaneously using shared page cache.

Result: 20 threads = 0.23s wall — only 1.6× slower than single thread (0.14s).
Compare: MongoDB 20 threads = 112s (~800× slower than single), Redis 20 threads = 130s (exactly linear).

### Why write_prices DNF at 28M nodes

Each Price node in Neo4j requires: node header (~40 bytes), property store (date string + 5 floats = ~80 bytes), relationship record for HAS_PRICE (~33 bytes), and index entries. Total ~200 bytes per node × 28M = ~5.6 GB on disk.

However, Neo4j loads the entire working set into pagecache during write — configured at 1 GB but JVM heap expands to handle the load. At 28M nodes, JVM heap + pagecache + OS overhead exceeded 31 GB.

Fix: increase docker memory limit and set `NEO4J_dbms_memory_heap_max__size=8G`, `NEO4J_dbms_memory_pagecache_size=4G` — requires 16 GB+ dedicated to Neo4j. See `BACKLOG.md` for the Neo4j increased memory phase.

### Why Neo4j RAM footprint is lowest of all server paradigms at idle

At 153 MB idle, Neo4j is lighter than MongoDB (~3.4 GB), SQL Server (~2.1 GB), and Redis (~1.8 GB). This is because Neo4j's JVM process starts lean — it only loads data into pagecache on first access (lazy loading). MongoDB and SQL Server pre-allocate buffer pools on startup. Redis loads all data into RAM permanently.

Under analytical load, Neo4j pagecache would grow — but for dev/test with small datasets, the footprint stays minimal.

### Why Graph DB is not a replacement for relational DB

Neo4j excels at: traversal queries (find related nodes N hops away), pattern matching (find circular relationships), and schema-flexible data.

Neo4j struggles at: analytical aggregations over millions of nodes (GROUP BY sector on 28M Price nodes would be slower than DuckDB), bulk data loading (13s for 100K vs DuckDB's 8s for 28M), and memory efficiency at scale (31 GB for 28M nodes vs DuckDB's 2.5 GB).

The right architecture: Graph DB for relationship queries, columnar DB for analytical queries — not either/or.
