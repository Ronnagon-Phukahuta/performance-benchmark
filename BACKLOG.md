# Benchmark Backlog

Items are loosely ordered by priority. Not a strict roadmap — 
new phases may be added based on findings or community feedback.

---

## In Progress
- [x] Phase 5 — Redpanda (Streaming)
- [x] Phase 6 — Neo4j (Graph DB)

## Planned — Optimization
- [ ] Phase 7 — Polars vs Pandas (same queries, measure delta)
- [ ] Phase 8 — Postgres with pgBouncer connection pooling
- [ ] Phase 9 — Indexed vs non-indexed (full paradigm comparison)
- [ ] Phase 10 — MongoDB optimized pipeline ($lookup order, SCAN vs KEYS)
- [ ] Phase 11 — SQL Server BCP vs pymssql (bypass TDS bottleneck)
- [ ] Phase X — Redpanda multi-partition (20 partitions, partition by ticker_id)
      Baseline: 1 partition, concurrent scale linear (5 threads = 5x slower)
      Expected: 20 partitions → 20 consumers read in parallel → near-linear speedup
      This is the correct Kafka parallelism pattern — architectural decision at topic creation
- [ ] Phase X — Redis Cluster mode (3 nodes, hash slot sharding)
      Baseline: single-threaded, concurrent scale linear
      Expected: 3 nodes → 3x throughput for key-distributed workloads
      Note: not true parallelism — horizontal scaling via sharding
- [ ] Phase X — Redpanda streaming ingest per paradigm
      Producer sends 1M rows → 6 consumers in parallel, each writing to different storage:
        Consumer A → DuckDB
        Consumer B → Postgres
        Consumer C → Parquet
        Consumer D → MongoDB
        Consumer E → Redis
        Consumer F → Neo4j
      Measure: rows/sec ingest rate, end-to-end latency, RAM per paradigm
      This answers: "which storage paradigm handles streaming ingest best?"
- [ ] Phase X — Neo4j with increased memory (heap 8G + pagecache 4G)
      Test if 28M Price nodes fit with proper memory allocation
      Baseline: DNF at default config (31GB RAM spike on 32GB system)

## Planned — New Datasets
- [ ] NASA satellite data — validate findings are dataset-agnostic
- [ ] IoT sensor data — high-frequency, small payload per record

## Ideas — May or May Not Happen
- [ ] Redis Cluster mode vs single instance
- [ ] ClickHouse — columnar OLAP comparison vs DuckDB
- [ ] TimescaleDB — time-series extension for Postgres
- [ ] Concurrent write benchmark (not just reads)
- [ ] ARM benchmark (Mac M-series vs x86)

---

> Phases 1–3 = baseline, no optimization.  
> Phase 4+ = targeted optimization and new paradigms.

---

## Planned — Cross-Paradigm Optimization
> Use the best tool for each job — combine paradigms for maximum performance

- [ ] Phase X — Redpanda → DuckDB pipeline
      Stream stock prices via Redpanda → consumer writes to DuckDB
      Measure: end-to-end latency, ingest throughput vs batch load
      Why: Redpanda handles ordering + durability, DuckDB handles analytics

- [ ] Phase X — Redis → Postgres cache layer
      Cache hot ticker queries in Redis, fall back to Postgres on miss
      Measure: cache hit rate, latency improvement hit vs miss (real DB, not simulated)
      Why: Redis 0.3ms hit vs Postgres 0.10s miss = 333x speedup on cache hit

- [ ] Phase X — Neo4j → DuckDB hybrid query
      Use Neo4j for graph traversal (find related tickers),
      then pass ticker list to DuckDB for analytical aggregation
      Measure: total latency vs pure SQL self-join + GROUP BY
      Why: Neo4j finds 910 related tickers in 0.07s,
           DuckDB aggregates them in milliseconds — SQL would need expensive JOIN

- [ ] Phase X — Full pipeline: Redpanda → DuckDB → Redis → Dashboard
      Simulate production stock dashboard:
        1. Redpanda streams price updates
        2. Consumer writes to DuckDB
        3. Redis caches dashboard queries
        4. Measure: time from price update to dashboard display
      This is the architecture real-time financial systems use

---

## Note on New Datasets
Cross-paradigm optimization phases above are designed for stock data.
When NASA satellite or IoT sensor datasets are added, cross-paradigm
patterns will differ based on data characteristics:
  - NASA: Redpanda → Parquet (sensor stream → columnar archive)
  - IoT:  Redis (device state cache) + DuckDB (time-window aggregation)
Each new dataset will have its own cross-paradigm optimization phase.

---

## Planned — Version Tracking
> Re-run benchmarks when major versions release to track performance evolution

- [ ] DuckDB — current 1.2.2, re-run on 2.x release
- [ ] Polars — current 1.20.0, re-run on 2.x release
- [ ] Pandas — current 2.2.3, re-run on 3.x release
- [ ] MongoDB — current 7.0.32, re-run on 8.x release
- [ ] Neo4j — current 5.26.25, re-run on 6.x release
- [ ] Redis — current 7.4.8, re-run on 8.x release
- [ ] Redpanda — current v26.1.6, re-run on major release
- [ ] kafka-python — current 2.3.1, re-run on 3.x release
- [ ] neo4j (Python driver) — current 6.1.0, re-run on 7.x release

Each version bump gets its own results file:
  results/base/bulk_load_benchmark_duckdb_v2.md
  Compare delta vs baseline in a version_comparison.md
