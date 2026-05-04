# Benchmark Backlog

Items are loosely ordered by priority. Not a strict roadmap — 
new phases may be added based on findings or community feedback.

---

## In Progress
- [ ] Phase 5 — Redpanda (Streaming)
- [ ] Phase 6 — Neo4j (Graph DB)

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
