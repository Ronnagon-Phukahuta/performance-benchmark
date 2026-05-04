# Performance Benchmark Summary — Phase 5

**Date:** 2026-05-05
**Phase:** 5 — Redpanda (Streaming)
**Dataset:** 28,151,758 rows · 8,049 tickers (5,884 stocks + 2,165 ETFs from Kaggle)
**Hardware:** Intel Core i5-12400F · 32 GB RAM · NVMe SSD
**Benchmark:** Anti-pattern batch dump (star_schema.py) / True use case streaming (streaming.py) — producer throughput, consumer throughput, end-to-end latency, concurrent reads

---

## Anti-Pattern: Batch Dump to Kafka (star_schema.py)

| Operation            | Duration    | Peak RAM | Rows       | Notes                                                                    |
|----------------------|-------------|----------|------------|--------------------------------------------------------------------------|
| write (28M SET via producer) | 1342.56 s | 5,300 MB | 28,151,758 | one message per row                                                |
| read (consume all)   | 209.35 s    | 4,825 MB | 7,396,498  | partial — consumer_timeout_ms exhausted before 28M                      |
| query_oltp           | 207.78 s    | 4,854 MB | 63         | must consume entire topic to find 63 rows — no index                    |
| concurrent 5 threads | 1,018.99 s  | 4,871 MB | DNF        | single partition bottleneck — linear scale                               |
| concurrent 10 threads | ~2,000 s extrapolated | — | DNF | single partition → linear                                       |
| concurrent 20 threads | ~4,000 s extrapolated | — | DNF | single partition → linear                                       |

> **This is the WRONG use case for Kafka/Redpanda.**
> Dumping 28M rows as a batch and consuming all messages to answer a query treats Kafka as a database — it is not.
> See streaming.py for the correct pattern.

---

## True Use Case: Real-Time Streaming (streaming.py)

### Throttled Producer (simulate market feed)

| Metric                  | Value              |
|-------------------------|--------------------|
| Target rate             | 1,000 rows/sec     |
| Actual rate             | 644 rows/sec       |
| kafka-python overhead   | ~36%               |
| End-to-end latency      | 168.6 ms           |
| Consumer lag            | 0 rows (consumer keeps up) |
| Rows                    | 100,000 (sample)   |
| Peak RAM                | 208.8 MB           |

### Max Throughput Producer

| Metric                          | Value              |
|---------------------------------|--------------------|
| Producer throughput             | 21,787 rows/sec    |
| Producer data rate              | 4.1 MB/sec         |
| Consumer throughput (→ DuckDB)  | 1,933 rows/sec     |
| Consumer/Producer ratio         | 8.9%               |
| Rows                            | 1,000,000          |
| Peak RAM                        | 677 MB             |

---

## Key Numbers

| Metric                          | Value              | Notes                                          |
|---------------------------------|--------------------|------------------------------------------------|
| Fastest operation               | 21,787 rows/sec    | Producer max throughput                        |
| Consumer bottleneck             | 1,933 rows/sec     | DuckDB insert speed, not Redpanda              |
| End-to-end latency              | 168.6 ms           | Throttled mode (1,000 rows/sec target)         |
| kafka-python overhead           | ~36%               | Target 1,000 → actual 644 rows/sec             |
| OLTP query (full topic scan)    | 207.78 s / 63 rows | vs 0.02s DuckDB index — 10,350× slower         |
| Concurrent DNF reason           | Single partition   | 5 threads = 5× wall time (linear scale)        |
| RAM footprint (idle)            | ~300 MB            | Disk-backed — no in-memory data like Redis     |

---

## Why — Technical Explanation

### Why Kafka/Redpanda is not a database

Kafka stores messages as an append-only log on disk — not a queryable store.
To answer "find ticker_id=1 between 2020-2023", a consumer must read every message from offset 0 and filter in Python. At 28M messages, this takes 207 seconds for 63 matching rows.

A database with a B-tree index answers the same query in 0.02s.

Kafka's strength is transport and durability, not query performance. The correct pattern: Kafka → consumer → write to DuckDB/Postgres → query the database.

### Why consumer is 11× slower than producer

Producer sends JSON messages over the network to the Redpanda broker — pure I/O, no storage cost.
Consumer receives messages AND inserts to DuckDB in batches of 10,000 rows.

DuckDB insert cost dominates: each `executemany(10K rows)` adds ~5ms overhead.
At 1M rows: 100 batches × 5ms = 500ms just for DuckDB overhead.

Producer has no storage cost — it just sends. Consumer does real work.

Production fix: consumer writes to Parquet (append-only) instead of DuckDB to remove transaction overhead. Or increase batch size to 100K.

### Why throttled producer achieves only 64% of target rate

kafka-python sends messages synchronously in the hot path despite pipeline batching.
Each `producer.send()` call serializes the value to JSON, encodes to bytes, acquires an internal lock, appends to the batch buffer, and checks if flush is needed — all before returning.

At 1,000 rows/sec target, each row has a 1ms budget.
The actual per-row overhead is ~1.5ms → actual rate 644 rows/sec.

Production fix: use confluent-kafka (C extension) instead of kafka-python (pure Python) — 5–10× lower per-message overhead.

### Why concurrent reads scale linearly (single partition bottleneck)

Kafka guarantees: within a partition, messages are ordered and assigned to exactly one consumer in a consumer group. With 1 partition and 5 consumers, only 1 consumer gets assigned — the other 4 sit idle waiting for rebalance.

Result: 5 threads = 5× wall time (1,019s vs 207s for 1 thread).

Fix: create topic with `num_partitions=20` → 20 consumers can read in parallel. This is an architectural decision made at topic creation time, not at query time.

See `BACKLOG.md` for the multi-partition optimization phase.

### Why Redpanda RAM footprint is low despite 28M messages

Redpanda (like Kafka) stores messages on disk as segment files — not in RAM.
RAM is used only for: OS page cache of hot segments, consumer group metadata, and broker internal state. At idle, Redpanda uses ~300 MB regardless of message count.

Compare: Redis stores everything in RAM — 1,835 MB for 28M keys permanently.
Redpanda disk usage grows with data; RAM stays roughly constant.

This makes Redpanda suitable for high-volume data pipelines where Redis would OOM.
