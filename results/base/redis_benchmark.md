# Performance Benchmark Summary — Phase 4

**Date:** 2026-05-04
**Phase:** 4 — Redis Key-Value Store
**Dataset:** 28,151,758 rows · 8,049 tickers (5,884 stocks + 2,165 ETFs from Kaggle)
**Hardware:** Intel Core i5-12400F · 32 GB RAM · NVMe SSD
**Benchmark:** Write (pipeline SET) / JOIN query (KEYS scan per ticker, GROUP BY sector) / OLTP lookup (single ticker, date range) / Sorted Set (ZADD + ZRANGE) / Cache simulation / Concurrent reads (N threads)

---

## Write Performance

| Method                       | Duration   | Peak RAM   | Rows          | Notes                                      |
|------------------------------|------------|------------|---------------|--------------------------------------------|
| Redis SET (pipeline 10K)     | 338.60 s   | 1,835 MB   | 28,151,758    | 1 key per row — all data permanently in RAM |

> All 28M keys stored in RAM permanently — Redis is fully in-memory. Unlike every other system tested, the 1,835 MB RAM footprint after write does not return to baseline. It stays at 1,835 MB until keys are explicitly deleted or Redis is restarted.

---

## JOIN Query (KEYS scan per ticker, GROUP BY sector)

| Method                        | Duration (10 tickers) | Extrapolated (8,049 tickers) | Peak RAM | Notes                                        |
|-------------------------------|-----------------------|------------------------------|----------|----------------------------------------------|
| Redis KEYS scan (sample of 10) | 62.28 s              | ~835 min ❌                  | 59 MB    | KEYS command blocks server — anti-pattern    |

> **KEYS command blocks Redis server — production anti-pattern.**
> `KEYS pattern` scans every key in the keyspace sequentially, blocking all other commands until complete. At 28M keys, one KEYS call takes ~6s. Benchmarked on 10 tickers only; extrapolated linearly to full 8,049 tickers.
> `SCAN` should be used instead — it iterates in small batches allowing other commands between iterations — but total scan time at 28M keys remains O(n). The real solution is to avoid key-pattern scanning entirely by using Sets or Sorted Sets to track keys by category.

---

## OLTP Query (single ticker lookup, date range 2020–2023)

| Method               | indexed   | no index  | Peak RAM | Rows | Notes                                                    |
|----------------------|-----------|-----------|----------|------|----------------------------------------------------------|
| Redis KEYS price:1:* | 6.39 s    | 6.42 s    | 59 MB    | 63   | No B-tree index — both use identical KEYS scan mechanism |

> Redis has no B-tree index concept. Both the "indexed" and "no-index" variants use the same `KEYS price:1:*` scan followed by a Python-side date filter. The near-identical timings (6.39s vs 6.42s) confirm there is no structural difference — both are O(n) key scans across 28M entries.

---

## Sorted Set (ZADD + ZRANGE top 10)

| Method                        | Duration | Peak RAM | Notes                                                          |
|-------------------------------|----------|----------|----------------------------------------------------------------|
| Redis ZADD + ZRANGE (100K sample) | 0.01 s | 72 MB  | avg_close computed via Polars group_by on 100K sample rows    |

> avg_close computed from `fact_prices_sample.csv` (100K rows) using Polars `.group_by()` — not from KEYS scanning. This is Redis's native strength: O(log n) sorted insert and O(log n + k) retrieval with no SQL, no full scan, no query planner.

---

## Cache Simulation (1000 random lookups)

| Metric                  | Value     |
|-------------------------|-----------|
| Total lookups           | 1,000     |
| Hit rate                | 52.7%     |
| Avg hit latency         | 0.308 ms  |
| Avg miss latency        | 0.336 ms  |
| Total time              | 1.28 s    |

> Hit and miss latency appear nearly identical because cache miss simulation uses a 1ms artificial `sleep()`. In production, a cache miss would trigger a real database query (Postgres: ~10–100ms, DuckDB: ~20ms), making cache hit advantage 30–300× more significant than the simulation suggests. The mechanism is correct — the magnitude of benefit is understated.

---

## Concurrent Reads (query_oltp × N threads, wall clock time)

| Threads | Wall Time  | Scaling   | Notes                                              |
|---------|------------|-----------|----------------------------------------------------|
| 1       | 6.39 s     | baseline  |                                                    |
| 5       | 31.61 s    | ~5× linear | Redis single-threaded — KEYS scans queue sequentially |
| 10      | 62.95 s    | ~10× linear |                                                   |
| 20      | 130.70 s   | ~20× linear |                                                   |

> Redis is single-threaded. All KEYS scans from concurrent threads queue sequentially behind each other — no parallel execution. 5 threads × 6.39s ≈ 31.6s wall time — exactly linear. This is expected behavior for KEYS-based queries and is not a Redis concurrency defect. For O(1) GET operations, Redis handles 100,000+ requests/second with no lock contention. The bottleneck here is the KEYS anti-pattern, not Redis's concurrency model.

---

## DNF / Extrapolated

| Method                         | Sample Size | Sample Duration | Extrapolated Full Dataset | Reason                              |
|--------------------------------|-------------|-----------------|---------------------------|-------------------------------------|
| Redis JOIN (KEYS scan all tickers) | 10 tickers | 62.28 s        | ~835 min ❌               | KEYS blocks server — anti-pattern   |

---

## Key Numbers

| Metric                          | Value        | Method                                   |
|---------------------------------|--------------|------------------------------------------|
| Fastest operation               | 0.01 s       | query_sorted_set (O(log n) native)       |
| Slowest operation               | 338.60 s     | write (28M individual SET via pipeline)  |
| Cache hit latency               | 0.308 ms     | query_cache_simulation                   |
| Cache miss latency (simulated)  | 0.336 ms     | 1ms artificial sleep — not representative |
| JOIN query extrapolated         | ~835 min ❌  | KEYS anti-pattern at 28M keys            |
| RAM footprint after write       | 1,835 MB     | Permanent — in-memory always             |
| OLTP query (ticker_id=1)        | 6.39 s       | KEYS scan — no B-tree index              |

---

## Why — Technical Explanation

### Why Redis write is slow despite pipeline batching

Redis pipeline sends 10,000 commands per batch, reducing round-trip overhead by eliminating per-command network acknowledgement. Despite this, writing 28M rows takes 338.60s — slower than DuckDB (8.44s), Parquet (8.07s), Postgres (224.45s), and MongoDB (221.15s).

The bottleneck is not network I/O — it is key insertion overhead. Each key `price:{ticker_id}:{date}` is a separate entry in Redis's internal dictionary (a hash table with open addressing). Every new key triggers: a hash function call, a collision probe, a dict entry allocation, TTL metadata initialization (~16 bytes), LRU clock entry (~8 bytes), and encoding overhead (~50–100 bytes per key beyond the value). At 28M keys, the hash table must rehash — doubling its internal array size — multiple times during population. Each rehash copies all existing entries to a new allocation.

Compare to DuckDB `COPY FROM CSV`: writes columnar blocks to disk in bulk passes — not 28M individual hash table insertions. DuckDB's bottleneck at 8.44s is I/O throughput to NVMe. Redis's bottleneck at 338.60s is CPU-bound hash table management at 28M key scale.

Pipeline batching helps: without it, round-trip latency alone would add ~2,800s (28M × 0.1ms per command). Chunking into 10,000-row pipelines reduces this to ~28M/10K = 2,800 round trips, recovering most of that overhead. But the hash table insertion cost is unavoidable regardless of batching.

### Why KEYS command is a production anti-pattern

Redis is single-threaded — all client commands execute on a single event loop. `KEYS pattern` implements a full keyspace scan: it iterates the internal dictionary from entry 0 to entry N, comparing each key against the glob pattern. During this scan, no other command can execute.

At 28M keys, one `KEYS price:1:*` call takes ~6s. During those 6 seconds, every other client waiting to read or write to Redis is blocked. In a production system with multiple application servers, this means all requests stall for 6 seconds — effectively a self-inflicted denial of service.

The official alternative is `SCAN cursor COUNT 100`, which iterates in small batches and returns a cursor for the next batch. Between batch iterations, other commands can execute. However, `SCAN` does not make the total scan faster — it still visits every key. At 28M keys the full iteration takes the same ~6s total; it is just spread across many small steps instead of one blocking call.

The real solution is to design the key structure to never require pattern scanning. If you need all keys for ticker_id=1, maintain a Redis Set `ticker_keys:1` and add each key to it at write time. Retrieving all keys for ticker 1 becomes `SMEMBERS ticker_keys:1` — O(n) in the number of matches, not O(N) in the total keyspace. This is the idiomatic Redis data modeling approach.

### Why Redis concurrent reads scale linearly (not sub-linear like DuckDB)

DuckDB concurrent reads share a buffer pool. When the first thread reads `fact_prices` into DuckDB's buffer cache, subsequent threads serve their queries from already-resident pages — the NVMe read happens once. At 20 concurrent threads, DuckDB's wall time (1.82s) is ~4× single-thread time rather than 20×, because most thread time is spent computing against cached data.

Redis is single-threaded. Each thread's `KEYS price:1:*` scan executes on Redis's single event loop — they cannot run in parallel. Thread 1 runs its 6.39s scan to completion, then thread 2 runs its 6.39s scan, and so on. Five threads produce exactly 5 × 6.39s = ~31.6s wall time. Ten threads produce ~62.95s. The scaling is perfectly linear because there is zero parallelism — it is a sequential queue.

This is not a bug or a Redis weakness in general. For O(1) GET operations, Redis's single-threaded model eliminates all lock contention and context switching overhead, achieving 100,000–1,000,000 operations/second with consistent sub-millisecond latency. The linear concurrent scaling observed here is exclusively caused by the KEYS anti-pattern — each thread's O(n) scan serializes against every other thread's O(n) scan.

### Why Sorted Set is Redis's strongest use case

Redis Sorted Sets are implemented as a skip list backed by a hash map. `ZADD key score member` inserts a member with a float score into the skip list in O(log n) time — the skip list maintains sorted order at insert time, not at query time. `ZRANGE key 0 9 REV WITHSCORES` retrieves the top 10 members in O(log n + 10) — descend the skip list to the correct rank, then read 10 consecutive entries.

Computing avg_close for 8,049 tickers with Polars `.group_by()` and bulk-loading into Redis via `ZADD` with a mapping dict completes in 0.01s. Retrieving the top 10 tickers by average close price is a single `ZRANGE` command — no SQL `GROUP BY`, no full scan, no query planner, no index lookup. The data structure itself maintains sorted order as a structural invariant.

This maps directly to real production use cases: leaderboards, rate limiters (sorted by request timestamp), time-series event ordering, priority queues, trending content rankings. None of these require relational semantics. A Postgres equivalent would require a full `SELECT ticker_id, AVG(close) FROM fact_prices GROUP BY ticker_id ORDER BY avg_close DESC LIMIT 10` — scanning all 28M rows every time. Redis's answer is: precompute the score once at write time, retrieve any top-N in microseconds forever after.

### Why Redis RAM footprint is permanent unlike other databases

Every other system tested in this benchmark uses RAM as a cache of disk-resident data:
- Postgres: 27 MB idle, spikes to 27 GB during full scans, returns to 27 MB after
- DuckDB: spikes to ~2–10 GB during queries, releases after
- Parquet: zero idle footprint, spikes during reads, releases after

Redis's architecture inverts this model: RAM is the primary storage; disk (RDB snapshots, AOF log) is the backup. After `write()` completes, the 1,835 MB footprint is the actual database — not a cache of something else. Deleting all keys or restarting Redis without persistence is the only way to recover that memory.

Production implications are significant. Redis capacity planning requires: data size × per-key overhead × safety margin. Each of the 28M keys carries ~64–128 bytes of internal metadata beyond the actual value (dict entry, SDS header, LRU field, encoding flags). 28M keys × 100 bytes overhead = ~2.8 GB of overhead alone. Always set `maxmemory` and an eviction policy (`allkeys-lru` or `volatile-lru`) in production — without it, Redis will grow until the OS OOM killer terminates the process.

For this benchmark's dataset (28M rows of financial data), Redis is architecturally mismatched as a primary store. Its correct role is a caching layer in front of a persistent database: store the 1,000 most frequently queried tickers in Redis, serve O(1) GETs at sub-millisecond latency, and fall back to Postgres or DuckDB for cache misses. The full 28M key load demonstrates the ceiling of the approach, not the recommended usage.

### Why cache hit and miss latency are similar in this simulation

The simulation measures cache hit latency as the time to call `client.get(key)` and receive a value — typically 0.3ms over localhost TCP. Cache miss latency measures the time to call `client.get(key)` and receive `None` — also ~0.3ms, because the network round-trip cost is the same whether the key exists or not.

The 1ms `time.sleep(0.001)` added on cache miss is the simulated downstream database query. In reality:
- Postgres query for 63 rows with index: ~30ms
- DuckDB query for 63 rows: ~20ms
- MongoDB point lookup: ~10ms

At realistic miss penalty of 50ms vs hit latency of 0.3ms, the cache hit advantage is ~166×. The simulation demonstrates the mechanism correctly — check cache first, fall back on miss, populate cache for future requests — but the `sleep(0.001)` dramatically understates the real-world benefit. The benchmark result of 52.7% hit rate with 1.28s total is meaningful only for the hit rate metric. The latency numbers require substitution with real downstream query times to be actionable.
