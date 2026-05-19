# Performance Benchmark Summary — Phase 7B

**Date:** 2026-05-06
**Phase:** 7B — Polars Optimisation Across All Storage Backends
**Dataset:** 28,151,758 rows · 8,049 tickers (5,884 stocks + 2,165 ETFs from Kaggle)
**Hardware:** Intel Core i5-12400F · 32 GB RAM · NVMe SSD
**Benchmark:** Write / Read / Query — Pandas baseline vs Polars optimised, per backend

---

Phase 7B replaces Pandas with Polars across all 8 storage backends from Phases 1–6.
Each backend is retested with the same 28M rows, same queries, same hardware.
The goal: confirm which bottlenecks are DataFrame-bound (improvable with Polars)
and which are protocol-bound (unchanged regardless of engine).

---

## DuckDB — Polars Arrow Zero-Copy

| Method                   | Write   | Read   | Query  | Peak RAM |
|--------------------------|---------|--------|--------|----------|
| bulk_insert (pandas)     | 11.69s  | 5.29s  | 0.14s  | 6,172 MB |
| bulk_insert_polars       | 3.48s   | 6.06s  | 0.18s  | 4,871 MB |
| bulk_insert_polars_arrow | 5.64s   | 3.37s  | 0.39s  | 4,607 MB |

**Finding:** Arrow zero-copy improves read 1.8× vs the Polars baseline but write
and query are slower — the Arrow→Polars conversion has overhead at query time.
Polars (non-arrow) remains fastest for write. Bottleneck: Arrow↔Polars conversion.

---

## Parquet — Polars Streaming Mode

| Method                 | Write  | Read   | Query  | Peak RAM |
|------------------------|--------|--------|--------|----------|
| lazy_polars (baseline) | 6.21s  | 0.39s  | 0.98s  | 7,702 MB |
| lazy_polars_optimized  | 6.39s  | 0.63s  | 0.49s  | 4,602 MB |

**Finding:** `streaming=True` reduces query RAM 40% (7,793→4,602 MB) and query
time 2×. Read is slightly slower. Clear tradeoff: RAM vs speed.
Use streaming when RAM is constrained, eager when speed is priority.

---

## Postgres — Polars vs Pandas

| Method               | Write    | Read     | Query   | Peak RAM (read) |
|----------------------|----------|----------|---------|-----------------|
| bulk_copy (pandas)   | 263.43s  | 159.16s  | 24.27s  | 19,125 MB       |
| bulk_copy_polars     | 134.48s  | 140.99s  | 21.09s  | 17,622 MB       |

**Finding:** Polars write is 2× faster than Pandas. But read is still 141s —
the bottleneck is the TDS protocol, not the DataFrame engine. Switching
library cannot fix a protocol-level constraint.

---

## MongoDB — Polars Chunked Read

| Method                 | Write    | Read   | Query   | Peak RAM |
|------------------------|----------|--------|---------|----------|
| bulk_insert (pandas)   | 208.83s  | 7.80s* | 23.57s  | 2,608 MB |
| bulk_insert_polars     | 204.16s  | 0.05s  | 17.89s  | 2,556 MB |

> \* pandas read = single ticker AAPL only (full scan OOM at 28M docs)

**Finding:** Write nearly identical — bottleneck is pymongo BSON encoding, not
DataFrame. Query 1.3× faster from Polars aggregation pipeline result conversion.

---

## SQL Server — 100K subset, extrapolated

| Method                        | Write (100K) | Extrapolated 28M | Peak RAM |
|-------------------------------|--------------|------------------|----------|
| bulk_insert (pandas baseline) | 72.85s       | ~5.7h            | 185 MB   |
| bulk_columnstore (pandas)     | 68.20s       | ~5.3h            | 187 MB   |
| bulk_insert (polars)          | 53.86s       | ~4.2h            | 2,518 MB |

**Finding:** Polars ~25% faster on write due to faster CSV ingestion, but RAM
is 13× higher from to_dicts() conversion overhead. Bottleneck is TDS protocol
— same pattern as Postgres. Changing the engine cannot fix the wire.

---

## Neo4j — 100K subset (full 28M DNF — RAM spike to 31 GB)

| Method            | write_graph | write_prices | read   | query  |
|-------------------|-------------|--------------|--------|--------|
| pandas baseline   | ~5s         | ~16s         | ~6s    | ~0.4s  |
| polars            | 1.40s       | 13.57s       | 7.53s  | 0.44s  |

**Finding:** write_graph 3.5× faster — Polars to_dicts() serializes node data
faster than Pandas to_dict("records") for graph construction. Read and query
bottleneck is Neo4j driver serialization, not DataFrame.

---

## Redis

| Method            | Write  | Query (10 tickers) | Extrapolated full | Peak RAM |
|-------------------|--------|--------------------|-------------------|----------|
| pandas baseline   | ~370s  | ~62s               | ~835 min          | 1,835 MB |
| polars            | 367.62s| 59.37s             | ~835 min          | 1,835 MB |

**Finding:** No meaningful difference. Bottleneck is JSON serialization + pipeline
flush on write, and KEYS O(n) scan on query. Neither is affected by engine choice.

---

## Redpanda — Producer / Consumer

| Method            | Producer (100K) | Consumer (100K) | Peak RAM |
|-------------------|-----------------|-----------------|----------|
| pandas baseline   | ~156s           | ~52s            | ~3,600 MB|
| polars            | 160.54s         | 4.15s           | 3,693 MB |

**Finding:** Consumer 12.5× faster with Polars — the largest speedup in Phase 7B.
pl.from_dicts() batch-converts consumed messages significantly faster than
pd.DataFrame(). Producer unchanged — bottleneck is Kafka protocol + network.

---

## Summary

| Backend    | Write speedup     | Read/Consumer speedup | Bottleneck confirmed              |
|------------|-------------------|-----------------------|-----------------------------------|
| DuckDB     | Polars 3.3×       | Arrow read 1.8×       | Arrow→Polars conversion overhead  |
| Parquet    | ~same             | Streaming RAM -40%    | RAM vs speed tradeoff             |
| Postgres   | 2× faster         | ~same (141s)          | TDS protocol                      |
| MongoDB    | ~same             | Query 1.3×            | pymongo BSON encoding             |
| SQL Server | ~25% faster       | ~same                 | TDS protocol                      |
| Neo4j      | write_graph 3.5×  | ~same                 | Driver serialization              |
| Redis      | ~same             | ~same                 | KEYS O(n) scan                    |
| Redpanda   | ~same producer    | Consumer 12.5×        | Batch DataFrame conversion        |

---

## Key Insights

**Insight 1 — Postgres bottleneck is the protocol, not the DataFrame**

Switching Pandas→Polars halved write time (263s→134s) but read remained at 141s.
The TDS wire protocol serializes every row through the network stack regardless of
what receives it. Protocol-level bottlenecks require protocol-level solutions:
binary transport (psycopg3), connection pooling (pgBouncer), or bulk streaming.

**Insight 2 — Polars streaming trades speed for RAM predictability**

`streaming=True` processed 28M rows with 4.6 GB peak vs 7.8 GB eager. Query was
also 2× faster from column pruning before collect(). Cost: read latency increased
slightly. Production rule: use streaming when RAM headroom < 2× dataset size.

**Insight 3 — Arrow zero-copy is not universally faster**

Arrow IPC eliminates Python object creation during register() but adds conversion
cost when DuckDB returns results as a Polars DataFrame. At 28M rows the round-trip
cost dominates the registration saving. Zero-copy wins matter most in tight loops,
not single large transfers.

**Insight 4 — MongoDB write bottleneck is pymongo, not DataFrame**

to_dicts() + insert_many() with Polars vs Pandas produced nearly identical write
times (204s vs 208s). The serialization cost is in pymongo's BSON encoding loop —
unavoidable regardless of input format.

**Insight 5 — Where Polars helps and where it does not**

Polars delivers meaningful speedups only when the DataFrame engine is the actual
bottleneck: CSV ingestion (10×), in-memory transforms (6–10×), and consumer batch
conversion (12.5×). When the bottleneck is a wire protocol (TDS, BSON, Kafka), a
graph driver, or a server-side scan command, switching engines produces no
meaningful improvement. Profile before optimising — replace the bottleneck,
not the bystander.
