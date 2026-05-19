═══════════════════════════════════════
SECTION 5: SQL Server — Polars vs Pandas
═══════════════════════════════════════
Benchmarked on 100K rows, extrapolated to 28M.

| Method | Write (100K) | Extrapolated 28M | RAM |
|---|---|---|---|
| bulk_insert pandas baseline | 72.85s | ~5.7h | 185MB |
| bulk_columnstore pandas baseline | 68.20s | ~5.3h | 187MB |
| bulk_insert polars | 53.86s | ~4.2h | 2,518MB |

Finding: Polars ~25% faster on write due to faster CSV ingestion,
but RAM is 13x higher from to_dicts() conversion overhead.
Bottleneck is TDS protocol — same pattern as Postgres.
Changing DataFrame engine cannot fix a wire protocol constraint.

═══════════════════════════════════════
SECTION 6: Neo4j — Polars vs Pandas
═══════════════════════════════════════
Benchmarked on 100K price nodes (full 28M DNF — RAM spike to 31GB).

| Method | write_graph | write_prices | read | query |
|---|---|---|---|---|
| pandas baseline | ~5s | ~16s | ~6s | ~0.4s |
| polars | 1.40s | 13.57s | 7.53s | 0.44s |

Finding: write_graph 3.5x faster — Polars to_dicts() serializes
node data faster than Pandas to_dict("records") for graph construction.
Read and query bottleneck is Neo4j driver serialization, not DataFrame.
write_prices limited to 100K — full 28M DNF regardless of engine.

═══════════════════════════════════════
SECTION 7: Redis — Polars vs Pandas
═══════════════════════════════════════

| Method | Write | Query (10 tickers) | Extrapolated full | RAM |
|---|---|---|---|---|
| pandas baseline | ~370s | ~62s | ~835min | 1,835MB |
| polars | 367.62s | 59.37s | ~835min | 1,835MB |

Finding: No meaningful difference. Redis bottleneck is JSON 
serialization + pipeline flush on write, and KEYS O(n) scan on query.
Neither is affected by DataFrame engine choice.
Polars provides zero benefit for Redis workloads.

═══════════════════════════════════════
SECTION 8: Redpanda — Polars vs Pandas
═══════════════════════════════════════

| Method | Producer (100K) | Consumer (100K) | RAM |
|---|---|---|---|
| pandas baseline | ~156s | ~52s | ~3,600MB |
| polars | 160.54s | 4.15s | 3,693MB |

Finding: Consumer 12.5x faster with Polars — the largest speedup
in Phase 7B. pl.from_dicts() batch-converts consumed messages
significantly faster than pd.DataFrame() at 100K rows.
Producer unchanged — bottleneck is Kafka protocol + network,
not serialization or DataFrame construction.

═══════════════════════════════════════
UPDATE SUMMARY TABLE at the end
═══════════════════════════════════════
Replace or append a complete Phase 7B summary table:

| Backend | Write speedup | Read/Consumer speedup | Bottleneck confirmed |
|---|---|---|---|
| DuckDB | Polars 3.3x | Arrow read 1.8x | Arrow→Polars conversion overhead |
| Parquet | ~same | Streaming RAM -40% | RAM vs speed tradeoff |
| Postgres | 2x faster | ~same (141s) | TDS protocol |
| MongoDB | ~same | Query 1.3x | pymongo BSON encoding |
| SQL Server | ~25% faster | ~same | TDS protocol |
| Neo4j | write_graph 3.5x | ~same | Driver serialization |
| Redis | ~same | ~same | KEYS O(n) scan |
| Redpanda | ~same producer | Consumer 12.5x | Batch DataFrame conversion |

ADD one final insight at the end:

Insight — Where Polars helps and where it does not
	Polars delivers meaningful speedups only when the DataFrame engine
	is the actual bottleneck: CSV ingestion (10x), in-memory transforms
	(6-10x), and consumer batch conversion (12.5x).
	When the bottleneck is a wire protocol (TDS, BSON, Kafka),
	a graph driver, or a Redis scan command, switching from Pandas
	to Polars produces no meaningful improvement.
	The lesson: profile before optimizing. Replace the bottleneck,
	not the bystander.
# Performance Benchmark Summary — Phase 7

**Date:** 2026-05-06  
**Dataset:** 28,151,758 rows · 8,049 tickers (5,884 stocks + 2,165 ETFs from Kaggle)  
**Hardware:** Intel Core i5-12400F · 32 GB RAM · NVMe SSD  
**Benchmark:** Write / Read / Analytical query (GROUP BY ticker → AVG/MAX/MIN close)

Phase 7 introduces targeted optimizations across four storage backends,
replacing Pandas with Polars and testing Arrow zero-copy (DuckDB),
streaming mode (Parquet), and Polars chunked reads (MongoDB/Postgres).
All baselines are from Phase 1–3. Same 28M rows, same queries, same hardware.

---

## DuckDB — Polars Arrow Zero-Copy

| Method                       | Write     | Read     | Query    | Peak RAM   |
|------------------------------|-----------|----------|----------|------------|
| bulk_insert (pandas)         | 11.69 s   | 5.29 s   | 0.14 s   | 6,172 MB   |
| bulk_insert_polars           | 3.48 s    | 6.06 s   | 0.18 s   | 4,871 MB   |
| bulk_insert_polars_arrow     | 5.64 s    | 3.37 s   | 0.39 s   | 4,607 MB   |

**Finding:** Arrow zero-copy improves read 1.8x vs the Polars baseline but write
and query are slower — the Arrow→Polars conversion has overhead at query time.
Polars (non-arrow) remains fastest for write.

---

## Parquet — Polars Streaming Mode

| Method                       | Write     | Read     | Query    | Peak RAM   |
|------------------------------|-----------|----------|----------|------------|
| lazy_polars (baseline)       | 6.21 s    | 0.39 s   | 0.98 s   | 7,702 MB   |
| lazy_polars_optimized        | 6.39 s    | 0.63 s   | 0.49 s   | 4,602 MB   |

**Finding:** `streaming=True` reduces query RAM 40% (7,793→4,602 MB) and
query speed 2x. Read is slightly slower. Clear tradeoff: RAM vs speed.
Use streaming when RAM is constrained, eager when speed is priority.

---

## Postgres — Polars vs Pandas

| Method                       | Write      | Read       | Query    | Peak RAM (read) |
|------------------------------|------------|------------|----------|-----------------|
| bulk_copy (pandas)           | 263.43 s   | 159.16 s   | 24.27 s  | 19,125 MB       |
| bulk_copy_polars             | 134.48 s   | 140.99 s   | 21.09 s  | 17,622 MB       |

**Finding:** Polars write is 2x faster than Pandas. But read is still 141 s —
the bottleneck is the TDS protocol/network, not the DataFrame engine. Changing
the DataFrame library cannot fix a protocol-level constraint.

---

## MongoDB — Polars Chunked Read

| Method                       | Write      | Read     | Query    | Peak RAM   |
|------------------------------|------------|----------|----------|------------|
| bulk_insert (pandas)         | 208.83 s   | 7.80 s * | 23.57 s  | 2,608 MB   |
| bulk_insert_polars           | 204.16 s   | 0.05 s   | 17.89 s  | 2,556 MB   |

\* pandas read = single ticker AAPL only (full scan OOM)  
Note: full scan omitted for both — would OOM at 28M docs.  
Read benchmark = single ticker (AAPL, 9,909 docs) — chunked Polars.

**Finding:** Write nearly identical — bottleneck is pymongo, not DataFrame.
Query 1.3x faster with Polars aggregation pipeline result conversion.

---

## Summary Comparison

| Backend              | Baseline write     | Optimized write | Speedup   | Bottleneck confirmed                    |
|----------------------|--------------------|------------------|-----------|-----------------------------------------|
| DuckDB (Arrow)       | 3.48 s (polars)    | 5.64 s           | —         | Arrow→Polars conversion overhead        |
| Parquet (streaming)  | 6.21 s             | 6.39 s           | ~same     | RAM: 40% reduction                      |
| Postgres (Polars)    | 263.43 s           | 134.48 s         | 2x        | TDS protocol (read unchanged)           |
| MongoDB (Polars)     | 208.83 s           | 204.16 s         | ~same     | pymongo BSON encoding                   |

---

## Key Insights

### Insight 1 — Postgres bottleneck is the protocol, not the DataFrame

Switching Pandas→Polars halved write time (263 s→134 s) but read
remained at 141 s. The TDS wire protocol serializes every row
through the network stack regardless of what receives it.
Phase 8 (pgBouncer) will test whether connection overhead compounds this.

### Insight 2 — Polars streaming trades speed for RAM predictability

`streaming=True` processed 28M rows with 4.6 GB peak vs 7.8 GB eager.
Query was 2x faster too — column pruning before `collect()` meant
less data ever entered memory. The cost: read latency increased slightly.
Production rule: use streaming when RAM headroom < 2× dataset size.

### Insight 3 — Arrow zero-copy is not universally faster

Arrow IPC eliminates Python object creation during `register()` but
adds conversion cost when DuckDB returns results as a Polars DataFrame.
At 28M rows the round-trip cost dominates the registration saving.
Zero-copy wins matter most in tight loops, not single large transfers.

### Insight 4 — MongoDB write bottleneck is pymongo, not DataFrame

`to_dicts()` + `insert_many()` with Polars vs Pandas produced nearly
identical write times (204 s vs 208 s). The serialization cost is in
pymongo's BSON encoding loop — unavoidable regardless of input format.
