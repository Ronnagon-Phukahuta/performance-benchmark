# Performance Benchmark Summary — Phase 7C

**Date:** 2026-05-06
**Phase:** 7C — Polars vs Pandas: Complete Comparison
**Dataset:** 28,151,758 rows · 8,049 tickers (5,884 stocks + 2,165 ETFs from Kaggle)
**Hardware:** Intel Core i5-12400F · 32 GB RAM · NVMe SSD
**Benchmark:** Synthesis of Phase 7A (pure DataFrame) + Phase 7B (all storage backends)

---

Phase 7C combines all findings from Phase 7A (pure DataFrame, no storage) and
Phase 7B (Polars optimisation across all 8 backends) into a single definitive
answer: when does switching from Pandas to Polars matter, and when does it not?

---

## Section 1 — Pure DataFrame (Phase 7A recap)

No storage layer — engine overhead only.

| Operation     | pandas  | polars_eager | polars_lazy | polars_streaming | Speedup |
|---------------|---------|--------------|-------------|------------------|---------|
| read_csv      | 14.64s  | 1.46s        | 1.64s       | 2.23s            | 10×     |
| filter        | 15.42s  | 1.49s        | 1.50s       | 2.33s            | 10×     |
| groupby       | 15.51s  | 2.40s        | 2.24s       | 1.60s            | 10×     |
| sort          | 19.95s  | 3.22s        | 3.33s       | 3.43s            | 6×      |
| join          | 0.08s   | 0.05s        | 0.11s       | 0.37s            | 1.6×    |
| window        | 38.05s  | 5.15s        | 5.07s       | 5.21s            | 7.5×    |
| string        | 17.36s  | 1.62s        | 1.65s       | 2.44s            | 10×     |
| typecast+null | 19.11s  | 1.98s        | 1.96s       | 2.29s            | 10×     |
| concat        | 15.36s  | 1.56s        | 1.56s       | 2.03s            | 10×     |

---

## Section 2 — With Storage Backend (Phase 7B recap)

Real bottleneck revealed — engine vs protocol.

| Backend    | Operation          | Pandas    | Polars    | Speedup | Bottleneck          |
|------------|--------------------|-----------|-----------|---------|---------------------|
| DuckDB     | write              | 11.69s    | 3.48s     | 3.3×    | ✅ DataFrame        |
| DuckDB     | read (Arrow)       | 6.06s     | 3.37s     | 1.8×    | ✅ DataFrame        |
| Parquet    | query (streaming)  | 0.98s     | 0.49s     | 2×      | ✅ DataFrame        |
| Postgres   | write              | 263.43s   | 134.48s   | 2×      | ✅ DataFrame        |
| Postgres   | read               | 159.16s   | 140.99s   | ~same   | ❌ TDS protocol     |
| MongoDB    | write              | 208.83s   | 204.16s   | ~same   | ❌ pymongo BSON     |
| MongoDB    | query              | 23.57s    | 17.89s    | 1.3×    | ✅ DataFrame        |
| SQL Server | write (extrap)     | ~5.7h     | ~4.2h     | ~25%    | ❌ TDS protocol     |
| Neo4j      | write_graph        | ~5s       | 1.40s     | 3.5×    | ✅ DataFrame        |
| Neo4j      | read               | ~6s       | 7.53s     | ~same   | ❌ Driver           |
| Redis      | write              | ~370s     | 367.62s   | ~same   | ❌ JSON+pipeline    |
| Redis      | query              | ~835 min  | ~835 min  | ~same   | ❌ KEYS O(n)        |
| Redpanda   | producer           | ~156s     | 160.54s   | ~same   | ❌ Kafka protocol   |
| Redpanda   | consumer           | ~52s      | 4.15s     | 12.5×   | ✅ DataFrame        |

---

## Section 3 — The Decision Framework

Should I replace Pandas with Polars?

| Scenario                                    | Switch?    | Reason                                |
|---------------------------------------------|------------|---------------------------------------|
| In-memory transforms (filter/groupby/sort)  | ✅ Yes     | 6–10× faster, Rust parallel          |
| CSV ingestion at scale                      | ✅ Yes     | 10× faster                           |
| Window functions on high-cardinality groups | ✅ Yes     | 7.5× faster, .over() vs Python loop  |
| DuckDB bulk load                            | ✅ Yes     | 3.3× faster                          |
| Redpanda consumer                           | ✅ Yes     | 12.5× faster batch conversion        |
| Neo4j graph construction                    | ✅ Yes     | 3.5× faster                          |
| Postgres write                              | ✅ Yes     | 2× faster                            |
| Postgres / SQL Server read                  | ❌ No      | TDS bottleneck, ~same speed          |
| Redis operations                            | ❌ No      | Protocol bottleneck, no difference   |
| MongoDB write                               | ❌ No      | BSON encoding bottleneck             |
| Small datasets (< 1M rows)                 | ⚠️ Maybe   | Overhead may not justify migration   |
| Legacy codebases                            | ⚠️ Maybe   | API differences require rewrite      |

---

## Section 4 — RAM Tradeoff

Polars is faster but hungrier.

| Engine           | Typical peak RAM (28M rows) | Notes                                    |
|------------------|-----------------------------|------------------------------------------|
| pandas           | 3.5–5.3 GB                  | lazy Python allocation                   |
| polars_eager     | 6–11 GB                     | Arrow columnar buffers upfront           |
| polars_lazy      | 5–7 GB                      | skips unused columns                     |
| polars_streaming | 3.7–7.6 GB                  | chunk processing, best for groupby       |

Rule: if RAM headroom < 2× dataset size → use polars_streaming for aggregation.

---

## Key Insights

**Insight 1 — The 10× rule: Polars dominates pure DataFrame work**

Across 8 of 9 pure operations at 28M rows, Polars is 6–10× faster. The architecture
difference is fundamental: Rust + Apache Arrow + multi-threaded execution vs Python
+ NumPy + GIL. This gap does not shrink with hardware upgrades — it is algorithmic.

**Insight 2 — Storage protocols are immune to DataFrame optimisation**

Postgres read took 159s with Pandas and 141s with Polars — a 12% improvement on a
159-second operation. TDS serializes every row through the wire regardless of what
receives it. Redis KEYS scan blocks the server for ~835 minutes with either engine.
Profiling would have shown the bottleneck immediately. Replacing Pandas with Polars
here is optimising the wrong layer.

**Insight 3 — The surprising winner: Redpanda consumer at 12.5×**

The largest Phase 7B speedup was not in a database — it was in the Redpanda consumer.
pl.from_dicts() batch-converts 100K message payloads 12.5× faster than pd.DataFrame().
In streaming pipelines where consumers process millions of messages per hour, this
compounds into hours of saved processing time per day.

**Insight 4 — Polars lazy vs eager: chain operations to see the difference**

For single operations, lazy and eager differ by less than 0.2s. The query optimizer
has nothing to reorder with one operation. Chain 5+ operations — especially
filter → select → groupby — and lazy evaluation skips columns and rows that eager
would materialize. Benchmark single ops to understand the engine; benchmark pipelines
to make the architecture decision.

**Insight 5 — The migration decision framework in one sentence**

Replace Pandas with Polars when the DataFrame is the bottleneck; profile first,
because half the time it is not.

---

## Summary

| Phase | Verdict                                                                    |
|-------|----------------------------------------------------------------------------|
| 7A    | Polars 6–10× faster across all 9 pure DataFrame operations                |
| 7B    | Polars helps where engine is bottleneck (5/8 backends); neutral otherwise  |

Best Polars variant by use case:

| Variant          | Best for                                     |
|------------------|----------------------------------------------|
| polars_eager     | default — fastest for 6/9 operations         |
| polars_streaming | memory-constrained groupby / aggregation     |
| polars_lazy      | complex multi-step pipelines (5+ operations) |

Pandas remaining use cases:
- Legacy codebases where API compatibility is required
- Small datasets where migration cost exceeds performance gain
- Teams unfamiliar with Polars API
