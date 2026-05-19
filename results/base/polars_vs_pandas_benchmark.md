═══════════════════════════════════════
TITLE: Phase 7C — Polars vs Pandas: Complete Comparison
═══════════════════════════════════════

INTRO:
Phase 7C combines all findings from Phase 7A (pure DataFrame, no storage)
and Phase 7B (Polars optimization across all 8 backends) into a single
definitive answer: when does switching from Pandas to Polars matter,
and when does it not?

═══════════════════════════════════════
SECTION 1: Pure DataFrame (Phase 7A recap)
═══════════════════════════════════════
Subtitle: No storage layer — engine overhead only

| Operation | pandas | polars_eager | polars_lazy | polars_streaming | Speedup |
|---|---|---|---|---|---|
| read_csv | 14.64s | 1.46s | 1.64s | 2.23s | 10x |
| filter | 15.42s | 1.49s | 1.50s | 2.33s | 10x |
| groupby | 15.51s | 2.40s | 2.24s | 1.60s | 10x |
| sort | 19.95s | 3.22s | 3.33s | 3.43s | 6x |
| join | 0.08s | 0.05s | 0.11s | 0.37s | 1.6x |
| window | 38.05s | 5.15s | 5.07s | 5.21s | 7.5x |
| string | 17.36s | 1.62s | 1.65s | 2.44s | 10x |
| typecast+null | 19.11s | 1.98s | 1.96s | 2.29s | 10x |
| concat | 15.36s | 1.56s | 1.56s | 2.03s | 10x |

═══════════════════════════════════════
SECTION 2: With Storage Backend (Phase 7B recap)
═══════════════════════════════════════
Subtitle: Real bottleneck revealed — engine vs protocol

| Backend | Operation | Pandas | Polars | Speedup | Bottleneck |
|---|---|---|---|---|---|
| DuckDB | write | 11.69s | 3.48s | 3.3x | ✅ DataFrame |
| DuckDB | read (Arrow) | 6.06s | 3.37s | 1.8x | ✅ DataFrame |
| Parquet | query (streaming) | 0.98s | 0.49s | 2x | ✅ DataFrame |
| Postgres | write | 263.43s | 134.48s | 2x | ✅ DataFrame |
| Postgres | read | 159.16s | 140.99s | ~same | ❌ TDS protocol |
| MongoDB | write | 208.83s | 204.16s | ~same | ❌ pymongo BSON |
| MongoDB | query | 23.57s | 17.89s | 1.3x | ✅ DataFrame |
| SQL Server | write (extrap) | ~5.7h | ~4.2h | ~25% | ❌ TDS protocol |
| Neo4j | write_graph | ~5s | 1.40s | 3.5x | ✅ DataFrame |
| Neo4j | read | ~6s | 7.53s | ~same | ❌ Driver |
| Redis | write | ~370s | 367.62s | ~same | ❌ JSON+pipeline |
| Redis | query | ~835min | ~835min | ~same | ❌ KEYS O(n) |
| Redpanda | producer | ~156s | 160.54s | ~same | ❌ Kafka protocol |
| Redpanda | consumer | ~52s | 4.15s | 12.5x | ✅ DataFrame |

═══════════════════════════════════════
SECTION 3: The Decision Framework
═══════════════════════════════════════
Subtitle: When to switch, when not to bother

Table — "Should I replace Pandas with Polars?":

| Scenario | Switch? | Reason |
|---|---|---|
| In-memory transforms (filter/groupby/sort) | ✅ Yes | 6–10x faster, Rust parallel |
| CSV ingestion at scale | ✅ Yes | 10x faster |
| Window functions on high-cardinality groups | ✅ Yes | 7.5x faster, .over() vs Python loop |
| DuckDB bulk load | ✅ Yes | 3.3x faster |
| Redpanda consumer | ✅ Yes | 12.5x faster batch conversion |
| Neo4j graph construction | ✅ Yes | 3.5x faster |
| Postgres write | ✅ Yes | 2x faster |
| Postgres/SQL Server read | ❌ No | TDS bottleneck, ~same speed |
| Redis operations | ❌ No | Protocol bottleneck, no difference |
| MongoDB write | ❌ No | BSON encoding bottleneck |
| Small datasets (< 1M rows) | ⚠️ Maybe | Overhead may not justify migration |
| Legacy codebases | ⚠️ Maybe | API differences require rewrite |

═══════════════════════════════════════
SECTION 4: RAM tradeoff
═══════════════════════════════════════
Subtitle: Polars is faster but hungrier

| Engine | Typical peak RAM (28M rows) | Notes |
|---|---|---|
| pandas | 3.5–5.3GB | lazy Python allocation |
| polars_eager | 6–11GB | Arrow columnar buffers upfront |
| polars_lazy | 5–7GB | skips unused columns |
| polars_streaming | 3.7–7.6GB | chunk processing, best for groupby |

Rule: if RAM headroom < 2x dataset size → use polars_streaming for aggregation.

═══════════════════════════════════════
SECTION 5: Key Insights (5 total)
═══════════════════════════════════════

Insight 1 — The 10x rule: Polars dominates pure DataFrame work
  Across 8 of 9 pure operations at 28M rows, Polars is 6–10x faster.
  The architecture difference is fundamental: Rust + Apache Arrow +
  multi-threaded execution vs Python + NumPy + GIL.
  This gap does not shrink with hardware upgrades — it is algorithmic.

Insight 2 — Storage protocols are immune to DataFrame optimization
  Postgres read took 159s with Pandas and 141s with Polars — a 12%
  improvement on a 159-second operation. TDS serializes every row
  through the wire regardless of what receives it. Redis KEYS scan
  blocks the server for ~835 minutes with either engine.
  Profiling would have shown the bottleneck immediately.
  Replacing Pandas with Polars here is optimizing the wrong layer.

Insight 3 — The surprising winner: Redpanda consumer at 12.5x
  The largest Phase 7B speedup was not in a database — it was in the
  Redpanda consumer. pl.from_dicts() batch-converts 100K message
  payloads 12.5x faster than pd.DataFrame(). In streaming pipelines
  where consumers process millions of messages per hour, this
  compounds into hours of saved processing time per day.

Insight 4 — Polars lazy vs eager: chain operations to see the difference
  For single operations, lazy and eager differ by less than 0.2s.
  The query optimizer has nothing to reorder with one operation.
  Chain 5+ operations — especially filter → select → groupby — and
  lazy evaluation skips columns and rows that eager would materialize.
  Benchmark single ops to understand the engine; benchmark pipelines
  to make the architecture decision.

Insight 5 — The migration decision framework in one sentence
  Replace Pandas with Polars when the DataFrame is the bottleneck;
  profile first, because half the time it is not.

═══════════════════════════════════════
FINAL SUMMARY BOX
═══════════════════════════════════════

Phase 7 verdict:
  Pure DataFrame (7A):     Polars 6–10x faster across all operations
  Storage backends (7B):   Polars helps where engine is bottleneck (5/8 backends)
                           Polars neutral where protocol is bottleneck (3/8 backends)
  
  Best Polars variant by use case:
    polars_eager     → default, fastest for 6/9 operations
    polars_streaming → memory-constrained groupby/aggregation
    polars_lazy      → complex multi-step pipelines
  
  Pandas remaining use cases:
    - Legacy codebases where API compatibility required
    - Small datasets where migration cost > performance gain
    - Teams unfamiliar with Polars API

  Phase 7D (next): pgBouncer, Arrow IPC, binary Postgres protocol,
  Redpanda multi-partition — attack the protocol bottlenecks directly.
