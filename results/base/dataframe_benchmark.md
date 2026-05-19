# Performance Benchmark Summary — Phase 7A

**Date:** 2026-05-06  
**Dataset:** 28,151,758 rows · 8,049 tickers (5,884 stocks + 2,165 ETFs from Kaggle)  
**Hardware:** Intel Core i5-12400F · 32 GB RAM · NVMe SSD  
**Benchmark:** 9 DataFrame operations (no storage I/O)

---

# Phase 7A — Pure DataFrame Benchmark: Pandas vs Polars

Phase 7A isolates the DataFrame engine from storage I/O entirely.
Same 28,151,758 rows, same 9 operations, four engines:
pandas (baseline), polars_eager, polars_lazy, polars_streaming.
No database, no network, no disk I/O after initial CSV load.
This answers: how much of your pipeline's slowness is the storage layer
vs the DataFrame engine itself?

---

## Results Table

| Operation      | pandas  | polars_eager | polars_lazy | polars_streaming | pandas vs best |
|---------------|---------|--------------|-------------|------------------|---------------|
| op1 read_csv  | 14.64s  | 1.46s        | 1.64s       | 2.23s            | 10x slower    |
| op2 filter    | 15.42s  | 1.49s        | 1.50s       | 2.33s            | 10x slower    |
| op3 groupby   | 15.51s  | 2.40s        | 2.24s       | 1.60s            | 10x slower    |
| op4 sort      | 19.95s  | 3.22s        | 3.33s       | 3.43s            | 6x slower     |
| op5 join      | 0.08s   | 0.05s        | 0.11s       | 0.37s            | 1.6x slower   |
| op6 window    | 38.05s  | 5.15s        | 5.07s       | 5.21s            | 7.5x slower   |
| op7 string    | 17.36s  | 1.62s        | 1.65s       | 2.44s            | 10x slower    |
| op8 typecast  | 19.11s  | 1.98s        | 1.96s       | 2.29s            | 10x slower    |
| op9 concat    | 15.36s  | 1.56s        | 1.56s       | 2.03s            | 10x slower    |

---

## RAM Table

| Operation      | pandas   | polars_eager | polars_lazy | polars_streaming |
|---------------|----------|--------------|-------------|------------------|
| op1 read_csv  | 3,648MB  | 5,948MB      | 6,632MB     | 7,411MB          |
| op2 filter    | 3,634MB  | 6,096MB      | 5,930MB     | 5,271MB          |
| op3 groupby   | 3,490MB  | 7,301MB      | 6,075MB     | 3,677MB          |
| op4 sort      | 4,179MB  | 8,606MB      | 7,131MB     | 7,632MB          |
| op5 join      | 220MB    | 8,514MB      | 6,733MB     | 6,358MB          |
| op6 window    | 5,298MB  | 10,778MB     | 10,252MB    | 7,844MB          |
| op7 string    | 3,582MB  | 8,208MB      | 4,768MB     | 4,570MB          |
| op8 typecast  | 4,500MB  | 6,584MB      | 6,072MB     | 5,800MB          |
| op9 concat    | 4,691MB  | 6,673MB      | 6,575MB     | 6,868MB          |

---

## Winner Per Operation

- op1  polars_eager   — 1.46s
- op2  polars_eager   — 1.49s
- op3  polars_streaming — 1.60s (streaming processes chunks, never loads all 28M at once)
- op4  polars_eager   — 3.22s
- op5  polars_eager   — 0.05s (100K sample, all engines close)
- op6  polars_lazy    — 5.07s
- op7  polars_eager   — 1.62s
- op8  polars_lazy    — 1.96s
- op9  polars_eager/lazy — 1.56s (tie)

---

## Key Insights

**Insight 1 — Pandas is 10x slower on almost every operation**  
Across 8 of 9 operations, Pandas is 6–10x slower than the fastest
Polars variant. The exception is op5 join on a 100K sample where
all engines converge. At 28M rows the GIL, Python object overhead,
and single-threaded execution compound into a consistent 10x penalty.
The gap is not a quirk — it is the architecture.

**Insight 2 — Streaming wins only at groupby, and the reason is specific**  
polars_streaming beat eager on op3 groupby (1.60s vs 2.40s, 33% faster)
but lost on every other operation. Streaming processes data in chunks
without materializing the full 28M rows — perfect for aggregation.
For operations that require the full dataset in memory (sort, window,
concat), streaming adds coordination overhead and loses.

**Insight 3 — Lazy vs Eager: almost no difference for single operations**  
polars_lazy and polars_eager differ by less than 0.2s on every op.
Lazy evaluation's query optimizer shines when multiple operations are
chained — it can reorder, push predicates down, and skip columns.
For isolated single operations, the optimization graph has nothing to
optimize. Chain 5+ operations and the gap widens significantly.

**Insight 4 — op6 window is Pandas' worst case: 38s vs 5s (7.5x)**  
groupby().rolling() in Pandas executes a Python-level loop per group.
With 8,049 tickers, that is 8,049 Python iterations regardless of
chunk size. Polars .over() dispatches to Rust parallel execution —
all groups computed simultaneously across CPU cores.
Window functions on high-cardinality groups are where Polars'
architecture advantage is most visible.

**Insight 5 — Pandas uses less RAM, Polars uses more — and both are correct**  
Pandas peak RAM: 3.5–5.3GB. Polars peak RAM: 5.9–10.8GB.
Polars allocates Apache Arrow columnar buffers upfront for parallel
access — more RAM, but faster computation. Pandas allocates lazily
in Python objects — less RAM, but slower per-operation.
The tradeoff: Polars trades RAM headroom for CPU throughput.
On memory-constrained systems, polars_streaming reduces peak RAM
significantly (op3: 7,301MB eager → 3,677MB streaming).

---

## Summary

Polars eager:     fastest for 6/9 operations
Polars lazy:      fastest for 2/9 operations  
Polars streaming: fastest for 1/9 operations (groupby only)
Pandas:           fastest for 0/9 operations

**Recommendation:**
- Default: polars_eager (fastest overall, simple API)
- Memory constrained: polars_streaming for groupby/aggregation
- Complex pipelines (5+ chained ops): polars_lazy
- Pandas: legacy compatibility only
