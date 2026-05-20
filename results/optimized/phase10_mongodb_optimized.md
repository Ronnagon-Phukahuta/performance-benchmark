# Phase 10 — MongoDB Optimisation Deep Dive

**Date:** 2026-05-20
**Phase:** 10 — MongoDB access pattern tuning
**Dataset:** 28,151,758 rows · 8,049 tickers
**Baseline:** Phase 6 — full dataset MongoDB write (OOM path)

---

Phase 10 analyses MongoDB performance in depth, covering index strategy, query
plan selection, OOM resolution, and write order effects. Most optimisations were
validated as part of the Phase 9 indexed benchmark.

---

## 1. OOM Resolution (Phase 6 → Phase 10)

### Problem

Phase 6 naively loaded the full 28M-row cursor into a Python list:
```python
docs = list(collection.find({}))  # OOM — 28M dicts in-flight
```
At ~500 bytes/document this requires ~14 GB, triggering an OOM kill before any
processing could begin.

### Solution: chunked iteration

```python
CHUNK = 50_000
cursor = collection.find({})
for batch in iter(lambda: list(islice(cursor, CHUNK)), []):
    process(batch)
```

Peak RAM dropped from OOM to under 2.6 GB. Throughput was unchanged — MongoDB
cursor iteration is constant-time regardless of chunk size.

---

## 2. Write Order Effect (ordered=False)

MongoDB's `insert_many()` has two modes:
- `ordered=True` (default): serial — stops on first error
- `ordered=False`: parallel server-side processing, continues on errors

Phase 1 tests showed `ordered=False` is 156× faster for bulk ingestion because
MongoDB can pipeline inserts across multiple storage engine threads.

```python
collection.insert_many(docs, ordered=False)  # Phase 9/10 default
```

For 28M rows the difference is measured in minutes vs hours.

---

## 3. Index Strategy by Query Pattern

### Single-field index: ticker_1

Optimal for:
- Point lookup: `{ticker: 'AAPL'}` → IXSCAN, 0.04s
- NOT recommended for GROUP BY: forces ordered scan, 40.99s vs 24.27s

### Compound index: ticker_1_date_1

Optimal for:
- Date range within a ticker: `{ticker: 'AAPL', date: {$gte: ..., $lte: ...}}` → 0.01s
- Covers both equality and range in one index entry

The left-most prefix rule applies. A compound index `(ticker, date)` also serves
pure ticker equality queries.

### No index: GROUP BY / $group aggregation pipeline

The `$group` stage with `{$natural: 1}` hint forces a collection scan, allowing
MongoDB to operate on bulk BSON document batches. Adding an index hint triggers
IXSCAN which enforces key ordering — this adds overhead for operations that
cannot skip rows:

| Method | Duration |
|---|---|
| $group, no index (COLLSCAN) | 24.27s |
| $group, ticker_1 hint (IXSCAN) | 40.99s |

**Conclusion**: do not hint an index on full-aggregation pipelines.

---

## 4. explain() Output Interpretation

MongoDB `explain()` (executionStats) for optimal queries:

```
IXSCAN on ticker_1
  totalDocsExamined  = nReturned       ← no wasted reads
  totalKeysExamined  = nReturned       ← perfect selectivity
  executionTimeMs    ≈ 36              ← 0.04s wall-clock
```

For optimal index usage, `totalDocsExamined == nReturned`. Any gap indicates
partial index utilisation (e.g., filtering post-scan).

---

## 5. Comparison vs Postgres at Same Queries

| Query | Postgres (no idx) | MongoDB (no idx) | Postgres (idx) | MongoDB (idx) |
|---|---|---|---|---|
| GROUP BY ticker | 37.08s | 24.27s | 18.92s | 40.99s ↑ |
| Lookup AAPL | 22.80s | 7.39s | **0.04s** | **0.04s** |
| Date range | 19.96s | — | 2.19s | **0.01s** |

MongoDB is faster without indexes (document BSON vs text decoding overhead in
Postgres). With indexes, both converge for point lookups. MongoDB's compound
index wins handily for date range queries.

---

## 6. When to Use MongoDB vs Postgres

| Scenario | Recommendation |
|---|---|
| Ad hoc aggregation over full dataset | Either — MongoDB slightly faster |
| Point lookup by known key | Both — identical with index |
| Range query on known key+date | MongoDB — compound index wins |
| Schema rigidity required | Postgres |
| Horizontal write scaling | MongoDB (ordered=False sharding) |
| SQL compatibility needed | Postgres |
