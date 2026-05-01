# Examples — What Not To Do (and Why)

This folder contains educational examples of code patterns that **cause failures at production scale**.
Every example is intentionally non-runnable (raises RuntimeError) to prevent accidental execution.

## OOM Examples (`oom/`)

| File | Issue | Scale | Result |
|---|---|---|---|
| mongodb_to_dicts_oom.py | `to_dicts()` on 28M docs | 28M rows | OOM — 31GB RAM, Windows dialog |
| mongodb_full_scan_oom.py | `find({})` full collection scan | 28M docs | MemoryError mid-cursor |

## Performance Examples (`performance/`)

| File | Issue | Scale | Result |
|---|---|---|---|
| parquet_partitioned_naive.py | O(n×p) per-ticker loop | 8,049 tickers | ~2h DNF |
| duckdb_row_by_row_vs_batch.py | Python loop overhead | 28M rows | batch ≈ row_by_row (~6h both) |

## Key Lesson

> These failures only appear at production scale. At 48 tickers / 420K rows (Phase 1), every pattern above completed successfully.
> Scale reveals what correctness hides.
