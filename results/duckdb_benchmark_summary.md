# DuckDB Benchmark Summary
## Dataset: 48 tickers, 420,922 rows

## Write Operations (sorted by speed)
| Method | Duration | RAM | CPU | Disk |
|---|---|---|---|---|
| bulk_insert (pandas) | 0.22s 🏆 | 182MB | 149% | 33MB |
| copy_csv (no pandas) | 0.33s | 97MB 🏆 | 153% | 69MB |
| bulk_insert (polars) | 1.11s | 226MB | 96% | 106MB |
| batch_insert (polars) | 310.12s | 174MB | 84% | 127MB |
| row_by_row (polars) | 310.85s | 321MB | 85% | 90MB |
| row_by_row (pandas) | 317.73s | 268MB | 85% | 17MB |
| batch_insert (pandas) | 320.85s | 262MB | 85% | 54MB |

## Read Operations (sorted by speed)
| Method | Duration | RAM | CPU | Disk |
|---|---|---|---|---|
| row_by_row (pandas) | 0.07s 🏆 | 155MB | 71% | 17MB |
| bulk_insert (pandas) | 0.09s | 173MB | 98% | 33MB |
| row_by_row (polars) | 0.09s | 224MB | 126% | 90MB |
| batch_insert (pandas) | 0.09s | 171MB | 103% | 54MB |
| batch_insert (polars) | 0.10s | 189MB | 92% | 127MB |
| bulk_insert (polars) | 0.10s | 241MB | 127% | 106MB |
| copy_csv | 0.55s | 143MB | 82% | 69MB |

## Key Insights
- bulk_insert (pandas) fastest write: 0.22s vs row_by_row 317s = **1,444x faster**
- copy_csv uses least RAM on write: 97MB (no pandas overhead)
- polars does NOT outperform pandas for DuckDB bulk operations
- All methods have near-identical query performance (~0.015-0.022s)
- row_by_row and batch_insert are equally slow regardless of pandas/polars
- copy_csv slowest on read (0.55s) because data not pre-loaded into memory
