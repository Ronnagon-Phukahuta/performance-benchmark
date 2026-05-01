# Postgres Benchmark Summary
## Dataset: 48 tickers, 420,922 rows

## Write Operations (sorted by speed)
| Method | Duration | RAM | CPU |
|---|---|---|---|
| bulk_copy (COPY command) | 3.59s 🏆 | 249MB | 85% |
| batch_insert (executemany) | 154.80s | 237MB | 33% |
| row_by_row (execute loop) | 160.33s | 235MB | 36% |

## Read Operations (sorted by speed)
| Method | Duration | RAM | CPU |
|---|---|---|---|
| row_by_row | 1.16s 🏆 | 338MB | 66% |
| batch_insert | 1.23s | 338MB | 65% |
| bulk_copy | 1.99s | 337MB | 57% |

## Query Operations (sorted by speed)
| Method | Duration | RAM | CPU |
|---|---|---|---|
| row_by_row | 0.07s 🏆 | 104MB | 0% |
| batch_insert | 0.12s | 104MB | 0% |
| bulk_copy | 0.22s | 103MB | 0% |

## Key Insights
- bulk_copy (COPY command) fastest write: 3.59s — 44x faster than row_by_row
- All methods read slowly (1-2s) due to TCP overhead even on localhost
- Postgres disk size shows 0MB because data lives inside Docker volume (not measured directly)
- RAM during read is very high (337-338MB) — postgres returns all data through network buffer
- Query performance varies 3x between methods (0.07s vs 0.22s) — surprising given same index
- batch_insert and row_by_row have similar write time — postgres overhead dominates over python loop

## Postgres vs Best Overall (parquet_single_file_polars)
| Metric | Postgres best (bulk_copy) | Parquet best (polars) | Winner |
|---|---|---|---|
| Write | 3.59s | 0.12s | Parquet 🏆 (30x faster) |
| Read | 1.16s | 0.02s | Parquet 🏆 (58x faster) |
| Query | 0.07s | 0.02s | Parquet 🏆 (3.5x faster) |
| RAM (write) | 249MB | 109MB | Parquet 🏆 (2.3x less) |
| Disk | N/A (Docker volume) | 5.8MB | - |

## When to use Postgres over Parquet
- Multiple concurrent users need same data
- Need ACID transactions
- Complex multi-table JOINs
- Data updates/deletes frequently
- Need SQL access from multiple applications
