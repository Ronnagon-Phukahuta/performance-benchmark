# Performance Benchmark

A systematic benchmark comparing storage engines, algorithms, and libraries for financial time-series data.

## Dataset
- 48 S&P 500 tickers, 420,922 rows, 40 years daily OHLCV (1980–2020)
- Source: Kaggle Stock Market Dataset

## What's Benchmarked

### Storage Engines
- **DuckDB** — embedded analytical database
- **Parquet** — columnar file format
- **Postgres** — traditional relational database (via Docker)

### Algorithms
- Row-by-row insert
- Bulk insert
- Batch insert
- COPY command (Postgres)
- Direct CSV read (DuckDB)
- Partitioned files (Parquet)
- Compressed variants: snappy, gzip

### Libraries
- **Pandas** vs **Polars** — for every applicable method

### Metrics
- Write speed (seconds)
- Read speed (seconds)
- Query speed (seconds) — `AVG/MAX/MIN close GROUP BY ticker`
- Peak RAM usage (MB)
- CPU utilization (%)
- Disk size (MB)

## Results Summary

### 🏆 Overall Champions

| Category | Champion | Score |
|---|---|---|
| Fastest Write | parquet + polars | 0.12s |
| Fastest Read | parquet + polars | 0.02s |
| Fastest Query | duckdb bulk_insert polars | 0.015s |
| Lowest RAM | parquet + polars | 109MB |
| Smallest Disk | parquet + polars | 5.8MB |

### Write Performance (top 8)

| Method | Duration | RAM | Disk |
|---|---|---|---|
| parquet_single_file_polars | 0.12s 🏆 | 109MB | 5.8MB |
| parquet_compressed_snappy | 0.18s | 146MB | 7.6MB |
| parquet_single_file_pandas | 0.18s | 158MB | 7.6MB |
| duckdb_bulk_insert_pandas | 0.22s | 182MB | 33MB |
| duckdb_copy_csv | 0.33s | 97MB | 69MB |
| parquet_compressed_gzip | 0.64s | 186MB | 6.2MB |
| parquet_partitioned | 1.02s | 127MB | 11.8MB |
| postgres_bulk_copy | 3.59s | 249MB | N/A |

### Read Performance (top 6)

| Method | Duration | RAM |
|---|---|---|
| parquet_single_file_polars | 0.02s 🏆 | 108MB |
| parquet_single_file_pandas | 0.07s | 217MB |
| parquet_compressed_snappy | 0.07s | 232MB |
| duckdb (all variants) | ~0.09s | 171-241MB |
| parquet_partitioned | 0.47s | 181MB |
| postgres (all variants) | 1.2-2.0s | 337-338MB |

### Query Performance (top 6)

| Method | Duration | RAM |
|---|---|---|
| duckdb_batch_insert_polars | 0.015s 🏆 | 146MB |
| duckdb_copy_csv | 0.017s | 91MB |
| parquet_single_file_polars | 0.017s | 115MB |
| duckdb (other variants) | ~0.019-0.022s | 94-164MB |
| postgres_row_by_row | 0.070s | 104MB |
| parquet_partitioned | 0.111s | 181MB |

## Key Insights

### The 1,444x Rule
DuckDB row_by_row (317s) vs bulk_insert (0.22s) = **1,444x difference**
Same data, same destination, different algorithm. Algorithm selection matters more than hardware.

### Pandas vs Polars
- For **DuckDB**: pandas bulk_insert wins (0.22s vs 1.11s)
- For **Parquet**: polars wins everything (write/read/query/RAM/disk)
- No universal winner — depends on storage backend

### Postgres TCP Overhead
Even on localhost, Postgres read is 58x slower than Parquet due to TCP buffer overhead.
Use Postgres when you need: concurrent access, ACID, complex JOINs, multi-application access.

## Recommendation by Use Case

| Use Case | Recommended |
|---|---|
| Analytics / read-heavy | Parquet + Polars |
| Fast ingestion | Parquet + Polars |
| Complex SQL queries | DuckDB + bulk_insert |
| Multi-user / concurrent | Postgres + COPY |
| Smallest disk | Parquet + Polars (5.8MB) |
| Lowest RAM | DuckDB copy_csv (97MB) |

## Setup

```bash
# Install dependencies
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

# Start Postgres
docker compose up -d

# Download data (requires Kaggle account)
# Place dataset in kaggle-dataset/stocks/

# Run all benchmarks
py -m loaders.duckdb.bulk_insert
py -m loaders.parquet.single_file_polars
py -m loaders.postgres.bulk_copy

# View results
py -m benchmark.run_all
```

## Project Structure

```
performance-benchmark/
├── benchmark/
│   ├── metrics.py        # measure speed, RAM, CPU, disk
│   └── run_all.py        # comparison table
├── loaders/
│   ├── duckdb/           # 7 DuckDB variants
│   ├── parquet/          # 4 Parquet variants
│   └── postgres/         # 3 Postgres variants
├── results/              # benchmark summaries
└── docker-compose.yml    # Postgres via Docker
```
