# Performance Benchmark — Final Summary
## Dataset: 48 tickers, 420,922 rows, 10 years daily OHLCV

---

## 🏆 Overall Champions

| Category | Champion | Score |
|---|---|---|
| Fastest Write | parquet_single_file_polars | 0.12s |
| Fastest Read | parquet_single_file_polars | 0.02s |
| Fastest Query | duckdb_batch_insert_polars | 0.015s |
| Lowest RAM | parquet_single_file_polars | 109MB |
| Smallest Disk | parquet_single_file_polars | 5.8MB |

---

## 📊 Write Operations — All Methods (sorted by speed)

| Method | Duration | RAM | CPU | Disk |
|---|---|---|---|---|
| parquet_single_file_polars | 0.12s 🏆 | 109MB | 189% | 5.8MB |
| parquet_compressed_snappy | 0.18s | 146MB | 91% | 7.6MB |
| parquet_single_file_pandas | 0.18s | 158MB | 99% | 7.6MB |
| duckdb_bulk_insert_pandas | 0.22s | 182MB | 150% | 33MB |
| duckdb_copy_csv | 0.33s | 97MB | 153% | 69MB |
| parquet_compressed_gzip | 0.64s | 186MB | 98% | 6.2MB |
| parquet_partitioned | 1.02s | 127MB | 101% | 11.8MB |
| duckdb_bulk_insert_polars | 1.11s | 226MB | 96% | 106MB |
| postgres_bulk_copy | 3.59s | 249MB | 85% | N/A |
| postgres_batch_insert | 154.80s | 237MB | 33% | N/A |
| postgres_row_by_row | 160.33s | 235MB | 36% | N/A |
| duckdb_batch_insert_polars | 310.12s | 174MB | 84% | 127MB |
| duckdb_row_by_row_polars | 310.85s | 321MB | 85% | 90MB |
| duckdb_row_by_row_pandas | 317.73s | 268MB | 85% | 17MB |
| duckdb_batch_insert_pandas | 320.85s | 262MB | 85% | 54MB |

---

## 📊 Read Operations — All Methods (sorted by speed)

| Method | Duration | RAM | CPU |
|---|---|---|---|
| parquet_single_file_polars | 0.02s 🏆 | 108MB | 0% |
| parquet_single_file_pandas | 0.07s | 217MB | 134% |
| parquet_compressed_snappy | 0.07s | 232MB | 106% |
| duckdb_bulk_insert_pandas | 0.09s | 173MB | 98% |
| duckdb_row_by_row_pandas | 0.09s | 186MB | 71% |
| duckdb_batch_insert_pandas | 0.09s | 171MB | 103% |
| duckdb_row_by_row_polars | 0.09s | 224MB | 126% |
| duckdb_batch_insert_polars | 0.10s | 189MB | 92% |
| duckdb_bulk_insert_polars | 0.10s | 241MB | 127% |
| parquet_partitioned | 0.47s | 181MB | 72% |
| duckdb_copy_csv | 0.55s | 143MB | 82% |
| postgres_row_by_row | 1.16s | 338MB | 66% |
| postgres_batch_insert | 1.23s | 338MB | 65% |
| postgres_bulk_copy | 1.99s | 337MB | 57% |

---

## 📊 Query Operations — All Methods (sorted by speed)

| Method | Duration | RAM | CPU |
|---|---|---|---|
| duckdb_batch_insert_polars | 0.015s 🏆 | 146MB | 0% |
| duckdb_copy_csv | 0.017s | 91MB | 0% |
| parquet_single_file_polars | 0.017s | 115MB | 0% |
| duckdb_batch_insert_pandas | 0.018s | 97MB | 0% |
| duckdb_bulk_insert_pandas | 0.019s | 94MB | 0% |
| duckdb_row_by_row_polars | 0.019s | 164MB | 0% |
| duckdb_row_by_row_pandas | 0.020s | 99MB | 0% |
| duckdb_bulk_insert_polars | 0.022s | 163MB | 0% |
| parquet_compressed_snappy | 0.041s | 226MB | 0% |
| parquet_single_file_pandas | 0.047s | 210MB | 0% |
| postgres_row_by_row | 0.070s | 104MB | 0% |
| parquet_partitioned | 0.111s | 181MB | 117% |
| postgres_batch_insert | 0.120s | 104MB | 0% |
| postgres_bulk_copy | 0.224s | 103MB | 0% |

---

## 💡 Key Insights

### Parquet + Polars
- Wins write, read, RAM, disk across all methods
- Polars uses zstd compression by default → smaller files than pandas snappy
- No server overhead, pure file I/O

### DuckDB
- bulk_insert (pandas) fastest write among database engines (0.22s)
- Query performance best among all methods (0.015s) when data pre-loaded
- copy_csv uses least RAM (97MB) but slowest read
- row_by_row and batch both ~320s — algorithm matters enormously

### Postgres
- COPY command 44x faster than row_by_row for write
- Read always slow (1-2s) due to TCP overhead even on localhost
- High RAM on read (338MB) — network buffer overhead
- Best for: concurrent access, ACID, complex JOINs, multi-app

### Pandas vs Polars
- For DuckDB: pandas bulk_insert wins (0.22s vs 1.11s)
- For Parquet: polars wins everything (write/read/query/RAM/disk)
- No universal winner — depends on storage backend

### The 1,444x Rule
- DuckDB row_by_row (317s) vs bulk_insert (0.22s) = 1,444x difference
- Same data, same destination, different algorithm
- This is why algorithm selection matters more than hardware

---

## 🎯 Recommendation by Use Case

| Use Case | Recommended |
|---|---|
| Analytics / read-heavy | Parquet + Polars |
| Fast ingestion, no queries | Parquet + Polars |
| Complex SQL queries | DuckDB bulk_insert |
| Multi-user / concurrent | Postgres + COPY |
| Smallest disk footprint | Parquet + Polars (5.8MB) |
| Lowest RAM | DuckDB copy_csv (97MB write) |
