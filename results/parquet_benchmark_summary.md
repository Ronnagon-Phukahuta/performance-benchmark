# Parquet Benchmark Summary
## Dataset: 48 tickers, 420,922 rows

## Write Operations (sorted by speed)
| Method | Duration | RAM | CPU | Disk |
|---|---|---|---|---|
| single_file_polars | 0.12s 🏆 | 109MB 🏆 | 189% | 5.8MB 🏆 |
| compressed_snappy | 0.18s | 146MB | 91% | 7.6MB |
| single_file (pandas) | 0.18s | 158MB | 99% | 7.6MB |
| compressed_gzip | 0.64s | 186MB | 98% | 6.2MB |
| partitioned | 1.02s | 127MB | 101% | 11.8MB |

## Read Operations (sorted by speed)
| Method | Duration | RAM | CPU | Disk |
|---|---|---|---|---|
| single_file_polars | 0.02s 🏆 | 108MB 🏆 | 0% | 5.8MB |
| single_file (pandas) | 0.07s | 217MB | 134% | 7.6MB |
| compressed_snappy | 0.07s | 232MB | 106% | 7.6MB |
| partitioned | 0.47s | 181MB | 72% | 11.8MB |

## Query Operations (sorted by speed)
| Method | Duration | RAM | CPU | Disk |
|---|---|---|---|---|
| single_file_polars | 0.02s 🏆 | 115MB 🏆 | 0% | 5.8MB |
| compressed_snappy | 0.04s | 226MB | 0% | 7.6MB |
| single_file (pandas) | 0.05s | 210MB | 0% | 7.6MB |
| partitioned | 0.11s | 181MB | 117% | 11.8MB |

## Compression Comparison
| Codec | Write Duration | Disk Size | Read Duration |
|---|---|---|---|
| None (default) | 0.18s | 7.6MB | 0.07s |
| Snappy | 0.18s | 7.6MB | 0.07s |
| Gzip | 0.64s | 6.2MB 🏆 | — |
| Polars default | 0.12s 🏆 | 5.8MB 🏆 | 0.02s 🏆 |

## Key Insights
- **Polars dominates all categories**: fastest write (0.12s), read (0.02s), query (0.02s), smallest disk (5.8MB), lowest RAM (109MB)
- **Snappy ≈ no compression** for this dataset: same disk size (7.6MB), same write speed — snappy overhead is negligible
- **Gzip saves ~19% disk** vs snappy (6.2MB vs 7.6MB) but costs 3.5× longer write time (0.64s vs 0.18s)
- **Partitioned write is slowest** (1.02s) due to 48 separate file opens, but uses least RAM during write (127MB)
- **Partitioned read is slowest** (0.47s) because of 48 separate `read_parquet` + `concat` calls
- All parquet query times are CPU-unloaded (0% CPU) except partitioned — groupby on pre-loaded columnar data is essentially free
- Polars file (5.8MB) is smaller than pandas snappy (7.6MB) — Polars uses zstd compression by default in `write_parquet`

## Parquet vs DuckDB Best (bulk_insert_pandas write: 0.22s)
| Metric | Parquet best (polars) | DuckDB best (bulk_insert) | Winner |
|---|---|---|---|
| Write | 0.12s | 0.22s | Parquet 🏆 (1.8× faster) |
| Read | 0.02s | 0.09s | Parquet 🏆 (4.5× faster) |
| Query | 0.02s | 0.02s | Tie |
| Disk | 5.8MB | 33MB | Parquet 🏆 (5.7× smaller) |
| RAM (write) | 109MB | 182MB | Parquet 🏆 (1.7× less) |

**Conclusion**: Parquet + Polars outperforms DuckDB bulk_insert on every metric except query (tie). Parquet is a pure storage format — no server, no overhead, and Polars' columnar engine reads it faster than DuckDB can deserialize its own binary format.
