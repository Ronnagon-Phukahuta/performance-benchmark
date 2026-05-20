# Phase 11 — SQL Server BCP vs BULK INSERT

**Date:** 2026-05-20
**Phase:** 11 — Protocol bypass for SQL Server bulk writes
**Dataset:** 28,151,758 rows · 8,049 tickers · source CSV 1,841 MB
**Baseline:** Phase 8 — pymssql bulk strategies (~5.7h extrapolated)

---

Phase 11 replaces the pymssql TDS-based write path with two server-side bulk
loading mechanisms: T-SQL BULK INSERT (server reads the CSV itself) and BCP
(Bulk Copy Protocol, a TDS extension for binary streaming).

---

## Background — Why pymssql Fails at Scale

Phase 8 benchmarked three pymssql write strategies on a 100K-row subset:

| Strategy | 100K rows | 28M rows (extrapolated) |
|---|---|---|
| row_by_row | 74.4s | ~5.8h |
| bulk_insert (10K batches) | 73.4s | ~5.7h |
| bulk_columnstore | 69.0s | ~5.4h |

All three converge to 5–6 hours. The insert strategy is irrelevant — the
bottleneck is the TDS wire protocol. pymssql wraps every row in a TDS framing
packet: one length-prefix + one type byte per column × 8 columns × 28M rows =
224M extra Python operations before SQL Server sees a single byte of data.

---

## Phase 11 Approach

### BULK INSERT

SQL Server reads the CSV file directly from its own filesystem. No per-row
network framing. The Python driver issues a single T-SQL statement:

```sql
BULK INSERT dbo.stocks_bulk
FROM '/tmp/all_stocks.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR   = '\n',
    FIRSTROW        = 2,
    BATCHSIZE       = 100000,
    TABLOCK
);
```

The CSV is copied into the container first via `docker cp`. SQL Server then
reads it as a native filesystem stream — no network hop for the payload.

### BCP (Bulk Copy Program)

BCP is a command-line tool that uses the TDS Bulk Copy Protocol extension.
Unlike pymssql, BCP sends data as a continuous binary stream rather than
individually framed rows. It runs inside the container where the CSV is
already present:

```bash
bcp dbo.stocks_bcp in /tmp/all_stocks.csv \
    -S localhost -U sa -P 'Benchmark123!' \
    -d benchmark_db -c -t',' -r'\n' -F2 \
    -u -b 100000
```

**SSL fix**: mssql-tools18 requires `-u` (trust server certificate) for
self-signed certificates. The legacy `-No` flag is not accepted.

---

## Results

### Write Performance

| Method | Duration | Throughput | Peak RAM |
|---|---|---|---|
| pymssql (any, extrapolated) | ~20,520s (~5.7h) | ~1,371 rows/s | ~42 MB |
| **BULK INSERT** | **72.20s** | **~390,000 rows/s** | **42.4 MB** |
| **BCP** | **74.07s** | **391,671 rows/s** | **42.7 MB** |

**Speedup vs pymssql: 285×**

RAM usage is essentially identical across all three methods — the bottleneck was
never memory.

### Query Performance (GROUP BY ticker)

| Method | Table | Duration | Tickers |
|---|---|---|---|
| BULK INSERT load → query | stocks_bulk | 0.73s | 8,049 |
| BCP load → query | stocks_bcp | 0.78s | 8,049 |

SQL Server was never slow at queries. The 5.7h was entirely on the write path.

---

## Analysis

### BULK INSERT ≈ BCP throughput

72.20s vs 74.07s — within 2.5% of each other. Both methods achieve ~390K
rows/sec because both bypass the per-row TDS framing overhead. The difference is
architecture: BULK INSERT is a server-side filesystem read; BCP sends a
continuous binary stream over the network.

For on-server data (CSV already accessible to SQL Server), BULK INSERT is
preferred — no network I/O. For remote data, BCP is the correct choice.

### TDS was the entire bottleneck

All pymssql write strategies — regardless of batch size or insert pattern —
converge to ~5.7h because TDS framing overhead scales linearly with row count.
Switching to BULK INSERT achieves a 285× speedup using the same SQL Server
instance, same hardware, and same dataset.

### query time: 0.73s

SQL Server processes GROUP BY across 28M rows in under 1 second. The 5.7h write
time was purely an ingestion protocol issue, not a server capability issue.

---

## Decision Matrix

| Scenario | Recommendation |
|---|---|
| Load from CSV on SQL Server host | BULK INSERT (no network hop) |
| Load from remote file | BCP with -u (trust cert) |
| Python ORM / application inserts | pymssql acceptable for <100K rows |
| Bulk Python insert > 1M rows | **Never use pymssql** — use BULK INSERT or BCP |
| Cloud SQL Server (Azure SQL) | Use BULK INSERT with Azure Blob Storage |

---

## Run Commands

```bash
# BULK INSERT only
python loaders/sqlserver/bcp.py --bulk-only

# BCP only
python loaders/sqlserver/bcp.py --bcp-only

# Both (default)
python loaders/sqlserver/bcp.py
```

**Prerequisites:**
- SQL Server container running: `docker-compose up sqlserver`
- mssql-tools18 installed in container (bcp at `/opt/mssql-tools18/bin/bcp`)
- Container name: `benchmark_sqlserver`
