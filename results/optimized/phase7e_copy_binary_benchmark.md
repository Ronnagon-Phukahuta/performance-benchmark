# Performance Benchmark Summary — Phase 7E

**Date:** 2026-05-19
**Phase:** 7E — PostgreSQL COPY Protocol: Binary vs CSV vs SELECT
**Dataset:** 28,151,758 rows · 8,049 tickers (5,884 stocks + 2,165 ETFs from Kaggle)
**Hardware:** Intel Core i5-12400F · 32 GB RAM · NVMe SSD
**Benchmark:** Three Postgres read strategies — SELECT binary, COPY BINARY, COPY CSV + Polars SIMD

---

Phase 7E tests the hypothesis that PostgreSQL's `COPY TO STDOUT` protocol is
fundamentally faster than `SELECT *` — even compared to psycopg3 `binary=True`.
The COPY path bypasses PostgreSQL's row executor entirely; the question is whether
the client-side deserialization cost offsets the server-side saving.

A surprise emerges: **text transfer + Polars SIMD parse outperforms binary transfer
+ C-extension deserialization** — both in speed and RAM.

---

## Write (28,151,758 rows via COPY FROM STDIN)

| Method                     | Duration   | Peak RAM    | vs psycopg2 baseline |
|----------------------------|------------|-------------|----------------------|
| psycopg2 baseline (Phase 1)| 263.43s    | 16,352 MB   | —                    |
| psycopg3 Phase 7D          | 272s       | —           | ~same                |
| psycopg3 Phase 7E          | 262.40s    | 7,905.9 MB  | ~same (-0.4%)        |

**Finding:** Write is stable across all psycopg3 runs. COPY FROM STDIN is a raw
byte stream at the wire level — driver version and binary mode have no effect.

---

## Read (SELECT * — 28,151,758 rows)

| Strategy                             | Duration   | Peak RAM     | vs psycopg2 (159s) | Speedup |
|--------------------------------------|------------|--------------|---------------------|---------|
| psycopg2 baseline (Phase 1)          | 159.16s    | 19,125 MB    | —                   | —       |
| psycopg3 + pgBouncer (Phase 7D best) | 109s       | —            | -31%                | 1.46×   |
| **[1] SELECT + binary cursor (rerun)**| **123.77s**| **20,719 MB**| -22%                | 1.29×   |
| **[2] COPY TO STDOUT FORMAT BINARY** | **81.68s** | **13,732 MB**| **-49%**            | **1.95×** |
| **[3] COPY TO STDOUT FORMAT CSV → Polars** | **67.23s** | **4,256.6 MB** | **-58%** | **2.37×** |

> Strategy 1 rerun (123.77s) differs from Phase 7D (142s) — measurement variance
> from system state, page cache warmth, and background process noise.

**Finding:** COPY CSV + Polars SIMD wins on **both** speed and RAM.
67s vs 123s SELECT binary = **1.84× faster**. 4.3 GB vs 20.7 GB = **4.9× lower RAM**.
Text on the wire is cheaper than row-level Python deserialization at this scale.

### Progressive improvement chain — Postgres read

```
psycopg2 SELECT (Phase 1)        159s   19,125 MB   baseline
psycopg3 SELECT binary (Phase 7D) 142s       —       -11%
psycopg3 + pgBouncer (Phase 7D)   109s       —       -31%
COPY BINARY (Phase 7E)             81s   13,732 MB   -49%
COPY CSV + Polars (Phase 7E)       67s    4,257 MB   -58%  ← winner
```

---

## Query (COPY BINARY → Polars → DuckDB GROUP BY)

| Method                              | Duration  | Peak RAM    | vs Postgres native |
|-------------------------------------|-----------|-------------|---------------------|
| Postgres native GROUP BY (psycopg2) | 24.27s    | 3,474 MB    | —                   |
| COPY BINARY → DuckDB (Phase 7E)     | 76.75s    | 15,563.6 MB | **3.2× slower**     |

**Finding:** Query via COPY-then-DuckDB is a regression. Pulling all 28M rows to
aggregate them in-process is always slower than pushing the GROUP BY to the server.
Postgres executes the aggregation once, transfers only 8,049 result rows (one per
ticker). COPY transfers 28M rows × 7 columns, then DuckDB aggregates them locally.
The takeaway: COPY is the right tool for bulk data transfer, not for aggregation.

---

## Key Insights

**Insight 1 — COPY bypasses the executor; SELECT does not — that is the speed difference**

A PostgreSQL `SELECT *` runs through the full planner and executor pipeline. Each
row passes through the heap access method, the output tuple formatter, and the wire
encoding function per column. `COPY TO STDOUT` uses a separate export path with no
planner involvement — raw heap pages are read and the COPY send functions run
directly. The server does less work per row, which accounts for the COPY BINARY
advantage over SELECT binary (81s vs 123s).

**Insight 2 — Polars SIMD parse beats C-extension binary deserialization**

Strategy 2 (COPY BINARY + psycopg3 C extension): 81.68s / 13.7 GB RAM.
Strategy 3 (COPY CSV + Polars `read_csv`): 67.23s / 4.3 GB RAM.

The COPY CSV approach transfers text bytes, then Polars parses the entire buffer in
one vectorized pass using SIMD instructions in Rust — no Python-level iteration.
COPY BINARY transfers fewer bytes (IEEE-754 vs ASCII digits), but psycopg3 still
iterates 28M rows in Python, creating one tuple per row via the C extension. Python
object creation overhead (28M tuples × 7 fields = 196M allocations) dominates.
The SIMD parser eliminates the Python loop entirely.

**Insight 3 — RAM reduction is the more significant win**

Speed: COPY CSV is 1.84× faster than SELECT binary.
RAM: COPY CSV uses 4,257 MB vs 20,719 MB for SELECT binary — **4.9× lower**.

The RAM difference matters more in production. SELECT binary with psycopg3
materializes all rows as Python tuples before DataFrame conversion: 28M tuples ×
7 values × ~56 bytes CPython overhead = ~11 GB, plus the DataFrame copy. COPY CSV
streams bytes into a BytesIO buffer and hands it directly to Polars' memory-mapped
parser — no intermediate Python objects. At production scale, this is the difference
between fitting in memory and requiring a larger machine.

**Insight 4 — Query regression: COPY is the wrong tool for aggregation**

Postgres GROUP BY: 24.27s, transfers 8,049 rows.
COPY BINARY → DuckDB: 76.75s, transfers 28,151,758 rows.

The regression is expected and illustrates a fundamental principle: push computation
to where the data lives. Postgres can aggregate 28M rows internally in 24 seconds.
Transferring those rows to Python and aggregating with DuckDB takes 76 seconds —
3.2× slower despite DuckDB being a fast in-process engine. COPY is optimal for bulk
export; `SELECT GROUP BY` is optimal for server-side aggregation.

**Insight 5 — The full optimisation story across seven phases**

| Phase | Change                            | Read time | RAM      |
|-------|-----------------------------------|-----------|----------|
| 1     | psycopg2 + Pandas (baseline)      | 159s      | 19,125 MB|
| 7B    | psycopg2 + Polars                 | 141s      | 17,622 MB|
| 7D    | psycopg3 binary direct            | 142s      | —        |
| 7D    | psycopg3 + pgBouncer              | 109s      | —        |
| 7E    | COPY BINARY + psycopg3            | 81s       | 13,732 MB|
| 7E    | **COPY CSV + Polars SIMD**        | **67s**   | **4,257 MB** |

Starting from 159s / 19 GB, the optimisation chain reached 67s / 4.3 GB —
**2.37× faster and 4.5× lower RAM** — without touching Postgres configuration,
hardware, or schema. Every gain came from choosing the right protocol and parser.

---

## Configuration

```python
# COPY TO STDOUT FORMAT CSV → Polars (winning strategy)
buf = io.BytesIO()
with psycopg.connect(**DB_PARAMS) as conn:
    with conn.cursor() as cur:
        with cur.copy("COPY stocks_copy_binary TO STDOUT (FORMAT CSV, HEADER TRUE)") as copy:
            for chunk in copy:
                buf.write(chunk)
buf.seek(0)
df = pl.read_csv(buf, schema_overrides={"volume": pl.Int64})

# COPY TO STDOUT FORMAT BINARY → Polars (runner-up)
with psycopg.connect(**DB_PARAMS) as conn:
    with conn.cursor() as cur:
        with cur.copy("COPY stocks_copy_binary TO STDOUT (FORMAT BINARY)") as copy:
            copy.set_types(["text", "text", "float8", "float8", "float8", "float8", "int8"])
            rows = list(copy.rows())
df = pl.DataFrame(dict(zip(COLUMNS, zip(*rows))))
```
