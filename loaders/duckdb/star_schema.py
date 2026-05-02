import os
import threading
import time

import duckdb

from benchmark.metrics import measure

DIM_CSV    = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "dim_symbols.csv")
FACT_CSV   = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices.csv")
DUCKDB_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "duckdb")
DUCKDB_PATH = os.path.join(DUCKDB_DIR, "star_schema.db")

CREATE_DIM_SQL = """
    CREATE TABLE dim_symbols (
        ticker_id  INT,
        ticker     VARCHAR,
        type       VARCHAR,
        sector     VARCHAR,
        industry   VARCHAR,
        exchange   VARCHAR
    )
"""

CREATE_FACT_SQL = """
    CREATE TABLE fact_prices (
        ticker_id  INT,
        date       DATE,
        open       DOUBLE,
        high       DOUBLE,
        low        DOUBLE,
        close      DOUBLE,
        volume     DOUBLE
    )
"""

QUERY_JOIN_SQL = """
    SELECT
        d.sector,
        AVG(f.close) AS avg_close,
        MAX(f.close) AS max_close,
        MIN(f.close) AS min_close
    FROM fact_prices f
    JOIN dim_symbols d ON f.ticker_id = d.ticker_id
    GROUP BY d.sector
    ORDER BY d.sector
"""

QUERY_OLTP_SQL = """
    SELECT *
    FROM fact_prices
    WHERE ticker_id = ?
      AND date BETWEEN ? AND ?
"""


def write_dim() -> None:
    os.makedirs(DUCKDB_DIR, exist_ok=True)
    dim_csv_abs = os.path.abspath(DIM_CSV)
    print(f"Running write_dim benchmark (COPY from {dim_csv_abs})...")
    with measure("duckdb_star_write_dim", data_path=DUCKDB_DIR) as m:
        con = duckdb.connect(DUCKDB_PATH)
        con.execute("DROP TABLE IF EXISTS dim_symbols")
        con.execute(CREATE_DIM_SQL)
        con.execute(f"COPY dim_symbols FROM '{dim_csv_abs}' (AUTO_DETECT TRUE)")
        row_count = con.execute("SELECT COUNT(*) FROM dim_symbols").fetchone()[0]
        con.close()
    print(f"write_dim done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Disk: {m.value.disk_size_mb:.1f}MB | Rows: {row_count:,}")


def write_fact() -> None:
    fact_csv_abs = os.path.abspath(FACT_CSV)
    print(f"Running write_fact benchmark (COPY from {fact_csv_abs})...")
    with measure("duckdb_star_write_fact", data_path=DUCKDB_DIR) as m:
        con = duckdb.connect(DUCKDB_PATH)
        con.execute("DROP TABLE IF EXISTS fact_prices")
        con.execute(CREATE_FACT_SQL)
        con.execute(f"COPY fact_prices FROM '{fact_csv_abs}' (AUTO_DETECT TRUE)")
        con.execute("CREATE INDEX idx_fact_ticker ON fact_prices(ticker_id)")
        row_count = con.execute("SELECT COUNT(*) FROM fact_prices").fetchone()[0]
        con.close()
    print(f"write_fact done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Disk: {m.value.disk_size_mb:.1f}MB | Rows: {row_count:,}")


def query_join() -> None:
    print("Running query_join benchmark (JOIN fact_prices → dim_symbols, GROUP BY sector)...")
    with measure("duckdb_star_query_join", data_path=DUCKDB_DIR) as m:
        con = duckdb.connect(DUCKDB_PATH)
        result = con.execute(QUERY_JOIN_SQL).fetchdf()
        con.close()
    print(f"query_join done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Sectors: {len(result)}")


def query_oltp() -> None:
    print("Running query_oltp benchmark (single ticker_id=1, date range 2020–2023)...")
    with measure("duckdb_star_query_oltp", data_path=DUCKDB_DIR) as m:
        con = duckdb.connect(DUCKDB_PATH)
        result = con.execute(QUERY_OLTP_SQL, [1, "2020-01-01", "2023-12-31"]).fetchdf()
        con.close()
    print(f"query_oltp done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Rows: {len(result):,}")


def query_concurrent(n_threads: int = 10) -> None:
    print(f"Running query_concurrent benchmark ({n_threads} threads)...")
    errors: list[Exception] = []

    def _worker():
        try:
            con = duckdb.connect(DUCKDB_PATH)
            con.execute(QUERY_JOIN_SQL).fetchdf()
            con.close()
        except Exception as exc:
            errors.append(exc)

    with measure(f"duckdb_star_concurrent_{n_threads}", data_path=DUCKDB_DIR) as m:
        threads = [threading.Thread(target=_worker) for _ in range(n_threads)]
        wall_start = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        wall_sec = time.perf_counter() - wall_start

    if errors:
        print(f"  {len(errors)} thread(s) raised errors: {errors[0]}")
    print(f"query_concurrent({n_threads}) done: wall={wall_sec:.2f}s | "
          f"measured={m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")


if __name__ == "__main__":
    write_dim()
    write_fact()
    query_join()
    query_oltp()
    for n in [5, 10, 20]:
        query_concurrent(n)
    print("All benchmarks complete.")
