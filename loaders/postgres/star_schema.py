import io
import os
import threading
import time

import psycopg2

from benchmark.metrics import measure

DIM_CSV  = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "dim_symbols.csv")
FACT_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices.csv")

DSN = dict(host="localhost", port=5432, user="benchmark", password="benchmark", dbname="benchmark_db")
  
CREATE_DIM_SQL = """
    CREATE TABLE dim_symbols (
        ticker_id INT PRIMARY KEY,
        ticker    VARCHAR(20),
        type      VARCHAR(10),
        sector    VARCHAR(50),
        industry  VARCHAR(50),
        exchange  VARCHAR(20)
    )
"""

CREATE_FACT_SQL = """
    CREATE TABLE fact_prices (
        ticker_id INT,
        date      DATE,
        open      FLOAT,
        high      FLOAT,
        low       FLOAT,
        close     FLOAT,
        volume    FLOAT
    )
"""

QUERY_JOIN_SQL = """
    SELECT s.sector,
           AVG(f.close) AS avg_close,
           MAX(f.close) AS max_close,
           MIN(f.close) AS min_close
    FROM fact_prices f
    JOIN dim_symbols s ON f.ticker_id = s.ticker_id
    GROUP BY s.sector
    ORDER BY s.sector
"""

QUERY_OLTP_SQL = """
    SELECT * FROM fact_prices
    WHERE ticker_id = %s
      AND date BETWEEN %s AND %s
"""

QUERY_OLTP_NO_INDEX_SQL = """
    SELECT * FROM fact_prices_no_index
    WHERE ticker_id = %s
      AND date BETWEEN %s AND %s
"""


def _connect():
    return psycopg2.connect(**DSN)


def _copy_csv(cur, table: str, csv_path: str) -> None:
    """Load a CSV file into a table using COPY via copy_expert."""
    with open(csv_path, "r", encoding="utf-8") as f:
        cur.copy_expert(
            f"COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)",
            f,
        )


def write_dim() -> None:
    print(f"Running write_dim benchmark (COPY {DIM_CSV} → dim_symbols)...")
    with measure("postgres_star_write_dim", data_path="") as m:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS dim_symbols CASCADE")
        cur.execute(CREATE_DIM_SQL)
        _copy_csv(cur, "dim_symbols", DIM_CSV)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM dim_symbols")
        row_count = cur.fetchone()[0]
        cur.close()
        conn.close()
    print(f"write_dim done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Rows: {row_count:,}")


def write_fact() -> None:
    print(f"Running write_fact benchmark (COPY {FACT_CSV} → fact_prices)...")
    with measure("postgres_star_write_fact", data_path="") as m:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS fact_prices")
        cur.execute(CREATE_FACT_SQL)
        _copy_csv(cur, "fact_prices", FACT_CSV)
        conn.commit()
        cur.execute("CREATE INDEX idx_fact_ticker ON fact_prices(ticker_id)")
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM fact_prices")
        row_count = cur.fetchone()[0]
        cur.close()
        conn.close()
    print(f"write_fact done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Rows: {row_count:,}")


def query_join() -> list:
    print("Running query_join benchmark (JOIN fact_prices → dim_symbols, GROUP BY sector)...")
    with measure("postgres_star_query_join", data_path="") as m:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(QUERY_JOIN_SQL)
        result = cur.fetchall()
        cur.close()
        conn.close()
    print(f"query_join done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Sectors: {len(result)}")
    return result


def query_oltp() -> list:
    print("Running query_oltp benchmark (ticker_id=1, date 2020–2023)...")
    with measure("postgres_star_query_oltp", data_path="") as m:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(QUERY_OLTP_SQL, (1, "2020-01-01", "2023-12-31"))
        result = cur.fetchall()
        cur.close()
        conn.close()
    print(f"query_oltp done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Rows: {len(result):,}")
    return result


def write_fact_no_index() -> None:
    print(f"Running write_fact_no_index benchmark (COPY {FACT_CSV} → fact_prices_no_index, no index)...")
    with measure("postgres_star_write_fact_no_index", data_path="") as m:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("DROP TABLE IF EXISTS fact_prices_no_index")
        cur.execute("""
            CREATE TABLE fact_prices_no_index (
                ticker_id INT,
                date      DATE,
                open      FLOAT,
                high      FLOAT,
                low       FLOAT,
                close     FLOAT,
                volume    FLOAT
            )
        """)
        _copy_csv(cur, "fact_prices_no_index", FACT_CSV)
        conn.commit()
        cur.execute("SELECT COUNT(*) FROM fact_prices_no_index")
        row_count = cur.fetchone()[0]
        cur.close()
        conn.close()
    print(f"write_fact_no_index done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Rows: {row_count:,}")


def query_oltp_no_index() -> list:
    print("Running query_oltp_no_index benchmark (fact_prices_no_index, ticker_id=1, date range 2020–2023)...")
    with measure("postgres_star_query_oltp_no_index", data_path="") as m:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(QUERY_OLTP_NO_INDEX_SQL, (1, "2020-01-01", "2023-12-31"))
        result = cur.fetchall()
        cur.close()
        conn.close()
    print(f"query_oltp_no_index done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Rows: {len(result):,}")
    return result


def query_concurrent(n_threads: int = 10) -> None:
    print(f"Running query_concurrent benchmark ({n_threads} threads)...")
    errors: list[Exception] = []

    def _worker():
        try:
            conn = _connect()
            cur = conn.cursor()
            cur.execute(QUERY_JOIN_SQL)
            cur.fetchall()
            cur.close()
            conn.close()
        except Exception as exc:
            errors.append(exc)

    with measure(f"postgres_star_concurrent_{n_threads}", data_path="") as m:
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
    write_fact_no_index()
    query_join()
    query_oltp()
    query_oltp_no_index()
    for n in [5, 10, 20]:
        query_concurrent(n)
    print("All benchmarks complete.")
