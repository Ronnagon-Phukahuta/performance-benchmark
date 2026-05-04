import os
import threading
import time

import polars as pl
from neo4j import GraphDatabase

from benchmark.metrics import measure

URI         = "bolt://localhost:7687"
AUTH        = ("neo4j", "Benchmark123!")
DIM_CSV     = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "dim_symbols.csv")
FACT_CSV    = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices.csv")
FACT_SAMPLE = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices_sample.csv")

BATCH_SIZE = 500


def _driver():
    return GraphDatabase.driver(URI, auth=AUTH)


def write_graph() -> None:
    print("Running write_graph benchmark (Ticker / Sector / Industry / Exchange nodes + relationships)...")
    df = pl.read_csv(DIM_CSV)
    rows = df.to_dicts()

    def _create_indexes(tx):
        tx.run("CREATE INDEX ticker_id_idx IF NOT EXISTS FOR (t:Ticker) ON (t.ticker_id)")

    def _write_batch(tx, batch):
        tx.run(
            """
            UNWIND $rows AS row
            MERGE (t:Ticker {ticker_id: row.ticker_id})
              ON CREATE SET t.ticker = row.ticker, t.type = row.type
            MERGE (s:Sector   {name: row.sector})
            MERGE (i:Industry {name: row.industry})
            MERGE (e:Exchange  {name: row.exchange})
            MERGE (t)-[:IN_SECTOR]->(s)
            MERGE (t)-[:IN_INDUSTRY]->(i)
            MERGE (t)-[:LISTED_ON]->(e)
            """,
            rows=batch,
        )

    with measure("neo4j_star_write_graph") as m:
        driver = _driver()
        with driver.session() as session:
            session.execute_write(_create_indexes)
            for start in range(0, len(rows), BATCH_SIZE):
                batch = rows[start : start + BATCH_SIZE]
                session.execute_write(_write_batch, batch)
        driver.close()

    print(
        f"write_graph done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
        f"Tickers: {len(rows):,}"
    )


def write_prices() -> None:

    print("Running write_prices benchmark (Price nodes + HAS_PRICE relationships, 100K sample)...")
    print("Note: full 28M nodes DNF — RAM spiked to 31GB on 32GB system")
    df = pl.read_csv(FACT_SAMPLE)
    rows = df.to_dicts()
    total = len(rows)

    def _create_price_index(tx):
        tx.run("CREATE INDEX price_date_idx IF NOT EXISTS FOR (p:Price) ON (p.date)")

    def _write_price_batch(tx, batch):
        tx.run(
            """
            UNWIND $rows AS row
            MERGE (t:Ticker {ticker_id: row.ticker_id})
            CREATE (p:Price {
                date:   row.date,
                open:   row.open,
                high:   row.high,
                low:    row.low,
                close:  row.close,
                volume: row.volume
            })
            CREATE (t)-[:HAS_PRICE]->(p)
            """,
            rows=batch,
        )

    with measure("neo4j_star_write_prices") as m:
        driver = _driver()
        with driver.session() as session:
            session.execute_write(_create_price_index)
            inserted = 0
            for start in range(0, total, BATCH_SIZE):
                batch = rows[start : start + BATCH_SIZE]
                session.execute_write(_write_price_batch, batch)
                inserted += len(batch)
                if inserted % 10_000 == 0:
                    print(f"  Inserted {inserted:,}/{total:,} price rows...")
        driver.close()

    print(
        f"write_prices done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
        f"Rows: {total:,}"
    )
    print("Extrapolation: 100K rows took 13.68s → 28M extrapolated = ~3,830s (~64 min)")
    print("RAM at 100K = 202MB → 28M extrapolated = ~56GB (exceeds 32GB system RAM)")


def query_join() -> None:
    print("Running query_join benchmark (MATCH Ticker→Sector + Ticker→Price, GROUP BY sector)...")

    def _run(tx):
        result = tx.run(
            """
            MATCH (t:Ticker)-[:IN_SECTOR]->(s:Sector)
            MATCH (t)-[:HAS_PRICE]->(p:Price)
            RETURN s.name AS sector,
                   avg(p.close) AS avg_close,
                   max(p.close) AS max_close,
                   min(p.close) AS min_close
            ORDER BY sector
            """
        )
        return result.data()

    with measure("neo4j_star_query_join") as m:
        driver = _driver()
        with driver.session() as session:
            rows = session.execute_read(_run)
        driver.close()

    print(
        f"query_join done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
        f"Sectors: {len(rows)}"
    )


def query_oltp() -> None:
    print("Running query_oltp benchmark (ticker_id=1, date range 2020-01-01..2023-12-31)...")

    def _run(tx):
        result = tx.run(
            """
            MATCH (t:Ticker {ticker_id: 1})-[:HAS_PRICE]->(p:Price)
            WHERE p.date >= '2020-01-01' AND p.date <= '2023-12-31'
            RETURN p
            """
        )
        return result.data()

    with measure("neo4j_star_query_oltp") as m:
        driver = _driver()
        with driver.session() as session:
            rows = session.execute_read(_run)
        driver.close()

    print(
        f"query_oltp done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
        f"Rows: {len(rows):,}"
    )


def query_traversal() -> None:
    print(
        "Running query_traversal benchmark (find all tickers in same sector as ticker_id=1)...\n"
        "Note: Graph DB native strength — 1-hop traversal.\n"
        "SQL equivalent: SELECT t2.ticker FROM dim_symbols t1 "
        "JOIN dim_symbols t2 ON t1.sector = t2.sector WHERE t1.ticker_id = 1"
    )

    def _run(tx):
        result = tx.run(
            """
            MATCH (t1:Ticker {ticker_id: 1})-[:IN_SECTOR]->(s:Sector)<-[:IN_SECTOR]-(t2:Ticker)
            WHERE t1 <> t2
            RETURN t2.ticker AS related_ticker, s.name AS sector
            ORDER BY t2.ticker
            """
        )
        return result.data()

    with measure("neo4j_star_query_traversal") as m:
        driver = _driver()
        with driver.session() as session:
            rows = session.execute_read(_run)
        driver.close()

    print(
        f"query_traversal done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
        f"Related tickers: {len(rows):,}"
    )


def query_concurrent(n_threads: int = 10) -> None:
    print(f"Running query_concurrent benchmark ({n_threads} threads, each runs query_join)...")
    errors: list[Exception] = []

    def _worker():
        try:
            query_join()
        except Exception as exc:
            errors.append(exc)

    with measure(f"neo4j_star_concurrent_{n_threads}") as m:
        threads = [threading.Thread(target=_worker) for _ in range(n_threads)]
        wall_start = time.perf_counter()
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        wall_sec = time.perf_counter() - wall_start

    if errors:
        print(f"  {len(errors)} thread(s) raised errors: {errors[0]}")
    print(
        f"query_concurrent({n_threads}) done: wall={wall_sec:.2f}s | "
        f"measured={m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB"
    )


if __name__ == "__main__":
    write_graph()
    write_prices()
    query_join()
    query_oltp()
    query_traversal()
    for n in [5, 10, 20]:
        query_concurrent(n)
