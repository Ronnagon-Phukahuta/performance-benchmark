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
    with measure("neo4j_polars_write", data_path="") as m:
        driver = _driver()
        with driver.session() as session:
            session.execute_write(_create_indexes)
            for start in range(0, len(rows), BATCH_SIZE):
                batch = rows[start : start + BATCH_SIZE]
                session.execute_write(_write_batch, batch)
        driver.close()
    print(f"write_graph done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Tickers: {len(rows):,}")

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
    with measure("neo4j_polars_write_prices", data_path="") as m:
        driver = _driver()
        with driver.session() as session:
            session.execute_write(_create_price_index)
            inserted = 0
            for start in range(0, total, BATCH_SIZE):
                batch = rows[start : start + BATCH_SIZE]
                session.execute_write(_write_price_batch, batch)
                inserted += len(batch)
                if inserted % 10_000 == 0:
                    print(f"  Inserted {inserted:,}/{total:,} prices...")
        driver.close()
    print(f"write_prices done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Prices: {total:,}")

def read_prices() -> pl.DataFrame:
    print("Running read_prices benchmark (MATCH all Price nodes)...")
    with measure("neo4j_polars_read", data_path="") as m:
        driver = _driver()
        with driver.session() as session:
            result = session.run("MATCH (p:Price) RETURN p.ticker_id AS ticker_id, p.date AS date, p.close AS close")
            records = [r.data() for r in result]
        driver.close()
        df = pl.from_dicts(records)
    print(f"read_prices done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(df):,}")
    return df

def query_prices() -> pl.DataFrame:
    print("Running query_prices benchmark (aggregate by ticker)...")
    with measure("neo4j_polars_query", data_path="") as m:
        driver = _driver()
        with driver.session() as session:
            result = session.run("""
                MATCH (t:Ticker)-[:HAS_PRICE]->(p:Price)
                WITH t.ticker_id AS ticker_id, AVG(p.close) AS avg_close, MAX(p.close) AS max_close, MIN(p.close) AS min_close
                RETURN ticker_id, avg_close, max_close, min_close
            """)
            records = [r.data() for r in result]
        driver.close()
        df = pl.from_dicts(records)
    print(f"query_prices done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Groups: {len(df):,}")
    return df

if __name__ == "__main__":
    write_graph()
    write_prices()
    read_prices()
    query_prices()
    print("All benchmarks complete. Results saved.")
