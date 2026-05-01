import os

import pandas as pd
import polars as pl
from pymongo import MongoClient

from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "benchmark"
COLLECTION = "stocks_ordered"


CHUNK_SIZE = 50_000


def _db():
    client = MongoClient(MONGO_URI)
    return client, client[DB_NAME]


def write():
    print("Loading CSV...")
    df = pl.read_csv(RAW_CSV)
    df.columns = [c.lower() for c in df.columns]
    df = df[["date", "ticker", "type", "open", "high", "low", "close", "volume"]]
    total = len(df)
    print(f"Loaded {total:,} rows from CSV")
    print("Running write benchmark (mongodb bulk_insert ordered=True)...")
    with measure("mongodb_bulk_ordered_write", data_path="") as m:
        client, db = _db()
        db[COLLECTION].drop()
        for i in range(0, total, CHUNK_SIZE):
            chunk = df.slice(i, CHUNK_SIZE)
            records = chunk.to_dicts()
            db[COLLECTION].insert_many(records, ordered=True)
            print(f"  Inserted {min(i + CHUNK_SIZE, total):,}/{total:,} rows...")
        db[COLLECTION].create_index("ticker")
        print("  Index created on ticker field")
        client.close()
    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")


def read():
    """MongoDB full scan is not applicable — document stores are optimized for filtered queries.
    Instead benchmark a realistic read: fetch all documents for a single ticker (AAPL)."""
    print("Note: full scan omitted — would OOM at 28M docs. Testing single-ticker query instead.")
    print("Running read benchmark (single ticker: AAPL)...")
    with measure("mongodb_bulk_ordered_read", data_path="") as m:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        result = list(db[COLLECTION].find({"ticker": "AAPL"}, {"_id": 0}))
        df = pd.DataFrame(result)
        client.close()
    print(f"Read done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(df):,}")
    return df


def query():
    print("Running query benchmark (aggregation pipeline)...")
    with measure("mongodb_bulk_ordered_query", data_path="") as m:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        pipeline = [
            {"$group": {
                "_id": "$ticker",
                "avg_close": {"$avg": "$close"},
                "max_close": {"$max": "$close"},
                "min_close": {"$min": "$close"}
            }}
        ]
        result = list(db[COLLECTION].aggregate(pipeline, allowDiskUse=True))
        client.close()
    print(f"Query done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Tickers: {len(result):,}")
    return result


if __name__ == "__main__":
    write()
    read()
    query()
    print("All benchmarks complete. Results saved.")
