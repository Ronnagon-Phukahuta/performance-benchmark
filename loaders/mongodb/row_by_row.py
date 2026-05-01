import os

import pandas as pd
import polars as pl
from pymongo import MongoClient
from tqdm import tqdm

from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "benchmark"
COLLECTION = "stocks_row_by_row"


def _db():
    client = MongoClient(MONGO_URI)
    return client, client[DB_NAME]


def write():
    print("Loading CSV...")
    df = pl.read_csv(RAW_CSV)
    df.columns = [c.lower() for c in df.columns]
    df = df[["date", "ticker", "type", "open", "high", "low", "close", "volume"]]
    records = df.to_dicts()
    total = len(records)
    print(f"Loaded {total:,} rows from CSV")
    print("Running write benchmark (mongodb row_by_row)...")
    with measure("mongodb_row_by_row_write", data_path="") as m:
        client, db = _db()
        db[COLLECTION].drop()
        for record in tqdm(
            records,
            desc="Inserting documents",
            unit="doc",
            bar_format="Inserting documents: {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
        ):
            db[COLLECTION].insert_one(record)
        client.close()
    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")


def read():
    print("Running read benchmark...")
    with measure("mongodb_row_by_row_read", data_path="") as m:
        client, db = _db()
        result = list(db[COLLECTION].find({}, {"_id": 0}))
        df = pd.DataFrame(result)
        client.close()
    print(f"Read done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return df


def query():
    print("Running query benchmark...")
    pipeline = [
        {"$group": {
            "_id": "$ticker",
            "avg_close": {"$avg": "$close"},
            "max_close": {"$max": "$close"},
            "min_close": {"$min": "$close"},
        }}
    ]
    with measure("mongodb_row_by_row_query", data_path="") as m:
        client, db = _db()
        result = list(db[COLLECTION].aggregate(pipeline))
        client.close()
    print(f"Query done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")
    return result


if __name__ == "__main__":
    write()
    read()
    query()
    print("All benchmarks complete. Results saved.")
