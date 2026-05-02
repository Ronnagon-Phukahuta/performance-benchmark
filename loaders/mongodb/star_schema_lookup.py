import os
import threading
import time

import polars as pl
from pymongo import MongoClient

from benchmark.metrics import measure

DIM_CSV  = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "dim_symbols.csv")
FACT_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices.csv")

MONGO_URI        = "mongodb://localhost:27017/"
DB_NAME          = "benchmark"
PRICES_COLL      = "star_prices"
SYMBOLS_COLL     = "symbols"

CHUNK_SIZE = 50_000

QUERY_JOIN_PIPELINE = [
    {
        "$group": {
            "_id": "$ticker_id",
            "avg_close": {"$avg": "$close"},
            "max_close": {"$max": "$close"},
            "min_close": {"$min": "$close"},
        }
    },
    {
        "$lookup": {
            "from": SYMBOLS_COLL,
            "localField": "_id",
            "foreignField": "ticker_id",
            "as": "dim",
        }
    },
    {"$unwind": "$dim"},
    {
        "$group": {
            "_id": "$dim.sector",
            "avg_close": {"$avg": "$avg_close"},
            "max_close": {"$max": "$max_close"},
            "min_close": {"$min": "$min_close"},
        }
    },
]

QUERY_OLTP_FILTER = {
    "ticker_id": 1,
    "date": {"$gte": "2020-01-01", "$lte": "2023-12-31"},
}


def _client():
    return MongoClient(MONGO_URI)


def write() -> None:
    print("Running write benchmark (mongodb_star_lookup — two collections)...")

    # --- symbols collection ---
    print(f"  Loading {DIM_CSV} → '{SYMBOLS_COLL}' collection...")
    dim = pl.read_csv(DIM_CSV, infer_schema_length=1000)
    symbols_records = dim.to_dicts()

    # --- prices collection (chunked) ---
    print(f"  Loading {FACT_CSV} → '{PRICES_COLL}' collection (chunks of {CHUNK_SIZE:,})...")
    fact = pl.read_csv(FACT_CSV, infer_schema_length=1000)
    total = len(fact)
    print(f"  Loaded {total:,} price rows")

    with measure("mongodb_star_lookup_write", data_path="") as m:
        client = _client()
        db = client[DB_NAME]

        # symbols
        db[SYMBOLS_COLL].drop()
        db[SYMBOLS_COLL].insert_many(symbols_records, ordered=False)
        db[SYMBOLS_COLL].create_index("ticker_id")
        print(f"  Symbols written: {len(symbols_records):,} docs, index created")

        # prices
        db[PRICES_COLL].drop()
        inserted = 0
        for start in range(0, total, CHUNK_SIZE):
            chunk = fact.slice(start, CHUNK_SIZE)
            records = chunk.select(
                ["ticker_id", "date", "open", "high", "low", "close", "volume"]
            ).to_dicts()
            db[PRICES_COLL].insert_many(records, ordered=False)
            inserted += len(records)
            print(f"  Inserted {inserted:,}/{total:,} price rows...")

        db[PRICES_COLL].create_index("ticker_id")
        print("  Index created on ticker_id in prices collection")
        client.close()

    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")


def query_join() -> list:
    print("Running query_join benchmark ($group → $lookup symbols → $group by sector)...")
    with measure("mongodb_star_lookup_query_join", data_path="") as m:
        client = _client()
        db = client[DB_NAME]
        result = list(db[PRICES_COLL].aggregate(QUERY_JOIN_PIPELINE, allowDiskUse=True))
        client.close()
    print(f"query_join done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Sectors: {len(result)}")
    return result


def query_oltp() -> list:
    print("Running query_oltp benchmark (ticker_id=1, date 2020–2023)...")
    with measure("mongodb_star_lookup_query_oltp", data_path="") as m:
        client = _client()
        db = client[DB_NAME]
        result = list(db[PRICES_COLL].find(QUERY_OLTP_FILTER, {"_id": 0}))
        client.close()
    print(f"query_oltp done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Rows: {len(result):,}")
    return result


def query_concurrent(n_threads: int = 10) -> None:
    print(f"Running query_concurrent benchmark ({n_threads} threads)...")
    errors: list[Exception] = []

    def _worker():
        try:
            query_join()
        except Exception as exc:
            errors.append(exc)

    with measure(f"mongodb_star_lookup_concurrent_{n_threads}", data_path="") as m:
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
    write()
    query_join()
    query_oltp()
    for n in [5, 10, 20]:
        query_concurrent(n)
    print("All benchmarks complete.")
