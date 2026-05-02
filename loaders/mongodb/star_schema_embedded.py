import os
import threading
import time

import polars as pl
from pymongo import MongoClient

from benchmark.metrics import measure

DIM_CSV  = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "dim_symbols.csv")
FACT_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices.csv")

MONGO_URI  = "mongodb://localhost:27017/"
DB_NAME    = "benchmark"
COLLECTION = "star_embedded"

CHUNK_SIZE = 50_000

QUERY_JOIN_PIPELINE = [
    {
        "$group": {
            "_id": "$sector",
            "avg_close": {"$avg": "$close"},
            "max_close": {"$max": "$close"},
            "min_close": {"$min": "$close"},
        }
    }
]


def _client():
    return MongoClient(MONGO_URI)


def _build_dim_lookup() -> dict[int, dict]:
    """Return {ticker_id: {sector, industry, exchange}} from dim_symbols.csv."""
    dim = pl.read_csv(DIM_CSV, infer_schema_length=1000)
    lookup: dict[int, dict] = {}
    for row in dim.iter_rows(named=True):
        lookup[row["ticker_id"]] = {
            "sector":   row["sector"],
            "industry": row["industry"],
            "exchange": row["exchange"],
        }
    return lookup


def write() -> None:
    print("Loading dim_symbols for lookup dict...")
    dim_lookup = _build_dim_lookup()
    print(f"  Loaded {len(dim_lookup):,} tickers into lookup dict")

    print(f"Loading {FACT_CSV} in chunks of {CHUNK_SIZE:,}...")
    fact = pl.read_csv(FACT_CSV, infer_schema_length=1000)
    total = len(fact)
    print(f"  Loaded {total:,} rows")

    print("Running write benchmark (mongodb_star_embedded, ordered=False)...")
    with measure("mongodb_star_embedded_write", data_path="") as m:
        client = _client()
        db = client[DB_NAME]
        db[COLLECTION].drop()

        inserted = 0
        for start in range(0, total, CHUNK_SIZE):
            chunk = fact.slice(start, CHUNK_SIZE)
            records = []
            for row in chunk.iter_rows(named=True):
                tid = row["ticker_id"]
                dim_info = dim_lookup.get(tid, {"sector": None, "industry": None, "exchange": None})
                records.append({
                    "ticker_id": tid,
                    "date":      row["date"],
                    "open":      row["open"],
                    "high":      row["high"],
                    "low":       row["low"],
                    "close":     row["close"],
                    "volume":    row["volume"],
                    "sector":    dim_info["sector"],
                    "industry":  dim_info["industry"],
                    "exchange":  dim_info["exchange"],
                })
            db[COLLECTION].insert_many(records, ordered=False)
            inserted += len(records)
            print(f"  Inserted {inserted:,}/{total:,} rows...")

        db[COLLECTION].create_index("ticker_id")
        print("  Index created on ticker_id field")
        client.close()

    print(f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB")


def query_join() -> list:
    print("Running query_join benchmark (GROUP BY sector, no $lookup)...")
    with measure("mongodb_star_embedded_query_join", data_path="") as m:
        client = _client()
        db = client[DB_NAME]
        result = list(db[COLLECTION].aggregate(QUERY_JOIN_PIPELINE, allowDiskUse=True))
        client.close()
    print(f"query_join done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
          f"Sectors: {len(result)}")
    return result


def query_oltp() -> list:
    print("Running query_oltp benchmark (ticker_id=1, date 2020–2023)...")
    with measure("mongodb_star_embedded_query_oltp", data_path="") as m:
        client = _client()
        db = client[DB_NAME]
        result = list(db[COLLECTION].find(
            {"ticker_id": 1, "date": {"$gte": "2020-01-01", "$lte": "2023-12-31"}},
            {"_id": 0},
        ))
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

    with measure(f"mongodb_star_embedded_concurrent_{n_threads}", data_path="") as m:
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
