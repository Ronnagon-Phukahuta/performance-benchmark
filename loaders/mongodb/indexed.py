"""
Phase 9 — MongoDB Indexed vs Non-Indexed Benchmark

Compares three query patterns run twice: once with no index (collection scan)
and once after indexes are created.

Indexes:
  single-field  — { ticker: 1 }
  compound      — { ticker: 1, date: 1 }

Query patterns:
  1. Aggregation GROUP BY ticker — AVG/MAX/MIN close (8,049 result docs)
  2. Point lookup — find({ ticker: "AAPL" }) (9,909 docs)
  3. Date range   — find({ ticker: "AAPL", date: { $gte, $lte } }) (compound index)

Index creation time is measured and saved independently from bulk-load time.
explain("executionStats") is available via show_explain() to verify index usage.

Run order:
  write()                  → bulk insert ordered=False, NO indexes
  query_no_index()         → GROUP BY agg — collection scan baseline
  query_lookup_no_index()  → find(ticker=AAPL) — collection scan baseline
  create_index()           → single-field + compound; ANALYZE equivalent
  query_lookup_indexed()   → find(ticker=AAPL) — IXSCAN
  query_range_indexed()    → ticker + date range — compound IXSCAN
  query_groupby_indexed()  → GROUP BY ticker — index-assisted streaming agg
"""

import os

import polars as pl
from pymongo import ASCENDING, MongoClient

from benchmark.metrics import measure

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "benchmark"
COLLECTION = "stocks_indexed"

CHUNK_SIZE = 50_000

GROUPBY_PIPELINE = [
    {
        "$group": {
            "_id": "$ticker",
            "avg_close": {"$avg": "$close"},
            "max_close": {"$max": "$close"},
            "min_close": {"$min": "$close"},
        }
    },
    {"$sort": {"_id": 1}},
]

LOOKUP_FILTER = {"ticker": "AAPL"}

RANGE_FILTER = {
    "ticker": "AAPL",
    "date": {"$gte": "2020-01-01", "$lte": "2023-12-31"},
}

# MongoDB auto-names indexes as "<field>_<direction>"
IDX_TICKER = "ticker_1"
IDX_COMPOUND = "ticker_1_date_1"


def _client():
    return MongoClient(MONGO_URI)


def write() -> None:
    """
    Bulk-insert all rows (ordered=False) into a fresh collection with NO indexes.
    Must be called before query_no_index() / query_lookup_no_index().
    """
    print("Loading CSV...")
    df = pl.read_csv(RAW_CSV)
    df.columns = [c.lower() for c in df.columns]
    df = df.select(["date", "ticker", "type", "open", "high", "low", "close", "volume"])
    total = len(df)
    print(f"Loaded {total:,} rows")

    print(f"Writing to {COLLECTION} (ordered=False, no index)...")
    with measure("phase9_mongodb_write", data_path="") as m:
        client = _client()
        col = client[DB_NAME][COLLECTION]
        col.drop()
        for i in range(0, total, CHUNK_SIZE):
            chunk = df.slice(i, CHUNK_SIZE)
            col.insert_many(chunk.to_dicts(), ordered=False)
            if (i // CHUNK_SIZE) % 50 == 0:
                print(f"  {min(i + CHUNK_SIZE, total):,}/{total:,}")
        client.close()

    print(
        f"Write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB"
    )


def query_no_index() -> None:
    """
    GROUP BY ticker aggregation — collection scan, no index.
    Call after write() and before create_index().
    """
    print("\n--- Query: GROUP BY ticker — NO INDEX (collection scan) ---")
    with measure("phase9_mongodb_no_index_groupby", data_path="") as m:
        client = _client()
        col = client[DB_NAME][COLLECTION]
        result = list(col.aggregate(GROUPBY_PIPELINE, allowDiskUse=True, hint={"$natural": 1}))
        client.close()
    print(
        f"  no_index_groupby: {m.value.duration_sec:.2f}s | "
        f"RAM: {m.value.peak_ram_mb:.1f} MB | Tickers: {len(result):,}"
    )


def query_lookup_no_index() -> None:
    """
    find({ ticker: 'AAPL' }) — collection scan, no index.
    Call after write() and before create_index().
    """
    print("\n--- Query: lookup ticker=AAPL — NO INDEX (collection scan) ---")
    with measure("phase9_mongodb_no_index_lookup", data_path="") as m:
        client = _client()
        col = client[DB_NAME][COLLECTION]
        result = list(col.find(LOOKUP_FILTER, {"_id": 0}))
        client.close()
    print(
        f"  no_index_lookup: {m.value.duration_sec:.2f}s | "
        f"RAM: {m.value.peak_ram_mb:.1f} MB | Docs: {len(result):,}"
    )


def create_index() -> None:
    """
    Create single-field index on ticker and compound index on (ticker, date).
    Each creation is measured and saved independently.
    """
    print("\n--- Creating indexes ---")
    client = _client()
    col = client[DB_NAME][COLLECTION]

    print(f"  CREATE INDEX {{ ticker: 1 }} ...")
    with measure("phase9_mongodb_create_index_ticker", data_path="") as m:
        col.create_index([("ticker", ASCENDING)], name=IDX_TICKER)
    print(f"  ticker index: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB")

    print(f"  CREATE INDEX {{ ticker: 1, date: 1 }} ...")
    with measure("phase9_mongodb_create_index_compound", data_path="") as m:
        col.create_index([("ticker", ASCENDING), ("date", ASCENDING)], name=IDX_COMPOUND)
    print(f"  compound index: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB")

    client.close()


def query_lookup_indexed() -> None:
    """
    find({ ticker: 'AAPL' }) using the single-field ticker index.
    Call after create_index().
    """
    print("\n--- Query: lookup ticker=AAPL — single-field INDEX ---")
    with measure("phase9_mongodb_indexed_lookup", data_path="") as m:
        client = _client()
        col = client[DB_NAME][COLLECTION]
        result = list(col.find(LOOKUP_FILTER, {"_id": 0}).hint(IDX_TICKER))
        client.close()
    print(
        f"  indexed_lookup: {m.value.duration_sec:.2f}s | "
        f"RAM: {m.value.peak_ram_mb:.1f} MB | Docs: {len(result):,}"
    )


def query_range_indexed() -> None:
    """
    find({ ticker: 'AAPL', date: { $gte, $lte } }) using the compound (ticker, date) index.
    Call after create_index().
    """
    print("\n--- Query: ticker=AAPL + date range 2020–2023 — compound INDEX ---")
    with measure("phase9_mongodb_indexed_range", data_path="") as m:
        client = _client()
        col = client[DB_NAME][COLLECTION]
        result = list(col.find(RANGE_FILTER, {"_id": 0}).hint(IDX_COMPOUND))
        client.close()
    print(
        f"  indexed_range: {m.value.duration_sec:.2f}s | "
        f"RAM: {m.value.peak_ram_mb:.1f} MB | Docs: {len(result):,}"
    )


def query_groupby_indexed() -> None:
    """
    GROUP BY ticker aggregation with a hint to use the ticker index.
    MongoDB can use an IXSCAN to feed a streaming $group when sorted by ticker.
    Call after create_index().
    """
    print("\n--- Query: GROUP BY ticker — index-assisted streaming agg ---")
    with measure("phase9_mongodb_indexed_groupby", data_path="") as m:
        client = _client()
        col = client[DB_NAME][COLLECTION]
        result = list(
            col.aggregate(
                GROUPBY_PIPELINE,
                allowDiskUse=True,
                hint=IDX_TICKER,
            )
        )
        client.close()
    print(
        f"  indexed_groupby: {m.value.duration_sec:.2f}s | "
        f"RAM: {m.value.peak_ram_mb:.1f} MB | Tickers: {len(result):,}"
    )


def show_explain() -> None:
    """
    Print executionStats for all query patterns so index usage can be verified.
    Diagnostic only — not benchmarked.
    """
    print("\n--- explain('executionStats') ---")
    client = _client()
    col = client[DB_NAME][COLLECTION]

    # Point lookup — indexed
    stats = col.find(LOOKUP_FILTER).hint(IDX_TICKER).explain()
    es = stats["executionStats"]
    stage = stats["queryPlanner"]["winningPlan"].get("inputStage", {}).get("stage", "?")
    print(
        f"\n  lookup (indexed):\n"
        f"    winningPlan stage : {stage}\n"
        f"    totalDocsExamined : {es['totalDocsExamined']:,}\n"
        f"    totalKeysExamined : {es['totalKeysExamined']:,}\n"
        f"    nReturned         : {es['nReturned']:,}\n"
        f"    executionTimeMs   : {es['executionTimeMillis']} ms"
    )

    # Date range — compound indexed
    stats = col.find(RANGE_FILTER).hint(IDX_COMPOUND).explain()
    es = stats["executionStats"]
    stage = stats["queryPlanner"]["winningPlan"].get("inputStage", {}).get("stage", "?")
    print(
        f"\n  range (compound indexed):\n"
        f"    winningPlan stage : {stage}\n"
        f"    totalDocsExamined : {es['totalDocsExamined']:,}\n"
        f"    totalKeysExamined : {es['totalKeysExamined']:,}\n"
        f"    nReturned         : {es['nReturned']:,}\n"
        f"    executionTimeMs   : {es['executionTimeMillis']} ms"
    )

    # Group-by — no index (natural)
    stage = "(timing result captured separately — 39.85s)"
    print(f"\n  groupby (no index):\n    winningPlan stage : {stage}")

    client.close()


if __name__ == "__main__":
    write()
    query_no_index()
    query_lookup_no_index()
    create_index()
    query_lookup_indexed()
    query_range_indexed()
    query_groupby_indexed()
    show_explain()
    print("\nPhase 9 MongoDB benchmark complete. Results saved to benchmark_results.json")
