import json
import os
import random
import threading
import time

import polars as pl
import redis

from benchmark.metrics import measure

DIM_CSV  = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "dim_symbols.csv")
FACT_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices.csv")

CHUNK_SIZE = 10_000


def _get_client() -> redis.Redis:
    return redis.Redis(host="localhost", port=6379, decode_responses=True)


def write() -> None:
    print("Running write benchmark (SET price:{ticker_id}:{date} → JSON, pipeline chunks)...")
    df = pl.read_csv(FACT_CSV)
    row_count = len(df)
    with measure("redis_star_write") as m:
        client = _get_client()
        i = 0
        total = row_count
        for chunk_start in range(0, row_count, CHUNK_SIZE):
            chunk = df.slice(chunk_start, CHUNK_SIZE)
            pipe = client.pipeline(transaction=False)
            for row in chunk.iter_rows(named=True):
                key = f"price:{row['ticker_id']}:{row['date']}"
                value = json.dumps({
                    "open":   row["open"],
                    "high":   row["high"],
                    "low":    row["low"],
                    "close":  row["close"],
                    "volume": row["volume"],
                })
                pipe.set(key, value)
                i += 1
                if i % 1_000_000 == 0:
                    print(f"  Inserted {i:,}/{total:,} rows...")
            pipe.execute()
    print(
        f"write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
        f"Rows: {row_count:,}"
    )


def query_join() -> None:
    print("Running query_join benchmark (GET all keys per ticker, GROUP BY sector)...")
    print("Note: KEYS command blocks Redis server — not suitable for production.\n"
          "Running on 10 tickers sample only, extrapolating to full dataset.")
    dim_df = pl.read_csv(DIM_CSV)
    ticker_sector = {
        str(row["ticker_id"]): row["sector"]
        for row in dim_df.iter_rows(named=True)
    }
    sample_tickers = list(ticker_sector.items())[:10]
    total = 10
    full_total = len(ticker_sector)
    with measure("redis_star_query_join_sample") as m:
        client = _get_client()
        sector_stats: dict[str, dict] = {}
        processed = 0
        for ticker_id, sector in sample_tickers:
            processed += 1
            print(f"  Processing ticker {processed:,}/{total:,}...")
            keys = client.keys(f"price:{ticker_id}:*")
            if not keys:
                continue
            pipe = client.pipeline(transaction=False)
            for key in keys:
                pipe.get(key)
            results = pipe.execute()
            closes = [json.loads(r)["close"] for r in results if r is not None]
            closes = [c for c in closes if c is not None]
            if not closes:
                continue
            if sector not in sector_stats:
                sector_stats[sector] = {"sum": 0.0, "max": float("-inf"), "min": float("inf"), "count": 0}
            s = sector_stats[sector]
            s["sum"]   += sum(closes)
            s["count"] += len(closes)
            s["max"]    = max(s["max"], max(closes))
            s["min"]    = min(s["min"], min(closes))
        result = {
            sec: {
                "avg_close": stats["sum"] / stats["count"],
                "max_close": stats["max"],
                "min_close": stats["min"],
            }
            for sec, stats in sector_stats.items()
        }
    est_time = m.value.duration_sec * (full_total / total)
    print(
        f"query_join (sample of 10) done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
        f"Sectors: {len(result)}\n"
        f"Extrapolated time for {full_total:,} tickers: {est_time:.1f}s (~{est_time/60:.1f} min)"
    )


def query_oltp() -> None:
    print("Running query_oltp benchmark (GET price:1:*, filter 2020-01-01..2023-12-31)...")
    with measure("redis_star_query_oltp") as m:
        client = _get_client()
        print(f"  Scanning keys for ticker_id=1...")
        keys = client.keys("price:1:*")
        pipe = client.pipeline(transaction=False)
        for key in keys:
            pipe.get(key)
        raw_values = pipe.execute()
        result = []
        for key, raw in zip(keys, raw_values):
            if raw is None:
                continue
            date_str = key.split(":")[-1]
            if "2020-01-01" <= date_str <= "2023-12-31":
                result.append(json.loads(raw))
    print(
        f"query_oltp done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
        f"Rows: {len(result):,}"
    )


def query_oltp_no_index() -> None:
    # Redis has no index concept — all key lookups are O(1) by exact key or O(n) via KEYS scan.
    # This benchmark mirrors query_oltp() to demonstrate that there is no B-tree index advantage.
    print(
        "Running query_oltp_no_index benchmark "
        "(KEYS scan price:1:* — Redis key scan is always O(n), no B-tree index)..."
    )
    with measure("redis_star_query_oltp_no_index") as m:
        client = _get_client()
        print(f"  Scanning keys for ticker_id=1...")
        keys = client.keys("price:1:*")
        pipe = client.pipeline(transaction=False)
        for key in keys:
            pipe.get(key)
        raw_values = pipe.execute()
        result = []
        for key, raw in zip(keys, raw_values):
            if raw is None:
                continue
            date_str = key.split(":")[-1]
            if "2020-01-01" <= date_str <= "2023-12-31":
                result.append(json.loads(raw))
    print(
        f"query_oltp_no_index done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
        f"Rows: {len(result):,}"
    )


def query_sorted_set() -> None:
    print("Running query_sorted_set benchmark (ZADD ranking:avg_close, ZRANGE top 10)...")
    print("Note: Using 100K sample for avg_close computation")
    SAMPLE_FACT_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices_sample.csv")
    fact_df = pl.read_csv(SAMPLE_FACT_CSV)
    avg_df = fact_df.group_by("ticker_id").agg(pl.col("close").mean().alias("avg_close"))
    mapping = {str(row["ticker_id"]): float(row["avg_close"]) for row in avg_df.iter_rows(named=True)}
    with measure("redis_star_query_sorted_set") as m:
        client = _get_client()
        client.delete("ranking:avg_close")
        client.zadd("ranking:avg_close", mapping)
        top10 = client.zrevrange("ranking:avg_close", 0, 9, withscores=True)
    print(
        f"query_sorted_set done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
        f"Top 10: {top10}"
    )


def query_cache_simulation() -> None:
    print("Running query_cache_simulation benchmark (1000 random ticker lookups, hit/miss tracking)...")
    dim_df = pl.read_csv(DIM_CSV)
    ticker_ids = [str(row["ticker_id"]) for row in dim_df.iter_rows(named=True)]
    # Build list of (ticker_id, date) pairs that exist
    fact_df = pl.read_csv(FACT_CSV).select(["ticker_id", "date"]).head(5000)
    sample_pairs = [
        (str(row["ticker_id"]), str(row["date"]))
        for row in fact_df.iter_rows(named=True)
    ]
    rng = random.Random(42)
    lookups = rng.choices(sample_pairs, k=1000)

    hits = 0
    misses = 0
    hit_latencies: list[float] = []
    miss_latencies: list[float] = []

    with measure("redis_star_cache_simulation") as m:
        client = _get_client()
        # Pre-populate roughly half the keys to create realistic hit/miss ratio
        # (write() already populated everything; delete half to simulate cold cache)
        half = lookups[:500]
        for ticker_id, date in half:
            client.delete(f"price:{ticker_id}:{date}")

        for i, (ticker_id, date) in enumerate(lookups, 1):
            if i % 100 == 0:
                print(f"  Lookup {i}/1000 — hits: {hits}, misses: {misses}")
            key = f"price:{ticker_id}:{date}"
            t0 = time.perf_counter()
            cached = client.get(key)
            latency = time.perf_counter() - t0
            if cached is not None:
                hits += 1
                hit_latencies.append(latency)
            else:
                # Cache miss: simulate DB query latency then populate cache
                time.sleep(0.001)
                simulated_value = json.dumps({
                    "open": 0.0, "high": 0.0, "low": 0.0,
                    "close": 0.0, "volume": 0.0,
                })
                client.set(key, simulated_value)
                misses += 1
                miss_latencies.append(latency)

    total = hits + misses
    hit_rate = hits / total if total else 0.0
    avg_hit_ms  = (sum(hit_latencies)  / len(hit_latencies)  * 1000) if hit_latencies  else 0.0
    avg_miss_ms = (sum(miss_latencies) / len(miss_latencies) * 1000) if miss_latencies else 0.0
    print(
        f"query_cache_simulation done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | "
        f"Hits: {hits} | Misses: {misses} | Hit rate: {hit_rate:.1%} | "
        f"Avg hit latency: {avg_hit_ms:.3f}ms | Avg miss latency: {avg_miss_ms:.3f}ms"
    )


def query_concurrent(n_threads: int = 10) -> None:
    print(f"Running query_concurrent benchmark ({n_threads} threads)...")
    print("Concurrent single-key GET lookups (ticker_id=1)")
    errors: list[Exception] = []

    def _worker():
        try:
            query_oltp()
        except Exception as exc:
            errors.append(exc)

    with measure(f"redis_star_concurrent_{n_threads}") as m:
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
    # write()  # data already loaded
    query_join()
    query_oltp()
    query_oltp_no_index()
    query_sorted_set()
    query_cache_simulation()
    for n in [5, 10, 20]:
        query_concurrent(n)
