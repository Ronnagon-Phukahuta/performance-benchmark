import json
import os
import polars as pl
import redis
from benchmark.metrics import measure

DIM_CSV  = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "dim_symbols.csv")
FACT_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices.csv")
CHUNK_SIZE = 10_000

def _get_client() -> redis.Redis:
    return redis.Redis(host="localhost", port=6379, decode_responses=True)

def write() -> None:
    print("Running write benchmark (SET price:{ticker_id}:{date} → JSON, pipeline chunks, Polars)...")
    df = pl.read_csv(FACT_CSV)
    row_count = len(df)
    with measure("redis_polars_write", data_path="") as m:
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
    print(f"write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {row_count:,}")

def query_join() -> pl.DataFrame:
    print("Running query_join benchmark (GET all keys per ticker, GROUP BY sector, Polars)...")
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
    with measure("redis_polars_query_join_sample", data_path="") as m:
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
        df = pl.from_dicts([
            {"sector": sec, **vals} for sec, vals in result.items()
        ])
    est_time = m.value.duration_sec * (full_total / total)
    print(f"query_join (sample of 10) done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Sectors: {len(df):,} | Est. full time: {est_time:.1f}s")
    return df

if __name__ == "__main__":
    write()
    query_join()
    print("All benchmarks complete. Results saved.")
