import hashlib
import os

import polars as pl

RAW_CSV = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "all_stocks.csv")
OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "star_schema")
DIM_SYMBOLS_PATH = os.path.join(OUT_DIR, "dim_symbols.csv")
FACT_PRICES_PATH = os.path.join(OUT_DIR, "fact_prices.csv")

SECTORS = [
    "Technology", "Healthcare", "Financials", "Energy", "Industrials",
    "Consumer Discretionary", "Utilities", "Materials", "Real Estate",
]
INDUSTRIES = [
    "Software", "Semiconductors", "Biotechnology", "Banking",
    "Oil & Gas", "Aerospace", "Retail", "Chemicals", "REITs",
]
EXCHANGES = ["NASDAQ", "NYSE", "AMEX"]


def stable_hash(s: str) -> int:
    return int(hashlib.md5(s.encode()).hexdigest(), 16)


def main() -> None:
    print(f"Reading {RAW_CSV} ...")
    df = pl.read_csv(RAW_CSV, infer_schema_length=1000)
    df.columns = [c.lower() for c in df.columns]
    print(f"  Loaded {len(df):,} rows, columns: {df.columns}")

    # --- dim_symbols ---
    tickers = (
        df.select(["ticker", "type"])
        .unique(subset=["ticker"])
        .sort("ticker")
    )

    ticker_list = tickers["ticker"].to_list()
    type_list   = tickers["type"].to_list()

    ticker_ids = list(range(1, len(ticker_list) + 1))
    sectors    = [SECTORS[stable_hash(t) % len(SECTORS)]    for t in ticker_list]
    industries = [INDUSTRIES[stable_hash(t) % len(INDUSTRIES)] for t in ticker_list]
    exchanges  = [EXCHANGES[stable_hash(t) % len(EXCHANGES)]  for t in ticker_list]

    dim_symbols = pl.DataFrame({
        "ticker_id": ticker_ids,
        "ticker":    ticker_list,
        "type":      type_list,
        "sector":    sectors,
        "industry":  industries,
        "exchange":  exchanges,
    })

    # --- fact_prices ---
    # Build ticker -> ticker_id mapping
    ticker_to_id = dict(zip(ticker_list, ticker_ids))

    fact_prices = (
        df.with_columns(
            pl.col("ticker")
            .replace_strict(ticker_to_id, return_dtype=pl.Int64)
            .alias("ticker_id")
        )
        .select(["ticker_id", "date", "open", "high", "low", "close", "volume"])
    )

    # --- Write output ---
    os.makedirs(OUT_DIR, exist_ok=True)

    print(f"\nWriting dim_symbols ({len(dim_symbols):,} rows) -> {DIM_SYMBOLS_PATH}")
    dim_symbols.write_csv(DIM_SYMBOLS_PATH)
    dim_size_kb = os.path.getsize(DIM_SYMBOLS_PATH) / 1024
    print(f"  Size: {dim_size_kb:.1f} KB")

    print(f"\nWriting fact_prices ({len(fact_prices):,} rows) -> {FACT_PRICES_PATH}")
    fact_prices.write_csv(FACT_PRICES_PATH)
    fact_size_mb = os.path.getsize(FACT_PRICES_PATH) / (1024 ** 2)
    print(f"  Size: {fact_size_mb:.2f} MB")

    print("\nDone.")
    print(f"  dim_symbols : {len(dim_symbols):,} tickers")
    print(f"  fact_prices : {len(fact_prices):,} price rows")


if __name__ == "__main__":
    main()
