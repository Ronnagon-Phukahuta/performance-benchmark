import os
import polars as pl

STOCKS_DIR = os.path.join(os.path.dirname(__file__), "..", "kaggle-dataset", "stocks")
ETFS_DIR = os.path.join(os.path.dirname(__file__), "..", "kaggle-dataset", "etfs")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "raw", "all_stocks.csv")

SCHEMA_CAST = {
    "date": pl.Utf8,
    "ticker": pl.Utf8,
    "type": pl.Utf8,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
}

REQUIRED_COLS = ["date", "open", "high", "low", "close", "volume"]


def _load_folder(folder: str, asset_type: str) -> tuple[list[pl.DataFrame], int]:
    """Read all CSVs from a folder, returning (frames, skipped_count)."""
    csv_files = sorted(f for f in os.listdir(folder) if f.endswith(".csv"))
    frames: list[pl.DataFrame] = []
    skipped = 0
    return csv_files, frames, skipped


def load_all(
    stocks_dir: str = STOCKS_DIR,
    etfs_dir: str = ETFS_DIR,
    output_path: str = OUTPUT_PATH,
) -> pl.DataFrame:
    sources = [
        (stocks_dir, "stock"),
        (etfs_dir, "etf"),
    ]

    # Build full file list: [(filepath, ticker, type), ...]
    all_files: list[tuple[str, str, str]] = []
    for folder, asset_type in sources:
        if not os.path.isdir(folder):
            print(f"[WARN] Folder not found, skipping: {folder}")
            continue
        for filename in sorted(os.listdir(folder)):
            if filename.endswith(".csv"):
                ticker = filename[:-4]
                all_files.append((os.path.join(folder, filename), ticker, asset_type))

    total = len(all_files)
    print(f"Found {total} CSV files total ({sum(1 for _, _, t in all_files if t == 'stock')} stocks, "
          f"{sum(1 for _, _, t in all_files if t == 'etf')} ETFs)")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    frames: list[pl.DataFrame] = []
    skipped = 0

    for i, (filepath, ticker, asset_type) in enumerate(all_files, start=1):
        try:
            df = pl.read_csv(filepath, infer_schema_length=1000)

            if len(df) == 0:
                print(f"[WARN] {ticker}: 0 rows, skipping")
                skipped += 1
                continue

            # Normalize column names
            rename_map = {col: col.lower().replace(" ", "_") for col in df.columns}
            df = df.rename(rename_map)

            # Drop adj_close if present
            if "adj_close" in df.columns:
                df = df.drop("adj_close")

            # Validate required columns
            missing = [c for c in REQUIRED_COLS if c not in df.columns]
            if missing:
                print(f"[WARN] {ticker}: missing columns {missing}, skipping")
                skipped += 1
                continue

            df = df.select(REQUIRED_COLS)
            df = df.with_columns([
                pl.lit(ticker).alias("ticker"),
                pl.lit(asset_type).alias("type"),
            ])
            df = df.select(["date", "ticker", "type", "open", "high", "low", "close", "volume"])
            df = df.cast(SCHEMA_CAST)

            frames.append(df)

        except Exception as e:
            print(f"[WARN] {ticker}: failed to read — {e}")
            skipped += 1

        if i % 500 == 0:
            print(f"Processed {i}/{total} files...")

    print(f"Processed {total}/{total} files. Skipped {skipped}.")

    if not frames:
        print("[ERROR] No data loaded.")
        return pl.DataFrame()

    print("Combining all DataFrames...")
    combined = pl.concat(frames, rechunk=True)

    print(f"Writing to {output_path}...")
    combined.write_csv(output_path)

    size_mb = os.path.getsize(output_path) / (1024 ** 2)
    stock_tickers = combined.filter(pl.col("type") == "stock")["ticker"].n_unique()
    etf_tickers = combined.filter(pl.col("type") == "etf")["ticker"].n_unique()

    print(f"\nDone.")
    print(f"  Total rows   : {len(combined):,}")
    print(f"  Total tickers: {combined['ticker'].n_unique():,}")
    print(f"  Stocks       : {stock_tickers:,} tickers")
    print(f"  ETFs         : {etf_tickers:,} tickers")
    print(f"  Output size  : {size_mb:.2f} MB")
    print(f"  Output path  : {output_path}")

    return combined


if __name__ == "__main__":
    load_all()
