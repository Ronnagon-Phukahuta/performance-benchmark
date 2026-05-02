import os

import polars as pl

INPUT  = os.path.join(os.path.dirname(__file__), "..", "data", "star_schema", "fact_prices.csv")
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "data", "star_schema", "fact_prices_sample.csv")


def main() -> None:
    print(f"Reading first 100,000 rows from {INPUT} ...")
    df = pl.read_csv(INPUT, n_rows=100_000)
    print(f"  Loaded {len(df):,} rows")

    print(f"Writing to {OUTPUT} ...")
    df.write_csv(OUTPUT)

    size_kb = os.path.getsize(OUTPUT) / 1024
    print(f"  Rows : {len(df):,}")
    print(f"  Size : {size_kb:.1f} KB")
    print("Done.")


if __name__ == "__main__":
    main()
