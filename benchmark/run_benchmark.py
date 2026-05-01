import importlib
import os
import traceback

import polars as pl

LOADER_ORDER = [
    "loaders.duckdb.bulk_insert",
    "loaders.duckdb.bulk_insert_polars",
    "loaders.duckdb.copy_csv",
    "loaders.duckdb.direct_parquet",
    "loaders.parquet.single_file",
    "loaders.parquet.single_file_polars",
    "loaders.parquet.lazy_polars",
    "loaders.parquet.compressed",
    "loaders.postgres.bulk_copy",
    "loaders.mongodb.bulk_insert",
    "loaders.mongodb.bulk_insert_ordered",
]

DNF_LOADERS = [
    "loaders.duckdb.row_by_row",
    "loaders.duckdb.row_by_row_polars",
    "loaders.duckdb.batch_insert",
    "loaders.duckdb.batch_insert_polars",
    "loaders.postgres.row_by_row",
    "loaders.postgres.batch_insert",
    "loaders.parquet.partitioned",
    "loaders.sqlserver.row_by_row",
    "loaders.sqlserver.bulk_insert",
    "loaders.sqlserver.bulk_columnstore",
    "loaders.mongodb.row_by_row",
]

FULL_ROW_COUNT = 28_151_758
SUBSET_ROWS = 100_000

_RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "raw")
_SUBSET_PATH = os.path.join(_RAW_DIR, "all_stocks_subset.csv")
_FULL_PATH = os.path.join(_RAW_DIR, "all_stocks.csv")


def run_dnf_subset():
    print("=== Running DNF variants on 100K subset + extrapolating to 28M ===")
    print("(includes slow/untested loaders: sqlserver variants, row-by-row, batch insert, partitioned parquet)")

    # Write subset CSV
    print(f"Creating {SUBSET_ROWS:,}-row subset at {_SUBSET_PATH}...")
    df = pl.read_csv(_FULL_PATH, n_rows=SUBSET_ROWS)
    df.write_csv(_SUBSET_PATH)
    print(f"Subset written: {len(df):,} rows")

    try:
        for module_name in DNF_LOADERS:
            print(f"\n=== Running (subset): {module_name} ===")
            try:
                mod = importlib.import_module(module_name)
                # Monkey-patch RAW_CSV to point at subset
                mod.RAW_CSV = _SUBSET_PATH

                try:
                    mod.write()
                    # Extrapolate write duration from last saved result
                    import json
                    results_file = os.path.join(os.path.dirname(__file__), "..", "results", "benchmark_results.json")
                    with open(results_file, "r", encoding="utf-8") as f:
                        records = json.load(f)
                    # Find the most recent write result for this module
                    op_prefix = module_name.replace("loaders.", "").replace(".", "_") + "_write"
                    matching = [r for r in records if r["operation_name"] == op_prefix]
                    if matching:
                        subset_sec = matching[-1]["duration_sec"]
                        extrapolated_sec = subset_sec * (FULL_ROW_COUNT / SUBSET_ROWS)
                        print(f"  [EXTRAPOLATED full 28M rows] ~{extrapolated_sec / 3600:.1f}h")
                except Exception:
                    print("[ERROR] write() failed:")
                    traceback.print_exc()

                try:
                    mod.read()
                except Exception:
                    print("[ERROR] read() failed:")
                    traceback.print_exc()

                try:
                    mod.query()
                except Exception:
                    print("[ERROR] query() failed:")
                    traceback.print_exc()

            except Exception:
                print(f"[ERROR] Could not import {module_name}:")
                traceback.print_exc()
    finally:
        if os.path.exists(_SUBSET_PATH):
            os.remove(_SUBSET_PATH)
            print(f"\nCleaned up subset file: {_SUBSET_PATH}")


def main():
    print("=== DNF variants: 100K subset + extrapolate ===")
    run_dnf_subset()
    print("\n=== Full 28M rows benchmark ===")
    for module_name in LOADER_ORDER:
        print(f"\n=== Running: {module_name} ===")
        try:
            mod = importlib.import_module(module_name)
            if module_name == "loaders.duckdb.direct_parquet":
                try:
                    mod.read()
                except Exception:
                    print("[ERROR] direct_parquet.read() failed:")
                    traceback.print_exc()
                try:
                    mod.query()
                except Exception:
                    print("[ERROR] direct_parquet.query() failed:")
                    traceback.print_exc()
            else:
                try:
                    mod.write()
                except Exception:
                    print("[ERROR] write() failed:")
                    traceback.print_exc()
                try:
                    mod.read()
                except Exception:
                    print("[ERROR] read() failed:")
                    traceback.print_exc()
                try:
                    mod.query()
                except Exception:
                    print("[ERROR] query() failed:")
                    traceback.print_exc()
        except Exception:
            print(f"[ERROR] Could not import {module_name}:")
            traceback.print_exc()
    print("\n=== Printing summary table ===")
    try:
        import benchmark.run_all
        benchmark.run_all.main()
    except Exception:
        print("[ERROR] Could not print summary table:")
        traceback.print_exc()

if __name__ == "__main__":
    main()
