"""
Phase 7A — Pure DataFrame Benchmark

Runs all 9 DataFrame operations across 4 engines (pandas, polars_eager,
polars_lazy, polars_streaming) and prints a side-by-side comparison table.

Usage:
    python -m benchmark.run_dataframe
"""
import importlib

import loaders.dataframe.pandas_benchmark as pandas_bm
import loaders.dataframe.polars_eager_benchmark as polars_eager_bm
import loaders.dataframe.polars_lazy_benchmark as polars_lazy_bm
import loaders.dataframe.polars_streaming_benchmark as polars_streaming_bm

_BOLD  = "\033[1m"
_GREEN = "\033[92m"
_RESET = "\033[0m"

ENGINES = [
    ("pandas",           pandas_bm),
    ("polars_eager",     polars_eager_bm),
    ("polars_lazy",      polars_lazy_bm),
    ("polars_streaming", polars_streaming_bm),
]

OPS = [
    "op1_read_csv",
    "op2_filter",
    "op3_groupby",
    "op4_sort",
    "op5_join",
    "op6_window",
    "op7_string",
    "op8_typecast_null",
    "op9_concat",
]

OP_LABELS = {
    "op1_read_csv":       "op1  read_csv",
    "op2_filter":         "op2  filter",
    "op3_groupby":        "op3  groupby",
    "op4_sort":           "op4  sort",
    "op5_join":           "op5  join",
    "op6_window":         "op6  window",
    "op7_string":         "op7  string",
    "op8_typecast_null":  "op8  typecast+null",
    "op9_concat":         "op9  concat",
}


def _print_comparison(results: dict[str, dict[str, float]]) -> None:
    engine_names = [name for name, _ in ENGINES]
    col_op  = 22
    col_eng = 17

    total_width = col_op + col_eng * len(engine_names)
    sep = "-" * total_width

    print(f"\n\n{_BOLD}{'=' * total_width}")
    print("  PHASE 7A — DATAFRAME BENCHMARK COMPARISON  (duration in seconds)")
    print(f"{'=' * total_width}{_RESET}")

    header = f"{'Operation':<{col_op}}" + "".join(f"{e:>{col_eng}}" for e in engine_names)
    print(_BOLD + header + _RESET)
    print(sep)

    for op in OPS:
        label = OP_LABELS.get(op, op)
        times = [results[e].get(op, -1.0) for e in engine_names]
        valid = [t for t in times if t >= 0]
        best = min(valid) if valid else None

        row = f"{label:<{col_op}}"
        for t in times:
            if t < 0:
                cell = f"{'N/A':>{col_eng}}"
                row += cell
            else:
                formatted = f"{t:.4f}s"
                if t == best:
                    row += f"{_GREEN}{formatted:>{col_eng}}{_RESET}"
                else:
                    row += f"{formatted:>{col_eng}}"
        print(row)

    print(sep)
    print(f"{_BOLD}{'(green = fastest per operation)':>{total_width}}{_RESET}")
    print()


def main() -> None:
    results: dict[str, dict[str, float]] = {name: {} for name, _ in ENGINES}

    for engine_name, module in ENGINES:
        print(f"\n{'=' * 65}")
        print(f"  Engine: {engine_name}")
        print(f"{'=' * 65}")
        for op_name in OPS:
            fn = getattr(module, op_name)
            br = fn()
            results[engine_name][op_name] = br.duration_sec

    _print_comparison(results)


if __name__ == "__main__":
    main()
