"""
Read results/benchmark_results.json and print formatted comparison tables.

Usage:
    python -m benchmark.run_all
"""
import json
import os

RESULTS_FILE = os.path.join(os.path.dirname(__file__), "..", "results", "benchmark_results.json")

# ANSI colours (work in any modern terminal)
_GREEN = "\033[92m"
_RED = "\033[91m"
_BOLD = "\033[1m"
_RESET = "\033[0m"


def _load_latest() -> dict[str, dict]:
    """Return one record per operation_name — the most recent by timestamp."""
    with open(RESULTS_FILE, "r", encoding="utf-8") as f:
        records: list[dict] = json.load(f)
    latest: dict[str, dict] = {}
    for r in records:
        name = r["operation_name"]
        if name not in latest or r["timestamp"] > latest[name]["timestamp"]:
            latest[name] = r
    return latest


def _op_type(name: str) -> str:
    for suffix in ("_write", "_read", "_query"):
        if name.endswith(suffix):
            return suffix.lstrip("_")
    return "other"


def _print_table(rows: list[dict], title: str, highlight: bool = False) -> None:
    """Print a formatted table. If highlight=True, mark fastest/slowest by duration."""
    COL_NAME = "operation_name"
    COL_DUR = "duration_sec"
    COL_RAM = "peak_ram_mb"
    COL_CPU = "cpu_percent"
    COL_DISK = "disk_size_mb"

    if not rows:
        print(f"\n{title}\n  (no data)")
        return

    # Column widths
    w_name = max(len(COL_NAME), max(len(r[COL_NAME]) for r in rows)) + 2
    w_dur  = max(len(COL_DUR),  12)
    w_ram  = max(len(COL_RAM),  12)
    w_cpu  = max(len(COL_CPU),  11)
    w_disk = max(len(COL_DISK), 12)

    header = (
        f"{'operation_name':<{w_name}}"
        f"{'duration_sec':>{w_dur}}"
        f"{'peak_ram_mb':>{w_ram}}"
        f"{'cpu_percent':>{w_cpu}}"
        f"{'disk_size_mb':>{w_disk}}"
    )
    sep = "-" * len(header)

    fastest_name = min(rows, key=lambda r: r[COL_DUR])[COL_NAME] if highlight else None
    slowest_name = max(rows, key=lambda r: r[COL_DUR])[COL_NAME] if highlight else None

    print(f"\n{_BOLD}{title}{_RESET}")
    print(sep)
    print(_BOLD + header + _RESET)
    print(sep)

    for r in rows:
        name = r[COL_NAME]
        line = (
            f"{name:<{w_name}}"
            f"{r[COL_DUR]:>{w_dur}.4f}"
            f"{r[COL_RAM]:>{w_ram}.2f}"
            f"{r[COL_CPU]:>{w_cpu}.2f}"
            f"{r[COL_DISK]:>{w_disk}.2f}"
        )
        if highlight and name == fastest_name:
            print(f"{_GREEN}{line}  << fastest{_RESET}")
        elif highlight and name == slowest_name:
            print(f"{_RED}{line}  << slowest{_RESET}")
        else:
            print(line)

    print(sep)


def main() -> None:
    if not os.path.isfile(RESULTS_FILE):
        print(f"No results file found at {RESULTS_FILE}")
        return

    latest = _load_latest()
    all_rows = sorted(latest.values(), key=lambda r: r["operation_name"])

    # ── Table 1: all operations ──────────────────────────────────────────────
    _print_table(all_rows, "ALL BENCHMARK RESULTS (latest run per operation, sorted by name)")

    # ── Tables 2-4: grouped by type, sorted by duration ──────────────────────
    for op_type in ("write", "read", "query"):
        group = sorted(
            [r for r in all_rows if _op_type(r["operation_name"]) == op_type],
            key=lambda r: r["duration_sec"],
        )
        _print_table(group, f"{op_type.upper()} OPERATIONS (sorted by duration)", highlight=True)

    print()


if __name__ == "__main__":
    main()
