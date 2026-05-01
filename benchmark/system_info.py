import json
import os
import platform
import subprocess
import sys

import duckdb
import pandas as pd
import polars as pl
import psutil

_BOLD = "\033[1m"
_RESET = "\033[0m"

_RESULTS_FILE = os.path.join(os.path.dirname(__file__), "..", "results", "benchmark_results.json")
_PROJECT_DRIVE = os.path.splitdrive(os.path.abspath(__file__))[0] + os.sep


def _get_cpu_name() -> str:
    try:
        lines = subprocess.check_output("wmic cpu get name", shell=True).decode().strip().split("\n")
        return lines[-1].strip()
    except Exception:
        return "N/A"


def _get_disk_type() -> str:
    try:
        output = subprocess.check_output("wmic diskdrive get model", shell=True).decode()
        if "nvme" in output.lower():
            return "NVMe SSD"
        elif "ssd" in output.lower():
            return "SSD"
        else:
            return "SSD/NVMe"
    except Exception:
        return "SSD/NVMe"


def _get_cpu_freq_ghz() -> str:
    try:
        freq = psutil.cpu_freq()
        if freq and freq.max:
            ghz = freq.max / 1000
            cpu_name = _get_cpu_name()
            if "12400F" in cpu_name:
                return f"{ghz:.2f} GHz base / 4.40 GHz boost"
            return f"{ghz:.2f} GHz (base clock, boost may be higher)"
        return "N/A"
    except Exception:
        return "N/A"


def _get_disk_info() -> tuple[str, str]:
    try:
        usage = psutil.disk_usage(_PROJECT_DRIVE)
        total_gb = f"{usage.total / (1024 ** 3):.1f} GB"
        free_gb = f"{usage.free / (1024 ** 3):.1f} GB"
        return total_gb, free_gb
    except Exception:
        return "N/A", "N/A"


def _get_results_info() -> tuple[str, str]:
    if not os.path.isfile(_RESULTS_FILE):
        return "N/A", "N/A"
    try:
        size_mb = f"{os.path.getsize(_RESULTS_FILE) / (1024 ** 2):.2f} MB"
    except Exception:
        size_mb = "N/A"
    try:
        with open(_RESULTS_FILE, "r", encoding="utf-8") as f:
            records = json.load(f)
        # Use count of records as a proxy for dataset exposure
        dataset_rows = "N/A"
        dataset_rows = f"{len(records)} benchmark records"
    except Exception:
        dataset_rows = "N/A"
    return dataset_rows, size_mb


def _print_section(title: str, rows: list[tuple[str, str]], w_key: int, w_val: int) -> None:
    sep = "-" * (w_key + w_val + 3)
    print(f"\n{_BOLD}{title}{_RESET}")
    print(sep)
    for key, val in rows:
        print(f"  {key:<{w_key}} {val}")
    print(sep)


def main() -> None:
    # ── Hardware ─────────────────────────────────────────────────────────────
    cpu_name = _get_cpu_name()
    try:
        physical_cores = str(psutil.cpu_count(logical=False))
    except Exception:
        physical_cores = "N/A"
    try:
        logical_cores = str(psutil.cpu_count(logical=True))
    except Exception:
        logical_cores = "N/A"
    cpu_freq = _get_cpu_freq_ghz()

    try:
        ram_gb = f"{psutil.virtual_memory().total / (1024 ** 3):.1f} GB"
    except Exception:
        ram_gb = "N/A"

    disk_total, disk_free = _get_disk_info()
    disk_type = _get_disk_type()

    hw_rows = [
        ("CPU name",     cpu_name),
        ("CPU cores",    f"{physical_cores} physical / {logical_cores} logical"),
        ("CPU max freq", cpu_freq),
        ("RAM total",    ram_gb),
        ("Disk drive",   _PROJECT_DRIVE),
        ("Disk total",   disk_total),
        ("Disk free",    disk_free),
        ("Disk type",    disk_type),
    ]

    # ── Software ─────────────────────────────────────────────────────────────
    try:
        os_info = platform.platform()
    except Exception:
        os_info = "N/A"
    try:
        python_ver = sys.version.split("\n")[0].strip()
    except Exception:
        python_ver = "N/A"
    try:
        polars_ver = pl.__version__
    except Exception:
        polars_ver = "N/A"
    try:
        duckdb_ver = duckdb.__version__
    except Exception:
        duckdb_ver = "N/A"
    try:
        pandas_ver = pd.__version__
    except Exception:
        pandas_ver = "N/A"
    try:
        psutil_ver = psutil.__version__
    except Exception:
        psutil_ver = "N/A"

    sw_rows = [
        ("OS",      os_info),
        ("Python",  python_ver),
        ("Polars",  polars_ver),
        ("DuckDB",  duckdb_ver),
        ("Pandas",  pandas_ver),
        ("Psutil",  psutil_ver),
    ]

    # ── Runtime context ───────────────────────────────────────────────────────
    dataset_rows, results_size = _get_results_info()

    rt_rows = [
        ("Benchmark records", dataset_rows),
        ("Results file size", results_size),
        ("Results file path", os.path.relpath(_RESULTS_FILE)),
    ]

    # ── Print ─────────────────────────────────────────────────────────────────
    all_rows = hw_rows + sw_rows + rt_rows
    w_key = max(len(k) for k, _ in all_rows) + 2
    w_val = max(len(v) for _, v in all_rows) + 2

    print(f"\n{_BOLD}{'=' * (w_key + w_val + 3)}{_RESET}")
    print(f"{_BOLD}  SYSTEM INFO{_RESET}")
    print(f"{_BOLD}{'=' * (w_key + w_val + 3)}{_RESET}")

    _print_section("HARDWARE", hw_rows, w_key, w_val)
    _print_section("SOFTWARE", sw_rows, w_key, w_val)
    _print_section("RUNTIME CONTEXT", rt_rows, w_key, w_val)


if __name__ == "__main__":
    main()
