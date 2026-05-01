import json
import os
import time
import threading
from contextlib import contextmanager
from dataclasses import dataclass, asdict
from datetime import datetime, timezone

import psutil


@dataclass
class BenchmarkResult:
    operation_name: str
    duration_sec: float
    peak_ram_mb: float
    cpu_percent: float
    disk_size_mb: float
    timestamp: str


def _get_disk_size_mb(path: str) -> float:
    """Return total size of a file or directory in MB."""
    if not os.path.exists(path):
        return 0.0
    if os.path.isfile(path):
        return os.path.getsize(path) / (1024 ** 2)
    total = 0
    for dirpath, _, filenames in os.walk(path):
        for fname in filenames:
            fp = os.path.join(dirpath, fname)
            try:
                total += os.path.getsize(fp)
            except OSError:
                pass
    return total / (1024 ** 2)


class _ResourceMonitor:
    """Background thread that samples RAM and CPU usage."""

    def __init__(self, interval: float = 0.05):
        self._interval = interval
        self._stop = threading.Event()
        self._process = psutil.Process()
        self.peak_ram_mb: float = 0.0
        self._cpu_samples: list[float] = []
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        # Prime CPU measurement so the first sample is meaningful
        self._process.cpu_percent(interval=None)
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join()

    @property
    def avg_cpu_percent(self) -> float:
        return sum(self._cpu_samples) / len(self._cpu_samples) if self._cpu_samples else 0.0

    def _run(self):
        while not self._stop.is_set():
            try:
                ram = self._process.memory_info().rss / (1024 ** 2)
                if ram > self.peak_ram_mb:
                    self.peak_ram_mb = ram
                cpu = self._process.cpu_percent(interval=None)
                if cpu > 0:
                    self._cpu_samples.append(cpu)
            except psutil.NoSuchProcess:
                break
            self._stop.wait(self._interval)


RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")


def _save_result(result: BenchmarkResult) -> None:
    os.makedirs(RESULTS_DIR, exist_ok=True)
    results_file = os.path.join(RESULTS_DIR, "benchmark_results.json")
    existing: list = []
    if os.path.isfile(results_file):
        with open(results_file, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = []
    existing.append(asdict(result))
    with open(results_file, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2)


@contextmanager
def measure(operation_name: str, data_path: str = ""):
    """
    Context manager that measures execution time, peak RAM, CPU utilization,
    and disk size of the given data_path after the block completes.

    Usage::

        with measure("duckdb_write", data_path="data/duckdb/") as result:
            # ... perform operation ...
            pass
        print(result.value)
    """

    class _ResultHolder:
        value: BenchmarkResult = None

    holder = _ResultHolder()
    monitor = _ResourceMonitor()
    monitor.start()
    start = time.perf_counter()
    try:
        yield holder
    finally:
        duration = time.perf_counter() - start
        monitor.stop()
        disk_mb = _get_disk_size_mb(data_path) if data_path else 0.0
        result = BenchmarkResult(
            operation_name=operation_name,
            duration_sec=round(duration, 4),
            peak_ram_mb=round(monitor.peak_ram_mb, 2),
            cpu_percent=round(monitor.avg_cpu_percent, 2),
            disk_size_mb=round(disk_mb, 4),
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        holder.value = result
        _save_result(result)
