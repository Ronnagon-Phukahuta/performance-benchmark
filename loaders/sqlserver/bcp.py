"""
Phase 11 — SQL Server BCP vs BULK INSERT Benchmark

Phase 1 baseline: all pymssql write paths converge to ~5.3–5.7h extrapolated.
Root cause: pymssql wraps every row in TDS framing regardless of batching.

Phase 11 bypasses TDS row framing with two server-side paths:

  BULK INSERT (T-SQL)
    SQL Server reads the CSV file directly from its own filesystem.
    No per-row TDS framing. Server parses the CSV internally.
    Requires the file to be inside the container — transferred via 'docker cp'.

  BCP (Bulk Copy Program)
    Client-side binary that uses the TDS Bulk Copy Protocol extension.
    Much faster than executemany but still sends data over the wire.
    Checked in order: Windows host bcp.exe → docker exec container bcp.

Protocol comparison:
  pymssql executemany  : TDS INSERT framing per row → ~5.7h at 28M rows
  BCP wire             : TDS Bulk Copy Protocol, binary batches → much faster
  BULK INSERT server   : no network transfer — server reads file from disk

Run order:
  start_sqlserver()          → start container if stopped (optional helper)
  write_bulk_insert()        → docker cp + T-SQL BULK INSERT
  write_bcp()                → host bcp / docker exec bcp (if available)
  query()                    → GROUP BY ticker for comparison
"""

import os
import subprocess
import tempfile

import polars as pl
import pymssql

from benchmark.metrics import measure

RAW_CSV = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "data", "raw", "all_stocks.csv")
)

HOST = "localhost"
PORT = 1433
USER = "sa"
PASSWORD = "Benchmark123!"
DATABASE = "master"
CONTAINER = "benchmark_sqlserver"

# Table names kept separate so both methods coexist for comparison
TABLE_BULK = "stocks_bulk_insert"
TABLE_BCP = "stocks_bcp"

CREATE_TABLE_SQL = """
    CREATE TABLE {table} (
        date    VARCHAR(20),
        ticker  VARCHAR(20),
        type    VARCHAR(10),
        [open]  FLOAT,
        high    FLOAT,
        low     FLOAT,
        [close] FLOAT,
        volume  FLOAT
    )
"""

QUERY_SQL = """
    SELECT ticker,
           AVG([close]) AS avg_close,
           MAX([close]) AS max_close,
           MIN([close]) AS min_close
    FROM {table}
    GROUP BY ticker
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _connect():
    return pymssql.connect(HOST, USER, PASSWORD, DATABASE, port=PORT)


def _check_bcp_host() -> str | None:
    """Return path to bcp.exe on Windows host, or None if not found."""
    try:
        r = subprocess.run(["bcp", "-v"], capture_output=True, timeout=5)
        if r.returncode == 0 or b"BCP" in r.stdout.upper() + r.stderr.upper():
            return "bcp"
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def _check_bcp_container() -> str | None:
    """Return in-container bcp path if the container is running, else None."""
    bcp_path = "/opt/mssql-tools18/bin/bcp"
    try:
        r = subprocess.run(
            ["docker", "exec", CONTAINER, "test", "-x", bcp_path],
            capture_output=True, timeout=5,
        )
        if r.returncode == 0:
            return bcp_path
    except (FileNotFoundError, subprocess.TimeoutExpired):
        pass
    return None


def _copy_csv_to_container(container_path: str = "/tmp/all_stocks.csv") -> str:
    """
    Copy the raw CSV into the SQL Server container.
    Returns the in-container path.  Raises on failure.
    """
    print(f"  Copying CSV to container ({os.path.getsize(RAW_CSV) / 1e9:.2f} GB)...")
    r = subprocess.run(
        ["docker", "cp", RAW_CSV, f"{CONTAINER}:{container_path}"],
        capture_output=True, text=True,
    )
    if r.returncode != 0:
        raise RuntimeError(f"docker cp failed: {r.stderr.strip()}")
    print(f"  Copied → {container_path}")
    return container_path


def _drop_create(table: str) -> None:
    conn = _connect()
    conn.autocommit(True)
    cur = conn.cursor()
    cur.execute(f"IF OBJECT_ID('{table}') IS NOT NULL DROP TABLE {table}")
    cur.execute(CREATE_TABLE_SQL.format(table=table))
    cur.close()
    conn.close()


# ---------------------------------------------------------------------------
# Benchmarks
# ---------------------------------------------------------------------------

def write_bulk_insert() -> None:
    """
    BULK INSERT — server reads CSV directly from its own filesystem.
    Steps:
      1. docker cp: transfer all_stocks.csv into the container (~2.5 GB)
      2. CREATE TABLE  (does not count toward measured time)
      3. BULK INSERT T-SQL  (measured)

    The measured window covers only the BULK INSERT execution — server-side I/O
    and parse, no Python object creation per row.
    """
    print("\n--- BULK INSERT (server-side CSV read) ---")
    container_csv = _copy_csv_to_container()

    print("  DROP / CREATE TABLE ...")
    _drop_create(TABLE_BULK)

    bulk_sql = f"""
        BULK INSERT {TABLE_BULK}
        FROM '{container_csv}'
        WITH (
            FIELDTERMINATOR = ',',
            ROWTERMINATOR   = '\\n',
            FIRSTROW        = 2,
            BATCHSIZE       = 100000,
            TABLOCK
        )
    """

    print("  Running BULK INSERT ...")
    with measure("phase11_sqlserver_bulk_insert_write", data_path="") as m:
        conn = _connect()
        conn.autocommit(True)
        cur = conn.cursor()
        cur.execute(bulk_sql)
        cur.close()
        conn.close()

    print(
        f"  BULK INSERT done: {m.value.duration_sec:.2f}s | "
        f"RAM: {m.value.peak_ram_mb:.1f} MB"
    )


def write_bcp() -> None:
    """
    BCP — Bulk Copy Program using the TDS Bulk Copy Protocol extension.
    Checked in order:
      1. bcp.exe on Windows host (mssql-tools installed locally)
      2. /opt/mssql-tools18/bin/bcp inside the SQL Server Docker container

    If neither is available, prints a diagnostic and skips.
    BCP requires the CSV to be accessible from where BCP runs:
      - host bcp  → reads from Windows filesystem directly
      - container bcp → reads from the in-container /tmp path (docker cp required)
    """
    print("\n--- BCP (Bulk Copy Protocol) ---")

    host_bcp = _check_bcp_host()
    container_bcp = _check_bcp_container()

    if host_bcp:
        print(f"  Using host bcp: {host_bcp}")
        csv_path = RAW_CSV
        bcp_cmd = [
            host_bcp,
            f"{DATABASE}.dbo.{TABLE_BCP}", "in", csv_path,
            "-S", f"{HOST},{PORT}",
            "-U", USER, "-P", PASSWORD,
            "-c", "-t,", "-b", "100000",
            "-F", "2",   # skip header row
        ]
    elif container_bcp:
        print(f"  Host bcp not found. Using container bcp: {container_bcp}")
        container_csv = _copy_csv_to_container()
        bcp_cmd = [
            "docker", "exec", CONTAINER,
            container_bcp,
            f"{DATABASE}.dbo.{TABLE_BCP}", "in", container_csv,
            "-S", "localhost",
            "-U", USER, "-P", PASSWORD,
            "-c", "-t,", "-b", "100000",
            "-F", "2",
            "-C", "65001",  # UTF-8
            "-u",           # trust server certificate (mssql-tools18)
        ]
    else:
        print(
            "  BCP not available (not on host, container not running or bcp absent).\n"
            "  To enable: install mssql-tools on Windows OR start benchmark_sqlserver container.\n"
            "  Skipping bcp benchmark — use write_bulk_insert() instead."
        )
        return

    print("  DROP / CREATE TABLE ...")
    _drop_create(TABLE_BCP)

    print(f"  Running BCP ({'host' if host_bcp else 'container'}) ...")
    with measure("phase11_sqlserver_bcp_write", data_path="") as m:
        r = subprocess.run(bcp_cmd, capture_output=True, text=True)

    if r.returncode != 0:
        print(f"  BCP failed (exit {r.returncode}):\n{r.stderr.strip()}")
        print(f"  stdout: {r.stdout.strip()[:500]}")
        return

    lines = [l for l in r.stdout.splitlines() if l.strip()]
    for line in lines[-6:]:
        print(f"  {line}")

    print(
        f"  BCP done: {m.value.duration_sec:.2f}s | "
        f"RAM: {m.value.peak_ram_mb:.1f} MB"
    )


def query(table: str = TABLE_BULK) -> None:
    """GROUP BY ticker — AVG/MAX/MIN close. Run after either write variant."""
    print(f"\n--- Query: GROUP BY ticker (table={table}) ---")
    with measure(f"phase11_sqlserver_query_{table}", data_path="") as m:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(QUERY_SQL.format(table=table))
        result = cur.fetchall()
        cur.close()
        conn.close()
    print(
        f"  query done: {m.value.duration_sec:.2f}s | "
        f"RAM: {m.value.peak_ram_mb:.1f} MB | Tickers: {len(result):,}"
    )


def check_rowcount(table: str = TABLE_BULK) -> None:
    """Print row count for a quick sanity check after load."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"  {table}: {count:,} rows")


if __name__ == "__main__":
    import sys
    print("=== Phase 11 — SQL Server BCP vs BULK INSERT ===")
    print(f"  Host bcp available : {_check_bcp_host() is not None}")
    print(f"  Container bcp avail: {_check_bcp_container() is not None}")
    print()

    run_bulk = "--bcp-only" not in sys.argv
    run_bcp  = "--bulk-only" not in sys.argv

    if run_bulk:
        write_bulk_insert()
        check_rowcount(TABLE_BULK)
        query(TABLE_BULK)

    if run_bcp:
        write_bcp()
        if _check_bcp_host() or _check_bcp_container():
            check_rowcount(TABLE_BCP)
            query(TABLE_BCP)

    print("\nPhase 11 complete. Results saved to benchmark_results.json")
