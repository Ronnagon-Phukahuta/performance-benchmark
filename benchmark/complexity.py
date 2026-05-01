from dataclasses import dataclass

_BOLD = "\033[1m"
_RESET = "\033[0m"


@dataclass
class ComplexityEntry:
    method: str
    technology: str
    write_complexity: str
    read_complexity: str
    query_complexity: str
    write_note: str
    read_note: str
    query_note: str


COMPLEXITY_TABLE: list[ComplexityEntry] = [
    ComplexityEntry(
        method="row_by_row (pandas)",
        technology="DuckDB",
        write_complexity="O(n)",
        read_complexity="O(n)",
        query_complexity="O(n)",
        write_note="1 Python call per row",
        read_note="full scan",
        query_note="full scan then filter",
    ),
    ComplexityEntry(
        method="row_by_row (polars)",
        technology="DuckDB",
        write_complexity="O(n)",
        read_complexity="O(n)",
        query_complexity="O(n)",
        write_note="1 Python call per row",
        read_note="full scan",
        query_note="full scan then filter",
    ),
    ComplexityEntry(
        method="batch_insert (pandas)",
        technology="DuckDB",
        write_complexity="O(n)",
        read_complexity="O(n)",
        query_complexity="O(n)",
        write_note="same loop, larger chunks",
        read_note="full scan",
        query_note="full scan then filter",
    ),
    ComplexityEntry(
        method="batch_insert (polars)",
        technology="DuckDB",
        write_complexity="O(n)",
        read_complexity="O(n)",
        query_complexity="O(n)",
        write_note="same loop, larger chunks",
        read_note="full scan",
        query_note="full scan then filter",
    ),
    ComplexityEntry(
        method="bulk_insert (pandas)",
        technology="DuckDB",
        write_complexity="O(1)",
        read_complexity="O(n)",
        query_complexity="O(k)",
        write_note="single vectorized call",
        read_note="full scan",
        query_note="columnar scan, k=cols used",
    ),
    ComplexityEntry(
        method="bulk_insert (polars)",
        technology="DuckDB",
        write_complexity="O(1)",
        read_complexity="O(n)",
        query_complexity="O(k)",
        write_note="single vectorized call",
        read_note="full scan",
        query_note="columnar scan, k=cols used",
    ),
    ComplexityEntry(
        method="copy_csv",
        technology="DuckDB",
        write_complexity="O(1)",
        read_complexity="O(n)",
        query_complexity="O(k)",
        write_note="no Python loop, C++ direct",
        read_note="full scan",
        query_note="columnar scan, k=cols used",
    ),
    ComplexityEntry(
        method="direct_parquet",
        technology="DuckDB",
        write_complexity="O(0)",
        read_complexity="O(n)",
        query_complexity="O(k)",
        write_note="no write step",
        read_note="full scan parquet",
        query_note="predicate + projection pushdown",
    ),
    ComplexityEntry(
        method="single_file (pandas)",
        technology="Parquet",
        write_complexity="O(1)",
        read_complexity="O(n)",
        query_complexity="O(k)",
        write_note="single vectorized write",
        read_note="full file load",
        query_note="column pruning only",
    ),
    ComplexityEntry(
        method="single_file (polars)",
        technology="Parquet",
        write_complexity="O(1)",
        read_complexity="O(n)",
        query_complexity="O(k)",
        write_note="single vectorized write",
        read_note="full file load",
        query_note="column pruning only",
    ),
    ComplexityEntry(
        method="lazy (polars)",
        technology="Parquet",
        write_complexity="O(1)",
        read_complexity="O(n)",
        query_complexity="O(k)",
        write_note="single vectorized write",
        read_note="lazy, collect only",
        query_note="predicate + projection pushdown",
    ),
    ComplexityEntry(
        method="compressed (snappy/gzip)",
        technology="Parquet",
        write_complexity="O(1)",
        read_complexity="O(n)",
        query_complexity="O(k)",
        write_note="single vectorized write",
        read_note="decompress + load",
        query_note="column pruning only",
    ),
    ComplexityEntry(
        method="partitioned (per ticker)",
        technology="Parquet",
        write_complexity="O(p)",
        read_complexity="O(1*)",
        query_complexity="O(1*)",
        write_note="p=num tickers, 1 file each",
        read_note="*if filter by ticker",
        query_note="*partition pruning if filtered",
    ),
    ComplexityEntry(
        method="bulk_copy (COPY FROM)",
        technology="Postgres",
        write_complexity="O(1)",
        read_complexity="O(n)",
        query_complexity="O(n)",
        write_note="server-side COPY, no Python loop",
        read_note="full table scan",
        query_note="no columnar, needs index",
    ),
    ComplexityEntry(
        method="batch_insert (psycopg2)",
        technology="Postgres",
        write_complexity="O(n)",
        read_complexity="O(n)",
        query_complexity="O(n)",
        write_note="executemany loop",
        read_note="full table scan",
        query_note="no columnar, needs index",
    ),
    ComplexityEntry(
        method="row_by_row (psycopg2)",
        technology="Postgres",
        write_complexity="O(n)",
        read_complexity="O(n)",
        query_complexity="O(n)",
        write_note="1 Python call per row",
        read_note="full table scan",
        query_note="no columnar, needs index",
    ),
]


def get_complexity_dict() -> dict[str, ComplexityEntry]:
    """Return COMPLEXITY_TABLE entries keyed by method name."""
    return {entry.method: entry for entry in COMPLEXITY_TABLE}


def print_complexity_table() -> None:
    """Print a formatted Big O complexity table for all loader variants."""
    w_method = max(len("method"), max(len(e.method) for e in COMPLEXITY_TABLE)) + 2
    w_tech   = max(len("technology"), max(len(e.technology) for e in COMPLEXITY_TABLE)) + 2
    w_write  = max(len("write"), max(len(e.write_complexity) for e in COMPLEXITY_TABLE)) + 2
    w_read   = max(len("read"), max(len(e.read_complexity) for e in COMPLEXITY_TABLE)) + 2
    w_query  = max(len("query"), max(len(e.query_complexity) for e in COMPLEXITY_TABLE)) + 2
    w_note   = max(len("write_note"), max(len(e.write_note) for e in COMPLEXITY_TABLE)) + 2

    header = (
        f"{'method':<{w_method}}"
        f"{'technology':<{w_tech}}"
        f"{'write':<{w_write}}"
        f"{'read':<{w_read}}"
        f"{'query':<{w_query}}"
        f"{'write_note':<{w_note}}"
    )
    sep = "-" * len(header)

    print(f"\n{_BOLD}BIG O COMPLEXITY — ALL LOADER VARIANTS{_RESET}")
    print(sep)
    print(_BOLD + header + _RESET)
    print(sep)

    for e in COMPLEXITY_TABLE:
        line = (
            f"{e.method:<{w_method}}"
            f"{e.technology:<{w_tech}}"
            f"{e.write_complexity:<{w_write}}"
            f"{e.read_complexity:<{w_read}}"
            f"{e.query_complexity:<{w_query}}"
            f"{e.write_note:<{w_note}}"
        )
        print(line)

    print(sep)


if __name__ == "__main__":
    print_complexity_table()
