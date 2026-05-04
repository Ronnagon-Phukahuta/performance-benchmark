import json
import os
import threading

import polars as pl
from kafka import KafkaConsumer, KafkaProducer

from benchmark.metrics import measure

BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "stock-prices"

FACT_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices.csv")
DIM_CSV  = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "dim_symbols.csv")


def write() -> None:
    print("Running write benchmark (Kafka producer → topic, one message per row)...")
    df = pl.read_csv(FACT_CSV)
    row_count = len(df)
    with measure("redpanda_star_write") as m:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        for i, row in enumerate(df.iter_rows(named=True), 1):
            producer.send(
                TOPIC,
                key=str(row["ticker_id"]).encode(),
                value={
                    "date":   row["date"],
                    "open":   row["open"],
                    "high":   row["high"],
                    "low":    row["low"],
                    "close":  row["close"],
                    "volume": row["volume"],
                },
            )
            if i % 1_000_000 == 0:
                print(f"  Sent {i:,}/{row_count:,} rows...")
        producer.flush()
    print(
        f"write done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB | "
        f"Rows: {row_count:,}"
    )


def read() -> None:
    print("Running read benchmark (Kafka consumer, consume all messages)...")
    with measure("redpanda_star_read") as m:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            consumer_timeout_ms=30000,
            value_deserializer=lambda v: json.loads(v.decode()),
        )
        row_count = sum(1 for _ in consumer)
        consumer.close()
    print(
        f"read done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB | "
        f"Rows consumed: {row_count:,}"
    )


def query_oltp() -> None:
    """
    Consume all messages from the topic and filter in Python:
      key == b'1'  AND  2020-01-01 <= date <= 2023-12-31

    Note: Kafka/Redpanda has no index — every query requires a full topic scan.
    """
    print("Running query_oltp benchmark (consume all, filter ticker_id=1, 2020–2023)...")
    print("Note: Kafka/Redpanda has no index — must consume entire topic and filter in Python.")
    with measure("redpanda_star_query_oltp") as m:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            value_deserializer=lambda v: json.loads(v.decode()),
        )
        matches = []
        for msg in consumer:
            if msg.key == b"1":
                date = msg.value.get("date", "")
                if "2020-01-01" <= date <= "2023-12-31":
                    matches.append(msg.value)
        consumer.close()
    print(
        f"query_oltp done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB | "
        f"Rows matched: {len(matches):,}"
    )


def query_concurrent(n_threads: int) -> None:
    """
    Spin up n_threads independent KafkaConsumers, each consuming the full topic
    and counting rows. Measures wall-clock time to completion.
    """
    print(f"Running query_concurrent benchmark ({n_threads} threads)...")
    counts: list[int] = [0] * n_threads
    errors: list[Exception | None] = [None] * n_threads

    def _consume(idx: int) -> None:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP_SERVERS,
                auto_offset_reset="earliest",
                consumer_timeout_ms=5000,
                value_deserializer=lambda v: json.loads(v.decode()),
                group_id=None,  # independent consumers, no group coordination
            )
            counts[idx] = sum(1 for _ in consumer)
            consumer.close()
        except Exception as exc:
            errors[idx] = exc

    with measure(f"redpanda_star_concurrent_{n_threads}") as m:
        threads = [threading.Thread(target=_consume, args=(i,), daemon=True) for i in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    failed = sum(1 for e in errors if e is not None)
    total_rows = sum(counts)
    print(
        f"query_concurrent({n_threads}) done: {m.value.duration_sec:.2f}s | "
        f"RAM: {m.value.peak_ram_mb:.1f} MB | "
        f"Total rows consumed: {total_rows:,} | Failed threads: {failed}"
    )
    if failed:
        for i, e in enumerate(errors):
            if e is not None:
                print(f"  Thread {i} error: {e}")


if __name__ == "__main__":
    # write()
    read()
    query_oltp()
    # query_concurrent DNF — single partition bottleneck
    # 5 threads = 1,018s (5x linear), extrapolated: 10=~2000s, 20=~4000s
    # Parallelism requires multiple partitions, not multiple consumers
    # for n in [5, 10, 20]:
    #     query_concurrent(n)
