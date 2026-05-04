import json
import os
import threading
import time

import duckdb
import polars as pl
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

from benchmark.metrics import measure

BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC_THROTTLED = "stock-prices-throttled"
TOPIC_MAX = "stock-prices-max"
TARGET_RATE = 1000  # rows/sec for throttled mode

FACT_CSV = os.path.join(os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices.csv")
DUCKDB_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "data", "duckdb", "streaming.db")


def _recreate_topic(topic: str, num_partitions: int = 1) -> None:
    """Delete topic if it exists, then create it fresh."""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        admin.delete_topics([topic])
        # Brief wait for deletion to propagate
        time.sleep(2)
    except UnknownTopicOrPartitionError:
        pass
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass  # topic already recreated, continue
    admin.close()


def _row_to_value(row: dict) -> dict:
    return {
        "ticker_id": row["ticker_id"],
        "date":      row["date"],
        "open":      row["open"],
        "high":      row["high"],
        "low":       row["low"],
        "close":     row["close"],
        "volume":    row["volume"],
        "_ts":       time.time(),  # produce timestamp for latency tracking
    }


def consumer_to_duckdb(topic: str, max_rows: int, results: dict) -> None:
    """
    Consume messages from topic in batches, insert into DuckDB.
    Runs in a background thread. Stops after max_rows consumed.
    Writes results (rows_consumed, avg_latency_ms) into the shared results dict.
    """
    os.makedirs(os.path.dirname(DUCKDB_PATH), exist_ok=True)
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS streaming_prices (
            ticker_id INTEGER,
            date      VARCHAR,
            open      DOUBLE,
            high      DOUBLE,
            low       DOUBLE,
            close     DOUBLE,
            volume    DOUBLE,
            produce_ts DOUBLE
        )
    """)

    time.sleep(2)  # wait for topic to have messages before first poll
    print(f"Consumer started, waiting for messages on topic '{topic}'...")

    # Use longer timeout for max throughput topic
    timeout_ms = 60000 if topic == TOPIC_MAX else 30000
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda v: json.loads(v.decode()),
    )

    batch: list[tuple] = []
    rows_consumed = 0
    latencies_ms: list[float] = []

    for msg in consumer:
        v = msg.value
        produce_ts = v.get("_ts", 0.0)
        latency_ms = (time.time() - produce_ts) * 1000 if produce_ts else 0.0
        latencies_ms.append(latency_ms)
        batch.append((
            v["ticker_id"], v["date"],
            v["open"], v["high"], v["low"], v["close"], v["volume"],
            produce_ts,
        ))
        rows_consumed += 1

        if rows_consumed % 10_000 == 0:
            print(f"  Consumer: {rows_consumed:,} rows received...")

        if len(batch) >= 10_000:
            con.executemany(
                "INSERT INTO streaming_prices VALUES (?,?,?,?,?,?,?,?)", batch
            )
            batch.clear()
            results["rows_consumed"] = rows_consumed  # incremental update

        if rows_consumed >= max_rows:
            break

    if batch:
        con.executemany(
            "INSERT INTO streaming_prices VALUES (?,?,?,?,?,?,?,?)", batch
        )

    consumer.close()
    con.close()

    results["rows_consumed"] = rows_consumed
    results["avg_latency_ms"] = (
        sum(latencies_ms) / len(latencies_ms) if latencies_ms else 0.0
    )


def producer_throttled() -> tuple[float, float]:
    """
    Send 100,000 rows to TOPIC_THROTTLED at TARGET_RATE rows/sec.
    Returns (actual_rows_per_sec, total_time_sec).
    """
    print(f"Running producer_throttled (100K rows @ {TARGET_RATE:,} rows/sec)...")
    _recreate_topic(TOPIC_THROTTLED)

    df = pl.read_csv(FACT_CSV, n_rows=100_000)
    row_count = len(df)
    interval = 1.0 / TARGET_RATE  # seconds per row

    with measure("redpanda_streaming_throttled_produce") as m:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        for row in df.iter_rows(named=True):
            t0 = time.perf_counter()
            producer.send(TOPIC_THROTTLED, value=_row_to_value(row))
            elapsed = time.perf_counter() - t0
            sleep_for = interval - elapsed
            if sleep_for > 0:
                time.sleep(sleep_for)
        producer.flush()

    actual_rps = row_count / m.value.duration_sec
    print(
        f"producer_throttled done: {m.value.duration_sec:.2f}s | "
        f"Actual rate: {actual_rps:,.0f} rows/sec | "
        f"RAM: {m.value.peak_ram_mb:.1f} MB"
    )
    return actual_rps, m.value.duration_sec


def producer_max_throughput() -> tuple[float, float]:
    """
    Send 1,000,000 rows to TOPIC_MAX as fast as possible.
    Returns (rows_per_sec, mb_per_sec).
    """
    print("Running producer_max_throughput (1M rows, no throttle)...")

    df = pl.read_csv(FACT_CSV, n_rows=1_000_000)
    row_count = len(df)

    # Estimate payload size from a sample
    sample_payload = json.dumps(_row_to_value(df.row(0, named=True))).encode()
    estimated_mb = len(sample_payload) * row_count / (1024 ** 2)

    with measure("redpanda_streaming_max_throughput_produce") as m:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode(),
            linger_ms=5,         # allow small batching for throughput
            batch_size=65536,    # 64 KB batch
        )
        for i, row in enumerate(df.iter_rows(named=True), 1):
            producer.send(TOPIC_MAX, value=_row_to_value(row))
            if i % 100_000 == 0:
                print(f"  Sent {i:,}/{row_count:,} rows...")
        producer.flush()

    rps = row_count / m.value.duration_sec
    mbps = estimated_mb / m.value.duration_sec
    print(
        f"producer_max_throughput done: {m.value.duration_sec:.2f}s | "
        f"{rps:,.0f} rows/sec | {mbps:.1f} MB/sec | "
        f"RAM: {m.value.peak_ram_mb:.1f} MB"
    )
    return rps, mbps


def run_throttled() -> None:
    print("\n=== run_throttled ===")
    consumer_results: dict = {}

    consumer_thread = threading.Thread(
        target=consumer_to_duckdb,
        args=(TOPIC_THROTTLED, 100_000, consumer_results),
        daemon=True,
    )

    with measure("redpanda_streaming_throttled") as m:
        consumer_thread.start()
        actual_rps, produce_time = producer_throttled()
        consumer_thread.join(timeout=120)

    rows_consumed = consumer_results.get("rows_consumed", 0)
    avg_latency_ms = consumer_results.get("avg_latency_ms", 0.0)
    consumer_lag = 100_000 - rows_consumed

    print(
        f"\nrun_throttled summary:"
        f"\n  Total wall time:     {m.value.duration_sec:.2f}s"
        f"\n  Producer rate:       {actual_rps:,.0f} rows/sec (target: {TARGET_RATE:,})"
        f"\n  Rows consumed:       {rows_consumed:,} / 100,000"
        f"\n  Consumer lag:        {consumer_lag:,} rows"
        f"\n  Avg end-to-end lat:  {avg_latency_ms:.1f} ms"
        f"\n  Peak RAM:            {m.value.peak_ram_mb:.1f} MB"
    )


def run_max_throughput() -> None:
    print("\n=== run_max_throughput ===")

    # --- Phase 1: Producer only ---
    _recreate_topic(TOPIC_MAX)
    time.sleep(3)  # wait for topic to be ready

    print("\n-- Phase 1: Producer --")
    rps, mbps = producer_max_throughput()

    # --- Phase 2: Consumer only (reads everything already in topic) ---
    print("\n-- Phase 2: Consumer --")
    consumer_results: dict = {}
    consumer_thread = threading.Thread(
        target=consumer_to_duckdb,
        args=(TOPIC_MAX, 1_000_000, consumer_results),
        daemon=True,
    )

    with measure("redpanda_streaming_max_throughput_consume") as mc:
        consumer_thread.start()
        consumer_thread.join(timeout=600)
        if consumer_thread.is_alive():
            consumer_thread.join(timeout=0)  # release — read last known value

    rows_consumed = consumer_results.get("rows_consumed", 0)
    consumer_rps = rows_consumed / mc.value.duration_sec if mc.value.duration_sec > 0 else 0.0
    ratio_pct = (consumer_rps / rps * 100) if rps > 0 else 0.0

    print(
        f"\nrun_max_throughput summary:"
        f"\n  Producer:                {rps:,.0f} rows/sec | {mbps:.1f} MB/sec"
        f"\n  Consumer:                {consumer_rps:,.0f} rows/sec"
        f"\n  Consumer/Producer ratio: {ratio_pct:.1f}%"
        f"\n  Rows consumed:           {rows_consumed:,} / 1,000,000"
        f"\n  Peak RAM (consume):      {mc.value.peak_ram_mb:.1f} MB"
    )


if __name__ == "__main__":
    # run_throttled()
    run_max_throughput()
