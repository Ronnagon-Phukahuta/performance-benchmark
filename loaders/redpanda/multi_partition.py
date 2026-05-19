"""
Phase 7D — Redpanda multi-partition benchmark

Compares single-partition baseline vs 4-partition parallel consumers.

Single partition (baseline from streaming.py):
  - Consumer throughput: ~1,933 rows/sec
  - All messages on one partition → one consumer thread

4-partition setup (this file):
  - Topic: 4 partitions, keyed by ticker_id % 4
  - Producer: assigns each message to partition via key
  - Consumer: 4 threads, each pinned to one partition via explicit assign()
  - Expected gain: ~4× throughput (near-linear with partition count)

Measurement:
  - write(): produce 1M rows to 4-partition topic, measure rows/sec + MB/sec
  - read():  4 consumer threads run in parallel, measure combined rows/sec
  - Prints comparison table vs single-partition baseline
"""

import json
import os
import threading
import time
from typing import Any

import polars as pl
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

from benchmark.metrics import measure

BOOTSTRAP_SERVERS = "localhost:19092"
TOPIC = "stock-prices-multi-partition"
NUM_PARTITIONS = 4

# Single-partition baseline (from streaming.py Phase 5 results)
BASELINE_PRODUCER_RPS = 21_787
BASELINE_CONSUMER_RPS = 1_933

FACT_CSV = os.path.join(
    os.path.dirname(__file__), "..", "..", "data", "star_schema", "fact_prices.csv"
)


def _recreate_topic(topic: str, num_partitions: int) -> None:
    """Delete and recreate topic with the specified partition count."""
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        admin.delete_topics([topic])
        time.sleep(2)  # wait for deletion to propagate
    except UnknownTopicOrPartitionError:
        pass
    try:
        admin.create_topics(
            [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1)]
        )
    except TopicAlreadyExistsError:
        pass
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
        "_ts":       time.time(),
    }


def _consume_partition(
    partition_id: int,
    max_rows: int,
    results: list[dict[str, Any]],
    idx: int,
) -> None:
    """
    Consume messages from a single partition.
    Each thread is pinned to one partition via explicit assign().
    Writes per-thread stats into results[idx].
    """
    tp = TopicPartition(TOPIC, partition_id)
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        consumer_timeout_ms=60_000,
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)

    rows_consumed = 0
    latencies_ms: list[float] = []
    t_start = time.perf_counter()

    for msg in consumer:
        v = msg.value
        produce_ts = v.get("_ts", 0.0)
        if produce_ts:
            latencies_ms.append((time.time() - produce_ts) * 1000)
        rows_consumed += 1
        if rows_consumed >= max_rows:
            break

    elapsed = time.perf_counter() - t_start
    consumer.close()

    results[idx] = {
        "partition": partition_id,
        "rows_consumed": rows_consumed,
        "duration_sec": elapsed,
        "rps": rows_consumed / elapsed if elapsed > 0 else 0.0,
        "avg_latency_ms": sum(latencies_ms) / len(latencies_ms) if latencies_ms else 0.0,
    }


def write() -> None:
    """
    Produce 1M rows to a 4-partition topic.

    Partition assignment: explicit partition= parameter, NOT key-based routing.
    kafka-python's default partitioner applies murmur2 hash to the key bytes —
    short keys like b"0"–b"3" do not hash evenly across 4 partitions and can
    leave 2 partitions empty. Passing partition= directly bypasses the hash.

    linger_ms scaled by NUM_PARTITIONS: each partition buffer fills at 1/N the
    rate of a single-partition topic. Without scaling, linger_ms fires before
    each buffer reaches batch_size, producing many small partial flushes and
    lower throughput. linger_ms * 4 restores equivalent batching efficiency.
    """
    print(f"Recreating topic '{TOPIC}' with {NUM_PARTITIONS} partitions...")
    _recreate_topic(TOPIC, NUM_PARTITIONS)
    time.sleep(2)  # wait for topic metadata to propagate

    print("Loading fact CSV...")
    df = pl.read_csv(FACT_CSV, n_rows=1_000_000)
    row_count = len(df)

    sample_payload = json.dumps(_row_to_value(df.row(0, named=True))).encode()
    estimated_mb = len(sample_payload) * row_count / (1024 ** 2)

    print(f"Running write benchmark (4-partition producer, {row_count:,} rows)...")
    with measure("redpanda_multi_partition_write", data_path="") as m:
        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode(),
            linger_ms=5 * NUM_PARTITIONS,   # scale with partition count
            batch_size=65_536,
        )
        for i, row in enumerate(df.iter_rows(named=True), 1):
            # Direct partition assignment — bypasses murmur2 hash routing
            partition = row["ticker_id"] % NUM_PARTITIONS
            producer.send(TOPIC, value=_row_to_value(row), partition=partition)
            if i % 200_000 == 0:
                print(f"  Sent {i:,}/{row_count:,} rows...")
        producer.flush()

    rps = row_count / m.value.duration_sec
    mbps = estimated_mb / m.value.duration_sec
    print(
        f"\nwrite done: {m.value.duration_sec:.2f}s"
        f" | {rps:,.0f} rows/sec"
        f" | {mbps:.1f} MB/sec"
        f" | RAM: {m.value.peak_ram_mb:.1f} MB"
    )
    print(f"  vs single-partition baseline: {BASELINE_PRODUCER_RPS:,} rows/sec")
    speedup = rps / BASELINE_PRODUCER_RPS
    print(f"  speedup: {speedup:.2f}×")


def read() -> None:
    """
    Consume all 1M rows using 4 parallel consumer threads (one per partition).
    Measures combined throughput vs single-partition baseline.
    """
    # Rows per partition (1M rows / 4 partitions, approximately)
    rows_per_partition = 1_000_000 // NUM_PARTITIONS

    print(
        f"Running read benchmark (4 parallel consumer threads, "
        f"~{rows_per_partition:,} rows/partition)..."
    )

    thread_results: list[dict[str, Any]] = [{}] * NUM_PARTITIONS
    threads = [
        threading.Thread(
            target=_consume_partition,
            args=(p, rows_per_partition, thread_results, p),
            daemon=True,
        )
        for p in range(NUM_PARTITIONS)
    ]

    with measure("redpanda_multi_partition_read", data_path="") as m:
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=120)

    # Aggregate results
    total_rows = sum(r.get("rows_consumed", 0) for r in thread_results)
    combined_rps = total_rows / m.value.duration_sec if m.value.duration_sec > 0 else 0.0
    avg_latency_ms = sum(
        r.get("avg_latency_ms", 0.0) for r in thread_results if r
    ) / NUM_PARTITIONS

    print(f"\nread done (wall-clock): {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f} MB")
    print(f"\nPer-partition breakdown:")
    for r in thread_results:
        if r:
            print(
                f"  partition {r['partition']}: "
                f"{r['rows_consumed']:,} rows | "
                f"{r['rps']:,.0f} rows/sec | "
                f"{r['avg_latency_ms']:.1f} ms avg latency"
            )
    print(
        f"\nCombined consumer throughput: {combined_rps:,.0f} rows/sec"
        f" (total {total_rows:,} rows)"
    )
    print(f"Avg end-to-end latency:       {avg_latency_ms:.1f} ms")
    print(f"\nvs single-partition baseline: {BASELINE_CONSUMER_RPS:,} rows/sec")
    speedup = combined_rps / BASELINE_CONSUMER_RPS
    print(f"speedup: {speedup:.2f}× (theoretical max: {NUM_PARTITIONS}×)")
    efficiency = speedup / NUM_PARTITIONS * 100
    print(f"partition efficiency: {efficiency:.0f}%")


if __name__ == "__main__":
    write()
    read()
    print("\nAll benchmarks complete. Results saved.")
