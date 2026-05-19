import json
import os
import threading
import time
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


def _recreate_topic(topic: str, num_partitions: int = 1) -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        admin.delete_topics([topic])
        time.sleep(2)
    except UnknownTopicOrPartitionError:
        pass
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=num_partitions, replication_factor=1)])
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

def producer(topic: str, max_rows: int = None, rate: int = None) -> None:
    print(f"Running producer benchmark (Polars, topic={topic})...")
    df = pl.read_csv(FACT_CSV)
    rows = df.to_dicts()
    if max_rows:
        rows = rows[:max_rows]
    _recreate_topic(topic)
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, value_serializer=lambda v: json.dumps(v).encode())
    with measure(f"redpanda_polars_producer", data_path="") as m:
        sent = 0
        for row in rows:
            producer.send(topic, _row_to_value(row))
            sent += 1
            if rate:
                time.sleep(1.0 / rate)
            if sent % 100_000 == 0:
                print(f"  Sent {sent:,} rows...")
        producer.flush()
    print(f"Producer done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {sent:,}")

def consumer(topic: str, max_rows: int = 100_000) -> pl.DataFrame:
    print(f"Running consumer benchmark (Polars, topic={topic})...")
    with measure(f"redpanda_polars_consumer", data_path="") as m:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            auto_offset_reset="earliest",
            consumer_timeout_ms=60000,
            value_deserializer=lambda v: json.loads(v.decode()),
        )
        records = []
        for i, msg in enumerate(consumer):
            records.append(msg.value)
            if i + 1 >= max_rows:
                break
        df = pl.from_dicts(records)
    print(f"Consumer done: {m.value.duration_sec:.2f}s | RAM: {m.value.peak_ram_mb:.1f}MB | Rows: {len(df):,}")
    return df

if __name__ == "__main__":
    producer(TOPIC_THROTTLED, max_rows=100_000, rate=TARGET_RATE)
    consumer(TOPIC_THROTTLED, max_rows=100_000)
    print("All benchmarks complete. Results saved.")
