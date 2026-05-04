"""
⚠️ ANTI-PATTERN — Using Kafka/Redpanda as a queryable database
Dumping 28M rows to a Kafka topic and consuming all to answer a single query
treats a message broker as a database. Results: 207s for 63 rows.
See streaming.py for the correct producer/consumer pattern.
"""

raise RuntimeError("This file is an educational example only.")

# ---- WHAT CAUSES THE PROBLEM ----
# producer sends 28M messages to topic (1,342s)
# query: find ticker_id=1 between 2020-2023
# consumer must read ALL 28M messages and filter in Python
# result: 207s to find 63 rows
#
# ---- WHY KAFKA IS NOT A DATABASE ----
# Kafka stores messages as append-only log — no index, no random access
# To answer any query: read from offset 0, filter in Python
# O(n) for every query regardless of selectivity
# A B-tree index (Postgres/DuckDB) answers same query in 0.02s
#
# ---- THE RIGHT USE CASE ----
# Kafka/Redpanda = transport layer, not storage layer
# Correct pattern:
#   Producer → Kafka topic → Consumer → write to DuckDB/Postgres → query DB
#   Kafka handles: ordering, durability, replay, fan-out
#   Database handles: indexing, filtering, aggregation
#
# ---- BENCHMARK COMPARISON ----
# Wrong (this file):  207s to find 63 rows (full topic scan)
# Right (DuckDB):     0.02s to find 63 rows (B-tree index lookup)
# Speedup:            10,350x
