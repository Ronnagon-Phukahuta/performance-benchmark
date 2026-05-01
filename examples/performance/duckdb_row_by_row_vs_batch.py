"""
📚 EDUCATIONAL EXAMPLE — Why row_by_row ≈ batch_insert at 28M rows
This explains the non-obvious result: batching 10,000 rows is not 10,000x faster than 1 row.
Not runnable — see row_by_row.py and batch_insert.py for actual implementations.
"""

raise RuntimeError("This file is an educational example only.")

# ---- THE SURPRISING RESULT ----
# row_by_row write:   76.67s on 100K rows → ~6.0h extrapolated
# batch_insert write: 77.87s on 100K rows → ~6.1h extrapolated
# Difference: ~1.5% — essentially identical
#
# ---- WHY BATCHING DOESN'T HELP HERE ----
# Intuition says: 10,000 rows per trip = 10,000x fewer round trips = 10,000x faster
# Reality: DuckDB runs IN THE SAME PROCESS as Python — no network socket, no TCP stack
#
# Each Python→DuckDB call costs:
#   - Serialize batch to list of tuples: O(batch_size)
#   - Acquire GIL
#   - Call C extension boundary: ~1-10 microseconds
#   - Release GIL
#
# row_by_row:   28,000,000 iterations × Python loop overhead
# batch_insert: 2,800 iterations × Python loop overhead (but each iteration is 10,000x heavier)
# Total Python-side work is nearly identical
#
# ---- WHAT ACTUALLY FIXES IT ----
# bulk_insert:  1 Python call, zero loop
#   con.register("df", polars_dataframe)
#   con.execute("INSERT INTO stocks SELECT * FROM df")
#   → 3.48s for 28M rows (vs ~6h for row_by_row)
#   → Uses Apache Arrow IPC zero-copy transfer — no Python serialization at all
#
# ---- THE LESSON ----
# For in-process databases (DuckDB, SQLite):
#   - Round trip cost ≈ 0 (no network)
#   - Python loop overhead IS the bottleneck
#   - Only vectorized/bulk operations escape this trap
# For network databases (Postgres, MongoDB, SQL Server):
#   - Batching helps more because network round trips are expensive
#   - But Python loop overhead still accumulates at 28M rows
