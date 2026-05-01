"""
⚠️ OOM EXAMPLE — DO NOT RUN ON LARGE DATASETS
This file is preserved for educational purposes only.
Running this on 28,151,758 documents caused Windows OOM dialog on 32GB RAM.
See bulk_insert.py for the fixed chunked approach.
"""

raise RuntimeError("This file is an educational example only. See bulk_insert.py for the correct implementation.")

# ---- WHAT CAUSES OOM ----
# Problem: to_dicts() materializes ALL documents into Python objects at once
# At 28M rows: ~28-31GB Python object overhead before insert_many() is even called
#
# df = pl.read_csv(RAW_CSV)
# records = df.to_dicts()  # ← THIS LINE: 28M dicts × ~1KB overhead = ~28GB RAM
# db[COLLECTION].insert_many(records, ordered=False)  # ← never reached on 32GB machine
#
# ---- WHY IT HAPPENS ----
# Each Python dict stores:
#   - Hash table header: ~232 bytes
#   - 8 key-value pairs × ~50 bytes = ~400 bytes
#   - String objects for keys (repeated 28M times, not shared)
# Total: ~1KB per document × 28M = ~28GB before pandas conversion
#
# ---- THE FIX ----
# Process in chunks of 50,000 documents at a time:
# for i in range(0, total, CHUNK_SIZE):
#     chunk = df.slice(i, CHUNK_SIZE)
#     records = chunk.to_dicts()  # only 50K dicts in memory
#     db[COLLECTION].insert_many(records, ordered=False)
#
# ---- BENCHMARK RESULT ----
# Bad (this file): OOM at ~31GB RAM, Windows OOM dialog appeared
# Good (chunked):  208.83s, peak RAM 2,608MB, all 28M docs inserted successfully
