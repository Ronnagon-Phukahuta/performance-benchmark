"""
⚠️ OOM EXAMPLE — DO NOT RUN ON LARGE COLLECTIONS
Full collection scan on 28M MongoDB documents caused MemoryError on 32GB RAM.
See bulk_insert.py read() for the correct single-ticker approach.
"""

raise RuntimeError("This file is an educational example only.")

# ---- WHAT CAUSES OOM ----
# result = list(db[COLLECTION].find({}, {"_id": 0}))  # ← fetches ALL 28M docs into RAM
# df = pd.DataFrame(result)                            # ← another ~6GB for DataFrame
#
# ---- WHY IT HAPPENS ----
# pymongo cursor fetches documents in batches but list() forces full materialization.
# 28M documents × ~1KB Python dict overhead = ~28GB
# pd.DataFrame(28M dicts) adds another ~6GB
# Total: ~34GB on a 32GB machine = MemoryError
#
# ---- THE FIX ----
# Option A: Query only what you need (single ticker with index)
#   result = list(db[COLLECTION].find({"ticker": "AAPL"}, {"_id": 0}))
#
# Option B: Use aggregation pipeline (server-side processing, streams result)
#   result = list(db[COLLECTION].aggregate(pipeline, allowDiskUse=True))
#   Only final grouped result (~8,049 dicts) comes to Python, not 28M docs
#
# ---- BENCHMARK RESULT ----
# Bad (this file): MemoryError mid-cursor, process killed
# Good (single ticker + index): 0.05s, 9,909 docs, 2,565MB RAM
