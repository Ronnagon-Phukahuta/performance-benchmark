"""
⚠️ PERFORMANCE EXAMPLE — Shows O(p) write scaling problem
Writing 8,049 individual Parquet files one at a time took ~2 hours at production scale.
See partitioned.py for the actual benchmark implementation.
"""

raise RuntimeError("This file is an educational example only.")

# ---- WHAT CAUSES SLOWNESS ----
# tickers = df["ticker"].unique().tolist()  # 8,049 tickers
# for ticker in tickers:                    # ← O(p) loop: 8,049 iterations
#     ticker_df = df[df["ticker"] == ticker]
#     path = os.path.join(PARTITION_DIR, f"{ticker}.parquet")
#     ticker_df.to_parquet(path, index=False)
#
# ---- WHY IT'S SLOW ----
# Each iteration:
#   1. Filter 28M rows for one ticker (full scan each time) = O(n) per ticker
#   2. Open a new file handle
#   3. Write Parquet footer (schema + row group metadata)
#   4. Close file handle
# Total: O(n×p) filtering + O(p) filesystem operations
# At 8,049 tickers: ~0.9ms per file open/close × 8,049 = ~7s just for I/O overhead
# But the O(n) filter per ticker is the real killer: 28M rows × 8,049 scans
#
# ---- EXTRAPOLATED RESULT ----
# On 100K subset (19 tickers): 0.23s — seemed fast!
# Extrapolated to 28M rows (8,049 tickers): ~2 hours DNF
#
# ---- THE FIX ----
# Option A: Sort by ticker first, then write sequentially (O(n log n) sort + O(p) write)
#   df.sort("ticker").write_parquet(..., use_pyarrow=True)  # Polars partitioned write
#
# Option B: Parallel writes with multiprocessing
#   with Pool(cpu_count()) as pool:
#       pool.map(write_ticker, tickers)
