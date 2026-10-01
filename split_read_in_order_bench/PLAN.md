# Plan: make `read_in_order_split_by_key_prefix_in` beat every alternative

Goal: one query streams rows of many markets in time order from a market-first table
(`ORDER BY (market_id, received_at, tie)`), with latency (server time to the last row, and the
first row) at or below the best alternative on every workload, and memory bounded by active
markets, not rows. Alternatives: market-first + full sort (today's best), time-first table,
1/10-minute bucket layouts.

## Workloads (layout-eval.py, price_change, 8 columns incl. 2 Nullable(UInt16))

| workload | shape |
|---|---|
| one_market_wide | 1 busy market, whole range |
| one_market_1min | 1 market, 1 minute |
| few_markets_wide | 10 random markets, whole range |
| nra_A_sparse | 3,642 markets, 43k spans, median 38 s (NRA windows ≤ 1 s, minute-rounded) |
| nra_B_mixed | 6,531 markets, 82k spans, some hours long |
| top500_wide | 500 busiest markets, whole range |
| rand5000_wide | 5,000 random markets, whole range |
| all_wide (bonus) | every market; time-first projection is the planned answer, not required |

Data: full universe 1 h / 3 h (77,889 markets, 143.5 M rows in 3 h) and set-B markets over 24 h.

## Where the time goes today (profiles, 2026-09-30)

Full universe, 1 h, `system.trace_log` CPU samples (2 ms), JIT off (test build):

| | patch | full sort |
|---|---|---|
| rand5000: wall / CPU / rows read → out | 7.3 s / 16.9 s / 14.5 M → 6.7 M | 2.3 s / 8.1 s / 10.8 M → 6.7 M |
| pipeline setup (single thread, before any read) | **1.8 s** | 0.05 s |
| reading (ISource::work, all threads) | 8.1 s CPU | 3.8 s CPU |
| merge (SplitMergingTransform, one thread) | 0.94 s (141 ns/row, 5,000 inputs) | — |
| gather (SplitGatherTransform, one thread) | **3.1 s** (470 ns/row: per-row virtual `insertFrom`, Nullable) | — |
| nra_A: wall / rows read → out | 3.3 s / 13 M → 0.27 M | 0.36 s / 9.6 M → 0.27 M |
| nra_A pipeline setup | **2.3 s** | — |

Setup cost per market: `MergeTreeReadPoolInOrder` ctor 0.61 s, `MergeTreeSelectProcessor::getPrewhereActions`
0.29 s and `transformHeader` 0.21 s (identical for every market, rebuilt 5,000 times),
`addSimpleTransform` 0.15 s. Mutex contention in `CachedCompressedReadBuffer::nextImpl` (~0.5 s).

Reads: the patch reads 34–100% more rows than the sort. Quiet markets share granules (1,024 rows),
so one granule is read and decompressed by several market readers; each reader also starts one
granule early (`lower_bound - 1`).

Span pruning: `(market_id, toStartOfInterval(received_at, 10 s)) IN (spans' buckets)` prunes
granules for both (sort 9.6 M → 3.3 M rows read, patch 13 M → 6 M) through the normal index path.
Evals use it for every pattern.

Earlier (top500, 3 h, 55 M rows): merge 66 ns/row after v5 (loser tree, key in registers,
branchless compare, prefetch); gather 34 ns/row without Nullable columns; read-ahead 16,384
removed stalls. Patch 5.2 s vs sort 10.7 s vs Rust client k-way 6.9 s.

## Changes (after review by Fable and Astra, 2026-10-01)

Both reviews agreed on the diagnosis and corrected three points: index marks are not time bounds
for lazy start (a granule's mark can belong to the previous market), shared reading must not fan
one reader out to several ports (deadlock / unbounded buffering), and the cache lock is the
uncompressed cache that every small per-market pool turns on (`max_marks_to_use_cache` is checked
per pool), which today also de-duplicates shared-block decompression.

0. **Measure first.** Compressed blocks and bytes read (`CompressedReadBufferBlocks`,
   `ReadCompressedBytes`) patch vs sort; `EXPLAIN PIPELINE` for where the span filter runs (a
   filter after the merge is single-threaded); which cache takes the mutex.
1. **Gather: Nullable through raw pointers** (nested data + null map), like fixed-size columns.
   No parallel gather (no ordered resize in the processor model; not needed below ~30 ns/row).
2. **Quick wins.** `KeyPrefixValueFilterTransform` passes a chunk through when every row matches
   (busy markets); no `convertToFullIfWrapped` copies unless needed.
3. **Setup once.** One `MergeTreeReadPoolInOrder` over every (market, part) entry, with one
   `MergeTreeReadTaskInfo` per data part shared by its entries, and processors built from one
   prebuilt prewhere info and header. Scaling unit is market × part. Target < 100 ms at 5,000.
4. **Shared compressed blocks read once.** Group neighbouring markets whose ranges share compressed
   blocks (mark `offset_in_compressed_file`), bounded so a group's rows fit the read-ahead budget;
   busy markets stay alone. One port per group; the merge keeps one cursor per market into the
   group's buffers (a buffer carries its market runs), so the tree and ranks stay per market.
   Then decide the uncompressed cache for this path (off, or threshold on total marks).
5. **Lazy start from span bounds.** An input is `setNeeded` only when its bound reaches the merge
   frontier; the bound is the market's first span start from the `(market, bucket)` set (rows
   below it are dropped by the filter anyway), not the index. Wide workloads have every market
   active, so this is for NRA-like regions: memory, open readers and first row follow the markets
   active at the frontier. Buffers released on finish.
6. **Merge ceiling, only if the profile shows it.** Time slices merged in parallel (busy markets
   cut at marks, quiet ones split in memory), slices in flight bounded; or two-level merge.
7. **Byte-based read-ahead** instead of 16,384 rows per input.

## Eval protocol

- `layout-eval.py` workloads above × {patch, market-first sort, time-first, bucket 1 min, bucket
  10 min}; span workloads use the bucket IN filter for every pattern.
- Per cell: server seconds, peak memory, rows read, time to first row (client, RowBinary), cold
  mark/uncompressed caches, 3 runs, median. Smoke on universe 1 h after every change; 3 h and
  set-B 24 h at the end.
- Correctness: `verify.sh` (28 checks) and `in_order` stateless tests (159) after every change;
  output of the patch equals the sort's (row hash) on every workload.
- Done when the patch is ≤ the best alternative on every workload except all_wide. If a workload
  cannot get there, show the floor: the profile of the remaining critical path and why it cannot
  shrink.

## Outcome (2026-10-01)

Done: the patch is at or below the best alternative on every workload at 1 h, 3 h and 24 h
(README "Results"; e.g. 5,000 random 24 h 8.5 s vs time-first 26 s, NRA A 24 h 1.84 s vs sort
2.08 s). Steps 0–3, 5 and 6 landed; step 4 became per-part reading groups (shared granules read
once by one reader, not shared buffers across readers); step 7 is still open (memory).

Beyond the plan, found by profiling:
- The merge emits runs (gallop + binary search below the runner-up) instead of one record per row.
- 4 gathers in parallel behind a round-robin deal and an in-order collect; no zero-fill.
- Step 6 as a frontier-based parallel top merge: rows below the least last key of the groups still
  reading are cut into tasks at sampled keys, merged and gathered by up to 8 workers.
- Readers are freed after their last block; buffers keep only output columns (an ALIAS's inputs and
  the merge's key copy were ~45% of buffered bytes).
- Planning looked up each value's ranges with a linear scan over the part's ranges (thousands with
  the span filter): binary search, −0.15 s on NRA A 24 h.
- Read-ahead 65,536 rows (was 16,384): readers paced 16k rows ahead of the merge left a 0.8 s tail
  at 3 threads on NRA A 24 h.

Not reached: memory on wide 24 h reads is 1.5–2× the sort (README "Floors").

Afterwards: the stock sort as 8 concurrent time slices reaches 12.4 s on 5,000 random / 24 h (patch
8.5 s, one-query sort 34.3 s), so the patch stays parked (README "Status").
