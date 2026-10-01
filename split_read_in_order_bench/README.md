# Streaming time-ordered reads over many key prefixes (`read_in_order_split_by_key_prefix_in`)

Experimental patch on v26.9.6.6-stable, 2026-09-30, optimized 2026-10-01. Status: **parked**. It
beats or ties the full sort, a time-first table and bucket layouts on every measured workload, but a
stock sort split into concurrent time slices gets most of the gain without a fork (see Status).

## Problem

Tables are stored market-first: `ORDER BY (market_id, received_at, tie)`. That is the best layout
for compression and for reading one market. A backtest replay needs rows of many markets in
**time** order:

```sql
SELECT ... FROM t WHERE market_id IN (<hundreds to thousands>) ORDER BY received_at, tie, market_id
```

Stock ClickHouse cannot read that in order (the key starts with `market_id`), so it reads every
matching row, sorts in memory, then outputs. Memory grows with the rows read and the first row
waits for the whole read. The alternatives were a client k-way merge of one query per market
(500–5,000 queries) or a time-first table (reads every market in the time range).

## What the patch does

With `read_in_order_split_by_key_prefix_in = 1`, when the query sorts by the key after its first
column and filters the first key column with `IN (set)`:

1. **Planning** (`optimizeReadInOrder.cpp`): the `IN` set on key column 0 is treated as fixed, so
   the rest of the key satisfies the `ORDER BY`. The sort description is matched to key positions
   (same direction, no collation, no monotonic wrapping); the split column is appended as the last
   tie-break when the query orders by it.
2. **Reading** (`ReadFromMergeTree::readInOrderSplitByKeyPrefix`): the set is sorted; for each value
   and part the primary index gives its granules (`[lower_bound − 1, upper_bound)`, intersected with
   the ranges the filter kept). One `MergeTreeReadPoolInOrder` and one prebuilt PREWHERE/header serve
   every reader (setup is no longer per value). Readers, per part:
   - **reading groups**: consecutive quiet values (together ≤ 32 granules, ≤ 64 values) share one
     reader, so a granule they share is decompressed once; the merge cuts each chunk by value;
   - **slices**: when few values are busy (≤ threads), a busy value's granules are cut into
     `clamp(marks / threads, 16, 256)`-granule slices read in parallel, a window ahead of the merge;
   - **lazy start**: an input whose first granule begins with its own value has that index mark as a
     bound; it is opened when the merge reaches the bound (at most 64 such readers ahead).
   - A reader's task, readers and buffers are freed right after its last block, not at query end.
   Then:
   - `SplitMergeKeysTransform` (per stream, parallel): merge key columns packed into order-preserving
     64-bit words (sign flip, descending flip), low 32 bits of the last word left for the input rank.
     It keeps only the columns the step outputs (plus the split column): not the key computed for the
     merge, not columns read only for PREWHERE (e.g. the inputs of an ALIAS like `received_at`).
   - **First level**: `SplitMergingTransform` per merge group of ~128 consecutive inputs, in parallel: a
     loser tree over the inputs' current packed keys; ties break by input rank. When the winner wins
     again, its rows below the runner-up are found by a linear probe then galloping, and emitted as one
     run record `(buffer, row, rows)`. A `SplitGatherTransform` per group copies runs into columns and
     forwards the keys.
   - **Top level** (more than one group): `SplitBatchingTransform` buffers the groups' streams and cuts
     every row below the frontier (the least last key of the groups still reading) into tasks of
     ~`max_block_size` rows between sampled keys. `SplitDealTransform` deals tasks round-robin to up to
     8 workers (`SplitTaskMergeTransform` + `SplitGatherTransform`), `SplitCollectTransform` takes them
     back in the same order. At most 4 × workers tasks wait; the groups are not read further until they go.
   - **One group**: one `SplitMergingTransform`, then 4 gathers dealt and collected the same way.
   - Gather: fixed-size and Nullable-over-fixed columns are copied by `memcpy` per run when runs
     average ≥ 4 rows, else value by value through raw pointers; output columns are not zero-filled.
3. Falls back to the normal plan for key types it cannot pack, more than 5 words of key, joins, FINAL,
   parallel replicas.

Correctness: `verify.sh` (31 on-vs-off md5 comparisons, incl. an expression in the key, ALIAS, DESC, read-ahead 1,
Nullable/String columns, subquery sets, window filters, key-first order); stateless tests matching
`in_order` (159 OK, 13 skipped, 0 failed) with the setting off and forced on.

## Results (2026-10-01)

`eval.py`: `price_change` (8 columns, 2 Nullable), every pattern on its own table layout, span
workloads prune granules with a `(market, 10 s bucket) IN` set for every pattern. Server seconds
(peak MiB), median of 3, cold mark cache, `use_uncompressed_cache = 0` (prod's setting), JIT off in
this build. Patch on the final binary; alternatives from the same day. The host is shared with prod
(load average 4–6), so cells move ±10% between runs. Output md5 equal across patterns in every row.

Full universe, 1 h (12:00–13:00, ~48 M rows in the hour):

| workload (rows out) | patch | market-first sort | time-first | 1-min bucket | 10-min bucket |
|---|---|---|---|---|---|
| 1 market | **0.07** (8) | **0.07** (9) | 0.86 | 0.30 | 0.11 |
| 1 market, 1 min | **0.04** | **0.04** | 0.10 | 0.05 | 0.06 |
| 10 markets | **0.07** | **0.07** | 0.68 | 0.28 | 0.30 |
| NRA A | **0.19** (15) | 0.23 (61) | 0.70 | 0.32 | 0.38 |
| NRA B | **0.46** (125) | 0.68 (96) | 1.00 | 1.04 | 1.24 |
| 500 busiest | **0.42** (208) | 1.28 (140) | 0.77 (126) | 1.74 | 1.94 |
| 5,000 random (6.7 M) | **0.71** (600) | 1.78 (205) | 0.76 (183) | 2.44 | 4.18 |

Full universe, 3 h (143.5 M rows, 77,889 markets):

| workload (rows out) | patch | market-first sort | time-first | 1-min bucket | 10-min bucket |
|---|---|---|---|---|---|
| 1 market (0.8 M) | **0.07** (33) | **0.07** (20) | 1.77 | 0.33 | 0.16 |
| 1 market, 1 min | **0.04** | **0.04** | 0.10 | 0.05 | 0.07 |
| 10 markets | **0.06** | 0.09 | 1.72 | 0.39 | 0.35 |
| NRA A (1.2 M) | **0.32** (71) | 0.43 (82) | 1.94 | 1.64 | 0.92 |
| NRA B (3.7 M) | **0.78** (332) | 1.44 (209) | 2.91 | 3.10 | 2.75 |
| 500 busiest (12.6 M) | **0.83** (472) | 2.60 (351) | 5.40 (303) | 5.77 | 7.51 |
| 5,000 random (19.9 M) | **1.10** (1,239) | 5.28 (538) | 11.28 (308) | 10.26 | 13.67 |

Set-B markets, 24 h (212 M rows):

| workload (rows out) | patch | market-first sort | time-first |
|---|---|---|---|
| 1 market (4.2 M) | **0.15** (156) | 0.19 (138) | 3.59 |
| 10 markets (0.4 M) | **0.10** | 0.15 | 2.64 |
| NRA A (7.3 M) | **1.84** (553) | 2.08 (245) | 4.57 (214) |
| NRA B (28.8 M) | **4.82** (2,373) | 9.36 (1,121) | 32.70 (668) |
| 500 busiest (88.9 M) | **2.76** (2,566) | 19.98 (2,257) | 27.13 (308) |
| 5,000 random (156 M) | **8.53** (7,590) | 36.34 (3,934) | 26.27 (312) |

Time to first row (client, RowBinary) is at or below the sort's everywhere except 5,000 random 24 h
(2.6 s vs 2.2 s); time-first gets its first row in ~0.4 s on wide reads.

### What each change bought (2026-10-01 round)

| change | measured on | before → after |
|---|---|---|
| one pool, one PREWHERE/header for all readers | rand5000 1 h setup | 1.8 s → < 0.1 s |
| gather through raw pointers incl. Nullable | rand5000 1 h gather | 470 → ~60 ns/row |
| reading groups per part (shared granules read once) | rows read, 24 h | −29% vs per-value readers |
| slices sized to the threads (was 16 granules) | 1 market 24 h blocks decompressed | 5,971 → 3,375 (367 → 205 MB) |
| merge emits runs (gallop + binary search) | 1 market 24 h merge | 63 → 1 ms |
| 4 parallel gathers, no zero-fill | 1 market 24 h | 0.33 → 0.16–0.21 s |
| free a reader right after its last block | rand5000 24 h memory | 9.6 → 8.3 GiB |
| buffer only output columns (not ALIAS inputs, not the merge's key copy) | rand5000 3 h / 24 h memory | 1.38 → 0.9 GiB / 8.3 → 5.8 GiB |
| per-value range lookup by binary search (planning) | NRA A 24 h | 2.55 → 2.0–2.1 s |
| parallel top merge (frontier → tasks → 8 workers) | rand5000 1 h / 24 h, top500 24 h | 1.10 → 0.68 s / 15.7 → 8.9 s / 7.4 → 3.9 s |
| at most 4 × workers tasks waiting | rand5000 24 h memory | 7.2 → 6.0 GiB |
| read-ahead 16,384 → 65,536 rows | NRA A 24 h / top500 3 h / NRA B 24 h | 2.0 → 1.6 s / 1.19 → 0.95 s / 5.3 → 4.6 s, memory +10–90% |

Tried, no gain: software prefetch in the gather (419 vs 386 ms); 8-granule reading groups (same
memory, slower); 4,096-row read-ahead (rand5000 24 h 15.7 → 19.3 s for −10% memory); slicing values
over half a thread's share of granules when many are busy (none qualified on NRA A); 8 lazy readers
ahead per group instead of 1 (no change).

### Floors (why it is not faster)

- **Wide reads (5,000 random, 1 h)**: planning 0.16 s on one thread (index analysis ~70 ms of it,
  common to every pattern), then reading 0.35 s on all 16 threads, then the parallel merge 0.25 s.
  The merge cannot start earlier: every market is active from the first row and a quiet market's
  first chunk is its whole hour, so almost all rows are read before the first row can go out.
  Market-first decompresses ~3× the rows it returns (a compressed block spans many markets), the
  same as the sort.
- **1 market / 10 markets / 1 minute**: ties with the sort to 10 ms; what is left is pipeline setup.
- **NRA A 24 h**: read-bound; the patch reads 4% more rows than the sort (a granule at a market
  boundary is read by both neighbours' readers when they are in different reading groups).
- **Memory**: wide 24 h reads hold 1.5–2× the sort (5,000 random: 7.6 vs 3.9 GiB) because rows
  wait in every stream's read-ahead (65,536 rows × ~4,700 streams) and carry 16 B of packed key each.
  The read-ahead is a latency/memory trade (`read_in_order_split_by_key_prefix_in_read_ahead_rows`);
  time-first stays at ~0.3 GiB on every workload.

## Status: parked

Beats or ties every alternative on every workload above (all_wide excluded: time-first is the
answer there). Still parked, because the stock sort gets most of the way without a fork: a full sort
reads and sorts blocks in parallel, then streams one k-way merge whose last 16 → 1 step is
single-threaded. Run as concurrent time slices (one sorted query per slice, read in order), the merge
is parallel: 5,000 random / 24 h takes 12.4 s with 8 slices of 3 h (4.3 GiB summed) against 34.3 s
for one query and 8.5 s (7.6 GiB) for the patch. On the motivating workload (NRA A 24 h) the patch
is 1.84 s vs 2.08 s. This build has the JIT off; prod's JIT compiles the sort's comparisons, so the
stock sort is likely closer still. Details: markets-v2 `docs/replay-regions/measurements.md`.

## Next steps

1. Memory on wide reads: a byte budget for read-ahead shared by all streams instead of rows per stream.
2. Merge boundary granules of neighbouring reading groups (the NRA A extra 4%).
3. Several tables: one patched query per table, streams merged on the client.
4. Upstream rather than carry a fork.

## Alternatives tested earlier

- **Minute-bucket layout**, tested: `ORDER BY (bucket, market_id, rx, tie)` streams only when the
  query spells the bucket in `ORDER BY` and in `WHERE`, and it lost to the market-first full sort
  on every region workload at both 1- and 10-minute buckets (markets-v2
  `docs/replay-regions/measurements.md`).
- **Spill for the full sort**: it did not trigger because it also waits for query memory to pass
  `max_bytes_ratio_before_external_sort` (0.5) × available memory; with the ratio at 0 it spills
  and cut 4 GiB to 1.1 GiB. Measured only while the page cache held the temp files, so its cost
  with real disk I/O is unknown.

## Build (from scratch ~1.5–2 h at -j16, ccache 20 GiB; incremental relink ~1–2 min)

```bash
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=clang-21 -DCMAKE_CXX_COMPILER=clang++-21 \
  -DENABLE_TESTS=0 -DENABLE_EMBEDDED_COMPILER=0 -DENABLE_RUST=0 -DENABLE_LIBRARIES=0 -DENABLE_UTILS=0 \
  -DENABLE_CLICKHOUSE_ALL=0 -DENABLE_CLICKHOUSE_SERVER=1 -DENABLE_CLICKHOUSE_CLIENT=1 -DENABLE_CLICKHOUSE_LOCAL=1 \
  -DPARALLEL_COMPILE_JOBS=16 -DPARALLEL_LINK_JOBS=1 -DCOMPILER_CACHE=ccache
ninja -C build clickhouse
```

The embedded compiler (JIT) is off, so sort comparisons in the stock full sort are slower than in
a default build; the patched merge does not use it.

## Files

- `PLAN.md`: the optimization plan and its review. `eval.py`: the workloads × patterns eval above.
- `setup.sh`: CI fast-test configs, a network namespace `chtest`, the server. `runtests.sh <pattern>`: stateless tests.
- `verify.sh`: on-vs-off correctness. `perf.sh`: loads the 3 h `pc` table. `q.sh`, `prof.sh`, `perf.sh`: timing and profiles.
- `fair.sh` + `summarize.py`: the comparison above; clients `rs/` (Rust, HTTP), `rsg/` (Rust, gRPC, one connection), `bench.mjs` (Node).
- `markets/`: market id sets. `nra-windows.sql`: NRA region query. `mf-load.sh`, `mf-query.py`, `mf-run.sh`, `mf-var.sh`: the one-query replay eval on the market-first layout.
