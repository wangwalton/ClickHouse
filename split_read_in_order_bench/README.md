# Streaming time-ordered reads over many key prefixes (`read_in_order_split_by_key_prefix_in`)

Experimental patch on v26.9.6.6-stable, 2026-09-30, optimized 2026-10-01. It is faster than the full
sort, a time-first table and bucket layouts on every measured workload but all markets at 1–3 h
(time-first) and the smallest ones (within 20 ms). On a market-first table with 8 KiB compressed
blocks it also holds less memory than the sort on long and busy reads (see Results and Status).

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
   - **Read-ahead**: `read_in_order_split_by_key_prefix_in_read_ahead_bytes` (512 MiB) is shared by
     the readers in proportion to their rows, each between an eighth of
     `read_in_order_split_by_key_prefix_in_read_ahead_rows` (65,536) and all of it, in blocks of half
     its share: busy markets read far ahead in large blocks, thousands of quiet ones hold little.
   - File buffers are capped at 16 KiB per column (a buffer sized to the range held a market's whole
     compressed range while its reader waited).
   Then:
   - `SplitMergeColumnsTransform` (per stream): keeps only the columns the step outputs, the split
     column and the merge keys; not columns read only for PREWHERE (e.g. the inputs of an ALIAS like
     `received_at`). A key the PREWHERE already computes (the time ALIAS, named differently by the
     new analyzer) is matched by expression and not computed again.
   - Merge keys are packed when the merge reaches a row (`packSplitRowKey`): order-preserving 64-bit
     words (sign flip, descending flip), low 32 bits of the last word left for the input rank. They
     are not stored per row, so a buffered row costs only its columns (26 B for `price_change`).
   - **First level**: `SplitMergingTransform` per merge group of ~128 consecutive inputs, in parallel: a
     loser tree over the inputs' current packed keys; ties break by input rank. When the winner wins
     again, its rows below the runner-up are found by a linear probe then galloping, and emitted as one
     run record `(buffer, row, rows)`. A `SplitGatherTransform` per group copies runs into columns.
   - **Top level** (more than one group): `SplitBatchingTransform` buffers the groups' streams and cuts
     every row below the frontier (the least last key of the groups still reading) into tasks between
     sampled keys, of `clamp(rows / 512, 8192, max_block_size)` rows. The groups share an eighth of the
     read-ahead budget (at most a sixteenth of the rows), so a few groups read far ahead and their
     merges run in parallel. `SplitDealTransform` deals tasks round-robin to up to 8 workers
     (`SplitTaskMergeTransform` + `SplitGatherTransform`), `SplitCollectTransform` takes them back in
     the same order. At most one task per worker waits; the groups are not read further until it goes.
   - **One group**: one `SplitMergingTransform`, then 4 gathers dealt and collected the same way.
   - Gather: fixed-size and Nullable-over-fixed columns are copied by `memcpy` per run when runs
     average ≥ 4 rows, else value by value through raw pointers; output columns are not zero-filled.
3. Falls back to the normal plan for key types it cannot pack, more than 5 words of key, joins, FINAL,
   parallel replicas.

Correctness: `verify.sh` (31 on-vs-off md5 comparisons, incl. an expression in the key, ALIAS, DESC, read-ahead 1,
Nullable/String columns, subquery sets, window filters, key-first order); stateless tests matching
`in_order` (159 OK, 13 skipped, 0 failed) with the setting off and forced on.

## Results (2026-10-01, memory round)

`eval.py` with `MF_TABLE=b8k`: `price_change` (8 columns, 2 Nullable); the market-first patterns read
a copy of the market-first table with `min_compress_block_size = 8192` (see "Table format"), the
other patterns their own layouts. Span workloads prune granules with a `(market, 10 s bucket) IN`
set for every pattern. Server seconds (peak MiB), median of 3, cold mark cache,
`use_uncompressed_cache = 0` (prod's setting), JIT off in this build. The host is shared with prod
(load average 3–11 during the runs), so cells move ±10%. Output md5 equal across patterns in every
row. Bold: fastest.

Full universe, 1 h (12:00–13:00):

| workload | patch | market-first sort | time-first | 1-min bucket | 10-min bucket |
|---|---|---|---|---|---|
| 1 market | 0.08 (8) | **0.07** (8) | 0.59 (61) | 0.40 (4) | 0.15 (4) |
| 1 market, 1 min | 0.05 (8) | **0.04** (0) | 0.10 (1) | 0.05 (1) | 0.06 (2) |
| 10 markets | 0.06 (8) | **0.05** (1) | 0.67 (29) | 0.27 (1) | 0.23 (2) |
| NRA A | **0.17** (13) | 0.18 (56) | 0.75 (54) | 0.37 (8) | 0.42 (19) |
| NRA B | **0.34** (75) | 0.59 (96) | 1.07 (89) | 0.89 (85) | 1.18 (78) |
| 500 busiest | **0.43** (108) | 1.25 (138) | 0.74 (126) | 1.79 (126) | 1.86 (128) |
| 5,000 random | **0.65** (249) | 1.74 (204) | 0.75 (165) | 2.44 (193) | 4.18 (137) |
| all markets | 6.33 (1,668) | 11.46 (1,183) | **4.92** (312) | 17.57 (358) | 37.26 (368) |

Full universe, 3 h (143.5 M rows, 77,889 markets):

| workload | patch | market-first sort | time-first | 1-min bucket | 10-min bucket |
|---|---|---|---|---|---|
| 1 market | **0.09** (21) | **0.09** (20) | 1.61 (64) | 0.30 (25) | 0.14 (21) |
| 1 market, 1 min | **0.04** (8) | **0.04** (0) | 0.10 (1) | **0.04** (1) | 0.06 (2) |
| 10 markets | **0.06** (8) | **0.06** (4) | 1.59 (65) | 0.40 (4) | 0.32 (4) |
| NRA A | **0.36** (25) | 0.40 (76) | 2.19 (71) | 1.26 (41) | 0.85 (70) |
| NRA B | **0.73** (189) | 1.42 (210) | 2.75 (207) | 3.50 (200) | 2.82 (212) |
| 500 busiest | **1.00** (266) | 3.21 (345) | 5.38 (298) | 6.34 (288) | 7.62 (274) |
| 5,000 random | **1.36** (636) | 5.88 (540) | 12.56 (308) | 9.85 (273) | 13.69 (292) |
| all markets | 18.67 (3,483) | 36.24 (3,611) | **18.38** (313) | 63.77 (356) | 121.90 (554) |

Set-B markets, 24 h (212 M rows):

| workload | patch | market-first sort | time-first | 1-min bucket | 10-min bucket |
|---|---|---|---|---|---|
| 1 market | **0.17** (77) | 0.18 (135) | 2.31 (114) | 1.92 (59) | 0.57 (119) |
| 1 market, 1 min | 0.06 (11) | **0.04** (1) | 0.08 (1) | 0.06 (1) | 0.21 (2) |
| 10 markets | 0.12 (11) | **0.11** (12) | 2.75 (65) | 1.72 (14) | 0.56 (11) |
| NRA A | **1.98** (207) | 2.15 (239) | 4.87 (211) | 4.68 (190) | 6.22 (225) |
| NRA B | **5.16** (1,147) | 9.49 (1,116) | 31.44 (653) | 12.98 (707) | 25.76 (659) |
| 500 busiest | **3.18** (718) | 20.81 (2,247) | 26.12 (308) | 32.37 (408) | 36.39 (299) |
| 5,000 random | **7.08** (2,250) | 33.51 (3,932) | 25.49 (308) | 57.47 (449) | 69.24 (319) |
| all markets | **9.22** (2,830) | 46.27 (5,321) | 22.48 (312) | 87.68 (471) | 107.41 (376) |

Against the market-first sort, the patch is faster or within 20 ms everywhere. It holds less
memory on every NRA and busiest-500 read but NRA B 24 h (+3%), on wide 24 h reads, and on 3 h of
all markets; the smallest reads hold a fixed 8–11 MiB of readers and merge against the sort's 0–12. It holds more on short wide reads of quiet markets (5,000 random 1 h / 3 h: +22% / +18%;
all markets 1 h: +41%): there each quiet market's range fits its least read-ahead, so both read
every row before the first goes out, and the patch adds its top-level merge's rows in flight.
Time-first holds ~0.3 GiB on every read and is fastest on all markets at 1–3 h; it is 2.4–8× slower
than the patch on wide 24 h reads.

### Table format: 8 KiB compressed blocks

The patch keeps a reader open per market while the merge waits for its rows' time, and an open
reader holds the decompressed block of every column it reads. With the default
`min_compress_block_size` (64 KiB) that is ~0.7 MiB per reader, ~3 GiB for 5,000 markets, whatever
the read-ahead. 8 KiB blocks cost 5–6% more disk (price_change day: 1.18 vs 1.11 GiB; universe 3 h:
840 vs 801 MiB) and leave the sort unchanged. Patch on both formats, final build, same day:

| workload | 64 KiB blocks | 8 KiB blocks | market-first sort |
|---|---|---|---|
| 5,000 random 24 h | 9.10 s (4,903 MiB) | 7.08 s (2,250 MiB) | 33.5–36.5 s (3,932 MiB) |
| all markets 24 h | 13.09 s (6,366) | 9.22 s (2,830) | 46.3–49.1 s (5,321) |
| NRA B 24 h | 5.98 s (1,697) | 5.16 s (1,147) | 9.5–9.7 s (1,116) |
| 500 busiest 24 h | 3.10 s (1,246) | 3.18 s (718) | 20.3–20.8 s (2,247) |
| 5,000 random 3 h | 1.35 s (665) | 1.36 s (636) | 5.1–5.9 s (540) |

### What each change bought (memory round, 2026-10-01)

5,000 random / 24 h unless noted; before: 8.5 s, 7.3 GiB on 64 KiB blocks.

| change | before → after |
|---|---|
| keys packed from the columns when the merge reaches a row, not stored (16 B/row) | 42 → 26 B per buffered row |
| PREWHERE's time column matched to the sort key by expression, not computed again | 34 → 26 B per buffered row (part of the line above) |
| file buffers capped at 16 KiB per column | 3.24 → 2.86 GiB (8 KiB blocks, 8,192-row read-ahead) |
| 8 KiB compressed blocks (table setting) | ≥ 4.4 GiB at any read-ahead → 2.0–2.3 GiB |
| read-ahead shared by bytes, by rows per reader; blocks of half a reader's share | busiest 500 24 h: 5.2 s at a flat 8,192 rows → 3.2 s, same memory |
| top-level read-ahead per group from the budget (≤ rows / 16), tasks of rows / 512, one waiting per worker | 5,000 random 1 h: 332 → 249 MiB |

Tried, no gain: tasks of a few marks each releasing the reader (memory follows the read-ahead, but
every task decompresses its blocks again: 13× blocks, 40 s at 2,048 rows); one byte budget for
the top level too (busiest 500 24 h 3.4 → 5.6 s: 8 groups with small read-ahead stop running in
parallel); 16 k-row tasks always (5,000 random 24 h 6.8 → 8.9 s).

### Where the memory goes now (5,000 random, 24 h, 2.25 GiB)

- Buffered rows: ~52 M rows × 26 B ≈ 1.3 GiB. The read-ahead floor (8,192 rows per reader, blocks of
  4,096) is most of it: a quiet market's block is hours of its rows.
- Readers: the rest, ~0.8 GiB for 3,668 open readers (an 8 KiB decompressed block and a ≤ 16 KiB
  file buffer per column).
- Top level: ~3 M rows (0.08 GiB) in flight.

Next for memory: smaller blocks for quiet readers with time pacing (read when the merge nears a
reader's last row, not when its rows run low), and merging quiet markets' readers that share
granules into one reader per stretch of the part.

## Status

The patch beats the market-first sort on time on every workload but the smallest (within 20 ms),
and on memory on every long or busy read; on short wide reads of quiet markets it holds up to
1.4× the sort. The time-sliced stock sort (5,000 random 24 h: 12.4 s, 4.3 GiB summed over 8 slices)
is slower and holds more than the patch on 8 KiB blocks (7.1 s, 2.25 GiB). On the 64 KiB-block table
the patch holds more than the sort on wide 24 h reads, so the memory gain needs the 8 KiB table
setting. Details of the time-sliced sort: markets-v2 `docs/replay-regions/measurements.md`.

## Next steps

1. Memory floor on wide 24 h reads (see above).
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
