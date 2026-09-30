# Streaming time-ordered reads over many key prefixes (`read_in_order_split_by_key_prefix_in`)

Experimental patch on v26.9.6.6-stable, 2026-09-30. Status: **parked**. It works and is measured,
but the workload that motivated it (NRA replay) turned out not to need it. This file records what
it does, why, what was measured, and what to try next.

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
   the part's primary index gives its granules (`[lower_bound − 1, upper_bound)`, intersected with
   the ranges the filter kept). One in-order reader per value, a filter that keeps only that value's
   rows, then:
   - `SplitMergeKeysTransform` (per value, parallel on the query threads): the merge key columns are
     packed into order-preserving 64-bit words (sign flip, descending flip), low 32 bits of the last
     word left for the input rank.
   - `SplitMergingTransform` (one): a tree of losers over an array of the current packed key of every
     input. Each input reads ahead `read_in_order_split_by_key_prefix_in_read_ahead_rows` (default
     16384, blocks of half that) so readers decompress in parallel while the merge runs; the merge
     prefetches each input's next key; ties break by input rank, so the split column is not compared
     when it is the last key. Output is a list of (buffer, row) records, not columns.
   - `SplitGatherTransform` (one, pipelined): copies the chosen rows into columns while the next
     block is merged.
3. Falls back to the normal plan for key types it cannot pack, more than 5 words of key, joins, FINAL,
   parallel replicas.

Correctness: `verify.sh` (28 on-vs-off md5 comparisons, incl. an expression in the key, ALIAS,
Nullable/String columns, subquery sets, window filters, key-first order); stateless tests matching
`in_order` (159 OK, 13 skipped, 0 failed) with the setting off and forced on.

## Results

Data: 3 h of `price_change`, 143.5M rows, 77,889 markets, 1 part. Same Rust consumer for every
approach, cold block cache, median of 3 (`fair.sh`, `summarize.py`, raw in `fair.tsv`).
Wall seconds; memory is ClickHouse's peak per query (the k-way figure is one of its many queries).

| set | full sort | patched | Rust k-way, gRPC, 1 connection | time-first table |
|---|---|---|---|---|
| rand100 | 0.10 | 0.08 | 0.12 | 1.8 |
| rand500 | 0.26 | 0.26 | 0.44 | 1.8 |
| top500 (55M rows) | 10.7 (1.4 GiB) | **5.2 (0.9 GiB)** | 6.9 (2.1 GiB summed) | 13.9 (0.4 GiB) |
| rand2000 | 0.85 | 1.05 | 1.97 | 1.9 |
| rand5000 | 2.2 | 3.0 | 4.1 | 2.1 |

- The patch beats the client k-way on every set, with 1 query instead of 500–5,000.
- It beats a full sort 2× on large reads (top500) with 35–58% less memory.
- It loses to a full sort at 2,000–5,000 small markets: one reader per market costs more to
  start than sorting 8.5M rows (first row 1.5 s vs 0.6 s).

How the merge got there (top500, merge thread only):

| version | ns/row | change |
|---|---|---|
| v2 | — (15 s wall) | `MergingSortedTransform` over per-value streams |
| v3 | ~105 | heap over cache-local 64 B entries, keys pre-converted to UInt64 |
| v4 | ~93 | key conversion moved to the parallel readers, gather on its own thread, split key dropped |
| v5 | ~66 | loser tree, winner key kept in registers, software prefetch of each input's next key, keys packed into 2 words |

The merge is single-threaded: about 66 ns/row, ~15M rows/s. Runs of one market in the merged
output average 1.36 rows, so batching runs does not help.

## Status: needs work, not dropped

The replay (markets-v2 `docs/replay-regions`) ships the stock full sort for now, one query per day.
This patch is still the direction: it is the one approach that streams every workload measured
(one market over a long range, sparse regions over many markets, many markets over one range)
without a chunk size, with memory per market instead of per row, and lets the consumer work while
ClickHouse reads instead of after it sorts. It needs steps 1–2 below before it can replace the sort.

## Why the full sort ships first

The target workload is NRA's replay regions: per day ~3,600 markets, each with many ~1 s windows
(see `nra-windows.sql`, a no-fee superset of NRA's windows, 10 s per day). That reads 2.4–8M rows a
day. A stock full sort of the region rows handles it (`mf-*.sh`): one day of the market-first
layout took 5 s and 1.1 GiB; all windows (not only ≤ 1 s ones) 15 s and 2.6–3.4 GiB.
The patch pays off only for many markets over long windows, which no current strategy needs, and
a fork costs a rebuild and rebase on every upgrade.

## Next steps

1. **Spans**: accept per-market `(from, to)` intervals, not one global range. The per-value granule
   ranges already come from the index; narrow them per span on the second key column and drop edge
   rows in the per-value filter. Needs a filter form the planner can see before reading, like a set.
2. **Lazy readers**: open an input when the merge time reaches its first span start, close it after
   its last. Memory, startup and tree size then follow the peak number of *active* markets, and the
   first row no longer waits for every market's first chunk (the rand2000/5000 loss).
3. **Two-level merge**: groups of ~512 inputs merged on their own threads with cache-resident
   trees, then a small top merge. Needed at tens of thousands of active inputs (16 tree levels,
   800 KB of keys).
4. **Several tables**: keep one patched query per table and merge the 5–7 streams on the client,
   rather than merging inside `UNION ALL`.
5. **Upstream** rather than carry a fork.

## Alternatives worth testing first

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

- `setup.sh`: CI fast-test configs, a network namespace `chtest`, the server. `runtests.sh <pattern>`: stateless tests.
- `verify.sh`: on-vs-off correctness. `perf.sh`: loads the 3 h `pc` table. `q.sh`, `prof.sh`, `perf.sh`: timing and profiles.
- `fair.sh` + `summarize.py`: the comparison above; clients `rs/` (Rust, HTTP), `rsg/` (Rust, gRPC, one connection), `bench.mjs` (Node).
- `markets/`: market id sets. `nra-windows.sql`: NRA region query. `mf-load.sh`, `mf-query.py`, `mf-run.sh`, `mf-var.sh`: the one-query replay eval on the market-first layout.
