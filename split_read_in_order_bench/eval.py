# Workloads × query patterns on price_change in the throwaway server (netns chtest), 2026-09-24.
# Copy of markets-v2 adhoc/replay-regions/layout-eval.py, plus: span workloads prune granules with the
# (market, 10 s bucket) IN filter for every pattern, median of RUNS runs, time to first row (RowBinary,
# client), and an md5 of the output (FIRST_ROW=0 / MD5=0 to skip).
# Usage: python3 eval.py <subset|universe> <hours> <start hh> [out.tsv] [workloads,...] [patterns,...]
#   universe: 12:00–15:00, every market (pcu_*); subset: the whole day, set B's markets only (20% of rows).
# Every cell: FORMAT Null, cold ClickHouse caches (mark + uncompressed), 10 GiB memory cap, no uncompressed cache (prod's default;
# the test server's CI config turns it on).
import subprocess, sys, time, random, os, hashlib, statistics

T = ['ip', 'netns', 'exec', 'chtest', '/var/tmp/chtest/bin/clickhouse-client', '--user', 'bench']
data, hours, start_hour = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
out = open(sys.argv[4] if len(sys.argv) > 4 else f'/tmp/layout-eval-{data}-{hours}h.tsv', 'a')
t0 = f'2026-09-24 {start_hour:02d}:00:00'
t1 = f"toDateTime64('{t0}', 6, 'UTC') + INTERVAL {hours} HOUR"
small_t0 = f"toDateTime64('{t0}', 6, 'UTC') + INTERVAL 30 MINUTE"

def spans(name):
    # Only spans overlapping the read range: the span tables hold the whole day.
    lo_us, hi_us = f"toUnixTimestamp64Micro(toDateTime64('{t0}', 6, 'UTC'))", f"toUnixTimestamp64Micro({t1})"
    within = f"FROM mf.spans_{name} WHERE e > {lo_us} AND s < {hi_us}"
    buckets = (f"(market_id, toStartOfInterval(received_at, INTERVAL 10 SECOND)) IN (SELECT m, arrayJoin(arrayMap(i -> "
               f"fromUnixTimestamp64Micro(i * 10000000), range(intDiv(greatest(s, {lo_us}), 10000000), intDiv(least(e, {hi_us}) - 1, 10000000) + 1))) {within})")
    return (f"(SELECT m {within})", buckets + f" AND dictGetOrDefault('mf.spans_{name}_d', 'one', toUInt64(market_id), toUnixTimestamp64Micro(received_at), 0) = 1")

# name → (market set table, time range, extra filter)
workloads = {
    'one_market_wide': ('mf.set_top1', (f"'{t0}'", t1), ''),
    'one_market_1min': ('mf.set_top1', (small_t0, f'{small_t0} + INTERVAL 1 MINUTE'), ''),
    'few_markets_wide': ('mf.set_rand10', (f"'{t0}'", t1), ''),
    'nra_A_sparse': (spans('A_min')[0], (f"'{t0}'", t1), spans('A_min')[1]),
    'nra_B_mixed': (spans('B_min')[0], (f"'{t0}'", t1), spans('B_min')[1]),
    'top500_wide': ('mf.set_top500', (f"'{t0}'", t1), ''),
    'rand5000_wide': ('mf.set_rand5000', (f"'{t0}'", t1), ''),
    'all_wide': ('mf.set_allu' if data == 'universe' else 'mf.set_all', (f"'{t0}'", t1), ''),
}
order = 'received_at, tie, market_id'
tables = {'universe': ('mf.pcu_mf', 'mf.pcu_tf', 'mf.pcu_mb1', 'mf.pcu_mb10'),
          'subset': ('mf.polymarket_ws_price_change', 'mf.pc_tf', 'mf.pc_mb1', 'mf.pc_mb10')}[data]
b1 = 'toStartOfMinute(received_at)'
b10 = 'toStartOfInterval(received_at, INTERVAL 10 MINUTE)'
patterns = {  # name → (table, ORDER BY, settings, bucket expression for pruning)
    'market_first_sort': (tables[0], order, '', None),
    'market_first_patch': (tables[0], order, ', read_in_order_split_by_key_prefix_in = 1', None),
    'time_first': (tables[1], order, '', None),
    'bucket_1min': (tables[2], f'{b1}, {order}', '', (b1, 'toStartOfMinute')),
    'bucket_10min': (tables[3], f'{b10}, {order}', '', (b10, 'toStartOfTenMinutes')),
}
only = set(sys.argv[5].split(',')) if len(sys.argv) > 5 and sys.argv[5] else None
only_patterns = set(sys.argv[6].split(',')) if len(sys.argv) > 6 else None
RUNS = int(os.environ.get('RUNS', 3))
FIRST_ROW = os.environ.get('FIRST_ROW', '1') == '1'
MD5 = os.environ.get('MD5', '1') == '1'

def drop_caches():
    ch('SYSTEM DROP UNCOMPRESSED CACHE; SYSTEM DROP MARK CACHE', '--multiquery')

def first_row_s(q):
    drop_caches()
    p = subprocess.Popen(T + ['-q', q.replace('FORMAT Null', 'FORMAT RowBinary')], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    start = time.time()
    p.stdout.read(1)
    elapsed = time.time() - start
    p.kill(); p.wait()
    return elapsed

def output_md5(q):
    p = subprocess.Popen(T + ['-q', q.replace('FORMAT Null', 'FORMAT RowBinary')], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    h = hashlib.md5()
    for chunk in iter(lambda: p.stdout.read(1 << 20), b''):
        h.update(chunk)
    p.wait()
    return h.hexdigest()[:10]

def ch(query, *args):
    return subprocess.run(T + list(args) + ['-q', query], capture_output=True, text=True)

for w, (market_set, (lo, hi), extra) in workloads.items():
    if only and w not in only:
        continue
    for p, (table, order_by, settings, bucket) in patterns.items():
        if only_patterns and p not in only_patterns:
            continue
        tag = f'le_{hours}h_{w}_{p}_{random.randint(0, 1 << 30)}'
        market_query = market_set if market_set.startswith('(') else f'(SELECT m FROM {market_set})'
        where = f"market_id IN {market_query} AND received_at >= {lo} AND received_at < {hi}" + (f' AND {extra}' if extra else '')
        if bucket:  # the index only prunes on the bucket column itself
            where += f" AND {bucket[0]} >= {bucket[1]}(toDateTime64({lo}, 6, 'UTC')) AND {bucket[0]} < {hi}"
        q = (f"SELECT market_id, received_at, tie, price, size, side, best_bid, best_ask FROM {table} WHERE {where} "
             f"ORDER BY {order_by} FORMAT Null SETTINGS max_memory_usage = 10737418240, max_execution_time = 900, use_uncompressed_cache = 0, log_comment = '{tag}'{settings}")
        results = []
        for run in range(RUNS):
            run_tag = f'{tag}_{run}'
            drop_caches()
            wall = time.time()
            r = ch(q.replace(tag, run_tag))
            wall = time.time() - wall
            ch('SYSTEM FLUSH LOGS query_log')
            stats = ch(f"SELECT query_duration_ms / 1000, memory_usage / 1048576, read_rows, result_rows, exception_code "
                       f"FROM system.query_log WHERE log_comment = '{run_tag}' AND type != 'QueryStart' FORMAT TSV").stdout.split()
            err = '' if r.returncode == 0 else r.stderr.strip().splitlines()[0][:120]
            results.append((wall, stats, err))
            if err:
                break
        med = lambda i: statistics.median(float(x[1][i]) for x in results) if results[0][1] else 0
        err = results[-1][2]
        first = f'{first_row_s(q):.2f}' if FIRST_ROW and not err else ''
        digest = output_md5(q) if MD5 and not err else ''
        stats = results[0][1]
        line = '\t'.join([data, str(hours), w, p, f'{statistics.median(x[0] for x in results):.2f}',
                          f'{med(0):.2f}', f'{med(1):.0f}', stats[2] if stats else '', stats[3] if stats else '',
                          stats[4] if stats else '', first, digest, err])
        print(line, flush=True)
        out.write(line + '\n')
        out.flush()
