import csv, statistics, sys, collections
rows = [r for r in csv.reader(open('/var/tmp/chtest/fair.tsv'), delimiter='\t') if len(r) >= 10]
g = collections.defaultdict(list)
hashes = collections.defaultdict(set)
for s, a, t, run, n, h, wall, first, peak, conns, ql in rows:
    g[(s, a, t)].append((float(wall), float(first), int(peak), int(conns), [int(x) for x in ql.split()]))
    hashes[s].add((n, h))
mib = lambda b: f'{b / 2**20:.0f}'
order = ['market_first_full_sort', 'market_first_patched', 'market_first_kway', 'time_first_stock']
for s in dict.fromkeys(k[0] for k in g):
    print(f'\n{s}: rows/hash across all runs: {sorted(hashes[s])}')
    print('approach\ttransport\twall_s\tfirst_row_ms\tserver_peak_MiB(sampled)\tmax_query_MiB\tqueries\ttcp_conns')
    for a in order:
        for t in ['http', 'grpc']:
            v = g.get((s, a, t))
            if not v: continue
            med = lambda i: statistics.median(x[i] for x in v)
            q = v[0][4]
            print(f'{a}\t{t}\t{med(0):.2f}\t{med(1):.0f}\t{mib(med(2))}\t{mib(statistics.median(x[4][1] for x in v))}\t{q[0]}\t{max(x[3] for x in v)}')
