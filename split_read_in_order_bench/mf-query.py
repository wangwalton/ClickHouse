# Builds the one-query replay over mf.* for a span file. argv: prune(0|1) bucket_s
import sys
prune, bucket = sys.argv[1] == '1', int(sys.argv[2])
B = bucket * 1_000_000
tables = {  # bus index, columns in the row
  'price_change': (0, 'price, size, side, best_bid, best_ask'),
  'book': (1, 'last_trade_price, bid_prices, bid_sizes, ask_prices, ask_sizes'),
  'best_bid_ask': (2, 'best_bid, best_ask, spread'),
  'last_trade_price': (3, 'price, size, side, was_normalized, transaction_hash'),
  'initial_book_dump': (4, 'last_trade_price, bid_prices, bid_sizes, ask_prices, ask_sizes, tick_size'),
  'tick_size_change': (5, 'payload'),
}
rx = 'toUnixTimestamp64Micro(x.received_at)'
buckets = (f"AND (x.market_id, intDiv({rx}, {B})) IN (SELECT m, arrayJoin(range(intDiv(s, {B}), intDiv(e - 1, {B}) + 1)) FROM spans) " if prune else '')
branches = []
for t, (bus, cols) in tables.items():
    extra = ' AND NOT x.snapshot' if t == 'initial_book_dump' else ''
    branches.append(
        f"SELECT 1 AS phase, {bus} AS bus, {rx} AS rx_, x.tie AS seq_, x.market_id AS market_, formatRow('RowBinary', x.market_id, x.timestamp, x.lag_50us, x.tie, {', '.join('x.' + c.strip() for c in cols.split(','))}) AS row "
        f"FROM mf.polymarket_ws_{t} AS x ASOF INNER JOIN spans ON x.market_id = spans.m AND {rx} >= spans.s "
        f"WHERE x.market_id IN (SELECT m FROM spans) AND x.received_at >= '2026-09-24 00:00:00' AND x.received_at < '2026-09-25 00:00:05' "
        f"AND {rx} < spans.e {buckets}{extra}")
seed_cols = tables['initial_book_dump'][1]
branches.append(
    f"SELECT 0, 4, toUnixTimestamp64Milli(x.timestamp) * 1000, 0, x.market_id, formatRow('RowBinary', x.market_id, x.timestamp, x.lag_50us, x.tie, {', '.join('x.' + c.strip() for c in seed_cols.split(','))}) "
    f"FROM mf.polymarket_ws_initial_book_dump AS x "
    f"WHERE x.snapshot AND x.market_id IN (SELECT m FROM spans) AND x.received_at >= '2026-09-23 23:40:00' AND x.received_at < '2026-09-25 00:01:00' "
    f"AND (x.market_id, toUnixTimestamp64Milli(x.timestamp) * 1000) IN (SELECT m, s FROM spans) "
    f"LIMIT 1 BY x.market_id, x.timestamp")
print(f"SELECT phase, bus, row FROM ({' UNION ALL '.join(branches)}) ORDER BY rx_, phase, seq_, bus, market_")
