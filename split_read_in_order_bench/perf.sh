# Streaming check on real data: 3 h of price_change in a market-first table.
set -uo pipefail
C="ip netns exec chtest /var/tmp/chtest/bin/clickhouse-client --user bench"
if [ "$($C -q "EXISTS TABLE pc" < /dev/null)" != 1 ]; then
  $C -q "CREATE TABLE pc (market_id UInt32, received_at DateTime64(6, 'UTC'), tie UInt32, price UInt16, size UInt32, side UInt8, best_bid Nullable(UInt16), best_ask Nullable(UInt16)) ENGINE = MergeTree ORDER BY (market_id, received_at, tie)" < /dev/null
  zstd -dc /var/tmp/chexport/price_change.native.zst | $C -q "INSERT INTO pc SELECT market_id, received_at, received_sequence, price, size, side, best_bid, best_ask FROM input('market_id UInt32, timestamp DateTime64(3, \'UTC\'), received_at DateTime64(6, \'UTC\'), received_sequence UInt32, price UInt16, size UInt32, side UInt8, best_bid Nullable(UInt16), best_ask Nullable(UInt16)') FORMAT Native"
  $C -q "OPTIMIZE TABLE pc FINAL" < /dev/null
fi
$C -q "SELECT count(), uniqExact(market_id) FROM pc" < /dev/null
$C -q "SELECT count() parts FROM system.parts WHERE table = 'pc' AND active" < /dev/null
run() {
  local name="$1" set="$2" split="$3" id="perf_${1}_${3}_$RANDOM"
  local start=$(date +%s.%N)
  $C --query_id "$id" -q "SELECT * FROM pc WHERE market_id IN $set ORDER BY received_at, tie FORMAT Null SETTINGS read_in_order_split_by_key_prefix_in = $split, max_execution_time = 0" < /dev/null
  local s=$(echo "$(date +%s.%N) - $start" | bc)
  $C -q "SYSTEM FLUSH LOGS query_log" < /dev/null
  echo "$name split=$split wall=${s}s $($C -q "SELECT 'rows=' || toString(read_rows), 'peak=' || formatReadableSize(memory_usage), 'first_block_ms?' FROM system.query_log WHERE query_id = '$id' AND type = 'QueryFinish'" < /dev/null)"
}
TOP10="(SELECT market_id FROM pc GROUP BY market_id ORDER BY count() DESC LIMIT 10)"
R2000="(SELECT DISTINCT market_id FROM pc WHERE cityHash64(market_id) % 20 = 0)"
ALL="(SELECT DISTINCT market_id FROM pc)"
for split in 0 1; do
  run top10 "$TOP10" $split
  run rand2000 "$R2000" $split
  run all "$ALL" $split
done
