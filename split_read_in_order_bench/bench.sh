# Patched single query vs per-market queries + Node k-way merge, on table pc (3 h of price_change).
set -uo pipefail
T=/var/tmp/chtest
C="ip netns exec chtest $T/bin/clickhouse-client --user bench"
q() { $C -q "$1" < /dev/null; }
mkdir -p $T/markets
[ -f $T/markets/rand10 ] || {
  for n in 10 100 500; do q "SELECT market_id FROM (SELECT DISTINCT market_id FROM pc) ORDER BY cityHash64(market_id) LIMIT $n" > $T/markets/rand$n; done
  q "SELECT market_id FROM pc GROUP BY market_id ORDER BY count() DESC LIMIT 500" > $T/markets/top500
  q "SELECT DISTINCT market_id FROM pc ORDER BY market_id" > $T/markets/all
}
# Peak of summed memory of the tagged queries running on the server, sampled every 0.2 s.
sample() {
  local tag=$1 peak=0 m
  while [ -e $T/sampling ]; do
    m=$(q "SELECT sum(memory_usage) FROM system.processes WHERE Settings['log_comment'] = '$tag'" 2>/dev/null || echo 0)
    [ "${m:-0}" -gt "$peak" ] && peak=$m
    sleep 0.2
  done
  echo $peak > $T/peak
}
bench() {
  local mode=$1 set=$2 tag="b_${CLIENT:-node}_${1}_${2}_$RANDOM"
  touch $T/sampling; sample $tag & local sp=$!
  local cmd="node $T/bench.mjs"; [ "${CLIENT:-node}" = rust ] && cmd=$T/rs/target/release/bench
  local out=$(SPLIT=${SPLIT:-1} TAG=$tag ip netns exec chtest $cmd $mode $T/markets/$set 2>&1 | tail -1)
  rm -f $T/sampling; wait $sp
  q "SYSTEM FLUSH LOGS query_log"
  local server=$(q "SELECT count(), formatReadableSize(max(memory_usage)), formatReadableSize(sum(memory_usage)), round(max(query_duration_ms) / 1000, 2) FROM system.query_log WHERE log_comment = '$tag' AND type = 'QueryFinish'" | tr '\t' ' ')
  echo "${CLIENT:-node} $mode split=${SPLIT:-1} $set $out server[queries maxmem summem max_s]=$server sampled_peak=$(numfmt --to=iec $(cat $T/peak))"
}
for set in ${SETS:-rand10 rand100 rand500 top500}; do
  bench single $set
  bench kway $set
done

