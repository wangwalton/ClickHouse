# Load 2026-09-24 of the set B markets into the throwaway server (netns chtest), market-first layout of PR #364.
set -euo pipefail
W=/root/germany-hetzner-32gb/markets-v2/.wt/ch-storage-format
T="ip netns exec chtest /var/tmp/chtest/bin/clickhouse-client --user bench"
P="clickhouse-client -d markets_v2"
$T -q "CREATE DATABASE IF NOT EXISTS mf"
# DDL: markets_v2 → mf, no TTL / cold volume.
awk '/^CREATE TABLE markets_v2\.polymarket_ws_/,/;$/' $W/chkit/migrations/markets_v2/20260926053618_storage_format.sql \
  | sed -e 's/markets_v2\./mf./' -e '/^TTL /d' -e 's/, materialize_ttl_recalculate_only = 1, storage_policy = .hot_cold.//' > /tmp/mf-ddl.sql
$T --multiquery < /tmp/mf-ddl.sql
cut -f1 /tmp/spans-B_min.tsv | sort -un > /tmp/mf-markets.txt
markets=$(paste -sd, /tmp/mf-markets.txt)
cols() { $T -q "SELECT name FROM system.columns WHERE database='mf' AND table='$1' AND default_kind != 'ALIAS' ORDER BY position FORMAT TSV" | paste -sd, ; }
expr() {  # target column → expression over the old-format row
  case $1 in
    lag_50us) echo "intDiv(toUnixTimestamp64Micro(received_at), 50) - toUnixTimestamp64Milli(timestamp) * 20 AS lag_50us" ;;
    tie) echo "toUInt8(received_sequence % 256) AS tie" ;;
    price|best_bid|best_ask|last_trade_price|tick_size) echo "$1 * 10 AS $1" ;;
    bid_prices|ask_prices) echo "arrayMap(p -> p * 10, $1) AS $1" ;;
    *) echo "$1" ;;
  esac
}
for t in price_change book best_bid_ask last_trade_price initial_book_dump tick_size_change new_market market_resolved; do
  c=$(cols polymarket_ws_$t)
  sel=$(for x in ${c//,/ }; do expr $x; done | paste -sd, | sed 's/,/, /g')
  lo="'2026-09-24 00:00:00'"; [ $t = initial_book_dump ] && lo="'2026-09-23 23:40:00'"
  start=$(date +%s)
  $P -q "SELECT $sel FROM polymarket_ws_$t WHERE received_at >= $lo AND received_at < '2026-09-25 00:00:05' AND market_id IN ($markets) FORMAT Native" \
    | $T -q "INSERT INTO mf.polymarket_ws_$t ($c) FORMAT Native"
  echo "$t: $($T -q "SELECT count(), formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE database='mf' AND table='polymarket_ws_$t' AND active FORMAT TSV") in $(( $(date +%s) - start )) s"
done
df -h / | tail -1
