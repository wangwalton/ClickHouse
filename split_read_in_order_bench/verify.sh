# Correctness: same rows in the same order with read_in_order_split_by_key_prefix_in on and off.
set -uo pipefail
C="ip netns exec chtest /var/tmp/chtest/bin/clickhouse-client"
$C -q "DROP TABLE IF EXISTS t" < /dev/null
$C -q "CREATE TABLE t (m UInt32, ts DateTime64(6), tie UInt32, v UInt64) ENGINE = MergeTree ORDER BY (m, ts, tie) SETTINGS index_granularity = 64" < /dev/null
$C -q "SYSTEM STOP MERGES t" < /dev/null
# 6 parts; market activity is skewed (m = floor(1000 * rand^3)); tie is unique so the order is total.
for p in 0 1 2 3 4 5; do
  $C -q "INSERT INTO t SELECT toUInt32(floor(1000 * pow(randCanonical(), 3))), toDateTime64('2026-09-24 12:00:00', 6) + toIntervalMicrosecond(rand64() % 3600000000), $p * 1000000 + number, rand64() FROM numbers(200000)" < /dev/null
done
$C -q "SELECT count(), uniqExact(m), count() FROM system.parts WHERE table = 't' AND active" < /dev/null
fail=0
check() {
  local name="$1" q="$2"
  a=$($C -q "$q SETTINGS read_in_order_split_by_key_prefix_in = 1" < /dev/null 2>&1 | md5sum)
  b=$($C -q "$q SETTINGS read_in_order_split_by_key_prefix_in = 0" < /dev/null 2>&1 | md5sum)
  plan=$($C -q "EXPLAIN PIPELINE $q SETTINGS read_in_order_split_by_key_prefix_in = 1" < /dev/null 2>&1 | grep -oE 'MergeSortingTransform|MergingSortedTransform|KeyPrefixValueFilterTransform|SplitMergingTransform|FinishSortingTransform' | sort | uniq -c | tr '\n' ' ')
  rows=$($C -q "SELECT count() FROM ($q)" < /dev/null 2>&1)
  if [ "$a" = "$b" ]; then echo "ok   $name rows=$rows plan: $plan"; else echo "FAIL $name rows=$rows plan: $plan"; fail=1; fi
}
S1="(7)"; S2="(3, 500)"; S10="(0,1,2,3,5,8,13,21,34,55)"
S300="(SELECT DISTINCT m FROM t WHERE m % 3 = 0)"
check one        "SELECT m, ts, tie, v FROM t WHERE m IN $S1 ORDER BY ts, tie"
check two        "SELECT m, ts, tie, v FROM t WHERE m IN $S2 ORDER BY ts, tie"
check ten        "SELECT m, ts, tie, v FROM t WHERE m IN $S10 ORDER BY ts, tie"
check subquery   "SELECT m, ts, tie, v FROM t WHERE m IN $S300 ORDER BY ts, tie"
check all        "SELECT m, ts, tie, v FROM t WHERE m IN (SELECT number FROM numbers(1000)) ORDER BY ts, tie"
check missing    "SELECT m, ts, tie, v FROM t WHERE m IN (999999, 3, 4000000) ORDER BY ts, tie"
check no_m_col   "SELECT ts, tie, v FROM t WHERE m IN $S10 ORDER BY ts, tie"
check window     "SELECT m, ts, tie FROM t WHERE m IN $S10 AND ts BETWEEN '2026-09-24 12:10:00' AND '2026-09-24 12:20:00' ORDER BY ts, tie"
check or_windows "SELECT m, ts, tie FROM t WHERE m IN $S10 AND ((m = 0 AND ts < '2026-09-24 12:05:00') OR (m = 5 AND ts > '2026-09-24 12:50:00') OR m > 5) ORDER BY ts, tie"
check limit      "SELECT m, ts, tie FROM t WHERE m IN $S10 ORDER BY ts, tie LIMIT 1000"
check desc       "SELECT m, ts, tie FROM t WHERE m IN $S10 ORDER BY ts DESC, tie DESC"
check m_first    "SELECT m, ts, tie FROM t WHERE m IN $S10 ORDER BY m, ts, tie"
check ts_only    "SELECT m, ts, tie FROM t WHERE m IN $S10 ORDER BY ts, tie, v"
check no_prewhere "SELECT m, ts, tie FROM t WHERE m IN $S10 ORDER BY ts, tie SETTINGS optimize_move_to_prewhere = 0"
check threads1   "SELECT m, ts, tie FROM t WHERE m IN $S10 ORDER BY ts, tie SETTINGS max_threads = 1"
check agg_after  "SELECT m, count() FROM (SELECT m, ts, tie FROM t WHERE m IN $S10 ORDER BY ts, tie) GROUP BY m ORDER BY m"
check union      "SELECT m, ts, tie FROM (SELECT m, ts, tie FROM t WHERE m IN $S2 UNION ALL SELECT m, ts, tie FROM t WHERE m IN (9, 10)) ORDER BY ts, tie"
check eq_single  "SELECT m, ts, tie FROM t WHERE m = 7 ORDER BY ts, tie"
check m_last     "SELECT m, ts, tie, v FROM t WHERE m IN $S10 ORDER BY ts, tie, m"
check m_last_desc "SELECT m, ts, tie FROM t WHERE m IN $S10 ORDER BY ts DESC, tie DESC, m DESC"
check m_desc_mix "SELECT m, ts, tie FROM t WHERE m IN $S10 ORDER BY ts, tie, m DESC"
check ra_1       "SELECT m, ts, tie, v FROM t WHERE m IN $S300 ORDER BY ts, tie, m SETTINGS read_in_order_split_by_key_prefix_in_read_ahead_rows = 1"
check ra_100     "SELECT m, ts, tie, v FROM t WHERE m IN $S300 ORDER BY ts, tie, m SETTINGS read_in_order_split_by_key_prefix_in_read_ahead_rows = 100, max_block_size = 777"
check ts_minute  "SELECT m, ts, tie FROM t WHERE m IN $S10 ORDER BY toStartOfMinute(ts), m, ts, tie"
# Production-like: key with an ALIAS expression, Nullable and String columns.
$C -q "DROP TABLE IF EXISTS t2" < /dev/null
$C -q "CREATE TABLE t2 (m UInt32, timestamp DateTime64(3, 'UTC'), lag UInt16, rx DateTime64(6, 'UTC') ALIAS timestamp + toIntervalMicrosecond(lag * 50), tie UInt32, bid Nullable(UInt16), s String) ENGINE = MergeTree ORDER BY (m, timestamp + toIntervalMicrosecond(lag * 50), tie) SETTINGS index_granularity = 128" < /dev/null
for p in 0 1 2; do
  $C -q "INSERT INTO t2 SELECT toUInt32(floor(300 * pow(randCanonical(), 3))), toDateTime64('2026-09-24 12:00:00', 3) + toIntervalMillisecond(rand() % 3600000), rand() % 1000, $p * 1000000 + number, if(rand() % 3 = 0, NULL, rand() % 1000), toString(rand() % 100) FROM numbers(100000)" < /dev/null
done
check t2_rx      "SELECT m, rx, tie, bid, s FROM t2 WHERE m IN (0,1,2,3,50,100,299) ORDER BY rx, tie, m"
check t2_rx_only "SELECT rx, tie, bid FROM t2 WHERE m IN (0,1,2,3,50,100,299) ORDER BY rx, tie"
check t2_sub     "SELECT m, rx, tie, s FROM t2 WHERE m IN (SELECT number FROM numbers(300) WHERE number % 2 = 1) ORDER BY rx, tie, m"
$C -q "SYSTEM START MERGES t" < /dev/null; $C -q "OPTIMIZE TABLE t FINAL" < /dev/null
check one_part   "SELECT m, ts, tie, v FROM t WHERE m IN $S10 ORDER BY ts, tie"
exit $fail
