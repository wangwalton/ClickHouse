# gRPC single-connection k-way benchmark; prints client result, server stats, and max TCP connections seen.
T=/var/tmp/chtest
C="ip netns exec chtest $T/bin/clickhouse-client --user bench"
for set in ${SETS:-rand10 rand100 rand500 top500}; do
  tag=g_${set}_$RANDOM
  touch $T/sampling
  ( peak=0; mem=0; while [ -e $T/sampling ]; do
      n=$(ip netns exec chtest ss -tn state established '( dport = :9100 )' | tail -n +2 | wc -l); [ $n -gt $peak ] && peak=$n
      m=$($C -q "SELECT sum(memory_usage) FROM system.processes WHERE Settings['log_comment'] = '$tag'" </dev/null 2>/dev/null); [ "${m:-0}" -gt $mem ] && mem=$m
      sleep 0.2; done; echo "$peak $mem" > $T/grpc-peak ) &
  out=$(TAG=$tag STREAM_WINDOW=${STREAM_WINDOW:-1048576} ip netns exec chtest $T/rsg/target/release/benchg $T/markets/$set 2>&1 | tail -2)
  rm -f $T/sampling; wait
  $C -q "SYSTEM FLUSH LOGS query_log" </dev/null
  server=$($C -q "SELECT count(), formatReadableSize(max(memory_usage)), round(max(query_duration_ms)/1000, 2) FROM system.query_log WHERE log_comment = '$tag' AND type = 'QueryFinish'" </dev/null | tr '\t' ' ')
  read conns mem < $T/grpc-peak
  echo "grpc $set $out server[queries maxmem max_s]=$server tcp_conns=$conns sampled_peak=$(numfmt --to=iec $mem)"
done
