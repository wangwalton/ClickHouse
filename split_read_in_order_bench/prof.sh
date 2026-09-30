# prof.sh <market set file> <settings>: CPU profile per thread of the ordered read, top functions of the busiest threads.
C="ip netns exec chtest /var/tmp/chtest/bin/clickhouse-client --user bench"
id=prof_$RANDOM
list=$(paste -sd, $1)
$C -q "SYSTEM DROP UNCOMPRESSED CACHE" </dev/null
$C --query_id $id -q "SELECT market_id, received_at, tie, price, size, side FROM pc WHERE market_id IN ($list) ORDER BY received_at, tie, market_id FORMAT RowBinary SETTINGS $2, query_profiler_cpu_time_period_ns = 2000000, query_profiler_real_time_period_ns = 0" </dev/null > /dev/null
$C -q "SYSTEM FLUSH LOGS" </dev/null
$C -q "SELECT round(query_duration_ms/1000,2) s, formatReadableSize(memory_usage), round(ProfileEvents['OSCPUVirtualTimeMicroseconds']/1e6,1) cpu_s FROM system.query_log WHERE query_id='$id' AND type='QueryFinish' FORMAT TSV" </dev/null
echo '--- samples per thread (2ms CPU each)'
$C -q "SELECT thread_id, count() c FROM system.trace_log WHERE query_id='$id' AND trace_type='CPU' GROUP BY thread_id ORDER BY c DESC LIMIT 6 FORMAT TSV" </dev/null
top=$($C -q "SELECT thread_id FROM system.trace_log WHERE query_id='$id' AND trace_type='CPU' GROUP BY thread_id ORDER BY count() DESC LIMIT 1" </dev/null)
echo "--- busiest thread $top: processor on stack (inclusive)"
$C -q "WITH arrayMap(a -> demangle(addressToSymbol(a)), trace) AS st SELECT count() c, arrayFirst(s -> s LIKE '%::work()%' OR s LIKE '%::generate()%' OR s LIKE '%::transform(%' OR s LIKE '%::consume(%' OR s LIKE '%Format%::write%' OR s LIKE '%::merge()%', st) f FROM system.trace_log WHERE query_id='$id' AND trace_type='CPU' AND thread_id=$top GROUP BY f ORDER BY c DESC LIMIT 8 SETTINGS allow_introspection_functions=1 FORMAT TSV" </dev/null | cut -c1-200
echo "--- busiest thread $top: self functions"
$C -q "SELECT count() c, demangle(addressToSymbol(trace[1])) f FROM system.trace_log WHERE query_id='$id' AND trace_type='CPU' AND thread_id=$top GROUP BY f ORDER BY c DESC LIMIT 14 SETTINGS allow_introspection_functions=1 FORMAT TSV" </dev/null | cut -c1-200
echo '--- all threads: processor on stack'
$C -q "WITH arrayMap(a -> demangle(addressToSymbol(a)), trace) AS st SELECT count() c, arrayFirst(s -> s LIKE '%::work()%' OR s LIKE '%::generate()%' OR s LIKE '%::transform(%' OR s LIKE '%::consume(%' OR s LIKE '%Format%::write%' OR s LIKE '%::merge()%', st) f FROM system.trace_log WHERE query_id='$id' AND trace_type='CPU' GROUP BY f ORDER BY c DESC LIMIT 10 SETTINGS allow_introspection_functions=1 FORMAT TSV" </dev/null | cut -c1-200
