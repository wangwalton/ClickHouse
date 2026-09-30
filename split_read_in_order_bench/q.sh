# q.sh <name> <market set SQL> <extra settings>: runs the ordered read once and prints query_log stats.
C="ip netns exec chtest /var/tmp/chtest/bin/clickhouse-client --user bench"
id=q_$1_$RANDOM
$C -q "SYSTEM DROP UNCOMPRESSED CACHE" </dev/null
$C --query_id $id -q "SELECT * FROM pc WHERE market_id IN $2 ORDER BY received_at, tie FORMAT Null SETTINGS $3" </dev/null 2>&1 | head -3
$C -q "SYSTEM FLUSH LOGS query_log" </dev/null
$C -q "SELECT '$1', round(query_duration_ms/1000,2) s, formatReadableSize(memory_usage) mem, formatReadableSize(ProfileEvents['CompressedReadBufferBytes']) decompressed, ProfileEvents['UncompressedCacheHits'] hits, ProfileEvents['UncompressedCacheMisses'] misses, round(ProfileEvents['OSCPUVirtualTimeMicroseconds']/1e6,1) cpu_s FROM system.query_log WHERE query_id='$id' AND type IN ('QueryFinish','ExceptionWhileProcessing') FORMAT TSV" </dev/null
