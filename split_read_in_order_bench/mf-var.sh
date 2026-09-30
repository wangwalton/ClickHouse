# mf-var.sh <label> <span set> <settings> <sed on query>
set -uo pipefail
T="ip netns exec chtest /var/tmp/chtest/bin/clickhouse-client --user bench"
tag=mfv_$RANDOM
q=$(python3 /tmp/mf-query.py 1 60 | sed "$4")
$T -q "SYSTEM DROP UNCOMPRESSED CACHE; SYSTEM DROP MARK CACHE" </dev/null
start=$(date +%s.%N)
$T --log_comment $tag --max_execution_time 0 --external --file=/tmp/spans-$2.tsv --name=spans --structure='m UInt32, s Int64, e Int64' -q "$q $3" > /tmp/mf-out.bin
code=$?; wall=$(echo "$(date +%s.%N) - $start" | bc)
$T -q "SYSTEM FLUSH LOGS query_log" </dev/null
echo "$1 $2 exit=$code client_wall=${wall:0:6}s out=$(du -h /tmp/mf-out.bin | cut -f1) | $($T -q "SELECT 'server', round(query_duration_ms/1000,2), 's mem', formatReadableSize(memory_usage), 'spill_parts', ProfileEvents['ExternalSortWritePart'], 'spill_bytes', formatReadableSize(ProfileEvents['ExternalSortCompressedBytes'])  FROM system.query_log WHERE log_comment = '$tag' AND type IN ('QueryFinish', 'ExceptionWhileProcessing') FORMAT TSV" </dev/null | tr '\t' ' ')"
