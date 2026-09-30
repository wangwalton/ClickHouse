# mf-run.sh <span set> <prune 0|1> <bucket_s> <format: Null|RowBinary>
set -uo pipefail
T="ip netns exec chtest /var/tmp/chtest/bin/clickhouse-client --user bench"
tag=mf_$1_p$2_$3_$4_$RANDOM
q=$(python3 /tmp/mf-query.py $2 $3)
$T -q "SYSTEM DROP UNCOMPRESSED CACHE; SYSTEM DROP MARK CACHE" </dev/null
start=$(date +%s.%N)
$T --log_comment $tag --max_execution_time 0 --external --file=/tmp/spans-$1.tsv --name=spans --structure='m UInt32, s Int64, e Int64' -q "$q FORMAT $4" > /tmp/mf-out.bin
code=$?
wall=$(echo "$(date +%s.%N) - $start" | bc)
$T -q "SYSTEM FLUSH LOGS query_log" </dev/null
echo "$1 prune=$2 bucket=$3 $4 exit=$code client_wall=${wall:0:6}s out=$(du -h /tmp/mf-out.bin | cut -f1) | $($T -q "SELECT 'server', round(query_duration_ms/1000,2), 's mem', formatReadableSize(memory_usage), 'read', read_rows, formatReadableSize(read_bytes), 'result_rows', result_rows, 'spill', formatReadableSize(ProfileEvents['ExternalSortWritePart'])  FROM system.query_log WHERE log_comment = '$tag' AND type IN ('QueryFinish', 'ExceptionWhileProcessing') FORMAT TSV" </dev/null | tr '\t' ' ')"
