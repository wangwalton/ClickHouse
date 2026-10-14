# Fair comparison: same Rust merge/consumer code, same table data, both transports, cold block cache, 3 runs.
# Prints TSV: set approach transport run rows hash wall_s first_row_ms sampled_server_peak tcp_conns "queries max_query_mem sum_query_mem"
T=/var/tmp/chtest
C="ip netns exec chtest $T/bin/clickhouse-client --user bench"
one() {  # set approach transport run  (env MODE TABLE SPLIT set by caller)
  local set=$1 approach=$2 transport=$3 run=$4 tag=f_${2}_${3}_$RANDOM
  $C -q "SYSTEM DROP UNCOMPRESSED CACHE" </dev/null
  touch $T/sampling
  ( peak=0; conns=0; while [ -e $T/sampling ]; do
      m=$($C -q "SELECT sum(memory_usage) FROM system.processes WHERE Settings['log_comment'] = '$tag'" </dev/null 2>/dev/null); [ "${m:-0}" -gt $peak ] && peak=$m
      n=$(ip netns exec chtest ss -tn state established '( dport = :8123 or dport = :9100 )' | tail -n +2 | wc -l); [ $n -gt $conns ] && conns=$n
      sleep 0.1; done; echo "$peak $conns" > $T/fair-peak ) &
  if [ $transport = http ]; then out=$(TABLE=$TABLE SPLIT=$SPLIT TAG=$tag ip netns exec chtest $T/rs/target/release/bench $MODE $T/markets/$set 2>&1 | tail -1)
  else out=$(MODE=$MODE TABLE=$TABLE SPLIT=$SPLIT TAG=$tag ip netns exec chtest $T/rsg/target/release/benchg $T/markets/$set 2>&1 | tail -1); fi
  rm -f $T/sampling; wait
  read peak conns < $T/fair-peak
  $C -q "SYSTEM FLUSH LOGS query_log" </dev/null
  local ql=$($C -q "SELECT count(), max(memory_usage), sum(memory_usage) FROM system.query_log WHERE log_comment = '$tag' AND type = 'QueryFinish'" </dev/null | tr '\t' ' ')
  python3 -c "import json,sys; r=json.loads(sys.argv[1]); print('\t'.join(map(str,['$set','$approach','$transport',$run,r['rows'],r['hash'],r['wall_s'],r['first_row_ms'],$peak,$conns,'$ql'])))" "$out" 2>/dev/null || echo -e "$set\t$approach\t$transport\t$run\tERROR ${out:0:200}"
}
for set in ${SETS:-rand100 rand500 top500 rand2000 rand5000}; do
  n=$(wc -l < $T/markets/$set)
  for run in 1 2 3; do
    for transport in http grpc; do
      MODE=single TABLE=pc SPLIT=0 one $set market_first_full_sort $transport $run
      MODE=single TABLE=pc SPLIT=1 one $set market_first_patched $transport $run
      MODE=single TABLE=pc_t SPLIT=0 one $set time_first_stock $transport $run
      if [ $transport = grpc ] || [ $n -le 2000 ]; then MODE=kway TABLE=pc SPLIT=0 one $set market_first_kway $transport $run; fi
    done
  done
done
