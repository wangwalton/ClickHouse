# Runs ClickHouse stateless tests matching $1 inside netns chtest. Extra args go to clickhouse-test.
set -uo pipefail
T=/var/tmp/chtest
export PATH=$T/bin:$PATH CLICKHOUSE_DISKS_FILES=$T/var/lib/clickhouse/disks CLICKHOUSE_USER_FILES=$T/var/lib/clickhouse/user_files
mkdir -p $CLICKHOUSE_DISKS_FILES
cd $T
pattern=$1; shift
ip netns exec chtest /var/tmp/ch-src/tests/clickhouse-test --queries /var/tmp/ch-src/tests/queries --no-random-settings --no-random-merge-tree-settings --no-long --no-stateful --timeout 120 --jobs 12 --no-zookeeper --no-shard "$@" -- "$pattern" 2>&1
