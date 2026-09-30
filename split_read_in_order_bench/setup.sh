# Installs CI fast-test configs under /var/tmp/chtest and starts the built server in netns chtest.
set -euo pipefail
SRC=/var/tmp/ch-src
T=/var/tmp/chtest
BIN=$T/bin
mkdir -p $BIN $T/etc/clickhouse-server $T/var/lib/clickhouse $T/var/log/clickhouse-server
for n in clickhouse clickhouse-server clickhouse-client clickhouse-local; do ln -sf $SRC/build/programs/clickhouse $BIN/$n; done
export PATH=$BIN:$PATH
cd $SRC
cp programs/server/config.xml programs/server/users.xml $T/etc/clickhouse-server/
./tests/config/install.sh $T/etc/clickhouse-server $T/etc/clickhouse-client --fast-test > $T/install.log 2>&1
rm -f $T/etc/clickhouse-server/config.d/secure_ports.xml
python3 - <<'PY'
from pathlib import Path
T='/var/tmp/chtest'; d=f'{T}/etc/clickhouse-server'
for cfg in [Path(f'{d}/config.xml')] + sorted(Path(f'{d}/config.d').glob('*.xml')):
    text = cfg.resolve().read_text()
    if '>/var/' not in text and '>/etc/' not in text: continue
    text = text.replace('>/var/', f'>{T}/var/').replace('>/etc/', f'>{T}/etc/')
    if cfg.is_symlink(): cfg.unlink()
    cfg.write_text(text)
PY
ip netns list | grep -q '^chtest' || { ip netns add chtest; ip netns exec chtest ip link set lo up; }
cd $T
ip netns exec chtest setsid $BIN/clickhouse-server --config-file $T/etc/clickhouse-server/config.xml --pid-file $T/server.pid -- \
  --path $T/var/lib/clickhouse/ --user_files_path $T/var/lib/clickhouse/user_files --top_level_domains_path $T/etc/clickhouse-server/top_level_domains \
  --logger.stderr $T/var/log/clickhouse-server/stderr.log > $T/server.out 2>&1 < /dev/null &
for i in $(seq 60); do ip netns exec chtest $BIN/clickhouse-client -q 'SELECT version()' 2>/dev/null && exit 0; sleep 1; done
echo 'server did not start'; tail -30 $T/var/log/clickhouse-server/stderr.log; exit 1
