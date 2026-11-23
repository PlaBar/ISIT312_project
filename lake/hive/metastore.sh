#!/bin/bash
# metastore.sh - start, stop or check the Hive metastore service.
#
#   bash lake/hive/metastore.sh start     start it in the background (about 20-30 s)
#   bash lake/hive/metastore.sh status    is it listening on port 9083?
#   bash lake/hive/metastore.sh stop
#
# On this VM hive-site.xml sets hive.metastore.uris (thrift://...:9083), so the hive command
# does not open the Derby database itself: it asks a separate metastore service, which must be
# running first ("Connection refused" otherwise). The service is the only process that opens
# Derby, so several hive sessions can run at the same time. Like HDFS, it has to be started
# again after every reboot. Log: $GSS_STAGING/reports/hive_metastore.log

source "$(dirname "$0")/../../preprocess/env.sh"
PORT=9083
LOG="$GSS_STAGING/reports/hive_metastore.log"
mkdir -p "$GSS_STAGING/reports"

listening() { (exec 3<>/dev/tcp/127.0.0.1/$PORT) 2>/dev/null; }
pid() { jps -m 2>/dev/null | awk '/HiveMetaStore/ {print $1}'; }

case "$1" in
  start)
    if listening; then echo "metastore already running on port $PORT"; exit 0; fi
    echo "starting the Hive metastore (log: $LOG)"
    nohup hive --service metastore >> "$LOG" 2>&1 &
    for i in $(seq 1 60); do
      sleep 2
      if listening; then echo "metastore running on port $PORT after $((i * 2)) s"; exit 0; fi
    done
    echo "metastore did not start within 120 s; last lines of $LOG:"
    tail -20 "$LOG"
    exit 1 ;;
  stop)
    p="$(pid)"
    if [ -z "$p" ]; then echo "metastore is not running"; exit 0; fi
    kill $p && echo "stopped metastore (pid $p)" ;;
  status)
    if listening; then echo "metastore running on port $PORT (pid $(pid))"; else echo "metastore NOT running"; exit 1; fi ;;
  *)
    echo "usage: bash lake/hive/metastore.sh start|stop|status"; exit 1 ;;
esac