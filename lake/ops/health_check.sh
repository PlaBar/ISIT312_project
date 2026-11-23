#!/bin/bash
# health_check.sh - one line on the state of the lake (run hourly by cron, or by hand).
#
#   bash lake/ops/health_check.sh
#
# Checks: NameNode and DataNode running, HDFS out of safe mode and its use, Hive metastore,
# and the last stream / nightly / weekly runs (status and age). Prints one line, appends it
# to $GSS_STAGING/reports/health.log and exits 1 if anything needs attention.

source "$(dirname "$0")/../../preprocess/env.sh"
mkdir -p "$GSS_STAGING/reports"
problems=()
running="$(jps 2>/dev/null)"
for p in NameNode DataNode; do
  echo "$running" | grep -q -w "$p" || problems+=("$p down")
done
if hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF; then
  used="$(hdfs dfsadmin -report 2>/dev/null | awk -F': ' '/^DFS Used%/ {print $2; exit}')"
else
  problems+=("HDFS unavailable or in safe mode"); used="?"
fi
bash "$PROJECT_ROOT/lake/hive/metastore.sh" status > /dev/null 2>&1 || problems+=("metastore down")

# last runs: status and how old (stream: 1 h, nightly: 26 h, weekly: 8 days)
runs=""
for s in stream:3600 nightly:93600 weekly:691200; do
  name="${s%%:*}"; max="${s##*:}"; f="$GSS_STAGING/state/last_${name}.txt"
  if [ -f "$f" ]; then
    status="$(awk '{print $1}' "$f")"; age=$(( $(date +%s) - $(stat -c %Y "$f") ))
    runs="$runs $name=$status/$((age / 60))min"
    [ "$status" != "OK" ] && problems+=("last $name run $status ($(awk '{print $5}' "$f"))")
    [ "$age" -gt "$max" ] && problems+=("no $name run for $((age / 3600)) h")
  else
    runs="$runs $name=never"
  fi
done

if [ ${#problems[@]} -eq 0 ]; then state="OK"; else state="ATTENTION: $(IFS=';'; echo "${problems[*]}")"; fi
line="$(date '+%Y-%m-%d %H:%M:%S') hdfs_used=$used$runs | $state"
echo "$line" | tee -a "$GSS_STAGING/reports/health.log"
[ ${#problems[@]} -eq 0 ]
