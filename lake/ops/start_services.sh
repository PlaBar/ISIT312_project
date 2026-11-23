#!/bin/bash
# start_services.sh - bring up what the lake needs after a reboot (run by cron @reboot).
#
#   bash lake/ops/start_services.sh
#
# 1. waits for the VirtualBox shared folder (the project) to be mounted
# 2. HDFS (start-dfs.sh) if the NameNode is not running, then waits until safe mode is off
# 3. the Hive metastore (lake/hive/metastore.sh)
# HBase and YARN are not started: HBase runs only during its nightly load, YARN only for the
# benchmarks (Step 29).

source "$(dirname "$0")/../../preprocess/env.sh"
LOG="$GSS_STAGING/reports/pipeline.log"
mkdir -p "$GSS_STAGING/reports"
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') [services] $*" | tee -a "$LOG"; }

for i in $(seq 1 30); do
  [ -f "$PROJECT_ROOT/lake/run_spark.sh" ] && break
  log "waiting for the project folder $PROJECT_ROOT"; sleep 10
done

if jps | grep -q -w NameNode; then
  log "HDFS already running"
else
  log "starting HDFS"; start-dfs.sh >> "$LOG" 2>&1
fi
log "waiting for HDFS to leave safe mode"
timeout 300 hdfs dfsadmin -safemode wait >> "$LOG" 2>&1 || { log "HDFS did not leave safe mode"; exit 1; }

bash "$PROJECT_ROOT/lake/hive/metastore.sh" start >> "$LOG" 2>&1 || { log "metastore did not start"; exit 1; }
log "services up: $(jps | awk '$2 != "Jps" {print $2}' | sort | tr '\n' ' ')"
