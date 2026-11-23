#!/bin/bash
# run_pipeline.sh - the scheduled runs of the GSS data lake (Step 28), for cron or by hand.
#
#   bash lake/ops/run_pipeline.sh stream     messages: release what has arrived, stream it in
#   bash lake/ops/run_pipeline.sh nightly    files: preprocess -> raw -> processed -> curated,
#                                            documents, HBase
#   bash lake/ops/run_pipeline.sh weekly     compact the stream's small files, rotate logs
#   bash lake/ops/run_pipeline.sh nightly --from Cleanse    resume a failed night at a step
#
# Each stage stops at the first failed step and records the result in
#   $GSS_STAGING/state/last_<stage>.txt     OK|FAILED, time, duration, failed step
# which health_check.sh reads. Output is appended to $GSS_STAGING/reports/pipeline.log.
# A stage does not start while the same stage is still running (e.g. a slow night).
# Spark jobs never overlap: lake/run_spark.sh makes them take turns.

source "$(dirname "$0")/../../preprocess/env.sh"
cd "$PROJECT_ROOT" || exit 1
stage="$1"; shift
from=""
[ "$1" = "--from" ] && from="$2"
mkdir -p "$GSS_STAGING/reports" "$GSS_STAGING/state"
LOG="$GSS_STAGING/reports/pipeline.log"
STATE="$GSS_STAGING/state/last_${stage}.txt"
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') [$stage] $*" | tee -a "$LOG"; }

case "$stage" in stream|nightly|weekly) ;; *)
  echo "usage: bash lake/ops/run_pipeline.sh stream|nightly|weekly [--from <step>]"; exit 1 ;;
esac

exec 7>"$GSS_STAGING/state/pipeline_${stage}.lock"
if ! flock -n 7; then log "previous $stage run still going; skipped"; exit 0; fi

start=$(date +%s)
started=0; [ -z "$from" ] && started=1
finish() {   # finish OK|FAILED [step]
  secs=$(( $(date +%s) - start ))
  echo "$1 $(date '+%Y-%m-%d %H:%M:%S') ${secs}s ${2:-}" > "$STATE"
  log "$1 in ${secs}s ${2:+(step $2)}"
  [ "$1" = "OK" ] && exit 0 || exit 1
}
run() {      # run <step name> <command...>: skipped until --from is reached
  local name="$1"; shift
  if [ $started -eq 0 ]; then
    if [ "$name" = "$from" ]; then started=1; else log "skip $name (resuming from $from)"; return; fi
  fi
  log "--- $name"
  "$@" >> "$LOG" 2>&1 || finish FAILED "$name"
}
hdfs_ready() {
  hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF" || { log "HDFS is not up (or in safe mode)"; finish FAILED hdfs; }
}

log "===== start"
hdfs_ready

if [ "$stage" = "stream" ]; then
  # In the company, new message files appear by themselves; here the next batch of the
  # generated messages is released, simulating what arrived in the last 15 minutes.
  run release python3 preprocess/pp_messages.py --limit "${GSS_STREAM_BATCH:-200}" --quiet
  # Start Spark (about 40 s) only if a landing file is newer than the last one streamed.
  newest="$(hdfs dfs -ls -R /lake/landing/messages 2>/dev/null | awk '$1 !~ /^d/ {print $6"T"$7}' | sort | tail -1)"
  last="$(cat "$GSS_STAGING/state/stream_newest_seen.txt" 2>/dev/null)"
  if [ -z "$newest" ] || [[ ! "$newest" > "$last" ]]; then
    log "no new message files since $last"
    finish OK
  fi
  run StreamMessages env GSS_STREAM_ONCE=1 bash lake/run_spark.sh lake/extract/Rules.scala lake/stream/StreamMessages.scala
  echo "$newest" > "$GSS_STAGING/state/stream_newest_seen.txt"
  finish OK
fi

if [ "$stage" = "nightly" ]; then
  run preprocess     bash preprocess/run_preprocess.sh
  run LandingToRaw   bash lake/run_spark.sh lake/ingest/LandingToRaw.scala
  run Cleanse        bash lake/run_spark.sh lake/process/Cleanse.scala
  run BuildCurated   bash lake/run_spark.sh lake/process/BuildCurated.scala
  run DefectClassify bash lake/run_spark.sh lake/process/DefectClassify.scala
  run LinkDocuments  bash lake/run_spark.sh lake/link/LinkDocuments.scala
  # HBase uses 1-1.5 GB of memory, so it runs only for its load: started here if it is not
  # already running, and stopped again afterwards
  if [ -n "$HBASE_HOME" ]; then
    hbase_started=0
    if ! jps | grep -q HMaster; then
      log "starting HBase for the load"; start-hbase.sh >> "$LOG" 2>&1; hbase_started=1; sleep 20
    fi
    run HBase bash lake/hbase/load_hbase.sh
    if [ $hbase_started -eq 1 ]; then log "stopping HBase"; stop-hbase.sh >> "$LOG" 2>&1; fi
  else
    log "HBase not installed: load skipped"
  fi
  finish OK
fi

if [ "$stage" = "weekly" ]; then
  run CompactStream bash lake/run_spark.sh lake/ops/CompactStream.scala
  # logs: keep each below 5 MB (one older copy kept as .1); old helper files go
  for f in "$GSS_STAGING"/reports/*.log; do
    [ -f "$f" ] && [ "$(stat -c %s "$f")" -gt 5242880 ] && mv "$f" "$f.1" && log "rotated $(basename "$f")"
  done
  find "$GSS_STAGING/reports" -name 'landing_to_raw_*.csv' -mtime +30 -delete
  find "$GSS_STAGING/reports" -name 'cleanse_dq_*.csv' -mtime +30 -delete
  hdfs dfs -rm -r -f -skipTrash /lake/tmp/hbase_requests >> "$LOG" 2>&1
  finish OK
fi
