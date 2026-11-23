#!/bin/bash
# release_loop.sh - feed messages to HDFS a batch at a time, to simulate live arrival.
#
# Every interval it preprocesses the oldest messages not yet released (pp_messages.py
# --limit) and releases them as one file per channel to /lake/landing/messages/<channel>/,
# where the streaming job (StreamMessages.scala, Step 23) picks them up.
#
#   bash preprocess/release_loop.sh                          100 messages every 60 s until all are out
#   bash preprocess/release_loop.sh --batch 50 --every 30    smaller batches, more often
#   bash preprocess/release_loop.sh --max-batches 10         stop after 10 batches
#   bash preprocess/release_loop.sh --no-hdfs                local test, nothing sent to HDFS
#
# To stop it: Ctrl+C, or from another terminal
#   touch $GSS_STAGING/state/release_loop.stop
# which ends it cleanly after the current batch. A batch is never released half-way:
# pp_messages.py only marks messages done once its release has finished.
#
# Each batch's summary is appended to $GSS_STAGING/reports/release_loop.log.

source "$(dirname "$0")/env.sh"
cd "$PROJECT_ROOT" || exit 1

batch=100
every=60
max_batches=0
hdfs_opt=""
while [ $# -gt 0 ]; do
  case "$1" in
    --batch) batch="$2"; shift ;;
    --every) every="$2"; shift ;;
    --max-batches) max_batches="$2"; shift ;;
    --no-hdfs) hdfs_opt="--no-hdfs" ;;
    *) echo "unknown option: $1"; exit 1 ;;
  esac
  shift
done

mkdir -p "$GSS_STAGING/reports" "$GSS_STAGING/state"
LOG="$GSS_STAGING/reports/release_loop.log"
STOP="$GSS_STAGING/state/release_loop.stop"
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG"; }

exec 9>"$GSS_STAGING/state/release_loop.lock"
if ! flock -n 9; then
  echo "release_loop.sh is already running"
  exit 0
fi
rm -f "$STOP"

if [ -z "$hdfs_opt" ]; then
  mode="$(hdfs dfsadmin -safemode get 2>&1)"
  if [[ "$mode" != *"OFF"* ]]; then
    log "FAILED: HDFS is not ready: $mode"
    exit 1
  fi
fi

log "=== release loop: $batch messages every $every s $hdfs_opt (stop: touch $STOP)"
trap 'log "=== stopped by Ctrl+C after $n batches, $total messages released"; exit 0' INT TERM

n=0
total=0
while true; do
  out="$(python3 preprocess/pp_messages.py --limit "$batch" $hdfs_opt 2>&1)"
  rc=$?
  summary="$(echo "$out" | grep '^RUN-')"
  if [ $rc -ne 0 ]; then
    echo "$out" | tail -5 >> "$LOG"
    log "FAILED: pp_messages.py exited with code $rc (details above in $LOG)"
    exit 1
  fi
  released="$(echo "$summary" | sed -n 's/.* \([0-9]*\) released.*/\1/p')"
  files="$(echo "$summary" | sed -n 's/.*: \([0-9]*\) new files.*/\1/p')"
  if [ "${files:-0}" -eq 0 ]; then
    log "=== no messages left: $n batches, $total messages released"
    break
  fi
  n=$((n + 1))
  total=$((total + ${released:-0}))
  log "batch $n: $summary"
  if [ "$max_batches" -gt 0 ] && [ "$n" -ge "$max_batches" ]; then
    log "=== reached --max-batches $max_batches: $total messages released"
    break
  fi
  for ((i = 0; i < every; i++)); do
    if [ -f "$STOP" ]; then
      rm -f "$STOP"
      log "=== stopped by $STOP after $n batches, $total messages released"
      exit 0
    fi
    sleep 1
  done
done