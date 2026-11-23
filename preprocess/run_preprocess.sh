#!/bin/bash
# run_preprocess.sh - batch preprocessing of every file-based source, for cron or by hand.
#
#   bash preprocess/run_preprocess.sh                 structured, events, documents -> HDFS
#   bash preprocess/run_preprocess.sh --no-hdfs       the same, kept local (testing)
#   bash preprocess/run_preprocess.sh --check         afterwards, compare with the planted errors
#   bash preprocess/run_preprocess.sh events          one or more named sources only
#
# Messages are not included: release_loop.sh (Step 15) feeds them a few at a time while
# the streaming job runs. Add "messages" by name to process all waiting messages at once.
#
# - Runs the sources in order and stops at the first failure (exit code 1).
# - Only new files are processed (PP-01), so a run with nothing new is quick and harmless.
# - Only one copy runs at a time; a second one started meanwhile exits straight away.
# - Everything is appended to $GSS_STAGING/reports/run_preprocess.log with a timestamp.

source "$(dirname "$0")/env.sh"
cd "$PROJECT_ROOT" || exit 1

hdfs_opt=""
check=0
sources=()
for a in "$@"; do
  case "$a" in
    --no-hdfs) hdfs_opt="--no-hdfs" ;;
    --check) check=1 ;;
    structured|events|documents|messages) sources+=("$a") ;;
    *) echo "unknown option: $a"; exit 1 ;;
  esac
done
[ ${#sources[@]} -eq 0 ] && sources=(structured events documents)

mkdir -p "$GSS_STAGING/reports" "$GSS_STAGING/state"
LOG="$GSS_STAGING/reports/run_preprocess.log"
log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG"; }

# one run at a time
exec 9>"$GSS_STAGING/state/run_preprocess.lock"
if ! flock -n 9; then
  log "another run_preprocess.sh is still running; exiting"
  exit 0
fi

log "=== start: ${sources[*]} ${hdfs_opt} (data: $GSS_STAGING)"

if [ -z "$hdfs_opt" ]; then
  if ! command -v hdfs > /dev/null; then
    log "FAILED: hdfs command not found (HADOOP_HOME=$HADOOP_HOME)"; exit 1
  fi
  mode="$(hdfs dfsadmin -safemode get 2>&1)"
  if [[ "$mode" != *"OFF"* ]]; then
    log "FAILED: HDFS is not ready: $mode"
    log "start it with start-dfs.sh, or use --no-hdfs"
    exit 1
  fi
fi

status=0
for s in "${sources[@]}"; do
  start=$(date +%s)
  log "--- $s"
  python3 "preprocess/pp_${s}.py" $hdfs_opt 2>&1 | grep -v "reading and checksums\|cleaning " | tee -a "$LOG"
  rc=${PIPESTATUS[0]}
  if [ "$rc" -ne 0 ]; then
    log "FAILED: $s exited with code $rc; later sources were not run"
    status=1
    break
  fi
  log "$s finished in $(( $(date +%s) - start )) s"
done

if [ "$status" -eq 0 ] && [ "$check" -eq 1 ]; then
  for s in "${sources[@]}"; do
    python3 evaluation/check_preprocessing.py "$s" | grep -E "^TOTAL|not explained" | sed "s/^/$s: /" | tee -a "$LOG"
  done
fi

log "=== end: $([ $status -eq 0 ] && echo OK || echo FAILED)"
exit $status