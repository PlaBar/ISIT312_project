#!/bin/bash
# run_spark.sh - run one Scala job file with spark-shell (the VM has no sbt to build jars).
#
#   bash lake/run_spark.sh lake/ingest/LandingToRaw.scala
#   bash lake/run_spark.sh lake/extract/Rules.scala lake/extract/RulesCheck.scala
#
# With several files, they are joined in that order into one script (a library such as
# Rules.scala first, the job last); the job's name, log and marker come from the last file.
#
# Settings (environment variables, all optional):
#   GSS_SPARK_MASTER   default local[2]  Spark runs inside one JVM on the VM; YARN is not needed
#   GSS_DRIVER_MEMORY  default 2g        memory for that JVM (the VM has 7.8 GB in total)
#   GSS_HDFS           default hdfs://localhost:8020
#
# Every job ends by printing GSS_JOB_OK. spark-shell itself always exits with code 0, even
# after an error, so this marker is how the wrapper knows the job really finished; without
# it the run counts as FAILED (exit code 1). The full output goes to
# $GSS_STAGING/reports/spark_<job>.log; the screen shows it without spark-shell's noise.
#
# Spark settings used for every job:
#   spark.sql.parquet.writeLegacyFormat=true  Parquet that Hive 2.1.1 can read (decimals)
#   spark.sql.session.timeZone=UTC            timestamps stored in UTC

source "$(dirname "$0")/../preprocess/env.sh"
cd "$PROJECT_ROOT" || exit 1

if [ $# -eq 0 ]; then
  echo "usage: bash lake/run_spark.sh [library.scala ...] <job.scala>"; exit 1
fi
for f in "$@"; do
  [ -f "$f" ] || { echo "file not found: $f"; exit 1; }
done
script="${!#}"
if ! command -v spark-shell > /dev/null; then
  echo "spark-shell not found (SPARK_HOME=$SPARK_HOME)"; exit 1
fi

mkdir -p "$GSS_STAGING/reports" "$GSS_STAGING/state"
name="$(basename "$script" .scala)"

# one Spark job at a time on this small VM (2 cores, 7.8 GB): a job started by cron while
# another runs waits for it (up to GSS_SPARK_WAIT seconds, default 1 hour) instead of both
# competing for memory. This also keeps the weekly compaction away from the stream.
exec 8>"$GSS_STAGING/state/spark.lock"
if ! flock -n 8; then
  echo "waiting for another Spark job to finish ($GSS_STAGING/state/spark.lock)"
  if ! flock -w "${GSS_SPARK_WAIT:-3600}" 8; then
    echo "gave up waiting for the Spark lock"; exit 1
  fi
fi
if [ $# -gt 1 ]; then                    # join library + job into one script for spark-shell -i
  joined="$GSS_STAGING/reports/.joined_${name}.scala"
  : > "$joined"
  for f in "$@"; do cat "$f" >> "$joined"; echo >> "$joined"; done
  script="$joined"
fi
LOG="$GSS_STAGING/reports/spark_${name}.log"
out="$(mktemp)"
echo "===== $(date '+%Y-%m-%d %H:%M:%S') $*" >> "$LOG"
start=$(date +%s)

spark-shell \
  --master "${GSS_SPARK_MASTER:-local[2]}" \
  --driver-memory "${GSS_DRIVER_MEMORY:-2g}" \
  --conf spark.sql.parquet.writeLegacyFormat=true \
  --conf spark.sql.session.timeZone=UTC \
  --conf spark.ui.showConsoleProgress=false \
  -i "$script" < /dev/null 2>&1 \
  | tee "$out" \
  | grep --line-buffered -v -E '^scala> *$|^ *\|$|NativeCodeLoader|log4j|Setting default log level|To adjust logging level|Spark context Web UI|Spark context available|Spark session available|^Welcome to|^      ____|^     / __|^    _\\ \\|^   /___/|^      /_/|^Using Scala version|^Type in expressions|^Type :help'

cat "$out" >> "$LOG"
secs=$(( $(date +%s) - start ))
if grep -q '^GSS_JOB_OK' "$out"; then
  echo "===== $name finished OK in ${secs} s" | tee -a "$LOG"
  rc=0
else
  echo "===== $name FAILED after ${secs} s (full output: $LOG)" | tee -a "$LOG"
  grep -E 'Exception|error:' "$out" | head -5
  rc=1
fi
rm -f "$out"
exit $rc