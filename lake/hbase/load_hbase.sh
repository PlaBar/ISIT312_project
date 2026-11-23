#!/bin/bash
# load_hbase.sh - load the customer requests into HBase (Step 26).
#
#   start-hbase.sh                   once per session (HDFS must already be running)
#   bash lake/hbase/load_hbase.sh
#
# 1. HBaseSink.scala (Spark) writes the requests as tab-separated lines to /lake/tmp/hbase_requests
# 2. create_table.hbase drops and recreates the table gss_requests
# 3. ImportTsv, HBase's own loader, puts the lines into the table. It is a MapReduce job,
#    run here in local mode inside one process (no YARN needed for 10,000 rows).
#    Tested with HBase 1.2.6 and Hadoop 2.7.3
# 4. demo_queries.hbase shows a prefix scan, two gets and the row count
# Log: $GSS_STAGING/reports/load_hbase.log

source "$(dirname "$0")/../../preprocess/env.sh"
cd "$PROJECT_ROOT" || exit 1
LOG="$GSS_STAGING/reports/load_hbase.log"
mkdir -p "$GSS_STAGING/reports"
step() { echo "$(date '+%H:%M:%S') --- $*" | tee -a "$LOG"; }

if [ -z "$HBASE_HOME" ] || ! command -v hbase > /dev/null; then
  echo "hbase not found (HBASE_HOME=$HBASE_HOME). Add HBase's bin folder to your PATH."; exit 1
fi
if ! jps | grep -q HMaster; then
  echo "HBase is not running: start it with  start-hbase.sh  (and check with jps: HMaster, HRegionServer)"; exit 1
fi

step "1/4 Spark: requests -> /lake/tmp/hbase_requests"
bash lake/run_spark.sh lake/hbase/HBaseSink.scala || exit 1

step "2/4 HBase shell: create table gss_requests"
hbase shell lake/hbase/create_table.hbase 2>&1 | grep -v -E "^SLF4J|^$" | tee -a "$LOG"

step "3/4 ImportTsv: load the lines"
start=$(date +%s)
# Started through "hadoop jar" with HBase's MapReduce classpath, as the HBase 1.x reference
# guide does it ("hbase ...ImportTsv" lacks the MapReduce libraries on this VM). The glob
# hbase-server-*.jar also matches the -tests jar, so the plain jar is picked explicitly.
server_jar="$(ls "$HBASE_HOME"/lib/hbase-server-*.jar | grep -v -- -tests | head -1)"
HADOOP_CLASSPATH="$(hbase mapredcp 2>/dev/null):$HBASE_HOME/conf" \
hadoop jar "$server_jar" importtsv \
  -Dmapreduce.framework.name=local \
  -Dimporttsv.columns="$(cat lake/hbase/importtsv_columns.txt)" \
  gss_requests "${GSS_HDFS:-hdfs://localhost:8020}/lake/tmp/hbase_requests" > "$LOG.importtsv" 2>&1
rc=$?
cat "$LOG.importtsv" >> "$LOG"
grep -E "Map input records|Map output records|Bad Lines|Job .* completed|ERROR" "$LOG.importtsv" | sed 's/^/    /'
if [ $rc -ne 0 ]; then echo "ImportTsv FAILED (exit $rc), see $LOG"; exit 1; fi
echo "    loaded in $(( $(date +%s) - start )) s"

step "4/4 HBase shell: demo lookups"
hbase shell lake/hbase/demo_queries.hbase 2>&1 | grep -v -E "^SLF4J" | tee -a "$LOG"
step "done"
