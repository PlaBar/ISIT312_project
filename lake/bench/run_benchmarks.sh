#!/bin/bash
# run_benchmarks.sh - Step 29: the same query in MapReduce, Hive and Spark, at three sizes,
# plus a single-request lookup in HBase and in Hive.
#
#   start-yarn.sh                              MapReduce and Hive run as YARN jobs here
#   bash lake/bench/run_benchmarks.sh
#   BENCH_SIZES="1 10" bash lake/bench/run_benchmarks.sh     fewer sizes, quicker
#
# Query (all engines): requests, total weight and high-confidence requests per month and route,
# on /lake/tmp/bench/requests_x<N> (x1 = the 10,707 real requests, x10 and x100 = copies).
#   MapReduce  Hadoop Streaming with lake/bench/mapper.py and reducer.py (reducer also as
#              combiner), one job per size, on YARN. The key is the first TWO fields, for
#              the map output and also for the combiner/reducer output: Streaming reads the
#              combiner's output with the reducer settings, and with the default (1 field)
#              the shuffle would sort by month only and split routes into several groups
#   Hive       one HiveQL GROUP BY per size, compiled to MapReduce, on YARN
#   Spark      lake/bench/BenchSpark.scala, local[2], all sizes in one session
# Times
#   wall    what the user waits for, start-up included (seconds, from this script)
#   engine  the query alone: Hive's "Time taken", Spark's own timer (first run); MapReduce
#           has no separate start-up, so wall = engine
# The three results of each size are compared line by line: they must be identical.
# Output: table on screen, $GSS_STAGING/reports/bench/results_<time>.csv, results in .../bench/

source "$(dirname "$0")/../../preprocess/env.sh"
cd "$PROJECT_ROOT" || exit 1
export BENCH_SIZES="${BENCH_SIZES:-1 10 100}"
export LC_ALL=C                     # one sort order for every result file, so they compare
OUT="$GSS_STAGING/reports/bench"
mkdir -p "$OUT"
STAMP="$(date +%Y%m%d_%H%M%S)"
RES="$OUT/results_$STAMP.csv"
LOG="$OUT/run_$STAMP.log"
echo "engine,size,rows,wall_s,engine_s,result_rows,same_as_spark" > "$RES"
now() { date +%s.%N; }
diff_s() { awk -v a="$1" -v b="$2" 'BEGIN {printf "%.1f", b - a}'; }
say() { echo "$(date '+%H:%M:%S') $*" | tee -a "$LOG"; }

# --- checks
for p in ResourceManager NodeManager; do
  jps | grep -q -w "$p" || { echo "YARN is not running ($p missing): start-yarn.sh"; exit 1; }
done
bash lake/hive/metastore.sh status > /dev/null || { echo "Hive metastore not running: bash lake/hive/metastore.sh start"; exit 1; }
streaming_jar="$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar 2>/dev/null | head -1)"
[ -n "$streaming_jar" ] || { echo "hadoop-streaming jar not found under $HADOOP_HOME/share/hadoop/tools/lib"; exit 1; }

# --- 1. data
say "1. preparing the data (sizes: $BENCH_SIZES)"
bash lake/run_spark.sh lake/bench/BenchPrepare.scala | tee -a "$LOG" | grep -E "requests_x|FAILED" || exit 1
base_rows="$(hdfs dfs -cat '/lake/tmp/bench/requests_x1/part-*' 2>/dev/null | wc -l)"
[ "$base_rows" -gt 0 ] || { echo "requests_x1 not found: include 1 in BENCH_SIZES"; exit 1; }

# --- 2. Spark (one session for all sizes)
say "2. Spark"
t0=$(now)
bash lake/run_spark.sh lake/bench/BenchSpark.scala > "$OUT/spark_$STAMP.out" 2>&1
t1=$(now)
grep -q GSS_JOB_OK "$OUT/spark_$STAMP.out" || { echo "BenchSpark failed, see $OUT/spark_$STAMP.out"; exit 1; }
grep "^BENCH" "$OUT/spark_$STAMP.out" | tee -a "$LOG"
spark_total=$(diff_s "$t0" "$t1")
spark_queries=$(grep "^BENCH" "$OUT/spark_$STAMP.out" | awk '{s += $5 + $8} END {printf "%.1f", s}')
say "   spark-shell run: ${spark_total} s in all, of which queries ${spark_queries} s (the rest is start-up)"
for n in $BENCH_SIZES; do
  sort -o "$OUT/spark_x$n.tsv" "$OUT/spark_x$n.tsv"
  e=$(grep "^BENCH spark x$n " "$OUT/spark_$STAMP.out" | awk '{print $5}')
  echo "spark,$n,$((base_rows * n)),,${e},$(wc -l < "$OUT/spark_x$n.tsv"),yes" >> "$RES"
done

# --- 3. MapReduce (Hadoop Streaming)
say "3. MapReduce (Hadoop Streaming on YARN)"
for n in $BENCH_SIZES; do
  hdfs dfs -rm -r -f -skipTrash "/lake/tmp/bench/out_mr_x$n" > /dev/null 2>&1
  t0=$(now)
  hadoop jar "$streaming_jar" \
    -D mapreduce.framework.name=yarn \
    -D mapreduce.job.name="gss-bench-x$n" \
    -D stream.num.map.output.key.fields=2 \
    -D stream.num.reduce.output.key.fields=2 \
    -D mapreduce.task.timeout=180000 \
    -D mapreduce.job.reduces=1 \
    -files lake/bench/mapper.py,lake/bench/reducer.py \
    -mapper "python3 mapper.py" -combiner "python3 reducer.py" -reducer "python3 reducer.py" \
    -input "/lake/tmp/bench/requests_x$n" -output "/lake/tmp/bench/out_mr_x$n" >> "$LOG" 2>&1
  rc=$?; t1=$(now)
  [ $rc -eq 0 ] || { echo "MapReduce x$n FAILED, see $LOG"; exit 1; }
  hdfs dfs -cat "/lake/tmp/bench/out_mr_x$n/part-*" | sort > "$OUT/mr_x$n.tsv"
  same=$(cmp -s "$OUT/mr_x$n.tsv" "$OUT/spark_x$n.tsv" && echo yes || echo NO)
  w=$(diff_s "$t0" "$t1")
  say "   x$n: ${w} s, $(wc -l < "$OUT/mr_x$n.tsv") result rows, same as Spark: $same"
  echo "mapreduce,$n,$((base_rows * n)),$w,$w,$(wc -l < "$OUT/mr_x$n.tsv"),$same" >> "$RES"
done

# --- 4. Hive (on YARN)
say "4. Hive (on YARN)"
ddl="CREATE DATABASE IF NOT EXISTS gss_bench;"
for n in $BENCH_SIZES; do
  ddl="$ddl DROP TABLE IF EXISTS gss_bench.requests_x$n;
CREATE EXTERNAL TABLE gss_bench.requests_x$n (request_key STRING, yyyymm STRING, origin STRING, dest STRING,
  cargo_type STRING, weight_kg BIGINT, confidence STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE LOCATION '/lake/tmp/bench/requests_x$n';"
done
hive -S -e "$ddl" >> "$LOG" 2>&1 || { echo "Hive table creation failed, see $LOG"; exit 1; }
for n in $BENCH_SIZES; do
  sql="SET mapreduce.framework.name=yarn;
SELECT yyyymm, concat(origin, '-', dest), COUNT(*), SUM(weight_kg), SUM(CASE WHEN confidence = 'high' THEN 1 ELSE 0 END)
FROM gss_bench.requests_x$n GROUP BY yyyymm, concat(origin, '-', dest);"
  t0=$(now)
  hive -e "$sql" > "$OUT/hive_x$n.raw" 2> "$OUT/hive_x$n.err"
  rc=$?; t1=$(now)
  [ $rc -eq 0 ] || { echo "Hive x$n FAILED:"; grep -E "FAILED|Error" "$OUT/hive_x$n.err" | head -3; exit 1; }
  sort "$OUT/hive_x$n.raw" > "$OUT/hive_x$n.tsv"
  e=$(grep "Time taken" "$OUT/hive_x$n.err" | tail -1 | awk '{print $3}')
  same=$(cmp -s "$OUT/hive_x$n.tsv" "$OUT/spark_x$n.tsv" && echo yes || echo NO)
  w=$(diff_s "$t0" "$t1")
  say "   x$n: ${w} s wall, ${e} s query, $(wc -l < "$OUT/hive_x$n.tsv") result rows, same as Spark: $same"
  echo "hive,$n,$((base_rows * n)),$w,$e,$(wc -l < "$OUT/hive_x$n.tsv"),$same" >> "$RES"
done

# --- 5. one request by key: HBase get vs Hive WHERE (only if HBase is running)
say "5. Single-request lookup"
key="$(grep "^get 'gss_requests'" lake/hbase/demo_queries.hbase 2>/dev/null | head -1 | cut -d"'" -f4)"
if [ -n "$key" ] && jps | grep -q HMaster; then
  t0=$(now)
  hb="$(echo "get 'gss_requests', '$key'" | hbase shell 2>/dev/null | grep "row(s) in" | tail -1)"
  t1=$(now)
  hb_e=$(echo "$hb" | awk '{print $4}')
  hb_w=$(diff_s "$t0" "$t1")
  msg="${key##*#}"
  t0=$(now)
  hv="$(hive -e "SELECT * FROM gss.request WHERE request_key = '$msg'" 2>&1 >/dev/null | grep "Time taken" | tail -1)"
  t1=$(now)
  hv_e=$(echo "$hv" | awk '{print $3}')
  hv_w=$(diff_s "$t0" "$t1")
  say "   HBase get : ${hb_e} s in HBase, ${hb_w} s with the shell's start-up"
  say "   Hive WHERE: ${hv_e} s query,    ${hv_w} s with Hive's start-up"
  echo "hbase_get,1,$base_rows,$hb_w,$hb_e,1," >> "$RES"
  echo "hive_lookup,1,$base_rows,$hv_w,$hv_e,1," >> "$RES"
else
  say "   skipped: start HBase (start-hbase.sh) and load it (Step 26) to include the lookup"
fi

# --- summary
echo
echo "Results ($RES)"
column -s, -t "$RES"
echo
echo "Spark start-up and planning: about $(awk -v t="$spark_total" -v q="$spark_queries" 'BEGIN {printf "%.0f", t - q}') s once per spark-shell run (not in the Spark rows)"
