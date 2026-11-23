#!/bin/bash
# run_monthly_report.sh - run lake/hive/monthly_report.hql for one month and keep the output.
#
#   bash lake/hive/run_monthly_report.sh             the last complete calendar month
#   bash lake/hive/run_monthly_report.sh 202603      a given month (yyyyMM)
#
# Needs HDFS and the Hive metastore (bash lake/hive/metastore.sh start).
# Output: on screen and in $GSS_STAGING/reports/monthly_report_<yyyyMM>.txt
# Hive runs each section as one or more MapReduce jobs in local mode, so the whole report
# takes a few minutes; the same SQL in Spark would take seconds (see Step 29).

source "$(dirname "$0")/../../preprocess/env.sh"
cd "$PROJECT_ROOT" || exit 1

month="${1:-$(date -d "$(date +%Y-%m-01) -1 month" +%Y%m)}"
if ! [[ "$month" =~ ^20[0-9]{2}(0[1-9]|1[0-2])$ ]]; then
  echo "usage: bash lake/hive/run_monthly_report.sh [yyyyMM]"; exit 1
fi
first="${month:0:4}-${month:4:2}-01"
prev="$(date -d "$first -1 month" +%Y%m)"
month_end="$(date -d "$first +1 month -1 day" +%Y%m%d)"

if ! bash lake/hive/metastore.sh status > /dev/null; then
  echo "The Hive metastore is not running: bash lake/hive/metastore.sh start"; exit 1
fi

mkdir -p "$GSS_STAGING/reports"
out="$GSS_STAGING/reports/monthly_report_${month}.txt"
echo "Monthly report $month (previous $prev, month end $month_end) -> $out"
start=$(date +%s)
hive -S --hivevar month="$month" --hivevar prev="$prev" --hivevar month_end="$month_end" \
     -f lake/hive/monthly_report.hql 2> "$out.log" | tee "$out"
rc=${PIPESTATUS[0]}
echo "finished in $(( $(date +%s) - start )) s"
if [ "$rc" -ne 0 ]; then
  echo "Hive FAILED (exit $rc). Errors:"; grep -E "FAILED|Error|Exception" "$out.log" | head -5
  exit 1
fi
