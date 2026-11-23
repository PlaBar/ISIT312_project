# env.sh - environment shared by the batch scripts, whether started by hand or by cron.
#
# Cron starts jobs with almost nothing set: no .bashrc, a short PATH, no JAVA_HOME and no
# HADOOP_HOME, so "hdfs" cannot start. This file fills in what is missing and leaves
# anything you have already set alone. Usage, from another script:
#   source "$(dirname "$0")/env.sh"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# data folder (see GSS_STAGING in generators/gen_common.py)
if [ -z "$GSS_STAGING" ]; then
  if [ -d "$HOME/gss-data/staging" ]; then
    export GSS_STAGING="$HOME/gss-data/staging"
  else
    export GSS_STAGING="$PROJECT_ROOT/staging"
  fi
fi

# Hadoop
if [ -z "$HADOOP_HOME" ]; then
  for d in /usr/share/hadoop /usr/local/hadoop /opt/hadoop; do
    if [ -x "$d/bin/hdfs" ]; then export HADOOP_HOME="$d"; break; fi
  done
fi
if [ -n "$HADOOP_HOME" ]; then
  export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"
fi

# Spark (no sbt on the VM: Scala jobs run with spark-shell, see lake/run_spark.sh)
if [ -z "$SPARK_HOME" ]; then
  for d in /usr/share/spark /usr/local/spark /opt/spark; do
    if [ -x "$d/bin/spark-shell" ]; then export SPARK_HOME="$d"; break; fi
  done
fi
if [ -n "$SPARK_HOME" ]; then
  export PATH="$SPARK_HOME/bin:$PATH"
fi
if [ -z "$HADOOP_CONF_DIR" ] && [ -n "$HADOOP_HOME" ] && [ -d "$HADOOP_HOME/etc/hadoop" ]; then
  export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
fi

# Hive and HBase (Steps 25-27): their own bin folders are usually not on the PATH
if [ -z "$HIVE_HOME" ]; then
  for d in /usr/share/hive /usr/local/hive /opt/hive; do
    if [ -x "$d/bin/hive" ]; then export HIVE_HOME="$d"; break; fi
  done
fi
if [ -n "$HIVE_HOME" ]; then
  export PATH="$HIVE_HOME/bin:$PATH"
fi
if [ -z "$HBASE_HOME" ]; then
  for d in /usr/share/hbase /usr/local/hbase /opt/hbase; do
    if [ -x "$d/bin/hbase" ]; then export HBASE_HOME="$d"; break; fi
  done
fi
if [ -n "$HBASE_HOME" ]; then
  export PATH="$HBASE_HOME/bin:$PATH"
fi

# Java programs started from here (hdfs, hive, hbase, hadoop jar) run in UTC, like Spark
# (spark.sql.session.timeZone=UTC). Without it Hive shows the UTC timestamps stored in
# Parquet in the VM's own time zone (seen in Step 25: 04:41 UTC shown as 15:41 in Sydney).
case "$HADOOP_CLIENT_OPTS" in
  *user.timezone*) ;;
  *) export HADOOP_CLIENT_OPTS="$HADOOP_CLIENT_OPTS -Duser.timezone=UTC" ;;
esac

# Java, needed by every hdfs command
if [ -z "$JAVA_HOME" ]; then
  j="$(readlink -f "$(command -v java)" 2>/dev/null)"
  if [ -n "$j" ]; then
    j="${j%/bin/java}"
    export JAVA_HOME="${j%/jre}"
  fi
fi

export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"