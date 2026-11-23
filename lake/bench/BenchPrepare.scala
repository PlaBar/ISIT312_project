// BenchPrepare.scala - test data for the Step 29 benchmark.
//
// Run by lake/bench/run_benchmarks.sh. Writes the 10,707 customer requests as plain
// tab-separated text, the one format that Hadoop Streaming, Hive and Spark all read the same
// way, in several sizes: x1 is the real data, x10 and x100 are copies with new keys, so the
// same query can be timed on about 10 thousand, 100 thousand and 1 million rows.
//   /lake/tmp/bench/requests_x<N>/part-*     request_key yyyymm origin dest cargo_type weight_kg confidence
// Missing values are written as "?" (text) and 0 (weight), so all three engines read them
// identically and their results can be compared line by line.
// Sizes: environment variable BENCH_SIZES, default "1 10 100".
//
// Spark 3.0.3, Scala 2.12, run with spark-shell -i (no sbt). One object = one unit for the REPL.

import org.apache.spark.sql.{functions => F}

object BenchPrepare {
  val hdfs = sys.env.getOrElse("GSS_HDFS", "hdfs://localhost:8020")
  val sizes = sys.env.getOrElse("BENCH_SIZES", "1 10 100").trim.split("\\s+").map(_.toInt).toSeq

  def run(): Int = {
    spark.sparkContext.setLogLevel("WARN")
    val base = spark.read.parquet(hdfs + "/lake/curated/request")
      .dropDuplicates("request_key")
      .select(F.col("request_key"),
              F.date_format(F.col("sent_date"), "yyyyMM").as("yyyymm"),
              F.coalesce(F.col("origin_iata"), F.lit("?")).as("origin"),
              F.coalesce(F.col("dest_iata"), F.lit("?")).as("dest"),
              F.coalesce(F.col("cargo_type"), F.lit("?")).as("cargo_type"),
              F.coalesce(F.col("weight_kg"), F.lit(0)).cast("string").as("weight_kg"),
              F.coalesce(F.col("confidence"), F.lit("?")).as("confidence"))
      .persist()
    val n0 = base.count()
    for (n <- sizes) {
      val copies = spark.range(n).withColumnRenamed("id", "copy")
      val df = base.crossJoin(copies)
        .withColumn("request_key", F.concat(F.col("request_key"), F.lit("-"), F.col("copy").cast("string")))
        .drop("copy")
      val parts = math.max(1, (n0 * n / 250000L).toInt + 1)              // about 250,000 rows per file
      val path = hdfs + s"/lake/tmp/bench/requests_x$n"
      df.select(F.concat_ws("\t", df.columns.map(F.col): _*)).repartition(parts).write.mode("overwrite").text(path)
      val bytes = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(hdfs), spark.sparkContext.hadoopConfiguration)
        .getContentSummary(new org.apache.hadoop.fs.Path(path)).getLength
      println(f"  requests_x$n%-4s ${n0 * n}%9d rows  ${bytes / 1048576.0}%7.1f MB  $parts%2d files")
    }
    base.unpersist()
    0
  }
}

val gssRc = BenchPrepare.run()
if (gssRc == 0) println("GSS_JOB_OK") else println("GSS_JOB_FAILED")
System.exit(gssRc)
