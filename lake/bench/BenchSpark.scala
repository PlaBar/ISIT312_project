// BenchSpark.scala - the Step 29 benchmark query in Spark.
//
// Run by lake/bench/run_benchmarks.sh. For each size (BENCH_SIZES, default "1 10 100") it
// runs the benchmark query twice on /lake/tmp/bench/requests_x<N> and prints the time of each:
//   requests, total weight and high-confidence requests per month and route
// The first run includes reading the files and planning; the second shows a warm run.
// Both are inside one Spark session, so Spark's start-up (measured separately by the shell
// script as the whole spark-shell run) is not in these numbers.
// The result of each size is written to $GSS_STAGING/reports/bench/spark_x<N>.tsv for the
// comparison with MapReduce and Hive.
//
// Spark 3.0.3, Scala 2.12, run with spark-shell -i (no sbt). One object = one unit for the REPL.

import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types._

object BenchSpark {
  val hdfs = sys.env.getOrElse("GSS_HDFS", "hdfs://localhost:8020")
  val staging = sys.env.getOrElse("GSS_STAGING", sys.props("user.home") + "/gss-data/staging")
  val sizes = sys.env.getOrElse("BENCH_SIZES", "1 10 100").trim.split("\\s+").map(_.toInt).toSeq
  val schema = StructType(Seq("request_key", "yyyymm", "origin", "dest", "cargo_type").map(StructField(_, StringType)) ++
    Seq(StructField("weight_kg", LongType), StructField("confidence", StringType)))

  def query(n: Int): Array[String] = {
    val df = spark.read.schema(schema).option("sep", "\t").csv(hdfs + s"/lake/tmp/bench/requests_x$n")
    df.groupBy(F.col("yyyymm"), F.concat(F.col("origin"), F.lit("-"), F.col("dest")).as("route"))
      .agg(F.count(F.lit(1)).as("requests"), F.sum("weight_kg").as("weight_kg"),
           F.sum(F.when(F.col("confidence") === "high", 1).otherwise(0)).as("high"))
      .collect()
      .map(r => Seq(r.getString(0), r.getString(1), r.getLong(2), r.getLong(3), r.getLong(4)).mkString("\t"))
  }

  def run(): Int = {
    spark.sparkContext.setLogLevel("WARN")
    new java.io.File(staging + "/reports/bench").mkdirs()
    for (n <- sizes) {
      val t1 = System.nanoTime; val res = query(n); val s1 = (System.nanoTime - t1) / 1e9
      val t2 = System.nanoTime; query(n); val s2 = (System.nanoTime - t2) / 1e9
      val w = new java.io.PrintWriter(staging + s"/reports/bench/spark_x$n.tsv", "UTF-8")
      try res.sorted.foreach(l => w.println(l)) finally w.close()
      println(f"BENCH spark x$n first $s1%.2f s  second $s2%.2f s  (${res.length} result rows)")
    }
    0
  }
}

val gssRc = BenchSpark.run()
if (gssRc == 0) println("GSS_JOB_OK") else println("GSS_JOB_FAILED")
System.exit(gssRc)
