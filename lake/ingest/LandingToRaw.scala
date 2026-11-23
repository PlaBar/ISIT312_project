// LandingToRaw.scala - Layer 2: copy landing files into the immutable raw zone as Parquet.
//
//   bash lake/run_spark.sh lake/ingest/LandingToRaw.scala
//
// For every source table it lists the landing files, skips those already in raw (raw keeps
// the name of the landing file each row came from, in _landing_file), reads only the new
// ones and appends them to
//     /lake/raw/<system>/<table>/dt=<load date>/   (Parquet, one file per table per load)
// Raw is append-only: nothing in it is ever changed or deleted by the pipeline.
//
// Sources: the 14 structured tables (CSV), the event log and the documents (JSON lines).
// Messages are not loaded here: StreamMessages.scala (Step 23) streams them into raw.
//
// Values stay exactly as preprocessing released them (CSV columns stay text); the columns
// _landing_file, _ingested_at and dt are added. Typing and cleansing happen in Cleanse.scala.
//
// Layer 2 check: afterwards every released file in staging/preprocess_manifest.csv is
// compared with raw, by (_src_file, _run_id): the rows in raw must equal rows_accepted.
// Any difference is listed and the job fails. The comparison is also written to
// $GSS_STAGING/reports/landing_to_raw_<time>.csv.
//
// Environment: GSS_HDFS (default hdfs://localhost:8020), GSS_STAGING, GSS_DT (load date,
// default today). Spark 3.0.3, Scala 2.12, run with spark-shell -i (no sbt).
// The whole job is one object so spark-shell reads it as a single unit.

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.hadoop.fs.{FileSystem, Path}

object LandingToRaw {
  val hdfs = sys.env.getOrElse("GSS_HDFS", "hdfs://localhost:8020")
  val landing = hdfs + "/lake/landing"
  val raw = hdfs + "/lake/raw"
  val staging = sys.env.getOrElse("GSS_STAGING", sys.props("user.home") + "/gss-data/staging")
  val dt = sys.env.getOrElse("GSS_DT", java.time.LocalDate.now.toString)
  val manifestSources = Seq("structured", "events", "documents")

  case class Src(source: String, system: String, table: String, pattern: String, format: String) {
    def target: String = raw + "/" + system + "/" + table
  }

  // landing files of a CSV table: <table>.csv or monthly <table>_<yyyymm>.csv
  def csv(system: String, table: String): Src =
    Src("structured", system, table,
        landing + "/exports/" + system + "/{" + table + ".csv," + table + "_[0-9]*.csv}", "csv")

  val sources: Seq[Src] = Seq(
    csv("netsuite", "customer"), csv("netsuite", "supplier"), csv("netsuite", "invoice"),
    csv("netsuite", "invoice_line"), csv("netsuite", "payment"), csv("netsuite", "purchase_order"),
    csv("netsuite", "po_line"),
    csv("lion", "airport"), csv("lion", "aircraft"), csv("lion", "flight"), csv("lion", "manifest"),
    csv("maint", "aircraft_ref"), csv("maint", "maintenance_event"), csv("maint", "defect"),
    Src("events", "events", "event", landing + "/exports/events/*.jsonl", "json"),
    Src("documents", "documents", "document", landing + "/documents/*/*.jsonl", "json")
  )

  def fs: FileSystem = FileSystem.get(new java.net.URI(hdfs), spark.sparkContext.hadoopConfiguration)

  def exists(path: String): Boolean = fs.exists(new Path(path))

  def landingFiles(pattern: String): Seq[String] = {
    val found = fs.globStatus(new Path(pattern))
    if (found == null) Seq.empty
    else found.filter(_.isFile).map(_.getPath.toString).filterNot(_.contains("/_")).sorted.toSeq
  }

  def alreadyLoaded(target: String): Set[String] =
    if (!exists(target)) Set.empty
    else spark.read.parquet(target).select("_landing_file").distinct().collect().map(_.getString(0)).toSet

  def read(s: Src, files: Seq[String]): DataFrame =
    if (s.format == "csv")
      spark.read.option("header", "true").option("escape", "\"").option("multiLine", "true").csv(files: _*)
    else
      spark.read.json(files: _*)

  def load(): Long = {
    var total = 0L
    for (s <- sources) {
      val all = landingFiles(s.pattern)
      val done = alreadyLoaded(s.target)
      val todo = all.filterNot(done.contains)
      if (todo.isEmpty) {
        println(f"  ${s.system}%-9s ${s.table}%-18s landing files ${all.size}%4d   new    0")
      } else {
        val df = read(s, todo)
          .withColumn("_landing_file", F.input_file_name())
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("dt", F.lit(dt))
          .cache()
        val n = df.count()
        df.coalesce(1).write.mode("append").partitionBy("dt").parquet(s.target)
        df.unpersist()
        total += n
        println(f"  ${s.system}%-9s ${s.table}%-18s landing files ${all.size}%4d   new ${todo.size}%4d   rows $n%7d")
      }
    }
    total
  }

  // Layer 2 check: rows in raw per (_src_file, _run_id) = rows_accepted in the manifest
  def check(): Int = {
    val manifestPath = "file://" + staging + "/preprocess_manifest.csv"
    val manifest = spark.read.option("header", "true").option("escape", "\"").csv(manifestPath)
      .filter(F.col("status") === "RELEASED" && F.col("source_type").isin(manifestSources: _*))
      .select(F.col("source_type"), F.col("file_name").as("_src_file"), F.col("run_id").as("_run_id"),
              F.col("rows_accepted").cast("long").as("manifest_rows"))
    val counts = sources.filter(s => exists(s.target)).map { s =>
      spark.read.parquet(s.target).groupBy("_src_file", "_run_id").count()
        .withColumnRenamed("count", "raw_rows").withColumn("raw_source", F.lit(s.source))
    }
    if (counts.isEmpty) {
      println("Layer 2 check: raw is empty")
      return 1
    }
    val rawCounts = counts.reduce(_ union _)
      .groupBy("_src_file", "_run_id").agg(F.sum("raw_rows").as("raw_rows"), F.first("raw_source").as("raw_source"))
    val cmp = manifest.join(rawCounts, Seq("_src_file", "_run_id"), "full_outer")
      .select(F.coalesce(F.col("source_type"), F.col("raw_source")).as("source"),
              F.col("_src_file"), F.col("_run_id"),
              F.coalesce(F.col("manifest_rows"), F.lit(0L)).as("manifest_rows"),
              F.coalesce(F.col("raw_rows"), F.lit(0L)).as("raw_rows"),
              F.col("source_type").isNull.as("not_in_manifest"))
      .withColumn("state",
        F.when(F.col("not_in_manifest"), "in raw, not in manifest")
         .when(F.col("manifest_rows") === F.col("raw_rows"), "ok")
         .when(F.col("raw_rows") === 0, "not loaded")
         .otherwise("row count differs"))
      .cache()

    val summary = cmp.groupBy("source").agg(
        F.count(F.lit(1)).as("files"),
        F.sum("manifest_rows").as("manifest_rows"),
        F.sum("raw_rows").as("raw_rows"),
        F.sum(F.when(F.col("state") =!= "ok", 1).otherwise(0)).as("problems"))
      .orderBy("source").collect()
    println("")
    println("Layer 2 check: raw rows vs rows_accepted in the preprocess manifest")
    println(f"  ${"source"}%-12s ${"files"}%7s ${"manifest rows"}%14s ${"raw rows"}%10s ${"problems"}%9s")
    for (r <- summary)
      println(f"  ${r.getString(0)}%-12s ${r.getLong(1)}%7d ${r.getLong(2)}%14d ${r.getLong(3)}%10d ${r.getLong(4)}%9d")

    val bad = cmp.filter(F.col("state") =!= "ok").orderBy("source", "_src_file").collect()
    val report = staging + "/reports/landing_to_raw_" +
      java.time.LocalDateTime.now.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss")) + ".csv"
    val lines = Seq("source,_src_file,_run_id,manifest_rows,raw_rows,state") ++
      cmp.orderBy("source", "_src_file").collect().map(r =>
        Seq(r.getAs[String]("source"), r.getAs[String]("_src_file"), r.getAs[String]("_run_id"),
            r.getAs[Long]("manifest_rows").toString, r.getAs[Long]("raw_rows").toString,
            r.getAs[String]("state")).mkString(","))
    java.nio.file.Files.createDirectories(java.nio.file.Paths.get(staging, "reports"))
    java.nio.file.Files.write(java.nio.file.Paths.get(report), (lines.mkString("\n") + "\n").getBytes("UTF-8"))
    println("  details: " + report)
    cmp.unpersist()

    if (bad.isEmpty) {
      println("  every released file is in raw with the right number of rows")
      0
    } else {
      println("  " + bad.length + " file(s) differ, for example:")
      for (r <- bad.take(10))
        println("    " + r.getAs[String]("state") + ": " + r.getAs[String]("_src_file") + " (" +
                r.getAs[String]("_run_id") + ") manifest " + r.getAs[Long]("manifest_rows") +
                ", raw " + r.getAs[Long]("raw_rows"))
      1
    }
  }

  def run(): Int = {
    spark.sparkContext.setLogLevel("WARN")
    println("LandingToRaw: load date dt=" + dt + ", lake " + hdfs)
    val n = load()
    println("Rows loaded this run: " + n)
    check()
  }
}

val gssRc = LandingToRaw.run()
if (gssRc == 0) println("GSS_JOB_OK") else println("GSS_JOB_FAILED")
System.exit(gssRc)