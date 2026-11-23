// Cleanse.scala - Layer 3 (structured): raw -> processed, with cross-table checks.
//
//   bash lake/run_spark.sh lake/process/Cleanse.scala
//
// Reads every structured table, the event log and the documents from /lake/raw/ and writes
// typed, checked tables to /lake/processed/<table>/ (Parquet). Processed is rebuilt from raw
// on every run, so the job can be rerun at any time and always gives the same result.
//
// Preprocessing already checked each file on its own. Cleanse does what needs the whole lake:
//   1. Types        text columns become date, timestamp, decimal(14,2) or int.
//                   Lion and event-log times are UAE local time; they are stored in UTC,
//                   like the message times.
//   2. Duplicates   a key that arrives in more than one landing file (e.g. an invoice repeated
//                   in the next month's export) is kept once: from the latest load, and within
//                   a load from the earliest file. The other copies go to quarantine.
//   3. References   every row must point at rows that exist and passed their own checks:
//                     invoice.customer_id -> customer      invoice_line.invoice_id -> invoice
//                     payment.invoice_id  -> invoice       purchase_order.supplier_id -> supplier
//                     po_line.po_id -> purchase_order      flight.aircraft_id -> aircraft
//                     flight.origin_icao, dest_icao -> airport
//                     manifest.flight_no -> flight         event.flight_no -> flight
//                     maintenance_event, defect .registration -> aircraft_ref
//                   Parents are checked before children, so a row whose parent was rejected
//                   (here or in preprocessing) is quarantined too.
//   4. Flags        invoice.total that differs from the sum of its lines by more than 0.01
//                   is kept but flagged in _cleanse_flags.
//
// Outputs
//   /lake/processed/<table>/               typed rows that passed, with their lineage columns
//   /lake/quarantine/bad_rows/<table>/     rows that failed, with _reason
//   /lake/processed/dq_report/             one row per table (also printed and written to
//                                          $GSS_STAGING/reports/cleanse_dq_<time>.csv)
// Accounting check: processed + duplicates + quarantined must equal the raw rows of every
// table, otherwise the job fails.
//
// Spark 3.0.3, Scala 2.12, run with spark-shell -i (no sbt). One object = one unit for the REPL.

import org.apache.spark.sql.{Column, DataFrame, functions => F}
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.fs.{FileSystem, Path}

object Cleanse {
  val hdfs = sys.env.getOrElse("GSS_HDFS", "hdfs://localhost:8020")
  val raw = hdfs + "/lake/raw"
  val processed = hdfs + "/lake/processed"
  val badRows = hdfs + "/lake/quarantine/bad_rows"
  val staging = sys.env.getOrElse("GSS_STAGING", sys.props("user.home") + "/gss-data/staging")
  val runTime = java.time.LocalDateTime.now.format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"))
  val localZone = "Asia/Dubai"

  // name in processed, folder in raw, key, columns with their type
  case class T(name: String, rawPath: String, key: Seq[String], cols: Seq[(String, String)])

  val S = "string"; val D = "date"; val TS = "timestamp"; val DEC = "decimal"; val I = "int"
  val tables: Seq[T] = Seq(
    T("customer", "netsuite/customer", Seq("customer_id"),
      Seq("customer_id" -> S, "name" -> S, "country" -> S, "payment_terms" -> S)),
    T("supplier", "netsuite/supplier", Seq("supplier_id"),
      Seq("supplier_id" -> S, "name" -> S, "category" -> S, "country" -> S)),
    T("invoice", "netsuite/invoice", Seq("invoice_id"),
      Seq("invoice_id" -> S, "customer_id" -> S, "invoice_date" -> D, "due_date" -> D, "currency" -> S,
          "total" -> DEC, "status" -> S, "memo" -> S)),
    T("invoice_line", "netsuite/invoice_line", Seq("line_id"),
      Seq("line_id" -> S, "invoice_id" -> S, "description" -> S, "qty" -> DEC, "unit_price" -> DEC, "amount" -> DEC)),
    T("payment", "netsuite/payment", Seq("payment_id"),
      Seq("payment_id" -> S, "invoice_id" -> S, "paid_date" -> D, "amount" -> DEC, "method" -> S)),
    T("purchase_order", "netsuite/purchase_order", Seq("po_id"),
      Seq("po_id" -> S, "supplier_id" -> S, "po_date" -> D, "status" -> S, "total" -> DEC)),
    T("po_line", "netsuite/po_line", Seq("po_line_id"),
      Seq("po_line_id" -> S, "po_id" -> S, "item_desc" -> S, "qty" -> DEC, "unit_cost" -> DEC, "amount" -> DEC)),
    T("airport", "lion/airport", Seq("icao_code"),
      Seq("icao_code" -> S, "iata_code" -> S, "name" -> S, "country" -> S)),
    T("aircraft", "lion/aircraft", Seq("aircraft_id"),
      Seq("aircraft_id" -> S, "registration" -> S, "type" -> S, "max_payload_kg" -> I)),
    T("flight", "lion/flight", Seq("flight_no"),
      Seq("flight_no" -> S, "aircraft_id" -> S, "origin_icao" -> S, "dest_icao" -> S, "sched_dep" -> TS,
          "sched_arr" -> TS, "actual_dep" -> TS, "actual_arr" -> TS, "status" -> S)),
    T("manifest", "lion/manifest", Seq("manifest_id"),
      Seq("manifest_id" -> S, "flight_no" -> S, "cargo_desc" -> S, "weight_kg" -> DEC, "volume_m3" -> DEC,
          "consignee" -> S, "awb_no" -> S)),
    T("aircraft_ref", "maint/aircraft_ref", Seq("registration"),
      Seq("registration" -> S, "type" -> S)),
    T("maintenance_event", "maint/maintenance_event", Seq("event_id"),
      Seq("event_id" -> S, "registration" -> S, "event_type" -> S, "due_date" -> D, "done_date" -> D,
          "flight_hours_at" -> DEC, "cycles_at" -> I, "status" -> S)),
    T("defect", "maint/defect", Seq("defect_id"),
      Seq("defect_id" -> S, "registration" -> S, "reported_date" -> D, "description" -> S, "severity" -> S,
          "resolved" -> S)),
    T("event", "events/event", Seq("event_id"),
      Seq("event_id" -> S, "flight_no" -> S, "event_type" -> S, "event_time" -> TS, "detail" -> S, "source" -> S)),
    T("document_text", "documents/document", Seq("doc_file"),
      Seq("doc_file" -> S, "doc_type" -> S, "text" -> S, "chars" -> I))
  )
  val lineage = Seq("_dq_flags", "_src_file", "_run_id", "_landing_file", "_ingested_at")

  // per table: raw, duplicates, quarantined (by reason), processed, flagged
  val stats = scala.collection.mutable.LinkedHashMap[String, scala.collection.mutable.LinkedHashMap[String, Long]]()
  def stat(table: String, what: String, n: Long): Unit = {
    val m = stats.getOrElseUpdate(table, scala.collection.mutable.LinkedHashMap[String, Long]())
    m(what) = m.getOrElse(what, 0L) + n
  }

  def fs: FileSystem = FileSystem.get(new java.net.URI(hdfs), spark.sparkContext.hadoopConfiguration)

  def typedColumn(c: String, ty: String): Column = ty match {
    case "date" => F.to_date(F.col(c), "yyyy-MM-dd").as(c)
    case "timestamp" => F.to_utc_timestamp(F.to_timestamp(F.col(c), "yyyy-MM-dd'T'HH:mm:ss"), localZone).as(c)
    case "decimal" => F.col(c).cast("decimal(14,2)").as(c)
    case "int" => F.col(c).cast("int").as(c)
    case _ => F.when(F.col(c) === "", F.lit(null).cast("string")).otherwise(F.col(c).cast("string")).as(c)
  }

  // read raw, set types, keep one row per key
  def load(t: T): DataFrame = {
    val path = raw + "/" + t.rawPath
    if (!fs.exists(new Path(path))) throw new RuntimeException("raw table missing: " + path + " (run LandingToRaw first)")
    val src = spark.read.parquet(path)
    val keep = t.cols.map { case (c, ty) => typedColumn(c, ty) } ++
      lineage.map(c => if (src.columns.contains(c)) F.col(c) else F.lit(null).cast("string").as(c)) :+
      F.col("dt").as("raw_dt")
    val typed = src.select(keep: _*)
    val n = typed.count()
    stat(t.name, "raw_rows", n)
    val w = Window.partitionBy(t.key.map(F.col): _*).orderBy(F.col("_ingested_at").desc, F.col("_src_file").asc)
    val ranked = typed.withColumn("_rn", F.row_number().over(w)).cache()
    val dups = ranked.filter(F.col("_rn") > 1).drop("_rn")
    val nd = dups.count()
    stat(t.name, "duplicates_removed", nd)
    if (nd > 0) quarantine(t.name, dups, "duplicate key across landing files")
    ranked.filter(F.col("_rn") === 1).drop("_rn").cache()
  }

  val quarantined = scala.collection.mutable.Map[String, DataFrame]()
  def quarantine(table: String, rows: DataFrame, reason: String): Unit = {
    val r = rows.withColumn("_reason", F.lit(reason))
    quarantined(table) = quarantined.get(table).map(_.unionByName(r)).getOrElse(r)
  }

  // keep rows whose column points at an existing parent key; quarantine the rest
  def mustReference(table: String, df: DataFrame, col: String, parent: DataFrame, parentCol: String): DataFrame = {
    val keys = parent.select(F.col(parentCol).as("_parent_key")).distinct()
    val ok = df.join(keys, df(col) === keys("_parent_key"), "left_semi")
    val bad = df.join(keys, df(col) === keys("_parent_key"), "left_anti")
    val nb = bad.count()
    stat(table, "quarantined", nb)
    if (nb > 0) {
      stat(table, "not found: " + col, nb)
      quarantine(table, bad, col + " not found in " + parentCol)
    }
    ok.cache()
  }

  def write(name: String, df: DataFrame): Long = {
    val out = if (df.columns.contains("_cleanse_flags")) df else df.withColumn("_cleanse_flags", F.lit(""))
    out.coalesce(1).write.mode("overwrite").parquet(processed + "/" + name)
    val n = spark.read.parquet(processed + "/" + name).count()
    stat(name, "processed_rows", n)
    n
  }

  def run(): Int = {
    spark.sparkContext.setLogLevel("WARN")
    println("Cleanse: raw -> processed, " + hdfs)
    val df = tables.map(x => x.name -> load(x)).toMap

    // parents with no references of their own
    for (n <- Seq("customer", "supplier", "airport", "aircraft", "aircraft_ref", "document_text")) stat(n, "quarantined", 0)
    val customer = df("customer"); val supplier = df("supplier"); val airport = df("airport")
    val aircraft = df("aircraft"); val aircraftRef = df("aircraft_ref")

    // NetSuite
    val invoice = mustReference("invoice", df("invoice"), "customer_id", customer, "customer_id")
    val lines = mustReference("invoice_line", df("invoice_line"), "invoice_id", invoice, "invoice_id")
    val payment = mustReference("payment", df("payment"), "invoice_id", invoice, "invoice_id")
    val po = mustReference("purchase_order", df("purchase_order"), "supplier_id", supplier, "supplier_id")
    val poLine = mustReference("po_line", df("po_line"), "po_id", po, "po_id")

    // Lion: a flight needs its aircraft and both airports
    val f1 = mustReference("flight", df("flight"), "aircraft_id", aircraft, "aircraft_id")
    val f2 = mustReference("flight", f1, "origin_icao", airport, "icao_code")
    val flight = mustReference("flight", f2, "dest_icao", airport, "icao_code")
    val manifest = mustReference("manifest", df("manifest"), "flight_no", flight, "flight_no")
    val event = mustReference("event", df("event"), "flight_no", flight, "flight_no")

    // maintenance programme
    val maint = mustReference("maintenance_event", df("maintenance_event"), "registration", aircraftRef, "registration")
    val defect = mustReference("defect", df("defect"), "registration", aircraftRef, "registration")

    // flag invoices whose total differs from their lines
    val sums = lines.groupBy("invoice_id").agg(F.sum("amount").as("_lines_total"))
    val invoiceFlagged = invoice.join(sums, Seq("invoice_id"), "left")
      .withColumn("_cleanse_flags",
        F.when(F.col("_lines_total").isNull, "no_lines")
         .when(F.abs(F.col("total") - F.col("_lines_total")) > 0.01, "total_mismatch")
         .otherwise(""))
      .drop("_lines_total")
    stat("invoice", "flagged_rows", invoiceFlagged.filter(F.col("_cleanse_flags") =!= "").count())

    val out = Seq("customer" -> customer, "supplier" -> supplier, "invoice" -> invoiceFlagged,
      "invoice_line" -> lines, "payment" -> payment, "purchase_order" -> po, "po_line" -> poLine,
      "airport" -> airport, "aircraft" -> aircraft, "flight" -> flight, "manifest" -> manifest,
      "aircraft_ref" -> aircraftRef, "maintenance_event" -> maint, "defect" -> defect,
      "event" -> event, "document_text" -> df("document_text"))
    for ((name, d) <- out) write(name, d)

    // quarantine: one folder per table, replaced on every run
    for (x <- tables) {
      val path = badRows + "/" + x.name
      quarantined.get(x.name) match {
        case Some(q) => q.withColumn("_cleanse_run", F.lit(runTime)).coalesce(1).write.mode("overwrite").parquet(path)
        case None => fs.delete(new Path(path), true)
      }
    }

    // data-quality report and accounting check
    val rows = tables.map { x =>
      val s = stats(x.name)
      val rawN = s.getOrElse("raw_rows", 0L); val dupN = s.getOrElse("duplicates_removed", 0L)
      val qN = s.getOrElse("quarantined", 0L); val pN = s.getOrElse("processed_rows", 0L)
      val flN = s.getOrElse("flagged_rows", 0L)
      val reasons = s.keys.filter(_.startsWith("not found: ")).map(k => k.stripPrefix("not found: ") + " not found=" + s(k)).mkString("; ")
      (x.name, rawN, dupN, qN, pN, flN, reasons, rawN == dupN + qN + pN)
    }
    println("")
    println("Data-quality report (raw = duplicates + quarantined + processed)")
    println(f"  ${"table"}%-18s ${"raw"}%7s ${"dups"}%5s ${"quar."}%6s ${"processed"}%9s ${"flagged"}%8s  adds up  reasons")
    for (r <- rows)
      println(f"  ${r._1}%-18s ${r._2}%7d ${r._3}%5d ${r._4}%6d ${r._5}%9d ${r._6}%8d  ${if (r._8) "yes" else "NO"}%-7s  ${r._7}")
    import spark.implicits._
    val report = rows.map(r => (runTime, r._1, r._2, r._3, r._4, r._5, r._6, r._7, r._8))
      .toDF("cleanse_run", "table_name", "raw_rows", "duplicates_removed", "quarantined", "processed_rows",
            "flagged_rows", "quarantine_reasons", "adds_up")
    report.coalesce(1).write.mode("overwrite").parquet(processed + "/dq_report")
    val local = java.nio.file.Paths.get(staging, "reports", "cleanse_dq_" + runTime + ".csv")
    java.nio.file.Files.createDirectories(local.getParent)
    val csvLines = "table,raw_rows,duplicates_removed,quarantined,processed_rows,flagged_rows,adds_up,quarantine_reasons" +:
      rows.map(r => Seq(r._1, r._2, r._3, r._4, r._5, r._6, r._8, "\"" + r._7 + "\"").mkString(","))
    java.nio.file.Files.write(local, (csvLines.mkString("\n") + "\n").getBytes("UTF-8"))
    println("  report: " + local + " and " + processed + "/dq_report")

    if (rows.forall(_._8)) 0 else {
      println("  ACCOUNTING CHECK FAILED for: " + rows.filterNot(_._8).map(_._1).mkString(", "))
      1
    }
  }
}

val gssRc = Cleanse.run()
if (gssRc == 0) println("GSS_JOB_OK") else println("GSS_JOB_FAILED")
System.exit(gssRc)