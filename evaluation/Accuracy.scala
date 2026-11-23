// Accuracy.scala - Step 30: score everything the lake inferred against the hidden truth.
//
//   bash lake/run_spark.sh evaluation/Accuracy.scala
//
// The generators wrote what is really true (generators/labels/*.csv); the pipeline never
// reads those files. This job compares the lake's outputs with them:
//   A. flight references   curated/flight_key_map    vs flight_ref_truth.csv   (Step 19)
//   B. defect classes      curated/fact_defect       vs defect_truth.csv       (Step 20)
//   C. customer requests   curated/request           vs requests_truth.csv     (Steps 21-23)
//   D. document links      curated/document_link     vs documents_truth.csv    (Step 24)
// (Preprocessing is scored separately against the planted errors:
//  python3 evaluation/check_preprocessing.py structured|events|messages|documents)
//
// Measures
//   precision  of the answers the lake gave, the share that is right
//   recall     of the true answers, the share the lake found
//   reachable recall: the same, counting only records that survived preprocessing and Cleanse
//              (a flight rejected in Step 10 cannot be linked to by any rule); the gap between
//              the two recalls is the cost of data quality, the rest is the cost of the rules
//   accuracy   for classes (defects, request fields): share of exactly right values, where
//              "nothing" counts as right when the truth also has nothing
//
// Output: printed, and $GSS_STAGING/reports/accuracy_<time>.csv (area, item, measure, value, n).
// Spark 3.0.3, Scala 2.12, run with spark-shell -i (no sbt). One object = one unit for the REPL.

import org.apache.spark.sql.{DataFrame, functions => F}

object Accuracy {
  val hdfs = sys.env.getOrElse("GSS_HDFS", "hdfs://localhost:8020")
  val project = sys.env.getOrElse("GSS_PROJECT", sys.props("user.dir"))
  val staging = sys.env.getOrElse("GSS_STAGING", sys.props("user.home") + "/gss-data/staging")
  val rows = scala.collection.mutable.ArrayBuffer[(String, String, String, Double, Long)]()

  def truth(name: String): DataFrame =
    spark.read.option("header", "true").csv("file://" + project + "/generators/labels/" + name + ".csv")
  def lake(path: String): DataFrame = spark.read.parquet(hdfs + path)
  def blankToNull(c: String) = F.when(F.trim(F.col(c)) === "", F.lit(null)).otherwise(F.col(c).cast("string"))

  def pct(a: Long, b: Long): Double = if (b == 0) Double.NaN else 100.0 * a / b
  def record(area: String, item: String, measure: String, a: Long, b: Long): Double = {
    val v = pct(a, b); rows += ((area, item, measure, v, b)); v
  }
  def show(v: Double): String = if (v.isNaN) "   n/a" else f"$v%6.1f"

  // precision / recall of a link column p (prediction) against t (truth), both may be null
  def linkScores(area: String, item: String, df: DataFrame, p: String, t: String, reachable: Option[String] = None): Unit = {
    val r = df.agg(
      F.sum(F.when(F.col(p).isNotNull, 1).otherwise(0)).as("pred"),
      F.sum(F.when(F.col(p).isNotNull && F.col(p) === F.col(t), 1).otherwise(0)).as("ok"),
      F.sum(F.when(F.col(t).isNotNull, 1).otherwise(0)).as("want"),
      F.sum(F.when(F.col(t).isNotNull && reachable.map(F.col).getOrElse(F.lit(true)), 1).otherwise(0)).as("want_reach")).first()
    val (pred, ok, want, wantR) = (r.getLong(0), r.getLong(1), r.getLong(2), r.getLong(3))
    val pr = record(area, item, "precision", ok, pred)
    val rc = record(area, item, "recall", ok, want)
    val rr = record(area, item, "reachable_recall", ok, wantR)
    println(f"  $item%-28s ${show(pr)} ${show(rc)} ${show(rr)}   ($ok%5d right of $pred%5d given, $want%5d true, $wantR%5d reachable)")
  }

  def header(title: String): Unit = {
    println("")
    println(title)
    println(f"  ${""}%-28s   prec recall reach.rec")
  }

  def run(): Int = {
    spark.sparkContext.setLogLevel("WARN")

    // ---------------------------------------------------------------- A. flight references
    header("A. Flight references of invoices and PO lines (BuildCurated flight-reference repair)")
    val flights = lake("/lake/processed/flight").select(F.col("flight_no").as("tf")).withColumn("in_lake", F.lit(true))
    val km = lake("/lake/curated/flight_key_map")
      .select(F.col("source_system"), F.col("source_ref"), blankToNull("flight_no").as("pred"), F.col("confidence"))
    val a = truth("flight_ref_truth").select(F.col("source_system"), F.col("source_ref"), blankToNull("flight_no").as("truth"))
      .join(km, Seq("source_system", "source_ref"), "left")
      .join(flights, F.col("truth") === F.col("tf"), "left")
      .withColumn("reach", F.col("in_lake").isNotNull && F.col("confidence").isNotNull)   // record and flight both survived
      .persist()
    for (s <- Seq("netsuite.invoice", "netsuite.po_line"))
      linkScores("flight_ref", s, a.filter(F.col("source_system") === s), "pred", "truth", Some("reach"))
    println("  precision by confidence of the match:")
    for (s <- Seq("netsuite.invoice", "netsuite.po_line"); c <- Seq("high", "medium", "low")) {
      val x = a.filter(F.col("source_system") === s && F.col("confidence") === c && F.col("pred").isNotNull)
      val n = x.count(); val ok = x.filter(F.col("pred") === F.col("truth")).count()
      if (n > 0) println(f"    $s%-17s $c%-7s ${show(record("flight_ref", s + "/" + c, "precision", ok, n))}%%  of $n%5d")
    }

    // ---------------------------------------------------------------- B. defects
    println("")
    println("B. Defect classification (DefectClassify keyword rules)")
    val b = truth("defect_truth")
      .join(lake("/lake/curated/fact_defect").withColumnRenamed("defect_key", "defect_id")
              .withColumnRenamed("system_class", "p_system").withColumnRenamed("component", "p_component"),
            Seq("defect_id"), "inner").persist()
    val nb = b.count()
    for ((item, p, t) <- Seq(("system_class", "p_system", "system_class"), ("component", "p_component", "component"),
                             ("severity (rules alone)", "severity_rule", "severity_class"),
                             ("severity (final)", "severity", "severity_class"))) {
      val ok = b.filter(F.lower(F.col(p)) === F.lower(F.col(t))).count()
      println(f"  $item%-28s ${show(record("defect", item, "accuracy", ok, nb))}%%  ($ok of $nb)")
    }

    // ---------------------------------------------------------------- C. requests
    header("C. Customer requests (Rules.scala in the message stream)")
    val inLake = lake("/lake/processed/message").select("msg_id").distinct().withColumn("msg_in_lake", F.lit(true))
    val req = lake("/lake/curated/request").dropDuplicates("request_key")
      .select(F.col("request_key").as("msg_id"), F.lit(true).as("found"), F.col("confidence"),
              F.col("origin_iata").as("p_origin"), F.col("dest_iata").as("p_dest"), F.col("cargo_type").as("p_cargo"),
              F.col("weight_kg").cast("string").as("p_weight"), F.col("ready_date").cast("string").as("p_ready"),
              F.col("special_handling").as("p_shc"))
    val c = truth("requests_truth")
      .join(inLake, Seq("msg_id"), "left").join(req, Seq("msg_id"), "left")
      .withColumn("is_req", F.col("is_request") === "1")
      .withColumn("p_req", F.when(F.col("found").isNotNull, F.lit("yes")))
      .withColumn("t_req", F.when(F.col("is_req"), F.lit("yes")))
      .withColumn("reach", F.col("msg_in_lake").isNotNull)
      .persist()
    linkScores("request", "request detection", c, "p_req", "t_req", Some("reach"))
    val both = c.filter(F.col("found").isNotNull && F.col("is_req")).persist()
    val nBoth = both.count()
    println(f"  field accuracy on the $nBoth requests found (nothing = nothing counts as right):")
    val fields = Seq(("origin_iata", "p_origin"), ("dest_iata", "p_dest"), ("cargo_type", "p_cargo"),
                     ("weight_kg", "p_weight"), ("ready_date", "p_ready"), ("special_handling", "p_shc"))
    for ((t, p) <- fields) {
      val ok = both.filter(blankToNull(p) <=> blankToNull(t)).count()
      println(f"    $t%-26s ${show(record("request", t, "accuracy", ok, nBoth))}%%")
    }
    val allRight = fields.map { case (t, p) => blankToNull(p) <=> blankToNull(t) }.reduce(_ && _)
    println("  all six fields right, by the confidence the rules gave:")
    for (conf <- Seq("high", "medium", "low")) {
      val x = both.filter(F.col("confidence") === conf); val n = x.count()
      val ok = x.filter(allRight).count()
      println(f"    $conf%-8s ${show(record("request", "all_fields/" + conf, "accuracy", ok, n))}%%  of $n%6d requests")
    }

    // ---------------------------------------------------------------- D. documents
    header("D. Document links (LinkDocuments)")
    val dl = lake("/lake/curated/document_link")
    val d = truth("documents_truth").join(dl.select(F.col("doc_file"), F.col("flight_key").as("p_flight"),
              F.col("awb_no").as("p_awb"), F.col("shipment_ref").as("p_ref"), F.col("quote_no").as("p_quote"),
              F.col("msg_id").as("p_msg"), F.col("link_method")), Seq("doc_file"), "left")
      .withColumn("reach", F.col("link_method").isNotNull || F.col("p_msg").isNotNull || F.col("p_flight").isNotNull)
      .withColumn("t_flight", blankToNull("flight_no")).withColumn("t_awb", blankToNull("awb_no"))
      .withColumn("t_ref", blankToNull("shipment_ref")).withColumn("t_quote", blankToNull("quote_no"))
      .withColumn("t_msg", blankToNull("msg_id")).persist()
    for ((item, p, t) <- Seq(("flight", "p_flight", "t_flight"), ("awb_no", "p_awb", "t_awb"), ("shipment_ref", "p_ref", "t_ref"),
                             ("quote_no", "p_quote", "t_quote"), ("message", "p_msg", "t_msg")))
      linkScores("document", item, d, p, t)
    println("  flight precision by link method:")
    d.filter(F.col("p_flight").isNotNull).groupBy("link_method")
      .agg(F.count(F.lit(1)).as("n"), F.sum(F.when(F.col("p_flight") === F.col("t_flight"), 1).otherwise(0)).as("ok"))
      .orderBy("link_method").collect().foreach { r =>
        val m = r.getString(0); val n = r.getLong(1); val ok = r.getLong(2)
        println(f"    $m%-16s ${show(record("document", "flight/" + m, "precision", ok, n))}%%  of $n%5d")
      }

    // ---------------------------------------------------------------- save
    val file = staging + "/reports/accuracy_" + java.time.LocalDateTime.now.toString.replace(":", "").take(15) + ".csv"
    val w = new java.io.PrintWriter(file, "UTF-8")
    try {
      w.println("area,item,measure,value_pct,n")
      rows.foreach { case (ar, it, m, v, n) => w.println(s"$ar,$it,$m,${if (v.isNaN) "" else f"$v%.2f"},$n") }
    } finally w.close()
    println("")
    println("Saved: " + file)
    0
  }
}

val gssRc = Accuracy.run()
if (gssRc == 0) println("GSS_JOB_OK") else println("GSS_JOB_FAILED")
System.exit(gssRc)