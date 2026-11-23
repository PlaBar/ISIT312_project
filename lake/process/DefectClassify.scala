// DefectClassify.scala - Layer 3: classify maintenance defect notes with keyword rules.
//
//   bash lake/run_spark.sh lake/process/DefectClassify.scala
//
// Each defect note ("hyd press low LH sys, topped up, monitor") gets
//   system_class  the earliest system term in the note (notes start with the system)
//   component     the longest (most specific) component term; a term written SYSTEM:term only
//                 counts for that system; otherwise the system's default component
//   severity_rule the highest severity among the matching terms (AOG > HIGH > MEDIUM > LOW)
// All rules are in lake/extract/dictionaries/defect_terms.txt and can be edited without
// touching this code. Terms match whole words, ignoring case.
//
// The maintenance system records a severity for most defects. The final severity is that
// value where present, otherwise the rule's (severity_from tells which). Where both exist
// they are compared, which tests the rules without the hidden truth file.
//
// Output: /lake/curated/fact_defect, rebuilt on every run.
// The notes are classified on the driver: 107 notes and a small dictionary. For millions of
// notes the same function would run as a Spark UDF with the dictionary broadcast.
//
// Spark 3.0.3, Scala 2.12, run with spark-shell -i (no sbt). One object = one unit for the REPL.

import org.apache.spark.sql.{functions => F}

object DefectClassify {
  val hdfs = sys.env.getOrElse("GSS_HDFS", "hdfs://localhost:8020")
  val processed = hdfs + "/lake/processed"
  val curated = hdfs + "/lake/curated"
  val project = sys.env.getOrElse("GSS_PROJECT", sys.props("user.dir"))
  val dictPath = project + "/lake/extract/dictionaries/defect_terms.txt"
  val rank = Map("LOW" -> 1, "MEDIUM" -> 2, "HIGH" -> 3, "AOG" -> 4)

  case class Rules(system: Seq[(String, String)], component: Seq[(String, String)], default: Map[String, String],
                   severity: Seq[(String, String)], defaultSeverity: String)

  def loadRules(): Rules = {
    val src = scala.io.Source.fromFile(dictPath, "UTF-8")
    val lines = try {
      src.getLines().map(_.trim).filter(l => l.nonEmpty && !l.startsWith("#")).map(_.split("\\|", -1).toSeq).toList
    } finally src.close()
    def section(name: String): Seq[(String, String)] = lines.filter(_.head == name).map(x => (x(1), x(2)))
    Rules(section("system"), section("component"), section("default").toMap, section("severity"),
          lines.find(_.head == "default_severity").map(_(1)).getOrElse("MEDIUM"))
  }

  def find(term: String, text: String): Option[Int] =
    ("""\b""" + java.util.regex.Pattern.quote(term.toLowerCase) + """\b""").r.findFirstMatchIn(text).map(_.start)

  // (system, component, severity, matched terms)
  def classify(desc: String, r: Rules): (String, String, String, String) = {
    val t = desc.toLowerCase
    val sysHits = r.system.flatMap { case (term, v) => find(term, t).map(pos => (pos, v, term)) }
    val system = if (sysHits.isEmpty) "UNKNOWN" else sysHits.minBy(h => (h._1, h._2))._2
    val compHits = r.component.flatMap { case (term, v) =>
      val i = term.lastIndexOf(':')
      val (scope, word) = if (i >= 0) (term.substring(0, i), term.substring(i + 1)) else ("", term)
      if (scope.nonEmpty && scope != system) None else find(word, t).map(_ => (word.length, v, word))
    }
    val component = if (compHits.isEmpty) r.default.getOrElse(system, "unknown") else compHits.maxBy(h => (h._1, h._2))._2
    val sevHits = r.severity.filter { case (term, _) => find(term, t).isDefined }
    val severity = if (sevHits.isEmpty) r.defaultSeverity else sevHits.maxBy(h => rank.getOrElse(h._2, 0))._2
    val matched = (sysHits.map(_._3) ++ compHits.map(_._3) ++ sevHits.map(_._1)).distinct.mkString(";")
    (system, component, severity, matched)
  }

  def run(): Int = {
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    println("DefectClassify: dictionary " + dictPath)
    val rules = loadRules()
    println("  rules: " + rules.system.size + " system, " + rules.component.size + " component, " +
            rules.severity.size + " severity terms")

    val defects = spark.read.parquet(processed + "/defect")
      .select("defect_id", "registration", "reported_date", "description", "severity", "resolved", "_src_file", "_run_id")
      .collect()
    val rows = defects.map { d =>
      val desc = if (d.isNullAt(3)) "" else d.getString(3)
      val (system, component, sevRule, matched) = classify(desc, rules)
      val sevSource = if (d.isNullAt(4)) null else d.getString(4)
      (d.getString(0), d.getString(1), d.getDate(2), desc, system, component, sevRule, sevSource,
       if (sevSource != null) sevSource else sevRule, if (sevSource != null) "source" else "rule",
       d.getString(5), matched, d.getString(6), d.getString(7))
    }.toSeq
    val df = rows.toDF("defect_key", "aircraft_key", "reported_date", "description", "system_class", "component",
                       "severity_rule", "severity_source", "severity", "severity_from", "resolved", "matched_terms",
                       "_src_file", "_run_id")
      .withColumn("date_key", F.date_format(F.col("reported_date"), "yyyyMMdd").cast("int"))
    df.coalesce(1).write.mode("overwrite").parquet(curated + "/fact_defect")
    val out = spark.read.parquet(curated + "/fact_defect")
    println("  fact_defect: " + out.count() + " rows")

    println("")
    println("Defects by system and severity")
    out.groupBy("system_class").pivot("severity", Seq("AOG", "HIGH", "MEDIUM", "LOW")).count()
      .na.fill(0).orderBy("system_class").show(false)
    val both = out.filter(F.col("severity_source").isNotNull)
    val agree = both.filter(F.col("severity_source") === F.col("severity_rule")).count()
    val unknown = out.filter(F.col("system_class") === "UNKNOWN").count()
    println("  severity from the rules where the maintenance system left it empty: " +
            out.filter(F.col("severity_from") === "rule").count())
    println("  rule severity = maintenance system severity: " + agree + " of " + both.count())
    println("  notes with no system term (UNKNOWN): " + unknown)
    out.select("description", "system_class", "component", "severity").show(5, false)
    0
  }
}

val gssRc = DefectClassify.run()
if (gssRc == 0) println("GSS_JOB_OK") else println("GSS_JOB_FAILED")
System.exit(gssRc)