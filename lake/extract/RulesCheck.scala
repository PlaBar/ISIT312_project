// RulesCheck.scala - check Rules.scala on the messages released to the lake.
//
//   python3 evaluation/rules_twin.py                                           (first)
//   bash lake/run_spark.sh lake/extract/Rules.scala lake/extract/RulesCheck.scala
//
// Reads every message in /lake/landing/messages, runs Rules.extract on each one (on the
// driver: a few thousand short texts) and compares the result with
//   1. the Python twin ($GSS_STAGING/reports/rules_twin_predictions.csv), message by message
//      and field by field. Both must agree on every message: a difference means the Scala
//      and the Python versions of a rule are not the same, which is a bug to fix first.
//   2. the hidden truth (generators/labels/requests_truth.csv): request detection and the
//      accuracy of each field. These numbers belong to the evaluation (Step 30), not to the
//      pipeline, which never reads the truth file.
// The job fails (no GSS_JOB_OK) if a single message disagrees with the twin.
//
// Spark 3.0.3, Scala 2.12, run with spark-shell -i (no sbt). One object = one unit for the REPL.

import org.apache.spark.sql.{functions => F}

object RulesCheck {
  val hdfs = sys.env.getOrElse("GSS_HDFS", "hdfs://localhost:8020")
  val staging = sys.env.getOrElse("GSS_STAGING", sys.props("user.home") + "/gss-data/staging")
  val fields = Seq("is_request", "origin_iata", "dest_iata", "cargo_type", "weight_kg", "ready_date",
                   "special_handling", "confidence", "missing")
  val scored = Seq("origin_iata", "dest_iata", "cargo_type", "weight_kg", "ready_date", "special_handling")

  // small CSV files without quoted commas -> header -> rows keyed by the first column
  def readCsv(path: String): Map[String, Map[String, String]] = {
    val src = scala.io.Source.fromFile(path, "UTF-8")
    val lines = try src.getLines().toList finally src.close()
    val header = lines.head.split(",", -1).toSeq
    lines.tail.filter(_.nonEmpty).map { l =>
      val v = l.split(",", -1).toSeq
      v.head -> header.zip(v).toMap
    }.toMap
  }

  def asRow(x: Rules.Extraction): Map[String, String] = Map(
    "is_request" -> (if (x.isRequest) "1" else "0"),
    "origin_iata" -> x.origin.getOrElse(""), "dest_iata" -> x.dest.getOrElse(""),
    "cargo_type" -> x.cargoType.getOrElse(""), "weight_kg" -> x.weightKg.map(_.toString).getOrElse(""),
    "ready_date" -> x.readyDate.getOrElse(""), "special_handling" -> x.specialHandling.getOrElse(""),
    "confidence" -> x.confidence.getOrElse(""), "missing" -> x.missing.getOrElse(""))

  def pct(a: Int, b: Int): String = if (b == 0) "n/a" else f"${100.0 * a / b}%.1f%%"

  def run(): Int = {
    spark.sparkContext.setLogLevel("WARN")
    val twinFile = staging + "/reports/rules_twin_predictions.csv"
    val truthFile = Rules.project + "/generators/labels/requests_truth.csv"
    if (!new java.io.File(twinFile).exists) {
      println("Twin predictions not found: run  python3 evaluation/rules_twin.py  first")
      return 1
    }
    val msgs = try {
      spark.read.json(hdfs + "/lake/landing/messages/*/*.jsonl")
        .select(F.col("msg_id"), F.col("direction"), F.col("sender.handle").as("handle"),
                F.col("sent_at"), F.col("body"))
        .collect()
    } catch {
      case e: org.apache.spark.sql.AnalysisException =>
        println("No messages in /lake/landing/messages yet (" + e.getMessage.take(80) + ")")
        println("Release some first:  python3 preprocess/pp_messages.py --limit 3000")
        return 1
    }
    println(s"Rules: ${Rules.dicts.airports.size} airport terms, ${Rules.dicts.cargo.size} cargo phrases, " +
            s"${Rules.dicts.shc.size} handling phrases, ${Rules.dicts.relative.size} relative date phrases")

    val t0 = System.nanoTime
    val preds = msgs.map { r =>
      r.getString(0) -> asRow(Rules.extract(r.getString(1), r.getString(2), r.getString(3), r.getString(4)))
    }.toMap
    val secs = (System.nanoTime - t0) / 1e9
    println(f"Read ${preds.size} messages from the landing zone in $secs%.1f s (${preds.size / math.max(secs, 0.001)}%.0f messages/s)")

    // 1. Scala vs Python twin
    val twin = readCsv(twinFile)
    val common = preds.keys.filter(twin.contains).toSeq.sorted
    val diffs = for (id <- common; f <- fields if preds(id)(f) != twin(id)(f))
      yield (id, f, twin(id)(f), preds(id)(f))
    val diffMsgs = diffs.map(_._1).distinct.size
    println(s"Agreement with the Python twin: ${common.size - diffMsgs} of ${common.size} messages identical")
    diffs.take(10).foreach { case (id, f, py, sc) => println(s"  DIFF $id $f: python '$py' scala '$sc'") }

    // 2. against the hidden truth
    val truth = readCsv(truthFile)
    val ids = preds.keys.filter(truth.contains).toSeq
    def isReq(m: Map[String, String]) = m("is_request") == "1"
    val tp = ids.count(i => isReq(preds(i)) && isReq(truth(i)))
    val fp = ids.count(i => isReq(preds(i)) && !isReq(truth(i)))
    val fn = ids.count(i => !isReq(preds(i)) && isReq(truth(i)))
    println(s"Request detection: precision ${pct(tp, tp + fp)}  recall ${pct(tp, tp + fn)}  (tp $tp fp $fp fn $fn)")
    val both = ids.filter(i => isReq(preds(i)) && isReq(truth(i)))
    println(s"Field accuracy on the ${both.size} requests found:")
    for (f <- scored) println(f"  $f%-17s ${pct(both.count(i => preds(i)(f) == truth(i)(f)), both.size)}")
    val byConf = both.groupBy(i => preds(i)("confidence")).toSeq.sortBy(_._1)
    for ((c, xs) <- byConf)
      println(f"  confidence $c%-7s ${xs.size}%6d requests, all fields right ${pct(xs.count(i => scored.forall(f => preds(i)(f) == truth(i)(f))), xs.size)}")

    if (common.isEmpty) { println("No messages in common with the twin file"); 1 }
    else if (diffMsgs > 0) { println("Scala and Python disagree: fix Rules.scala or rules_twin.py first"); 1 }
    else 0
  }
}

val gssRc = RulesCheck.run()
if (gssRc == 0) println("GSS_JOB_OK") else println("GSS_JOB_FAILED")
System.exit(gssRc)