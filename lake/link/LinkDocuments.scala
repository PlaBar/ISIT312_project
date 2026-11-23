// LinkDocuments.scala - Layer 3: link every document to its flight, shipment, quote and message.
//
//   bash lake/run_spark.sh lake/link/LinkDocuments.scala
//
// Needs: Cleanse (processed/document_text, flight, manifest, airport) and the message stream
// (processed/message). Rebuilds its outputs on every run.
//
// 1. Read each document (6,111 short texts, so on the driver): the numbers printed on it
//      AWB 176-50000101 (and whether its check digit is valid), flight GS0001 / "GS 1" / "GS1",
//      PO-000002, SHP-2023-0001, Q-2023-0003, PRM-2023-0001, pieces, weight
// 2. Message: the message that carries the document as an attachment, or that quotes its
//    number ("Please see our quotation Q-2023-0003 attached")
// 3. Flight, first rule that applies:
//      awb_manifest    a valid AWB found in the LION cargo manifest            high
//      flight_no       a flight number that exists in LION                     high
//      message_thread  the one flight named in the document's message thread
//                      (the booking: "Booking confirmed on GS0004 ...")        medium
//      awb_document    a valid AWB the manifest lacks, printed on an air
//                      waybill that is already linked                          high
//      pieces_weight   packing list only: the one air waybill with the same
//                      pieces and weight, dated within 4 days                  low
// 4. Filled in from the flight: the AWB number (a valid printed one, else the manifest's,
//    else the one on the flight's air waybill) and the shipment reference (from the
//    flight's packing list) for air waybills and permits.
//
// Outputs
//   /lake/curated/document_link        one row per document with the links and the method
//   /lake/curated/flight_documents     one row per flight that operated: which documents it
//                                      has, whether a clearance permit was required
//                                      (restricted route KBL MGQ JUB BGW, or DG / pharma /
//                                      valuables / vehicles on the air waybill) and present
//   /lake/quarantine/unlinked_documents  documents linked to neither a flight nor a message
//
// The same rules are in evaluation/link_twin.py, scored there against the hidden truth.
// Spark 3.0.3, Scala 2.12, run with spark-shell -i (no sbt). One object = one unit for the REPL.

import org.apache.spark.sql.{Row, functions => F}
import org.apache.spark.sql.types._

object LinkDocuments {
  val hdfs = sys.env.getOrElse("GSS_HDFS", "hdfs://localhost:8020")
  val processed = hdfs + "/lake/processed"
  val curated = hdfs + "/lake/curated"
  val quarantine = hdfs + "/lake/quarantine"
  val restricted = Set("KBL", "MGQ", "JUB", "BGW")
  val permitShc = Set("DGR", "PIL", "VAL")

  val awbRe = """\b(\d{3})-(\d{8})\b""".r
  val awbFull = """^(\d{3})-(\d{8})$""".r
  val flightRe = """\bGS ?(\d{1,4})\b""".r
  val poRe = """\bPO-\d{6}\b""".r
  val shpRe = """\bSHP-\d{4}-\d{4}\b""".r
  val quoteRe = """\bQ-\d{4}-\d{4}\b""".r
  val permitRe = """\bPRM-\d{4}-\d{4}\b""".r
  val piecesRe = """(?:Total pieces|Pieces):\s*(\d+)""".r
  val weightRe = """(?:Total weight|Gross weight):\s*([\d,]+)""".r
  val dateInName = """_(\d{8})_\d+\.txt$""".r
  val shcRe = """Special handling:\s*([A-Z]+)""".r
  val natureRe = """Nature of goods:\s*([^\n]+)""".r

  case class Doc(docFile: String, docType: String, docDate: String, awbPrinted: String, flightPrinted: String,
                 poId: String, shipmentRef: String, quoteNo: String, permitNo: String, pieces: String,
                 weight: String, shc: String, nature: String,
                 var awbValid: Boolean = false, var msgId: String = "", var threadId: String = "",
                 var flightNo: String = "", var method: String = "", var awbNo: String = "")

  def first(r: scala.util.matching.Regex, t: String, g: Int = 0): String =
    r.findFirstMatchIn(t).map(_.group(g)).getOrElse("")

  def gs(n: String): String = "GS%04d".format(n.toInt)

  def awbValid(a: String): Boolean = a match {
    case awbFull(_, s) => s.substring(0, 7).toInt % 7 == s.substring(7).toInt
    case _ => false
  }

  def parse(file: String, docType: String, text: String): Doc = Doc(
    file, docType, first(dateInName, file, 1), first(awbRe, text),
    flightRe.findFirstMatchIn(text).map(m => gs(m.group(1))).getOrElse(""),
    first(poRe, text), first(shpRe, text), first(quoteRe, text), first(permitRe, text),
    first(piecesRe, text, 1), first(weightRe, text, 1).replace(",", ""),
    first(shcRe, text, 1), first(natureRe, text, 1).trim)

  def daysBetween(a: String, b: String): Long = {
    val f = java.time.format.DateTimeFormatter.BASIC_ISO_DATE
    math.abs(java.time.temporal.ChronoUnit.DAYS.between(java.time.LocalDate.parse(a, f), java.time.LocalDate.parse(b, f)))
  }

  // same order of rules as evaluation/link_twin.py
  def link(docs: Seq[Doc], flights: Set[String], awbFlight: Map[String, String], flightAwb: Map[String, String],
           msgs: Seq[(String, String, String, Seq[String])]): Unit = {
    val byAttachment = scala.collection.mutable.Map[String, (String, String)]()
    val byQuoteNo = scala.collection.mutable.Map[String, (String, String)]()
    val threadFlights = scala.collection.mutable.Map[String, Set[String]]()
    for ((msgId, threadId, body, atts) <- msgs) {
      atts.foreach(a => byAttachment(a) = (msgId, threadId))
      quoteRe.findAllIn(body).foreach(q => if (!byQuoteNo.contains(q)) byQuoteNo(q) = (msgId, threadId))
      flightRe.findAllMatchIn(body).foreach(m => threadFlights(threadId) = threadFlights.getOrElse(threadId, Set()) + gs(m.group(1)))
    }
    for (d <- docs) {
      d.awbValid = d.awbPrinted.nonEmpty && awbValid(d.awbPrinted)
      val (m, t) = byAttachment.get(d.docFile).orElse(byQuoteNo.get(d.quoteNo)).getOrElse(("", ""))
      d.msgId = m; d.threadId = t
      if (d.awbValid && awbFlight.contains(d.awbPrinted)) { d.flightNo = awbFlight(d.awbPrinted); d.method = "awb_manifest" }
      else if (flights.contains(d.flightPrinted)) { d.flightNo = d.flightPrinted; d.method = "flight_no" }
      else if (d.threadId.nonEmpty && threadFlights.getOrElse(d.threadId, Set()).size == 1) {
        d.flightNo = threadFlights(d.threadId).head; d.method = "message_thread"
      }
    }
    val awbDocFlight = docs.filter(d => d.docType == "AWB" && d.flightNo.nonEmpty && d.awbValid)
      .groupBy(_.awbPrinted).map { case (a, ds) => a -> ds.head.flightNo }
    for (d <- docs if d.flightNo.isEmpty && d.awbValid && awbDocFlight.contains(d.awbPrinted)) {
      d.flightNo = awbDocFlight(d.awbPrinted); d.method = "awb_document"
    }
    val awbs = docs.filter(d => d.docType == "AWB" && d.flightNo.nonEmpty)
    for (d <- docs if d.docType == "PACKING" && d.flightNo.isEmpty && d.pieces.nonEmpty && d.weight.nonEmpty) {
      val fits = awbs.filter(a => a.pieces == d.pieces && a.weight == d.weight && a.docDate.nonEmpty &&
                                  d.docDate.nonEmpty && daysBetween(a.docDate, d.docDate) <= 4).map(_.flightNo).toSet
      if (fits.size == 1) { d.flightNo = fits.head; d.method = "pieces_weight" }
    }
  }

  def run(): Int = {
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val docRows = spark.read.parquet(processed + "/document_text").select("doc_file", "doc_type", "text").collect()
    val docs = docRows.map(r => parse(r.getString(0), r.getString(1), r.getString(2))).toSeq
    val flight = spark.read.parquet(processed + "/flight")
    val flights = flight.select("flight_no").as[String].collect().toSet
    val manifest = spark.read.parquet(processed + "/manifest").select("flight_no", "awb_no")
      .filter(F.col("awb_no").rlike("^\\d{3}-\\d{8}$")).as[(String, String)].collect()
    val awbFlight = manifest.map { case (f, a) => a -> f }.toMap
    val flightAwb = manifest.groupBy(_._1).map { case (f, xs) => f -> xs.head._2 }
    // only messages that can link something: attachments, a quote number or a flight number
    val msgs = spark.read.parquet(processed + "/message")
      .filter(F.col("n_attachments") > 0 || F.col("body").rlike("Q-\\d{4}-\\d{4}|GS ?\\d"))
      .select("msg_id", "thread_id", "body", "attachments").collect()
      .map(r => (r.getString(0), Option(r.getString(1)).getOrElse(""), Option(r.getString(2)).getOrElse(""),
                 Option(r.getString(3)).map(_.split(";").toSeq.filter(_.nonEmpty)).getOrElse(Seq())))
      .toSeq
    println(s"Read ${docs.size} documents, ${flights.size} flights, ${awbFlight.size} manifest AWBs, ${msgs.size} messages")

    link(docs, flights, awbFlight, flightAwb, msgs)
    val awbDocOfFlight = docs.filter(d => d.docType == "AWB" && d.flightNo.nonEmpty && d.awbValid)
      .groupBy(_.flightNo).map { case (f, ds) => f -> ds.head.awbPrinted }
    val refOfFlight = docs.filter(d => d.docType == "PACKING" && d.flightNo.nonEmpty && d.shipmentRef.nonEmpty)
      .groupBy(_.flightNo).map { case (f, ds) => f -> ds.head.shipmentRef }
    val conf = Map("awb_manifest" -> "high", "flight_no" -> "high", "awb_document" -> "high", "message_thread" -> "medium", "pieces_weight" -> "low")
    def opt(s: String): String = if (s == null || s.isEmpty) null else s

    val rows = docs.map { d =>
      val f = d.flightNo
      val awb = if (d.awbValid) d.awbPrinted else flightAwb.getOrElse(f, awbDocOfFlight.getOrElse(f, ""))
      val ref = if (d.docType != "QUOTE" && d.shipmentRef.isEmpty) refOfFlight.getOrElse(f, "") else d.shipmentRef
      val docNo = d.docType match {
        case "AWB" => d.awbPrinted case "QUOTE" => d.quoteNo case "PERMIT" => d.permitNo case _ => d.shipmentRef }
      Row(d.docFile, d.docType, opt(docNo), opt(awb), d.awbPrinted.isEmpty || d.awbValid, opt(f), opt(ref),
          opt(d.quoteNo), opt(d.poId), opt(d.msgId), opt(d.threadId), opt(d.method), conf.get(d.method).orNull,
          f.nonEmpty || d.msgId.nonEmpty, opt(d.shc), opt(d.nature))
    }
    val schema = StructType(Seq("doc_file", "doc_type", "doc_no", "awb_no").map(StructField(_, StringType)) ++
      Seq(StructField("awb_check_ok", BooleanType)) ++
      Seq("flight_key", "shipment_ref", "quote_no", "po_id", "msg_id", "thread_id", "link_method", "link_confidence")
        .map(StructField(_, StringType)) ++
      Seq(StructField("linked", BooleanType), StructField("special_handling", StringType), StructField("nature_of_goods", StringType)))
    val links = spark.createDataFrame(java.util.Arrays.asList(rows: _*), schema)
      .withColumn("_linked_at", F.current_timestamp()).persist()
    links.coalesce(1).write.mode("overwrite").parquet(curated + "/document_link")
    links.filter(!F.col("linked")).withColumn("_reason", F.lit("no flight and no message found"))
      .write.mode("overwrite").parquet(quarantine + "/unlinked_documents")

    // documents per flight, and the permit check
    val airport = spark.read.parquet(processed + "/airport").select("icao_code", "iata_code")
    val flown = flight.filter(F.col("status") =!= "CANCELLED")
      .join(airport.withColumnRenamed("icao_code", "origin_icao").withColumnRenamed("iata_code", "origin_iata"), Seq("origin_icao"), "left")
      .join(airport.withColumnRenamed("icao_code", "dest_icao").withColumnRenamed("iata_code", "dest_iata"), Seq("dest_icao"), "left")
      .select(F.col("flight_no").as("flight_key"), F.col("origin_iata"), F.col("dest_iata"))
    val perFlight = links.filter(F.col("flight_key").isNotNull).groupBy("flight_key").agg(
      F.sum(F.when(F.col("doc_type") === "AWB", 1).otherwise(0)).as("n_awb"),
      F.sum(F.when(F.col("doc_type") === "PACKING", 1).otherwise(0)).as("n_packing"),
      F.sum(F.when(F.col("doc_type") === "PERMIT", 1).otherwise(0)).as("n_permit"),
      F.sum(F.when(F.col("doc_type") === "QUOTE", 1).otherwise(0)).as("n_quote"),
      F.max(F.when(F.col("doc_type") === "AWB", F.col("special_handling"))).as("awb_special_handling"),
      F.max(F.when(F.col("doc_type") === "AWB", F.col("nature_of_goods"))).as("awb_nature_of_goods"))
    val fd = flown.join(perFlight, Seq("flight_key"), "left").na.fill(0L, Seq("n_awb", "n_packing", "n_permit", "n_quote"))
      .withColumn("permit_required",
        F.col("origin_iata").isin(restricted.toSeq: _*) || F.col("dest_iata").isin(restricted.toSeq: _*) ||
        F.coalesce(F.col("awb_special_handling").isin(permitShc.toSeq: _*), F.lit(false)) ||
        F.coalesce(F.lower(F.col("awb_nature_of_goods")).startsWith("vehicle"), F.lit(false)))
      .withColumn("permit_ok", !F.col("permit_required") || F.col("n_permit") > 0)
      .withColumn("documents_complete", F.col("n_awb") > 0 && F.col("n_packing") > 0 && F.col("permit_ok"))
      .persist()
    fd.coalesce(1).write.mode("overwrite").parquet(curated + "/flight_documents")

    println("Documents by type and link method:")
    links.groupBy("doc_type", "link_method").count()
      .withColumn("link_method", F.coalesce(F.col("link_method"), F.lit("(message only / unlinked)")))
      .orderBy("doc_type", "link_method").collect()
      .foreach(r => println(f"  ${r.getString(0)}%-8s ${r.getString(1)}%-26s ${r.getLong(2)}%5d"))
    val nUnlinked = links.filter(!F.col("linked")).count()
    val nBadAwb = links.filter(!F.col("awb_check_ok")).count()
    println(s"Unlinked (to quarantine/unlinked_documents): $nUnlinked")
    println(s"Documents with an AWB that fails the check digit: $nBadAwb (linked by flight number instead)")
    val s = fd.agg(F.count("*"), F.sum(F.col("permit_required").cast("int")), F.sum((!F.col("permit_ok")).cast("int")),
                   F.sum((!F.col("documents_complete")).cast("int"))).first()
    println(s"Flights operated ${s.getLong(0)}: permit required ${s.getLong(1)}, permit missing ${s.getLong(2)}, " +
            s"documents incomplete ${s.getLong(3)}")
    fd.filter(!F.col("documents_complete")).select("flight_key", "origin_iata", "dest_iata", "n_awb", "n_packing", "n_permit", "permit_required")
      .orderBy("flight_key").show(8, false)
    links.unpersist(); fd.unpersist()
    0
  }
}

val gssRc = LinkDocuments.run()
if (gssRc == 0) println("GSS_JOB_OK") else println("GSS_JOB_FAILED")
System.exit(gssRc)