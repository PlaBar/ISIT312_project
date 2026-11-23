// BuildCurated.scala - Layer 3: flight-reference repair, canonical keys, dimensions and facts.
//
//   bash lake/run_spark.sh lake/process/BuildCurated.scala
//
// Reads /lake/processed/ (written by Cleanse.scala) and writes the curated zone,
// /lake/curated/<table>/ (Parquet), rebuilt on every run.
//
// 1. Flight-reference repair. Invoice memos and PO descriptions name a flight only in free
//    text ("Cargo charter Dubai to Entebbe, 1 Mar", "JetA-1 DWC 12/03 flt to NBO"). For each
//    one the job reads
//      - airports: IATA codes and city names ("Dubai" may mean DWC or DXB), and their roles:
//        "ex X" is the origin; of two airports the first is the origin; a single airport is
//        the origin for fuel ("fuel", "uplift", "Jet A1") and the destination otherwise
//      - a date in any of 7 formats; a date without a year takes the year that puts it
//        nearest the document date (invoices follow the flight, POs come before it)
//      - an aircraft registration (A6-GSB or A6GSB)
//      - for POs whose text names no airport: the supplier's station ("Nairobi Ground
//        Services" -> NBO, destination; fuel suppliers -> origin; navigation -> its country)
//    and keeps the flights that fit. When several fit, the document's numbers decide:
//      - invoice: the charter-fee line's quantity is the round-trip block hours, and its unit
//        price is the rate-card rate of the aircraft type (in AED if the invoice is in AED)
//      - fuel PO: litres / (block hours x fuel burn of the aircraft type) lies in 0.93-1.12
//    Without a date the flight is searched in a window (invoice date -3..0 days, PO date
//    +1..+5 days), but only when such numbers exist. All lines of one PO serve one flight,
//    so an unresolved line takes the flight its sibling lines resolved to.
//    A reference stays unmatched when no flight or more than one fits: a wrong match would
//    put revenue or cost on the wrong flight. Results with method and confidence:
//    curated/flight_key_map. Measured against the hidden truth in Step 30 (Accuracy.scala).
//    Rate card and fuel burn are reference data [A] (dim_aircraft).
//
// 2. Star schema, written as Spark SQL queries (keys: flight_key = flight_no, customer_key = customer_id,
//    supplier_key = supplier_id, aircraft_key = registration, airport_key = IATA code,
//    date_key = yyyyMMdd as int; dates of flights are UAE local dates)
//      dim_date, dim_customer, dim_supplier, dim_airport, dim_aircraft
//      fact_flight       one row per flight: times (UTC), delay, block hours, cargo weight,
//                        customer, revenue_usd, cost_usd, margin_usd, event counts
//      fact_invoice      invoices in USD with payments, outstanding amount and their flight
//      fact_cost         PO lines in USD with supplier category and their flight
//      fact_maintenance  maintenance events with lateness
//      fact_event        operational events with their flight and date
//
// Layer 3 check: prints one flight's revenue, cost and margin, and how revenue and cost
// split between matched and unmatched documents.
//
// Spark 3.0.3, Scala 2.12, run with spark-shell -i (no sbt). One object = one unit for the REPL.

import org.apache.spark.sql.{DataFrame, Row, functions => F}
import java.time.LocalDate

object BuildCurated {
  val hdfs = sys.env.getOrElse("GSS_HDFS", "hdfs://localhost:8020")
  val processed = hdfs + "/lake/processed"
  val curated = hdfs + "/lake/curated"
  val localZone = "Asia/Dubai"
  val AED = 3.6725

  // reference data [A]: rate card (USD per block hour) and fuel burn (litres per block hour)
  val rate = Map("B737-400F" -> 8500.0, "A300B4-200F" -> 13500.0, "IL-76TD" -> 15500.0)
  val burn = Map("B737-400F" -> 3000.0, "A300B4-200F" -> 7000.0, "IL-76TD" -> 8500.0)
  // city names used in free text [A]; "Dubai" may mean either Dubai airport
  val cityOf = Seq("DWC" -> "Dubai", "DXB" -> "Dubai", "SHJ" -> "Sharjah", "NBO" -> "Nairobi",
    "ADD" -> "Addis Ababa", "EBB" -> "Entebbe", "JUB" -> "Juba", "MGQ" -> "Mogadishu", "FIH" -> "Kinshasa",
    "LOS" -> "Lagos", "KBL" -> "Kabul", "BGW" -> "Baghdad", "EBL" -> "Erbil", "TAS" -> "Tashkent",
    "KHI" -> "Karachi", "DAC" -> "Dhaka")
  val cityCodes: Map[String, Set[String]] =
    cityOf.groupBy(_._2.toLowerCase).map { case (city, xs) => city -> xs.map(_._1).toSet }

  def p(name: String): DataFrame = spark.read.parquet(processed + "/" + name)

  // ---------------------------------------------------------------- flight-reference repair
  case class Fl(no: String, date: LocalDate, o: String, d: String, reg: String, typ: String, bh: Double)

  val months = Map("jan" -> 1, "feb" -> 2, "mar" -> 3, "apr" -> 4, "may" -> 5, "jun" -> 6,
    "jul" -> 7, "aug" -> 8, "sep" -> 9, "oct" -> 10, "nov" -> 11, "dec" -> 12)
  val datePatterns = Seq(
    """\b(\d{1,2})[/.-](\d{1,2})[/.-](\d{4})\b""".r -> "dmy4",
    """\b(\d{1,2})\.(\d{1,2})\.(\d{2})\b""".r -> "dmy2",
    """\b(\d{1,2})-([A-Za-z]{3})-(\d{2})\b""".r -> "dmon2",
    """\b(\d{1,2}) ([A-Za-z]{3})[a-z]* (\d{4})\b""".r -> "dmon4",
    """\b(\d{1,2}) ([A-Za-z]{3})[a-z]*\b""".r -> "dmon",
    """\b(\d{1,2})/(\d{1,2})\b""".r -> "dm")
  val iataToken = """\b[A-Z]{3}\b""".r
  val exPattern = """\bex ([A-Za-z ]+)""".r
  val fuelWords = """(?i)\b(fuel|uplift|jet ?a-?1)\b""".r
  val regPattern = """\bA6-?(GS[A-D])\b""".r

  def findDate(text: String, doc: LocalDate, after: Boolean): Option[LocalDate] = {
    for ((rx, kind) <- datePatterns) {
      val found = rx.findFirstMatchIn(text)
      if (found.isDefined) {
        val g = found.get
        val result = try {
          val day = g.group(1).toInt
          val month = if (kind.startsWith("dmon")) months.getOrElse(g.group(2).toLowerCase.take(3), 0) else g.group(2).toInt
          if (month == 0) None
          else if (kind == "dmy4" || kind == "dmon4") Some(LocalDate.of(g.group(3).toInt, month, day))
          else if (kind == "dmy2" || kind == "dmon2") Some(LocalDate.of(2000 + g.group(3).toInt, month, day))
          else {
            var best: Option[(Int, LocalDate)] = None
            for (y <- Seq(doc.getYear - 1, doc.getYear, doc.getYear + 1)) {
              val d = try { Some(LocalDate.of(y, month, day)) } catch { case _: Exception => None }
              if (d.isDefined) {
                val gap = (if (after) doc.toEpochDay - d.get.toEpochDay else d.get.toEpochDay - doc.toEpochDay).toInt
                val score = math.abs(gap) + (if (gap >= 0) 0 else 30)
                if (best.isEmpty || score < best.get._1) best = Some((score, d.get))
              }
            }
            best.map(_._2)
          }
        } catch { case _: Exception => None }
        if (result.isDefined) return result
      }
    }
    None
  }

  def findAirports(text: String, iata: Set[String]): Seq[(Int, Set[String])] = {
    val codes = iataToken.findAllMatchIn(text).filter(m => iata.contains(m.matched)).map(m => (m.start, Set(m.matched))).toSeq
    val low = text.toLowerCase
    val cities = cityCodes.toSeq.flatMap { case (city, cs) =>
      ("""\b""" + java.util.regex.Pattern.quote(city) + """\b""").r.findAllMatchIn(low).map(m => (m.start, cs)).toSeq }
    (codes ++ cities).sortBy(_._1)
  }

  def roles(text: String, found: Seq[(Int, Set[String])]): (Option[Set[String]], Option[Set[String]]) = {
    if (found.isEmpty) return (None, None)
    val ex = exPattern.findFirstMatchIn(text)
    if (ex.isDefined && found.size >= 2) {
      val pos = ex.get.start(1)
      val o = found.filter(_._1 == pos).map(_._2)
      if (o.nonEmpty) return (Some(o.head), Some(found.filter(_._1 != pos).head._2))
    }
    if (found.size >= 2) (Some(found(0)._2), Some(found(1)._2))
    else if (fuelWords.findFirstIn(text).isDefined) (Some(found(0)._2), None)
    else (None, Some(found(0)._2))
  }

  def findReg(text: String): Option[String] = regPattern.findFirstMatchIn(text).map(m => "A6-" + m.group(1))

  // station: (role, codes) with role origin, dest or touch; evidence: does a flight fit the numbers
  def matchRef(text: String, doc: LocalDate, after: Boolean, iata: Set[String], byDate: Map[LocalDate, Seq[Fl]],
               station: Option[(String, Set[String])], evidence: Option[Fl => Boolean]): (Option[String], String) = {
    val d = findDate(text, doc, after)
    val found = findAirports(text, iata)
    var (o, de) = roles(text, found)
    var touch: Option[Set[String]] = None
    if (found.isEmpty && station.isDefined) {
      val (role, codes) = station.get
      if (role == "origin") o = Some(codes) else if (role == "dest") de = Some(codes) else touch = Some(codes)
    }
    val reg = findReg(text)
    if (d.isEmpty && o.isEmpty && de.isEmpty && touch.isEmpty && evidence.isEmpty) return (None, "no reference")
    def ok(f: Fl): Boolean = o.forall(_.contains(f.o)) && de.forall(_.contains(f.d)) &&
      touch.forall(t => t.contains(f.o) || t.contains(f.d)) && reg.forall(_ == f.reg)
    var how = ""
    val pool: Seq[Fl] =
      if (d.isDefined) { how = "date"; byDate.getOrElse(d.get, Seq.empty) }
      else if (evidence.isEmpty) return (None, "no date")
      else {
        how = "window"
        val days = if (after) -3 to 0 else 1 to 5
        days.flatMap(k => byDate.getOrElse(doc.plusDays(k), Seq.empty))
      }
    var c = pool.filter(ok)
    if (c.size > 1 && evidence.isDefined) {
      val c2 = c.filter(evidence.get)
      if (c2.nonEmpty) { c = c2; how += "+amounts" }
    }
    if (c.size == 1) {
      if (found.nonEmpty || station.isDefined) how += (if (o.isDefined && de.isDefined) "+route" else "+station")
      (Some(c.head.no), how)
    } else (None, if (c.nonEmpty) "ambiguous" else "no match")
  }

  def confidence(method: String): String =
    if (method.startsWith("date") && method.endsWith("+route")) "high"
    else if (method.startsWith("date") || method == "same PO") "medium"
    else if (method.startsWith("window")) "low"
    else "none"

  def dec(r: Row, i: Int): Option[Double] = if (r.isNullAt(i)) None else Some(r.getDecimal(i).doubleValue)
  def str(r: Row, i: Int): String = if (r.isNullAt(i)) "" else r.getString(i)

  def repair(): DataFrame = {
    import spark.implicits._
    val airport = p("airport")
    val iata = airport.select("iata_code").collect().map(_.getString(0)).toSet
    val countryOf = airport.select("iata_code", "country").collect().map(r => r.getString(0) -> r.getString(1)).toMap
    val flights = p("flight")
      .join(airport.select(F.col("icao_code").as("origin_icao"), F.col("iata_code").as("o")), Seq("origin_icao"), "left")
      .join(airport.select(F.col("icao_code").as("dest_icao"), F.col("iata_code").as("d")), Seq("dest_icao"), "left")
      .join(p("aircraft").select("aircraft_id", "registration", "type"), Seq("aircraft_id"), "left")
      .select(F.col("flight_no"), F.to_date(F.from_utc_timestamp(F.col("sched_dep"), localZone)).as("dep_date"),
              F.col("o"), F.col("d"), F.col("registration"), F.col("type"),
              ((F.unix_timestamp(F.col("sched_arr")) - F.unix_timestamp(F.col("sched_dep"))) / 3600.0).as("bh"))
      .collect().map(r => Fl(r.getString(0), r.getDate(1).toLocalDate, r.getString(2), r.getString(3),
                             r.getString(4), r.getString(5), r.getDouble(6)))
    val byDate = flights.toSeq.groupBy(_.date)
    println("  flights to match against: " + flights.length)

    // invoices, with their charter-fee line
    val charter = p("invoice_line").filter(F.col("description") === "Charter fee")
      .groupBy("invoice_id").agg(F.first("qty").as("cqty"), F.first("unit_price").as("cprice"))
    val invRows = p("invoice").join(charter, Seq("invoice_id"), "left")
      .select("invoice_id", "memo", "invoice_date", "currency", "cqty", "cprice").collect()
    val invResults = invRows.map { r =>
      val text = str(r, 1)
      val doc = r.getDate(2).toLocalDate
      val ev: Option[Fl => Boolean] = for (qty <- dec(r, 4); price <- dec(r, 5)) yield {
        val usd = if (str(r, 3) == "AED") price / AED else price
        (f: Fl) => math.abs(f.bh * 2 - qty) < 0.01 && math.abs(rate.getOrElse(f.typ, -1.0) - usd) < 1.0
      }
      val (fno, how) = matchRef(text, doc, after = true, iata, byDate, None, ev)
      ("netsuite.invoice", r.getString(0), "", fno, how, text)
    }

    // PO lines, with PO date and supplier
    val poRows = p("po_line")
      .join(p("purchase_order").select("po_id", "po_date", "supplier_id"), Seq("po_id"))
      .join(p("supplier").select(F.col("supplier_id"), F.col("name").as("sup_name"), F.col("category"),
                                 F.col("country").as("sup_country")), Seq("supplier_id"), "left")
      .select("po_line_id", "po_id", "item_desc", "qty", "po_date", "sup_name", "category", "sup_country").collect()
    val poResults = poRows.map { r =>
      val text = str(r, 2)
      val doc = r.getDate(4).toLocalDate
      val supName = str(r, 5).toLowerCase
      val category = str(r, 6)
      val station: Option[(String, Set[String])] = {
        val city = cityCodes.keys.find(c => supName.startsWith(c))
        if (city.isDefined) {
          val role = category match {
            case "FUEL" => "origin"
            case "GROUND_HANDLING" | "AIRPORT_CHARGES" => "dest"
            case _ => ""
          }
          if (role.nonEmpty) Some((role, cityCodes(city.get))) else None
        } else if (category == "NAVIGATION")
          Some(("touch", countryOf.filter(_._2 == str(r, 7)).keySet))
        else None
      }
      val ev: Option[Fl => Boolean] =
        if (category == "FUEL" && !r.isNullAt(3)) {
          val litres = r.getDecimal(3).doubleValue
          Some((f: Fl) => { val x = litres / (f.bh * burn.getOrElse(f.typ, 1e9)); x >= 0.93 && x <= 1.12 })
        } else None
      val (fno, how) = matchRef(text, doc, after = false, iata, byDate, station, ev)
      ("netsuite.po_line", r.getString(0), r.getString(1), fno, how, text)
    }

    // lines of one PO serve one flight
    val resolvedPerPo = poResults.filter(_._4.isDefined).groupBy(_._3).map { case (po, xs) => po -> xs.flatMap(_._4).toSet }
    val poFinal = poResults.map { x =>
      if (x._4.isEmpty && resolvedPerPo.get(x._3).exists(_.size == 1))
        (x._1, x._2, x._3, Some(resolvedPerPo(x._3).head), "same PO", x._6)
      else x
    }

    (invResults ++ poFinal).toSeq
      .map(x => (x._1, x._2, x._4, x._5, confidence(x._5), x._6))
      .toDF("source_system", "source_ref", "flight_no", "method", "confidence", "text")
  }

  // ---------------------------------------------------------------- star schema, in SQL
  // Processed tables are available as p_<table>; each curated table becomes a view as soon as
  // it is written, so later queries can use it (fact_flight is built from fact_invoice and
  // fact_cost). The same queries work in Hive with few changes.
  def dk(c: String): String = s"CAST(date_format($c, 'yyyyMMdd') AS INT)"
  def usd(c: String): String = s"CASE WHEN currency = 'AED' THEN ROUND($c / $AED, 2) ELSE $c END"

  val queries: Seq[(String, String)] = Seq(
    "dim_date" -> s"""
      SELECT ${dk("d")} AS date_key, d AS date, year(d) AS year, quarter(d) AS quarter, month(d) AS month,
             date_format(d, 'MMMM') AS month_name, dayofmonth(d) AS day, date_format(d, 'EEEE') AS weekday,
             dayofweek(d) IN (1, 7) AS is_weekend
      FROM (SELECT explode(sequence(to_date('2023-09-01'), to_date('2026-12-31'), interval 1 day)) AS d) days""",

    "dim_customer" -> """
      SELECT customer_id AS customer_key, name, country, payment_terms FROM p_customer""",

    "dim_supplier" -> """
      SELECT supplier_id AS supplier_key, name, category, country FROM p_supplier""",

    "dim_airport" -> """
      SELECT a.iata_code AS airport_key, a.icao_code, a.name, c.city, a.country
      FROM p_airport a LEFT JOIN city_ref c ON a.iata_code = c.iata_code""",

    "dim_aircraft" -> """
      SELECT a.registration AS aircraft_key, a.aircraft_id, a.type, a.max_payload_kg,
             r.rate_usd_per_bh, r.fuel_l_per_bh
      FROM p_aircraft a LEFT JOIN aircraft_type_ref r ON a.type = r.type""",

    "fact_invoice" -> s"""
      WITH paid AS (
        SELECT p.invoice_id, SUM(CASE WHEN i.currency = 'AED' THEN ROUND(p.amount / $AED, 2) ELSE p.amount END) AS paid_usd,
               MAX(p.paid_date) AS last_paid_date
        FROM p_payment p JOIN p_invoice i ON p.invoice_id = i.invoice_id
        GROUP BY p.invoice_id),
      m AS (
        SELECT source_ref AS invoice_id, flight_no, method FROM flight_key_map WHERE source_system = 'netsuite.invoice')
      SELECT i.invoice_id AS invoice_key, i.customer_id AS customer_key, ${dk("i.invoice_date")} AS date_key,
             ${dk("i.due_date")} AS due_date_key, m.flight_no AS flight_key, m.method AS match_method,
             i.currency, i.total, CAST(${usd("i.total")} AS DECIMAL(14,2)) AS total_usd,
             CAST(COALESCE(paid.paid_usd, 0) AS DECIMAL(14,2)) AS paid_usd,
             CAST(CASE WHEN i.status = 'VOID' THEN 0 ELSE ${usd("i.total")} - COALESCE(paid.paid_usd, 0) END
                  AS DECIMAL(14,2)) AS outstanding_usd,
             i.status, paid.last_paid_date, datediff(paid.last_paid_date, i.invoice_date) AS days_to_pay,
             i._cleanse_flags, i._src_file, i._run_id
      FROM p_invoice i
      LEFT JOIN m ON i.invoice_id = m.invoice_id
      LEFT JOIN paid ON i.invoice_id = paid.invoice_id""",

    "fact_cost" -> s"""
      WITH m AS (
        SELECT source_ref AS po_line_id, flight_no, method FROM flight_key_map WHERE source_system = 'netsuite.po_line')
      SELECT l.po_line_id AS cost_key, l.po_id, o.supplier_id AS supplier_key, s.category,
             ${dk("o.po_date")} AS date_key, m.flight_no AS flight_key, m.method AS match_method, l.item_desc,
             l.amount AS amount_usd, o.status AS po_status, l._src_file, l._run_id
      FROM p_po_line l
      JOIN p_purchase_order o ON l.po_id = o.po_id
      LEFT JOIN p_supplier s ON o.supplier_id = s.supplier_id
      LEFT JOIN m ON l.po_line_id = m.po_line_id""",

    "fact_flight" -> s"""
      WITH rev AS (
        SELECT flight_key, SUM(total_usd) AS revenue_usd, COUNT(*) AS n_invoices, MIN(customer_key) AS customer_key
        FROM fact_invoice WHERE flight_key IS NOT NULL AND status <> 'VOID' GROUP BY flight_key),
      cost AS (
        SELECT flight_key, SUM(amount_usd) AS cost_usd, COUNT(*) AS n_cost_lines
        FROM fact_cost WHERE flight_key IS NOT NULL AND po_status <> 'CANCELLED' GROUP BY flight_key),
      cargo AS (
        SELECT flight_no, SUM(weight_kg) AS cargo_weight_kg, MIN(awb_no) AS awb_no FROM p_manifest GROUP BY flight_no),
      ev AS (
        SELECT flight_no, COUNT(*) AS n_events,
               SUM(CASE WHEN event_type = 'DELAY' THEN 1 ELSE 0 END) AS n_delay_events,
               SUM(CASE WHEN event_type = 'AOG' THEN 1 ELSE 0 END) AS n_aog_events
        FROM p_event GROUP BY flight_no)
      SELECT f.flight_no AS flight_key, ${dk(s"from_utc_timestamp(f.sched_dep, '$localZone')")} AS date_key,
             ac.registration AS aircraft_key, o.iata_code AS origin_key, d.iata_code AS dest_key,
             rev.customer_key, f.status,
             f.sched_dep AS sched_dep_utc, f.sched_arr AS sched_arr_utc,
             f.actual_dep AS actual_dep_utc, f.actual_arr AS actual_arr_utc,
             ROUND((unix_timestamp(f.sched_arr) - unix_timestamp(f.sched_dep)) / 3600.0, 2) AS block_hours,
             CAST((unix_timestamp(f.actual_dep) - unix_timestamp(f.sched_dep)) / 60 AS INT) AS dep_delay_min,
             cargo.cargo_weight_kg, cargo.awb_no,
             COALESCE(rev.n_invoices, 0) AS n_invoices,
             CAST(COALESCE(rev.revenue_usd, 0) AS DECIMAL(14,2)) AS revenue_usd,
             COALESCE(cost.n_cost_lines, 0) AS n_cost_lines,
             CAST(COALESCE(cost.cost_usd, 0) AS DECIMAL(14,2)) AS cost_usd,
             CAST(COALESCE(rev.revenue_usd, 0) - COALESCE(cost.cost_usd, 0) AS DECIMAL(14,2)) AS margin_usd,
             COALESCE(ev.n_events, 0) AS n_events, COALESCE(ev.n_delay_events, 0) AS n_delay_events,
             COALESCE(ev.n_aog_events, 0) AS n_aog_events,
             f._src_file, f._run_id
      FROM p_flight f
      LEFT JOIN p_airport o ON f.origin_icao = o.icao_code
      LEFT JOIN p_airport d ON f.dest_icao = d.icao_code
      LEFT JOIN p_aircraft ac ON f.aircraft_id = ac.aircraft_id
      LEFT JOIN rev ON f.flight_no = rev.flight_key
      LEFT JOIN cost ON f.flight_no = cost.flight_key
      LEFT JOIN cargo ON f.flight_no = cargo.flight_no
      LEFT JOIN ev ON f.flight_no = ev.flight_no""",

    "fact_maintenance" -> s"""
      SELECT event_id AS maintenance_key, registration AS aircraft_key, event_type,
             ${dk("due_date")} AS due_date_key, ${dk("done_date")} AS done_date_key, status,
             flight_hours_at, cycles_at, datediff(done_date, due_date) AS days_late, _src_file, _run_id
      FROM p_maintenance_event""",

    "fact_event" -> s"""
      SELECT event_id AS event_key, flight_no AS flight_key, event_type, event_time AS event_time_utc,
             ${dk(s"from_utc_timestamp(event_time, '$localZone')")} AS date_key, detail, source, _src_file, _run_id
      FROM p_event"""
  )

  val processedTables = Seq("customer", "supplier", "invoice", "invoice_line", "payment", "purchase_order",
    "po_line", "airport", "aircraft", "flight", "manifest", "event", "maintenance_event")

  def run(): Int = {
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    println("BuildCurated: processed -> curated, " + hdfs)

    println("1. Flight-reference repair")
    val keyMap = repair()
    keyMap.coalesce(1).write.mode("overwrite").parquet(curated + "/flight_key_map")
    spark.read.parquet(curated + "/flight_key_map").createOrReplaceTempView("flight_key_map")
    spark.sql("""SELECT source_system, confidence, COUNT(*) AS n FROM flight_key_map
                 GROUP BY source_system, confidence ORDER BY source_system, confidence""").collect()
      .foreach(r => println(f"  ${r.getString(0)}%-17s ${r.getString(1)}%-7s ${r.getLong(2)}%6d"))

    println("2. Dimensions and facts")
    for (t <- processedTables) p(t).createOrReplaceTempView("p_" + t)
    cityOf.toDF("iata_code", "city").createOrReplaceTempView("city_ref")
    rate.toSeq.map { case (t, r) => (t, r, burn(t)) }.toDF("type", "rate_usd_per_bh", "fuel_l_per_bh")
      .createOrReplaceTempView("aircraft_type_ref")
    for ((name, sql) <- queries) {
      spark.sql(sql).coalesce(1).write.mode("overwrite").parquet(curated + "/" + name)
      val written = spark.read.parquet(curated + "/" + name)
      written.createOrReplaceTempView(name)
      println(f"  $name%-18s ${written.count()}%7d rows")
    }

    println("")
    println("Layer 3 check: one flight - revenue, cost, margin")
    spark.sql("""SELECT flight_key, date_key, origin_key, dest_key, aircraft_key, customer_key,
                        revenue_usd, cost_usd, margin_usd, n_invoices, n_cost_lines
                 FROM fact_flight WHERE revenue_usd > 0 AND cost_usd > 0 AND date_key >= 20240301
                 ORDER BY date_key, flight_key LIMIT 1""").show(false)
    val c = spark.sql("""SELECT
        (SELECT SUM(total_usd) FROM fact_invoice WHERE status <> 'VOID' AND flight_key IS NOT NULL),
        (SELECT SUM(total_usd) FROM fact_invoice WHERE status <> 'VOID'),
        (SELECT SUM(amount_usd) FROM fact_cost WHERE po_status <> 'CANCELLED' AND flight_key IS NOT NULL),
        (SELECT SUM(amount_usd) FROM fact_cost WHERE po_status <> 'CANCELLED'),
        (SELECT COUNT(*) FROM fact_flight WHERE revenue_usd > 0),
        (SELECT COUNT(*) FROM fact_flight WHERE cost_usd > 0),
        (SELECT COUNT(*) FROM fact_flight)""").first()
    val (revM, revAll, costM, costAll) = (c.getDecimal(0).doubleValue, c.getDecimal(1).doubleValue,
                                          c.getDecimal(2).doubleValue, c.getDecimal(3).doubleValue)
    println(f"  revenue on flights $revM%,.0f of $revAll%,.0f USD (${100 * revM / revAll}%.1f%%)")
    println(f"  cost on flights    $costM%,.0f of $costAll%,.0f USD (${100 * costM / costAll}%.1f%%)")
    println("  flights with revenue: " + c.getLong(4) + ", with cost: " + c.getLong(5) + ", of " + c.getLong(6))
    0
  }
}

val gssRc = BuildCurated.run()
if (gssRc == 0) println("GSS_JOB_OK") else println("GSS_JOB_FAILED")
System.exit(gssRc)