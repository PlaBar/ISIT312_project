// Rules.scala - read a customer message: is it a cargo request, and what does it ask for?
//
// A library, not a job: it is loaded in front of the job that uses it.
//   bash lake/run_spark.sh lake/extract/Rules.scala lake/extract/RulesCheck.scala     (Step 22)
//   bash lake/run_spark.sh lake/extract/Rules.scala lake/stream/StreamMessages.scala  (Step 23)
//
// Rules.extract(direction, senderHandle, sentAtUtc, body) returns an Extraction:
//   isRequest       inbound, not from a GSS address, uses a request word (quote, rate, rfq,
//                   need, ...) and names a destination, a cargo type or a weight
//   origin, dest    IATA codes. Pairs first: "A-B", "A/B", "A > B", "A to B" (A is the
//                   origin), "B ex A", "B from A"; otherwise a single place after from/ex
//                   (origin) or to/into/for (destination)
//   cargoType       the longest cargo phrase, typo-tolerant (cargo_types.txt)
//   weightKg        the first number with a weight unit, not after "over", "pallets", ... ;
//                   tonnes are converted to kg
//   readyDate       the first date phrase that is not right after a context word ("as
//                   discussed on the phone 12/03"), resolved against the sent date in UAE time
//   specialHandling the longest special-handling phrase (exact words only)
//   confidence      high = all five of origin, destination, cargo, weight and date found;
//                   medium = destination plus cargo or weight; low = the rest
//   missing         the fields not found, for the follow-up question to the customer
//
// All vocabulary is in lake/extract/dictionaries/{airports,cargo_types,date_phrases}.txt.
// The same logic is written in Python in evaluation/rules_twin.py and tested against the
// hidden truth there; RulesCheck.scala checks that both give the same answers.
//
// Pure Scala (no Spark), so it works on the driver and inside a Spark UDF. Spark 3.0.3,
// Scala 2.12, loaded with spark-shell -i.

object Rules extends Serializable {
  import java.time.{LocalDate, LocalDateTime}
  import java.util.regex.Pattern
  import scala.util.Try

  case class Extraction(isRequest: Boolean, origin: Option[String], dest: Option[String],
                        cargoType: Option[String], weightKg: Option[Int], readyDate: Option[String],
                        specialHandling: Option[String], confidence: Option[String], missing: Option[String])

  val project = sys.env.getOrElse("GSS_PROJECT", sys.props("user.dir"))
  val dictDir = project + "/lake/extract/dictionaries/"
  val ownDomain = "@gss.example"
  val noRequest = Extraction(false, None, None, None, None, None, None, None, None)

  // ---------------------------------------------------------------- dictionaries
  def load(name: String): List[Array[String]] = {
    val src = scala.io.Source.fromFile(dictDir + name, "UTF-8")
    try src.getLines().map(_.trim).filter(l => l.nonEmpty && !l.startsWith("#")).map(_.split("\\|", -1)).toList
    finally src.close()
  }

  class Dicts extends Serializable {
    private val a = load("airports.txt")
    val airports: List[(String, String, Boolean)] = a.map(x => (x(0), x(1), x.length > 2 && x(2) == "exact_case"))
    private val c = load("cargo_types.txt")
    val cargo: List[(String, String)] = c.filter(_(0) == "cargo").map(x => (x(1), x(2)))
    val shc: List[(String, String)] = c.filter(_(0) == "shc").map(x => (x(1), x(2)))
    val units: Map[String, Int] = c.filter(_(0) == "unit").map(x => (x(1), x(2).toInt)).toMap
    val notWeight: List[String] = c.filter(_(0) == "not_weight").map(_(1))
    val request: List[String] = c.filter(_(0) == "request").map(_(1))
    private val d = load("date_phrases.txt")
    val months: Map[String, Int] = d.filter(_(0) == "month").map(x => (x(1), x(2).toInt)).toMap
    val weekdays: Map[String, Int] = d.filter(_(0) == "weekday").map(x => (x(1), x(2).toInt)).toMap
    val relative: List[(String, String)] = d.filter(_(0) == "relative").map(x => (x(1), x(2)))
    val context: List[String] = d.filter(_(0) == "context").map(_(1))
  }

  @transient lazy val dicts = new Dicts()

  // ---------------------------------------------------------------- helpers
  def wordRe(term: String): String = "(?<![A-Za-z0-9])" + Pattern.quote(term) + "(?![A-Za-z0-9])"

  // start/end of every whole-word match
  // compiled patterns are kept, so each is compiled once per JVM
  @transient lazy val cache = scala.collection.concurrent.TrieMap[(String, Int), Pattern]()
  def pat(p: String, flags: Int = Pattern.CASE_INSENSITIVE): Pattern = cache.getOrElseUpdate((p, flags), Pattern.compile(p, flags))

  def findAll(term: String, text: String, ignoreCase: Boolean = true): List[(Int, Int)] = {
    val m = pat(wordRe(term), if (ignoreCase) Pattern.CASE_INSENSITIVE else 0).matcher(text)
    val out = scala.collection.mutable.ListBuffer[(Int, Int)]()
    while (m.find()) out += ((m.start, m.end))
    out.toList
  }

  def found(p: String, text: String): Boolean = pat(p).matcher(text).find()

  // keep the longest spans, drop any that overlap a kept one; result in text order
  def longestNonOverlapping[T](hits: List[(Int, Int, T)]): List[(Int, Int, T)] = {
    val kept = scala.collection.mutable.ListBuffer[(Int, Int, T)]()
    for (h <- hits.sortBy(h => (-(h._2 - h._1), h._1)))
      if (kept.forall(k => h._2 <= k._1 || h._1 >= k._2)) kept += h
    kept.toList.sortBy(_._1)
  }

  // at most one letter dropped, added or changed, or two neighbouring letters swapped
  def near(a: String, b: String): Boolean = {
    if (a == b) true
    else if (math.abs(a.length - b.length) > 1) false
    else if (a.length == b.length) {
      val diff = a.indices.filter(i => a(i) != b(i))
      diff.length == 1 || (diff.length == 2 && diff(1) == diff(0) + 1 && a(diff(0)) == b(diff(1)) && a(diff(1)) == b(diff(0)))
    } else {
      val (s, l) = if (a.length < b.length) (a, b) else (b, a)
      l.indices.exists(i => l.substring(0, i) + l.substring(i + 1) == s)
    }
  }

  val wordPattern = Pattern.compile("[a-z0-9\\-]+")

  // value of the longest phrase found (ties: the earliest in the text, then file order)
  def phraseValue(phrases: List[(String, String)], text: String, fuzzy: Boolean): Option[String] = {
    val low = text.toLowerCase
    val wm = wordPattern.matcher(low)
    val wb = scala.collection.mutable.ArrayBuffer[(String, Int)]()
    while (wm.find()) wb += ((wm.group(), wm.start))
    val words = wb.toVector
    var best: Option[((Int, Int), String)] = None
    for ((phrase, value) <- phrases) {
      var pos: Option[Int] = findAll(phrase, low).headOption.map(_._1)
      if (pos.isEmpty && fuzzy) {
        val pw = phrase.split(" ").toVector
        val joined = pw.mkString
        val n = pw.length
        var i = 0
        while (pos.isEmpty && i <= words.length - n) {
          val window = words.slice(i, i + n).map(_._1)
          val diff = (0 until n).filter(k => window(k) != pw(k))
          var ok = diff.length == 1 && pw(diff(0)).length >= 5 && near(window(diff(0)), pw(diff(0)))
          if (!ok && n > 1) {
            val jw = window.mkString
            ok = jw == joined || (joined.length >= 7 && pw.map(_.length).min >= 3 && near(jw, joined))
          }
          if (ok) pos = Some(words(i)._2)
          i += 1
        }
        if (pos.isEmpty && n > 1)
          pos = words.find(w => w._1 == joined || (joined.length >= 5 && near(w._1, joined))).map(_._2)
      }
      for (p <- pos) {
        val key = (phrase.length, -p)
        if (best.isEmpty || Ordering[(Int, Int)].gt(key, best.get._1)) best = Some((key, value))
      }
    }
    best.map(_._2)
  }

  // ---------------------------------------------------------------- route
  val pairFwd = Pattern.compile("""(\s*[-/>]\s*|\s+to\s+)""", Pattern.CASE_INSENSITIVE)
  val pairRev = Pattern.compile("""\s+(ex|from)\s+""", Pattern.CASE_INSENSITIVE)
  val beforeOrigin = """(?:^|[^a-z])(from|ex)\s+$"""
  val beforeDest = """(?:^|[^a-z])(to|into|for)\s+$"""

  // (start, end, (code, weak)); weak = a code that is also an English word ("add"), in lower
  // case. route() only takes a place that sits next to a route marker (a pair separator or
  // from/ex/to/into/for), so "please add insurance" never becomes Addis Ababa
  def airportMentions(text: String, d: Dicts): List[(Int, Int, (String, Boolean))] = {
    val hits = for {
      (term, code, exact) <- d.airports
      (s, e) <- findAll(term, text)
    } yield (s, e, (code, exact && text.substring(s, e) != term))
    longestNonOverlapping(hits)
  }

  def route(text: String, d: Dicts): (Option[String], Option[String]) = {
    val ms = airportMentions(text, d).toVector
    for (i <- 0 until ms.length - 1) {
      val (a, b) = (ms(i), ms(i + 1))
      val gap = text.substring(a._2, b._1)
      if (a._3._1 != b._3._1) {
        if (pairFwd.matcher(gap).matches()) return (Some(a._3._1), Some(b._3._1))
        if (pairRev.matcher(gap).matches()) return (Some(b._3._1), Some(a._3._1))
      }
    }
    var origin: Option[String] = None
    var dest: Option[String] = None
    for ((s, _, (code, _)) <- ms) {
      val before = text.substring(math.max(0, s - 8), s)
      if (found(beforeOrigin, before) && origin.isEmpty) origin = Some(code)
      else if (found(beforeDest, before) && dest.isEmpty) dest = Some(code)
    }
    if (origin.isDefined && origin == dest) origin = None
    (origin, dest)
  }

  // ---------------------------------------------------------------- weight
  val numberUnit = Pattern.compile("""(?<![A-Za-z0-9.,])(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*([a-z]+)\b""",
                                   Pattern.CASE_INSENSITIVE)

  def weight(text: String, d: Dicts): Option[Int] = {
    val m = numberUnit.matcher(text)
    while (m.find()) {
      val unit = m.group(2).toLowerCase
      if (d.units.contains(unit)) {
        val before = text.substring(math.max(0, m.start - 12), m.start).toLowerCase
        if (!d.notWeight.exists(w => found(wordRe(w) + """\s*$""", before))) {
          val kg = math.round(m.group(1).replace(",", "").toDouble * d.units(unit)).toInt
          if (kg > 0) return Some(kg)
        }
      }
    }
    None
  }

  // ---------------------------------------------------------------- ready date
  def date(y: Int, m: Int, d: Int): Option[LocalDate] = Try(LocalDate.of(y, m, d)).toOption

  def nextOnOrAfter(sent: LocalDate, day: Int, month: Int): Option[LocalDate] =
    Seq(sent.getYear, sent.getYear + 1).flatMap(y => date(y, month, day)).find(x => !x.isBefore(sent))

  def readyDate(text: String, sent: LocalDate, d: Dicts): Option[String] = {
    val low = text.toLowerCase
    val monthAlt = d.months.keys.toList.sortBy(k => (-k.length, k)).mkString("|")
    val c = scala.collection.mutable.ListBuffer[(Int, Int, Option[LocalDate])]()
    // f returns None when the match is not a date candidate at all, Some(None) when it is a
    // candidate that names no real date ("31/02"): it still hides overlapping matches
    def scan(p: String)(f: java.util.regex.Matcher => Option[Option[LocalDate]]): Unit = {
      val m = pat(p, 0).matcher(low)
      while (m.find()) for (x <- f(m)) c += ((m.start, m.end, x))
    }
    scan("""(?<![\d/.])(\d{1,2})[/.](\d{1,2})[/.](\d{4})(?![\d/]|\.\d)""") { m =>
      date(m.group(3).toInt, m.group(2).toInt, m.group(1).toInt).map(Some(_)) }
    scan("""(?<![\d/.])(\d{1,2})/(\d{1,2})(?![\d/])""") { m =>
      val (dd, mm) = (m.group(1).toInt, m.group(2).toInt)
      if (mm >= 1 && mm <= 12) Some(nextOnOrAfter(sent, dd, mm)) else None }
    scan("""(?<![a-z0-9])(\d{1,2})(?:st|nd|rd|th)?\s+(""" + monthAlt + """)(?![a-z])""") { m =>
      Some(nextOnOrAfter(sent, m.group(1).toInt, d.months(m.group(2)))) }
    scan("""(?<![a-z])(""" + monthAlt + """)\s+(\d{1,2})(?:st|nd|rd|th)?(?![\d])""") { m =>
      Some(nextOnOrAfter(sent, m.group(2).toInt, d.months(m.group(1)))) }
    scan("""\bon the (\d{1,2})(?:st|nd|rd|th)\b""") { m =>
      val day = m.group(1).toInt
      Some(Seq(0, 1).flatMap { k =>
        val first = sent.withDayOfMonth(1).plusMonths(k)
        date(first.getYear, first.getMonthValue, day)
      }.find(x => !x.isBefore(sent))) }
    for ((name, wd) <- d.weekdays; (s, e) <- findAll(name, low)) {
      val days = Math.floorMod(wd - sent.getDayOfWeek.getValue, 7)
      c += ((s, e, Some(sent.plusDays(if (days == 0) 7 else days))))
    }
    val rel = for ((phrase, rule) <- d.relative; (s, e) <- findAll(phrase, low)) yield (s, e, rule)
    for ((s, e, rule) <- longestNonOverlapping(rel)) {
      val x: Option[LocalDate] =
        if (rule == "SENT") Some(sent)
        else if (rule.startsWith("SENT+")) Some(sent.plusDays(rule.substring(5).toLong))
        else if (rule == "NEXT_MONDAY") Some(sent.plusDays(8 - sent.getDayOfWeek.getValue))
        else None                                                    // IGNORE
      if (rule != "IGNORE") c += ((s, e, x))
    }
    longestNonOverlapping(c.toList).iterator.filter { case (s, _, _) =>
      val before = low.substring(math.max(0, s - 30), s)
      !d.context.exists(w => found(wordRe(w), before))
    }.map(_._3).collectFirst { case Some(x) => x.toString }
  }

  // ---------------------------------------------------------------- one message
  // "2024-03-12T04:41:07Z" -> the UAE (UTC+4, no daylight saving) calendar date
  def sentLocalDate(sentAtUtc: String): LocalDate =
    LocalDateTime.parse(sentAtUtc.substring(0, 19)).plusHours(4).toLocalDate

  def extract(direction: String, senderHandle: String, sentAtUtc: String, body: String): Extraction = {
    val d = dicts
    val text = Option(body).getOrElse("")
    val handle = Option(senderHandle).getOrElse("").toLowerCase
    if (direction != "inbound" || handle.endsWith(ownDomain) || sentAtUtc == null) return noRequest
    val (origin, dest) = route(text, d)
    val cargo = phraseValue(d.cargo, text, fuzzy = true)
    val kg = weight(text, d)
    val asks = d.request.exists(w => findAll(w, text).nonEmpty)
    if (!(asks && (dest.isDefined || cargo.isDefined || kg.isDefined))) return noRequest
    val ready = readyDate(text, sentLocalDate(sentAtUtc), d)
    val missing = Seq("origin_iata" -> origin, "dest_iata" -> dest, "cargo_type" -> cargo,
                      "weight_kg" -> kg, "ready_date" -> ready).filter(_._2.isEmpty).map(_._1)
    val confidence = if (missing.isEmpty) "high" else if (dest.isDefined && (cargo.isDefined || kg.isDefined)) "medium" else "low"
    Extraction(true, origin, dest, cargo, kg, ready, phraseValue(d.shc, text, fuzzy = false),
               Some(confidence), if (missing.isEmpty) None else Some(missing.mkString(";")))
  }
}