// DelayCauses.scala - Job 3: why are flights late or cancelled, and who could prevent it?
//
//   bash lake/run_spark.sh lake/jobs/DelayCauses.scala
//
// Reads the free-text detail of every operational event (fact_event, e.g. "dep delayed 42 min,
// customs clearance pending") and sorts it into a cause group by keywords:
//   Crew, Customer and cargo, Ground services, Weather, Customs and permits,
//   Air traffic control, Technical, Planning, Other
// and each group into who controls it: GSS (crew, technical, planning), GSS suppliers (ground
// services), the customer (cargo, documents), or nobody (weather, ATC, authorities).
// Delay minutes stated in the text ("delay 79m", "+40min", "delayed 42 min") are extracted;
// the flight's measured departure delay comes from fact_flight.
// Output: /lake/curated/delay_cause (one row per event), printed summary:
//   punctuality by year, causes ranked, controllable share, worst routes and their main cause.
//
// Spark 3.0.3, Scala 2.12, run with spark-shell -i (no sbt). One object = one unit for the REPL.

object DelayCauses {
  val hdfs = sys.env.getOrElse("GSS_HDFS", "hdfs://localhost:8020")
  val curated = hdfs + "/lake/curated"

  def run(): Int = {
    spark.sparkContext.setLogLevel("WARN")
    Seq("fact_event", "fact_flight").foreach(t => spark.read.parquet(curated + "/" + t).createOrReplaceTempView(t))

    val dc = spark.sql("""
      WITH e AS (
        SELECT e.*, lower(e.detail) AS d FROM fact_event e),
      g AS (
        SELECT e.*,
               CASE WHEN d RLIKE 'crew' THEN 'Crew'
                    WHEN d RLIKE 'cargo acceptance|pallets|cargo not ready|documents from shipper|customer postponed' THEN 'Customer and cargo'
                    WHEN d RLIKE 'catering|ground handler|fuel truck' THEN 'Ground services'
                    WHEN d RLIKE 'weather' THEN 'Weather'
                    WHEN d RLIKE 'customs|permit' THEN 'Customs and permits'
                    WHEN d RLIKE 'atc' THEN 'Air traffic control'
                    WHEN d RLIKE 'aog|unserviceable|u/s|engine|brake|hyd|apu|spare|tech|maintenance' THEN 'Technical'
                    WHEN d RLIKE 'schedule change|payload' THEN 'Planning'
                    ELSE 'Other' END AS cause_group,
               CAST(COALESCE(NULLIF(regexp_extract(d, '(\\d+)\\s*(min|m\\b)', 1), ''),
                             NULLIF(regexp_extract(d, '\\+(\\d+)', 1), '')) AS INT) AS minutes_stated
        FROM e)
      SELECT g.event_key, g.flight_key, g.event_type, g.event_time_utc, g.date_key, g.detail, g.source,
             g.cause_group,
             CASE WHEN g.cause_group IN ('Crew', 'Technical', 'Planning') THEN 'GSS'
                  WHEN g.cause_group = 'Ground services' THEN 'GSS suppliers'
                  WHEN g.cause_group = 'Customer and cargo' THEN 'Customer'
                  WHEN g.cause_group IN ('Weather', 'Air traffic control', 'Customs and permits') THEN 'External'
                  ELSE 'Unknown' END AS controlled_by,
             g.minutes_stated, f.dep_delay_min, f.origin_key, f.dest_key, f.aircraft_key, f.status AS flight_status
      FROM g LEFT JOIN fact_flight f ON g.flight_key = f.flight_key""").persist()
    dc.coalesce(1).write.mode("overwrite").parquet(curated + "/delay_cause")
    dc.createOrReplaceTempView("dc")

    println("Punctuality by year (on time = departed within 15 minutes of schedule)")
    spark.sql("""SELECT CAST(floor(date_key / 10000) AS INT) AS yr,
                        SUM(CASE WHEN status <> 'CANCELLED' THEN 1 ELSE 0 END) AS flights,
                        SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled,
                        ROUND(100.0 * SUM(CASE WHEN dep_delay_min <= 15 THEN 1 ELSE 0 END)
                              / SUM(CASE WHEN dep_delay_min IS NOT NULL THEN 1 ELSE 0 END), 1) AS on_time_pct,
                        ROUND(AVG(CASE WHEN dep_delay_min > 15 THEN dep_delay_min END), 0) AS avg_delay_when_late_min
                 FROM fact_flight GROUP BY floor(date_key / 10000) ORDER BY yr""").show(10, false)
    println("Causes, most frequent first")
    spark.sql("""SELECT cause_group, controlled_by, COUNT(*) AS events, COUNT(DISTINCT flight_key) AS flights,
                        SUM(CASE WHEN event_type = 'CANCELLATION' THEN 1 ELSE 0 END) AS cancellations,
                        SUM(CASE WHEN event_type = 'AOG' THEN 1 ELSE 0 END) AS aog,
                        ROUND(AVG(CASE WHEN dep_delay_min > 0 THEN dep_delay_min END), 0) AS avg_flight_delay_min
                 FROM dc GROUP BY cause_group, controlled_by ORDER BY events DESC""").show(20, false)
    println("Who controls the disruption")
    spark.sql("""SELECT controlled_by, COUNT(*) AS events,
                        ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS share_pct
                 FROM dc GROUP BY controlled_by ORDER BY events DESC""").show(10, false)
    println("Routes with the longest average departure delay (at least 15 flights) and their most frequent cause")
    spark.sql("""
      WITH r AS (
        SELECT concat(origin_key, '-', dest_key) AS route, COUNT(*) AS flights, ROUND(AVG(dep_delay_min), 1) AS avg_delay_min
        FROM fact_flight WHERE status <> 'CANCELLED' GROUP BY concat(origin_key, '-', dest_key) HAVING COUNT(*) >= 15),
      c AS (
        SELECT concat(origin_key, '-', dest_key) AS route, cause_group,
               ROW_NUMBER() OVER (PARTITION BY concat(origin_key, '-', dest_key) ORDER BY COUNT(*) DESC, cause_group) AS rn
        FROM dc GROUP BY concat(origin_key, '-', dest_key), cause_group)
      SELECT r.route, r.flights, r.avg_delay_min, c.cause_group AS main_cause
      FROM r LEFT JOIN c ON r.route = c.route AND c.rn = 1
      ORDER BY r.avg_delay_min DESC LIMIT 8""").show(8, false)
    println("Written: " + dc.count() + " events to /lake/curated/delay_cause")
    dc.unpersist()
    0
  }
}

val gssRc = DelayCauses.run()
if (gssRc == 0) println("GSS_JOB_OK") else println("GSS_JOB_FAILED")
System.exit(gssRc)
