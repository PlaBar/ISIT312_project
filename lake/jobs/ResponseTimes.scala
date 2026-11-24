// ResponseTimes.scala - Job 2: how fast does the charter desk answer customer requests?
//
//   bash lake/run_spark.sh lake/jobs/ResponseTimes.scala
//
// For every request in /lake/curated/request, the first outbound GSS message in the same
// thread after it (a quote, a question for missing details, or a "no capacity" reply) from
// /lake/processed/message. Response time = minutes between the two. Service target (SLA):
// a first answer within 2 hours (GSS_SLA_MINUTES to change it).
// Requests with no reply in the lake are counted as unanswered (often the reply was
// rejected in preprocessing, so this is an upper bound).
// Breakdowns: channel, time of day in the UAE, weekday, and the request's confidence
// (do incomplete requests wait longer?).
// Output: /lake/curated/request_response (one row per request), printed summary.
// Uses the message stream's outputs: run after StreamMessages.
//
// Spark 3.0.3, Scala 2.12, run with spark-shell -i (no sbt). One object = one unit for the REPL.

object ResponseTimes {
  val hdfs = sys.env.getOrElse("GSS_HDFS", "hdfs://localhost:8020")
  val sla = sys.env.getOrElse("GSS_SLA_MINUTES", "120").toInt

  def run(): Int = {
    spark.sparkContext.setLogLevel("WARN")
    spark.read.parquet(hdfs + "/lake/curated/request").dropDuplicates("request_key").createOrReplaceTempView("request")
    spark.read.parquet(hdfs + "/lake/processed/message").dropDuplicates("msg_id").createOrReplaceTempView("message")

    val rr = spark.sql(s"""
      WITH replies AS (
        SELECT thread_id, sent_at AS reply_at FROM message WHERE direction = 'outbound'),
      firsts AS (
        SELECT r.request_key, MIN(p.reply_at) AS first_reply_at
        FROM request r LEFT JOIN replies p ON r.thread_id = p.thread_id AND p.reply_at > r.sent_at
        GROUP BY r.request_key)
      SELECT r.request_key, r.thread_id, r.channel, r.sender_handle, r.sent_at, f.first_reply_at,
             CAST((unix_timestamp(f.first_reply_at) - unix_timestamp(r.sent_at)) / 60 AS INT) AS response_min,
             hour(from_utc_timestamp(r.sent_at, 'Asia/Dubai')) AS uae_hour,
             date_format(from_utc_timestamp(r.sent_at, 'Asia/Dubai'), 'E') AS uae_weekday,
             CASE WHEN hour(from_utc_timestamp(r.sent_at, 'Asia/Dubai')) < 6 THEN '1 night 00-06'
                  WHEN hour(from_utc_timestamp(r.sent_at, 'Asia/Dubai')) < 12 THEN '2 morning 06-12'
                  WHEN hour(from_utc_timestamp(r.sent_at, 'Asia/Dubai')) < 18 THEN '3 afternoon 12-18'
                  ELSE '4 evening 18-24' END AS uae_time_band,
             r.confidence, r.missing_fields, r.date_key,
             CASE WHEN f.first_reply_at IS NULL THEN 'unanswered'
                  WHEN (unix_timestamp(f.first_reply_at) - unix_timestamp(r.sent_at)) / 60 <= $sla THEN 'within SLA'
                  ELSE 'late' END AS sla_result
      FROM request r JOIN firsts f ON r.request_key = f.request_key""").persist()
    rr.coalesce(1).write.mode("overwrite").parquet(hdfs + "/lake/curated/request_response")
    rr.createOrReplaceTempView("rr")

    def summary(expr: String, name: String, title: String): Unit = {
      println(title)
      spark.sql(s"""SELECT $expr AS $name, COUNT(*) AS requests,
                           percentile_approx(response_min, 0.5) AS median_min,
                           percentile_approx(response_min, 0.9) AS p90_min,
                           ROUND(100.0 * SUM(CASE WHEN sla_result = 'within SLA' THEN 1 ELSE 0 END) / COUNT(*), 1) AS within_sla_pct,
                           SUM(CASE WHEN sla_result = 'unanswered' THEN 1 ELSE 0 END) AS unanswered
                    FROM rr GROUP BY $expr ORDER BY $name""").show(30, false)
    }
    println(s"First response to customer requests (SLA: first answer within $sla minutes)")
    summary("'all requests'", "scope", "Overall")
    summary("channel", "channel", "By channel")
    summary("uae_time_band", "time_band", "By time of day the request arrived (UAE time)")
    summary("confidence", "confidence_level", "By how complete the request was (confidence from Rules.scala)")
    summary("CAST(floor(date_key / 10000) AS INT)", "yr", "By year")
    println("Slowest 10 answered requests")
    spark.sql("""SELECT request_key, channel, sent_at, first_reply_at, response_min, confidence
                 FROM rr WHERE response_min IS NOT NULL ORDER BY response_min DESC LIMIT 10""").show(10, false)
    println("Written: " + rr.count() + " requests to /lake/curated/request_response")
    rr.unpersist()
    0
  }
}

val gssRc = ResponseTimes.run()
if (gssRc == 0) println("GSS_JOB_OK") else println("GSS_JOB_FAILED")
System.exit(gssRc)
