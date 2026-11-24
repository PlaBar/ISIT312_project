// CustomerSegments.scala - Job 1: which customers matter most, and which ones pay badly?
//
//   bash lake/run_spark.sh lake/jobs/CustomerSegments.scala
//
// For every customer, from fact_invoice and dim_customer (VOID invoices left out):
//   value      revenue in the last 12 months of data, flights, months since the last invoice
//              -> tier A (top third by revenue), B, C, or DORMANT (no invoice for 6 months)
//   payment    share of paid invoices paid after their terms (NET15/30/45, PREPAID = 0 days),
//              average days to pay, unpaid amount more than 60 days past due
//              -> RELIABLE, SLOW or RISKY
//   action     what sales and finance should do with the customer, from the two above
// Output: /lake/curated/customer_segment (one row per customer), printed summary.
// After the first run: bash lake/run_spark.sh lake/hive/HiveDdl.scala && hive -f lake/hive/create_tables.hql
// makes it the Hive table gss.customer_segment.
//
// Spark 3.0.3, Scala 2.12, run with spark-shell -i (no sbt). One object = one unit for the REPL.

object CustomerSegments {
  val hdfs = sys.env.getOrElse("GSS_HDFS", "hdfs://localhost:8020")
  val curated = hdfs + "/lake/curated"

  def run(): Int = {
    spark.sparkContext.setLogLevel("WARN")
    Seq("fact_invoice", "dim_customer").foreach(t => spark.read.parquet(curated + "/" + t).createOrReplaceTempView(t))

    spark.sql("""
      WITH inv AS (
        SELECT i.*, CASE c.payment_terms WHEN 'NET15' THEN 15 WHEN 'NET30' THEN 30 WHEN 'NET45' THEN 45 ELSE 0 END AS terms_days
        FROM fact_invoice i JOIN dim_customer c ON i.customer_key = c.customer_key
        WHERE i.status <> 'VOID'),
      lim AS (
        SELECT MAX(date_key) AS end_key, to_date(CAST(MAX(date_key) AS STRING), 'yyyyMMdd') AS end_date FROM inv)
      SELECT inv.customer_key,
             COUNT(*) AS invoices,
             COUNT(DISTINCT inv.flight_key) AS flights,
             CAST(SUM(CASE WHEN inv.date_key > lim.end_key - 10000 THEN inv.total_usd ELSE 0 END) AS DECIMAL(14,2)) AS revenue_usd_12m,
             CAST(SUM(inv.total_usd) AS DECIMAL(14,2)) AS revenue_usd_all,
             datediff(MAX(lim.end_date), to_date(CAST(MAX(inv.date_key) AS STRING), 'yyyyMMdd')) AS days_since_last_invoice,
             ROUND(AVG(inv.days_to_pay), 1) AS avg_days_to_pay,
             SUM(CASE WHEN inv.days_to_pay IS NOT NULL THEN 1 ELSE 0 END) AS paid_invoices,
             SUM(CASE WHEN inv.days_to_pay > inv.terms_days THEN 1 ELSE 0 END) AS paid_late,
             CAST(SUM(inv.outstanding_usd) AS DECIMAL(14,2)) AS outstanding_usd,
             CAST(SUM(CASE WHEN inv.outstanding_usd > 0
                       AND datediff(lim.end_date, to_date(CAST(inv.due_date_key AS STRING), 'yyyyMMdd')) > 60
                      THEN inv.outstanding_usd ELSE 0 END) AS DECIMAL(14,2)) AS overdue_60_usd
      FROM inv CROSS JOIN lim
      GROUP BY inv.customer_key""").createOrReplaceTempView("agg")

    val seg = spark.sql("""
      WITH ranked AS (
        SELECT a.*, NTILE(3) OVER (ORDER BY a.revenue_usd_12m DESC) AS third,
               ROUND(CASE WHEN a.paid_invoices > 0 THEN 100.0 * a.paid_late / a.paid_invoices END, 1) AS late_pct
        FROM agg a),
      classed AS (
        SELECT r.*,
               CASE WHEN r.days_since_last_invoice > 180 OR r.revenue_usd_12m = 0 THEN 'DORMANT'
                    WHEN r.third = 1 THEN 'A' WHEN r.third = 2 THEN 'B' ELSE 'C' END AS value_tier,
               CASE WHEN r.overdue_60_usd > 50000 OR r.late_pct > 50 THEN 'RISKY'
                    WHEN r.overdue_60_usd > 0 OR r.late_pct > 20 THEN 'SLOW'
                    ELSE 'RELIABLE' END AS payment_class
        FROM ranked r)
      SELECT c.customer_key, d.name, d.country, d.payment_terms, c.invoices, c.flights,
             c.revenue_usd_12m, c.revenue_usd_all, c.days_since_last_invoice, c.avg_days_to_pay,
             c.late_pct, c.outstanding_usd, c.overdue_60_usd, c.value_tier, c.payment_class,
             CASE WHEN c.payment_class = 'RISKY' AND c.overdue_60_usd > 0 THEN 'Collect overdue before the next booking'
                  WHEN c.value_tier = 'A' AND c.payment_class = 'RELIABLE' THEN 'Key account: protect capacity and pricing'
                  WHEN c.value_tier = 'A' THEN 'Key account: review credit terms'
                  WHEN c.value_tier = 'B' AND c.payment_class = 'RELIABLE' THEN 'Grow: offer regular slots'
                  WHEN c.value_tier = 'DORMANT' THEN 'Re-engage or close the account'
                  WHEN c.payment_class = 'RISKY' THEN 'Prepayment only'
                  ELSE 'Maintain' END AS action
      FROM classed c JOIN dim_customer d ON c.customer_key = d.customer_key""").persist()
    seg.coalesce(1).write.mode("overwrite").parquet(curated + "/customer_segment")
    seg.createOrReplaceTempView("seg")

    println("Customers by value tier and payment behaviour (revenue: last 12 months of data, USD)")
    spark.sql("""SELECT value_tier, payment_class, COUNT(*) AS customers, ROUND(SUM(revenue_usd_12m), 0) AS revenue_usd_12m,
                        ROUND(100 * SUM(revenue_usd_12m) / SUM(SUM(revenue_usd_12m)) OVER (), 1) AS revenue_share_pct,
                        ROUND(SUM(overdue_60_usd), 0) AS overdue_60_usd
                 FROM seg GROUP BY value_tier, payment_class ORDER BY value_tier, payment_class""").show(20, false)
    println("Recommended action")
    spark.sql("SELECT action, COUNT(*) AS customers FROM seg GROUP BY action ORDER BY customers DESC").show(10, false)
    println("Top 10 customers by revenue in the last 12 months")
    spark.sql("""SELECT name, value_tier, payment_class, ROUND(revenue_usd_12m, 0) AS revenue_usd_12m, flights,
                        avg_days_to_pay, late_pct, ROUND(overdue_60_usd, 0) AS overdue_60_usd
                 FROM seg ORDER BY revenue_usd_12m DESC LIMIT 10""").show(10, false)
    println("Written: " + seg.count() + " customers to /lake/curated/customer_segment")
    seg.unpersist()
    0
  }
}

val gssRc = CustomerSegments.run()
if (gssRc == 0) println("GSS_JOB_OK") else println("GSS_JOB_FAILED")
System.exit(gssRc)
