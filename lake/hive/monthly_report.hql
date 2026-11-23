-- monthly_report.hql - the GSS monthly management report, from the curated star schema in Hive.
--
-- Run it through the wrapper, which fills in the month and saves the output:
--   bash lake/hive/run_monthly_report.sh            the last complete month
--   bash lake/hive/run_monthly_report.sh 202603     a given month (yyyyMM)
-- Variables (set by the wrapper): month (yyyyMM), prev (the month before), month_end (yyyyMMdd)
--
-- Sections
--   1. Headline: flights, block hours, cargo, revenue, cost, margin, punctuality (vs last month)
--   2. Routes: the 10 biggest by revenue, with margin
--   3. Customers: the 10 biggest by invoiced amount, with what is still unpaid
--   4. Costs by supplier category, and how much of each could be tied to a flight
--   5. Fleet: flights, block hours, revenue per block hour, AOG events, maintenance, defects
--   6. Sales pipeline: customer requests, how complete they were, how many became flights,
--      the most requested routes and cargo types
--   7. Receivables at month end: unpaid invoices by days overdue
--   8. Data quality and compliance: flight documents and permits, flights without invoice or cost
--
-- Notes
--   Revenue and cost per flight come from the invoices and purchase-order lines that the
--   flight-reference repair (BuildCurated.scala) could tie to a flight: about 93% of revenue
--   and 87% of cost. Sections 3 and 4 show all of it, tied to a flight or not.
--   date_key is yyyyMMdd (UAE date), so floor(date_key / 100) is the month as yyyyMM.
--   Outstanding amounts are as of the last load, not as of the month end.

SET mapreduce.framework.name=local;
SET hive.cli.print.header=true;
SET hive.resultset.use.unique.column.names=false;

!echo ;
!echo ===== GSS monthly report ${hivevar:month} (previous month ${hivevar:prev}) =====;

!echo ;
!echo ----- 1. Headline -----;
SELECT floor(f.date_key / 100) AS yyyymm,
       SUM(CASE WHEN f.status <> 'CANCELLED' THEN 1 ELSE 0 END) AS flights,
       SUM(CASE WHEN f.status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled,
       SUM(CASE WHEN f.status = 'DIVERTED' THEN 1 ELSE 0 END) AS diverted,
       ROUND(SUM(CASE WHEN f.status <> 'CANCELLED' THEN f.block_hours ELSE 0 END), 1) AS block_hours,
       ROUND(SUM(f.cargo_weight_kg) / 1000, 1) AS cargo_tonnes,
       ROUND(SUM(f.revenue_usd), 0) AS revenue_usd,
       ROUND(SUM(f.cost_usd), 0) AS cost_usd,
       ROUND(SUM(f.margin_usd), 0) AS margin_usd,
       ROUND(100 * SUM(f.margin_usd) / SUM(f.revenue_usd), 1) AS margin_pct,
       ROUND(AVG(f.dep_delay_min), 1) AS avg_delay_min,
       ROUND(100.0 * SUM(CASE WHEN f.dep_delay_min <= 15 THEN 1 ELSE 0 END)
             / SUM(CASE WHEN f.dep_delay_min IS NOT NULL THEN 1 ELSE 0 END), 1) AS on_time_pct
FROM gss.fact_flight f
WHERE floor(f.date_key / 100) IN (${hivevar:prev}, ${hivevar:month})
GROUP BY floor(f.date_key / 100)
ORDER BY yyyymm;

!echo ;
!echo ----- 2. Top 10 routes by revenue -----;
SELECT concat(f.origin_key, '-', f.dest_key) AS route,
       COUNT(*) AS flights,
       ROUND(SUM(f.cargo_weight_kg) / 1000, 1) AS cargo_tonnes,
       ROUND(SUM(f.revenue_usd), 0) AS revenue_usd,
       ROUND(SUM(f.margin_usd), 0) AS margin_usd,
       ROUND(100 * SUM(f.margin_usd) / SUM(f.revenue_usd), 1) AS margin_pct
FROM gss.fact_flight f
WHERE floor(f.date_key / 100) = ${hivevar:month} AND f.status <> 'CANCELLED'
GROUP BY concat(f.origin_key, '-', f.dest_key)
ORDER BY revenue_usd DESC
LIMIT 10;

!echo ;
!echo ----- 3. Top 10 customers by amount invoiced -----;
SELECT c.name AS customer,
       COUNT(*) AS invoices,
       ROUND(SUM(i.total_usd), 0) AS invoiced_usd,
       ROUND(SUM(i.paid_usd), 0) AS paid_usd,
       ROUND(SUM(i.outstanding_usd), 0) AS outstanding_usd,
       SUM(CASE WHEN i.flight_key IS NULL THEN 1 ELSE 0 END) AS not_tied_to_flight
FROM gss.fact_invoice i
LEFT JOIN gss.dim_customer c ON i.customer_key = c.customer_key
WHERE floor(i.date_key / 100) = ${hivevar:month} AND i.status <> 'VOID'
GROUP BY c.name
ORDER BY invoiced_usd DESC
LIMIT 10;

!echo ;
!echo ----- 4. Costs by supplier category -----;
SELECT COALESCE(k.category, '(unknown)') AS category,
       COUNT(*) AS po_lines,
       ROUND(SUM(k.amount_usd), 0) AS cost_usd,
       ROUND(100.0 * SUM(CASE WHEN k.flight_key IS NOT NULL THEN k.amount_usd ELSE 0 END)
             / SUM(k.amount_usd), 1) AS pct_tied_to_flight
FROM gss.fact_cost k
WHERE floor(k.date_key / 100) = ${hivevar:month} AND k.po_status <> 'CANCELLED'
GROUP BY COALESCE(k.category, '(unknown)')
ORDER BY cost_usd DESC;

!echo ;
!echo ----- 5. Fleet -----;
WITH fl AS (
  SELECT aircraft_key, COUNT(*) AS flights, SUM(block_hours) AS bh, SUM(revenue_usd) AS rev,
         SUM(n_aog_events) AS aog
  FROM gss.fact_flight
  WHERE floor(date_key / 100) = ${hivevar:month} AND status <> 'CANCELLED'
  GROUP BY aircraft_key),
mx AS (
  SELECT aircraft_key, COUNT(*) AS done, SUM(CASE WHEN days_late > 0 THEN 1 ELSE 0 END) AS late
  FROM gss.fact_maintenance
  WHERE floor(done_date_key / 100) = ${hivevar:month}
  GROUP BY aircraft_key),
df AS (
  SELECT aircraft_key, COUNT(*) AS defects,
         SUM(CASE WHEN severity IN ('HIGH', 'AOG') THEN 1 ELSE 0 END) AS serious
  FROM gss.fact_defect
  WHERE floor(date_key / 100) = ${hivevar:month}
  GROUP BY aircraft_key)
SELECT a.aircraft_key AS aircraft,
       a.`type` AS aircraft_type,
       COALESCE(fl.flights, 0) AS flights,
       ROUND(COALESCE(fl.bh, 0), 1) AS block_hours,
       ROUND(fl.rev / fl.bh, 0) AS revenue_per_bh,
       COALESCE(fl.aog, 0) AS aog_events,
       COALESCE(mx.done, 0) AS maintenance_done,
       COALESCE(mx.late, 0) AS maintenance_late,
       COALESCE(df.defects, 0) AS defects,
       COALESCE(df.serious, 0) AS defects_high_or_aog
FROM gss.dim_aircraft a
LEFT JOIN fl ON a.aircraft_key = fl.aircraft_key
LEFT JOIN mx ON a.aircraft_key = mx.aircraft_key
LEFT JOIN df ON a.aircraft_key = df.aircraft_key
ORDER BY aircraft;

!echo ;
!echo ----- 6a. Sales pipeline: customer requests -----;
SELECT COUNT(*) AS requests,
       SUM(CASE WHEN r.confidence = 'high' THEN 1 ELSE 0 END) AS high_conf,
       SUM(CASE WHEN r.confidence = 'medium' THEN 1 ELSE 0 END) AS medium_conf,
       SUM(CASE WHEN r.confidence = 'low' THEN 1 ELSE 0 END) AS low_conf,
       ROUND(100.0 * SUM(CASE WHEN r.missing_fields IS NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_complete,
       SUM(CASE WHEN b.thread_id IS NOT NULL THEN 1 ELSE 0 END) AS became_flights,
       ROUND(100.0 * SUM(CASE WHEN b.thread_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 1) AS conversion_pct
FROM gss.request r
LEFT JOIN (SELECT DISTINCT thread_id FROM gss.document_link
           WHERE doc_type = 'QUOTE' AND flight_key IS NOT NULL) b ON r.thread_id = b.thread_id
WHERE floor(r.date_key / 100) = ${hivevar:month};

!echo ;
!echo ----- 6b. Most requested routes (origin ? = not stated) -----;
SELECT concat(COALESCE(origin_iata, '?'), '-', COALESCE(dest_iata, '?')) AS route,
       COUNT(*) AS requests,
       ROUND(SUM(weight_kg) / 1000, 1) AS tonnes_asked
FROM gss.request
WHERE floor(date_key / 100) = ${hivevar:month}
GROUP BY concat(COALESCE(origin_iata, '?'), '-', COALESCE(dest_iata, '?'))
ORDER BY requests DESC
LIMIT 8;

!echo ;
!echo ----- 6c. Requested cargo types -----;
SELECT COALESCE(cargo_type, '(not stated)') AS cargo_type,
       COUNT(*) AS requests,
       SUM(CASE WHEN special_handling IS NOT NULL THEN 1 ELSE 0 END) AS with_special_handling
FROM gss.request
WHERE floor(date_key / 100) = ${hivevar:month}
GROUP BY COALESCE(cargo_type, '(not stated)')
ORDER BY requests DESC;

!echo ;
!echo ----- 7. Unpaid invoices at month end, by days overdue -----;
SELECT x.days_overdue,
       COUNT(*) AS invoices,
       ROUND(SUM(x.outstanding_usd), 0) AS outstanding_usd
FROM (
  SELECT i.outstanding_usd,
         CASE WHEN i.days <= 0 THEN '1 not yet due'
              WHEN i.days <= 30 THEN '2 1-30 days'
              WHEN i.days <= 60 THEN '3 31-60 days'
              WHEN i.days <= 90 THEN '4 61-90 days'
              ELSE '5 over 90 days' END AS days_overdue
  FROM (
    SELECT outstanding_usd,
           datediff(from_unixtime(unix_timestamp('${hivevar:month_end}', 'yyyyMMdd'), 'yyyy-MM-dd'),
                    from_unixtime(unix_timestamp(CAST(due_date_key AS STRING), 'yyyyMMdd'), 'yyyy-MM-dd')) AS days
    FROM gss.fact_invoice
    WHERE date_key <= ${hivevar:month_end} AND outstanding_usd > 0 AND status <> 'VOID'
  ) i
) x
GROUP BY x.days_overdue
ORDER BY x.days_overdue;

!echo ;
!echo ----- 8. Data quality and compliance (flights operated this month) -----;
SELECT COUNT(*) AS flights,
       SUM(CASE WHEN d.documents_complete THEN 0 ELSE 1 END) AS documents_incomplete,
       SUM(CASE WHEN d.permit_required THEN 1 ELSE 0 END) AS permit_required,
       SUM(CASE WHEN d.permit_ok THEN 0 ELSE 1 END) AS permit_missing,
       SUM(CASE WHEN f.n_invoices = 0 THEN 1 ELSE 0 END) AS no_invoice_tied,
       SUM(CASE WHEN f.n_cost_lines = 0 THEN 1 ELSE 0 END) AS no_cost_tied,
       SUM(CASE WHEN f.n_delay_events > 0 THEN 1 ELSE 0 END) AS with_delay_events
FROM gss.fact_flight f
LEFT JOIN gss.flight_documents d ON f.flight_key = d.flight_key
WHERE floor(f.date_key / 100) = ${hivevar:month} AND f.status <> 'CANCELLED';

!echo ;
!echo ===== end of report =====;
