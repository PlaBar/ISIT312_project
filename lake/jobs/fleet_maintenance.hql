-- fleet_maintenance.hql - Job 5: how hard is each aircraft worked, and is its maintenance on time?
--
--   hive -f lake/jobs/fleet_maintenance.hql | tee $GSS_STAGING/reports/fleet_maintenance.txt
--
-- From the curated star schema:
--   1. Utilisation in the last 12 months of data: flights, block hours, hours per day,
--      revenue per block hour, per aircraft
--   2. Reliability over the whole period: defects and AOG events per 100 block hours,
--      serious defects (HIGH or AOG severity), per aircraft
--   3. Maintenance punctuality: checks done, done late, average and worst days late,
--      per aircraft and check type
--   4. Open maintenance: checks still planned or overdue, earliest due first
-- Needs the metastore (bash lake/hive/metastore.sh start). Runs as local MapReduce, a few minutes.

SET mapreduce.framework.name=local;
SET hive.cli.print.header=true;
SET hive.resultset.use.unique.column.names=false;
SET hive.strict.checks.cartesian.product=false;
SET hive.mapred.mode=nonstrict;

!echo ;
!echo ----- 1. Utilisation, last 12 months of data -----;
SELECT f.aircraft_key AS aircraft, a.`type` AS aircraft_type,
       COUNT(*) AS flights,
       ROUND(SUM(f.block_hours), 0) AS block_hours,
       ROUND(SUM(f.block_hours) / 365, 2) AS block_hours_per_day,
       ROUND(SUM(f.revenue_usd) / SUM(f.block_hours), 0) AS revenue_per_bh,
       SUM(f.n_aog_events) AS aog_events
FROM gss.fact_flight f
JOIN gss.dim_aircraft a ON f.aircraft_key = a.aircraft_key
CROSS JOIN (SELECT MAX(date_key) AS end_key FROM gss.fact_flight) lim
WHERE f.status <> 'CANCELLED' AND f.date_key > lim.end_key - 10000
GROUP BY f.aircraft_key, a.`type`
ORDER BY aircraft;

!echo ;
!echo ----- 2. Reliability per 100 block hours, whole period -----;
WITH hrs AS (
  SELECT aircraft_key, SUM(block_hours) AS bh, SUM(n_aog_events) AS aog
  FROM gss.fact_flight WHERE status <> 'CANCELLED' GROUP BY aircraft_key),
dfx AS (
  SELECT aircraft_key, COUNT(*) AS defects,
         SUM(CASE WHEN severity IN ('HIGH', 'AOG') THEN 1 ELSE 0 END) AS serious
  FROM gss.fact_defect GROUP BY aircraft_key)
SELECT h.aircraft_key AS aircraft, ROUND(h.bh, 0) AS block_hours,
       COALESCE(d.defects, 0) AS defects, COALESCE(d.serious, 0) AS serious_defects,
       ROUND(100 * COALESCE(d.defects, 0) / h.bh, 2) AS defects_per_100bh,
       ROUND(100 * h.aog / h.bh, 2) AS aog_per_100bh
FROM hrs h LEFT JOIN dfx d ON h.aircraft_key = d.aircraft_key
ORDER BY defects_per_100bh DESC;

!echo ;
!echo ----- 3. Maintenance punctuality by aircraft and check type -----;
SELECT aircraft_key AS aircraft, event_type AS check_type,
       COUNT(*) AS done,
       SUM(CASE WHEN days_late > 0 THEN 1 ELSE 0 END) AS done_late,
       ROUND(AVG(CASE WHEN days_late > 0 THEN days_late END), 1) AS avg_days_late,
       MAX(days_late) AS worst_days_late
FROM gss.fact_maintenance
WHERE done_date_key IS NOT NULL
GROUP BY aircraft_key, event_type
ORDER BY aircraft, check_type;

!echo ;
!echo ----- 4. Open maintenance: planned or overdue, earliest due first -----;
SELECT maintenance_key, aircraft_key AS aircraft, event_type AS check_type, status,
       due_date_key, flight_hours_at, cycles_at
FROM gss.fact_maintenance
WHERE status IN ('PLANNED', 'OVERDUE')
ORDER BY due_date_key
LIMIT 15;
