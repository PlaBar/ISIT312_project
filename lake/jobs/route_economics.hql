-- route_economics.hql - Job 4: which routes earn the most per flying hour, and where is demand unmet?
--
--   hive -f lake/jobs/route_economics.hql | tee $GSS_STAGING/reports/route_economics.txt
--
-- Whole period (Nov 2023 to Oct 2026), from the curated star schema:
--   1. Route profitability: revenue and margin per block hour (the aircraft's scarce
--      resource), load factor (cargo weight / aircraft payload), only flights whose
--      invoices could be tied to them, so margins are not diluted by missing revenue
--   2. Demand against supply by destination: customer requests (Rules.scala) against flights
--      operated, how many requests became flights, and the tonnes asked for but not flown
--   3. Cancellations by route
-- Needs the metastore (bash lake/hive/metastore.sh start). Runs as local MapReduce, a few minutes.

SET mapreduce.framework.name=local;
SET hive.cli.print.header=true;
SET hive.resultset.use.unique.column.names=false;

!echo ;
!echo ----- 1. Route profitability per block hour (routes with 10+ flights) -----;
SELECT concat(f.origin_key, '-', f.dest_key) AS route,
       COUNT(*) AS flights,
       ROUND(SUM(f.block_hours), 0) AS block_hours,
       ROUND(SUM(f.revenue_usd) / SUM(f.block_hours), 0) AS revenue_per_bh,
       ROUND(SUM(f.margin_usd) / SUM(f.block_hours), 0) AS margin_per_bh,
       ROUND(100 * SUM(f.margin_usd) / SUM(f.revenue_usd), 1) AS margin_pct,
       ROUND(100 * AVG(f.cargo_weight_kg / a.max_payload_kg), 1) AS load_factor_pct
FROM gss.fact_flight f
JOIN gss.dim_aircraft a ON f.aircraft_key = a.aircraft_key
WHERE f.status <> 'CANCELLED' AND f.revenue_usd > 0 AND f.block_hours > 0
GROUP BY concat(f.origin_key, '-', f.dest_key)
HAVING COUNT(*) >= 10
ORDER BY margin_per_bh DESC;

!echo ;
!echo ----- 2. Demand against supply by destination -----;
WITH asked AS (
  SELECT r.dest_iata AS dest,
         COUNT(*) AS requests,
         ROUND(SUM(r.weight_kg) / 1000, 0) AS tonnes_asked,
         SUM(CASE WHEN b.thread_id IS NOT NULL THEN 1 ELSE 0 END) AS became_flights
  FROM gss.request r
  LEFT JOIN (SELECT DISTINCT thread_id FROM gss.document_link
             WHERE doc_type = 'QUOTE' AND flight_key IS NOT NULL) b ON r.thread_id = b.thread_id
  WHERE r.dest_iata IS NOT NULL
  GROUP BY r.dest_iata),
flown AS (
  SELECT dest_key AS dest, COUNT(*) AS flights, ROUND(SUM(cargo_weight_kg) / 1000, 0) AS tonnes_flown
  FROM gss.fact_flight WHERE status <> 'CANCELLED'
  GROUP BY dest_key)
SELECT a.dest, p.city,
       a.requests, a.became_flights,
       ROUND(100.0 * a.became_flights / a.requests, 1) AS conversion_pct,
       COALESCE(f.flights, 0) AS flights_in,
       a.tonnes_asked, COALESCE(f.tonnes_flown, 0) AS tonnes_flown,
       a.requests - a.became_flights AS requests_not_flown
FROM asked a
LEFT JOIN flown f ON a.dest = f.dest
LEFT JOIN gss.dim_airport p ON a.dest = p.airport_key
ORDER BY requests_not_flown DESC;

!echo ;
!echo ----- 3. Cancellations by route -----;
SELECT concat(origin_key, '-', dest_key) AS route,
       COUNT(*) AS planned,
       SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled,
       ROUND(100.0 * SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) / COUNT(*), 1) AS cancelled_pct,
       SUM(CASE WHEN status = 'DIVERTED' THEN 1 ELSE 0 END) AS diverted
FROM gss.fact_flight
GROUP BY concat(origin_key, '-', dest_key)
HAVING SUM(CASE WHEN status IN ('CANCELLED', 'DIVERTED') THEN 1 ELSE 0 END) > 0
ORDER BY cancelled DESC, diverted DESC
LIMIT 10;
