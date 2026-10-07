-- Validation queries - run after a pipeline update.

-- 1. Row counts through the medallion layers
SELECT 'bronze.rides_raw'     AS layer, COUNT(*) AS row_count FROM rides.bronze.rides_raw
UNION ALL SELECT 'bronze.bulk_rides',     COUNT(*) FROM rides.bronze.bulk_rides
UNION ALL SELECT 'silver.stg_rides',      COUNT(*) FROM rides.silver.stg_rides
UNION ALL SELECT 'silver.rides_quarantine', COUNT(*) FROM rides.silver.rides_quarantine
UNION ALL SELECT 'silver.silver_obt',     COUNT(*) FROM rides.silver.silver_obt
UNION ALL SELECT 'gold.fact_rides',       COUNT(*) FROM rides.gold.fact_rides;

-- 2. Fact grain check: must return zero rows
SELECT ride_id, COUNT(*) AS n
FROM rides.gold.fact_rides
GROUP BY ride_id
HAVING COUNT(*) > 1;

-- 3. SCD Type 2 history for the renamed city
SELECT city_id, city, state, region, __START_AT, __END_AT
FROM rides.gold.dim_location
WHERE city_id = 2
ORDER BY __START_AT;

-- 4. Exactly one current row per city: must return zero rows
SELECT city_id, COUNT(*) AS current_rows
FROM rides.gold.dim_location
WHERE __END_AT IS NULL
GROUP BY city_id
HAVING COUNT(*) <> 1;

-- 5. Orphan check: facts without a matching current city (should be 0)
SELECT COUNT(*) AS orphan_facts
FROM rides.gold.fact_rides f
LEFT JOIN rides.gold.dim_location d
  ON f.city_id = d.city_id AND d.__END_AT IS NULL
WHERE d.city_id IS NULL;

-- 6. Data-quality expectation results from the pipeline event log
SELECT
  timestamp,
  details:flow_progress.data_quality.expectations AS expectations
FROM event_log(TABLE(rides.silver.stg_rides))
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality.expectations IS NOT NULL
ORDER BY timestamp DESC
LIMIT 20;

-- 7. Dashboard-ready aggregate
SELECT *
FROM rides.gold.city_hourly_metrics
ORDER BY booking_hour DESC, gross_revenue DESC
LIMIT 50;
