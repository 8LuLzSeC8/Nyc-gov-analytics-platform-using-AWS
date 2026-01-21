-- =============================================================================
-- Author: Analytics Engineering
-- Owner: Analytics
-- Purpose:
--   - Common analytical queries on final_fact.trip_fact
--   - Used for validation, BI, and ad-hoc analysis
-- Dependencies:
--   - final_fact.trip_fact
--   - final_dim.zone_dim
--   - final_dim.vendor_dim
--   - final_dim.ratecode_dim
--   - final_dim.payment_type_dim
-- Change Log:
--   - 2026-01-21: Initial version
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Daily trips & revenue trend
-- -----------------------------------------------------------------------------
SELECT
  DATE(pickup_datetime) AS trip_date,
  COUNT(*)              AS trips,
  SUM(total_amount)     AS total_revenue
FROM final_fact.trip_fact
GROUP BY 1
ORDER BY 1;


-- -----------------------------------------------------------------------------
-- 2. Top pickup zones by trip count
-- -----------------------------------------------------------------------------
SELECT
  z.zone,
  z.borough,
  COUNT(*) AS trips
FROM final_fact.trip_fact f
JOIN final_dim.zone_dim z
  ON f.pickup_zone_sk = z.zone_sk
GROUP BY 1,2
ORDER BY trips DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
-- 3. Revenue by vendor
-- -----------------------------------------------------------------------------
SELECT
  v.vendor_name,
  COUNT(*)          AS trips,
  SUM(total_amount) AS revenue
FROM final_fact.trip_fact f
JOIN final_dim.vendor_dim v
  ON f.vendor_sk = v.vendor_sk
GROUP BY 1
ORDER BY revenue DESC;


-- -----------------------------------------------------------------------------
-- 4. Average fare by ratecode
-- -----------------------------------------------------------------------------
SELECT
  r.rate_code_name,
  AVG(total_amount) AS avg_fare
FROM final_fact.trip_fact f
JOIN final_dim.ratecode_dim r
  ON f.ratecode_sk = r.ratecode_sk
GROUP BY 1
ORDER BY avg_fare DESC;


-- -----------------------------------------------------------------------------
-- 5. Payment type distribution
-- -----------------------------------------------------------------------------
SELECT
  p.payment_type_name,
  COUNT(*) AS trips
FROM final_fact.trip_fact f
JOIN final_dim.payment_type_dim p
  ON f.payment_type_sk = p.payment_type_sk
GROUP BY 1
ORDER BY trips DESC;


-- -----------------------------------------------------------------------------
-- 6. Trip duration sanity check (DQ-style query)
-- -----------------------------------------------------------------------------
SELECT
  COUNT(*) AS negative_duration_trips
FROM final_fact.trip_fact
WHERE dropoff_datetime < pickup_datetime;
