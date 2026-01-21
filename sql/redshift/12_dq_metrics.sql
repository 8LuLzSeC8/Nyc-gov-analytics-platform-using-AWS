-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Analytics Engineering
-- Purpose:
--   - Compute data quality metrics for final_fact.trip_fact
--   - Used by assertions and monitoring (no pass/fail here)
-- Dependencies:
--   - final_fact.trip_fact
-- Quality expectations:
--   - Query returns exactly one row of metrics
--   - Metrics are deterministic for a given data slice
-- Change Log:
--   - 2026-01-21: Initial version
-- =============================================================================

WITH base AS (
  SELECT *
  FROM final_fact.trip_fact
),
metrics AS (
  SELECT
    COUNT(*)::BIGINT AS total_rows,

    -- Completeness
    AVG((pickup_datetime IS NOT NULL)::INT)::NUMERIC(10,6)  AS pickup_ts_fill_rate,
    AVG((dropoff_datetime IS NOT NULL)::INT)::NUMERIC(10,6) AS dropoff_ts_fill_rate,
    AVG((total_amount IS NOT NULL)::INT)::NUMERIC(10,6)     AS total_amount_fill_rate,

    -- Validity
    SUM((dropoff_datetime < pickup_datetime)::INT)::BIGINT AS bad_time_order_count,
    SUM((trip_distance < 0)::INT)::BIGINT                  AS negative_distance_count,

    -- Referential integrity (FK coverage)
    AVG((vendor_sk IS NOT NULL)::INT)::NUMERIC(10,6)        AS vendor_fk_fill_rate,
    AVG((ratecode_sk IS NOT NULL)::INT)::NUMERIC(10,6)      AS ratecode_fk_fill_rate,
    AVG((payment_type_sk IS NOT NULL)::INT)::NUMERIC(10,6)  AS payment_fk_fill_rate,
    AVG((pickup_zone_sk IS NOT NULL)::INT)::NUMERIC(10,6)   AS pickup_zone_fk_fill_rate,
    AVG((dropoff_zone_sk IS NOT NULL)::INT)::NUMERIC(10,6)  AS dropoff_zone_fk_fill_rate

  FROM base
)
SELECT *
FROM metrics;
