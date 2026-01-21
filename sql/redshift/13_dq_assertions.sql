-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Analytics Engineering
-- Purpose:
--   - Execute DQ assertions for final_fact.trip_fact
--   - Persist PASS/FAIL results into final_dq.test_results (append-only)
-- Dependencies:
--   - final_dq.assert_true
--   - final_fact.trip_fact
-- Quality expectations:
--   - Each assertion writes exactly one row to final_dq.test_results
--   - Thresholds are explicit and reviewable in code
-- Change Log:
--   - 2026-01-21: Initial version
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1) Basic row count (must be > 0)
-- -----------------------------------------------------------------------------
DO $$
DECLARE
  v_total BIGINT;
BEGIN
  SELECT COUNT(*) INTO v_total
  FROM final_fact.trip_fact;

  PERFORM final_dq.assert_true(
    'final_fact.trip_fact.row_count_gt_0',
    (v_total > 0),
    v_total::NUMERIC,
    1,
    'Row count should be greater than 0'
  );
END $$;


-- -----------------------------------------------------------------------------
-- 2) No negative distances
-- -----------------------------------------------------------------------------
DO $$
DECLARE
  v_bad BIGINT;
BEGIN
  SELECT COUNT(*) INTO v_bad
  FROM final_fact.trip_fact
  WHERE trip_distance < 0;

  PERFORM final_dq.assert_true(
    'final_fact.trip_fact.negative_distance_count_eq_0',
    (v_bad = 0),
    v_bad::NUMERIC,
    0,
    'Trip distance must not be negative'
  );
END $$;


-- -----------------------------------------------------------------------------
-- 3) No dropoff before pickup
-- -----------------------------------------------------------------------------
DO $$
DECLARE
  v_bad BIGINT;
BEGIN
  SELECT COUNT(*) INTO v_bad
  FROM final_fact.trip_fact
  WHERE dropoff_datetime < pickup_datetime;

  PERFORM final_dq.assert_true(
    'final_fact.trip_fact.bad_time_order_count_eq_0',
    (v_bad = 0),
    v_bad::NUMERIC,
    0,
    'Dropoff must not be earlier than pickup'
  );
END $$;


-- -----------------------------------------------------------------------------
-- 4) Completeness: pickup/dropoff timestamps >= 0.999
-- -----------------------------------------------------------------------------
DO $$
DECLARE
  v_fill NUMERIC;
BEGIN
  SELECT AVG((pickup_datetime IS NOT NULL)::INT)::NUMERIC(10,6)
  INTO v_fill
  FROM final_fact.trip_fact;

  PERFORM final_dq.assert_true(
    'final_fact.trip_fact.pickup_ts_fill_rate_ge_0_999',
    (v_fill >= 0.999),
    v_fill,
    0.999,
    'pickup_datetime fill rate should be at least 0.999'
  );
END $$;

DO $$
DECLARE
  v_fill NUMERIC;
BEGIN
  SELECT AVG((dropoff_datetime IS NOT NULL)::INT)::NUMERIC(10,6)
  INTO v_fill
  FROM final_fact.trip_fact;

  PERFORM final_dq.assert_true(
    'final_fact.trip_fact.dropoff_ts_fill_rate_ge_0_999',
    (v_fill >= 0.999),
    v_fill,
    0.999,
    'dropoff_datetime fill rate should be at least 0.999'
  );
END $$;


-- -----------------------------------------------------------------------------
-- 5) Completeness: total_amount >= 0.999
-- -----------------------------------------------------------------------------
DO $$
DECLARE
  v_fill NUMERIC;
BEGIN
  SELECT AVG((total_amount IS NOT NULL)::INT)::NUMERIC(10,6)
  INTO v_fill
  FROM final_fact.trip_fact;

  PERFORM final_dq.assert_true(
    'final_fact.trip_fact.total_amount_fill_rate_ge_0_999',
    (v_fill >= 0.999),
    v_fill,
    0.999,
    'total_amount fill rate should be at least 0.999'
  );
END $$;


-- -----------------------------------------------------------------------------
-- 6) FK coverage checks (dim joins should succeed most of the time)
--     Adjust thresholds if you expect unknowns.
-- -----------------------------------------------------------------------------
DO $$
DECLARE
  v_fill NUMERIC;
BEGIN
  SELECT AVG((vendor_sk IS NOT NULL)::INT)::NUMERIC(10,6)
  INTO v_fill
  FROM final_fact.trip_fact;

  PERFORM final_dq.assert_true(
    'final_fact.trip_fact.vendor_fk_fill_rate_ge_0_995',
    (v_fill >= 0.995),
    v_fill,
    0.995,
    'vendor_sk should be present for at least 99.5% of rows'
  );
END $$;

DO $$
DECLARE
  v_fill NUMERIC;
BEGIN
  SELECT AVG((ratecode_sk IS NOT NULL)::INT)::NUMERIC(10,6)
  INTO v_fill
  FROM final_fact.trip_fact;

  PERFORM final_dq.assert_true(
    'final_fact.trip_fact.ratecode_fk_fill_rate_ge_0_995',
    (v_fill >= 0.995),
    v_fill,
    0.995,
    'ratecode_sk should be present for at least 99.5% of rows'
  );
END $$;

DO $$
DECLARE
  v_fill NUMERIC;
BEGIN
  SELECT AVG((payment_type_sk IS NOT NULL)::INT)::NUMERIC(10,6)
  INTO v_fill
  FROM final_fact.trip_fact;

  PERFORM final_dq.assert_true(
    'final_fact.trip_fact.payment_fk_fill_rate_ge_0_995',
    (v_fill >= 0.995),
    v_fill,
    0.995,
    'payment_type_sk should be present for at least 99.5% of rows'
  );
END $$;

DO $$
DECLARE
  v_fill NUMERIC;
BEGIN
  SELECT AVG((pickup_zone_sk IS NOT NULL)::INT)::NUMERIC(10,6)
  INTO v_fill
  FROM final_fact.trip_fact;

  PERFORM final_dq.assert_true(
    'final_fact.trip_fact.pickup_zone_fk_fill_rate_ge_0_995',
    (v_fill >= 0.995),
    v_fill,
    0.995,
    'pickup_zone_sk should be present for at least 99.5% of rows'
  );
END $$;

DO $$
DECLARE
  v_fill NUMERIC;
BEGIN
  SELECT AVG((dropoff_zone_sk IS NOT NULL)::INT)::NUMERIC(10,6)
  INTO v_fill
  FROM final_fact.trip_fact;

  PERFORM final_dq.assert_true(
    'final_fact.trip_fact.dropoff_zone_fk_fill_rate_ge_0_995',
    (v_fill >= 0.995),
    v_fill,
    0.995,
    'dropoff_zone_sk should be present for at least 99.5% of rows'
  );
END $$;

COMMIT;

-- View latest results
SELECT *
FROM final_dq.test_results
ORDER BY created_at DESC, test_name
LIMIT 100;
