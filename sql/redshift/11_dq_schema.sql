-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Analytics Engineering
-- Purpose:
--   - Create Data Quality (DQ) framework objects
--   - Provide append-only audit table for test results
--   - Provide reusable assertion function for SQL-based checks
-- Dependencies: None
-- Quality expectations:
--   - Every DQ test writes exactly one auditable PASS/FAIL row
--   - Results are append-only (no updates)
--   - Framework is reusable across schemas/tables
-- Change Log:
--   - 2026-01-21: Initial version
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1) DQ schema
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS final_dq;

-- -----------------------------------------------------------------------------
-- 2) Test results table (append-only audit)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS final_dq.test_results (
  test_run_id   UUID        NOT NULL DEFAULT gen_random_uuid(),
  test_name     TEXT        NOT NULL,
  passed        BOOLEAN     NOT NULL,
  metric_value  NUMERIC     NULL,
  threshold     NUMERIC     NULL,
  details       TEXT        NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_by    TEXT        NOT NULL DEFAULT current_user
);

CREATE INDEX IF NOT EXISTS idx_test_results_created_at
  ON final_dq.test_results(created_at);

CREATE INDEX IF NOT EXISTS idx_test_results_test_name
  ON final_dq.test_results(test_name);

-- -----------------------------------------------------------------------------
-- 3) Assertion helper function
-- -----------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE OR REPLACE FUNCTION final_dq.assert_true(
  p_test_name TEXT,
  p_passed BOOLEAN,
  p_metric_value NUMERIC DEFAULT NULL,
  p_threshold NUMERIC DEFAULT NULL,
  p_details TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
  INSERT INTO final_dq.test_results (
    test_name,
    passed,
    metric_value,
    threshold,
    details
  )
  VALUES (
    p_test_name,
    p_passed,
    p_metric_value,
    p_threshold,
    p_details
  );
END;
$$;

COMMIT;
