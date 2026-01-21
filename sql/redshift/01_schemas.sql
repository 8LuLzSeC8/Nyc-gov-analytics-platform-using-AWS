-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Analytics Engineering
-- Purpose: Create database and core schemas for downstream analytics (final layer)
-- Dependencies: None
-- Quality expectations:
--   - Schemas exist before any downstream objects are created
--   - Script is idempotent (safe to re-run)
-- Change Log:
--   - 2026-01-21: Initial version (staging/mdm/dim/fact)
-- =============================================================================

BEGIN;

-- Create database (run as a privileged user)
CREATE DATABASE analytics;

-- Final-layer schemas
CREATE SCHEMA IF NOT EXISTS final_staging;
CREATE SCHEMA IF NOT EXISTS final_mdm;
CREATE SCHEMA IF NOT EXISTS final_dim;
CREATE SCHEMA IF NOT EXISTS final_fact;

-- Data quality framework schema (for tests/results/asserts)
CREATE SCHEMA IF NOT EXISTS final_dq;

COMMIT;
