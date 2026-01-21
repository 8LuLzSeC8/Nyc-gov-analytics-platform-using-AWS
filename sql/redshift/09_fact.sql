-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Analytics Engineering
-- Purpose:
--   - Access curated trips data in S3 via AWS Glue Data Catalog (Spectrum)
--   - Stage curated data from spectrum.finalrun_id_test_15
--   - Build final_fact.trip_fact by joining staged trips to final_dim dimensions
-- Dependencies:
--   - AWS Glue Data Catalog database: final_glue_db
--   - Spectrum table: spectrum.finalrun_id_test_15
--   - final_dim.vendor_dim
--   - final_dim.ratecode_dim
--   - final_dim.payment_type_dim
--   - final_dim.zone_dim
-- Quality expectations:
--   - Fact rows are appendable per run_id
--   - All joins are LEFT joins (no row loss)
--   - Unknown dimension values handled via COALESCE
-- Change Log:
--   - 2026-01-21: Initial version
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 0) External schema for Glue Catalog access
-- -----------------------------------------------------------------------------
CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum
FROM DATA CATALOG
DATABASE 'final_glue_db'
IAM_ROLE default;

-- -----------------------------------------------------------------------------
-- 1) Staging table (matches Spectrum schema)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS final_staging.trips_curated;

CREATE TABLE final_staging.trips_curated
AS
SELECT *
FROM spectrum.finalrun_id_test_15
LIMIT 0;

-- Load curated data (full load or filtered by run_id)
TRUNCATE TABLE final_staging.trips_curated;

INSERT INTO final_staging.trips_curated
SELECT *
FROM spectrum.finalrun_id_test_15;
-- WHERE run_id = 'test-15';  -- optional slice control

-- -----------------------------------------------------------------------------
-- 2) Fact table in final_fact
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS final_fact.trip_fact;

CREATE TABLE final_fact.trip_fact (
  trip_fact_sk          BIGINT IDENTITY(1,1),

  vendor_sk             BIGINT,
  ratecode_sk           BIGINT,
  payment_type_sk       BIGINT,
  pickup_zone_sk        BIGINT,
  dropoff_zone_sk       BIGINT,

  pickup_datetime       TIMESTAMP,
  dropoff_datetime      TIMESTAMP,

  passenger_count       INTEGER,
  trip_distance         DOUBLE PRECISION,

  fare_amount           DOUBLE PRECISION,
  extra                 DOUBLE PRECISION,
  mta_tax               DOUBLE PRECISION,
  tip_amount            DOUBLE PRECISION,
  tolls_amount          DOUBLE PRECISION,
  improvement_surcharge DOUBLE PRECISION,
  total_amount          DOUBLE PRECISION,
  congestion_surcharge  DOUBLE PRECISION,
  airport_fee           DOUBLE PRECISION,
  cbd_congestion_fee    DOUBLE PRECISION,

  run_id                VARCHAR(256),
  ingested_at_utc       VARCHAR(256),

  created_at            TIMESTAMP DEFAULT GETDATE()
)
DISTSTYLE AUTO
SORTKEY (pickup_datetime);

-- -----------------------------------------------------------------------------
-- 3) Populate fact table
-- -----------------------------------------------------------------------------
INSERT INTO final_fact.trip_fact (
  vendor_sk,
  ratecode_sk,
  payment_type_sk,
  pickup_zone_sk,
  dropoff_zone_sk,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee,
  cbd_congestion_fee,
  run_id,
  ingested_at_utc
)
SELECT
  v.vendor_sk,
  r.ratecode_sk,
  p.payment_type_sk,
  zpu.zone_sk,
  zdo.zone_sk,

  t.tpep_pickup_datetime,
  t.tpep_dropoff_datetime,

  t.passenger_count,
  t.trip_distance,

  t.fare_amount,
  t.extra,
  t.mta_tax,
  t.tip_amount,
  t.tolls_amount,
  t.improvement_surcharge,
  t.total_amount,
  t.congestion_surcharge,
  t.airport_fee,
  t.cbd_congestion_fee,

  t.run_id,
  t.ingested_at_utc
FROM final_staging.trips_curated t
LEFT JOIN final_dim.vendor_dim v
  ON v.vendor_id = t.vendorid
LEFT JOIN final_dim.ratecode_dim r
  ON r.rate_code_id = COALESCE(t.ratecodeid, 99)
LEFT JOIN final_dim.payment_type_dim p
  ON p.payment_type_id = COALESCE(t.payment_type, 5)
LEFT JOIN final_dim.zone_dim zpu
  ON zpu.location_id = t.pulocationid
LEFT JOIN final_dim.zone_dim zdo
  ON zdo.location_id = t.dolocationid;

COMMIT;
