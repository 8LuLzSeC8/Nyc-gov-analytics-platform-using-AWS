-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Analytics Engineering
-- Purpose:
--   - Load Zone Master Snapshot from S3 into final_staging
--   - Upsert authoritative master records into final_mdm (snapshot-driven SCD2)
-- Dependencies:
--   - S3 object: master zone snapshot (schema shown in source system)
-- Quality expectations:
--   - final_staging schema matches snapshot schema exactly
--   - golden_sk uniquely identifies a zone master record version
--   - location_id is the business key for zone
--   - Upsert is repeatable (idempotent) for a given snapshot load
-- Change Log:
--   - 2026-01-21: Initial version
-- =============================================================================

BEGIN;

-- -----------------------------------------------------------------------------
-- 1) Staging table: schema matches the S3 snapshot exactly
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS final_staging.zone_master_snapshot_raw;

CREATE TABLE final_staging.zone_master_snapshot_raw (
  golden_sk       BIGINT,
  location_id     INT,
  borough         VARCHAR(256),
  zone            VARCHAR(256),
  service_zone    VARCHAR(256),
  effective_from  TIMESTAMP,
  effective_to    TIMESTAMP,
  is_current      BOOLEAN,
  source_batch_id VARCHAR(256),
  created_at      TIMESTAMP,
  snapshot_id     VARCHAR(256),
  snapshot_ts     TIMESTAMP
);

-- Load data into staging (truncate + copy pattern)
TRUNCATE TABLE final_staging.zone_master_snapshot_raw;

COPY final_staging.zone_master_snapshot_raw
FROM 's3://<your-bucket>/<your-prefix>/zone_master_snapshot/'
IAM_ROLE '<your-redshift-iam-role-arn>'
FORMAT AS PARQUET;

-- -----------------------------------------------------------------------------
-- 2) MDM table: authoritative zone master (snapshot-driven)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS final_mdm.zone_master (
  golden_sk       BIGINT       NOT NULL,
  location_id     INT          NOT NULL,
  borough         VARCHAR(256),
  zone            VARCHAR(256),
  service_zone    VARCHAR(256),
  effective_from  TIMESTAMP,
  effective_to    TIMESTAMP,
  is_current      BOOLEAN,
  source_batch_id VARCHAR(256),
  created_at      TIMESTAMP,
  snapshot_id     VARCHAR(256),
  snapshot_ts     TIMESTAMP,

  loaded_at       TIMESTAMP    NOT NULL DEFAULT GETDATE(),
  updated_at      TIMESTAMP    NOT NULL DEFAULT GETDATE(),

  CONSTRAINT pk_zone_master PRIMARY KEY (golden_sk)
)
DISTSTYLE ALL
SORTKEY (location_id, snapshot_ts);

-- -----------------------------------------------------------------------------
-- 3) Upsert procedure: merge snapshot rows into final_mdm.zone_master
--    - Inserts new golden_sk
--    - Updates existing golden_sk if attributes/metadata changed
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE final_mdm.sp_upsert_zone_master_snapshot()
LANGUAGE plpgsql
AS $$
BEGIN

  -- Update existing rows when the same golden_sk arrives with any change
  UPDATE final_mdm.zone_master tgt
  SET
    location_id     = src.location_id,
    borough         = src.borough,
    zone            = src.zone,
    service_zone    = src.service_zone,
    effective_from  = src.effective_from,
    effective_to    = src.effective_to,
    is_current      = src.is_current,
    source_batch_id = src.source_batch_id,
    created_at      = src.created_at,
    snapshot_id     = src.snapshot_id,
    snapshot_ts     = src.snapshot_ts,
    updated_at      = GETDATE()
  FROM final_staging.zone_master_snapshot_raw src
  WHERE tgt.golden_sk = src.golden_sk
    AND (
      COALESCE(tgt.location_id, -1)            <> COALESCE(src.location_id, -1)
      OR COALESCE(tgt.borough, '')             <> COALESCE(src.borough, '')
      OR COALESCE(tgt.zone, '')                <> COALESCE(src.zone, '')
      OR COALESCE(tgt.service_zone, '')        <> COALESCE(src.service_zone, '')
      OR COALESCE(tgt.effective_from, '1900-01-01'::timestamp)
         <> COALESCE(src.effective_from, '1900-01-01'::timestamp)
      OR COALESCE(tgt.effective_to, '1900-01-01'::timestamp)
         <> COALESCE(src.effective_to, '1900-01-01'::timestamp)
      OR COALESCE(tgt.is_current, false)       <> COALESCE(src.is_current, false)
      OR COALESCE(tgt.source_batch_id, '')     <> COALESCE(src.source_batch_id, '')
      OR COALESCE(tgt.snapshot_id, '')         <> COALESCE(src.snapshot_id, '')
      OR COALESCE(tgt.snapshot_ts, '1900-01-01'::timestamp)
         <> COALESCE(src.snapshot_ts, '1900-01-01'::timestamp)
    );

  -- Insert new rows
  INSERT INTO final_mdm.zone_master (
    golden_sk,
    location_id,
    borough,
    zone,
    service_zone,
    effective_from,
    effective_to,
    is_current,
    source_batch_id,
    created_at,
    snapshot_id,
    snapshot_ts
  )
  SELECT
    src.golden_sk,
    src.location_id,
    src.borough,
    src.zone,
    src.service_zone,
    src.effective_from,
    src.effective_to,
    src.is_current,
    src.source_batch_id,
    src.created_at,
    src.snapshot_id,
    src.snapshot_ts
  FROM final_staging.zone_master_snapshot_raw src
  LEFT JOIN final_mdm.zone_master tgt
    ON tgt.golden_sk = src.golden_sk
  WHERE tgt.golden_sk IS NULL;

END;
$$;

-- Execute upsert
CALL final_mdm.sp_upsert_zone_master_snapshot();

COMMIT;
