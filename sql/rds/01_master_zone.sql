-- ============================================================
-- NYC Taxi MDM (Zones) - V2 Schema (3 tables + proc)
--  1) mdm_zone_record_v2  : all ingested rows per batch + stewardship status
--  2) mdm_zone_match_v2   : matched pairs + target + zone names
--  3) mdm_zone_golden_v2  : SCD2 golden records
--  Procedure:
--    sp_publish_zone_golden_v2(p_batch_id TEXT)
--  View:
--    v_mdm_zone_golden_current_v2
-- ============================================================

-- Optional: ensure crypto functions exist for gen_random_uuid() (if you use it elsewhere)
-- CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ----------------------------
-- 1) Record table (staging + stewardship)
-- ----------------------------
CREATE TABLE IF NOT EXISTS mdm_zone_record_v2 (
  batch_id       TEXT        NOT NULL,
  location_id    INT         NOT NULL,
  borough        TEXT        NOT NULL,
  zone           TEXT        NOT NULL,
  service_zone   TEXT,

  status         TEXT        NOT NULL CHECK (status IN ('APPROVED','PENDING','REJECTED')),
  source_file    TEXT,

  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (batch_id, location_id)
);

CREATE INDEX IF NOT EXISTS ix_mdm_zone_record_v2_status
  ON mdm_zone_record_v2 (status);

CREATE INDEX IF NOT EXISTS ix_mdm_zone_record_v2_batch
  ON mdm_zone_record_v2 (batch_id);

CREATE INDEX IF NOT EXISTS ix_mdm_zone_record_v2_location
  ON mdm_zone_record_v2 (location_id);


-- ----------------------------
-- 2) Match table (pairs + score + target + zone names)
-- ----------------------------
CREATE TABLE IF NOT EXISTS mdm_zone_match_v2 (
  match_id              BIGSERIAL   PRIMARY KEY,
  batch_id              TEXT        NOT NULL,

  location_id_1         INT         NOT NULL,
  location_id_2         INT         NOT NULL,

  borough               TEXT        NOT NULL,
  zone_1                TEXT        NOT NULL,
  zone_2                TEXT        NOT NULL,

  score                 INT         NOT NULL CHECK (score BETWEEN 0 AND 100),
  confidence_tier       TEXT        NOT NULL CHECK (confidence_tier IN ('HIGH','MEDIUM')),
  action                TEXT        NOT NULL CHECK (action IN ('STEWARD_REVIEW','AUTO_MERGE')),

  recommended_golden_id INT         NOT NULL,

  created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_mdm_zone_match_v2_batch
  ON mdm_zone_match_v2 (batch_id);

CREATE INDEX IF NOT EXISTS ix_mdm_zone_match_v2_ids
  ON mdm_zone_match_v2 (location_id_1, location_id_2);

CREATE INDEX IF NOT EXISTS ix_mdm_zone_match_v2_action
  ON mdm_zone_match_v2 (action);


-- ----------------------------
-- 3) Golden table (SCD2)
-- ----------------------------
CREATE TABLE IF NOT EXISTS mdm_zone_golden_v2 (
  golden_sk       BIGSERIAL   PRIMARY KEY,

  location_id     INT         NOT NULL,
  borough         TEXT        NOT NULL,
  zone            TEXT        NOT NULL,
  service_zone    TEXT,

  record_hash     TEXT        NOT NULL,

  effective_from  TIMESTAMPTZ NOT NULL,
  effective_to    TIMESTAMPTZ,
  is_current      BOOLEAN     NOT NULL DEFAULT TRUE,

  source_batch_id TEXT        NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One current golden per LocationID
CREATE UNIQUE INDEX IF NOT EXISTS ux_mdm_zone_golden_v2_current
  ON mdm_zone_golden_v2(location_id)
  WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS ix_mdm_zone_golden_v2_location
  ON mdm_zone_golden_v2 (location_id);

CREATE INDEX IF NOT EXISTS ix_mdm_zone_golden_v2_batch
  ON mdm_zone_golden_v2 (source_batch_id);


-- ============================================================
-- Procedure: publish APPROVED records from mdm_zone_record_v2 into mdm_zone_golden_v2 (SCD2)
-- Idempotent per batch: re-running will not duplicate identical current records.
-- ============================================================
CREATE OR REPLACE PROCEDURE sp_publish_zone_golden_v2(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
  -- Expire current golden rows if incoming approved differs (hash mismatch)
  UPDATE mdm_zone_golden_v2 g
  SET effective_to = now(),
      is_current = FALSE
  FROM (
    SELECT
      r.location_id,
      md5(coalesce(r.borough,'') || '|' || coalesce(r.zone,'') || '|' || coalesce(r.service_zone,'')) AS new_hash
    FROM mdm_zone_record_v2 r
    WHERE r.batch_id = p_batch_id
      AND r.status = 'APPROVED'
  ) incoming
  WHERE g.location_id = incoming.location_id
    AND g.is_current = TRUE
    AND g.record_hash <> incoming.new_hash;

  -- Insert new current golden rows for:
  -- - new LocationIDs (no current row)
  -- - changed LocationIDs (hash differs from current)
  INSERT INTO mdm_zone_golden_v2 (
    location_id, borough, zone, service_zone,
    record_hash,
    effective_from, effective_to, is_current,
    source_batch_id
  )
  SELECT
    r.location_id,
    r.borough,
    r.zone,
    r.service_zone,
    md5(coalesce(r.borough,'') || '|' || coalesce(r.zone,'') || '|' || coalesce(r.service_zone,'')) AS record_hash,
    now(),
    NULL,
    TRUE,
    p_batch_id
  FROM mdm_zone_record_v2 r
  LEFT JOIN mdm_zone_golden_v2 g
    ON g.location_id = r.location_id AND g.is_current = TRUE
  WHERE r.batch_id = p_batch_id
    AND r.status = 'APPROVED'
    AND (
      g.location_id IS NULL OR
      g.record_hash <> md5(coalesce(r.borough,'') || '|' || coalesce(r.zone,'') || '|' || coalesce(r.service_zone,''))
    );

END;
$$;
-- Example call:
CALL sp_publish_zone_golden_v2('my batch id here');

-- ----------------------------
-- Current golden snapshot view
-- ----------------------------
CREATE OR REPLACE VIEW v_mdm_zone_golden_current_v2 AS
SELECT
  location_id,
  borough,
  zone,
  service_zone,
  effective_from,
  effective_to,
  is_current,
  source_batch_id
FROM mdm_zone_golden_v2
WHERE is_current = TRUE;

-- ----------------------------
-- All golden snapshot view
CREATE OR REPLACE VIEW v_mdm_zone_golden_all_v2 AS
SELECT
  golden_sk,
  location_id,
  borough,
  zone,
  service_zone,
  record_hash,
  effective_from,
  effective_to,
  is_current,
  source_batch_id,
  created_at
FROM mdm_zone_golden_v2;
