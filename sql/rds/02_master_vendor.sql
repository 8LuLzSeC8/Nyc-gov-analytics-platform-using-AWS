-- ============================================================
-- NYC Taxi MDM (Vendor) - V2 Schema (3 tables + proc + view)
--  1) mdm_vendor_record_v2
--  2) mdm_vendor_match_v2
--  3) mdm_vendor_golden_v2
--  Procedure:
--    sp_publish_vendor_golden_v2(p_batch_id TEXT)
--  View:
--    v_mdm_vendor_golden_current_v2
-- ============================================================

-- ----------------------------
-- 1) Record table (staging + stewardship)
-- ----------------------------
CREATE TABLE IF NOT EXISTS mdm_vendor_record_v2 (
  batch_id       TEXT        NOT NULL,
  vendor_id      INT         NOT NULL,
  vendor_name    TEXT        NOT NULL,

  status         TEXT        NOT NULL CHECK (status IN ('APPROVED','PENDING','REJECTED')),
  source_file    TEXT,

  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (batch_id, vendor_id)
);

CREATE INDEX IF NOT EXISTS ix_mdm_vendor_record_v2_status
  ON mdm_vendor_record_v2 (status);

CREATE INDEX IF NOT EXISTS ix_mdm_vendor_record_v2_batch
  ON mdm_vendor_record_v2 (batch_id);

CREATE INDEX IF NOT EXISTS ix_mdm_vendor_record_v2_vendor
  ON mdm_vendor_record_v2 (vendor_id);


-- ----------------------------
-- 2) Match table (pairs + score + target + names)
-- (May be unused if you don't do matching, but kept for pattern parity)
-- ----------------------------
CREATE TABLE IF NOT EXISTS mdm_vendor_match_v2 (
  match_id              BIGSERIAL   PRIMARY KEY,
  batch_id              TEXT        NOT NULL,

  vendor_id_1           INT         NOT NULL,
  vendor_id_2           INT         NOT NULL,

  vendor_name_1         TEXT        NOT NULL,
  vendor_name_2         TEXT        NOT NULL,

  score                 INT         NOT NULL CHECK (score BETWEEN 0 AND 100),
  confidence_tier       TEXT        NOT NULL CHECK (confidence_tier IN ('HIGH','MEDIUM')),
  action                TEXT        NOT NULL CHECK (action IN ('STEWARD_REVIEW','AUTO_MERGE')),

  recommended_golden_id INT         NOT NULL,

  created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_mdm_vendor_match_v2_batch
  ON mdm_vendor_match_v2 (batch_id);

CREATE INDEX IF NOT EXISTS ix_mdm_vendor_match_v2_ids
  ON mdm_vendor_match_v2 (vendor_id_1, vendor_id_2);

CREATE INDEX IF NOT EXISTS ix_mdm_vendor_match_v2_action
  ON mdm_vendor_match_v2 (action);


-- ----------------------------
-- 3) Golden table (SCD2)
-- ----------------------------
CREATE TABLE IF NOT EXISTS mdm_vendor_golden_v2 (
  golden_sk       BIGSERIAL   PRIMARY KEY,

  vendor_id       INT         NOT NULL,
  vendor_name     TEXT        NOT NULL,

  record_hash     TEXT        NOT NULL,

  effective_from  TIMESTAMPTZ NOT NULL,
  effective_to    TIMESTAMPTZ,
  is_current      BOOLEAN     NOT NULL DEFAULT TRUE,

  source_batch_id TEXT        NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One current golden per VendorID
CREATE UNIQUE INDEX IF NOT EXISTS ux_mdm_vendor_golden_v2_current
  ON mdm_vendor_golden_v2(vendor_id)
  WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS ix_mdm_vendor_golden_v2_vendor
  ON mdm_vendor_golden_v2 (vendor_id);

CREATE INDEX IF NOT EXISTS ix_mdm_vendor_golden_v2_batch
  ON mdm_vendor_golden_v2 (source_batch_id);


-- ============================================================
-- Procedure: publish APPROVED records into golden (SCD2)
-- Idempotent per batch.
-- ============================================================
CREATE OR REPLACE PROCEDURE sp_publish_vendor_golden_v2(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
  -- Expire current golden rows if incoming approved differs (hash mismatch)
  UPDATE mdm_vendor_golden_v2 g
  SET effective_to = now(),
      is_current = FALSE
  FROM (
    SELECT
      r.vendor_id,
      md5(coalesce(r.vendor_name,'')) AS new_hash
    FROM mdm_vendor_record_v2 r
    WHERE r.batch_id = p_batch_id
      AND r.status = 'APPROVED'
  ) incoming
  WHERE g.vendor_id = incoming.vendor_id
    AND g.is_current = TRUE
    AND g.record_hash <> incoming.new_hash;

  -- Insert new current golden rows for new or changed vendor_ids
  INSERT INTO mdm_vendor_golden_v2 (
    vendor_id, vendor_name,
    record_hash,
    effective_from, effective_to, is_current,
    source_batch_id
  )
  SELECT
    r.vendor_id,
    r.vendor_name,
    md5(coalesce(r.vendor_name,'')) AS record_hash,
    now(),
    NULL,
    TRUE,
    p_batch_id
  FROM mdm_vendor_record_v2 r
  LEFT JOIN mdm_vendor_golden_v2 g
    ON g.vendor_id = r.vendor_id AND g.is_current = TRUE
  WHERE r.batch_id = p_batch_id
    AND r.status = 'APPROVED'
    AND (
      g.vendor_id IS NULL OR
      g.record_hash <> md5(coalesce(r.vendor_name,''))
    );
END;
$$;


-- ----------------------------
-- Current golden snapshot view
-- ----------------------------
CREATE OR REPLACE VIEW v_mdm_vendor_golden_current_v2 AS
SELECT
  vendor_id,
  vendor_name,
  effective_from,
  effective_to,
  is_current,
  source_batch_id
FROM mdm_vendor_golden_v2
WHERE is_current = TRUE;


-- ----------------------------
-- Seed example: direct insert into record table + publish
-- ----------------------------
-- Use a batch id you choose:
-- e.g. 'batch_2026_01_21'
INSERT INTO mdm_vendor_record_v2 (batch_id, vendor_id, vendor_name, status, source_file)
VALUES
  ('"batch_20260120_01"', 1, 'Creative Mobile Technologies, LLC', 'APPROVED', 'manual_seed'),
  ('"batch_20260120_01"', 2, 'Curb Mobility, LLC',              'APPROVED', 'manual_seed'),
  ('"batch_20260120_01"', 6, 'Myle Technologies Inc',           'APPROVED', 'manual_seed'),
  ('"batch_20260120_01"', 7, 'Helix',                           'APPROVED', 'manual_seed')
ON CONFLICT (batch_id, vendor_id) DO UPDATE
SET vendor_name = EXCLUDED.vendor_name,
    status      = EXCLUDED.status,
    source_file = EXCLUDED.source_file,
    updated_at  = now();

CALL sp_publish_vendor_golden_v2('"batch_20260120_01"');

-- ----------------------------
-- All golden snapshot view
CREATE OR REPLACE VIEW v_mdm_vendor_golden_all_v2 AS
SELECT
  golden_sk,
  vendor_id,
  vendor_name,
  record_hash,
  effective_from,
  effective_to,
  is_current,
  source_batch_id,
  created_at
FROM mdm_vendor_golden_v2;

