-- ============================================================
-- NYC Taxi MDM (Rate Code) - V2 Schema (3 tables + proc + views)
--  1) mdm_rate_code_record_v2  : ingested rows per batch + stewardship status
--  2) mdm_rate_code_match_v2   : matched pairs + target + rate code names
--  3) mdm_rate_code_golden_v2  : SCD2 golden records
--  Procedure:
--    sp_publish_rate_code_golden_v2(p_batch_id TEXT)
--  Views:
--    v_mdm_rate_code_golden_current_v2
--    v_mdm_rate_code_golden_all_v2
-- ============================================================

-- ----------------------------
-- 1) Record table (staging + stewardship)
-- ----------------------------
CREATE TABLE IF NOT EXISTS mdm_rate_code_record_v2 (
  batch_id         TEXT        NOT NULL,
  rate_code_id     INT         NOT NULL,
  rate_code_name   TEXT        NOT NULL,

  status           TEXT        NOT NULL CHECK (status IN ('APPROVED','PENDING','REJECTED')),
  source_file      TEXT,

  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (batch_id, rate_code_id)
);

CREATE INDEX IF NOT EXISTS ix_mdm_rate_code_record_v2_status
  ON mdm_rate_code_record_v2 (status);

CREATE INDEX IF NOT EXISTS ix_mdm_rate_code_record_v2_batch
  ON mdm_rate_code_record_v2 (batch_id);

CREATE INDEX IF NOT EXISTS ix_mdm_rate_code_record_v2_code
  ON mdm_rate_code_record_v2 (rate_code_id);


-- ----------------------------
-- 2) Match table (pairs + score + target + rate code names)
-- (kept for pattern parity; optional to use)
-- ----------------------------
CREATE TABLE IF NOT EXISTS mdm_rate_code_match_v2 (
  match_id              BIGSERIAL   PRIMARY KEY,
  batch_id              TEXT        NOT NULL,

  rate_code_id_1        INT         NOT NULL,
  rate_code_id_2        INT         NOT NULL,

  rate_code_name_1      TEXT        NOT NULL,
  rate_code_name_2      TEXT        NOT NULL,

  score                 INT         NOT NULL CHECK (score BETWEEN 0 AND 100),
  confidence_tier       TEXT        NOT NULL CHECK (confidence_tier IN ('HIGH','MEDIUM')),
  action                TEXT        NOT NULL CHECK (action IN ('STEWARD_REVIEW','AUTO_MERGE')),

  recommended_golden_id INT         NOT NULL,

  created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_mdm_rate_code_match_v2_batch
  ON mdm_rate_code_match_v2 (batch_id);

CREATE INDEX IF NOT EXISTS ix_mdm_rate_code_match_v2_ids
  ON mdm_rate_code_match_v2 (rate_code_id_1, rate_code_id_2);

CREATE INDEX IF NOT EXISTS ix_mdm_rate_code_match_v2_action
  ON mdm_rate_code_match_v2 (action);


-- ----------------------------
-- 3) Golden table (SCD2)
-- ----------------------------
CREATE TABLE IF NOT EXISTS mdm_rate_code_golden_v2 (
  golden_sk       BIGSERIAL   PRIMARY KEY,

  rate_code_id    INT         NOT NULL,
  rate_code_name  TEXT        NOT NULL,

  record_hash     TEXT        NOT NULL,

  effective_from  TIMESTAMPTZ NOT NULL,
  effective_to    TIMESTAMPTZ,
  is_current      BOOLEAN     NOT NULL DEFAULT TRUE,

  source_batch_id TEXT        NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One current golden per RateCodeID
CREATE UNIQUE INDEX IF NOT EXISTS ux_mdm_rate_code_golden_v2_current
  ON mdm_rate_code_golden_v2(rate_code_id)
  WHERE is_current = TRUE;

CREATE INDEX IF NOT EXISTS ix_mdm_rate_code_golden_v2_code
  ON mdm_rate_code_golden_v2 (rate_code_id);

CREATE INDEX IF NOT EXISTS ix_mdm_rate_code_golden_v2_batch
  ON mdm_rate_code_golden_v2 (source_batch_id);

-- recommended for audit/history lookups
CREATE INDEX IF NOT EXISTS ix_mdm_rate_code_golden_v2_hist
  ON mdm_rate_code_golden_v2 (rate_code_id, effective_from DESC);


-- ============================================================
-- Procedure: publish APPROVED records from mdm_rate_code_record_v2
-- into mdm_rate_code_golden_v2 (SCD2)
-- Idempotent per batch: re-running will not duplicate identical current records.
-- ============================================================
CREATE OR REPLACE PROCEDURE sp_publish_rate_code_golden_v2(p_batch_id TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
  -- Expire current golden rows if incoming approved differs (hash mismatch)
  UPDATE mdm_rate_code_golden_v2 g
  SET effective_to = now(),
      is_current = FALSE
  FROM (
    SELECT
      r.rate_code_id,
      md5(coalesce(r.rate_code_name,'')) AS new_hash
    FROM mdm_rate_code_record_v2 r
    WHERE r.batch_id = p_batch_id
      AND r.status = 'APPROVED'
  ) incoming
  WHERE g.rate_code_id = incoming.rate_code_id
    AND g.is_current = TRUE
    AND g.record_hash <> incoming.new_hash;

  -- Insert new current golden rows for:
  -- - new RateCodeIDs (no current row)
  -- - changed RateCodeIDs (hash differs from current)
  INSERT INTO mdm_rate_code_golden_v2 (
    rate_code_id, rate_code_name,
    record_hash,
    effective_from, effective_to, is_current,
    source_batch_id
  )
  SELECT
    r.rate_code_id,
    r.rate_code_name,
    md5(coalesce(r.rate_code_name,'')) AS record_hash,
    now(),
    NULL,
    TRUE,
    p_batch_id
  FROM mdm_rate_code_record_v2 r
  LEFT JOIN mdm_rate_code_golden_v2 g
    ON g.rate_code_id = r.rate_code_id AND g.is_current = TRUE
  WHERE r.batch_id = p_batch_id
    AND r.status = 'APPROVED'
    AND (
      g.rate_code_id IS NULL OR
      g.record_hash <> md5(coalesce(r.rate_code_name,''))
    );

END;
$$;

-- Example call:
-- CALL sp_publish_rate_code_golden_v2('my batch id here');


-- ----------------------------
-- Current golden snapshot view
-- ----------------------------
CREATE OR REPLACE VIEW v_mdm_rate_code_golden_current_v2 AS
SELECT
  rate_code_id,
  rate_code_name,
  effective_from,
  effective_to,
  is_current,
  source_batch_id
FROM mdm_rate_code_golden_v2
WHERE is_current = TRUE;


-- ----------------------------
-- All golden history + audit view (like your screenshot output)
-- ----------------------------
CREATE OR REPLACE VIEW v_mdm_rate_code_golden_all_v2 AS
SELECT
  golden_sk,
  rate_code_id,
  rate_code_name,
  effective_from,
  effective_to,
  is_current,
  source_batch_id,
  created_at
FROM mdm_rate_code_golden_v2;


-- ----------------------------
-- Seed example: direct insert into record table + publish
-- ----------------------------
INSERT INTO mdm_rate_code_record_v2 (batch_id, rate_code_id, rate_code_name, status, source_file)
VALUES
  ('batch_20260121_01',  1, 'Standard rate',         'APPROVED', 'manual_seed'),
  ('batch_20260121_01',  2, 'JFK',                   'APPROVED', 'manual_seed'),
  ('batch_20260121_01',  3, 'Newark',                'APPROVED', 'manual_seed'),
  ('batch_20260121_01',  4, 'Nassau or Westchester', 'APPROVED', 'manual_seed'),
  ('batch_20260121_01',  5, 'Negotiated fare',       'APPROVED', 'manual_seed'),
  ('batch_20260121_01',  6, 'Group ride',            'APPROVED', 'manual_seed'),
  ('batch_20260121_01', 99, 'Unknown / Null',        'APPROVED', 'manual_seed')
ON CONFLICT (batch_id, rate_code_id) DO UPDATE
SET rate_code_name = EXCLUDED.rate_code_name,
    status         = EXCLUDED.status,
    source_file    = EXCLUDED.source_file,
    updated_at     = now();

CALL sp_publish_rate_code_golden_v2('batch_20260121_01');
