-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Analytics Engineering
-- Purpose:
--   - Create a conformed Ratecode dimension from final_mdm.ratecode_master
--   - Expose the latest (current) ratecode attributes for analytics joins
-- Dependencies:
--   - final_mdm.ratecode_master
-- Quality expectations:
--   - One row per rate_code_id in the dimension (current record only)
--   - No duplicate rate_code_id
--   - Script is idempotent (rebuild via truncate+insert)
-- Change Log:
--   - 2026-01-21: Initial version
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS final_dim.ratecode_dim (
  ratecode_sk     BIGINT IDENTITY(1,1),
  rate_code_id    INT NOT NULL,
  rate_code_name  VARCHAR(256),

  mdm_golden_sk   BIGINT,
  snapshot_id     VARCHAR(256),
  snapshot_ts     TIMESTAMP,

  created_at      TIMESTAMP DEFAULT GETDATE(),
  updated_at      TIMESTAMP DEFAULT GETDATE(),

  CONSTRAINT pk_ratecode_dim PRIMARY KEY (ratecode_sk)
)
DISTSTYLE ALL
SORTKEY (rate_code_id);

-- Rebuild dimension deterministically
TRUNCATE TABLE final_dim.ratecode_dim;

INSERT INTO final_dim.ratecode_dim (
  rate_code_id,
  rate_code_name,
  mdm_golden_sk,
  snapshot_id,
  snapshot_ts
)
SELECT
  rm.rate_code_id,
  rm.rate_code_name,
  rm.golden_sk,
  rm.snapshot_id,
  rm.snapshot_ts
FROM final_mdm.ratecode_master rm
WHERE rm.is_current = TRUE;

COMMIT;
