-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Analytics Engineering
-- Purpose:
--   - Create a conformed Zone dimension from final_mdm.zone_master
--   - Expose the latest (current) zone attributes for analytics joins
-- Dependencies:
--   - final_mdm.zone_master
-- Quality expectations:
--   - One row per location_id in the dimension (current record only)
--   - No duplicate location_id
--   - Script is idempotent (rebuild via create/replace view or truncate+insert)
-- Change Log:
--   - 2026-01-21: Initial version
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS final_dim.zone_dim (
  zone_sk        BIGINT IDENTITY(1,1),
  location_id    INT NOT NULL,
  borough        VARCHAR(256),
  zone           VARCHAR(256),
  service_zone   VARCHAR(256),

  mdm_golden_sk  BIGINT,
  snapshot_id    VARCHAR(256),
  snapshot_ts    TIMESTAMP,

  created_at     TIMESTAMP DEFAULT GETDATE(),
  updated_at     TIMESTAMP DEFAULT GETDATE(),

  CONSTRAINT pk_zone_dim PRIMARY KEY (zone_sk)
)
DISTSTYLE ALL
SORTKEY (location_id);

-- Rebuild dimension deterministically
TRUNCATE TABLE final_dim.zone_dim;

INSERT INTO final_dim.zone_dim (
  location_id,
  borough,
  zone,
  service_zone,
  mdm_golden_sk,
  snapshot_id,
  snapshot_ts
)
SELECT
  zm.location_id,
  zm.borough,
  zm.zone,
  zm.service_zone,
  zm.golden_sk,
  zm.snapshot_id,
  zm.snapshot_ts
FROM final_mdm.zone_master zm
WHERE zm.is_current = TRUE;

COMMIT;
