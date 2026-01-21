-- =============================================================================
-- Author: Data Engineering Team
-- Owner: Analytics Engineering
-- Purpose:
--   - Create a conformed Vendor dimension from final_mdm.vendor_master
--   - Expose the latest (current) vendor attributes for analytics joins
-- Dependencies:
--   - final_mdm.vendor_master
-- Quality expectations:
--   - One row per vendor_id in the dimension (current record only)
--   - No duplicate vendor_id
--   - Script is idempotent (rebuild via truncate+insert)
-- Change Log:
--   - 2026-01-21: Initial version
-- =============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS final_dim.vendor_dim (
  vendor_sk      BIGINT IDENTITY(1,1),
  vendor_id      INT NOT NULL,
  vendor_name    VARCHAR(256),

  mdm_golden_sk  BIGINT,
  snapshot_id    VARCHAR(256),
  snapshot_ts    TIMESTAMP,

  created_at     TIMESTAMP DEFAULT GETDATE(),
  updated_at     TIMESTAMP DEFAULT GETDATE(),

  CONSTRAINT pk_vendor_dim PRIMARY KEY (vendor_sk)
)
DISTSTYLE ALL
SORTKEY (vendor_id);

-- Rebuild dimension deterministically
TRUNCATE TABLE final_dim.vendor_dim;

INSERT INTO final_dim.vendor_dim (
  vendor_id,
  vendor_name,
  mdm_golden_sk,
  snapshot_id,
  snapshot_ts
)
SELECT
  vm.vendor_id,
  vm.vendor_name,
  vm.golden_sk,
  vm.snapshot_id,
  vm.snapshot_ts
FROM final_mdm.vendor_master vm
WHERE vm.is_current = TRUE;

COMMIT;
