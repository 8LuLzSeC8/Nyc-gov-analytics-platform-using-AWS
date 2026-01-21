# Data Lake Layout & Layer Responsibilities

This platform follows a layered S3 data lake architecture to clearly separate
raw data, validated data, curated data, and governance artifacts.

Each layer has a defined purpose, ownership, and quality expectation.

---

## Raw Layer (Landing Zone)

Purpose:  
Stores data exactly as it arrives. No transformations are applied.

This layer is append-only and acts as the immutable source of truth.

Locations:
- raw/master/csv/  
  Incoming master data files (zone master uploads)
- raw/trips/  
  Raw NYC taxi trip parquet files

Rules:
- No deletes or updates
- Schema may vary
- Data quality is not enforced here

---

## Validated Layer (Approved & Controlled)

Purpose:  
Contains data that has passed validation and stewardship approval.

This is the first layer allowed to feed downstream systems.

Locations:
- validated/master_snapshot/zonesnapshots/  
  Golden records created after steward approval
- validated/vendor_snapshot/  
  Vendor reference data snapshots
- validated/ratecode_snapshot/  
  Rate code reference data snapshots
- validated/trips_validated/  
  Cleaned trip data produced by Glue Job 1
- validated/quarantine/  
  Rejected records failing validation rules

Rules:
- Only approved or validated data is stored here
- Quarantine data is retained for audit and debugging
- Master snapshots must be fresh for pipeline execution

---

## Curated Layer (Analytics-Ready)

Purpose:  
Contains fully enriched, analytics-ready datasets.

Locations:
- curated/trips_enriched/  
  Trip data enriched with pickup and dropoff master data

Rules:
- Schema is stable
- Data is trusted and governed
- No run identifiers are exposed to downstream users

---

## Audit & Metrics Layer

Purpose:  
Stores governance evidence, data quality metrics, and pipeline execution results.

Locations:
- audit/metrics/  
  JSON files with freshness, quality, and enrichment metrics

Used for:
- Technical monitoring dashboards
- Governance and compliance reporting
- Steward review and traceability

---

## Manifests Layer

Purpose:  
Stores pipeline execution metadata and run manifests.

Locations:
- manifests/  
  Manifest files describing pipeline runs and outputs

Used for:
- Traceability
- Debugging
- Reproducibility

---

## Layer Ownership Summary

Raw layer is owned by the system and ingestion process.  
Validated layer is owned by data stewards and governance rules.  
Curated layer is owned by analytics and downstream consumers.  
Audit and manifests are owned by the platform for compliance and monitoring.

This separation ensures data governance is enforced by design, not by convention.
