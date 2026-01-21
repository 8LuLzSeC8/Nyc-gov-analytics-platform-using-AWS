# NYC Taxi Governed Data Platform — Overview

This repository contains a governed data pipeline for NYC Taxi trips with:
- master data stewardship (approval + golden records)
- a controlled ETL pipeline (raw → validated → curated)
- downstream analytics (RDS/Redshift + QuickSight dashboard)

The platform is designed to show how governance fits into an end-to-end data product, not just how to move data.

---

## What problem this solves

Trip data becomes unreliable if:
- master/reference data changes without review
- pipelines process stale reference snapshots
- bad records are not isolated
- downstream reporting does not have a clear “single source of truth”

This project adds governance controls so that:
- master changes are reviewed
- golden records are generated after approval
- downstream processing uses only approved snapshots
- data quality checks are recorded and visible

---

## High-level flow (two pipelines + one downstream model)

### 1) Master Data Management (MDM) pipeline (Zone master)
This pipeline runs when a **Zone master CSV** is created or uploaded.

**Trigger**
- Upload/new file for Zone master data

**Processing**
- A Step Function starts a Glue job that loads Zone CSV into **RDS**
- Matching/comparison rules run to detect duplicates / mismatches (stewardship checks)
- An SNS notification is sent to the **data steward** confirming the upload + any rule results

**Steward review + approval**
- Steward reviews the RDS tables
- Approval flow runs
- Approved records are used to generate **golden records**

> Note: For demo simplicity, other master datasets (Vendor, RateCode) are created directly in RDS since they are small and stable.

---

### 2) Golden snapshot publishing (steward-controlled)
After approval and golden record creation:

**Steward action**
- Steward manually runs a Glue job that exports the golden records to S3 as a “snapshot”

**Output**
- Golden snapshots are written into the **validated** layer (master snapshot location)

This snapshot is what the main pipeline uses for enrichment and governance validation.

---

### 3) Main governed pipeline (Trips pipeline)
This is the main Step Functions workflow that processes trip data end-to-end.

**Key idea**
- The pipeline only proceeds using the latest approved master snapshot in `validated/`

**Stages**
- Freshness check (ensure the master snapshot is recent)
- Approval gate (Approve/Reject via email link)
- Glue Job 1: Raw trips → Validated trips (casting + basic validation + quarantine bad rows)
- Glue Job 2: Validated trips + Master snapshot → Curated trips (enrichment)
- DQ validation (join success rates, thresholds)
- Audit logging + notifications

Outputs:
- `validated/trips_validated/run_id=.../`
- `validated/quarantine/run_id=.../`
- `curated/trips_enriched/run_id=.../`
- `audit/metrics/...` (used for governance visibility)

---

## Downstream analytics flow (Curated → Reporting)
Once curated data is produced:

1. **Crawlers** run on:
   - master snapshots
   - curated trips
   to capture schemas.

2. Data is loaded into **staging**, then into reporting tables:
   - load to staging
   - load master tables
   - derive dimension tables from master
   - populate fact table from staging while joining to dimensions

3. **QuickSight dashboard**
   - Redshift is added as a data source
   - A custom SQL dataset is created
   - Dashboard is built for reporting and governance storytelling

---

## Repository structure (what to look at)

- `infra/terraform/`
  - Terraform IaC for all resources (S3, IAM, Step Functions, Glue, Lambda, SNS, etc.)
- `infra/terraform/envs/dev/`
  - main environment deployment
- `infra/terraform/bootstrap/`
  - Terraform backend bootstrap (remote state)
- `src/`
  - Glue and Lambda code used in the pipelines
- `sql/`
  - SQL scripts used for RDS/Redshift staging + modeling
- `docs/`
  - Architecture, governance rules, and runbooks

---
