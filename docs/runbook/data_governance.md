# Data Governance & Stewardship

## 1. Purpose

This document describes how data governance, stewardship, and quality control are implemented in the **NYC Taxi Governed Data Platform**.

The goal is to ensure:

- Trusted master data (golden records)
- Controlled approvals before data is used downstream
- Traceability and auditability of all data changes
- Separation between raw, validated, and curated data
- Clear accountability between automated pipelines and human stewards

---

## 2. Governance Operating Model

This platform follows a **hybrid governance model**:

| Layer | Responsibility | Control Type |
|------|---------------|--------------|
| Raw | Ingest data as-is | Automated |
| Validated | Apply quality & schema rules | Automated |
| Master Data (RDS) | Maintain golden records | Steward-managed |
| Curated | Business-ready analytics data | Automated |
| Audit | Capture pipeline decisions | Automated |

Human approval is required **only for master data**, while transactional data is governed automatically.

---

## 3. Master Data Stewardship

### 3.1 Master Domains

The following domains are governed as master data:

| Domain | Source | Storage | Stewardship |
|------|------|--------|-------------|
| Zones | CSV upload | RDS | Manual approval |
| Vendors | Predefined | RDS | Manual |
| Rate Codes | Predefined | RDS | Manual |

### 3.2 Master Data Flow

1. A master data file is uploaded to S3 (`raw/master/`)
2. A Step Function triggers a Glue job to load data into RDS
3. Matching rules are applied to detect duplicates or conflicts
4. A steward is notified via SNS
5. Steward reviews and approves changes
6. Approved records become **golden records**
7. Steward manually triggers a Glue job to publish a **snapshot** into:
   - `validated/master_snapshot/zonesnapshots/`
   - `validated/vendor_snapshot/`
   - `validated/ratecode_snapshot/`

Only snapshot data is allowed to enter downstream pipelines.

---

## 4. Survivorship Rules (Golden Record Logic)

When multiple records exist for the same business key, survivorship rules are applied.

### 4.1 Zone Master Survivorship

**Business Key:** `location_id`

| Rule | Description |
|-----|------------|
| Latest Approved Wins | Most recent steward-approved record is selected |
| Completeness Priority | Records with non-null `borough` and `zone` are preferred |
| Manual Override | Steward can manually correct values in RDS |

### 4.2 Vendor Master Survivorship

**Business Key:** `vendor_id`

| Rule | Description |
|-----|------------|
| Source Priority | Steward-managed table always overrides raw input |
| Stable Attributes | Vendor name is treated as authoritative once approved |

---

## 5. Data Quality Rules

### 5.1 Raw → Validated (Glue Job 1)

| Rule | Action |
|------|-------|
| Null `PULocationID` or `DOLocationID` | Send to quarantine |
| Invalid data types | Send to quarantine |
| Valid records | Written to validated layer |

Rejected records are stored in:

- `validated/quarantine/`

### 5.2 Validated → Curated (Glue Job 2)

| Rule | Action |
|------|-------|
| Missing zone joins | Allowed (left join) |
| Join coverage below threshold | Pipeline fails |
| Metrics generated | Written to audit/metrics |

---

## 6. Audit & Lineage

Every pipeline execution is logged in DynamoDB:

- Run start
- Approval decisions
- Quality results
- Final status (SUCCESS / FAILED)

Metrics are stored in:

- `audit/metrics/_METRICS.json`

This enables full traceability of every dataset used for analytics.

---

## 7. Governance Metrics (CloudWatch)

The platform publishes governance KPIs to CloudWatch:

- Total rows processed
- Pickup zone completeness rate
- Dropoff zone completeness rate
- Approved vs rejected runs
- Snapshot freshness

These metrics support governance dashboards and operational monitoring.

---

## 8. Access Control

| Resource | Access |
|----------|-------|
| Raw data | Read-only |
| Validated data | Pipeline-only |
| Curated data | Analytics |
| Master data | Steward-only |
| Audit data | Platform admin |

IAM roles enforce least privilege access across all layers.

---

## 9. Governance Summary

This platform ensures that:

- No data reaches analytics without approval
- All master data is steward-controlled
- All transformations are auditable
- Quality rules are enforced consistently
- Downstream users consume only curated, trusted data

