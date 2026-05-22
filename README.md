# cdc-data-governance-aws

CDC-based incremental ingestion using AWS DMS with a full data governance framework — automated data quality monitoring, Glue Data Catalog lineage, KMS encryption, and IAM role-based access control. Built from real work on the **AT&T data pipeline at HGS** (Sep 2020 – Sep 2021).

---

## Why This Exists

Batch ETL jobs were keeping Redshift 24 hours behind source systems. Downstream teams — both BI and classification model experiments — were consuming stale data without knowing it. This framework replaced batch with CDC, so Redshift stays in sync with sub-minute latency, and every change is governed, encrypted, and auditable from the moment it leaves the source.

---

## Architecture

```
Source Databases
(SQL Server, Oracle, MySQL)
         │
         │  Row-level CDC: INSERT / UPDATE / DELETE
         ▼
    AWS DMS Replication Task
    Minimal-latency change streaming → S3
         │
         ▼
   Amazon S3  (CDC Landing Zone)
   KMS Customer Managed Key encryption
   Bucket policies: least-privilege per service role
         │
         ▼
   AWS Glue ETL
   Applies changes to target schema
   Schema drift detection before every run
         │
         ▼
   AWS Glue Data Catalog
   Central metadata layer: schema versioning,
   table lineage, feature discoverability
   across S3 / Redshift / Athena
         │
         ▼
   Amazon Redshift
   Star / snowflake schema
   Column-level security on PII columns
   IAM RBAC — least-privilege per role
         │
         ▼
   DataQualityMonitor
   Automated checks after every CDC batch
   SNS alerts on any threshold breach
```

---

## Stack

| Component | Technology |
|---|---|
| CDC Engine | AWS DMS |
| Data Catalog | AWS Glue Data Catalog |
| Processing | AWS Glue ETL |
| Warehouse | Amazon Redshift |
| Alerting | Amazon SNS |
| Encryption | AWS KMS (Customer Managed Key) |
| Access Control | AWS IAM (RBAC) |
| Storage | Amazon S3 |

---

## Repository Structure

```
cdc-data-governance-aws/
├── cdc_governance.py        # CDC task management, DQ framework, catalog, KMS, IAM
└── README.md
```

---

## CDC Ingestion — `cdc_governance.py`

AWS DMS captures every row-level change (INSERT, UPDATE, DELETE) from source databases and streams them to S3 with sub-minute latency. Key design decisions:

- **Resume-capable** — task restarts from last committed position after a failure, no data loss, no re-processing from the beginning
- **Separate replication tasks per source** — isolates failures so an Oracle outage doesn't block the SQL Server stream
- **Full LOB mode** for large column support where needed, limited LOB elsewhere to keep throughput high

---

## DataQualityMonitor Class

Runs automatically after every CDC batch before data is promoted to production tables. Any failure fires an SNS alert immediately.

| Check | Description | Default Threshold |
|---|---|---|
| Row count | Validates table is above expected minimum | Configurable per table |
| Null validation | Flags columns exceeding null % threshold | 5% |
| Duplicate detection | Identifies duplicate primary key combinations | Zero tolerance |
| Schema drift | Compares actual columns against expected schema definition | Any deviation alerts |

Schema drift detection is particularly important here — source system DDL changes in CDC pipelines are silent killers. A column rename or type change in the source silently corrupts downstream data if undetected.

---

## Data Governance Layer

**Glue Data Catalog**
Central metadata store covering all tables across S3, Redshift, and Athena. Maintains schema version history so any schema change is recorded with timestamp and diff. Teams can discover, understand, and audit any table's lineage without asking the pipeline team.

**KMS Encryption**
Customer Managed Key (CMK) applied end-to-end:
- S3 CDC landing bucket — SSE-KMS on all objects
- Redshift cluster — encrypted at rest with the same CMK
- Key rotation policy enforced via KMS key policy

**IAM Role-Based Access Control**
Least-privilege roles per service:
- DMS replication role — S3 write only to CDC landing prefix
- Glue execution role — S3 read/write + Catalog read/write, no Redshift direct access
- Redshift load role — S3 read from staging prefix only

**Redshift Column-Level Security**
PII columns (SSN, DOB, address fields) restricted at the column level per user group. BI users see masked values; data engineers see full data only through audited roles.

---

## Schema Design

Star and snowflake schemas designed with the BI team — **40% query time reduction** vs flat table designs. Fact tables partitioned by `load_date` for efficient incremental queries.

---

## Setup

1. Create DMS replication instance + source and target endpoints
2. Configure CDC task — set `start-replication-from` to `latest-position` for incremental, `beginning` for initial full load
3. Deploy Glue catalog with table definitions and expected schema configs
4. Set up SNS topic, subscribe team distribution list
5. Apply KMS CMK to S3 bucket (default encryption) and Redshift cluster
6. Configure IAM roles per service with least-privilege policies

---

## Based On

Real work on the **AT&T data pipeline at HGS** — Sep 2020 to Sep 2021. Replaced daily batch loads with sub-minute CDC delivery to Redshift, with automated governance across schema changes, data quality, and access control for a regulated telecom data environment.

---

## Author

**Premchand Kothapalli**
Senior AI / ML Engineer | AWS · Azure AI Foundry · LangGraph · PySpark
[LinkedIn](https://linkedin.com/in/pc-kothapalli) · premchandkdata@gmail.com · [GitHub](https://github.com/premchand2001)
