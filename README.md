# cdc-data-governance-aws

CDC-based incremental ingestion using AWS DMS with a full data governance framework — automated data quality monitoring, Glue Data Catalog lineage, KMS encryption, and IAM role-based access control. Built from real work on the AT&T data pipeline at HGS (2020–2021).

## Architecture

```
Source Databases (SQL Server, Oracle, MySQL)
         │  (Change Data Capture)
         ▼
    AWS DMS Replication Task
    (INSERT / UPDATE / DELETE → S3)
         │
         ▼
   Amazon S3 (CDC landing zone)
   [KMS encrypted, bucket policies enforced]
         │
         ▼
   AWS Glue ETL + Data Catalog
   (schema versioning, lineage, metadata)
         │
         ▼
   Amazon Redshift (star/snowflake schema)
   [column-level security, IAM roles]
         │
         ▼
   Data Quality Monitor (SNS alerts)
   row counts · nulls · duplicates · schema drift
```

## Stack

| Component | Technology |
|-----------|-----------|
| CDC Engine | AWS DMS |
| Data Catalog | AWS Glue Data Catalog |
| Processing | AWS Glue ETL |
| Warehouse | Amazon Redshift |
| Alerting | Amazon SNS |
| Encryption | AWS KMS |
| Access Control | AWS IAM |
| Storage | Amazon S3 |

## Files

```
cdc-data-governance-aws/
├── cdc_governance.py       # CDC task management, DQ framework, catalog, KMS, IAM
└── README.md
```

## Key Features

### CDC Ingestion
- Real-time change capture (INSERT / UPDATE / DELETE) from source DBs via AWS DMS
- Minimal latency — sub-minute delivery to Redshift
- Resume-capable: task restarts from last committed position

### Data Quality Framework (`DataQualityMonitor`)
| Check | Description |
|-------|-------------|
| Row count | Ensures table above expected minimum |
| Null validation | Alerts if null % exceeds threshold (default 5%) |
| Duplicate detection | Flags duplicate key combinations |
| Schema drift | Compares actual vs expected columns |

All failures trigger SNS alerts automatically.

### Data Governance
- **Glue Data Catalog** — centralized metadata, schema versioning, table lineage across S3/Redshift/Athena
- **KMS CMK encryption** — end-to-end encryption for S3 and Redshift data at rest
- **IAM role-based access** — least-privilege roles per service (Glue, DMS, Redshift)
- **Redshift column-level security** — restricts PII columns per user group

### Schema Design
Star and snowflake schemas designed with BI team — **40% query time reduction** vs flat table designs.

## Setup

1. Create DMS replication instance + source/target endpoints
2. Configure CDC task with appropriate start type (`resume-processing` for incremental)
3. Deploy Glue catalog with table definitions
4. Set up SNS topic and subscribe team emails
5. Apply KMS key to S3 bucket and Redshift cluster

## Based On

Real work from **AT&T via HGS** (Sep 2020 – Sep 2021): CDC pipeline keeping Redshift in sync with source systems, with automated governance across schema changes, data quality, and access control.
