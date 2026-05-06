"""
CDC-Based Incremental Ingestion & Data Governance Framework
AWS DMS + Redshift + Glue Data Catalog + SNS + KMS + IAM
Author: Premchand Kothapalli
"""

import boto3
import psycopg2
import logging
import json
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ─── AWS Clients ──────────────────────────────────────────────────────────────

dms_client   = boto3.client("dms",            region_name="us-east-1")
glue_client  = boto3.client("glue",           region_name="us-east-1")
sns_client   = boto3.client("sns",            region_name="us-east-1")
s3_client    = boto3.client("s3",             region_name="us-east-1")
iam_client   = boto3.client("iam",            region_name="us-east-1")
kms_client   = boto3.client("kms",            region_name="us-east-1")

SNS_TOPIC_ARN    = "arn:aws:sns:us-east-1:123456789:data-quality-alerts"
GLUE_DATABASE    = "healthcare_catalog"
S3_BUCKET        = "your-data-lake-bucket"
DMS_TASK_ARN     = "arn:aws:dms:us-east-1:123456789:task:your-task-id"
REDSHIFT_CONFIG  = {
    "host":     "your-cluster.us-east-1.redshift.amazonaws.com",
    "port":     5439,
    "dbname":   "healthdw",
    "user":     "admin",
    "password": "your_password",
}


# ─── AWS DMS: CDC Task Management ─────────────────────────────────────────────

def start_cdc_task(task_arn: str, start_type: str = "resume-processing"):
    """
    Start or resume a CDC DMS replication task.
    start_type options: 'start-replication', 'resume-processing', 'reload-target'
    """
    logger.info(f"[DMS] Starting CDC task: {task_arn} | mode={start_type}")
    response = dms_client.start_replication_task(
        ReplicationTaskArn=task_arn,
        StartReplicationTaskType=start_type,
    )
    status = response["ReplicationTask"]["Status"]
    logger.info(f"[DMS] Task status: {status}")
    return status


def get_cdc_task_status(task_arn: str) -> dict:
    """Get current CDC replication task status and stats."""
    response = dms_client.describe_replication_tasks(
        Filters=[{"Name": "replication-task-arn", "Values": [task_arn]}]
    )
    if not response["ReplicationTasks"]:
        raise ValueError(f"DMS task not found: {task_arn}")

    task = response["ReplicationTasks"][0]
    stats = task.get("ReplicationTaskStats", {})
    return {
        "status":          task["Status"],
        "full_load_rows":  stats.get("FullLoadRowsTransferred", 0),
        "cdc_rows":        stats.get("CdcRowsInserted", 0) + stats.get("CdcRowsUpdated", 0) + stats.get("CdcRowsDeleted", 0),
        "latency_seconds": stats.get("CdcLatencySource", 0),
    }


# ─── Data Quality Framework ────────────────────────────────────────────────────

class DataQualityMonitor:
    """
    Automated data quality monitoring with SNS alerting.
    Checks: row counts, nulls, duplicates, schema drift.
    AT&T CDC pipeline — built to catch issues before they reach BI layer.
    """

    def __init__(self, table_name: str, conn_config: dict):
        self.table_name = table_name
        self.conn_config = conn_config
        self.issues = []

    def _run_query(self, sql: str):
        conn = psycopg2.connect(**self.conn_config)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall()
        finally:
            conn.close()

    def check_row_count(self, expected_min: int = 1) -> bool:
        result = self._run_query(f"SELECT COUNT(*) FROM {self.table_name};")
        count = result[0][0]
        if count < expected_min:
            self.issues.append(f"Row count too low: {count} (expected >= {expected_min})")
            return False
        logger.info(f"[DQ] Row count OK: {self.table_name} = {count:,}")
        return True

    def check_null_columns(self, columns: list, threshold_pct: float = 5.0) -> bool:
        passed = True
        total = self._run_query(f"SELECT COUNT(*) FROM {self.table_name};")[0][0]
        for col in columns:
            null_count = self._run_query(f"SELECT COUNT(*) FROM {self.table_name} WHERE {col} IS NULL;")[0][0]
            pct = (null_count / total * 100) if total > 0 else 0
            if pct > threshold_pct:
                self.issues.append(f"Column '{col}' has {pct:.1f}% nulls (threshold: {threshold_pct}%)")
                passed = False
            else:
                logger.info(f"[DQ] Null check OK: {col} = {pct:.2f}%")
        return passed

    def check_duplicates(self, key_columns: list) -> bool:
        keys = ", ".join(key_columns)
        result = self._run_query(f"""
            SELECT COUNT(*) FROM (
                SELECT {keys}, COUNT(*) AS cnt
                FROM {self.table_name}
                GROUP BY {keys}
                HAVING COUNT(*) > 1
            ) dups;
        """)
        dup_count = result[0][0]
        if dup_count > 0:
            self.issues.append(f"Found {dup_count} duplicate key combinations: ({keys})")
            return False
        logger.info(f"[DQ] Duplicate check OK: {self.table_name}")
        return True

    def check_schema_drift(self, expected_columns: list) -> bool:
        result = self._run_query(f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = '{self.table_name.split('.')[-1]}'
            ORDER BY ordinal_position;
        """)
        actual_cols = [r[0] for r in result]
        missing = set(expected_columns) - set(actual_cols)
        extra   = set(actual_cols) - set(expected_columns)

        if missing:
            self.issues.append(f"Schema drift — missing columns: {missing}")
        if extra:
            logger.warning(f"[DQ] Extra columns detected (may be ok): {extra}")

        return len(missing) == 0

    def run_all_checks(self, key_columns: list, nullable_checks: list, expected_columns: list) -> bool:
        logger.info(f"[DQ] Running all checks for {self.table_name}")
        results = [
            self.check_row_count(),
            self.check_null_columns(nullable_checks),
            self.check_duplicates(key_columns),
            self.check_schema_drift(expected_columns),
        ]
        passed = all(results)
        if not passed:
            self._send_alert()
        return passed

    def _send_alert(self):
        message = f"Data Quality Issues — {self.table_name}\n\n" + "\n".join(f"• {i}" for i in self.issues)
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"[DQ ALERT] {self.table_name}",
            Message=message,
        )
        logger.warning(f"[SNS] DQ alert sent for {self.table_name}")


# ─── Glue Data Catalog ─────────────────────────────────────────────────────────

def register_table_in_catalog(table_name: str, s3_location: str, columns: list, partition_keys: list = None):
    """
    Register or update a table in AWS Glue Data Catalog.
    Enables full data lineage, schema versioning, and Athena access.
    """
    column_defs = [{"Name": c["name"], "Type": c["type"], "Comment": c.get("comment", "")} for c in columns]

    table_input = {
        "Name": table_name,
        "StorageDescriptor": {
            "Columns": column_defs,
            "Location": s3_location,
            "InputFormat":  "org.apache.hadoop.mapred.TextInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
        },
        "PartitionKeys": [{"Name": p, "Type": "string"} for p in (partition_keys or [])],
        "TableType": "EXTERNAL_TABLE",
    }

    try:
        glue_client.create_table(DatabaseName=GLUE_DATABASE, TableInput=table_input)
        logger.info(f"[Glue] Table created: {GLUE_DATABASE}.{table_name}")
    except glue_client.exceptions.AlreadyExistsException:
        glue_client.update_table(DatabaseName=GLUE_DATABASE, TableInput=table_input)
        logger.info(f"[Glue] Table updated: {GLUE_DATABASE}.{table_name}")


# ─── KMS Encryption ────────────────────────────────────────────────────────────

def create_data_encryption_key(alias: str, description: str) -> str:
    """Create a KMS CMK for encrypting S3 and Redshift data."""
    response = kms_client.create_key(
        Description=description,
        KeyUsage="ENCRYPT_DECRYPT",
        Origin="AWS_KMS",
        Tags=[{"TagKey": "Project", "TagValue": "data-governance"}],
    )
    key_id = response["KeyMetadata"]["KeyId"]
    kms_client.create_alias(AliasName=f"alias/{alias}", TargetKeyId=key_id)
    logger.info(f"[KMS] Created CMK: alias/{alias} → {key_id}")
    return key_id


# ─── IAM Role-Based Access ─────────────────────────────────────────────────────

def create_pipeline_role(role_name: str, trusted_service: str = "glue.amazonaws.com") -> str:
    """Create an IAM role with least-privilege access for pipeline execution."""
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": trusted_service},
            "Action": "sts:AssumeRole",
        }],
    }
    response = iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(trust_policy),
        Description=f"Pipeline role for {trusted_service}",
    )
    logger.info(f"[IAM] Created role: {role_name}")
    return response["Role"]["Arn"]


if __name__ == "__main__":
    # Example: Start CDC task and run DQ checks
    status = get_cdc_task_status(DMS_TASK_ARN)
    logger.info(f"[MAIN] CDC Task Status: {status}")

    monitor = DataQualityMonitor("public.claims_incremental", REDSHIFT_CONFIG)
    monitor.run_all_checks(
        key_columns=["claim_id"],
        nullable_checks=["member_id", "service_date", "billed_amount"],
        expected_columns=["claim_id", "member_id", "provider_id", "service_date", "billed_amount", "paid_amount"],
    )
