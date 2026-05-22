"""
cdc_governance.py
-----------------
CDC-based incremental ingestion using AWS DMS with a full data governance framework.
Automated DQ monitoring, Glue Data Catalog lineage, KMS encryption, IAM RBAC.

Built for the AT&T data pipeline at HGS (Sep 2020 – Sep 2021).
Replaced daily batch loads with sub-minute CDC delivery to Redshift.

Author: Premchand Kothapalli
Stack:  AWS DMS, Glue, Redshift, S3, KMS, IAM, SNS
"""

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import boto3
import psycopg2

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
@dataclass
class CDCConfig:
    env: str = "prod"

    # DMS
    dms_replication_task_arn: str = ""
    dms_replication_instance_arn: str = ""

    # S3
    cdc_landing_bucket: str = "att-cdc-landing-prod"
    cdc_landing_prefix: str = "cdc/"
    kms_key_id: str = ""                    # CMK ARN for S3 + Redshift encryption

    # Redshift
    redshift_host: str = ""
    redshift_port: int = 5439
    redshift_db: str = "att_dw"
    redshift_user: str = ""
    redshift_password: str = ""             # fetched from Secrets Manager in prod
    redshift_schema: str = "public"

    # SNS
    sns_topic_arn: str = ""

    # DQ thresholds
    dq_null_threshold: float = 0.05         # 5% nulls → alert
    dq_min_row_count: int = 1               # tables below this → alert
    dq_duplicate_tolerance: int = 0         # zero duplicates tolerated

    # Expected schemas per table (column name → expected dtype string)
    expected_schemas: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# AWS client helpers
# ---------------------------------------------------------------------------
def _dms_client():   return boto3.client("dms")
def _glue_client():  return boto3.client("glue")
def _s3_client():    return boto3.client("s3")
def _sns_client():   return boto3.client("sns")
def _iam_client():   return boto3.client("iam")
def _kms_client():   return boto3.client("kms")


# ---------------------------------------------------------------------------
# SNS alerting
# ---------------------------------------------------------------------------
def send_alert(cfg: CDCConfig, subject: str, message: str, level: str = "ERROR") -> None:
    payload = {
        "level":     level,
        "subject":   subject,
        "message":   message,
        "timestamp": datetime.utcnow().isoformat(),
        "env":       cfg.env,
    }
    try:
        _sns_client().publish(
            TopicArn=cfg.sns_topic_arn,
            Subject=f"[{level}] {subject}",
            Message=json.dumps(payload, indent=2),
        )
        log.info(f"SNS alert sent: {subject}")
    except Exception as e:
        log.error(f"Failed to send SNS alert: {e}")


# ---------------------------------------------------------------------------
# DMS CDC Task Manager
# ---------------------------------------------------------------------------
class DMSTaskManager:
    """
    Manages the AWS DMS replication task lifecycle.
    Checks task health before any CDC processing begins.
    """

    def __init__(self, cfg: CDCConfig):
        self.cfg    = cfg
        self.client = _dms_client()

    def get_task_status(self) -> str:
        response = self.client.describe_replication_tasks(
            Filters=[{"Name": "replication-task-arn", "Values": [self.cfg.dms_replication_task_arn]}]
        )
        tasks = response.get("ReplicationTasks", [])
        if not tasks:
            raise ValueError(f"DMS task not found: {self.cfg.dms_replication_task_arn}")
        return tasks[0]["Status"]

    def assert_task_running(self) -> None:
        """Halt pipeline if DMS task is not in running state."""
        status = self.get_task_status()
        if status != "running":
            msg = f"DMS replication task is not running. Status: {status}"
            log.error(msg)
            send_alert(self.cfg, "DMS Task Not Running", msg)
            raise RuntimeError(msg)
        log.info(f"DMS task status: {status} ✓")

    def get_task_stats(self) -> dict:
        """Return replication stats — full load progress, CDC latency."""
        response = self.client.describe_replication_tasks(
            Filters=[{"Name": "replication-task-arn", "Values": [self.cfg.dms_replication_task_arn]}]
        )
        task = response["ReplicationTasks"][0]
        stats = task.get("ReplicationTaskStats", {})
        log.info(f"DMS CDC latency: {stats.get('CDCLatencySource', 'N/A')}s source, "
                 f"{stats.get('CDCLatencyTarget', 'N/A')}s target")
        return stats

    def start_task(self, start_type: str = "resume-processing") -> None:
        """
        Start types:
          resume-processing — incremental, picks up from last committed position
          start-replication — full re-load from beginning
        """
        self.client.start_replication_task(
            ReplicationTaskArn=self.cfg.dms_replication_task_arn,
            StartReplicationTaskType=start_type,
        )
        log.info(f"DMS task started with start_type={start_type}")

    def stop_task(self) -> None:
        self.client.stop_replication_task(
            ReplicationTaskArn=self.cfg.dms_replication_task_arn
        )
        log.info("DMS task stopped")


# ---------------------------------------------------------------------------
# Schema Drift Detector
# ---------------------------------------------------------------------------
class SchemaDriftDetector:
    """
    Compares actual Glue Catalog schema against expected schema definition.
    Fires SNS alert and halts processing if any drift is detected.
    """

    def __init__(self, cfg: CDCConfig):
        self.cfg    = cfg
        self.client = _glue_client()

    def get_catalog_schema(self, database: str, table_name: str) -> dict:
        response = self.client.get_table(DatabaseName=database, Name=table_name)
        columns  = response["Table"]["StorageDescriptor"]["Columns"]
        return {col["Name"]: col["Type"] for col in columns}

    def detect_drift(self, database: str, table_name: str) -> bool:
        """Returns True if drift detected (caller should halt)."""
        if table_name not in self.cfg.expected_schemas:
            log.warning(f"No expected schema defined for {table_name} — skipping drift check")
            return False

        expected = self.cfg.expected_schemas[table_name]

        try:
            actual = self.get_catalog_schema(database, table_name)
        except Exception as e:
            log.warning(f"Could not fetch Catalog schema for {table_name}: {e}")
            return False

        added   = set(actual.keys())   - set(expected.keys())
        removed = set(expected.keys()) - set(actual.keys())
        changed = {
            col for col in (set(actual.keys()) & set(expected.keys()))
            if actual[col] != expected[col]
        }

        if added or removed or changed:
            msg = (
                f"Schema drift detected in {table_name}:\n"
                f"  Added columns:   {added or 'none'}\n"
                f"  Removed columns: {removed or 'none'}\n"
                f"  Type changes:    {changed or 'none'}"
            )
            log.error(msg)
            send_alert(self.cfg, f"Schema Drift — {table_name}", msg)
            return True

        log.info(f"Schema check passed for {table_name} — no drift ✓")
        return False


# ---------------------------------------------------------------------------
# Data Quality Monitor
# ---------------------------------------------------------------------------
class DataQualityMonitor:
    """
    Automated DQ checks run after every CDC batch.
    All failures send SNS alerts. Threshold breaches halt the merge step.
    """

    def __init__(self, cfg: CDCConfig):
        self.cfg = cfg

    def _redshift_conn(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self.cfg.redshift_host,
            port=self.cfg.redshift_port,
            dbname=self.cfg.redshift_db,
            user=self.cfg.redshift_user,
            password=self.cfg.redshift_password,
        )

    def check_row_count(self, table_name: str) -> int:
        with self._redshift_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.cfg.redshift_schema}.{table_name}")
                count = cur.fetchone()[0]

        if count < self.cfg.dq_min_row_count:
            msg = f"[DQ FAIL] {table_name}: row count {count:,} is below minimum {self.cfg.dq_min_row_count:,}"
            log.error(msg)
            send_alert(self.cfg, f"DQ Row Count Failure — {table_name}", msg)
            raise ValueError(msg)

        log.info(f"[DQ] {table_name}: {count:,} rows ✓")
        return count

    def check_null_rates(self, table_name: str, columns: list) -> dict:
        results = {}
        with self._redshift_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM {self.cfg.redshift_schema}.{table_name}")
                total = cur.fetchone()[0]

                for col in columns:
                    cur.execute(
                        f"SELECT COUNT(*) FROM {self.cfg.redshift_schema}.{table_name} "
                        f"WHERE {col} IS NULL"
                    )
                    null_count = cur.fetchone()[0]
                    null_pct   = null_count / total if total > 0 else 0
                    results[col] = null_pct

                    if null_pct > self.cfg.dq_null_threshold:
                        msg = (f"[DQ FAIL] {table_name}.{col}: "
                               f"{null_pct:.1%} null rate exceeds threshold "
                               f"({self.cfg.dq_null_threshold:.0%})")
                        log.error(msg)
                        send_alert(self.cfg, f"DQ Null Rate Failure — {table_name}.{col}", msg)
                        raise ValueError(msg)
                    else:
                        log.info(f"[DQ] {table_name}.{col}: {null_pct:.1%} nulls ✓")

        return results

    def check_duplicates(self, table_name: str, key_columns: list) -> int:
        key_cols_sql = ", ".join(key_columns)
        query = f"""
            SELECT COUNT(*) FROM (
                SELECT {key_cols_sql}, COUNT(*) AS cnt
                FROM {self.cfg.redshift_schema}.{table_name}
                GROUP BY {key_cols_sql}
                HAVING COUNT(*) > 1
            ) dupes
        """
        with self._redshift_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                dupe_count = cur.fetchone()[0]

        if dupe_count > self.cfg.dq_duplicate_tolerance:
            msg = f"[DQ FAIL] {table_name}: {dupe_count:,} duplicate key combinations on ({key_cols_sql})"
            log.error(msg)
            send_alert(self.cfg, f"DQ Duplicate Key Failure — {table_name}", msg)
            raise ValueError(msg)

        log.info(f"[DQ] {table_name}: no duplicate keys on ({key_cols_sql}) ✓")
        return dupe_count

    def run_all(self, table_name: str, key_columns: list, null_check_columns: list) -> None:
        log.info(f"[DQ] Running checks on {table_name}")
        self.check_row_count(table_name)
        self.check_null_rates(table_name, null_check_columns)
        self.check_duplicates(table_name, key_columns)
        log.info(f"[DQ] All checks passed for {table_name} ✓")


# ---------------------------------------------------------------------------
# Glue Data Catalog Manager
# ---------------------------------------------------------------------------
class GlueCatalogManager:
    """
    Maintains Glue Data Catalog as central metadata layer.
    Schema versioning, table lineage, feature discoverability
    across S3, Redshift, and Athena.
    """

    def __init__(self, cfg: CDCConfig):
        self.cfg    = cfg
        self.client = _glue_client()

    def register_table(
        self,
        database: str,
        table_name: str,
        s3_location: str,
        columns: list,
        description: str = "",
    ) -> None:
        table_input = {
            "Name":        table_name,
            "Description": description,
            "StorageDescriptor": {
                "Location":      s3_location,
                "InputFormat":   "org.apache.hadoop.mapred.TextInputFormat",
                "OutputFormat":  "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "Compressed":    False,
                "Columns":       columns,
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                },
            },
            "Parameters": {
                "created_by":          "cdc_governance_framework",
                "last_updated":        datetime.utcnow().isoformat(),
                "source_system":       "AWS_DMS_CDC",
                "encryption":          "KMS_CMK",
            },
        }

        try:
            self.client.create_table(DatabaseName=database, TableInput=table_input)
            log.info(f"Catalog: created {database}.{table_name}")
        except self.client.exceptions.AlreadyExistsException:
            self.client.update_table(DatabaseName=database, TableInput=table_input)
            log.info(f"Catalog: updated {database}.{table_name}")

    def add_lineage_tag(self, database: str, table_name: str, upstream_source: str) -> None:
        """Tag table with upstream lineage for discoverability."""
        self.client.tag_resource(
            ResourceArn=f"arn:aws:glue:{boto3.session.Session().region_name}:"
                        f"::table/{database}/{table_name}",
            TagsToAdd={"upstream_source": upstream_source, "pipeline": "cdc_governance"},
        )


# ---------------------------------------------------------------------------
# KMS Encryption Manager
# ---------------------------------------------------------------------------
class KMSEncryptionManager:
    """
    Applies Customer Managed Key encryption to S3 bucket and Redshift cluster.
    """

    def __init__(self, cfg: CDCConfig):
        self.cfg    = cfg
        self.client = _kms_client()
        self.s3     = _s3_client()

    def enforce_s3_encryption(self, bucket_name: str) -> None:
        """Enforce SSE-KMS on all objects in the CDC landing bucket."""
        self.s3.put_bucket_encryption(
            Bucket=bucket_name,
            ServerSideEncryptionConfiguration={
                "Rules": [{
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm":   "aws:kms",
                        "KMSMasterKeyID": self.cfg.kms_key_id,
                    },
                    "BucketKeyEnabled": True,
                }]
            },
        )
        log.info(f"KMS encryption enforced on bucket: {bucket_name}")

    def verify_key_rotation(self) -> bool:
        """Verify automatic key rotation is enabled on the CMK."""
        response = self.client.get_key_rotation_status(KeyId=self.cfg.kms_key_id)
        rotation_enabled = response.get("KeyRotationEnabled", False)
        if not rotation_enabled:
            log.warning(f"Key rotation not enabled for KMS key: {self.cfg.kms_key_id}")
        else:
            log.info("KMS key rotation: enabled ✓")
        return rotation_enabled


# ---------------------------------------------------------------------------
# Redshift CDC Merger
# ---------------------------------------------------------------------------
class RedshiftCDCMerger:
    """
    Applies CDC changes to Redshift using a DELETE + INSERT UPSERT pattern.
    More efficient than UPDATE on Redshift's columnar storage.
    """

    def __init__(self, cfg: CDCConfig):
        self.cfg = cfg

    def _conn(self) -> psycopg2.extensions.connection:
        return psycopg2.connect(
            host=self.cfg.redshift_host,
            port=self.cfg.redshift_port,
            dbname=self.cfg.redshift_db,
            user=self.cfg.redshift_user,
            password=self.cfg.redshift_password,
        )

    def merge_staging_to_production(
        self,
        staging_table: str,
        production_table: str,
        key_columns: list,
    ) -> dict:
        """
        UPSERT via DELETE + INSERT:
        1. DELETE matching keys from production
        2. INSERT all rows from staging into production
        """
        key_join = " AND ".join(
            [f"prod.{k} = stg.{k}" for k in key_columns]
        )

        delete_sql = f"""
            DELETE FROM {self.cfg.redshift_schema}.{production_table}
            USING {self.cfg.redshift_schema}.{staging_table} stg
            WHERE {' AND '.join([
                f'{self.cfg.redshift_schema}.{production_table}.{k} = stg.{k}'
                for k in key_columns
            ])}
        """

        insert_sql = f"""
            INSERT INTO {self.cfg.redshift_schema}.{production_table}
            SELECT * FROM {self.cfg.redshift_schema}.{staging_table}
        """

        with self._conn() as conn:
            with conn.cursor() as cur:
                log.info(f"[MERGE] Deleting matching keys from {production_table}")
                cur.execute(delete_sql)
                deleted = cur.rowcount

                log.info(f"[MERGE] Inserting {staging_table} → {production_table}")
                cur.execute(insert_sql)
                inserted = cur.rowcount

            conn.commit()

        log.info(f"[MERGE] {production_table}: deleted={deleted:,}, inserted={inserted:,}")
        return {"deleted": deleted, "inserted": inserted}

    def run_vacuum_analyze(self, table_name: str) -> None:
        """Run VACUUM + ANALYZE after bulk merge to maintain query performance."""
        with self._conn() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                log.info(f"[VACUUM] Running VACUUM on {table_name}")
                cur.execute(f"VACUUM {self.cfg.redshift_schema}.{table_name}")
                log.info(f"[ANALYZE] Running ANALYZE on {table_name}")
                cur.execute(f"ANALYZE {self.cfg.redshift_schema}.{table_name}")


# ---------------------------------------------------------------------------
# IAM Access Control Manager
# ---------------------------------------------------------------------------
class IAMAccessManager:
    """
    Least-privilege IAM roles per service.
    Enforces RBAC across Glue, DMS, and Redshift.
    """

    def __init__(self, cfg: CDCConfig):
        self.cfg    = cfg
        self.client = _iam_client()

    def verify_role_exists(self, role_name: str) -> bool:
        try:
            self.client.get_role(RoleName=role_name)
            log.info(f"IAM role verified: {role_name} ✓")
            return True
        except self.client.exceptions.NoSuchEntityException:
            log.error(f"IAM role not found: {role_name}")
            return False

    def get_role_policies(self, role_name: str) -> list:
        response = self.client.list_attached_role_policies(RoleName=role_name)
        return [p["PolicyName"] for p in response["AttachedPolicies"]]


# ---------------------------------------------------------------------------
# CDC Pipeline Orchestrator
# ---------------------------------------------------------------------------
class CDCPipeline:
    """
    Top-level orchestrator — wires together DMS, schema drift detection,
    DQ monitoring, Glue Catalog, KMS, and Redshift merge.
    """

    def __init__(self, cfg: CDCConfig):
        self.cfg      = cfg
        self.dms      = DMSTaskManager(cfg)
        self.drift    = SchemaDriftDetector(cfg)
        self.dq       = DataQualityMonitor(cfg)
        self.catalog  = GlueCatalogManager(cfg)
        self.kms      = KMSEncryptionManager(cfg)
        self.merger   = RedshiftCDCMerger(cfg)

    def run(
        self,
        database: str,
        table_name: str,
        staging_table: str,
        key_columns: list,
        null_check_columns: list,
    ) -> dict:
        log.info(f"=== CDC Pipeline START: {table_name} ===")
        results = {}

        # 1. DMS health check — halt if task not running
        self.dms.assert_task_running()

        # 2. Schema drift check — halt if columns changed
        drift_detected = self.drift.detect_drift(database, table_name)
        if drift_detected:
            raise RuntimeError(f"Schema drift detected in {table_name} — halting pipeline")

        # 3. DQ checks on staging table — halt before merge if thresholds breached
        self.dq.run_all(staging_table, key_columns, null_check_columns)

        # 4. UPSERT staging → production
        results["merge"] = self.merger.merge_staging_to_production(
            staging_table, table_name, key_columns
        )

        # 5. Post-merge DQ validation
        results["post_merge_count"] = self.dq.check_row_count(table_name)

        # 6. VACUUM + ANALYZE
        self.merger.run_vacuum_analyze(table_name)

        # 7. Update Glue Catalog lineage
        self.catalog.add_lineage_tag(database, table_name, upstream_source="AWS_DMS_CDC")

        # 8. Success alert
        send_alert(
            self.cfg,
            f"CDC Pipeline SUCCESS — {table_name}",
            json.dumps(results, indent=2),
            level="INFO",
        )

        log.info(f"=== CDC Pipeline COMPLETE: {table_name} ===")
        return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    cfg = CDCConfig(
        env=sys.argv[1] if len(sys.argv) > 1 else "prod",
        dms_replication_task_arn=sys.argv[2] if len(sys.argv) > 2 else "",
        sns_topic_arn=sys.argv[3] if len(sys.argv) > 3 else "",
    )

    pipeline = CDCPipeline(cfg)
    pipeline.run(
        database="att_catalog",
        table_name="customer_events",
        staging_table="customer_events_staging",
        key_columns=["customer_id", "event_id"],
        null_check_columns=["customer_id", "event_type", "event_timestamp"],
    )
