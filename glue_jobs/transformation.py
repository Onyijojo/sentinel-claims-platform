"""
Glue Job — Transformation
S3 Raw Zone (CSV) → S3 Landing Zone (Parquet)

Job type: Glue Spark ETL (PySpark)
Input:    s3://sentinel-claims-data/raw/{entity}/{ingestion_date}/
Output:   s3://sentinel-claims-data/landing/{entity}/{ingestion_date}/ (Parquet)

Key behaviours:
  - Schema evolution is detected for every entity on every run.
    Changes are logged to S3 (audit trail) and written as a small Parquet
    file to landing/schema_evolution/ so loading.py can COPY them into
    warehouse.schema_evolution_log for historical querying.
  - Claims v1 and v2 are unified into a single dataset.
    v1 rows get claim_severity = 'unknown' to align with v2's schema.
  - Rows with NULL policy_id are dropped from claims —
    unlinked claims cannot be joined to the dimensional model.
  - All other data quality rules are applied (NULL fills, casing, type casts).

Run locally (requires PySpark):  spark-submit glue_jobs/transformation.py
Deploy to Glue:                   Upload to S3, create a Glue Spark ETL job
"""
import json
import logging
import os
import sys
from datetime import datetime

import boto3
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    _IN_GLUE = True
except ImportError:
    _IN_GLUE = False

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("sentinel.transformation")

# ── Job initialisation ──────────────────────────────────────────────────────

if _IN_GLUE:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)
    if "--ingestion_date" in sys.argv:
        INGESTION_DATE = sys.argv[sys.argv.index("--ingestion_date") + 1]
    else:
        INGESTION_DATE = datetime.now().strftime("%Y-%m-%d")
else:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("sentinel_transformation").getOrCreate()
    INGESTION_DATE = os.environ.get("INGESTION_DATE", datetime.now().strftime("%Y-%m-%d"))

BUCKET = "sentinel-claims-data"
RAW = f"s3://{BUCKET}/raw"
LANDING = f"s3://{BUCKET}/landing"
REGISTRY_PREFIX = "schema_registry"

s3 = boto3.client("s3")


# ── Schema Evolution Tracking ───────────────────────────────────────────────

def _load_registry(entity: str) -> dict | None:
    """Return the stored schema snapshot for an entity, or None on first run."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"{REGISTRY_PREFIX}/{entity}/current.json")
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None


def _save_registry(entity: str, columns: list) -> None:
    payload = {"entity": entity, "columns": columns, "as_of": INGESTION_DATE}
    s3.put_object(
        Bucket=BUCKET,
        Key=f"{REGISTRY_PREFIX}/{entity}/current.json",
        Body=json.dumps(payload, indent=2),
    )


def detect_schema_evolution(entity: str, df) -> dict | None:
    """
    Compare the DataFrame schema against the stored registry.

    On every run:
      - If a registry exists and columns differ → record the change, save history snapshot
      - Always update current.json with the latest schema

    Returns a change-record dict if evolution was detected, else None.
    """
    current_cols = [f.name for f in df.schema.fields]
    registry = _load_registry(entity)

    event = None
    if registry:
        prev_cols = registry["columns"]
        added   = [c for c in current_cols if c not in prev_cols]
        removed = [c for c in prev_cols if c not in current_cols]

        if added or removed:
            event = {
                "entity":            entity,
                "detection_date":    INGESTION_DATE,
                "previous_version":  registry["as_of"],
                "current_version":   INGESTION_DATE,
                "previous_columns":  json.dumps(prev_cols),
                "current_columns":   json.dumps(current_cols),
                "added_columns":     json.dumps(added),
                "removed_columns":   json.dumps(removed),
            }
            # Write history snapshot to S3 for permanent audit trail
            s3.put_object(
                Bucket=BUCKET,
                Key=f"{REGISTRY_PREFIX}/{entity}/history/{INGESTION_DATE}.json",
                Body=json.dumps(event, indent=2),
            )
            log.warning(
                "Schema evolution detected in '%s': added=%s  removed=%s",
                entity, added, removed,
            )

    _save_registry(entity, current_cols)
    return event


def write_evolution_log(events: list) -> None:
    """
    Write schema evolution events as Parquet to landing/schema_evolution/.
    loading.py will COPY this into warehouse.schema_evolution_log.
    Only runs when at least one evolution event occurred today.
    """
    if not events:
        log.info("No schema evolution events today.")
        return
    ev_df = spark.createDataFrame(events)
    path = f"{LANDING}/schema_evolution/{INGESTION_DATE}/"
    ev_df.write.mode("overwrite").parquet(path)
    log.info("Wrote %d evolution event(s) → %s", len(events), path)


# ── Per-Entity Transformations ──────────────────────────────────────────────

def transform_claimants():
    df = spark.read.option("header", True).option("inferSchema", True).csv(
        f"{RAW}/claimants/{INGESTION_DATE}/"
    )
    df = (
        df
        # Fill NULL gender — 330 nulls in source data
        .withColumn("gender", F.coalesce(F.col("gender"), F.lit("Unknown")))
        .withColumn("claimant_id", F.col("claimant_id").cast("int"))
        .withColumn("employer_id", F.col("employer_id").cast("int"))
        .withColumn("date_of_birth", F.to_date(F.col("date_of_birth")))
        .withColumn("employment_start_date", F.to_date(F.col("employment_start_date")))
        .withColumn("created_at", F.to_timestamp(F.col("created_at")))
        .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
        .filter(F.col("claimant_id").isNotNull())
    )
    return df


def transform_claims():
    """
    Unify claims_v1 and claims_v2 into one dataset.

    Schema evolution history for claims:
      v1 (11 cols) → v2 (12 cols): added claim_severity ('low'|'medium'|'high')
      v1 rows receive claim_severity = 'unknown' to satisfy v2 schema.

    Data quality rules applied:
      - Drop rows where policy_id IS NULL (cannot join to dimensional model)
      - Fill NULL approved_amount → 0.0
      - Lowercase claim_status (mixed casing 'Open'/'open' in source)
    """
    v1 = spark.read.option("header", True).option("inferSchema", True).csv(
        f"{RAW}/claims/{INGESTION_DATE}/claims_v1.csv"
    )
    v2 = spark.read.option("header", True).option("inferSchema", True).csv(
        f"{RAW}/claims/{INGESTION_DATE}/claims_v2.csv"
    )

    # Add claim_severity to v1 rows before union
    v1 = v1.withColumn("claim_severity", F.lit("unknown").cast(StringType()))

    # v1 and v2 contain the same claim_ids — v2 is the authoritative version.
    # Only keep v1 rows for claims that do not exist in v2.
    v1_only = v1.join(v2.select("claim_id"), on="claim_id", how="left_anti")
    claims = v2.unionByName(v1_only)

    claims = (
        claims
        .withColumn("claim_status",    F.lower(F.col("claim_status")))
        .withColumn("approved_amount", F.coalesce(F.col("approved_amount"), F.lit(0.0)))
        .withColumn("claim_amount",    F.col("claim_amount").cast("double"))
        .withColumn("approved_amount", F.col("approved_amount").cast("double"))
        .withColumn("claim_id",        F.col("claim_id").cast("int"))
        .withColumn("claimant_id",     F.col("claimant_id").cast("int"))
        .withColumn("policy_id",       F.col("policy_id").cast("int"))
        .withColumn("incident_date",   F.to_date(F.col("incident_date")))
        .withColumn("report_date",     F.to_date(F.col("report_date")))
        .withColumn("created_at",      F.to_timestamp(F.col("created_at")))
        .withColumn("updated_at",      F.to_timestamp(F.col("updated_at")))
    )

    # Drop rows with NULL policy_id — unlinked claims have no policy context
    before = claims.count()
    claims = claims.filter(F.col("policy_id").isNotNull())
    dropped = before - claims.count()
    log.info("Claims: dropped %d rows with NULL policy_id (%d remain)", dropped, claims.count())

    return claims


def transform_employers():
    return (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{RAW}/employers/{INGESTION_DATE}/")
        .withColumn("employer_id", F.col("employer_id").cast("int"))
        .withColumn("policy_id",   F.col("policy_id").cast("int"))
        .withColumn("created_at",  F.to_timestamp(F.col("created_at")))
        .withColumn("updated_at",  F.to_timestamp(F.col("updated_at")))
        .filter(F.col("employer_id").isNotNull())
    )


def transform_payments():
    return (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{RAW}/payments/{INGESTION_DATE}/")
        .withColumn("payment_id",     F.col("payment_id").cast("int"))
        .withColumn("claim_id",       F.col("claim_id").cast("int"))
        .withColumn("payment_amount", F.col("payment_amount").cast("double"))
        .withColumn("payment_date",   F.to_date(F.col("payment_date")))
        .withColumn("created_at",     F.to_timestamp(F.col("created_at")))
        .filter(F.col("payment_id").isNotNull())
    )


def transform_policies():
    return (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{RAW}/policies/{INGESTION_DATE}/")
        .withColumn("policy_id",       F.col("policy_id").cast("int"))
        .withColumn("premium_amount",  F.col("premium_amount").cast("double"))
        .withColumn("start_date",      F.to_date(F.col("start_date")))
        .withColumn("end_date",        F.to_date(F.col("end_date")))
        .withColumn("created_at",      F.to_timestamp(F.col("created_at")))
        .withColumn("updated_at",      F.to_timestamp(F.col("updated_at")))
        .filter(F.col("policy_id").isNotNull())
    )


def write_parquet(df, entity: str) -> None:
    path = f"{LANDING}/{entity}/{INGESTION_DATE}/"
    df.write.mode("overwrite").parquet(path)
    log.info("Wrote %-12s → %s  (%d rows)", entity, path, df.count())


# ── Main ────────────────────────────────────────────────────────────────────

evolution_events = []

for entity, transform_fn in [
    ("claimants", transform_claimants),
    ("claims",    transform_claims),
    ("employers", transform_employers),
    ("payments",  transform_payments),
    ("policies",  transform_policies),
]:
    df = transform_fn()
    event = detect_schema_evolution(entity, df)
    if event:
        evolution_events.append(event)
    write_parquet(df, entity)

write_evolution_log(evolution_events)

if _IN_GLUE:
    job.commit()

log.info("Transformation complete — Parquet files written to s3://%s/landing/%s/", BUCKET, INGESTION_DATE)
