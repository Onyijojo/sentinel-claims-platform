import json
import logging
import os
import sys
from datetime import datetime

import boto3
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

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


# ── Helpers ─────────────────────────────────────────────────────────────────

def trim_strings(df):
    """Trim leading/trailing whitespace from every string column."""
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))
    return df


def ensure_columns(df, cols):
    """Add any missing columns as NULL so downstream transforms don't fail."""
    for c in cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast(StringType()))
    return df


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
    ev_df = ev_df.withColumn("detection_date", F.to_date(F.col("detection_date")))
    path = f"{LANDING}/schema_evolution/{INGESTION_DATE}/"
    ev_df.write.mode("overwrite").parquet(path)
    log.info("Wrote %d evolution event(s) → %s", len(events), path)


# ── Per-Entity Transformations ──────────────────────────────────────────────

def transform_claimants():
    df = spark.read.option("header", True).option("inferSchema", True).csv(
        f"{RAW}/claimants/{INGESTION_DATE}/"
    )
    df = trim_strings(df)
    df = ensure_columns(df, [
        "claimant_id", "first_name", "last_name", "date_of_birth", "gender",
        "employment_start_date", "employer_id", "created_at", "updated_at",
    ])
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
    w = Window.partitionBy("claimant_id").orderBy(F.col("updated_at").desc())
    df = (
        df
        .withColumn("rn", F.row_number().over(w))
        .withColumn("effective_from", F.coalesce(F.to_date(F.col("created_at")), F.to_date(F.col("updated_at")), F.current_date()))
        .withColumn("effective_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.when(F.col("rn") == 1, F.lit(True)).otherwise(F.lit(False)))
        .drop("rn")
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

    v1 = trim_strings(v1)
    v2 = trim_strings(v2)

    required_cols = [
        "claim_id", "claimant_id", "policy_id",
        "incident_date", "report_date",
        "claim_type", "claim_status", "claim_severity",
        "claim_amount", "approved_amount",
        "created_at", "updated_at",
    ]
    v1 = ensure_columns(v1, required_cols)
    v2 = ensure_columns(v2, required_cols)

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
    df = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{RAW}/employers/{INGESTION_DATE}/")
    )
    df = trim_strings(df)
    df = ensure_columns(df, ["employer_id", "policy_id", "created_at", "updated_at"])
    df = (
        df
        .withColumn("employer_id", F.col("employer_id").cast("int"))
        .withColumn("policy_id",   F.col("policy_id").cast("int"))
        .withColumn("created_at",  F.to_timestamp(F.col("created_at")))
        .withColumn("updated_at",  F.to_timestamp(F.col("updated_at")))
        .filter(F.col("employer_id").isNotNull())
    )
    w = Window.partitionBy("employer_id").orderBy(F.col("updated_at").desc())
    df = (
        df
        .withColumn("rn", F.row_number().over(w))
        .withColumn("effective_from", F.coalesce(F.to_date(F.col("created_at")), F.to_date(F.col("updated_at")), F.current_date()))
        .withColumn("effective_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.col("rn") == 1)
        .drop("rn")
    )
    return df


def transform_payments():
    df = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{RAW}/payments/{INGESTION_DATE}/")
    )
    df = trim_strings(df)
    df = ensure_columns(df, ["payment_id", "claim_id"])
    df = (
        df
        .withColumn("payment_id",     F.col("payment_id").cast("int"))
        .withColumn("claim_id",       F.col("claim_id").cast("int"))
        .withColumn("payment_amount", F.col("payment_amount").cast("double"))
        .withColumn("payment_date",   F.to_date(F.col("payment_date")))
        .withColumn("created_at",     F.to_timestamp(F.col("created_at")))
        .filter(F.col("payment_id").isNotNull())
    )
    return df.dropDuplicates(["payment_id"])


def transform_policies():
    df = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{RAW}/policies/{INGESTION_DATE}/")
    )
    df = trim_strings(df)
    df = ensure_columns(df, [
        "policy_id", "policy_number", "start_date", "end_date", "created_at", "updated_at",
    ])
    df = (
        df
        .withColumn("policy_id",       F.col("policy_id").cast("int"))
        .withColumn("premium_amount",  F.col("premium_amount").cast("double"))
        .withColumn("start_date",      F.to_date(F.col("start_date")))
        .withColumn("end_date",        F.to_date(F.col("end_date")))
        .withColumn("created_at",      F.to_timestamp(F.col("created_at")))
        .withColumn("updated_at",      F.to_timestamp(F.col("updated_at")))
        .filter(F.col("policy_id").isNotNull())
    )
    w = Window.partitionBy("policy_id").orderBy(F.col("updated_at").desc())
    df = (
        df
        .withColumn("rn", F.row_number().over(w))
        .withColumn("effective_from", F.coalesce(F.col("start_date"), F.current_date()))
        .withColumn("effective_to", F.col("end_date"))
        .withColumn("is_current", F.col("rn") == 1)
        .drop("rn")
    )
    return df


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
