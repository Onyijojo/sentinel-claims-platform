"""
Glue Job — Loading
S3 Landing Zone (Parquet) → Redshift (staging → warehouse → analytics)

Job type: Python Shell (uses Redshift Data API — no Spark, no JDBC driver)
Input:    s3://sentinel-claims-data/landing/{entity}/{ingestion_date}/ (Parquet)
Output:   Redshift:
            staging.stg_*        ← COPY from Parquet
            warehouse.dim_*      ← SCD Type 2 merge
            warehouse.fact_*     ← fact inserts
            analytics.*          ← view refresh

Deploy to Glue: Upload to S3, create a Glue Python Shell job pointing to this script

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
REQUIRED GLUE JOB PARAMETERS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  --redshift_endpoint  <workgroup>.<account-id>.<region>.redshift-serverless.amazonaws.com
  --secret_arn         arn:aws:secretsmanager:...:secret:redshift!sentinel-namespace-admin-...
  --iam_role_arn       arn:aws:iam::<account-id>:role/sentinel-redshift-role

OPTIONAL GLUE JOB PARAMETERS
  --ingestion_date  YYYY-MM-DD   (defaults to today)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WHERE TO FIND EACH VALUE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  redshift_endpoint:
    Redshift → Serverless → Workgroups → sentinel-workgroup → Endpoint
    Copy only the hostname (not the port or database)

  secret_arn:
    Secrets Manager → find redshift!sentinel-namespace-admin → copy ARN

  iam_role_arn:
    IAM → Roles → sentinel-redshift-role → copy ARN
"""
import json
import logging
import sys
from datetime import datetime

import boto3
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger("sentinel.loading")


def _get_arg(name: str, default=None):
    key = f"--{name}"
    if key in sys.argv:
        return sys.argv[sys.argv.index(key) + 1]
    return default


BUCKET             = "sentinel-claims-data"
LANDING            = f"s3://{BUCKET}/landing"
DATABASE           = "dev"
REGION             = "us-east-1"
INGESTION_DATE     = _get_arg("ingestion_date", datetime.now().strftime("%Y-%m-%d"))
REDSHIFT_ENDPOINT  = _get_arg("redshift_endpoint")
SECRET_ARN         = _get_arg("secret_arn")
IAM_ROLE           = _get_arg("iam_role_arn")

for param, val in [("--redshift_endpoint", REDSHIFT_ENDPOINT), ("--secret_arn", SECRET_ARN), ("--iam_role_arn", IAM_ROLE)]:
    if not val:
        raise RuntimeError(f"Missing required job parameter: {param}\nSee the script header for instructions.")

# Fetch DB credentials from Secrets Manager
sm      = boto3.client("secretsmanager", region_name=REGION)
secret  = json.loads(sm.get_secret_value(SecretId=SECRET_ARN)["SecretString"])
DB_USER = secret["username"]
DB_PASS = secret["password"]

conn = psycopg2.connect(
    host=REDSHIFT_ENDPOINT,
    port=5439,
    dbname=DATABASE,
    user=DB_USER,
    password=DB_PASS,
    sslmode="require",
)
conn.autocommit = False


# ── SQL execution helper ─────────────────────────────────────────────────────

def run_sql(sql: str, label: str = "") -> None:
    """Execute SQL and commit. Rolls back on failure."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        log.info("OK   %s", label or sql[:70].strip())
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"SQL failed [{label}]: {e}") from None


# ── 1. COPY staging tables from Parquet landing zone ────────────────────────
#
# FORMAT AS PARQUET lets Redshift infer column types from the Parquet metadata —
# no need for IGNOREHEADER, DATEFORMAT, or TIMEFORMAT options.

STAGING_LOADS = [
    ("stg_claimants", f"{LANDING}/claimants/{INGESTION_DATE}/"),
    ("stg_claims",    f"{LANDING}/claims/{INGESTION_DATE}/"),
    ("stg_employers", f"{LANDING}/employers/{INGESTION_DATE}/"),
    ("stg_payments",  f"{LANDING}/payments/{INGESTION_DATE}/"),
    ("stg_policies",  f"{LANDING}/policies/{INGESTION_DATE}/"),
]

for table, s3_path in STAGING_LOADS:
    run_sql(f"TRUNCATE staging.{table};", label=f"TRUNCATE {table}")
    run_sql(
        f"""
        COPY staging.{table}
        FROM '{s3_path}'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET;
        """,
        label=f"COPY {table}",
    )

# Load schema evolution log if today's file exists (not written every run)
try:
    run_sql(
        f"""
        COPY warehouse.schema_evolution_log
            (entity, detection_date, previous_version, current_version,
             previous_columns, current_columns, added_columns, removed_columns)
        FROM '{LANDING}/schema_evolution/{INGESTION_DATE}/'
        IAM_ROLE '{IAM_ROLE}'
        FORMAT AS PARQUET;
        """,
        label="COPY schema_evolution_log",
    )
except RuntimeError as e:
    if "file not found" in str(e).lower() or "0 rows" in str(e).lower():
        log.info("No schema evolution events today — skipping schema_evolution_log load.")
    else:
        raise


# ── 2. Warehouse layer: SCD Type 2 merge ────────────────────────────────────

# dim_claimant — expire changed rows, insert new current row
run_sql(
    """
    UPDATE warehouse.dim_claimant
    SET    is_current  = FALSE,
           expiry_date = CURRENT_DATE - 1
    FROM   staging.stg_claimants s
    WHERE  warehouse.dim_claimant.claimant_id = s.claimant_id
      AND  warehouse.dim_claimant.is_current  = TRUE
      AND  (warehouse.dim_claimant.gender      <> s.gender
         OR warehouse.dim_claimant.employer_id <> s.employer_id);
    """,
    label="SCD2 expire dim_claimant",
)

run_sql(
    """
    INSERT INTO warehouse.dim_claimant
        (claimant_id, first_name, last_name, date_of_birth, gender,
         employment_start_date, employer_id, effective_date, expiry_date, is_current)
    SELECT s.claimant_id, s.first_name, s.last_name, s.date_of_birth, s.gender,
           s.employment_start_date, s.employer_id,
           CURRENT_DATE, '9999-12-31'::DATE, TRUE
    FROM   staging.stg_claimants s
    WHERE  NOT EXISTS (
        SELECT 1 FROM warehouse.dim_claimant d
        WHERE  d.claimant_id = s.claimant_id AND d.is_current = TRUE
    );
    """,
    label="SCD2 insert dim_claimant",
)

# dim_policy — SCD2 (coverage_type or dates changed)
run_sql(
    """
    UPDATE warehouse.dim_policy
    SET    is_current  = FALSE,
           expiry_date = CURRENT_DATE - 1
    FROM   staging.stg_policies s
    WHERE  warehouse.dim_policy.policy_id    = s.policy_id
      AND  warehouse.dim_policy.is_current   = TRUE
      AND  warehouse.dim_policy.coverage_type <> s.coverage_type;
    """,
    label="SCD2 expire dim_policy",
)

run_sql(
    """
    INSERT INTO warehouse.dim_policy
        (policy_id, policy_number, coverage_type, start_date, end_date,
         premium_amount, effective_date, expiry_date, is_current)
    SELECT s.policy_id, s.policy_number, s.coverage_type, s.start_date, s.end_date,
           s.premium_amount, CURRENT_DATE, '9999-12-31'::DATE, TRUE
    FROM   staging.stg_policies s
    WHERE  NOT EXISTS (
        SELECT 1 FROM warehouse.dim_policy d
        WHERE  d.policy_id = s.policy_id AND d.is_current = TRUE
    );
    """,
    label="SCD2 insert dim_policy",
)

# dim_employer — SCD2 (industry or location changed)
run_sql(
    """
    UPDATE warehouse.dim_employer
    SET    is_current  = FALSE,
           expiry_date = CURRENT_DATE - 1
    FROM   staging.stg_employers s
    WHERE  warehouse.dim_employer.employer_id = s.employer_id
      AND  warehouse.dim_employer.is_current  = TRUE
      AND  (warehouse.dim_employer.industry  <> s.industry
         OR warehouse.dim_employer.location  <> s.location);
    """,
    label="SCD2 expire dim_employer",
)

run_sql(
    """
    INSERT INTO warehouse.dim_employer
        (employer_id, company_name, industry, location, policy_id,
         effective_date, expiry_date, is_current)
    SELECT s.employer_id, s.company_name, s.industry, s.location, s.policy_id,
           CURRENT_DATE, '9999-12-31'::DATE, TRUE
    FROM   staging.stg_employers s
    WHERE  NOT EXISTS (
        SELECT 1 FROM warehouse.dim_employer d
        WHERE  d.employer_id = s.employer_id AND d.is_current = TRUE
    );
    """,
    label="SCD2 insert dim_employer",
)


# ── 3. Fact tables ───────────────────────────────────────────────────────────
# Truncate before every load — staging is already fully reloaded each run,
# so fact tables must be too. This guarantees idempotency regardless of
# how many times the job has run or failed mid-way.

run_sql("TRUNCATE warehouse.fact_payment;", label="TRUNCATE fact_payment")
run_sql("TRUNCATE warehouse.fact_claim;",   label="TRUNCATE fact_claim")

run_sql(
    """
    INSERT INTO warehouse.fact_claim
        (claim_id, claimant_key, policy_key, employer_key,
         incident_date_key, report_date_key,
         claim_type, claim_status, claim_severity,
         claim_amount, approved_amount)
    SELECT
        c.claim_id,
        dc.claimant_key,
        dp.policy_key,
        de.employer_key,
        TO_CHAR(c.incident_date, 'YYYYMMDD')::INTEGER,
        TO_CHAR(c.report_date,   'YYYYMMDD')::INTEGER,
        c.claim_type,
        c.claim_status,
        c.claim_severity,
        c.claim_amount,
        c.approved_amount
    FROM   staging.stg_claims c
    JOIN   warehouse.dim_claimant dc ON dc.claimant_id = c.claimant_id AND dc.is_current = TRUE
    JOIN   warehouse.dim_policy   dp ON dp.policy_id   = c.policy_id   AND dp.is_current = TRUE
    JOIN   warehouse.dim_employer de ON de.employer_id = dc.employer_id AND de.is_current = TRUE;
    """,
    label="INSERT fact_claim",
)

run_sql(
    """
    INSERT INTO warehouse.fact_payment
        (payment_id, claim_key, payment_date_key, payment_amount, payment_type)
    SELECT
        p.payment_id,
        fc.claim_key,
        TO_CHAR(p.payment_date, 'YYYYMMDD')::INTEGER,
        p.payment_amount,
        p.payment_type
    FROM   staging.stg_payments p
    JOIN   warehouse.fact_claim fc ON fc.claim_id = p.claim_id;
    """,
    label="INSERT fact_payment",
)


# ── 4. Analytics layer — refresh views ──────────────────────────────────────

run_sql(
    """
    CREATE OR REPLACE VIEW analytics.claims_summary AS
    SELECT
        de.company_name,
        de.industry,
        dp.coverage_type,
        fc.claim_type,
        fc.claim_status,
        fc.claim_severity,
        COUNT(*)                AS total_claims,
        SUM(fc.claim_amount)    AS total_claimed,
        SUM(fc.approved_amount) AS total_approved,
        AVG(fc.claim_amount)    AS avg_claim_amount
    FROM   warehouse.fact_claim fc
    JOIN   warehouse.dim_employer de ON fc.employer_key = de.employer_key AND de.is_current = TRUE
    JOIN   warehouse.dim_policy   dp ON fc.policy_key   = dp.policy_key   AND dp.is_current = TRUE
    GROUP  BY 1, 2, 3, 4, 5, 6;
    """,
    label="REFRESH analytics.claims_summary",
)

run_sql(
    """
    CREATE OR REPLACE VIEW analytics.schema_evolution_history AS
    SELECT
        entity,
        detection_date,
        previous_version,
        current_version,
        added_columns,
        removed_columns
    FROM   warehouse.schema_evolution_log
    ORDER  BY detection_date DESC, entity;
    """,
    label="REFRESH analytics.schema_evolution_history",
)

log.info(
    "Loading complete — staging → warehouse → analytics for ingestion_date=%s",
    INGESTION_DATE,
)
