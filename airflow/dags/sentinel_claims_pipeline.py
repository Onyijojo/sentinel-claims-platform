from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    "owner": "sentinel",
    "retries": 0,
    "email_on_failure": False,
}

CHECKS = [
    (
        "fact_claim PK uniqueness",
        """SELECT CASE WHEN COUNT(*) = COUNT(DISTINCT claim_id) THEN 'PASS' ELSE 'FAIL' END AS result
           FROM warehouse.fact_claim""",
    ),
    (
        "fact_claim FK claimant_key",
        """SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
           FROM warehouse.fact_claim fc
           LEFT JOIN warehouse.dim_claimant dc ON fc.claimant_key = dc.claimant_key
           WHERE dc.claimant_key IS NULL AND fc.claimant_key IS NOT NULL""",
    ),
    (
        "fact_claim non-negative amounts",
        """SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
           FROM warehouse.fact_claim
           WHERE claim_amount < 0 OR approved_amount < 0""",
    ),
    (
        "fact_claim row count >= 2000",
        """SELECT CASE WHEN COUNT(*) >= 2000 THEN 'PASS' ELSE 'FAIL' END AS result
           FROM warehouse.fact_claim""",
    ),
    (
        "fact_claim valid date keys",
        """SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
           FROM warehouse.fact_claim fc
           LEFT JOIN warehouse.dim_date d ON fc.incident_date_key = d.date_key
           WHERE d.date_key IS NULL AND fc.incident_date_key IS NOT NULL""",
    ),
    (
        "dim_claimant SCD2 integrity",
        """SELECT CASE WHEN MAX(current_count) = 1 THEN 'PASS' ELSE 'FAIL' END AS result
           FROM (SELECT claimant_id, COUNT(*) AS current_count
                 FROM warehouse.dim_claimant WHERE is_current = TRUE GROUP BY claimant_id)""",
    ),
    (
        "claim_status standardized",
        """SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
           FROM warehouse.fact_claim
           WHERE claim_status NOT IN ('open', 'closed', 'denied')""",
    ),
    (
        "dim_policy SCD2 integrity",
        """SELECT CASE WHEN MAX(current_count) = 1 THEN 'PASS' ELSE 'FAIL' END AS result
           FROM (SELECT policy_id, COUNT(*) AS current_count
                 FROM warehouse.dim_policy WHERE is_current = TRUE GROUP BY policy_id)""",
    ),
    (
        "fact_payment FK claim_key",
        """SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result
           FROM warehouse.fact_payment fp
           LEFT JOIN warehouse.fact_claim fc ON fp.claim_key = fc.claim_key
           WHERE fc.claim_key IS NULL""",
    ),
]


def run_quality_checks(**context):
    import time
    import boto3

    client = boto3.client("redshift-data", region_name="us-east-1")

    def execute_and_fetch(query):
        resp = client.execute_statement(
            WorkgroupName="sentinel-workgroup",
            Database="dev",
            SecretArn=Variable.get("secret_arn"),
            Sql=query,
        )
        statement_id = resp["Id"]
        while True:
            status = client.describe_statement(Id=statement_id)["Status"]
            if status == "FINISHED":
                break
            if status in ("FAILED", "ABORTED"):
                raise RuntimeError(f"Query failed: {client.describe_statement(Id=statement_id).get('Error')}")
            time.sleep(2)
        result = client.get_statement_result(Id=statement_id)
        return result["Records"][0][0]["stringValue"]

    failed = []
    print("\n========== DATA QUALITY CHECKS ==========")
    for check_name, query in CHECKS:
        status = execute_and_fetch(query)
        print(f"  [{status}] {check_name}")
        if status == "FAIL":
            failed.append(check_name)

    print(f"  {len(CHECKS) - len(failed)}/{len(CHECKS)} checks passed")
    print("=========================================\n")

    if failed:
        raise ValueError(f"Quality checks FAILED: {', '.join(failed)}")


with DAG(
    dag_id="sentinel_claims_pipeline",
    default_args=default_args,
    description="Daily ETL: S3 CSV -> Glue -> Redshift -> Quality Checks",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["sentinel", "etl"],
) as dag:

    extraction = GlueJobOperator(
        task_id="sentinel_extraction",
        job_name="sentinel-extraction",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
        script_args={
            "--gdrive_folder_id":          "{{ var.value.gdrive_folder_id }}",
            "--ingestion_date":            "{{ ds }}",
            "--additional-python-modules": "gdown",
        },
    )

    transformation = GlueJobOperator(
        task_id="sentinel_transformation",
        job_name="sentinel-transformation",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
        script_args={
            "--ingestion_date": "{{ ds }}",
        },
    )

    loading = GlueJobOperator(
        task_id="sentinel_loading",
        job_name="sentinel-loading",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
        script_args={
            "--ingestion_date":            "{{ ds }}",
            "--redshift_endpoint":         "{{ var.value.redshift_endpoint }}",
            "--secret_arn":                "{{ var.value.secret_arn }}",
            "--iam_role_arn":              "{{ var.value.iam_role_arn }}",
            "--additional-python-modules": "psycopg2-binary",
        },
    )

    quality_checks = PythonOperator(
        task_id="quality_checks",
        python_callable=run_quality_checks,
    )

    extraction >> transformation >> loading >> quality_checks
