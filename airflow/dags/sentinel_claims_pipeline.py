from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    "owner": "sentinel",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="sentinel_claims_pipeline",
    default_args=default_args,
    description="Daily ETL: S3 CSV -> Glue -> Redshift",
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
            "--ingestion_date":      "{{ ds }}",
            "--redshift_endpoint":   "{{ var.value.redshift_endpoint }}",
            "--secret_arn":          "{{ var.value.secret_arn }}",
            "--iam_role_arn":        "{{ var.value.iam_role_arn }}",
        },
    )

    extraction >> transformation >> loading
