"""
Production data pipeline:  Ingest -> Transform -> Load -> dbt.

DAG-parsing rules followed here:
    * No expensive setup at module import time. The Airflow scheduler re-parses
      every DAG file every few seconds, so module-level work runs constantly.
    * Logging handlers are attached inside task callables (run-time), not here.
    * Jinja templating only renders inside templated operator parameters
      (e.g. bash_command), never inside arbitrary Python — so we don't try
      to template handler kwargs at parse time.
"""
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator    # DummyOperator is deprecated
from airflow.operators.python import PythonOperator

# Reusable failure callback (lives in airflow/callbacks/logger.py)
from callbacks.logger import on_failure_callback


# ---- Helpers -----------------------------------------------------------------

def _attach_cloudwatch_handler():
    """
    Attach a CloudWatch handler to the root logger at TASK runtime.

    Called from inside each PythonOperator callable, never at module load.
    Safe to call multiple times — guards against duplicate handlers.
    """
    root = logging.getLogger()
    if any(h.__class__.__name__ == "CloudWatchLogHandler" for h in root.handlers):
        return  # already attached on this worker

    try:
        import watchtower  # imported lazily so DAG parse never fails on missing dep
    except ImportError:
        root.warning("watchtower not installed; skipping CloudWatch handler.")
        return

    handler = watchtower.CloudWatchLogHandler(
        log_group="airflow-dags",
        # Use real env vars / context — Jinja '{{ ... }}' does NOT render here.
        stream_name=os.environ.get("AIRFLOW_CTX_TASK_ID", "unknown_task"),
    )
    handler.setFormatter(
        logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    )
    root.addHandler(handler)
    root.setLevel(logging.INFO)


# ---- Task callables ----------------------------------------------------------

def run_ingestion():
    _attach_cloudwatch_handler()
    log = logging.getLogger(__name__)
    log.info("Ingesting data from source to S3...")
    # TODO: import and call your real ingestion function from extract_data.py


def run_transformation():
    _attach_cloudwatch_handler()
    log = logging.getLogger(__name__)
    log.info("Formatting files / cleaning up S3 bucket...")


def run_load():
    _attach_cloudwatch_handler()
    log = logging.getLogger(__name__)
    log.info("Executing COPY command to load S3 data into Redshift Raw...")


# ---- DAG ---------------------------------------------------------------------

DBT_PROJECT_DIR = os.environ.get(
    "DBT_PROJECT_DIR",
    "/opt/airflow/dbt",  # TODO: override via env or set the real path
)

default_args = {
    "owner": "data_engineering_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="production_data_pipeline",
    default_args=default_args,
    description="Daily pipeline: Ingest -> Transform -> Load -> dbt",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["core_pipeline"],
) as dag:

    ingestion_task = PythonOperator(
        task_id="ingest_data_to_s3",
        python_callable=run_ingestion,
    )

    transformation_task = PythonOperator(
        task_id="pre_load_transformation",
        python_callable=run_transformation,
    )

    load_task = PythonOperator(
        task_id="load_to_redshift_raw",
        python_callable=run_load,
    )

    dbt_run_task = BashOperator(
        task_id="run_dbt_models",
        # Jinja templating works here — bash_command is a templated field.
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps && dbt build",
    )

    pipeline_success = EmptyOperator(task_id="pipeline_success")

    ingestion_task >> transformation_task >> load_task >> dbt_run_task >> pipeline_success
