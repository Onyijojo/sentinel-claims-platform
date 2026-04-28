import logging
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Try to import watchtower for CloudWatch logging
try:
    import watchtower
    WATCHTOWER_AVAILABLE = True
except ImportError:
    WATCHTOWER_AVAILABLE = False
    print("Warning: watchtower not installed. Using fallback logger.")


# 2. Configure Logging (CloudWatch via watchtower, with fallback)
def setup_logging():
    """
    Set up logging to stream to CloudWatch using watchtower.
    Falls back to standard logging if watchtower is unavailable.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Console handler for local debugging
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    if WATCHTOWER_AVAILABLE:
        try:
            # Add CloudWatch handler using watchtower
            cw_handler = watchtower.CloudWatchLogHandler(
                log_group='airflow-dags',
                stream_name='{{ task_instance_key_str }}'
            )
            cw_handler.setFormatter(console_format)
            logger.addHandler(cw_handler)
            logger.info("CloudWatch logging via watchtower enabled successfully.")
        except Exception as e:
            logger.warning(f"Failed to configure watchtower CloudWatch logging: {e}. Using fallback.")
            _setup_fallback_logging(logger)
    else:
        _setup_fallback_logging(logger)
    
    return logger


def _setup_fallback_logging(logger):
    """
    Fallback logging setup using the logger.py pattern.
    This ensures logging works even without watchtower.
    """
    # The fallback is already handled by console handler above
    # The on_failure_callback from logger.py will be used for task failures
    logger.info("Using fallback logging configuration.")


# Initialize logging
logger = setup_logging()


# 3. Define Failure Callback (from logger.py)
def on_failure_callback(context):
    """
    This function runs whenever a task fails.
    It logs the error so CloudWatch can pick it up.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    err = context.get('exception')
    
    logger.error(f"ALARM: Task {task_id} in DAG {dag_id} failed with error: {err}")


# 4. Define Default Arguments (Retries & Error Handling)
default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,  # Set to true and add email to get alerts
    'email_on_retry': False,
    'retries': 3,  # Retry up to 3 times if a task fails
    'retry_delay': timedelta(minutes=5),  # Wait 5 minutes between retries
    'on_failure_callback': on_failure_callback,
}

# 2. Instantiate the DAG (Scheduling)
with DAG(
    'production_data_pipeline',
    default_args=default_args,
    description='Daily pipeline: Ingest -> Transform -> Load -> dbt',
    schedule_interval='@daily', # Runs once a day at midnight UTC
    start_date=datetime(2023, 10, 1), # When the DAG should conceptually start
    catchup=False, # Don't run historical dates if starting fresh
    tags=['core_pipeline'],
) as dag:

    # --- 3. Define the Tasks ---

    # Task 1: Ingestion (e.g., triggering your Python script to get Google Drive data)
    # Note: In reality, you'd import your python function here.
    def run_ingestion():
        print("Ingesting data from source to S3...")
        # Your python logic goes here

    ingestion_task = PythonOperator(
        task_id='ingest_data_to_s3',
        python_callable=run_ingestion,
    )

    # Task 2: Transformation (Pre-load formatting if necessary)
    def run_transformation():
        print("Formatting files or cleaning up S3 bucket...")

    transformation_task = PythonOperator(
        task_id='pre_load_transformation',
        python_callable=run_transformation,
    )

    # Task 3: Load (Moving data from S3 to Redshift)
    # Note: You can also use the S3ToRedshiftOperator for this!
    def run_load():
        print("Executing COPY command to load S3 data into Redshift Raw...")

    load_task = PythonOperator(
        task_id='load_to_redshift_raw',
        python_callable=run_load,
    )

    # Task 4: dbt Run (Executing your data models)
    # If dbt is installed on the same machine, BashOperator is easiest.
    # If using dbt Cloud, you would use the DbtCloudRunJobOperator instead.
    dbt_run_task = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /path/to/your/dbt/project && dbt build',
    )

    # Optional: A dummy task to signal the pipeline finished successfully
    pipeline_success = DummyOperator(
        task_id='pipeline_success'
    )

    # --- 4. Set Task Dependencies (The Orchestration) ---
    # The ">>" operator dictates the exact order of execution
    ingestion_task >> transformation_task >> load_task >> dbt_run_task >> pipeline_success