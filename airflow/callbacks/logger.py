"""
Reusable Airflow callbacks.

Lives under airflow/callbacks/ (NOT airflow/logs/, which Airflow uses for its own
runtime log files). Import from your DAG via:

    from callbacks.logger import on_failure_callback

…assuming `airflow/` is on PYTHONPATH (it is by default when DAGs live in
airflow/dags/).
"""
import logging

log = logging.getLogger(__name__)


def on_failure_callback(context):
    """
    Airflow task failure hook.

    Logs to the standard Airflow logger. CloudWatch will pick this up if the
    root logger has a watchtower handler attached at task-run time (see the
    DAG file for that wiring).
    """
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    err = context.get("exception")
    log.error("ALARM: Task %s in DAG %s failed with error: %s", task_id, dag_id, err)
