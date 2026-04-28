import logging

def on_failure_callback(context):
    """
    This function runs whenever a task fails.
    It logs the error so CloudWatch can pick it up.
    """
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    err = context.get('exception')
    
    logging.error(f"ALARM: Task {task_id} in DAG {dag_id} failed with error: {err}")

# Add this to your DAG default_args
default_args = {
    'on_failure_callback': on_failure_callback,
    # ... other args
}