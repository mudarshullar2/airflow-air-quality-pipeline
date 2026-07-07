from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

def run_my_func():
    logging.info("Running every 6 hours")

def alert_on_failure(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    logging.error(f"Alert: {dag_id}, {task_id} failed")

default_args = {
        "owner": "airflow",
        "start_date": datetime(2026, 7, 6),
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": alert_on_failure,
    }

with DAG(
    dag_id="ping_dag",
    default_args=default_args,
    schedule="*/5 * * * *",
    catchup=True,
) as dag:
    execute_python_task = PythonOperator(
        task_id="run_my_func",
        python_callable=run_my_func,
    )
