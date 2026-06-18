from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import logging

default_args = {
        "owner": "airflow",
        "start_date": datetime(2026, 4, 18)
    }

def run_my_func():
    logging.info("Running five minutes scheduler")

with DAG(
    dag_id="five_minute_schedule_dag",
    default_args=default_args,
    schedule="*/5 * * * *",
    catchup=False,
) as dag:
    execute_python_task = PythonOperator(
        task_id="run_my_func",
        python_callable=run_my_func,
    )
