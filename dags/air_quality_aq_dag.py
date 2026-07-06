from datetime import datetime, timedelta
from airflow.sdk import dag, task
import requests
import logging
import duckdb
import pandas as pd
import os

URL = (
    "https://api.openaq.org/v3/locations/2178/latest"
)
API_KEY = os.getenv("OPENAQ_API_KEY")
DATA_DIR = "/opt/airflow/data/"
CSV_PATH = f"{DATA_DIR}/air_quality_staging.csv"
DB_PATH = f"{DATA_DIR}/air_quality.duckdb"

def alert_on_failure(context):
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    logging.error(f"Alert: {dag_id}, {task_id} failed")

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(hours=6),
    "on_failure_callback": alert_on_failure
}

@dag(
    dag_id="air_quality_elt_dag",
    start_date=datetime(2026, 7, 6),
    catchup=False,
    schedule="0 */6 * * *",
    default_args=default_args
)

def air_quality_elt_dag():

    @task
    def get_and_log():
        resp = requests.get(URL, headers={"X-API-Key": API_KEY}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        logging.info(f"API returned {len(data.get('results', []))} sensor readings")

        df = pd.json_normalize(data['results'])
        df = df.rename(columns={
            "datetime.utc": "datetime_utc",
            "datetime.local": "datetime_local",
            "coordinates.latitude": "latitude",
            "coordinates.longitude": "longitude",
            "sensorsId": "sensor_id",
            "locationsId": "location_id"
        })

        df.to_csv(CSV_PATH, mode="w", header=True, index=False)
        logging.info(f"Staged {len(df)} rows to {CSV_PATH}")

    @task
    def load_to_warehouse():
        conn = duckdb.connect(DB_PATH)

        try:
            conn.execute("""
                create table if not exists air_quality_latest (
                    value double,
                    sensor_id bigint,
                    location_id bigint,
                    datetime_utc timestamp,
                    datetime_local varchar,
                    latitude double,
                    longitude double)
                """
            )

            conn.execute("""
                insert into air_quality_latest
                select value, sensor_id, location_id, datetime_utc, datetime_local, latitude, longitude
                from read_csv_auto(?)
            """, [CSV_PATH])

            n = conn.execute("select count(*) from air_quality_latest").fetchone()[0]
            preview = conn.execute(
                "select * from air_quality_latest order by datetime_utc desc limit 5"
            ).fetchdf()
            logging.info(f"air_quality_latest row count: {n} rows")
        finally:
            conn.close()

    get_and_log()
    load_to_warehouse()

air_quality_elt_dag()
