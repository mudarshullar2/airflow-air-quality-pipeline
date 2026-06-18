from datetime import datetime
from airflow.sdk import dag, task
from config.settings import URL
import requests
import logging
import pandas as pd

@dag(
    dag_id="dag_with_http_operator",
    start_date=datetime(2026, 4, 18),
    catchup=False,
    schedule="0 */6 * * *",
)
def weather_alt_dag():
    @task
    def get_and_log():
        resp = requests.get(URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        logging.info(f"Content of api_response is {data}")
        out_path = "/opt/airflow/data/weather_staging.csv"

        current = data["current"]
        df = pd.DataFrame([current])
        df["Date"] = pd.to_datetime(df["time"]).dt.date
        df["Time"] = pd.to_datetime(df["time"]).dt.time
        df = df.drop("time", axis=1)
        df = df.rename(
            columns={
                "temperature_2m": "temperature",
                "relative_humidity_2m": "rel_humidity",
            }
        )

        with open(out_path, "a", newline="") as f:
            df.to_csv(f, index=False)

        logging.info(f"Content of csv file is {df}")
        logging.info(f"Wrote {out_path}")
        return data

    get_and_log()

weather_alt_dag()
