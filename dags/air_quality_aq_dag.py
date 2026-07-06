from datetime import datetime, timedelta
from airflow.sdk import dag, task
import requests, logging, duckdb, os, time
import pandas as pd

BASE_URL = "https://api.openaq.org/v3/locations"
BBOX = "13.088,52.338,13.761,52.675"
URL = (
    f"{BASE_URL}?bbox={BBOX}&limit=100"
)

API_KEY = os.getenv("OPENAQ_API_KEY")
DATA_DIR = "/opt/airflow/data/"
CSV_PATH = f"{DATA_DIR}/air_quality_staging.csv"
DB_PATH = f"{DATA_DIR}/air_quality.duckdb"
SENSORS_CSV = f"{DATA_DIR}/air_quality_sensors.csv"
LOCATION_CSV = f"{DATA_DIR}/air_quality_locations.csv"

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
    def discover_station_ids() -> list[int]:
        resp = requests.get(BASE_URL, headers={"X-API-KEY": API_KEY}, params={"bbox": BBOX, "limit": 100}, timeout=30)
        resp.raise_for_status()
        ids = [loc["id"] for loc in resp.json().get("results", [])]
        logging.info(f"Discovered {len(ids)} stations in Berlin")
        return ids

    @task(max_active_tis_per_dag=2, retries=5, retry_delay=timedelta(seconds=30))
    def fetch_latest(location_id: int) -> list[dict]:
        url = f"{BASE_URL}/{location_id}/latest"
        for attempt in range(4):
            resp = requests.get(url, headers={"X-API-Key": API_KEY}, timeout=30)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 5)) + 1
                logging.warning(f"429 on {location_id}, sleeping {wait}s (attempt {attempt + 1})")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return [
                {
                    "value": r["value"],
                    "sensor_id": r["sensorsId"],
                    "location_id": r["locationsId"],
                    "datetime_utc": r["datetime"]["utc"],
                    "datetime_local": r["datetime"]["local"],
                    "latitude": r["coordinates"]["latitude"],
                    "longitude": r["coordinates"]["longitude"],
                }
                for r in resp.json().get("results", [])
            ]
        raise RuntimeError(f"Still 429 on {location_id} after retries")

    @task
    def load_to_warehouse(mapped_rows: list[dict]):
        all_rows = [row for batch in mapped_rows for row in batch]
        if not all_rows:
            logging.warning("nothing to load")
            return
        df = pd.DataFrame(all_rows)
        conn = duckdb.connect(DB_PATH)
        try:
            conn.execute(
                """
                create table if not exists air_quality_latest(
                    value double,
                    sensor_id bigint,
                    location_id bigint,
                    datetime_utc timestamp,
                    datetime_locat varchar,
                    latitude double,
                    longitude double
                )
                """
            )

            conn.execute(
                """
                insert into air_quality_latest
                select value, sensor_id, location_id, datetime_utc, datetime_local, latitude, longitude
                from df
                """
            )

            n = conn.execute("select count(*) from air_quality_latest").fetchdf()
            logging.info(f"air_quality_latest contains {n} rows")
        finally:
            conn.close()

    @task
    def get_and_log():
        resp = requests.get(URL, headers={"X-API-KEY": API_KEY}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        logging.info(f"API returned {len(data.get('results', []))} sensor readings")

        sensor_rows = []
        for loc in data["results"]:
            for s in loc["sensors"]:
                sensor_rows.append({
                    "sensor_id": s["id"],
                    "location_id": loc["id"],
                    "parameter_name": s["parameter"]["name"],
                    "display_name": s["parameter"]["displayName"],
                    "units": s["parameter"]["units"]
                })
        sensors_df = pd.DataFrame(sensor_rows)

        location_rows = []
        for loc in data["results"]:
            location_rows.append({
                "location_id": loc["id"],
                "name": loc.get("name"),
                "locality": loc.get("locality"),
                "country": loc.get("country", {}).get("name"),
                "latitude": loc.get("coordinates", {}).get("latitude"),
                "longitude": loc.get("coordinates", {}).get("longitude"),
                "timezone": loc.get("timezone"),
                "datetime_last_utc": loc.get("datetimeLast", {}).get("utc")
            })
        locations_df = pd.DataFrame(location_rows)

        sensors_df.to_csv(SENSORS_CSV, mode="w", header=True, index=False)
        locations_df.to_csv(LOCATION_CSV, mode="w", header=True, index=False)
        logging.info(f"Staged {len(locations_df)} locations, {len(sensors_df)} sensors")

    @task
    def load_dim_to_warehouse():
        conn = duckdb.connect(DB_PATH)

        try:

            ## dim_sensors table
            conn.execute(
                """
                create table if not exists dim_sensors(
                    sensor_id bigint,
                    location_id bigint,
                    parameter_name varchar,
                    display_name varchar,
                    units varchar
                )
                """
            )

            conn.execute("""
                insert into dim_sensors
                select *
                from read_csv_auto(?)
            """, [SENSORS_CSV])

            # dim_location table
            conn.execute(
                """
                create table if not exists dim_locations(
                    location_id bigint,
                    name varchar,
                    locality varchar,
                    country varchar,
                    latitude double,
                    longitude double,
                    timezone varchar,
                    datetime_last_utc timestamp        
                )
                """
            )

            conn.execute("""
                insert into dim_locations
                select * 
                from read_csv_auto(?)
            """, [LOCATION_CSV])

            sensors_n = conn.execute("select count(*) from dim_sensors").fetchone()[0]
            locations_n = conn.execute("select count(*) from dim_locations").fetchone()[0]

            logging.info(f"dim_location: {sensors_n} rows, dim_sensor: {locations_n} rows")
        finally:
            conn.close()

    # latest measurements
    ids = discover_station_ids()
    rows = fetch_latest.expand(location_id=ids)
    load_to_warehouse(rows)

    # dims
    get_and_log() >> load_dim_to_warehouse()

air_quality_elt_dag()
