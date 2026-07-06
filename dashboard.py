import duckdb
import streamlit as st

DB_PATH = "data/air_quality.duckdb"

st.set_page_config(page_title="Air Quality - Open AQ", layout="wide")
st.title("Air Quality - Open AQ")

@st.cache_data(ttl=60)
def load_data():
    conn = duckdb.connect(DB_PATH, read_only=True)
    df = conn.execute(
        """
        select 
            m.datetime_utc,
            m.value,
            m.sensor_id,
            s.display_name as pollutant,
            s.parameter_name,
            s.units,
            l.name as station_name,
            l.locality,
            m.latitude,
            m.longitude,
        from air_quality_latest m
        left join dim_sensors s on m.sensor_id = s.sensor_id
        left join dim_locations l on m.location_id = l.location_id
        order by m.datetime_utc
        """
    ).fetchdf()

    conn.close()
    return df

try:
    df = load_data()
except Exception as e:
    st.error(f"Read failed: {type(e).__name__}: {e}")
    st.stop()

if df.empty:
    st.warning("No data yet found")
    st.stop()

stations = sorted(df["station_name"].dropna().unique())
if stations:
   picked = st.multiselect("Filter by station", stations, default=stations)
df = df[df["station_name"].isin(picked)]

c1, c2, c3 = st.columns(3)
c1.metric("Total readings", f"{len(df)} readings")
c2.metric("Pollutants tracked", df["pollutant"].nunique())
c3.metric("Latest reading (UTC)", str(df["datetime_utc"].max()))

st.subheader("Values over time, by pollutant")
st.caption(
    "Lines are labelled by pollutant (from the sensor dimension table). "
    "If multiple stations are selected, values are averaged per pollutant."
)
pivot = df.pivot_table(index="datetime_utc", columns="pollutant", values="value")
st.line_chart(pivot)

st.subheader("Most recent value per pollutant")
latest = (
    df.sort_values("datetime_utc")
    .groupby(["station_name", "pollutant"])
    .tail(1)
    .loc[:, ["station_name", "pollutant", "value", "units", "datetime_utc"]]
    .sort_values(["station_name", "pollutant"])
    .reset_index(drop=True)
)
st.dataframe(latest, use_container_width=True)

coords = df[["latitude", "longitude"]].dropna().drop_duplicates()
if not coords.empty:
    st.subheader("Station locations")
    st.map(coords)

with st.expander("Raw data"):
    st.dataframe(
        df.sort_values("datetime_utc", ascending=False),
        use_container_width=True,
    )
