import duckdb
import pandas as pd
import streamlit as st

DB_PATH = "data/air_quality.duckdb"

st.set_page_config(page_title="Air Quality - Open AQ", layout="wide")
st.title("Air Quality - Open AQ")

@st.cache_data(ttl=60)
def load_data():
    conn = duckdb.connect(DB_PATH, read_only=True)
    df = conn.execute(
        """
        select *
        from air_quality_latest
        order by datetime_utc
        """
    ).fetchdf()

    conn.close()
    return df

try:
    df = load_data()
except Exception as e:
    st.error(f"Read failed: {type(e).__name__}: {e}")
    st.stop()

c1, c2, c3 = st.columns(3)
c1.metric("Total readings", f"{len(df):,}")
c2.metric("Sensors seen", df["sensor_id"].nunique())
c3.metric("Latest reading (UTC)", str(df["datetime_utc"].max()))

st.subheader("Values over time, by sensor")
st.caption(
    "Lines are labelled by sensor_id. The /latest response doesn't include the "
    "pollutant name, so to show 'PM2.5' etc. you'd join sensor_id to sensor "
    "metadata from /v3/locations/{id}"
)

pivot = df.pivot_table(index="datetime_utc", columns="sensor_id", values="value")
st.line_chart(pivot)

st.subheader("Most recent value per sensor")
latest = (
    df.sort_values("datetime_utc")
    .groupby("sensor_id")
    .tail(1)
    .loc[:, ["sensor_id", "datetime_utc", "value"]]
    .reset_index(drop=True)
)
st.dataframe(latest, use_container_width=True)

coords = df[["latitude", "longitude"]].dropna().drop_duplicates()
if not coords.empty:
    st.subheader("Station location")
    st.map(coords)

with st.expander("Raw data"):
    st.dataframe(
        df.sort_values("datetime_utc", ascending=False),
        use_container_width=True,
    )