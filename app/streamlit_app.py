import duckdb
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Citi Bike Analytics", layout="wide")

DB_PATH = "data/warehouse/citibike.duckdb"

def run_query(query: str):
    with duckdb.connect(DB_PATH, read_only=True) as con:
        return con.execute(query).df()

st.title("Citi Bike Analytics Dashboard")

daily_trips = run_query("""
    SELECT
        ride_date,
        SUM(trip_count) AS total_trips
    FROM fact_daily_trips
    GROUP BY 1
    ORDER BY 1
""")

member_split = run_query("""
    SELECT
        ride_date,
        member_casual,
        SUM(trip_count) AS total_trips
    FROM fact_daily_trips
    GROUP BY 1, 2
    ORDER BY 1, 2
""")

bike_type = run_query("""
    SELECT
        ride_date,
        rideable_type,
        SUM(trip_count) AS total_trips
    FROM fact_daily_trips
    GROUP BY 1, 2
    ORDER BY 1, 2
""")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Daily ridership")
    fig1 = px.line(
        daily_trips,
        x="ride_date",
        y="total_trips",
        render_mode="svg"
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader("Trips by rider type")
    fig2 = px.line(
        member_split,
        x="ride_date",
        y="total_trips",
        color="member_casual",
        render_mode="svg"
    )
    st.plotly_chart(fig2, use_container_width=True)

st.subheader("Trips by bike type")
fig3 = px.line(
    bike_type,
    x="ride_date",
    y="total_trips",
    color="rideable_type",
    render_mode="svg"
)
st.plotly_chart(fig3, use_container_width=True)