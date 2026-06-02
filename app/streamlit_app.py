import duckdb
import streamlit as st
import plotly.express as px

st.set_page_config(page_title="Citi Bike Analytics", layout="wide")

DB_PATH = "data/warehouse/citibike.duckdb"

def run_query(query: str):
    with duckdb.connect(DB_PATH, read_only=True) as con:
        return con.execute(query).df()

meta = run_query("""
    SELECT
        MIN(ride_date) AS min_date,
        MAX(ride_date) AS max_date
    FROM fact_daily_trips
""")

min_date = meta.loc[0, "min_date"]
max_date = meta.loc[0, "max_date"]

member_options = run_query("""
    SELECT DISTINCT member_casual
    FROM fact_daily_trips
    ORDER BY 1
""")["member_casual"].tolist()

bike_options = run_query("""
    SELECT DISTINCT rideable_type
    FROM fact_daily_trips
    ORDER BY 1
""")["rideable_type"].tolist()

st.sidebar.header("Filters")

date_range = st.sidebar.date_input(
    "Ride date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
)

selected_members = st.sidebar.multiselect(
    "Rider type",
    options=member_options,
    default=member_options,
)

selected_bikes = st.sidebar.multiselect(
    "Bike type",
    options=bike_options,
    default=bike_options,
)

start_date, end_date = date_range

member_sql = ", ".join(f"'{x}'" for x in selected_members)
bike_sql = ", ".join(f"'{x}'" for x in selected_bikes)

base_filter = f"""
WHERE ride_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
  AND member_casual IN ({member_sql})
  AND rideable_type IN ({bike_sql})
"""

kpis = run_query(f"""
    SELECT
        SUM(trip_count) AS total_trips,
        AVG(avg_trip_duration_min) AS avg_trip_duration_min,
        COUNT(DISTINCT ride_date) AS active_days
    FROM fact_daily_trips
    {base_filter}
""")

daily_trips = run_query(f"""
    SELECT
        ride_date,
        SUM(trip_count) AS total_trips
    FROM fact_daily_trips
    {base_filter}
    GROUP BY 1
    ORDER BY 1
""")

member_split = run_query(f"""
    SELECT
        ride_date,
        member_casual,
        SUM(trip_count) AS total_trips
    FROM fact_daily_trips
    {base_filter}
    GROUP BY 1, 2
    ORDER BY 1, 2
""")

bike_type = run_query(f"""
    SELECT
        ride_date,
        rideable_type,
        SUM(trip_count) AS total_trips
    FROM fact_daily_trips
    {base_filter}
    GROUP BY 1, 2
    ORDER BY 1, 2
""")

day_type = run_query(f"""
    SELECT
        day_type,
        SUM(trip_count) AS total_trips
    FROM fact_daily_trips
    {base_filter}
    GROUP BY 1
    ORDER BY 1
""")

st.title("Citi Bike Analytics Dashboard")
st.caption("Local analytics pipeline built with DuckDB and Streamlit")

c1, c2, c3 = st.columns(3)
c1.metric("Total trips", f"{int(kpis.loc[0, 'total_trips']):,}")
c2.metric("Avg trip duration (min)", f"{kpis.loc[0, 'avg_trip_duration_min']:.1f}")
c3.metric("Active days", f"{int(kpis.loc[0, 'active_days'])}")

col1, col2 = st.columns(2)

with col1:
    fig1 = px.line(
        daily_trips,
        x="ride_date",
        y="total_trips",
        title="Daily ridership",
        render_mode="svg",
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.line(
        member_split,
        x="ride_date",
        y="total_trips",
        color="member_casual",
        title="Trips by rider type",
        render_mode="svg",
    )
    st.plotly_chart(fig2, use_container_width=True)

col3, col4 = st.columns(2)

with col3:
    fig3 = px.line(
        bike_type,
        x="ride_date",
        y="total_trips",
        color="rideable_type",
        title="Trips by bike type",
        render_mode="svg",
    )
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    fig4 = px.bar(
        day_type,
        x="day_type",
        y="total_trips",
        color="day_type",
        title="Weekday vs weekend trips",
    )
    st.plotly_chart(fig4, use_container_width=True)