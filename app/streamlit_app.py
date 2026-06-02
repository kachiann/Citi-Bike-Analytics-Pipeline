from pathlib import Path
import subprocess
import sys

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Citi Bike Analytics", layout="wide")

APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent
DB_PATH = ROOT_DIR / "data" / "warehouse" / "citibike.duckdb"
BUILD_SCRIPT = ROOT_DIR / "scripts" / "build_warehouse.py"


def ensure_database():
    if DB_PATH.exists():
        return

    if not BUILD_SCRIPT.exists():
        st.error("Warehouse build script not found.")
        st.stop()

    with st.spinner("Building local analytics warehouse..."):
        result = subprocess.run(
            [sys.executable, str(BUILD_SCRIPT)],
            cwd=str(ROOT_DIR),
            capture_output=True,
            text=True,
        )

    if result.returncode != 0:
        st.error("Failed to build DuckDB warehouse.")
        st.code(result.stdout + "\n" + result.stderr)
        st.stop()

    if not DB_PATH.exists():
        st.error("Warehouse build completed, but database file was not created.")
        st.stop()


@st.cache_data(show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        return con.execute(query).df()


ensure_database()


def to_sql_in(values: list[str]) -> str:
    escaped = [value.replace("'", "''") for value in values]
    return ", ".join(f"'{value}'" for value in escaped)


meta = run_query(
    """
    SELECT
        MIN(ride_date) AS min_date,
        MAX(ride_date) AS max_date
    FROM fact_daily_trips
    """
)

min_date = meta.loc[0, "min_date"]
max_date = meta.loc[0, "max_date"]

member_options = run_query(
    """
    SELECT DISTINCT member_casual
    FROM fact_daily_trips
    ORDER BY 1
    """
)["member_casual"].tolist()

bike_options = run_query(
    """
    SELECT DISTINCT rideable_type
    FROM fact_daily_trips
    ORDER BY 1
    """
)["rideable_type"].tolist()

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

if len(date_range) != 2:
    st.warning("Please select a valid start and end date.")
    st.stop()

if not selected_members:
    st.warning("Please select at least one rider type.")
    st.stop()

if not selected_bikes:
    st.warning("Please select at least one bike type.")
    st.stop()

start_date, end_date = date_range
member_sql = to_sql_in(selected_members)
bike_sql = to_sql_in(selected_bikes)

base_filter = f"""
WHERE ride_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
  AND member_casual IN ({member_sql})
  AND rideable_type IN ({bike_sql})
"""

kpis = run_query(
    f"""
    WITH period_bounds AS (
        SELECT
            DATE '{start_date}' AS current_start,
            DATE '{end_date}' AS current_end,
            CAST((DATE '{end_date}' - DATE '{start_date}') + 1 AS INTEGER) AS period_days
    ),
    current_period AS (
        SELECT
            COALESCE(SUM(trip_count), 0) AS total_trips,
            COALESCE(AVG(avg_trip_duration_min), 0) AS avg_trip_duration_min,
            COUNT(DISTINCT ride_date) AS active_days
        FROM fact_daily_trips
        WHERE ride_date BETWEEN DATE '{start_date}' AND DATE '{end_date}'
          AND member_casual IN ({member_sql})
          AND rideable_type IN ({bike_sql})
    ),
    previous_period AS (
        SELECT
            COALESCE(SUM(trip_count), 0) AS total_trips_prev
        FROM fact_daily_trips, period_bounds
        WHERE ride_date BETWEEN current_start - period_days AND current_start - 1
          AND member_casual IN ({member_sql})
          AND rideable_type IN ({bike_sql})
    )
    SELECT *
    FROM current_period, previous_period
    """
)

daily_trips = run_query(
    f"""
    WITH daily AS (
        SELECT
            ride_date,
            SUM(trip_count) AS total_trips
        FROM fact_daily_trips
        {base_filter}
        GROUP BY 1
    )
    SELECT
        ride_date,
        total_trips,
        AVG(total_trips) OVER (
            ORDER BY ride_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS trips_7d_ma
    FROM daily
    ORDER BY 1
    """
)

monthly_trips = run_query(
    f"""
    SELECT
        date_trunc('month', ride_date) AS ride_month,
        SUM(trip_count) AS total_trips
    FROM fact_daily_trips
    {base_filter}
    GROUP BY 1
    ORDER BY 1
    """
)

member_split = run_query(
    f"""
    SELECT
        ride_date,
        member_casual,
        SUM(trip_count) AS total_trips
    FROM fact_daily_trips
    {base_filter}
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
)

bike_type = run_query(
    f"""
    SELECT
        ride_date,
        rideable_type,
        SUM(trip_count) AS total_trips
    FROM fact_daily_trips
    {base_filter}
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
)

day_type = run_query(
    f"""
    SELECT
        day_type,
        SUM(trip_count) AS total_trips
    FROM fact_daily_trips
    {base_filter}
    GROUP BY 1
    ORDER BY 1
    """
)

extract_df = run_query(
    f"""
    SELECT
        ride_date,
        member_casual,
        rideable_type,
        day_type,
        trip_count,
        ROUND(avg_trip_duration_min, 2) AS avg_trip_duration_min
    FROM fact_daily_trips
    {base_filter}
    ORDER BY ride_date, member_casual, rideable_type
    """
)

st.title("Citi Bike Analytics Dashboard")
st.caption("Local 2024 Citi Bike analytics pipeline using DuckDB, SQL, Streamlit, and Plotly")

st.info(
    f"Current filters — Dates: {start_date} to {end_date} | "
    f"Rider types: {', '.join(selected_members)} | "
    f"Bike types: {', '.join(selected_bikes)}"
)

curr_total = float(kpis.loc[0, "total_trips"])
prev_total = float(kpis.loc[0, "total_trips_prev"])
avg_duration = float(kpis.loc[0, "avg_trip_duration_min"])
active_days = int(kpis.loc[0, "active_days"])

delta_text = None
if prev_total > 0:
    delta_pct = ((curr_total - prev_total) / prev_total) * 100
    delta_text = f"{delta_pct:.1f}% vs previous period"

c1, c2, c3 = st.columns(3)
c1.metric("Total trips", f"{int(curr_total):,}", delta_text)
c2.metric("Avg trip duration (min)", f"{avg_duration:.1f}")
c3.metric("Active days", f"{active_days}")

tab1, tab2, tab3 = st.tabs(["Overview", "Segments", "Data extract"])

with tab1:
    col1, col2 = st.columns(2)

    with col1:
        fig_daily = px.line(
            daily_trips,
            x="ride_date",
            y=["total_trips", "trips_7d_ma"],
            title="Daily ridership and 7-day moving average",
            labels={"value": "Trips", "ride_date": "Ride date", "variable": "Series"},
            render_mode="svg",
        )
        fig_daily.update_layout(legend_title_text="")
        st.plotly_chart(fig_daily, use_container_width=True)

    with col2:
        fig_day_type = px.bar(
            day_type,
            x="day_type",
            y="total_trips",
            color="day_type",
            title="Weekday vs weekend trips",
            labels={"day_type": "Day type", "total_trips": "Trips"},
        )
        fig_day_type.update_layout(showlegend=False)
        st.plotly_chart(fig_day_type, use_container_width=True)

    fig_monthly = px.bar(
        monthly_trips,
        x="ride_month",
        y="total_trips",
        title="Monthly ridership",
        labels={"ride_month": "Month", "total_trips": "Trips"},
    )
    st.plotly_chart(fig_monthly, use_container_width=True)

with tab2:
    col3, col4 = st.columns(2)

    with col3:
        fig_member = px.line(
            member_split,
            x="ride_date",
            y="total_trips",
            color="member_casual",
            title="Trips by rider type",
            labels={
                "ride_date": "Ride date",
                "total_trips": "Trips",
                "member_casual": "Rider type",
            },
            render_mode="svg",
        )
        st.plotly_chart(fig_member, use_container_width=True)

    with col4:
        fig_bike = px.line(
            bike_type,
            x="ride_date",
            y="total_trips",
            color="rideable_type",
            title="Trips by bike type",
            labels={
                "ride_date": "Ride date",
                "total_trips": "Trips",
                "rideable_type": "Bike type",
            },
            render_mode="svg",
        )
        st.plotly_chart(fig_bike, use_container_width=True)

with tab3:
    st.dataframe(extract_df, use_container_width=True)
    csv_bytes = extract_df.to_csv(index=False).encode("utf-8")
    st.download_button(
        label="Download filtered data as CSV",
        data=csv_bytes,
        file_name="citibike_filtered_extract.csv",
        mime="text/csv",
    )