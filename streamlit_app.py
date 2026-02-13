import pandas as pd
import streamlit as st
from google.cloud import bigquery

# -----------------------------
# Config
# -----------------------------
st.set_page_config(page_title="Citi Bike Analytics", layout="wide")

PROJECT = "dezoomcamp-citibike-free"
TABLE = f"`{PROJECT}.marts.fact_trips`"

client = bigquery.Client(project=PROJECT)

@st.cache_data(ttl=3600)
def query_bq(sql: str) -> pd.DataFrame:
    return client.query(sql).to_dataframe()

def fmt_int(x):
    try:
        return f"{int(x):,}"
    except Exception:
        return str(x)

def fmt_seconds_to_min(sec):
    try:
        return f"{(float(sec)/60):.1f} min"
    except Exception:
        return str(sec)

def pct_change(curr, prev):
    try:
        curr = float(curr)
        prev = float(prev)
        if prev == 0:
            return None
        return (curr - prev) / prev
    except Exception:
        return None

# -----------------------------
# Header
# -----------------------------
st.title("🚲 Citi Bike Analytics Dashboard")
st.write(
    """
This dashboard is built on a BigQuery warehouse table (`marts.fact_trips`) populated from
NYC Citi Bike trip history files (Batch pipeline → GCS → BigQuery).
Use the filters to explore rider behavior and time trends.
"""
)

# -----------------------------
# Sidebar filters
# -----------------------------
st.sidebar.header("Filters")

default_start = pd.to_datetime("2024-01-01").date()
default_end = pd.to_datetime("2024-12-31").date()

start_date = st.sidebar.date_input("Start date", value=default_start)
end_date = st.sidebar.date_input("End date", value=default_end)

if start_date > end_date:
    st.error("Start date must be before end date.")
    st.stop()

days_selected = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days
default_grain = "Monthly" if days_selected > 45 else "Daily"
grain = st.sidebar.radio("Time grain", options=["Daily", "Monthly"], index=0 if default_grain == "Daily" else 1)

# Get filter values from BQ (cached)
@st.cache_data(ttl=3600)
def get_distinct_values():
    sql = f"""
    SELECT
      ARRAY_AGG(DISTINCT member_casual IGNORE NULLS) AS rider_types,
      ARRAY_AGG(DISTINCT rideable_type IGNORE NULLS) AS bike_types
    FROM {TABLE}
    """
    df = query_bq(sql)
    rider_types = sorted(df.loc[0, "rider_types"]) if "rider_types" in df.columns else []
    bike_types = sorted(df.loc[0, "bike_types"]) if "bike_types" in df.columns else []
    return rider_types, bike_types

rider_types, bike_types = get_distinct_values()

rider_filter = st.sidebar.multiselect(
    "Rider type",
    options=rider_types,
    default=rider_types
)

bike_filter = st.sidebar.multiselect(
    "Bike type",
    options=bike_types,
    default=bike_types
)

# Build WHERE clauses (partition filter ALWAYS included)
where_clauses = [f"ride_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')"]

if rider_filter:
    rider_in = ", ".join([f"'{x}'" for x in rider_filter])
    where_clauses.append(f"member_casual IN ({rider_in})")

if bike_filter:
    bike_in = ", ".join([f"'{x}'" for x in bike_filter])
    where_clauses.append(f"rideable_type IN ({bike_in})")

where_sql = " AND ".join(where_clauses)

# -----------------------------
# KPI row
# -----------------------------
sql_kpis = f"""
SELECT
  COUNT(*) AS trips,
  AVG(ride_duration_sec) AS avg_duration_sec,
  SAFE_DIVIDE(SUM(CASE WHEN member_casual = 'member' THEN 1 ELSE 0 END), COUNT(*)) AS member_share,
  SAFE_DIVIDE(SUM(CASE WHEN LOWER(rideable_type) LIKE '%electric%' THEN 1 ELSE 0 END), COUNT(*)) AS electric_share,
  (SELECT AS STRUCT rideable_type, COUNT(*) c
   FROM {TABLE}
   WHERE {where_sql}
   GROUP BY rideable_type
   ORDER BY c DESC
   LIMIT 1) AS top_bike
FROM {TABLE}
WHERE {where_sql}
"""
kpi = query_bq(sql_kpis).iloc[0]

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total trips", fmt_int(kpi["trips"]))
col2.metric("Avg duration", fmt_seconds_to_min(kpi["avg_duration_sec"]))
col3.metric("Member share", f"{(float(kpi['member_share']) * 100):.1f}%")
col4.metric("Electric share", f"{(float(kpi['electric_share']) * 100):.1f}%")
top_bike_type = kpi["top_bike"]["rideable_type"] if kpi["top_bike"] is not None else "N/A"
top_bike_count = kpi["top_bike"]["c"] if kpi["top_bike"] is not None else 0
col5.metric("Top bike type", f"{top_bike_type} ({fmt_int(top_bike_count)})")

# -----------------------------
# MoM KPI (Month over Month)
# -----------------------------
sql_mom = f"""
WITH monthly AS (
  SELECT
    DATE_TRUNC(ride_date, MONTH) AS month,
    COUNT(*) AS trips
  FROM {TABLE}
  WHERE ride_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')
  GROUP BY month
),
last_two AS (
  SELECT * FROM monthly ORDER BY month DESC LIMIT 2
)
SELECT
  (SELECT month FROM last_two ORDER BY month DESC LIMIT 1) AS curr_month,
  (SELECT trips FROM last_two ORDER BY month DESC LIMIT 1) AS curr_trips,
  (SELECT month FROM last_two ORDER BY month DESC LIMIT 1 OFFSET 1) AS prev_month,
  (SELECT trips FROM last_two ORDER BY month DESC LIMIT 1 OFFSET 1) AS prev_trips
"""
df_mom = query_bq(sql_mom)

mom_left, mom_right = st.columns([1, 2])
if df_mom.empty or df_mom.iloc[0]["prev_trips"] is None:
    mom_left.metric("MoM change (trips)", "N/A")
else:
    curr_trips = df_mom.iloc[0]["curr_trips"]
    prev_trips = df_mom.iloc[0]["prev_trips"]
    mom = pct_change(curr_trips, prev_trips)
    mom_left.metric(
        "MoM change (trips)",
        f"{mom*100:.1f}%" if mom is not None else "N/A",
        delta=f"{int(curr_trips - prev_trips):,} trips"
    )
mom_right.caption("MoM compares the last month in the selected range to the previous month.")

st.divider()

# -----------------------------
# Charts row 1
# -----------------------------
left, right = st.columns([1, 1])

# Q2: Member vs Casual distribution
with left:
    st.subheader("Trips by Rider Type (Member vs Casual)")
    sql_cat = f"""
    SELECT
      member_casual,
      COUNT(*) AS trip_count
    FROM {TABLE}
    WHERE {where_sql}
    GROUP BY member_casual
    ORDER BY trip_count DESC
    """
    df_cat = query_bq(sql_cat)
    if df_cat.empty:
        st.info("No data for the selected filters.")
    else:
        df_cat_display = df_cat.copy()
        df_cat_display["trip_count"] = df_cat_display["trip_count"].map(fmt_int)
        st.dataframe(df_cat_display, use_container_width=True, hide_index=True)
        st.bar_chart(df_cat.set_index("member_casual")[["trip_count"]])

# Q1: Ridership over time (Daily or Monthly)
with right:
    title = "Trips Over Time (Daily)" if grain == "Daily" else "Trips Over Time (Monthly)"
    st.subheader(title)

    if grain == "Daily":
        sql_ts = f"""
        SELECT
          ride_date AS period,
          COUNT(*) AS trip_count
        FROM {TABLE}
        WHERE {where_sql}
        GROUP BY period
        ORDER BY period
        """
    else:
        sql_ts = f"""
        SELECT
          DATE_TRUNC(ride_date, MONTH) AS period,
          COUNT(*) AS trip_count
        FROM {TABLE}
        WHERE {where_sql}
        GROUP BY period
        ORDER BY period
        """

    df_ts = query_bq(sql_ts)
    if df_ts.empty:
        st.info("No data for the selected filters.")
    else:
        st.line_chart(df_ts.set_index("period")[["trip_count"]])

        with st.expander("Show table"):
            df_ts_display = df_ts.copy()
            df_ts_display["trip_count"] = df_ts_display["trip_count"].map(fmt_int)
            st.dataframe(df_ts_display, use_container_width=True, hide_index=True)

st.divider()

# -----------------------------
# Q3: Electric vs Classic / Bike type mix over time (Monthly)
# -----------------------------
st.subheader("Bike Type Usage Trends Over Time")
st.caption("Monthly trip counts split by `rideable_type` (e.g., classic vs electric).")

sql_mix = f"""
SELECT
  DATE_TRUNC(ride_date, MONTH) AS month,
  rideable_type,
  COUNT(*) AS trip_count
FROM {TABLE}
WHERE {where_sql}
GROUP BY month, rideable_type
ORDER BY month, rideable_type
"""
df_mix = query_bq(sql_mix)

if df_mix.empty:
    st.info("No data for the selected filters.")
else:
    mix_pivot = df_mix.pivot(index="month", columns="rideable_type", values="trip_count").fillna(0)
    st.line_chart(mix_pivot)

    with st.expander("Show bike mix table"):
        df_mix_display = df_mix.copy()
        df_mix_display["trip_count"] = df_mix_display["trip_count"].map(fmt_int)
        st.dataframe(df_mix_display, use_container_width=True, hide_index=True)

st.divider()

# -----------------------------
# Weekday vs Weekend split
# -----------------------------
st.subheader("Weekday vs Weekend Split")

sql_weekday = f"""
SELECT
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM ride_date) IN (1,7) THEN 'Weekend'
    ELSE 'Weekday'
  END AS day_type,
  COUNT(*) AS trip_count,
  ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_share
FROM {TABLE}
WHERE {where_sql}
GROUP BY day_type
ORDER BY trip_count DESC
"""
df_weekday = query_bq(sql_weekday)

if df_weekday.empty:
    st.info("No data for the selected filters.")
else:
    lw, rw = st.columns([1, 1])
    with lw:
        st.dataframe(df_weekday, use_container_width=True, hide_index=True)
    with rw:
        st.bar_chart(df_weekday.set_index("day_type")[["trip_count"]])

st.divider()

# -----------------------------
# Top stations
# -----------------------------
st.subheader("Top Start Stations")

sql_top_stations = f"""
SELECT
  start_station_name,
  COUNT(*) AS trip_count
FROM {TABLE}
WHERE {where_sql}
  AND start_station_name IS NOT NULL
GROUP BY start_station_name
ORDER BY trip_count DESC
LIMIT 10
"""
df_top = query_bq(sql_top_stations)

if df_top.empty:
    st.info("No station data for the selected filters.")
else:
    df_top_display = df_top.copy()
    df_top_display["trip_count"] = df_top_display["trip_count"].map(fmt_int)
    st.dataframe(df_top_display, use_container_width=True, hide_index=True)

st.divider()

# -----------------------------
# Insights box (auto bullets, deterministic)
# -----------------------------
st.subheader("Insights")

sql_insights = f"""
WITH base AS (
  SELECT
    COUNT(*) AS trips,
    AVG(ride_duration_sec) AS avg_dur,
    SAFE_DIVIDE(SUM(CASE WHEN member_casual='member' THEN 1 ELSE 0 END), COUNT(*)) AS member_share
  FROM {TABLE}
  WHERE {where_sql}
),
bike_mix AS (
  SELECT
    rideable_type,
    COUNT(*) AS trips
  FROM {TABLE}
  WHERE {where_sql}
  GROUP BY rideable_type
  ORDER BY trips DESC
),
top_bike AS (
  SELECT AS STRUCT rideable_type, trips
  FROM bike_mix
  LIMIT 1
),
weekend AS (
  SELECT
    SAFE_DIVIDE(
      SUM(CASE WHEN EXTRACT(DAYOFWEEK FROM ride_date) IN (1,7) THEN 1 ELSE 0 END),
      COUNT(*)
    ) AS weekend_share
  FROM {TABLE}
  WHERE {where_sql}
)
SELECT
  base.trips,
  base.avg_dur,
  base.member_share,
  weekend.weekend_share,
  top_bike.rideable_type AS top_bike_type,
  top_bike.trips AS top_bike_trips
FROM base, weekend, top_bike
"""
ins = query_bq(sql_insights).iloc[0]

trips = int(ins["trips"])
avg_dur_min = float(ins["avg_dur"]) / 60.0 if ins["avg_dur"] is not None else None
member_share = float(ins["member_share"]) * 100 if ins["member_share"] is not None else None
weekend_share = float(ins["weekend_share"]) * 100 if ins["weekend_share"] is not None else None
top_bike_type = ins["top_bike_type"]
top_bike_trips = int(ins["top_bike_trips"]) if ins["top_bike_trips"] is not None else None

bullets = [
    f"Total trips in selected period: **{trips:,}**."
]

if avg_dur_min is not None:
    bullets.append(f"Average trip duration: **{avg_dur_min:.1f} minutes**.")

if member_share is not None:
    bullets.append(f"Rider mix: **{member_share:.1f}% members** (and {100-member_share:.1f}% casual).")

if weekend_share is not None:
    bullets.append(f"Trip timing: **{weekend_share:.1f}%** of trips occur on weekends.")

if top_bike_type is not None and top_bike_trips is not None:
    bullets.append(f"Most used bike type: **{top_bike_type}** with **{top_bike_trips:,}** trips.")

# Simple trend bullet if there’s enough range
if days_selected >= 60:
    sql_trend = f"""
    WITH monthly AS (
      SELECT DATE_TRUNC(ride_date, MONTH) AS month, COUNT(*) AS trips
      FROM {TABLE}
      WHERE ride_date BETWEEN DATE('{start_date}') AND DATE('{end_date}')
      GROUP BY month
    )
    SELECT
      (SELECT trips FROM monthly ORDER BY month ASC LIMIT 1) AS first_trips,
      (SELECT trips FROM monthly ORDER BY month DESC LIMIT 1) AS last_trips
    """
    t = query_bq(sql_trend).iloc[0]
    if t["first_trips"] is not None and t["last_trips"] is not None:
        first_trips = int(t["first_trips"])
        last_trips = int(t["last_trips"])
        delta = last_trips - first_trips
        direction = "increased" if delta > 0 else "decreased" if delta < 0 else "stayed flat"
        bullets.append(f"Overall trend: trips **{direction}** from first to last month (Δ {delta:,}).")

st.info("\n".join([f"• {b}" for b in bullets]))

st.caption(
    "Warehouse: BigQuery `marts.fact_trips` (partitioned by ride_date, clustered by member_casual and rideable_type)."
)
