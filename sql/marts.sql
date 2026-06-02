CREATE OR REPLACE TABLE fact_daily_trips AS
SELECT
    ride_date,
    member_casual,
    rideable_type,
    day_type,
    COUNT(*) AS trip_count,
    AVG(trip_duration_min) AS avg_trip_duration_min
FROM stg_trips
WHERE ride_date >= DATE '2024-01-01'
  AND ride_date <  DATE '2025-01-01'
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2, 3, 4;