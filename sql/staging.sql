CREATE OR REPLACE TABLE stg_trips AS
SELECT
    ride_id,
    rideable_type,
    TRY_CAST(started_at AS TIMESTAMP) AS started_at,
    TRY_CAST(ended_at AS TIMESTAMP) AS ended_at,
    CAST(TRY_CAST(started_at AS TIMESTAMP) AS DATE) AS ride_date,
    date_part('year', TRY_CAST(started_at AS TIMESTAMP)) AS ride_year,
    date_part('month', TRY_CAST(started_at AS TIMESTAMP)) AS ride_month,
    date_part('dow', TRY_CAST(started_at AS TIMESTAMP)) AS ride_dow,
    CASE
        WHEN date_part('dow', TRY_CAST(started_at AS TIMESTAMP)) IN (0, 6) THEN 'weekend'
        ELSE 'weekday'
    END AS day_type,
    start_station_name,
    end_station_name,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    member_casual,
    source_filename,
    date_diff('minute', TRY_CAST(started_at AS TIMESTAMP), TRY_CAST(ended_at AS TIMESTAMP)) AS trip_duration_min
FROM raw_trips
WHERE TRY_CAST(started_at AS TIMESTAMP) IS NOT NULL
  AND TRY_CAST(ended_at AS TIMESTAMP) IS NOT NULL
  AND TRY_CAST(ended_at AS TIMESTAMP) >= TRY_CAST(started_at AS TIMESTAMP);