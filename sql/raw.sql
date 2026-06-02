CREATE OR REPLACE TABLE raw_trips AS
SELECT
    * EXCLUDE(source_filename),
    source_filename
FROM read_csv_auto(
    'data/raw/202401-citibike-tripdata_*.csv',
    union_by_name = true,
    filename = 'source_filename'
);

SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT source_filename) AS files_loaded
FROM raw_trips;

SELECT
    source_filename,
    COUNT(*) AS row_count
FROM raw_trips
GROUP BY 1
ORDER BY 1;