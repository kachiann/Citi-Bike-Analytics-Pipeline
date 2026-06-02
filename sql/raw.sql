CREATE OR REPLACE TABLE raw_trips AS
SELECT
    * EXCLUDE(source_filename),
    source_filename
FROM read_csv_auto(
    'data/raw/202401-citibike-tripdata_*.csv',
    union_by_name = true,
    filename = 'source_filename',
    sample_size = -1,
    types = {
        'start_station_id': 'VARCHAR',
        'end_station_id': 'VARCHAR'
    }
);