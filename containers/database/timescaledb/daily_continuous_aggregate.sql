-- Create daily continuous aggregate from hourly continuous aggregate.
-- Requires cagg_hourly_readings_unit to exist first (see continuous_aggregates.sql).
-- TimescaleDB supports hierarchical continuous aggregates: a daily CA can source
-- from an hourly CA, which in turn sources from the readings_hypertable.
CREATE MATERIALIZED VIEW cagg_daily_readings_unit
WITH (timescaledb.continuous) AS
SELECT
    meter_id,
    AVG(reading_rate) AS reading_rate,
    MAX(max_rate) AS max_rate,
    MIN(min_rate) AS min_rate,
    time_bucket(INTERVAL '1 day', time_interval) AS time_interval
FROM cagg_hourly_readings_unit
GROUP BY
    meter_id,
    time_bucket(INTERVAL '1 day', time_interval)
WITH NO DATA;

-- Get earliest and latest timestamps for entire refresh period.
SELECT
    MIN(time_interval) AS earliest,
    MAX(time_interval) AS latest
FROM cagg_hourly_readings_unit;

-- Refresh daily continuous aggregate to generate all data from cagg_hourly_readings_unit
-- using earliest and latest timestamps from above.
CALL refresh_continuous_aggregate(
    'cagg_daily_readings_unit',
    '2020-01-01 00:00:00',
    '2021-12-31 23:45:00'
);

-- Add automatic refresh policy: re-materializes data once per day,
-- covering data from 8 days ago up to 1 day ago.
-- start_offset is 1 day longer than hourly to ensure hourly data is settled first.
SELECT add_continuous_aggregate_policy('cagg_daily_readings_unit',
    start_offset    => INTERVAL '8 days',
    end_offset      => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 day',
    if_not_exists   => TRUE
);

-- View all rows in the daily continuous aggregate.
SELECT * FROM cagg_daily_readings_unit;
