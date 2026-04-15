/*
 * NOTE: This file is superseded by hourly_continuous_aggregate.sql which
 * should be used instead. That file handles time intervals properly by
 * splitting readings that span multiple hours into hourly slices, replicating
 * the logic of the hourly_readings_unit materialized view accurately.
 *
 * This file is kept for reference only, as it demonstrates the basic setup
 * of a TimescaleDB continuous aggregate before the more accurate approach
 * was developed. The key limitation here is that readings are not split
 * across hour boundaries, so a reading spanning multiple hours is assigned
 * entirely to the bucket of its start_timestamp, which produces different
 * results from the materialized view.
 */

-- Create Materialized View with continuous aggregates from hypertable.
CREATE MATERIALIZED VIEW cagg_hourly_readings_unit
WITH (timescaledb.continuous) AS
SELECT
    meter_id,
    AVG(reading) AS reading_rate,
    MAX(reading) AS max_rate,
    MIN(reading) AS min_rate,
    time_bucket(INTERVAL '1 hour', start_timestamp) AS time_interval
FROM readings_hypertable
GROUP BY
    meter_id,
    time_interval
WITH NO DATA;

-- Get earliest and latest timestamps for entire refresh period.
SELECT
    MIN(start_timestamp) AS earliest,
    MAX(start_timestamp) AS latest
FROM readings_hypertable;

-- Refresh continuous aggregates to generate all data from readings_hypertable
-- using earliest and latest timestamps from above.
CALL refresh_continuous_aggregate(
  'cagg_hourly_readings_unit',
  '2020-01-01 00:00:00',
  '2021-12-31 23:45:00'
);

-- View all rows in new continuous aggregates Materialized View.
SELECT * FROM cagg_hourly_readings_unit;

SELECT * FROM hourly_readings_unit;