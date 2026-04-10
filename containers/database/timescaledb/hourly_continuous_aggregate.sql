/*
 * This file sets up the necessary database objects to benchmark TimescaleDB
 * continuous aggregates against the existing PostgreSQL materialized views
 * for hourly meter readings.
 *
 * It creates a hypertable (hypertable_hourly_split) that splits raw meter
 * readings from readings_hypertable into hourly intervals, replicating the
 * logic used in the existing hourly_readings_unit materialized view. A
 * continuous aggregate (hourly_readings_unit_cagg) is then built on top of
 * this hypertable to replicate the materialized view as accurately as possible.
 *
 * The goal is to compare query speeds between the original materialized views
 * and TimescaleDB continuous aggregates to determine whether migrating to
 * TimescaleDB is worth the effort.
 *
 * Setup steps:
 * 1. Ensure the TimescaleDB extension is enabled in the oed database
 * 2. Ensure readings_hypertable is populated with data (npm run testData)
 * 3. Run this file in pgAdmin against the oed database
 *
 * To tear down all objects created by this file:
 *     DROP MATERIALIZED VIEW IF EXISTS hourly_readings_unit_cagg;
 *     DROP TABLE IF EXISTS hypertable_hourly_split;
 */

-- 1. Create an hourly split hypertable to store readings split into hourly intervals.
-- unit_represent and sec_in_rate are included so the continuous aggregate can
-- handle quantity, flow, and raw readings differently without needing to join
-- to the meters and units tables at query time.
CREATE TABLE hypertable_hourly_split (
    meter_id INTEGER NOT NULL,
    reading FLOAT NOT NULL,
    start_timestamp TIMESTAMP NOT NULL,
    end_timestamp TIMESTAMP NOT NULL,
    unit_represent unit_represent_type NOT NULL,
    sec_in_rate FLOAT NOT NULL);

-- 2. Convert the table into a TimescaleDB hypertable, partitioned by start_timestamp.
-- This enables time-series optimizations and allows continuous aggregates to be built on top.
SELECT create_hypertable('hypertable_hourly_split', 'start_timestamp');

-- 3. Populate the hypertable by splitting each raw reading from readings_hypertable
-- into one row per hour it spans. For example, a reading spanning 3 hours becomes 3 rows,
-- each representing the portion of the reading that falls within that hour.
-- Quantity readings are scaled proportionally by overlap duration.
-- Flow/raw readings are already a rate, so they are normalized to per hour using sec_in_rate.
INSERT INTO hypertable_hourly_split
SELECT
    r.meter_id,
    CASE WHEN u.unit_represent = 'quantity'::unit_represent_type THEN
        r.reading *
        extract(EPOCH FROM (
            least(r.end_timestamp, gen.interval_start + '1 hour'::INTERVAL)
            - greatest(r.start_timestamp, gen.interval_start)
        )) / extract(EPOCH FROM (r.end_timestamp - r.start_timestamp))
    WHEN (u.unit_represent = 'flow'::unit_represent_type OR u.unit_represent = 'raw'::unit_represent_type) THEN
        r.reading * 3600 / u.sec_in_rate
    END AS reading,
    greatest(r.start_timestamp, gen.interval_start) AS start_timestamp,
    least(r.end_timestamp, gen.interval_start + '1 hour'::INTERVAL) AS end_timestamp,
    u.unit_represent,
    u.sec_in_rate
FROM ((readings_hypertable r
    INNER JOIN meters m ON r.meter_id = m.id)
    INNER JOIN units u ON m.unit_id = u.id)
    CROSS JOIN LATERAL generate_series(
        date_trunc('hour', r.start_timestamp),
        date_trunc_up('hour', r.end_timestamp) - '1 hour'::INTERVAL,
        '1 hour'::INTERVAL
    ) gen(interval_start);

-- 4. Verify row count of the hourly split hypertable.
-- Will be higher than hourly_readings_unit since each raw reading
-- can produce multiple rows before aggregation.
SELECT COUNT(*) FROM hypertable_hourly_split;

-- 5. Verify row count of the original materialized view.
-- Will be lower since it aggregates to one row per meter per hour.
SELECT COUNT(*) FROM hourly_readings_unit;

-- 6. Check how many distinct (meter, hour) combinations exist in the hypertable.
-- Should match the count from hourly_readings_unit below if data transferred correctly.
SELECT COUNT(*) FROM (
    SELECT DISTINCT meter_id, date_trunc('hour', start_timestamp)
    FROM hypertable_hourly_split) s;

-- 7. Check how many distinct (meter, hour) combinations exist in the materialized view.
-- Should match the count from hypertable_hourly_split above if data transferred correctly.
SELECT COUNT(*) FROM (
    SELECT DISTINCT meter_id, lower(time_interval)
    FROM hourly_readings_unit) s;

-- 8. Create a continuous aggregate on top of hypertable_hourly_split.
-- This replicates the logic of hourly_readings_unit but uses TimescaleDB's
-- continuous aggregate mechanism instead of a regular materialized view,
-- allowing for automatic incremental refresh as new data arrives.
CREATE MATERIALIZED VIEW hourly_readings_unit_cagg
WITH (timescaledb.continuous) AS
SELECT
    meter_id,
    time_bucket('1 hour', start_timestamp) AS bucket,
    -- Compute weighted average reading rate, matching the logic in hourly_readings_unit.
    -- quantity readings are converted to a rate per hour using overlap duration as the weight.
    -- flow/raw readings are already normalized to per hour in hypertable_hourly_split.
    CASE WHEN unit_represent = 'quantity'::unit_represent_type THEN
        sum(reading * 3600 / extract(EPOCH FROM (end_timestamp - start_timestamp))
            * extract(EPOCH FROM (end_timestamp - start_timestamp))
        ) / sum(extract(EPOCH FROM (end_timestamp - start_timestamp)))
    WHEN (unit_represent = 'flow'::unit_represent_type OR unit_represent = 'raw'::unit_represent_type) THEN
        sum(reading * extract(EPOCH FROM (end_timestamp - start_timestamp))
        ) / sum(extract(EPOCH FROM (end_timestamp - start_timestamp)))
    END AS reading_rate,
    -- Convert reading to rate before taking max/min, matching the logic in hourly_readings_unit
    CASE WHEN unit_represent = 'quantity'::unit_represent_type THEN
        max(reading * 3600 / extract(EPOCH FROM (end_timestamp - start_timestamp)))
    WHEN (unit_represent = 'flow'::unit_represent_type OR unit_represent = 'raw'::unit_represent_type) THEN
        max(reading)
    END AS max_rate,
    CASE WHEN unit_represent = 'quantity'::unit_represent_type THEN
        min(reading * 3600 / extract(EPOCH FROM (end_timestamp - start_timestamp)))
    WHEN (unit_represent = 'flow'::unit_represent_type OR unit_represent = 'raw'::unit_represent_type) THEN
        min(reading)
    END AS min_rate,
    unit_represent,
    sec_in_rate
FROM hypertable_hourly_split
GROUP BY meter_id, bucket, unit_represent, sec_in_rate;

-- 9. Compare reading_rate values between the original materialized view and the continuous aggregate.
-- Results are ordered by largest difference first to surface any inaccuracies.
-- Differences should be zero or extremely close to zero (floating point rounding only).
SELECT
    mv.meter_id,
    lower(mv.time_interval) AS mv_time,
    mv.reading_rate AS mv_reading_rate,
    cagg.bucket AS cagg_time,
    cagg.reading_rate AS cagg_reading_rate,
    mv.reading_rate - cagg.reading_rate AS difference
FROM hourly_readings_unit mv
INNER JOIN hourly_readings_unit_cagg cagg
    ON mv.meter_id = cagg.meter_id
    AND lower(mv.time_interval) = cagg.bucket
ORDER BY abs(mv.reading_rate - cagg.reading_rate) DESC
LIMIT 20;

-- Verify accuracy of max_rate and min_rate between the original materialized
-- view (hourly_readings_unit) and the continuous aggregate (hourly_readings_unit_cagg).
-- Results are ordered by largest max_rate difference first to surface any inaccuracies.
-- Differences should be zero or extremely close to zero (floating point rounding only).
SELECT
    mv.meter_id,
    lower(mv.time_interval) AS mv_time,
    mv.max_rate AS mv_max_rate,
    cagg.max_rate AS cagg_max_rate,
    mv.max_rate - cagg.max_rate AS max_difference,
    mv.min_rate AS mv_min_rate,
    cagg.min_rate AS cagg_min_rate,
    mv.min_rate - cagg.min_rate AS min_difference
FROM hourly_readings_unit mv
INNER JOIN hourly_readings_unit_cagg cagg
    ON mv.meter_id = cagg.meter_id
    AND lower(mv.time_interval) = cagg.bucket
ORDER BY abs(mv.max_rate - cagg.max_rate) DESC
LIMIT 20;