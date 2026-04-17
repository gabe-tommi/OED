/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

/*
 * SUPERSEDED — the continuous aggregate implementation has moved.
 *
 * The hourly and daily continuous aggregates are now implemented in:
 *   containers/database/timescaledb/continuous_aggregates.sql  (hourly)
 *   containers/database/timescaledb/daily_continuous_aggregate.sql (daily)
 *
 * These source from readings_hypertable (hourly) and cagg_hourly_readings_unit (daily),
 * using TimescaleDB hierarchical continuous aggregates rather than converting the
 * existing OED materialized views.
 *
 * Why the original approach in this file didn't work:
 *
 * 1. hourly_readings_unit uses CROSS JOIN LATERAL generate_series() to split readings
 *    that span multiple hours into separate buckets. TimescaleDB continuous aggregates
 *    do not support LATERAL joins, so hourly must stay as a regular materialized view.
 *
 * 2. daily_readings_unit is built on top of hourly_readings_unit. TimescaleDB continuous
 *    aggregates can only source from a hypertable or another continuous aggregate — not
 *    from a regular materialized view. So daily can't be a continuous aggregate either
 *    as long as it depends on hourly.
 *
 * 3. group_daily_readings_unit also uses a LATERAL join (unnest + get_graphic_unit),
 *    which has the same restriction.
 *
 * The new approach sidesteps these restrictions by building a parallel set of CAs
 * (cagg_hourly_readings_unit, cagg_daily_readings_unit) that aggregate directly from
 * readings_hypertable, independent of the existing OED views.
 *
 * The script below is kept for reference only and should NOT be run.
 */

-- Step 1: Check if TimescaleDB extension is available
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        RAISE EXCEPTION 'TimescaleDB extension not installed. Run timescaledb_migration.sql first.';
    END IF;
    RAISE NOTICE 'TimescaleDB extension found';
END $$;

-- Step 2: Drop existing daily_readings_unit if it's a regular materialized view
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_matviews
        WHERE schemaname = 'public'
        AND matviewname = 'daily_readings_unit'
    ) AND NOT EXISTS (
        SELECT 1
        FROM timescaledb_information.continuous_aggregates
        WHERE view_name = 'daily_readings_unit'
    ) THEN
        DROP MATERIALIZED VIEW daily_readings_unit CASCADE;
        RAISE NOTICE 'Dropped existing daily_readings_unit materialized view';
    ELSIF EXISTS (
        SELECT 1
        FROM timescaledb_information.continuous_aggregates
        WHERE view_name = 'daily_readings_unit'
    ) THEN
        RAISE NOTICE 'daily_readings_unit is already a continuous aggregate - skipping';
    END IF;
END $$;

-- Step 3: Create daily_readings_unit as continuous aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_readings_unit
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 day', lower(h.time_interval)) AS day_bucket,
    h.meter_id,
    AVG(h.reading_rate) AS reading_rate,
    MAX(h.max_rate) AS max_rate,
    MIN(h.min_rate) AS min_rate,
    tsrange(
        time_bucket(INTERVAL '1 day', lower(h.time_interval)),
        time_bucket(INTERVAL '1 day', lower(h.time_interval)) + INTERVAL '1 day',
        '()'
    ) AS time_interval
FROM hourly_readings_unit h
GROUP BY day_bucket, h.meter_id;

RAISE NOTICE 'Created/verified daily_readings_unit continuous aggregate';

-- Step 4: Create index for daily_readings_unit
CREATE INDEX IF NOT EXISTS idx_daily_readings_unit
ON daily_readings_unit USING GIST(time_interval, meter_id);

RAISE NOTICE 'Created/verified index on daily_readings_unit';

-- Step 5: Drop existing group_daily_readings_unit if it's a regular materialized view
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_matviews
        WHERE schemaname = 'public'
        AND matviewname = 'group_daily_readings_unit'
    ) AND NOT EXISTS (
        SELECT 1
        FROM timescaledb_information.continuous_aggregates
        WHERE view_name = 'group_daily_readings_unit'
    ) THEN
        DROP MATERIALIZED VIEW group_daily_readings_unit CASCADE;
        RAISE NOTICE 'Dropped existing group_daily_readings_unit materialized view';
    ELSIF EXISTS (
        SELECT 1
        FROM timescaledb_information.continuous_aggregates
        WHERE view_name = 'group_daily_readings_unit'
    ) THEN
        RAISE NOTICE 'group_daily_readings_unit is already a continuous aggregate - skipping';
    END IF;
END $$;

-- Step 6: Create group_daily_readings_unit as continuous aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS group_daily_readings_unit
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 day', lower(dr.time_interval)) AS day_bucket,
    gdm.group_id,
    SUM(dr.reading_rate * c.slope + c.intercept) AS reading_rate,
    gu.graphic_unit_id,
    tsrange(
        time_bucket(INTERVAL '1 day', lower(dr.time_interval)),
        time_bucket(INTERVAL '1 day', lower(dr.time_interval)) + INTERVAL '1 day',
        '()'
    ) AS time_interval
FROM daily_readings_unit dr
INNER JOIN groups_deep_meters gdm ON dr.meter_id = gdm.meter_id
INNER JOIN meters m ON m.id = dr.meter_id
INNER JOIN units u ON m.unit_id = u.id
INNER JOIN cik c ON c.source_id = m.unit_id
INNER JOIN LATERAL (
    SELECT graphic_unit_id
    FROM unnest(get_graphic_unit(gdm.group_id)) AS gu(graphic_unit_id)
) gu ON c.destination_id = gu.graphic_unit_id
GROUP BY day_bucket, gdm.group_id, gu.graphic_unit_id
ORDER BY day_bucket, gu.graphic_unit_id, gdm.group_id;

RAISE NOTICE 'Created/verified group_daily_readings_unit continuous aggregate';

-- Step 7: Create index for group_daily_readings_unit
CREATE INDEX IF NOT EXISTS idx_group_daily_readings_unit
ON group_daily_readings_unit USING GIST(time_interval, graphic_unit_id, group_id);

RAISE NOTICE 'Created/verified index on group_daily_readings_unit';

-- Step 8: Initial refresh of continuous aggregates
DO $$
BEGIN
    RAISE NOTICE 'Performing initial refresh of daily_readings_unit...';
    CALL refresh_continuous_aggregate('daily_readings_unit', NULL, NULL);
    RAISE NOTICE 'Completed refresh of daily_readings_unit';

    RAISE NOTICE 'Performing initial refresh of group_daily_readings_unit...';
    CALL refresh_continuous_aggregate('group_daily_readings_unit', NULL, NULL);
    RAISE NOTICE 'Completed refresh of group_daily_readings_unit';
END $$;

-- Step 9: Add automatic refresh policies
-- These policies automatically refresh the continuous aggregates on a schedule
DO $$
BEGIN
    -- Add policy for daily_readings_unit
    -- Refresh data from 7 days ago up to 1 hour ago, once per day
    IF NOT EXISTS (
        SELECT 1
        FROM timescaledb_information.jobs j
        WHERE j.config::json->>'mat_hypertable_id' = (
            SELECT materialization_hypertable_schema || '.' || materialization_hypertable_name
            FROM timescaledb_information.continuous_aggregates
            WHERE view_name = 'daily_readings_unit'
        )
    ) THEN
        PERFORM add_continuous_aggregate_policy('daily_readings_unit',
            start_offset => INTERVAL '7 days',
            end_offset => INTERVAL '1 hour',
            schedule_interval => INTERVAL '1 day',
            if_not_exists => TRUE
        );
        RAISE NOTICE 'Added refresh policy for daily_readings_unit';
    ELSE
        RAISE NOTICE 'Refresh policy for daily_readings_unit already exists';
    END IF;

    -- Add policy for group_daily_readings_unit
    IF NOT EXISTS (
        SELECT 1
        FROM timescaledb_information.jobs j
        WHERE j.config::json->>'mat_hypertable_id' = (
            SELECT materialization_hypertable_schema || '.' || materialization_hypertable_name
            FROM timescaledb_information.continuous_aggregates
            WHERE view_name = 'group_daily_readings_unit'
        )
    ) THEN
        PERFORM add_continuous_aggregate_policy('group_daily_readings_unit',
            start_offset => INTERVAL '7 days',
            end_offset => INTERVAL '1 hour',
            schedule_interval => INTERVAL '1 day',
            if_not_exists => TRUE
        );
        RAISE NOTICE 'Added refresh policy for group_daily_readings_unit';
    ELSE
        RAISE NOTICE 'Refresh policy for group_daily_readings_unit already exists';
    END IF;
END $$;

-- Step 10: Display summary
DO $$
DECLARE
    daily_count INTEGER;
    group_daily_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO daily_count FROM daily_readings_unit;
    SELECT COUNT(*) INTO group_daily_count FROM group_daily_readings_unit;

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Continuous Aggregate Migration Complete';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'daily_readings_unit rows: %', daily_count;
    RAISE NOTICE 'group_daily_readings_unit rows: %', group_daily_count;
    RAISE NOTICE '';
    RAISE NOTICE 'Automatic refresh policies:';
    RAISE NOTICE '  - Refresh window: 7 days ago to 1 hour ago';
    RAISE NOTICE '  - Schedule: Once per day';
    RAISE NOTICE '';
    RAISE NOTICE 'Verify with:';
    RAISE NOTICE '  SELECT * FROM timescaledb_information.continuous_aggregates;';
    RAISE NOTICE '  SELECT * FROM timescaledb_information.jobs;';
    RAISE NOTICE '========================================';
END $$;
