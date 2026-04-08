/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

-- Optional migration script for existing installs.
-- It can be run manually and is written to be safe for repeat runs.

CREATE EXTENSION IF NOT EXISTS timescaledb;

DO $$
BEGIN
	IF NOT EXISTS (
		SELECT 1
		FROM timescaledb_information.hypertables
		WHERE hypertable_name = 'readings'
	) THEN
		PERFORM create_hypertable(
			'readings',
			'start_timestamp',
			chunk_time_interval => INTERVAL '7 days',
			migrate_data => TRUE,
			if_not_exists => TRUE
		);
	END IF;
END $$;

ALTER TABLE readings SET (
	timescaledb.compress,
	timescaledb.compress_segmentby = 'meter_id',
	timescaledb.compress_orderby = 'start_timestamp DESC'
);

SELECT add_compression_policy('readings', INTERVAL '14 days', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS readings_end_timestamp_idx ON readings (end_timestamp DESC);
CREATE INDEX IF NOT EXISTS readings_meter_time_idx ON readings (meter_id, start_timestamp DESC, end_timestamp DESC);
