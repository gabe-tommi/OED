/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

-- create readings table
CREATE TABLE IF NOT EXISTS readings (
  meter_id INT NOT NULL REFERENCES meters(id),
  reading FLOAT NOT NULL,
  start_timestamp TIMESTAMP NOT NULL,
	end_timestamp TIMESTAMP NOT NULL,
	CHECK (start_timestamp < readings.end_timestamp),
  PRIMARY KEY (meter_id, start_timestamp)
);

-- Convert readings table to TimescaleDB hypertable for automatic time-based partitioning
-- Partitions data into 7-day chunks for efficient parallel queries
SELECT create_hypertable('readings', 'start_timestamp', 
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Enable compression with optimized settings for meter data
-- Segments by meter_id for efficient per-meter queries
-- Orders by timestamp for better compression ratios
ALTER TABLE readings SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'meter_id',
    timescaledb.compress_orderby = 'start_timestamp DESC'
);

-- Automatically compress data older than 14 days (saves ~90% storage)
SELECT add_compression_policy('readings', INTERVAL '14 days', if_not_exists => TRUE);

-- Additional indexes for common query patterns
CREATE INDEX IF NOT EXISTS readings_end_timestamp_idx ON readings (end_timestamp DESC);
CREATE INDEX IF NOT EXISTS readings_meter_time_idx ON readings (meter_id, start_timestamp DESC, end_timestamp DESC);
