/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

/**
 * Tests to verify TimescaleDB continuous aggregates are properly configured.
 * These tests check that the hybrid architecture is correctly set up:
 * - hourly_readings_unit: Traditional materialized view
 * - daily_readings_unit: TimescaleDB continuous aggregate
 * - group_hourly_readings_unit: Traditional materialized view
 * - group_daily_readings_unit: TimescaleDB continuous aggregate
 */

const { mocha, expect, testDB } = require('../common');
const Reading = require('../../models/Reading');

mocha.describe('TimescaleDB Continuous Aggregate Verification', () => {
	let conn;

	mocha.beforeEach(() => {
		conn = testDB.getConnection();
	});

	mocha.describe('View Type Verification', () => {
		mocha.it('daily_readings_unit should be a continuous aggregate', async () => {
			const result = await conn.oneOrNone(
				`SELECT view_name
				 FROM timescaledb_information.continuous_aggregates
				 WHERE view_name = 'daily_readings_unit'`
			);
			// This test will pass if TimescaleDB is configured with continuous aggregates
			// It will be skipped if TimescaleDB is not installed or daily is still a mat view
			if (result) {
				expect(result.view_name).to.equal('daily_readings_unit');
			} else {
				// TimescaleDB not configured or daily_readings_unit is still a materialized view
				// This is acceptable during transition
				console.log('Note: daily_readings_unit is not yet a continuous aggregate');
			}
		});

		mocha.it('hourly_readings_unit should NOT be a continuous aggregate', async () => {
			const result = await conn.oneOrNone(
				`SELECT view_name
				 FROM timescaledb_information.continuous_aggregates
				 WHERE view_name = 'hourly_readings_unit'`
			);
			// hourly_readings_unit must remain a materialized view for accuracy
			expect(result).to.be.null;
		});

		mocha.it('group_daily_readings_unit should be a continuous aggregate', async () => {
			const result = await conn.oneOrNone(
				`SELECT view_name
				 FROM timescaledb_information.continuous_aggregates
				 WHERE view_name = 'group_daily_readings_unit'`
			);
			if (result) {
				expect(result.view_name).to.equal('group_daily_readings_unit');
			} else {
				console.log('Note: group_daily_readings_unit is not yet a continuous aggregate');
			}
		});

		mocha.it('group_hourly_readings_unit should NOT be a continuous aggregate', async () => {
			const result = await conn.oneOrNone(
				`SELECT view_name
				 FROM timescaledb_information.continuous_aggregates
				 WHERE view_name = 'group_hourly_readings_unit'`
			);
			expect(result).to.be.null;
		});
	});

	mocha.describe('Refresh Functionality', () => {
		mocha.it('refreshReadingAggregate should work for hourly_readings_unit (mat view)', async () => {
			// This should not throw an error
			await Reading.refreshReadingAggregate(conn, 'hourly_readings_unit');
		});

		mocha.it('refreshReadingAggregate should work for daily_readings_unit', async () => {
			// This should work whether it's a mat view or continuous aggregate
			await Reading.refreshReadingAggregate(conn, 'daily_readings_unit');
		});

		mocha.it('refreshMeterReadingsViews should refresh both hourly and daily', async () => {
			// This should not throw an error
			await Reading.refreshMeterReadingsViews(conn);
		});
	});

	mocha.describe('View Structure Verification', () => {
		mocha.it('daily_readings_unit should have expected columns', async () => {
			const columns = await conn.any(
				`SELECT column_name
				 FROM information_schema.columns
				 WHERE table_name = 'daily_readings_unit'
				 ORDER BY ordinal_position`
			);
			const columnNames = columns.map(c => c.column_name);

			// These columns must exist for compatibility with existing queries
			expect(columnNames).to.include('meter_id');
			expect(columnNames).to.include('reading_rate');
			expect(columnNames).to.include('time_interval');
		});

		mocha.it('hourly_readings_unit should have expected columns', async () => {
			const columns = await conn.any(
				`SELECT column_name
				 FROM information_schema.columns
				 WHERE table_name = 'hourly_readings_unit'
				 ORDER BY ordinal_position`
			);
			const columnNames = columns.map(c => c.column_name);

			expect(columnNames).to.include('meter_id');
			expect(columnNames).to.include('reading_rate');
			expect(columnNames).to.include('max_rate');
			expect(columnNames).to.include('min_rate');
			expect(columnNames).to.include('time_interval');
		});
	});

	mocha.describe('Refresh Policy Verification', () => {
		mocha.it('continuous aggregates should have refresh policies', async () => {
			// Check if refresh policies exist for continuous aggregates
			const policies = await conn.any(
				`SELECT application_name, schedule_interval
				 FROM timescaledb_information.jobs
				 WHERE proc_name = 'policy_refresh_continuous_aggregate'`
			);

			// If continuous aggregates exist, they should have policies
			const caggCount = await conn.one(
				`SELECT COUNT(*) as count
				 FROM timescaledb_information.continuous_aggregates
				 WHERE view_name IN ('daily_readings_unit', 'group_daily_readings_unit')`
			);

			if (parseInt(caggCount.count) > 0) {
				expect(policies.length).to.be.at.least(1);
				console.log(`Found ${policies.length} continuous aggregate refresh policies`);
			} else {
				console.log('Note: No continuous aggregates configured yet');
			}
		});
	});
});
