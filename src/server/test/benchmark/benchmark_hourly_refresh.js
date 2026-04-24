/**
 * OED TimescaleDB Hourly Refresh Benchmark
 *
 * Purpose
 * -------
 * This benchmark focuses on the current prototype comparison:
 *   1. old hourly materialized view path  -> hourly_readings_unit
 *   2. new Timescale hourly cagg path     -> cagg_hourly_readings_unit
 *
 * For each cik_vary segment scenario, the script simulates a "client checked in an
 * hour later" workflow:
 *   - copy the selected meter's most recent hour of readings forward by 1 hour
 *   - insert those new rows into both readings and readings_hypertable
 *   - refresh the old hourly materialized view fully
 *   - refresh the Timescale continuous aggregate only for the recent window
 *   - run the same cik_vary overlap join against both hourly structures
 *   - record refresh time, query time, total wait time, and row counts
 *
 * Prerequisites
 * -------------
 * 1. Start the Docker database:
 *      docker compose up -d database
 *
 * 2. Ensure the Timescale prototype objects already exist in the benchmark database:
 *      - containers/database/timescaledb/hypertable.sql
 *      - containers/database/timescaledb/continuous_aggregates.sql
 *
 * 3. Ensure the selected benchmark meter has historical readings.
 *    Optional test-data helpers that already exist in this repo:
 *      npm run generateFifteenMinuteTestingData
 *      npm run generateFourHourTestingData
 *      npm run generateTestingData
 *
 *    The existing automated generators in src/server/data/automatedTestingData.js
 *    are useful for creating a known 2020 workload. This benchmark does not call
 *    them automatically because they mutate the broader OED dataset and are often
 *    run intentionally by hand before benchmarking.
 *
 * How To Run
 * ----------
 * Default:
 *      npm run benchmarkHourlyRefresh
 *
 * With overrides:
 *      METER_ID=1 DEST_ID=1 RUNS=3 npm run benchmarkHourlyRefresh
 *
 * Useful environment variables:
 *      DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
 *      METER_ID     -> benchmark meter id (default: 1)
 *      DEST_ID      -> graphic unit id; defaults to meter.default_graphic_unit, then 1
 *      RUNS         -> number of timed query runs per scenario (default: 3)
 *      OUTPUT_FILE  -> JSON output path
 *
 * Output
 * ------
 * Writes benchmark_hourly_refresh_results.json by default.
 * Use src/server/test/benchmark/benchmark_hourly_refresh_chart.html to visualize it.
 * 
 * Goes without saying, but don't run this in a dev environment. This is strictly for generated test data only while running
 * the docker image.
 */

process.env.TZ = 'UTC';

const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const DEFAULT_OUTPUT = path.join(__dirname, 'benchmark_hourly_refresh_results.json');
const INSERT_SHIFT_MS = 60 * 60 * 1000;
const QUERY_RUNS = parseIntegerEnv('RUNS', 3);
const METER_ID = parseIntegerEnv('METER_ID', 1);

const pool = new Pool({
	host: process.env.DB_HOST || '127.0.0.1',
	port: parseIntegerEnv('DB_PORT', 5432),
	database: process.env.DB_NAME || 'oed',
	user: process.env.DB_USER || 'oed',
	password: process.env.DB_PASSWORD
});

const OLD_HOURLY_QUERY = `
	SELECT
		hourly.meter_id,
		hourly.reading_rate * c.slope + c.intercept AS converted_reading,
		lower(hourly.time_interval) AS start_timestamp,
		upper(hourly.time_interval) AS end_timestamp
	FROM hourly_readings_unit hourly
	INNER JOIN meters m ON m.id = hourly.meter_id
	INNER JOIN cik_vary c
		ON c.source_id = m.unit_id
		AND c.destination_id = $2
		AND tsrange(c.start_time, c.end_time, '()') && hourly.time_interval
	WHERE hourly.meter_id = $1
		AND tsrange($3::timestamp, $4::timestamp, '[)') @> hourly.time_interval
	ORDER BY start_timestamp;
`;

const NEW_CAGG_QUERY = `
	SELECT
		ca.meter_id,
		ca.reading_rate * c.slope + c.intercept AS converted_reading,
		ca.time_interval AS start_timestamp,
		ca.time_interval + interval '1 hour' AS end_timestamp
	FROM cagg_hourly_readings_unit ca
	INNER JOIN meters m ON m.id = ca.meter_id
	INNER JOIN cik_vary c
		ON c.source_id = m.unit_id
		AND c.destination_id = $2
		AND tsrange(c.start_time, c.end_time, '()')
			&& tsrange(ca.time_interval, ca.time_interval + interval '1 hour', '()')
	WHERE ca.meter_id = $1
		AND ca.time_interval >= $3::timestamp
		AND ca.time_interval < $4::timestamp
	ORDER BY ca.time_interval;
`;

const SCENARIOS = [
	{ name: '1 segment (no variation)', mode: 'single' },
	{ name: 'daily segments', interval: '1 day' },
	{ name: 'hourly segments', interval: '1 hour' },
	{ name: '15-minute segments', interval: '15 minutes' }
];

/**
 * Converts and environment variable name to an integer
 * 
 * @param {*} name : Environment variable name as String
 * @param {*} fallback : Return code if parse for name fails (name is undefined or '')
 * @returns parsed integer 'parsed' on success, or 'fallback' if name is a bad input. 
 */

function parseIntegerEnv(name, fallback) {
	const raw = process.env[name];
	if (raw === undefined || raw === '') {
		return fallback;
	}
	const parsed = Number.parseInt(raw, 10);
	if (Number.isNaN(parsed)) {
		throw new Error(`Environment variable ${name} must be an integer, got "${raw}"`);
	}
	return parsed;
}

function roundMs(ms) {
	return Math.round(ms);
}

function floorToHour(dateLike) {
	const date = new Date(dateLike);
	date.setMinutes(0, 0, 0);
	return date;
}

function ceilToHour(dateLike) {
	const date = new Date(dateLike);
	if (
		date.getMinutes() === 0 &&
		date.getSeconds() === 0 &&
		date.getMilliseconds() === 0
	) {
		return date;
	}
	const ceil = new Date(date);
	ceil.setHours(ceil.getHours() + 1, 0, 0, 0);
	return ceil;
}

function addHours(dateLike, hours) {
	return new Date(new Date(dateLike).getTime() + (hours * 60 * 60 * 1000));
}

function addMs(dateLike, ms) {
	return new Date(new Date(dateLike).getTime() + ms);
}

function formatTs(dateLike) {
	return new Date(dateLike).toISOString().replace('T', ' ').replace('Z', '+00:00');
}

/**
 * Queries the database for hourly_readings_unit, readings_hypertable, and
 * cagg_hourly_readings_unit and ensures all are present in the current DB connection environment.
 * Throws an error if any are missing.
 * @param {*} client : PoolClient type from PostGres import, init before call
 */
async function assertRequiredObjects(client) {
	const result = await client.query(`
		SELECT
			to_regclass('public.hourly_readings_unit') AS hourly_view,
			to_regclass('public.readings_hypertable') AS hypertable,
			to_regclass('public.cagg_hourly_readings_unit') AS cagg
	`);
	const row = result.rows[0];
	if (!row.hourly_view) {
		throw new Error('Missing hourly_readings_unit. Create OED reading views first.');
	}
	if (!row.hypertable) {
		throw new Error('Missing readings_hypertable. Run containers/database/timescaledb/hypertable.sql first.');
	}
	if (!row.cagg) {
		throw new Error('Missing cagg_hourly_readings_unit. Run containers/database/timescaledb/continuous_aggregates.sql first.');
	}
}

/**
 * Queries the current DB connection and selects rows from the meters table joined to readings table.
 * 
 * If there are no readings for the query above, this errors out and displays the responsible meter id.
 * 
 * Then ensures the number  of records in readings_hypertable matches the rows read from the first query for the same meter, only then returns context 
 * 
 * @param {*} client : PoolClient type from PostGres import, init before call
 */

async function loadMeterContext(client) {
	const meterResult = await client.query(`
		SELECT
			m.id,
			m.unit_id,
			m.default_graphic_unit,
			m.reading_frequency,
			MIN(r.start_timestamp) AS min_start,
			MAX(r.end_timestamp) AS max_end,
			COUNT(*)::int AS reading_count
		FROM meters m
		INNER JOIN readings r ON r.meter_id = m.id
		WHERE m.id = $1
		GROUP BY m.id, m.unit_id, m.default_graphic_unit, m.reading_frequency
	`, [METER_ID]);

	if (meterResult.rowCount === 0) {
		throw new Error(`Meter ${METER_ID} was not found or has no readings in readings.`);
	}

	const meter = meterResult.rows[0];
	const hypertableResult = await client.query(`
		SELECT COUNT(*)::int AS reading_count
		FROM readings_hypertable
		WHERE meter_id = $1
	`, [METER_ID]);

	const hypertableCount = hypertableResult.rows[0].reading_count;
	if (hypertableCount !== meter.reading_count) {
		throw new Error(
			`Meter ${METER_ID} row-count mismatch between readings (${meter.reading_count}) and readings_hypertable (${hypertableCount}). ` +
			'Sync the hypertable prototype before benchmarking.'
		);
	}

	const destinationId = process.env.DEST_ID
		? parseIntegerEnv('DEST_ID', 1)
		: (meter.default_graphic_unit || 1);

	return {
		meterId: meter.id,
		sourceId: meter.unit_id,
		destinationId,
		readingFrequency: meter.reading_frequency,
		baseMinStart: meter.min_start,
		baseMaxEnd: meter.max_end,
		baseReadingCount: meter.reading_count
	};
}
/**
 * 
 * Queries readings for the given meterId between one hour before baseMaxEnd and baseMaxEnd.
 * then returns a map of readings returned mapped to start and end timestamps.
 * 
 * See loadMeterContext for info on these fields.
 * 
 * @param {*} client PoolClient type from PostGres import, init before call
 * @param {*} meterId field from the current meter context
 * @param {*} baseMaxEndn field from the current meter context
 * 
 */

async function loadInsertedRowsTemplate(client, meterId, baseMaxEnd) {
	const oneHourEarlier = addHours(baseMaxEnd, -1);
	let result = await client.query(`
		SELECT reading, start_timestamp, end_timestamp
		FROM readings
		WHERE meter_id = $1
			AND start_timestamp >= $2::timestamp
			AND end_timestamp <= $3::timestamp
		ORDER BY start_timestamp
	`, [meterId, oneHourEarlier, baseMaxEnd]);

	// if (result.rowCount === 0) {
	// 	result = await client.query(`
	// 		SELECT reading, start_timestamp, end_timestamp
	// 		FROM readings
	// 		WHERE meter_id = $1
	// 		ORDER BY start_timestamp DESC
	// 		LIMIT 4
	// 	`, [meterId]);
	// 	result.rows.reverse();
	// }

	if (result.rowCount === 0) {
		throw new Error(`Unable to build simulated inserts for meter ${meterId}; no source readings were found.`);
	}

	return result.rows.map((row, index) => ({
		reading: Number(row.reading) + ((index + 1) * 0.0001),
		start_timestamp: addMs(row.start_timestamp, INSERT_SHIFT_MS),
		end_timestamp: addMs(row.end_timestamp, INSERT_SHIFT_MS)
	}));
}

/**
 * 
 * Performs cleanup for the last inserted query, so query after an insert can be simulated
 * continuously.
 * 
 * @param {*} client PoolClient type from PostGres import, init before call
 * @param {*} meterId received from meter context
 * @param {*} insertedRows received from inserted rows template
 */

async function ensureCleanInsertedRange(client, meterId, insertedRows) {
	const rangeStart = insertedRows[0].start_timestamp;
	const rangeEnd = insertedRows[insertedRows.length - 1].end_timestamp;
	const params = [meterId, rangeStart, rangeEnd];
	await client.query(
		'DELETE FROM readings WHERE meter_id = $1 AND start_timestamp >= $2 AND start_timestamp < $3',
		params
	);
	await client.query(
		'DELETE FROM readings_hypertable WHERE meter_id = $1 AND start_timestamp >= $2 AND start_timestamp < $3',
		params
	);
}

async function insertRows(client, tableName, meterId, rows) {
	const placeholders = [];
	const values = [];
	let index = 1;

	for (const row of rows) {
		placeholders.push(`($${index}, $${index + 1}, $${index + 2}, $${index + 3})`);
		values.push(meterId, row.reading, row.start_timestamp, row.end_timestamp);
		index += 4;
	}

	await client.query(
		`INSERT INTO ${tableName} (meter_id, reading, start_timestamp, end_timestamp) VALUES ${placeholders.join(', ')}`,
		values
	);
}

async function resetScenarioCik(client, sourceId, destinationId) {
	await client.query(
		'DELETE FROM cik_vary WHERE source_id = $1 AND destination_id = $2',
		[sourceId, destinationId]
	);
}

async function insertScenarioSegments(client, scenario, sourceId, destinationId, rangeStart, rangeEndExclusive) {
	if (scenario.mode === 'single') {
		await client.query(`
			INSERT INTO cik_vary (source_id, destination_id, start_time, end_time, slope, intercept)
			VALUES ($1, $2, '-infinity', 'infinity', 0.12, 0)
		`, [sourceId, destinationId]);
		return;
	}

	await client.query(`
		WITH bounds AS (
			SELECT
				$1::int AS source_id,
				$2::int AS destination_id,
				$3::timestamp AS range_start,
				$4::timestamp AS range_end,
				$5::interval AS segment_size
		)
		INSERT INTO cik_vary (source_id, destination_id, start_time, end_time, slope, intercept)
		SELECT
			b.source_id,
			b.destination_id,
			gs AS start_time,
			LEAST(gs + b.segment_size, b.range_end) AS end_time,
			0.08 + mod(extract(epoch FROM gs)::numeric, 11) * 0.005 AS slope,
			0 AS intercept
		FROM bounds b,
		LATERAL generate_series(
			b.range_start,
			b.range_end - interval '1 millisecond',
			b.segment_size
		) gs
		WHERE gs < b.range_end
	`, [sourceId, destinationId, rangeStart, rangeEndExclusive, scenario.interval]);
}

async function countScenarioSegments(client, sourceId, destinationId) {
	const result = await client.query(`
		SELECT COUNT(*)::int AS count
		FROM cik_vary
		WHERE source_id = $1 AND destination_id = $2
	`, [sourceId, destinationId]);
	return result.rows[0].count;
}

async function timeOperation(operation) {
	const start = process.hrtime.bigint();
	const result = await operation();
	const end = process.hrtime.bigint();
	return {
		elapsedMs: Number(end - start) / 1_000_000,
		result
	};
}

async function timeQuery(client, query, params, runs) {
	const timings = [];
	let rowCount = 0;

	for (let i = 0; i < runs; i += 1) {
		const start = process.hrtime.bigint();
		const result = await client.query(query, params);
		const end = process.hrtime.bigint();
		timings.push(Number(end - start) / 1_000_000);
		rowCount = result.rowCount;
	}

	return {
		avgMs: roundMs(timings.reduce((sum, ms) => sum + ms, 0) / timings.length),
		minMs: roundMs(Math.min(...timings)),
		maxMs: roundMs(Math.max(...timings)),
		rowCount
	};
}

async function refreshOldHourly(client) {
	await client.query('REFRESH MATERIALIZED VIEW hourly_readings_unit');
}

async function refreshNewHourly(client, refreshStart, refreshEndExclusive) {
	await client.query(
		`CALL refresh_continuous_aggregate('cagg_hourly_readings_unit', $1::timestamp, $2::timestamp)`,
		[refreshStart, refreshEndExclusive]
	);
}

async function restoreBaselineState(client, context, insertedRows) {
	await ensureCleanInsertedRange(client, context.meterId, insertedRows);
	await resetScenarioCik(client, context.sourceId, context.destinationId);
	await refreshOldHourly(client);
	await refreshNewHourly(
		client,
		floorToHour(context.baseMinStart),
		addHours(ceilToHour(context.baseMaxEnd), 1)
	);
}

async function runBenchmark() {
	const client = await pool.connect();
	let context;
	let insertedRows;

	try {
		await assertRequiredObjects(client);

		context = await loadMeterContext(client);
		insertedRows = await loadInsertedRowsTemplate(client, context.meterId, context.baseMaxEnd);

		const queryStart = floorToHour(context.baseMinStart);
		const queryEndExclusive = addHours(ceilToHour(insertedRows[insertedRows.length - 1].end_timestamp), 1);
		const refreshStart = floorToHour(insertedRows[0].start_timestamp);
		const refreshEndExclusive = addHours(ceilToHour(insertedRows[insertedRows.length - 1].end_timestamp), 1);
		const outputFile = process.env.OUTPUT_FILE || DEFAULT_OUTPUT;

		console.log('OED Hourly Refresh Benchmark');
		console.log('============================');
		console.log(`meter_id=${context.meterId}`);
		console.log(`source_id=${context.sourceId}`);
		console.log(`destination_id=${context.destinationId}`);
		console.log(`base readings=${context.baseReadingCount}`);
		console.log(`query runs per scenario=${QUERY_RUNS}`);
		console.log(`query range=${formatTs(queryStart)} -> ${formatTs(queryEndExclusive)}`);
		console.log(`simulated inserted rows=${insertedRows.length}`);
		console.log(`incremental refresh window=${formatTs(refreshStart)} -> ${formatTs(refreshEndExclusive)}\n`);

		console.log('Normalizing baseline state before timing...');
		await restoreBaselineState(client, context, insertedRows);

		const results = [];
		for (const scenario of SCENARIOS) {
			console.log(`Scenario: ${scenario.name}`);

			await ensureCleanInsertedRange(client, context.meterId, insertedRows);
			await resetScenarioCik(client, context.sourceId, context.destinationId);
			await insertScenarioSegments(
				client,
				scenario,
				context.sourceId,
				context.destinationId,
				queryStart,
				queryEndExclusive
			);

			const actualSegments = await countScenarioSegments(client, context.sourceId, context.destinationId);
			await insertRows(client, 'readings', context.meterId, insertedRows);
			await insertRows(client, 'readings_hypertable', context.meterId, insertedRows);

			process.stdout.write('  old hourly refresh... ');
			const oldRefresh = await timeOperation(() => refreshOldHourly(client));
			console.log(`${roundMs(oldRefresh.elapsedMs)}ms`);

			process.stdout.write('  new cagg refresh...   ');
			const newRefresh = await timeOperation(() => refreshNewHourly(client, refreshStart, refreshEndExclusive));
			console.log(`${roundMs(newRefresh.elapsedMs)}ms`);

			process.stdout.write('  old hourly query...   ');
			const oldQuery = await timeQuery(
				client,
				OLD_HOURLY_QUERY,
				[context.meterId, context.destinationId, queryStart, queryEndExclusive],
				QUERY_RUNS
			);
			console.log(`avg=${oldQuery.avgMs}ms rows=${oldQuery.rowCount}`);

			process.stdout.write('  new cagg query...     ');
			const newQuery = await timeQuery(
				client,
				NEW_CAGG_QUERY,
				[context.meterId, context.destinationId, queryStart, queryEndExclusive],
				QUERY_RUNS
			);
			console.log(`avg=${newQuery.avgMs}ms rows=${newQuery.rowCount}\n`);

			if (oldQuery.rowCount !== newQuery.rowCount) {
				console.warn(
					`  warning: row-count mismatch old=${oldQuery.rowCount} new=${newQuery.rowCount}`
				);
			}

			results.push({
				name: scenario.name,
				segments: actualSegments,
				insertedRows: insertedRows.length,
				rowCountMatch: oldQuery.rowCount === newQuery.rowCount,
				oldHourly: {
					refreshMs: roundMs(oldRefresh.elapsedMs),
					queryAvgMs: oldQuery.avgMs,
					queryMinMs: oldQuery.minMs,
					queryMaxMs: oldQuery.maxMs,
					totalMs: roundMs(oldRefresh.elapsedMs) + oldQuery.avgMs,
					rowCount: oldQuery.rowCount
				},
				newHourly: {
					refreshMs: roundMs(newRefresh.elapsedMs),
					queryAvgMs: newQuery.avgMs,
					queryMinMs: newQuery.minMs,
					queryMaxMs: newQuery.maxMs,
					totalMs: roundMs(newRefresh.elapsedMs) + newQuery.avgMs,
					rowCount: newQuery.rowCount
				}
			});

			await ensureCleanInsertedRange(client, context.meterId, insertedRows);
		}

		const payload = {
			meta: {
				generatedAt: new Date().toISOString(),
				meterId: context.meterId,
				sourceId: context.sourceId,
				destinationId: context.destinationId,
				baseReadingCount: context.baseReadingCount,
				queryRuns: QUERY_RUNS,
				queryStart: formatTs(queryStart),
				queryEndExclusive: formatTs(queryEndExclusive),
				refreshStart: formatTs(refreshStart),
				refreshEndExclusive: formatTs(refreshEndExclusive),
				insertedRows: insertedRows.length
			},
			results
		};

		fs.writeFileSync(outputFile, JSON.stringify(payload, null, 2));
		console.log(`✓ Results saved to ${outputFile}`);

		console.log('\nSummary');
		console.log('─'.repeat(108));
		console.log(
			'Scenario'.padEnd(24) +
			'Segs'.padEnd(10) +
			'Old Refresh'.padEnd(14) +
			'Old Query'.padEnd(12) +
			'New Refresh'.padEnd(14) +
			'New Query'.padEnd(12) +
			'Old Total'.padEnd(12) +
			'New Total'
		);
		console.log('─'.repeat(108));
		for (const row of results) {
			console.log(
				row.name.padEnd(24) +
				String(row.segments).padEnd(10) +
				(`${row.oldHourly.refreshMs}ms`).padEnd(14) +
				(`${row.oldHourly.queryAvgMs}ms`).padEnd(12) +
				(`${row.newHourly.refreshMs}ms`).padEnd(14) +
				(`${row.newHourly.queryAvgMs}ms`).padEnd(12) +
				(`${row.oldHourly.totalMs}ms`).padEnd(12) +
				`${row.newHourly.totalMs}ms`
			);
		}
		console.log('─'.repeat(108));

	} finally {
		if (context && insertedRows) {
			try {
				console.log('\nRestoring baseline state...');
				await restoreBaselineState(client, context, insertedRows);
				console.log('✓ Baseline restored');
			} catch (restoreError) {
				console.error('\nFailed to restore baseline state:');
				console.error(restoreError.message);
			}
		}
		client.release();
		await pool.end();
	}
}

runBenchmark().catch(error => {
	console.error('\nBenchmark failed:');
	console.error(error.message);
	console.error(error);
	process.exitCode = 1;
});
