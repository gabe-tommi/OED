/**
 * OED TimescaleDB Performance Benchmark
 *
 * Purpose
 * -------
 * This compares the original raw readings table, the Timescale hypertable, and
 * the hourly continuous aggregate using the same cik_vary overlap join.
 *
 * Run with:
 *   node src/server/test/benchmark/benchmark_hypertable.js
 *
 * Useful environment variables:
 *   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
 *   METER_ID
 *   DEST_ID
 *   RUNS
 *   OUTPUT_FILE
 */

process.env.TZ = 'UTC';

const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const DEFAULT_OUTPUT = path.join(__dirname, 'benchmark_hypertable_results.json');
const METER_ID = parseIntegerEnv('METER_ID', 1);
const RUNS = parseIntegerEnv('RUNS', 3);

const pool = new Pool({
	host: process.env.DB_HOST || '127.0.0.1',
	port: parseIntegerEnv('DB_PORT', 5432),
	database: process.env.DB_NAME || 'oed',
	user: process.env.DB_USER || 'oed',
	password: process.env.DB_PASSWORD || 'opened'
});

const QUERY_READINGS = `
	SELECT
		r.meter_id,
		r.reading * c.slope + c.intercept AS converted_reading,
		r.start_timestamp,
		r.end_timestamp
	FROM readings r
	INNER JOIN cik_vary c
		ON c.source_id = $1
		AND c.destination_id = $2
		AND tsrange(c.start_time, c.end_time, '()') && tsrange(r.start_timestamp, r.end_timestamp, '()')
	WHERE r.meter_id = $3
	ORDER BY r.start_timestamp;
`;

const QUERY_HYPERTABLE = `
	SELECT
		r.meter_id,
		r.reading * c.slope + c.intercept AS converted_reading,
		r.start_timestamp,
		r.end_timestamp
	FROM readings_hypertable r
	INNER JOIN cik_vary c
		ON c.source_id = $1
		AND c.destination_id = $2
		AND tsrange(c.start_time, c.end_time, '()') && tsrange(r.start_timestamp, r.end_timestamp, '()')
	WHERE r.meter_id = $3
	ORDER BY r.start_timestamp;
`;

const QUERY_CAGG = `
	SELECT
		ca.meter_id,
		ca.reading_rate * c.slope + c.intercept AS converted_reading,
		ca.time_interval AS start_timestamp
	FROM cagg_hourly_readings_unit ca
	INNER JOIN cik_vary c
		ON c.source_id = $1
		AND c.destination_id = $2
		AND tsrange(c.start_time, c.end_time, '()') && tsrange(ca.time_interval, ca.time_interval + interval '1 hour', '()')
	WHERE ca.meter_id = $3
	ORDER BY ca.time_interval;
`;

const SCENARIO_DEFS = [
	{ name: '1 segment (no variation)', mode: 'single' },
	{ name: '12 segments (monthly)', interval: '1 month' },
	{ name: '52 segments (weekly)', interval: '1 week' },
	{ name: '365 segments (daily)', interval: '1 day' },
	{ name: '2160 segments (every 4 hours)', interval: '4 hours' },
	{ name: '8760 segments (hourly)', interval: '1 hour' },
	{ name: '17520 segments (every 30 min)', interval: '30 minutes' }
];

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

function addMs(dateLike, ms) {
	return new Date(new Date(dateLike).getTime() + ms);
}

async function assertRequiredObjects(client) {
	const result = await client.query(`
		SELECT
			to_regclass('public.readings') AS readings,
			to_regclass('public.readings_hypertable') AS readings_hypertable,
			to_regclass('public.cagg_hourly_readings_unit') AS cagg,
			to_regclass('public.cik_vary') AS cik_vary
	`);
	const row = result.rows[0];
	if (!row.readings) {
		throw new Error('Missing readings table.');
	}
	if (!row.readings_hypertable) {
		throw new Error('Missing readings_hypertable. Run the Timescale setup first.');
	}
	if (!row.cagg) {
		throw new Error('Missing cagg_hourly_readings_unit. Run the Timescale setup first.');
	}
	if (!row.cik_vary) {
		throw new Error('Missing cik_vary table.');
	}
}

async function loadMeterContext(client) {
	const result = await client.query(`
		SELECT
			m.id,
			m.unit_id,
			m.default_graphic_unit,
			MIN(r.start_timestamp) AS min_start,
			MAX(r.end_timestamp) AS max_end,
			COUNT(*)::int AS reading_count
		FROM meters m
		INNER JOIN readings r ON r.meter_id = m.id
		WHERE m.id = $1
		GROUP BY m.id, m.unit_id, m.default_graphic_unit
	`, [METER_ID]);

	if (result.rowCount === 0) {
		throw new Error(`Meter ${METER_ID} was not found or has no readings.`);
	}

	const row = result.rows[0];
	return {
		meterId: row.id,
		sourceId: row.unit_id,
		destinationId: process.env.DEST_ID
			? parseIntegerEnv('DEST_ID', 1)
			: (row.default_graphic_unit || 1),
		rangeStart: row.min_start,
		rangeEndExclusive: addMs(row.max_end, 1),
		readingCount: row.reading_count
	};
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

async function timeQuery(client, query, sourceId, destinationId, meterId, runs = RUNS) {
	const times = [];
	let rowCount = 0;

	for (let i = 0; i < runs; i += 1) {
		const start = process.hrtime.bigint();
		const result = await client.query(query, [sourceId, destinationId, meterId]);
		const end = process.hrtime.bigint();
		times.push(Number(end - start) / 1_000_000);
		rowCount = result.rowCount;
	}

	return {
		avg: Math.round(times.reduce((a, b) => a + b, 0) / times.length),
		min: Math.round(Math.min(...times)),
		max: Math.round(Math.max(...times)),
		rowCount
	};
}

async function runBenchmark() {
	const client = await pool.connect();

	try {
		await assertRequiredObjects(client);
		const context = await loadMeterContext(client);
		const outputFile = process.env.OUTPUT_FILE || DEFAULT_OUTPUT;
		const results = [];

		console.log('OED TimescaleDB Benchmark');
		console.log('=========================');
		console.log(`meter_id=${context.meterId}`);
		console.log(`source_id=${context.sourceId}`);
		console.log(`destination_id=${context.destinationId}`);
		console.log(`reading_count=${context.readingCount}`);
		console.log(`range=${context.rangeStart.toISOString()} -> ${context.rangeEndExclusive.toISOString()}`);
		console.log(`runs per scenario=${RUNS}\n`);

		for (const scenario of SCENARIO_DEFS) {
			console.log(`Scenario: ${scenario.name}`);

			await resetScenarioCik(client, context.sourceId, context.destinationId);
			await insertScenarioSegments(
				client,
				scenario,
				context.sourceId,
				context.destinationId,
				context.rangeStart,
				context.rangeEndExclusive
			);

			const actualSegments = await countScenarioSegments(client, context.sourceId, context.destinationId);

			process.stdout.write('  readings...            ');
			const timingsA = await timeQuery(client, QUERY_READINGS, context.sourceId, context.destinationId, context.meterId);
			console.log(`avg=${timingsA.avg}ms  rows=${timingsA.rowCount}`);

			process.stdout.write('  readings_hypertable... ');
			const timingsB = await timeQuery(client, QUERY_HYPERTABLE, context.sourceId, context.destinationId, context.meterId);
			console.log(`avg=${timingsB.avg}ms  rows=${timingsB.rowCount}`);

			process.stdout.write('  cagg_hourly...         ');
			const timingsC = await timeQuery(client, QUERY_CAGG, context.sourceId, context.destinationId, context.meterId);
			console.log(`avg=${timingsC.avg}ms  rows=${timingsC.rowCount}\n`);

			results.push({
				name: scenario.name,
				segments: actualSegments,
				readings: { avgMs: timingsA.avg, minMs: timingsA.min, maxMs: timingsA.max, rowCount: timingsA.rowCount },
				hypertable: { avgMs: timingsB.avg, minMs: timingsB.min, maxMs: timingsB.max, rowCount: timingsB.rowCount },
				cagg: { avgMs: timingsC.avg, minMs: timingsC.min, maxMs: timingsC.max, rowCount: timingsC.rowCount }
			});
		}

		await resetScenarioCik(client, context.sourceId, context.destinationId);
		fs.writeFileSync(outputFile, JSON.stringify(results, null, 2));
		console.log(`✓ Results saved to ${outputFile}`);

		const w = 36;
		console.log('\n' + '─'.repeat(82));
		console.log('Scenario'.padEnd(w) + 'Segs'.padEnd(8) + 'readings'.padEnd(14) + 'hypertable'.padEnd(14) + 'cagg');
		console.log('─'.repeat(82));
		for (const r of results) {
			console.log(
				r.name.padEnd(w) +
				String(r.segments).padEnd(8) +
				(`${r.readings.avgMs}ms`).padEnd(14) +
				(`${r.hypertable.avgMs}ms`).padEnd(14) +
				`${r.cagg.avgMs}ms`
			);
		}
		console.log('─'.repeat(82));
	} finally {
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
