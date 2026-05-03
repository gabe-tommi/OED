/**
 * OED cik_vary Query Performance Benchmark
 *
 * Purpose
 * -------
 * This is the original baseline benchmark for the expensive cik_vary overlap join.
 * It measures how query time scales as the number of time-varying conversion
 * segments increases when reading directly from the raw readings table.
 *
 * Run with:
 *   node src/server/test/benchmark/benchmark.js
 *
 * Useful environment variables:
 *   DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
 *   METER_ID
 *   DEST_ID
 *   RUNS
 *   OUTPUT_FILE
 *
 * Output:
 *   benchmark_results.json by default
 *
 * Notes
 * -----
 * Older versions of this file hardcoded:
 *   - DB password
 *   - source/destination IDs
 *   - scenario date windows
 *
 * This version derives meter context and benchmark date ranges from the live DB
 * so it can still be used after the benchmark dataset evolves.
 */

process.env.TZ = 'UTC';

const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const DEFAULT_OUTPUT = path.join(__dirname, 'benchmark_results.json');
const METER_ID = parseIntegerEnv('METER_ID', 1);
const RUNS = parseIntegerEnv('RUNS', 3);

const pool = new Pool({
	host: process.env.DB_HOST || '127.0.0.1',
	port: parseIntegerEnv('DB_PORT', 5432),
	database: process.env.DB_NAME || 'oed',
	user: process.env.DB_USER || 'oed',
	password: process.env.DB_PASSWORD || 'opened'
});

const BENCHMARK_QUERY = `
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

function floorToIntervalStart(dateLike) {
	return new Date(dateLike);
}

function addMs(dateLike, ms) {
	return new Date(new Date(dateLike).getTime() + ms);
}

async function assertRequiredObjects(client) {
	const result = await client.query(`
		SELECT to_regclass('public.cik_vary') AS cik_vary, to_regclass('public.readings') AS readings
	`);
	if (!result.rows[0].cik_vary) {
		throw new Error('Missing cik_vary table.');
	}
	if (!result.rows[0].readings) {
		throw new Error('Missing readings table.');
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

async function timeQuery(client, sourceId, destinationId, meterId, runs = RUNS) {
	const times = [];
	let rowCount = 0;

	for (let i = 0; i < runs; i += 1) {
		const start = process.hrtime.bigint();
		const result = await client.query(BENCHMARK_QUERY, [sourceId, destinationId, meterId]);
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

		console.log('OED cik_vary Benchmark');
		console.log('======================');
		console.log(`meter_id=${context.meterId}`);
		console.log(`source_id=${context.sourceId}`);
		console.log(`destination_id=${context.destinationId}`);
		console.log(`reading_count=${context.readingCount}`);
		console.log(`range=${context.rangeStart.toISOString()} -> ${context.rangeEndExclusive.toISOString()}`);
		console.log(`runs per scenario=${RUNS}\n`);

		for (const scenario of SCENARIO_DEFS) {
			process.stdout.write(`Running: ${scenario.name}... `);

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
			const timing = await timeQuery(client, context.sourceId, context.destinationId, context.meterId);

			const result = {
				name: scenario.name,
				segments: actualSegments,
				avgMs: timing.avg,
				minMs: timing.min,
				maxMs: timing.max,
				rowCount: timing.rowCount
			};

			results.push(result);
			console.log(`avg=${result.avgMs}ms  min=${result.minMs}ms  max=${result.maxMs}ms  rows=${result.rowCount}`);
		}

		await resetScenarioCik(client, context.sourceId, context.destinationId);
		fs.writeFileSync(outputFile, JSON.stringify(results, null, 2));

		console.log(`\n✓ Results saved to ${outputFile}`);
		console.log('\nSummary Table:');
		console.log('─'.repeat(70));
		console.log('Scenario                          Segments   Avg(ms)   Min(ms)   Max(ms)');
		console.log('─'.repeat(70));
		for (const r of results) {
			console.log(
				r.name.padEnd(34) +
				String(r.segments).padEnd(11) +
				String(r.avgMs).padEnd(10) +
				String(r.minMs).padEnd(10) +
				String(r.maxMs)
			);
		}
		console.log('─'.repeat(70));
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
