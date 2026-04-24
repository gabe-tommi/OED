/**
 * OED TimescaleDB Continuous Aggregate Update Patterns Benchmark
 *
 * Purpose
 * -------
 * This benchmark measures the cost of updating the Timescale prototype for a
 * single meter under several insertion/backfill patterns.
 *
 * The script focuses on the new prototype objects only:
 *   - readings_hypertable
 *   - cagg_hourly_readings_unit
 *
 * For each scenario it measures:
 *   - hypertable insert time
 *   - continuous aggregate refresh time
 *   - total update time
 *   - hypertable storage growth
 *   - cagg storage growth
 *   - hypertable chunk growth
 *   - materialized hourly bucket count in affected windows
 *
 * Scenarios covered
 * -----------------
 * 1. Append 1 hour after the latest reading
 * 2. Backfill 1 hour two years before the earliest reading
 * 3. Future-gap worst case:
 *      - insert one hour far in the future
 *      - insert another hour before it, still leaving gaps
 * 4. Append a 1 day block
 * 5. Append a 30 day block
 * 6. Append the meter's full available history (~1 year in the generated test data)
 *
 * Prerequisites
 * -------------
 * 1. Start the Docker database:
 *      docker compose up -d database
 *
 * 2. Ensure the Timescale prototype objects already exist:
 *      - containers/database/timescaledb/hypertable.sql
 *      - containers/database/timescaledb/continuous_aggregates.sql
 *
 * 3. Ensure the selected benchmark meter has historical data in readings_hypertable.
 *    Helpful existing generators in this repo:
 *      npm run generateFifteenMinuteTestingData
 *      npm run generateTestingData
 *
 * How To Run
 * ----------
 * Default:
 *      npm run benchmarkCaggUpdatePatterns
 *
 * With overrides:
 *      METER_ID=1 npm run benchmarkCaggUpdatePatterns
 *
 * Useful environment variables:
 *      DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
 *      METER_ID     -> benchmark meter id (default: 1)
 *      OUTPUT_FILE  -> JSON output path
 *
 * Output
 * ------
 * Writes benchmark_cagg_update_patterns_results.json by default.
 * Use src/server/test/benchmark/benchmark_cagg_update_patterns_chart.html to visualize it.
 *
 * Safety
 * ------
 * This benchmark is intended for generated benchmark data in the Docker DB only.
 * It inserts and deletes data from readings_hypertable during the run.
 */

process.env.TZ = 'UTC';

const { Pool } = require('pg');
const fs = require('fs');
const path = require('path');

const DEFAULT_OUTPUT = path.join(__dirname, 'benchmark_cagg_update_patterns_results.json');
const METER_ID = parseIntegerEnv('METER_ID', 1);
const INSERT_BATCH_SIZE = parseIntegerEnv('INSERT_BATCH_SIZE', 5000);

const pool = new Pool({
	host: process.env.DB_HOST || '127.0.0.1',
	port: parseIntegerEnv('DB_PORT', 5432),
	database: process.env.DB_NAME || 'oed',
	user: process.env.DB_USER || 'oed',
	password: process.env.DB_PASSWORD || 'opened'
});

// Task definitions for the full benchmark:
// 1) append 1 hour after the latest reading
// 2) backfill 1 hour two years before the earliest reading
// 3) future-gap insert, then insert another block before it with a gap
// 4) append 1 day
// 5) append 30 days
// 6) append full available history
const SCENARIOS = [
	{
		name: 'Append 1 hour after latest data',
		phases: [
			{
				name: 'append 1 hour',
				sourceMode: 'last_duration',
				durationHours: 1,
				targetStart: context => addDays(ceilToHour(context.baseMaxEnd), 0)
			}
		]
	},
	{
		name: 'Backfill 1 hour two years before earliest data',
		phases: [
			{
				name: 'backfill 1 hour',
				sourceMode: 'last_duration',
				durationHours: 1,
				targetStart: context => addYears(floorToHour(context.baseMinStart), -2)
			}
		]
	},
	{
		name: 'Future gap then add before it with gap',
		phases: [
			{
				name: 'future gap block',
				sourceMode: 'last_duration',
				durationHours: 1,
				targetStart: context => addDays(ceilToHour(context.baseMaxEnd), 180)
			},
			{
				name: 'pre-gap block',
				sourceMode: 'last_duration',
				durationHours: 1,
				targetStart: context => addDays(ceilToHour(context.baseMaxEnd), 90)
			}
		]
	},
	{
		name: 'Append 1 day block',
		phases: [
			{
				name: 'append 1 day',
				sourceMode: 'last_duration',
				durationHours: 24,
				targetStart: context => addDays(ceilToHour(context.baseMaxEnd), 7)
			}
		]
	},
	{
		name: 'Append 30 day block',
		phases: [
			{
				name: 'append 30 days',
				sourceMode: 'last_duration',
				durationHours: 24 * 30,
				targetStart: context => addDays(ceilToHour(context.baseMaxEnd), 30)
			}
		]
	},
	{
		name: 'Append full available history (~1 year)',
		phases: [
			{
				name: 'append full history',
				sourceMode: 'full_history',
				targetStart: context => addDays(ceilToHour(context.baseMaxEnd), 730)
			}
		]
	}
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
	if (date.getMinutes() === 0 && date.getSeconds() === 0 && date.getMilliseconds() === 0) {
		return date;
	}
	const ceil = new Date(date);
	ceil.setHours(ceil.getHours() + 1, 0, 0, 0);
	return ceil;
}

function addHours(dateLike, hours) {
	return new Date(new Date(dateLike).getTime() + (hours * 60 * 60 * 1000));
}

function addDays(dateLike, days) {
	return addHours(dateLike, days * 24);
}

function addYears(dateLike, years) {
	const date = new Date(dateLike);
	date.setFullYear(date.getFullYear() + years);
	return date;
}

function formatTs(dateLike) {
	return new Date(dateLike).toISOString().replace('T', ' ').replace('Z', '+00:00');
}

function bytesToMiB(bytes) {
	return Number((bytes / (1024 * 1024)).toFixed(2));
}

async function assertRequiredObjects(client) {
	const result = await client.query(`
		SELECT
			to_regclass('public.readings_hypertable') AS hypertable,
			to_regclass('public.cagg_hourly_readings_unit') AS cagg
	`);
	const row = result.rows[0];
	if (!row.hypertable) {
		throw new Error('Missing readings_hypertable. Run containers/database/timescaledb/hypertable.sql first.');
	}
	if (!row.cagg) {
		throw new Error('Missing cagg_hourly_readings_unit. Run containers/database/timescaledb/continuous_aggregates.sql first.');
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
		INNER JOIN readings_hypertable r ON r.meter_id = m.id
		WHERE m.id = $1
		GROUP BY m.id, m.unit_id, m.default_graphic_unit
	`, [METER_ID]);

	if (result.rowCount === 0) {
		throw new Error(`Meter ${METER_ID} was not found or has no readings in readings_hypertable.`);
	}

	const row = result.rows[0];
	return {
		meterId: row.id,
		sourceId: row.unit_id,
		destinationId: row.default_graphic_unit,
		baseMinStart: row.min_start,
		baseMaxEnd: row.max_end,
		baseReadingCount: row.reading_count
	};
}

async function loadSourceRows(client, context, phase) {
	// Loads the source rows that will be copied into each task window.
	// "last_duration" is used by tasks 1 through 5.
	// "full_history" is used by task 6.
	if (phase.sourceMode === 'full_history') {
		const result = await client.query(`
			SELECT reading, start_timestamp, end_timestamp
			FROM readings_hypertable
			WHERE meter_id = $1
			ORDER BY start_timestamp
		`, [context.meterId]);
		return result.rows;
	}

	const durationHours = phase.durationHours;
	const sourceEnd = context.baseMaxEnd;
	const sourceStart = addHours(sourceEnd, -durationHours);

	const result = await client.query(`
		SELECT reading, start_timestamp, end_timestamp
		FROM readings_hypertable
		WHERE meter_id = $1
			AND start_timestamp >= $2::timestamp
			AND end_timestamp <= $3::timestamp
		ORDER BY start_timestamp
	`, [context.meterId, sourceStart, sourceEnd]);

	if (result.rowCount > 0) {
		return result.rows;
	}

	throw new Error(`Unable to load source rows for phase "${phase.name}" on meter ${context.meterId}.`);
}

function buildShiftedRows(sourceRows, targetStart, valueBump = 0) {
	// Shifts real source rows into the benchmark's target time window.
	// This is the core translation step used by every task:
	// append, backfill, gap insertion, and large-block append.
	const deltaMs = new Date(targetStart).getTime() - new Date(sourceRows[0].start_timestamp).getTime();
	return sourceRows.map((row, index) => {
		const startTimestamp = new Date(new Date(row.start_timestamp).getTime() + deltaMs);
		const endTimestamp = new Date(new Date(row.end_timestamp).getTime() + deltaMs);
		if (endTimestamp <= startTimestamp) {
			throw new Error(
				`Shifted row became invalid after timestamp translation: ` +
				`${formatTs(startTimestamp)} -> ${formatTs(endTimestamp)}`
			);
		}
		return {
		reading: Number(row.reading) + valueBump + ((index + 1) * 0.000001),
			start_timestamp: startTimestamp,
			end_timestamp: endTimestamp
		};
	});
}

function chunkRows(rows, size) {
	const chunks = [];
	for (let i = 0; i < rows.length; i += size) {
		chunks.push(rows.slice(i, i + size));
	}
	return chunks;
}

async function insertRowsInBatches(client, meterId, rows) {
	// Hypertable insert path under test.
	// This matters most for the larger tasks: 1 day, 30 days, and full history.
	for (const batch of chunkRows(rows, INSERT_BATCH_SIZE)) {
		const placeholders = [];
		const values = [];
		let index = 1;
		for (const row of batch) {
			placeholders.push(`($${index}, $${index + 1}, $${index + 2}, $${index + 3})`);
			values.push(meterId, row.reading, row.start_timestamp, row.end_timestamp);
			index += 4;
		}
		await client.query(
			`INSERT INTO readings_hypertable (meter_id, reading, start_timestamp, end_timestamp) VALUES ${placeholders.join(', ')}`,
			values
		);
	}
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

async function refreshCaggWindow(client, start, endExclusive) {
	// Timescale continuous aggregate refresh path under test.
	// Every task refreshes only the changed time window instead of the full history.
	await client.query(
		`CALL refresh_continuous_aggregate('cagg_hourly_readings_unit', $1::timestamp, $2::timestamp)`,
		[start, endExclusive]
	);
}

async function measureRelationStats(client) {
	// Captures space and chunk growth caused by each task.
	// This block supports the storage and chunk-growth graphs.
	const result = await client.query(`
		SELECT
			pg_total_relation_size('readings_hypertable')::bigint AS hypertable_bytes,
			pg_total_relation_size('cagg_hourly_readings_unit')::bigint AS cagg_bytes,
			(
				SELECT COUNT(*)::int
				FROM timescaledb_information.chunks
				WHERE hypertable_schema = 'public'
					AND hypertable_name = 'readings_hypertable'
			) AS hypertable_chunks
	`);
	return {
		hypertableBytes: Number(result.rows[0].hypertable_bytes),
		caggBytes: Number(result.rows[0].cagg_bytes),
		hypertableChunks: Number(result.rows[0].hypertable_chunks)
	};
}

async function countCaggRowsInWindow(client, meterId, start, endExclusive) {
	// Counts how many hourly cagg rows were affected by the task's refresh window.
	// This is most useful for the 1 day, 30 day, and full-history tasks.
	const result = await client.query(`
		SELECT COUNT(*)::int AS count
		FROM cagg_hourly_readings_unit
		WHERE meter_id = $1
			AND time_interval >= $2::timestamp
			AND time_interval < $3::timestamp
	`, [meterId, start, endExclusive]);
	return Number(result.rows[0].count);
}

async function cleanupRanges(client, meterId, ranges) {
	// Removes benchmark inserts and re-refreshes those windows to restore the DB.
	// Shared cleanup for all tasks.
	for (const range of ranges) {
		await client.query(`
			DELETE FROM readings_hypertable
			WHERE meter_id = $1
				AND start_timestamp >= $2::timestamp
				AND start_timestamp < $3::timestamp
		`, [meterId, range.rangeStart, range.rangeEndExclusive]);
	}

	for (const range of ranges) {
		await refreshCaggWindow(client, range.refreshStart, range.refreshEndExclusive);
	}
}

async function runScenario(client, context, scenario) {
	// Runs one full scenario end-to-end:
	// choose source rows, shift them into the task window, insert into the hypertable,
	// refresh the cagg, and record timing/space/chunk/materialized-row results.
	const beforeStats = await measureRelationStats(client);
	const cleanupRanges = [];
	const phases = [];

	let totalInsertMs = 0;
	let totalRefreshMs = 0;
	let totalInsertedRows = 0;
	let totalMaterializedRows = 0;

	try {
		for (let i = 0; i < scenario.phases.length; i += 1) {
			const phase = scenario.phases[i];
			const sourceRows = await loadSourceRows(client, context, phase);
			const targetStart = phase.targetStart(context);
			const insertedRows = buildShiftedRows(sourceRows, targetStart, i + 1);
			const rangeStart = insertedRows[0].start_timestamp;
			const rangeEnd = insertedRows[insertedRows.length - 1].end_timestamp;
			const rangeEndExclusive = addHours(ceilToHour(rangeEnd), 1);
			const refreshStart = floorToHour(rangeStart);
			const refreshEndExclusive = rangeEndExclusive;

			const insertTiming = await timeOperation(() => insertRowsInBatches(client, context.meterId, insertedRows));
			const refreshTiming = await timeOperation(() => refreshCaggWindow(client, refreshStart, refreshEndExclusive));
			const materializedRows = await countCaggRowsInWindow(client, context.meterId, refreshStart, refreshEndExclusive);

			totalInsertMs += insertTiming.elapsedMs;
			totalRefreshMs += refreshTiming.elapsedMs;
			totalInsertedRows += insertedRows.length;
			totalMaterializedRows += materializedRows;

			// Save the exact inserted and refreshed windows so we can clean them up later.
			cleanupRanges.push({
				rangeStart,
				rangeEndExclusive,
				refreshStart,
				refreshEndExclusive
			});

			phases.push({
				name: phase.name,
				insertedRows: insertedRows.length,
				insertMs: roundMs(insertTiming.elapsedMs),
				refreshMs: roundMs(refreshTiming.elapsedMs),
				totalMs: roundMs(insertTiming.elapsedMs + refreshTiming.elapsedMs),
				rangeStart: formatTs(rangeStart),
				rangeEndExclusive: formatTs(rangeEndExclusive),
				materializedRows
			});
		}
	} catch (error) {
		error.cleanupRanges = cleanupRanges;
		throw error;
	}

	const afterStats = await measureRelationStats(client);

	return {
		name: scenario.name,
		phaseCount: phases.length,
		insertedRows: totalInsertedRows,
		totalInsertMs: roundMs(totalInsertMs),
		totalRefreshMs: roundMs(totalRefreshMs),
		totalUpdateMs: roundMs(totalInsertMs + totalRefreshMs),
		hypertableBytesDelta: afterStats.hypertableBytes - beforeStats.hypertableBytes,
		caggBytesDelta: afterStats.caggBytes - beforeStats.caggBytes,
		totalBytesDelta: (afterStats.hypertableBytes - beforeStats.hypertableBytes) + (afterStats.caggBytes - beforeStats.caggBytes),
		hypertableBytesDeltaMiB: bytesToMiB(afterStats.hypertableBytes - beforeStats.hypertableBytes),
		caggBytesDeltaMiB: bytesToMiB(afterStats.caggBytes - beforeStats.caggBytes),
		totalBytesDeltaMiB: bytesToMiB((afterStats.hypertableBytes - beforeStats.hypertableBytes) + (afterStats.caggBytes - beforeStats.caggBytes)),
		chunkDelta: afterStats.hypertableChunks - beforeStats.hypertableChunks,
		hypertableChunksBefore: beforeStats.hypertableChunks,
		hypertableChunksAfter: afterStats.hypertableChunks,
		materializedRows: totalMaterializedRows,
		phases,
		cleanupRanges
	};
}

async function runBenchmark() {
	// Top-level benchmark driver that runs all greenlit tasks, writes JSON output,
	// prints the console summary, and performs cleanup on success or failure.
	const client = await pool.connect();
	let context;
	let lastCleanupRanges = [];

	try {
		await assertRequiredObjects(client);
		context = await loadMeterContext(client);

		const outputFile = process.env.OUTPUT_FILE || DEFAULT_OUTPUT;

		console.log('OED TimescaleDB Update Patterns Benchmark');
		console.log('=========================================');
		console.log(`meter_id=${context.meterId}`);
		console.log(`source_id=${context.sourceId}`);
		console.log(`destination_id=${context.destinationId}`);
		console.log(`base readings in hypertable=${context.baseReadingCount}`);
		console.log(`history range=${formatTs(context.baseMinStart)} -> ${formatTs(context.baseMaxEnd)}`);
		console.log(`insert batch size=${INSERT_BATCH_SIZE}\n`);

		const results = [];

		for (const scenario of SCENARIOS) {
			console.log(`Scenario: ${scenario.name}`);
			let scenarioResult;
			try {
				scenarioResult = await runScenario(client, context, scenario);
			} catch (error) {
				if (error.cleanupRanges && error.cleanupRanges.length > 0) {
					lastCleanupRanges = lastCleanupRanges.concat(error.cleanupRanges);
				}
				throw error;
			}
			lastCleanupRanges = lastCleanupRanges.concat(scenarioResult.cleanupRanges);

			console.log(`  inserted rows... ${scenarioResult.insertedRows}`);
			console.log(`  insert time..... ${scenarioResult.totalInsertMs}ms`);
			console.log(`  refresh time.... ${scenarioResult.totalRefreshMs}ms`);
			console.log(`  total update.... ${scenarioResult.totalUpdateMs}ms`);
			console.log(`  hypertable delta ${scenarioResult.hypertableBytesDeltaMiB} MiB`);
			console.log(`  cagg delta...... ${scenarioResult.caggBytesDeltaMiB} MiB`);
			console.log(`  chunk delta..... ${scenarioResult.chunkDelta}`);
			console.log(`  cagg rows....... ${scenarioResult.materializedRows}\n`);

			results.push({
				name: scenarioResult.name,
				phaseCount: scenarioResult.phaseCount,
				insertedRows: scenarioResult.insertedRows,
				totalInsertMs: scenarioResult.totalInsertMs,
				totalRefreshMs: scenarioResult.totalRefreshMs,
				totalUpdateMs: scenarioResult.totalUpdateMs,
				hypertableBytesDelta: scenarioResult.hypertableBytesDelta,
				caggBytesDelta: scenarioResult.caggBytesDelta,
				totalBytesDelta: scenarioResult.totalBytesDelta,
				hypertableBytesDeltaMiB: scenarioResult.hypertableBytesDeltaMiB,
				caggBytesDeltaMiB: scenarioResult.caggBytesDeltaMiB,
				totalBytesDeltaMiB: scenarioResult.totalBytesDeltaMiB,
				chunkDelta: scenarioResult.chunkDelta,
				hypertableChunksBefore: scenarioResult.hypertableChunksBefore,
				hypertableChunksAfter: scenarioResult.hypertableChunksAfter,
				materializedRows: scenarioResult.materializedRows,
				phases: scenarioResult.phases
			});
		}

		const payload = {
			meta: {
				generatedAt: new Date().toISOString(),
				meterId: context.meterId,
				sourceId: context.sourceId,
				destinationId: context.destinationId,
				baseReadingCount: context.baseReadingCount,
				baseMinStart: formatTs(context.baseMinStart),
				baseMaxEnd: formatTs(context.baseMaxEnd),
				insertBatchSize: INSERT_BATCH_SIZE
			},
			results
		};

		fs.writeFileSync(outputFile, JSON.stringify(payload, null, 2));
		console.log(`✓ Results saved to ${outputFile}`);

		console.log('\nSummary');
		console.log('─'.repeat(116));
		console.log(
			'Scenario'.padEnd(34) +
			'Rows'.padEnd(10) +
			'Insert'.padEnd(12) +
			'Refresh'.padEnd(12) +
			'Total'.padEnd(12) +
			'HT MiB'.padEnd(12) +
			'Cagg MiB'.padEnd(12) +
			'Chunks'
		);
		console.log('─'.repeat(116));
		for (const row of results) {
			console.log(
				row.name.padEnd(34) +
				String(row.insertedRows).padEnd(10) +
				(`${row.totalInsertMs}ms`).padEnd(12) +
				(`${row.totalRefreshMs}ms`).padEnd(12) +
				(`${row.totalUpdateMs}ms`).padEnd(12) +
				(`${row.hypertableBytesDeltaMiB}`).padEnd(12) +
				(`${row.caggBytesDeltaMiB}`).padEnd(12) +
				String(row.chunkDelta)
			);
		}
		console.log('─'.repeat(116));
	} finally {
		if (context && lastCleanupRanges.length > 0) {
			try {
				console.log('\nRestoring unfinished scenario state...');
				await cleanupRanges(client, context.meterId, lastCleanupRanges);
				console.log('✓ Cleanup complete');
			} catch (cleanupError) {
				console.error('\nFailed to clean benchmark inserts:');
				console.error(cleanupError.message);
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
