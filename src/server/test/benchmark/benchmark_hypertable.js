/**
 * OED TimescaleDB Performance Benchmark
 *
 * Compares query performance across three structures:
 *   A) readings                  — original table (baseline)
 *   B) readings_hypertable       — TimescaleDB hypertable
 *   C) cagg_hourly_readings_unit — TimescaleDB continuous aggregate (hourly buckets)
 *
 * Same test data as original benchmark:
 *   - 1 year of 15-minute readings for meter_id=1 (~35,040 rows)
 *   - 7 cik_vary segment scenarios from 1 to 17,520 segments
 *   - 3 runs per scenario, reports avg/min/max ms
 *
 * Run with: node benchmark_hypertable.js
 * Requires: npm install pg
 * Output:   benchmark_hypertable_results.json
 * 
 * This required a lot of finagling with the new sql to get it hooked up to pgadmin
 * You could connect directly to the local server and compose up
 * 
 */

const { Pool } = require('pg');
const fs = require('fs');

const pool = new Pool({
    host: '127.0.0.1',
    port: 5432,
    database: 'oed',
    user: 'oed',
    password: 'opened'
});

// const SOURCE_ID = 11;
// const DEST_ID = 1;
// const METER_ID = 1;

const SOURCE_ID = 4;
const DEST_ID = 1;
const METER_ID = 1;

// A: Original readings table (baseline)
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

// B: Hypertable — same query, different table, TimescaleDB chunking applies
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

// C: Continuous aggregate — 8,760 hourly buckets vs 35,040 raw readings
// Smaller row count reduces overlap join surface
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

function makeSegmentInsert(intervalStr, startDate, endDate) {
    return `
        INSERT INTO cik_vary (source_id, destination_id, start_time, end_time, slope, intercept)
        SELECT
            ${SOURCE_ID}, ${DEST_ID},
            gs AS start_time,
            gs + '${intervalStr}'::interval AS end_time,
            0.08 + random() * 0.08 AS slope,
            0
        FROM generate_series(
            '${startDate}'::timestamp,
            '${endDate}'::timestamp,
            '${intervalStr}'::interval
        ) gs;
    `;
}

// const SCENARIOS = [
//     {
//         name: '1 segment (no variation)',
//         segments: 1,
//         insert: `INSERT INTO cik_vary (source_id, destination_id, start_time, end_time, slope, intercept)
//                  VALUES (${SOURCE_ID}, ${DEST_ID}, '-infinity', 'infinity', 0.12, 0);`
//     },
//     {
//         name: '12 segments (monthly)',
//         segments: 12,
//         insert: makeSegmentInsert('1 month', '2024-01-01', '2024-11-01')
//     },
//     {
//         name: '52 segments (weekly)',
//         segments: 52,
//         insert: makeSegmentInsert('1 week', '2024-01-01', '2024-12-23')
//     },
//     {
//         name: '365 segments (daily)',
//         segments: 365,
//         insert: makeSegmentInsert('1 day', '2024-01-01', '2024-12-30')
//     },
//     {
//         name: '2160 segments (every 4 hours)',
//         segments: 2160,
//         insert: makeSegmentInsert('4 hours', '2024-01-01', '2024-12-30 20:00:00')
//     },
//     {
//         name: '8760 segments (hourly)',
//         segments: 8760,
//         insert: makeSegmentInsert('1 hour', '2024-01-01', '2024-12-30 23:00:00')
//     },
//     {
//         name: '17520 segments (every 30 min)',
//         segments: 17520,
//         insert: makeSegmentInsert('30 minutes', '2024-01-01', '2024-12-30 23:30:00')
//     }
// ];

const SCENARIOS = [
    {
        name: '1 segment (no variation)',
        segments: 1,
        insert: `INSERT INTO cik_vary (source_id, destination_id, start_time, end_time, slope, intercept)
                 VALUES (${SOURCE_ID}, ${DEST_ID}, '-infinity', 'infinity', 0.12, 0);`
    },
    {
        name: '12 segments (monthly)',
        segments: 12,
        insert: makeSegmentInsert('1 month', '2020-01-01', '2020-12-31')
    },
    {
        name: '52 segments (weekly)',
        segments: 52,
        insert: makeSegmentInsert('1 week', '2020-01-01', '2020-12-31')
    },
    {
        name: '365 segments (daily)',
        segments: 365,
        insert: makeSegmentInsert('1 day', '2020-01-01', '2020-12-31')
    },
    {
        name: '2160 segments (every 4 hours)',
        segments: 2160,
        insert: makeSegmentInsert('4 hours', '2020-01-01', '2020-12-31')
    },
    {
        name: '8760 segments (hourly)',
        segments: 8760,
        insert: makeSegmentInsert('1 hour', '2020-01-01', '2020-12-31')
    },
    {
        name: '17520 segments (every 30 min)',
        segments: 17520,
        insert: makeSegmentInsert('30 minutes', '2020-01-01', '2020-12-31')
    }
];

async function timeQuery(client, query, runs = 3) {
    const times = [];
    let rowCount = 0;
    for (let i = 0; i < runs; i++) {
        const start = process.hrtime.bigint();
        const result = await client.query(query, [SOURCE_ID, DEST_ID, METER_ID]);
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
    const results = [];

    console.log('OED TimescaleDB Benchmark');
    console.log('=========================');
    console.log(`Testing source_id=${SOURCE_ID}, dest_id=${DEST_ID}, meter_id=${METER_ID}`);
    console.log('3 runs per query per scenario\n');

    try {
        for (const scenario of SCENARIOS) {
            console.log(`Scenario: ${scenario.name}`);

            await client.query(
                'DELETE FROM cik_vary WHERE source_id = $1 AND destination_id = $2',
                [SOURCE_ID, DEST_ID]
            );
            await client.query(scenario.insert);

            const countResult = await client.query(
                'SELECT COUNT(*) FROM cik_vary WHERE source_id = $1 AND destination_id = $2',
                [SOURCE_ID, DEST_ID]
            );
            const actualSegments = parseInt(countResult.rows[0].count);

            process.stdout.write('  readings...            ');
            const timingsA = await timeQuery(client, QUERY_READINGS);
            console.log(`avg=${timingsA.avg}ms  rows=${timingsA.rowCount}`);

            process.stdout.write('  readings_hypertable... ');
            const timingsB = await timeQuery(client, QUERY_HYPERTABLE);
            console.log(`avg=${timingsB.avg}ms  rows=${timingsB.rowCount}`);

            process.stdout.write('  cagg_hourly...         ');
            const timingsC = await timeQuery(client, QUERY_CAGG);
            console.log(`avg=${timingsC.avg}ms  rows=${timingsC.rowCount}\n`);

            results.push({
                name: scenario.name,
                segments: actualSegments,
                readings:    { avgMs: timingsA.avg, minMs: timingsA.min, maxMs: timingsA.max, rowCount: timingsA.rowCount },
                hypertable:  { avgMs: timingsB.avg, minMs: timingsB.min, maxMs: timingsB.max, rowCount: timingsB.rowCount },
                cagg:        { avgMs: timingsC.avg, minMs: timingsC.min, maxMs: timingsC.max, rowCount: timingsC.rowCount }
            });
        }

        await client.query(
            'DELETE FROM cik_vary WHERE source_id = $1 AND destination_id = $2',
            [SOURCE_ID, DEST_ID]
        );

        fs.writeFileSync('benchmark_hypertable_results.json', JSON.stringify(results, null, 2));
        console.log('✓ Results saved to benchmark_hypertable_results.json');

        const w = 36;
        console.log('\n' + '─'.repeat(82));
        console.log('Scenario'.padEnd(w) + 'Segs'.padEnd(8) + 'readings'.padEnd(14) + 'hypertable'.padEnd(14) + 'cagg');
        console.log('─'.repeat(82));
        for (const r of results) {
            console.log(
                r.name.padEnd(w) +
                String(r.segments).padEnd(8) +
                (r.readings.avgMs + 'ms').padEnd(14) +
                (r.hypertable.avgMs + 'ms').padEnd(14) +
                (r.cagg.avgMs + 'ms')
            );
        }
        console.log('─'.repeat(82));

    } catch (err) {
        console.error('\nError:', err.message);
        console.error(err);
    } finally {
        client.release();
        await pool.end();
    }
}

runBenchmark();
