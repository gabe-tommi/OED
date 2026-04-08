/**
 * OED cik_vary Query Performance Benchmark
 * 
 * Run with: node benchmark.js
 * Requires: npm install pg
 * 
 * This script tests how query time scales as you increase
 * the number of time-varying conversion segments in cik_vary.
 * This will generate a json, which is viewable via the html file
 * /src/server/test/benchmark/benchmark_chart.html
 * Just open it in your browser and drag the json this script generates into the file field
 * the html has an embedded script which will display your query data niceley for you
 */

const { Pool } = require('pg');
const fs = require('fs');

// Match your docker-compose.yml settings
const pool = new Pool({
    host: '127.0.0.1',
    port: 5432,
    database: 'oed',
    user: 'oed',
    password: 'opened'
});

// The source/destination pair to test - change if needed
const SOURCE_ID = 11;
const DEST_ID = 1;
const METER_ID = 1;

// Benchmark query - does the expensive time-overlap join
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

// Generate INSERT for conversion segments across a date range
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
        insert: makeSegmentInsert('1 month', '2024-01-01', '2024-11-01')
    },
    {
        name: '52 segments (weekly)',
        segments: 52,
        insert: makeSegmentInsert('1 week', '2024-01-01', '2024-12-23')
    },
    {
        name: '365 segments (daily)',
        segments: 365,
        insert: makeSegmentInsert('1 day', '2024-01-01', '2024-12-30')
    },
    {
        name: '2160 segments (every 4 hours)',
        segments: 2160,
        insert: makeSegmentInsert('4 hours', '2024-01-01', '2024-12-30 20:00:00')
    },
    {
        name: '8760 segments (hourly)',
        segments: 8760,
        insert: makeSegmentInsert('1 hour', '2024-01-01', '2024-12-30 23:00:00')
    },
    {
        name: '17520 segments (every 30 min)',
        segments: 17520,
        insert: makeSegmentInsert('30 minutes', '2024-01-01', '2024-12-30 23:30:00')
    }
];

// Run query N times and return average, min, max
async function timeQuery(client, runs = 3) {
    const times = [];
    let rowCount = 0;

    for (let i = 0; i < runs; i++) {
        const start = process.hrtime.bigint();
        const result = await client.query(BENCHMARK_QUERY, [SOURCE_ID, DEST_ID, METER_ID]);
        const end = process.hrtime.bigint();
        times.push(Number(end - start) / 1_000_000); // convert to ms
        rowCount = result.rowCount;
    }

    return {
        avg: times.reduce((a, b) => a + b, 0) / times.length,
        min: Math.min(...times),
        max: Math.max(...times),
        rowCount
    };
}

async function runBenchmark() {
    const client = await pool.connect();
    const results = [];

    console.log('OED cik_vary Benchmark');
    console.log('======================');
    console.log(`Testing source_id=${SOURCE_ID}, destination_id=${DEST_ID}, meter_id=${METER_ID}`);
    console.log('Each scenario runs 3 times, reporting avg/min/max ms\n');

    try {
        for (const scenario of SCENARIOS) {
            process.stdout.write(`Running: ${scenario.name}... `);

            // Clear previous segments
            await client.query(
                'DELETE FROM cik_vary WHERE source_id = $1 AND destination_id = $2',
                [SOURCE_ID, DEST_ID]
            );

            // Insert this scenario's segments
            await client.query(scenario.insert);

            // Verify segment count
            const countResult = await client.query(
                'SELECT COUNT(*) FROM cik_vary WHERE source_id = $1 AND destination_id = $2',
                [SOURCE_ID, DEST_ID]
            );
            const actualSegments = parseInt(countResult.rows[0].count);

            // Time the benchmark query
            const timing = await timeQuery(client);

            const result = {
                name: scenario.name,
                segments: actualSegments,
                avgMs: Math.round(timing.avg),
                minMs: Math.round(timing.min),
                maxMs: Math.round(timing.max),
                rowCount: timing.rowCount
            };

            results.push(result);
            console.log(`avg=${result.avgMs}ms  min=${result.minMs}ms  max=${result.maxMs}ms  rows=${result.rowCount}`);
        }

        // Clean up
        await client.query(
            'DELETE FROM cik_vary WHERE source_id = $1 AND destination_id = $2',
            [SOURCE_ID, DEST_ID]
        );

        // Write results to JSON for the chart
        fs.writeFileSync('benchmark_results.json', JSON.stringify(results, null, 2));

        console.log('\n✓ Results saved to benchmark_results.json');
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

    } catch (err) {
        console.error('\nError:', err.message);
        console.error(err);
    } finally {
        client.release();
        await pool.end();
    }
}

runBenchmark();