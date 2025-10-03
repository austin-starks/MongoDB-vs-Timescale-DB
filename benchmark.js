#!/usr/bin/env node

/**
 * Comprehensive Database Performance Benchmark with Positions
 * Compares MongoDB vs PostgreSQL end-to-end query performance
 *
 * Usage: node benchmark.js --mongo=<connection-string> --postgres=<connection-string>
 */

const { MongoClient, ObjectId } = require("mongodb");
const { Pool } = require("pg");

// Configuration
const BACKTEST_ID = "68d03f8c394ed157de606401";
const PORTFOLIO_ID = "66490bb3c319fe3771bfe0ec";
const TOTAL_RUNS = 20;
const PARALLEL_BATCH_SIZE = 4;
const MONGO_POOL_SIZE = 10;
const PG_POOL_SIZE = 10;

// Parse command line arguments
const args = process.argv.slice(2).reduce((acc, arg) => {
  const firstEquals = arg.indexOf("=");
  const key = arg.substring(0, firstEquals).replace("--", "");
  const value = arg.substring(firstEquals + 1);
  acc[key] = value;
  return acc;
}, {});

if (!args.mongo || !args.postgres) {
  console.error(
    "Usage: node benchmark.js --mongo=<connection-string> --postgres=<connection-string>"
  );
  process.exit(1);
}

// Utility functions
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const formatTime = (ms) => `${ms.toFixed(2)}ms`;
const formatBytes = (bytes) => {
  const gb = bytes / (1024 * 1024 * 1024);
  return gb >= 1
    ? `${gb.toFixed(2)} GB`
    : `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
};

const calculateStats = (times) => {
  const sorted = [...times].sort((a, b) => a - b);
  const sum = times.reduce((a, b) => a + b, 0);
  return {
    runs: times,
    count: times.length,
    avg: sum / times.length,
    median: sorted[Math.floor(sorted.length / 2)],
    min: Math.min(...times),
    max: Math.max(...times),
    p95: sorted[Math.floor(sorted.length * 0.95)],
  };
};

// ============================================================================
// STORAGE METRICS
// ============================================================================

async function getMongoStorageMetrics(db) {
  const collections = ["backtesthistories", "portfoliohistories"];

  let totalSize = 0;
  let totalIndexSize = 0;
  const details = {};

  for (const collName of collections) {
    const stats = await db.command({ collStats: collName });

    // Use storageSize for actual compressed disk usage, not size
    const dataSize = stats.storageSize || 0;
    const indexSize = stats.totalIndexSize || 0;
    const total = dataSize + indexSize;

    details[collName] = {
      data: dataSize,
      indexes: indexSize,
      total: total,
      // Optional: track compression ratio
      uncompressed: stats.size || 0,
      compressionRatio: stats.size ? (stats.size / dataSize).toFixed(2) : "N/A",
    };

    totalSize += dataSize;
    totalIndexSize += indexSize;
  }

  return {
    totalData: totalSize,
    totalIndexes: totalIndexSize,
    totalSize: totalSize + totalIndexSize,
    details,
  };
}

async function getPostgresStorageMetrics(pgPool) {
  const tables = [
    "backtest_history",
    "backtest_positions",
    "portfolio_history",
    "portfolio_positions",
  ];

  let totalSize = 0;
  let totalIndexSize = 0;
  const details = {};

  for (const tableName of tables) {
    let dataSize = 0;
    let indexSize = 0;
    let total = 0;
    let compressionStats = null;

    // First, check if it's a hypertable
    try {
      const hypertableCheck = await pgPool.query(
        `SELECT COUNT(*) as is_hypertable 
         FROM timescaledb_information.hypertables 
         WHERE hypertable_name = $1`,
        [tableName]
      );

      if (hypertableCheck.rows[0].is_hypertable > 0) {
        // It's a hypertable - use TimescaleDB functions
        const sizeResult = await pgPool.query(
          `SELECT 
             hypertable_size($1::regclass) as total_size,
             hypertable_detailed_size($1::regclass) as detailed_size`,
          [tableName]
        );

        total = parseInt(sizeResult.rows[0].total_size);

        // Get detailed breakdown
        const detailedResult = await pgPool.query(
          `SELECT * FROM hypertable_detailed_size($1::regclass)`,
          [tableName]
        );

        if (detailedResult.rows[0]) {
          const detailsRow = detailedResult.rows[0];
          dataSize = parseInt(detailsRow.table_bytes || 0);
          indexSize = parseInt(detailsRow.index_bytes || 0);

          // Note: total includes toast and other overhead
          total = parseInt(detailsRow.total_bytes || 0);
        }

        // Get compression stats if available
        try {
          const compressionResult = await pgPool.query(
            `SELECT
              SUM(before_compression_total_bytes) as uncompressed_size,
              SUM(after_compression_total_bytes) as compressed_size
             FROM timescaledb_information.compressed_chunk_stats
             WHERE hypertable_name = $1`,
            [tableName]
          );

          if (
            compressionResult.rows[0] &&
            compressionResult.rows[0].uncompressed_size > 0
          ) {
            const uncompressed = parseInt(
              compressionResult.rows[0].uncompressed_size
            );
            const compressed = parseInt(
              compressionResult.rows[0].compressed_size
            );
            compressionStats = {
              uncompressed: uncompressed,
              compressed: compressed,
              ratio: (uncompressed / compressed).toFixed(2),
            };
          }
        } catch (err) {
          // Compression stats not available
        }
      } else {
        // Regular table - use standard PostgreSQL functions
        const result = await pgPool.query(
          `SELECT
            pg_total_relation_size($1) as total_size,
            pg_relation_size($1) as table_size,
            pg_indexes_size($1) as indexes_size`,
          [tableName]
        );

        const row = result.rows[0];
        dataSize = parseInt(row.table_size);
        indexSize = parseInt(row.indexes_size);
        total = parseInt(row.total_size);
      }
    } catch (err) {
      // Fallback to standard PostgreSQL functions if TimescaleDB not available
      const result = await pgPool.query(
        `SELECT
          pg_total_relation_size($1) as total_size,
          pg_relation_size($1) as table_size,
          pg_indexes_size($1) as indexes_size`,
        [tableName]
      );

      const row = result.rows[0];
      dataSize = parseInt(row.table_size);
      indexSize = parseInt(row.indexes_size);
      total = parseInt(row.total_size);
    }

    details[tableName] = {
      data: dataSize,
      indexes: indexSize,
      total: total,
      compression: compressionStats,
    };

    totalSize += dataSize;
    totalIndexSize += indexSize;
  }

  // Get the actual total from all chunks and tables
  let actualTotal = 0;
  try {
    const actualTotalResult = await pgPool.query(
      `
      SELECT SUM(total_bytes) as total
      FROM (
        SELECT hypertable_size(format('%I.%I', schema_name, table_name)::regclass) as total_bytes
        FROM timescaledb_information.hypertables
        WHERE hypertable_name = ANY($1::text[])
        UNION ALL
        SELECT pg_total_relation_size(tablename::regclass) as total_bytes
        FROM pg_tables
        WHERE tablename = ANY($1::text[])
        AND tablename NOT IN (
          SELECT hypertable_name FROM timescaledb_information.hypertables
        )
      ) sizes
    `,
      [tables]
    );

    actualTotal = parseInt(actualTotalResult.rows[0].total || 0);
  } catch (err) {
    // If query fails, use the sum we calculated
    actualTotal = totalSize + totalIndexSize;
  }

  return {
    totalData: totalSize,
    totalIndexes: totalIndexSize,
    totalSize: actualTotal || totalSize + totalIndexSize, // Use actual total if available
    details,
  };
}

// ============================================================================
// MONGODB QUERIES
// ============================================================================

async function mongoBacktestWithPositions(db) {
  const historyDocs = await db
    .collection("backtesthistories")
    .aggregate([
      { $match: { backtestId: new ObjectId(BACKTEST_ID) } },
      { $sort: { time: 1 } },
      {
        $project: {
          _id: 0,
          time: 1,
          value: 1,
          comparisonValue: 1,
          positions: 1,
        },
      },
    ])
    .toArray();

  const valueHistory = [];
  const comparisonHistory = [];
  let positionCount = 0;

  for (const doc of historyDocs) {
    valueHistory.push({
      time: doc.time,
      value: doc.value,
      positions: doc.positions || [],
    });
    positionCount += (doc.positions || []).length;
    if (doc.comparisonValue !== null && doc.comparisonValue !== undefined) {
      comparisonHistory.push({
        time: doc.time,
        value: doc.comparisonValue,
      });
    }
  }

  return { valueHistory, positionCount };
}

async function mongoPortfolioHistory(db) {
  const portfolioObjectId = new ObjectId(PORTFOLIO_ID);
  const now = new Date();
  const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
  const oneWeekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
  const oneMonthAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);

  const result = await db
    .collection("portfoliohistories")
    .aggregate([
      { $match: { portfolioId: portfolioObjectId } },
      { $sort: { time: 1 } },
      {
        $facet: {
          // Get bounds to determine first and last times
          bounds: [
            {
              $group: {
                _id: null,
                firstTime: { $min: "$time" },
                lastTime: { $max: "$time" },
                count: { $sum: 1 },
              },
            },
          ],
          // Get first document
          first: [{ $limit: 1 }],
          // Get last document
          last: [{ $sort: { time: -1 } }, { $limit: 1 }],
          // Get all middle documents for bucketing
          middle: [
            { $skip: 1 }, // Skip first
            {
              $addFields: {
                bucketCategory: {
                  $switch: {
                    branches: [
                      {
                        case: { $gte: ["$time", oneDayAgo] },
                        then: {
                          $dateTrunc: {
                            date: "$time",
                            unit: "minute",
                            binSize: 15,
                          },
                        },
                      },
                      {
                        case: { $gte: ["$time", oneWeekAgo] },
                        then: {
                          $dateTrunc: {
                            date: "$time",
                            unit: "hour",
                            binSize: 4,
                          },
                        },
                      },
                      {
                        case: { $gte: ["$time", oneMonthAgo] },
                        then: {
                          $dateTrunc: {
                            date: "$time",
                            unit: "hour",
                            binSize: 12,
                          },
                        },
                      },
                    ],
                    default: { $dateTrunc: { date: "$time", unit: "day" } },
                  },
                },
              },
            },
            {
              $group: {
                _id: "$bucketCategory",
                doc: { $first: "$$ROOT" },
              },
            },
            { $replaceRoot: { newRoot: "$doc" } },
          ],
        },
      },
      // Process facet results
      {
        $project: {
          allDocs: {
            $concatArrays: [
              "$first",
              {
                $cond: {
                  if: {
                    $and: [
                      { $gt: [{ $size: "$bounds" }, 0] },
                      { $gt: [{ $size: "$last" }, 0] },
                      {
                        $ne: [
                          { $arrayElemAt: ["$bounds.firstTime", 0] },
                          { $arrayElemAt: ["$bounds.lastTime", 0] },
                        ],
                      },
                    ],
                  },
                  then: {
                    $concatArrays: ["$middle", "$last"],
                  },
                  else: "$middle",
                },
              },
            ],
          },
        },
      },
      { $unwind: "$allDocs" },
      { $replaceRoot: { newRoot: "$allDocs" } },
      {
        $project: {
          _id: 0,
          time: 1,
          value: 1,
          positions: 1,
        },
      },
      { $sort: { time: 1 } },
    ])
    .toArray();

  const positionCount = result.reduce(
    (sum, doc) => sum + (doc.positions?.length || 0),
    0
  );

  return { valueHistory: result, positionCount };
}

// ============================================================================
// POSTGRESQL QUERIES
// ============================================================================

async function pgBacktestWithPositions(pgPool) {
  // Query 1: Get bucketed history
  const historyResult = await pgPool.query(
    `
    WITH numbered_history AS (
        SELECT
            *,
            ROW_NUMBER() OVER(ORDER BY time ASC) as rn_asc,
            ROW_NUMBER() OVER(ORDER BY time DESC) as rn_desc
        FROM backtest_history
        WHERE "backtestId" = $1
    )
    (
        SELECT time, value, "comparisonValue", time as representative_time
        FROM numbered_history
        WHERE rn_asc = 1
    )
    UNION ALL
    (
        SELECT
            time_bucket('1 day', time) as time,
            last(value, time) as value,
            last("comparisonValue", time) as "comparisonValue",
            last(time, time) as representative_time
        FROM numbered_history
        WHERE rn_asc > 1 AND rn_desc > 1
        GROUP BY 1
    )
    UNION ALL
    (
        SELECT time, value, "comparisonValue", time as representative_time
        FROM numbered_history
        WHERE rn_desc = 1 AND rn_asc > 1
    )
    ORDER BY time ASC;
  `,
    [BACKTEST_ID]
  );

  // Query 2: Get all positions
  const positionsResult = await pgPool.query(
    `
    SELECT
        time, symbol, name, type, quantity, "lastPrice", "averageCost"
    FROM backtest_positions
    WHERE "backtestId" = $1
    ORDER BY time ASC, symbol ASC;
  `,
    [BACKTEST_ID]
  );

  // Aggregate positions by time
  const positionsByTime = new Map();
  for (const row of positionsResult.rows) {
    const timeKey = row.time.getTime();
    if (!positionsByTime.has(timeKey)) {
      positionsByTime.set(timeKey, []);
    }
    positionsByTime.get(timeKey).push({
      asset: {
        symbol: row.symbol,
        name: row.name,
        type: row.type,
      },
      quantity: row.quantity,
      lastPrice: row.lastprice,
      averageCost: row.averagecost,
    });
  }

  // Combine results
  const valueHistory = [];
  const comparisonHistory = [];
  let positionCount = 0;

  for (const row of historyResult.rows) {
    const positions =
      positionsByTime.get(row.representative_time.getTime()) || [];
    valueHistory.push({
      time: row.time,
      value: row.value,
      positions: positions,
    });
    positionCount += positions.length;
    if (row.comparisonValue !== null) {
      comparisonHistory.push({
        time: row.time,
        value: row.comparisonValue,
      });
    }
  }

  return { valueHistory, positionCount };
}

async function pgPortfolioHistory(pgPool) {
  const result = await pgPool.query(
    `
    WITH
    portfolio_bounds AS (
        SELECT
            min(time) as first_time,
            max(time) as last_time
        FROM portfolio_history
        WHERE "portfolioId" = $1
    ),
    recent_15min AS (
        SELECT
            time_bucket('15 minutes', time) as bucket_time,
            min(time) as representative_time,
            first(value, time) as value
        FROM portfolio_history
        WHERE "portfolioId" = $1
          AND time >= now() - interval '1 day'
          AND time > (SELECT first_time FROM portfolio_bounds)
          AND time < (SELECT last_time FROM portfolio_bounds)
        GROUP BY 1
    ),
    week_4hour AS (
        SELECT
            time_bucket('4 hours', time) as bucket_time,
            min(time) as representative_time,
            first(value, time) as value
        FROM portfolio_history
        WHERE "portfolioId" = $1
          AND time >= now() - interval '1 week'
          AND time < now() - interval '1 day'
          AND time > (SELECT first_time FROM portfolio_bounds)
          AND time < (SELECT last_time FROM portfolio_bounds)
        GROUP BY 1
    ),
    month_12hour AS (
        SELECT
            time_bucket('12 hours', time) as bucket_time,
            min(time) as representative_time,
            first(value, time) as value
        FROM portfolio_history
        WHERE "portfolioId" = $1
          AND time >= now() - interval '1 month'
          AND time < now() - interval '1 week'
          AND time > (SELECT first_time FROM portfolio_bounds)
          AND time < (SELECT last_time FROM portfolio_bounds)
        GROUP BY 1
    ),
    older_daily AS (
        SELECT
            time_bucket('1 day', time) as bucket_time,
            min(time) as representative_time,
            first(value, time) as value
        FROM portfolio_history
        WHERE "portfolioId" = $1
          AND time < now() - interval '1 month'
          AND time > (SELECT first_time FROM portfolio_bounds)
          AND time < (SELECT last_time FROM portfolio_bounds)
        GROUP BY 1
    )
    (
        SELECT
            ph.time, ph.value, COALESCE(pos.positions, '[]'::jsonb) as positions
        FROM portfolio_history ph
        LEFT JOIN LATERAL (
             SELECT jsonb_agg(jsonb_build_object('asset', jsonb_build_object('symbol', pp.symbol, 'name', pp.name, 'type', pp.type), 'quantity', pp.quantity, 'lastPrice', pp."lastPrice", 'averageCost', pp."averageCost") ORDER BY pp.symbol) as positions
             FROM portfolio_positions pp WHERE pp."portfolioId" = $1 AND pp.time = ph.time
        ) AS pos ON true
        WHERE ph."portfolioId" = $1
          AND ph.time = (SELECT first_time FROM portfolio_bounds)
    )
    UNION ALL
    (
        SELECT
            ph.time, ph.value, COALESCE(pos.positions, '[]'::jsonb) as positions
        FROM portfolio_history ph
        LEFT JOIN LATERAL (
             SELECT jsonb_agg(jsonb_build_object('asset', jsonb_build_object('symbol', pp.symbol, 'name', pp.name, 'type', pp.type), 'quantity', pp.quantity, 'lastPrice', pp."lastPrice", 'averageCost', pp."averageCost") ORDER BY pp.symbol) as positions
             FROM portfolio_positions pp WHERE pp."portfolioId" = $1 AND pp.time = ph.time
        ) AS pos ON true
        WHERE ph."portfolioId" = $1
          AND ph.time = (SELECT last_time FROM portfolio_bounds)
          AND (SELECT first_time FROM portfolio_bounds) != (SELECT last_time FROM portfolio_bounds)
    )
    UNION ALL
    (
        SELECT 
            r15.bucket_time as time, 
            r15.value,
            COALESCE(pos.positions, '[]'::jsonb) as positions
        FROM recent_15min r15
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(jsonb_build_object('asset', jsonb_build_object('symbol', pp.symbol, 'name', pp.name, 'type', pp.type), 'quantity', pp.quantity, 'lastPrice', pp."lastPrice", 'averageCost', pp."averageCost") ORDER BY pp.symbol) as positions
            FROM portfolio_positions pp WHERE pp."portfolioId" = $1 AND pp.time = r15.representative_time
        ) AS pos ON true
    )
    UNION ALL
    (
        SELECT 
            w4h.bucket_time as time,
            w4h.value,
            COALESCE(pos.positions, '[]'::jsonb) as positions
        FROM week_4hour w4h
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(jsonb_build_object('asset', jsonb_build_object('symbol', pp.symbol, 'name', pp.name, 'type', pp.type), 'quantity', pp.quantity, 'lastPrice', pp."lastPrice", 'averageCost', pp."averageCost") ORDER BY pp.symbol) as positions
            FROM portfolio_positions pp WHERE pp."portfolioId" = $1 AND pp.time = w4h.representative_time
        ) AS pos ON true
    )
    UNION ALL
    (
        SELECT 
            m12h.bucket_time as time,
            m12h.value,
            COALESCE(pos.positions, '[]'::jsonb) as positions
        FROM month_12hour m12h
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(jsonb_build_object('asset', jsonb_build_object('symbol', pp.symbol, 'name', pp.name, 'type', pp.type), 'quantity', pp.quantity, 'lastPrice', pp."lastPrice", 'averageCost', pp."averageCost") ORDER BY pp.symbol) as positions
            FROM portfolio_positions pp WHERE pp."portfolioId" = $1 AND pp.time = m12h.representative_time
        ) AS pos ON true
    )
    UNION ALL
    (
        SELECT 
            od.bucket_time as time,
            od.value,
            COALESCE(pos.positions, '[]'::jsonb) as positions
        FROM older_daily od
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(jsonb_build_object('asset', jsonb_build_object('symbol', pp.symbol, 'name', pp.name, 'type', pp.type), 'quantity', pp.quantity, 'lastPrice', pp."lastPrice", 'averageCost', pp."averageCost") ORDER BY pp.symbol) as positions
            FROM portfolio_positions pp WHERE pp."portfolioId" = $1 AND pp.time = od.representative_time
        ) AS pos ON true
    )
    ORDER BY time ASC;
  `,
    [PORTFOLIO_ID]
  );

  const valueHistory = result.rows;
  const positionCount = valueHistory.reduce((sum, row) => {
    return sum + (row.positions ? row.positions.length : 0);
  }, 0);

  return { valueHistory, positionCount };
}

// ============================================================================
// BENCHMARK RUNNERS
// ============================================================================

async function runQuery(db, queryName, queryFn, dbType) {
  const times = [];
  const numBatches = Math.ceil(TOTAL_RUNS / PARALLEL_BATCH_SIZE);

  for (let batch = 0; batch < numBatches; batch++) {
    const batchStart = batch * PARALLEL_BATCH_SIZE;
    const batchSize = Math.min(PARALLEL_BATCH_SIZE, TOTAL_RUNS - batchStart);

    console.log(
      `  Batch ${batch + 1}/${numBatches} (${batchSize} parallel requests):`
    );

    const promises = [];
    for (let i = 0; i < batchSize; i++) {
      const runNumber = batchStart + i + 1;
      promises.push(
        (async () => {
          const start = Date.now();
          const result = await queryFn(db);
          const elapsed = Date.now() - start;
          return { runNumber, elapsed, result };
        })()
      );
    }

    const batchResults = await Promise.all(promises);

    for (const { runNumber, elapsed, result } of batchResults) {
      times.push(elapsed);
      const rowCount = result.valueHistory.length;
      const posCount = result.positionCount;
      console.log(
        `    Run ${runNumber}: ${formatTime(
          elapsed
        )} (${rowCount.toLocaleString()} rows, ${posCount} positions)`
      );
    }

    if (batch < numBatches - 1) {
      await sleep(200);
    }
  }

  return calculateStats(times);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

async function runBenchmark() {
  console.log("═══════════════════════════════════════════════════════════");
  console.log("  DATABASE COMPARISON: MONGODB vs POSTGRESQL/TIMESCALEDB");
  console.log("  (20 requests in batches of 4 parallel)");
  console.log("═══════════════════════════════════════════════════════════\n");

  let mongoClient, mongoDb, pgPool;
  const results = {};

  try {
    // Connect to databases
    console.log("Connecting to MongoDB...");
    mongoClient = new MongoClient(args.mongo, {
      maxPoolSize: MONGO_POOL_SIZE,
      minPoolSize: MONGO_POOL_SIZE,
    });
    await mongoClient.connect();
    mongoDb = mongoClient.db("nexustrade");
    console.log(`✓ MongoDB connected (pool size: ${MONGO_POOL_SIZE})`);

    console.log("Connecting to PostgreSQL...");
    pgPool = new Pool({
      connectionString: args.postgres,
      max: PG_POOL_SIZE,
      min: PG_POOL_SIZE,
    });
    console.log(`✓ PostgreSQL connected (pool size: ${PG_POOL_SIZE})\n`);

    // Get storage metrics
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    console.log("STORAGE ANALYSIS");
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    const mongoStorage = await getMongoStorageMetrics(mongoDb);

    console.log("MongoDB Collections:");
    for (const [coll, stats] of Object.entries(mongoStorage.details)) {
      console.log(`  ${coll}:`);
      console.log(`    Data:        ${formatBytes(stats.data)} (compressed)`);
      console.log(`    Uncompressed: ${formatBytes(stats.uncompressed)}`);
      console.log(`    Compression:  ${stats.compressionRatio}x`);
      console.log(`    Indexes:     ${formatBytes(stats.indexes)}`);
      console.log(`    Total:       ${formatBytes(stats.total)}`);
    }
    console.log(
      `\n  MongoDB Total (data + indexes): ${formatBytes(
        mongoStorage.totalSize
      )}`
    );
    console.log(`    - Data:    ${formatBytes(mongoStorage.totalData)}`);
    console.log(`    - Indexes: ${formatBytes(mongoStorage.totalIndexes)}`);

    const pgStorage = await getPostgresStorageMetrics(pgPool);

    console.log("\n\nPostgreSQL/TimescaleDB Tables:");
    for (const [table, stats] of Object.entries(pgStorage.details)) {
      console.log(`  ${table}:`);
      console.log(`    Data:         ${formatBytes(stats.data)}`);
      if (stats.compression) {
        console.log(
          `    Uncompressed: ${formatBytes(stats.compression.uncompressed)}`
        );
        console.log(`    Compression:  ${stats.compression.ratio}x`);
      }
      console.log(`    Indexes:      ${formatBytes(stats.indexes)}`);
      console.log(`    Total:        ${formatBytes(stats.total)}`);
    }
    console.log(
      `\n  PostgreSQL/TimescaleDB Total (data + indexes): ${formatBytes(
        pgStorage.totalSize
      )}`
    );
    console.log(`    - Data:    ${formatBytes(pgStorage.totalData)}`);
    console.log(`    - Indexes: ${formatBytes(pgStorage.totalIndexes)}`);

    // Run benchmarks
    console.log("\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    console.log("QUERY PERFORMANCE");
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    console.log("BACKTEST: Positions + History");
    console.log("─────────────────────────────────────────────────────────\n");

    console.log("MongoDB:");
    results.mongo_backtest = await runQuery(
      mongoDb,
      "Backtest",
      mongoBacktestWithPositions,
      "mongo"
    );

    console.log("\nPostgreSQL:");
    results.pg_backtest = await runQuery(
      pgPool,
      "Backtest",
      pgBacktestWithPositions,
      "postgres"
    );

    console.log("\n\nPORTFOLIO: Multi-resolution bucketing with positions");
    console.log("─────────────────────────────────────────────────────────\n");

    console.log("MongoDB:");
    results.mongo_portfolio = await runQuery(
      mongoDb,
      "Portfolio",
      mongoPortfolioHistory,
      "mongo"
    );

    console.log("\nPostgreSQL:");
    results.pg_portfolio = await runQuery(
      pgPool,
      "Portfolio",
      pgPortfolioHistory,
      "postgres"
    );

    // Print summary
    console.log("\n\n");
    console.log("═══════════════════════════════════════════════════════════");
    console.log("  COMPREHENSIVE RESULTS");
    console.log(
      "═══════════════════════════════════════════════════════════\n"
    );

    // Performance Summary
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    console.log("QUERY PERFORMANCE COMPARISON");
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    const printComparison = (name, mongoStats, pgStats) => {
      const diff = (
        ((pgStats.avg - mongoStats.avg) / mongoStats.avg) *
        100
      ).toFixed(1);
      const faster = pgStats.avg < mongoStats.avg ? "PostgreSQL" : "MongoDB";
      const pct = Math.abs(diff);

      console.log(`${name}:`);
      console.log(`  MongoDB:`);
      console.log(`    Avg:    ${formatTime(mongoStats.avg)}`);
      console.log(`    Median: ${formatTime(mongoStats.median)}`);
      console.log(`    P95:    ${formatTime(mongoStats.p95)}`);
      console.log(
        `    Range:  ${formatTime(mongoStats.min)} - ${formatTime(
          mongoStats.max
        )}`
      );
      console.log(`  PostgreSQL:`);
      console.log(`    Avg:    ${formatTime(pgStats.avg)}`);
      console.log(`    Median: ${formatTime(pgStats.median)}`);
      console.log(`    P95:    ${formatTime(pgStats.p95)}`);
      console.log(
        `    Range:  ${formatTime(pgStats.min)} - ${formatTime(pgStats.max)}`
      );
      console.log(`  Winner: ${faster} is ${pct}% faster\n`);
    };

    printComparison(
      "Backtest Query",
      results.mongo_backtest,
      results.pg_backtest
    );
    printComparison(
      "Portfolio Query",
      results.mongo_portfolio,
      results.pg_portfolio
    );

    const mongoTotal = results.mongo_backtest.avg + results.mongo_portfolio.avg;
    const pgTotal = results.pg_backtest.avg + results.pg_portfolio.avg;
    const totalDiff = (((pgTotal - mongoTotal) / mongoTotal) * 100).toFixed(1);
    const totalFaster = pgTotal < mongoTotal ? "PostgreSQL" : "MongoDB";

    console.log("Combined Total (both queries):");
    console.log(`  MongoDB:    ${formatTime(mongoTotal)}`);
    console.log(`  PostgreSQL: ${formatTime(pgTotal)}`);
    console.log(`  Winner: ${totalFaster} is ${Math.abs(totalDiff)}% faster`);

    // Storage Summary
    console.log("\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    console.log("STORAGE COMPARISON");
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    const mongoGB = mongoStorage.totalSize / (1024 * 1024 * 1024);
    const pgGB = pgStorage.totalSize / (1024 * 1024 * 1024);
    const storageDiff = (((pgGB - mongoGB) / mongoGB) * 100).toFixed(1);

    console.log(
      `MongoDB:                ${formatBytes(mongoStorage.totalSize)}`
    );
    console.log(`PostgreSQL/TimescaleDB: ${formatBytes(pgStorage.totalSize)}`);
    console.log(
      `\nDifference: PostgreSQL uses ${storageDiff}% ${
        pgGB > mongoGB ? "more" : "less"
      } storage than MongoDB`
    );

    // Cost Analysis
    console.log("\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    console.log("COST ANALYSIS (4 CPU / 16GB Memory)");
    console.log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n");

    console.log(
      "Current MongoDB Setup (90GB storage, includes operational data):"
    );
    console.log("  Monthly Cost: $231.35");
    console.log("  Storage:      90 GB total");
    console.log(`  Time Series:  ${formatBytes(mongoStorage.totalSize)}`);

    console.log(
      "\nPostgreSQL/TimescaleDB Option (separate time series database):"
    );
    console.log("  Monthly Cost: $590.00 (additional, time series only)");
    console.log(
      `  Storage:      ${formatBytes(pgStorage.totalSize)} (time series data)`
    );
    console.log("  Note:         Would be in addition to operational database");

    console.log("\n\nCOST SUMMARY:");
    console.log("──────────────────────────────────────────────────────────");
    console.log("  MongoDB (current):    $231.35/month (all-in-one)");
    console.log("  PostgreSQL (new):     $821.35/month ($231.35 + $590.00)");
    console.log("  Additional Cost:      $590.00/month (+255%)");
    console.log("──────────────────────────────────────────────────────────\n");
  } catch (error) {
    console.error("Error running benchmark:", error);
    process.exit(1);
  } finally {
    if (mongoClient) await mongoClient.close();
    if (pgPool) await pgPool.end();
  }
}

runBenchmark().catch(console.error);
