# MongoDB vs PostgreSQL/TimescaleDB Performance Benchmark

A comprehensive performance comparison tool for time-series workloads, comparing MongoDB's document-based storage against PostgreSQL with TimescaleDB extension.

> **Article**: [I went through hell and back migrating to TimescaleDB. I didn't last two weeks.](https://medium.com/p/3e2c4fda4e06/)

## Quick Results Summary

| Metric | MongoDB | PostgreSQL/TimescaleDB | Winner |
|--------|---------|------------------------|---------|
| **Total Storage** | 7.73 GB | 136.93 GB | MongoDB (17.7x smaller) |
| **Backtest Query (avg)** | 274ms | 549ms | MongoDB (99.9% faster) |
| **Portfolio Query (avg)** | 938ms | 716ms | PostgreSQL (23.7% faster) |
| **Combined Performance** | 1,213ms | 1,265ms | MongoDB (4.3% faster) |
| **Monthly Cost** | $231.35 | $821.35 | MongoDB (255% cheaper) |

## Overview

This benchmark evaluates database performance for financial time-series data, specifically:
- **Backtest histories** with embedded position data
- **Portfolio histories** with multi-resolution time bucketing
- **Storage efficiency** including compression and indexing
- **Query performance** under concurrent load (20 requests in batches of 4)

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd database-benchmark

# Install dependencies
npm install mongodb pg
```

## Usage

```bash
node benchmark.js --mongo=<mongodb-connection-string> --postgres=<postgresql-connection-string>
```

### Example

```bash
node benchmark.js \
  --mongo="mongodb://user:pass@host:27017/nexustrade" \
  --postgres="postgresql://user:pass@host:5432/timeseries"
```

## Benchmark Configuration

```javascript
const BACKTEST_ID = "68d03f8c394ed157de606401";
const PORTFOLIO_ID = "66490bb3c319fe3771bfe0ec";
const TOTAL_RUNS = 20;
const PARALLEL_BATCH_SIZE = 4;
const MONGO_POOL_SIZE = 10;
const PG_POOL_SIZE = 10;
```

## Test Scenarios

### 1. Backtest Query
- Retrieves historical backtest data with embedded positions
- **MongoDB**: 381 rows with 1,900 positions embedded
- **PostgreSQL**: 259 rows with 1,290 positions in separate table

### 2. Portfolio Query  
- Multi-resolution time bucketing based on age of data:
  - Last 24 hours: 15-minute buckets
  - Last week: 4-hour buckets
  - Last month: 12-hour buckets
  - Older: Daily buckets
- Includes position data at each time point

## Database Schema

### MongoDB Collections
```javascript
// backtesthistories
{
  backtestId: ObjectId,
  time: Date,
  value: Number,
  comparisonValue: Number,
  positions: [
    {
      asset: { symbol, name, type },
      quantity: Number,
      lastPrice: Number,
      averageCost: Number
    }
  ]
}

// portfoliohistories
{
  portfolioId: ObjectId,
  time: Date,
  value: Number,
  positions: [...] // Same structure as above
}
```

### PostgreSQL/TimescaleDB Tables
```sql
-- Hypertables with time-based partitioning
CREATE TABLE backtest_history (
  backtestId TEXT,
  time TIMESTAMPTZ,
  value NUMERIC,
  comparisonValue NUMERIC
);

CREATE TABLE backtest_positions (
  backtestId TEXT,
  time TIMESTAMPTZ,
  symbol TEXT,
  name TEXT,
  type TEXT,
  quantity NUMERIC,
  lastPrice NUMERIC,
  averageCost NUMERIC
);

-- Similar structure for portfolio_history and portfolio_positions
```

## Key Findings

### Storage Efficiency
MongoDB's document model with embedded positions provides **17.7x better storage efficiency** compared to PostgreSQL's normalized tables:
- MongoDB leverages compression (8.79x for backtest data)
- PostgreSQL requires massive indexes (97.10 GB) for time-series queries
- Embedded documents eliminate join overhead

### Query Performance
- **Backtest queries**: MongoDB 99.9% faster due to document locality
- **Portfolio queries**: PostgreSQL 23.7% faster with optimized time bucketing
- **Overall**: MongoDB 4.3% faster for combined workload

### Cost Analysis
For equivalent 4 CPU / 16GB configurations:
- **MongoDB on DigitalOcean**: $231.35/month (all-in-one solution)
- **Timescale Cloud**: $821.35/month (requires separate time-series instance)
- **Savings**: MongoDB reduces costs by 255%

## Methodology

1. **Connection Pooling**: Both databases use 10 connections for fair comparison
2. **Parallel Load**: 4 concurrent requests per batch to simulate real-world usage
3. **Warm Cache**: First batch may show higher latency due to cold cache
4. **Statistical Analysis**: Captures average, median, P95, and range across 20 runs

## Cloud Platforms Tested

- **MongoDB**: DigitalOcean Managed Database (4 CPU / 16GB RAM)
- **TimescaleDB**: Timescale Cloud (4 CPU / 16GB RAM)

Both platforms were configured with equivalent resources for fair comparison.

## Requirements

- Node.js 14+
- MongoDB 4.4+ (with compression enabled)
- PostgreSQL 12+ with TimescaleDB 2.0+

## Dependencies

```json
{
  "mongodb": "^5.0.0",
  "pg": "^8.0.0"
}
```

## Output

The script provides comprehensive metrics including:
- Storage analysis with compression ratios
- Query performance statistics (avg, median, P95)
- Cost comparison based on cloud pricing
- Detailed per-run timing data

## License

MIT

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## Author

Austin Starks

## Related Article

This benchmark is supplementary material for: [I went through hell and back migrating to TimescaleDB. I didn't last two weeks.](https://medium.com/p/3e2c4fda4e06/)

---

*Note: Results may vary based on hardware, network conditions, and database configurations. This benchmark represents a specific workload pattern and may not reflect all use cases. Tests were conducted on DigitalOcean Managed MongoDB vs Timescale Cloud services.*
