# Analytics (Embedded SQL on Streams)

Streamline's analytics module provides **zero-ETL SQL analytics** on streaming
data using an embedded [DuckDB](https://duckdb.org/) engine. Query topics
directly with standard SQL—no separate data warehouse required.

> **Stability: Stable** — Breaking changes only in major versions.

## Architecture

```
┌────────────┐        ┌──────────────────────┐        ┌────────────┐
│ CLI / HTTP │──SQL──▶ │   DuckDB Engine       │──read─▶│  Topics    │
│  client    │◀─JSON──│  (in-memory tables)   │        │ (storage)  │
└────────────┘        └──────────────────────┘        └────────────┘
```

1. **Topic extraction** — A regex scanner detects `streamline_topic('name')`
   (or `streamline_topic_<name>`) references in the SQL string.
2. **Table materialisation** — Each referenced topic is read partition-by-partition
   through the `TopicDataSource` trait and loaded into a temporary DuckDB table.
3. **Query execution** — Standard SQL (including aggregations, joins, window
   functions) runs against the materialised tables.
4. **Result delivery** — Results are returned as JSON rows with pagination,
   caching, and optional cursor-based iteration.

## Prerequisites

Build Streamline with the `analytics` feature:

```bash
cargo build --features analytics
# or the full edition
cargo build --features full
```

## Supported SQL Syntax & Functions

The analytics engine supports the full DuckDB SQL dialect:

### Data Types

`BOOLEAN`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`,
`VARCHAR`, `BLOB`, `TIMESTAMP`, `DATE`, `TIME`, `INTERVAL`, `JSON`, `MAP`, `LIST`.

### Aggregate Functions

`COUNT(*)`, `SUM(expr)`, `AVG(expr)`, `MIN(expr)`, `MAX(expr)`,
`STDDEV(expr)`, `VARIANCE(expr)`, `STRING_AGG(expr, sep)`,
`APPROX_COUNT_DISTINCT(expr)`, `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY expr)`.

### Window Functions

`ROW_NUMBER()`, `RANK()`, `DENSE_RANK()`, `NTILE(n)`,
`LAG(expr, offset)`, `LEAD(expr, offset)`,
`FIRST_VALUE(expr)`, `LAST_VALUE(expr)`,
`SUM(...) OVER (...)`, `AVG(...) OVER (...)`.

### JSON Functions

```sql
-- Arrow operator (extract as string)
value->>'field_name'

-- Extract nested fields
value->'nested'->'field'

-- DuckDB JSON functions
json_extract(value, '$.field')
json_extract_string(value, '$.field')
```

### Date/Time Functions

`now()`, `current_timestamp`, `date_trunc('unit', ts)`, `date_part('unit', ts)`,
`age(ts1, ts2)`, `epoch_ms(bigint)`.

Interval arithmetic: `timestamp + interval '1 hour'`,
`timestamp - interval '7 days'`.

## Query Examples

### Virtual Tables

Topics are exposed as virtual tables. Two syntaxes are supported:

```sql
-- Function-call syntax (recommended)
SELECT * FROM streamline_topic('events') LIMIT 10

-- Direct table name
SELECT * FROM streamline_topic_events LIMIT 10
```

Each record has these columns:

| Column      | Type      | Description                |
|-------------|-----------|----------------------------|
| `offset`    | BIGINT    | Record offset in partition |
| `partition` | INTEGER   | Partition number           |
| `timestamp` | TIMESTAMP | Record timestamp           |
| `key`       | VARCHAR   | Record key (if present)    |
| `value`     | VARCHAR   | JSON-encoded payload       |
| `headers`   | VARCHAR   | JSON-encoded headers       |

### Aggregation

```sql
SELECT
  value->>'action' AS action,
  COUNT(*) AS cnt,
  SUM(CAST(value->>'amount' AS INTEGER)) AS total,
  AVG(CAST(value->>'amount' AS DOUBLE)) AS avg_amount
FROM streamline_topic('events')
GROUP BY value->>'action'
ORDER BY cnt DESC
```

### Filtering

```sql
SELECT *
FROM streamline_topic('events')
WHERE value->>'action' = 'purchase'
  AND CAST(value->>'amount' AS INTEGER) > 50
```

### Time-Based Queries

```sql
-- Last hour
SELECT * FROM streamline_topic('events')
WHERE timestamp > now() - interval '1 hour'

-- Hourly buckets
SELECT
  date_trunc('hour', timestamp) AS hour,
  COUNT(*) AS events
FROM streamline_topic('events')
GROUP BY 1
ORDER BY 1
```

### Cross-Topic Joins

```sql
SELECT
  e.key AS user_id,
  e.value->>'action' AS action,
  m.value->>'cpu' AS cpu
FROM streamline_topic('events') e
JOIN streamline_topic('metrics') m
  ON date_trunc('second', e.timestamp) = date_trunc('second', m.timestamp)
```

### Window Functions

```sql
SELECT
  key,
  value->>'amount' AS amount,
  ROW_NUMBER() OVER (PARTITION BY key ORDER BY timestamp) AS seq,
  SUM(CAST(value->>'amount' AS INTEGER))
    OVER (PARTITION BY key ORDER BY timestamp) AS running_total
FROM streamline_topic('events')
```

## Performance Tips

1. **Use `LIMIT`** — Avoid `SELECT *` without a limit on large topics.
2. **Filter by partition** — Reduce data scanned:
   ```sql
   SELECT * FROM streamline_topic('events')
   WHERE partition = 0
   ```
3. **Enable caching** — Repeated queries are served from cache (default TTL: 60 s).
   Disable with `--no-cache` for real-time needs.
4. **Use `--max-rows`** — Cap result set size from the CLI.
5. **Materialised views** — Pre-compute expensive aggregations and refresh them
   on a schedule.
6. **Use EXPLAIN** — Inspect the query plan before running heavy queries:
   ```bash
   streamline-cli query "SELECT ..." --explain
   ```

## API Reference

### CLI Commands

```bash
# Execute a query
streamline-cli query "SELECT * FROM streamline_topic('events') LIMIT 10"

# JSON output
streamline-cli query "..." --output-format json

# Limit rows
streamline-cli query "..." --max-rows 500

# Disable caching
streamline-cli query "..." --no-cache

# Set timeout (seconds)
streamline-cli query "..." --timeout 60

# Show execution plan
streamline-cli query "..." --explain

# Use SQLite engine instead of DuckDB
streamline-cli query "..." --engine sqlite
```

### HTTP Endpoints

All endpoints are served on the HTTP API port (default `:9094`).

| Method   | Path                            | Description                   |
|----------|---------------------------------|-------------------------------|
| `POST`   | `/api/v1/query`                 | Execute a SQL query           |
| `POST`   | `/api/v1/query/explain`         | Get query execution plan      |
| `GET`    | `/api/v1/query/cache/stats`     | Query cache statistics        |
| `DELETE`  | `/api/v1/query/cache`           | Clear query cache             |
| `POST`   | `/api/v1/views`                 | Create a materialised view    |
| `GET`    | `/api/v1/views`                 | List materialised views       |
| `GET`    | `/api/v1/views/{name}`          | Get a specific view           |
| `DELETE`  | `/api/v1/views/{name}`          | Delete a view                 |
| `POST`   | `/api/v1/views/{name}/refresh`  | Refresh a view                |
| `GET`    | `/api/v1/views/{name}/query`    | Query a view                  |

#### POST /api/v1/query

```bash
curl -X POST http://localhost:9094/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{
    "sql": "SELECT COUNT(*) FROM streamline_topic_events",
    "max_rows": 100,
    "timeout_ms": 30000,
    "enable_cache": true,
    "cache_ttl_seconds": 60
  }'
```

**Cursor-based pagination:**

```bash
# First page
curl -X POST http://localhost:9094/api/v1/query \
  -d '{"sql": "SELECT * FROM streamline_topic_events", "max_rows": 50}'

# Response includes: "next_cursor": "NTA"

# Next page
curl -X POST http://localhost:9094/api/v1/query \
  -d '{"sql": "SELECT * FROM streamline_topic_events", "max_rows": 50, "cursor": "NTA"}'
```

#### POST /api/v1/query/explain

```bash
curl -X POST http://localhost:9094/api/v1/query/explain \
  -H 'Content-Type: application/json' \
  -d '{"sql": "SELECT COUNT(*) FROM streamline_topic_events"}'
```

Returns the DuckDB query plan, referenced topics, and rewritten SQL.

## Materialised Views

```bash
# Create a view (HTTP)
curl -X POST http://localhost:9094/api/v1/views \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "hourly_events",
    "query": "SELECT date_trunc('"'"'hour'"'"', timestamp) AS hour, COUNT(*) AS cnt FROM streamline_topic_events GROUP BY 1",
    "refresh_interval_seconds": 300
  }'

# Refresh
curl -X POST http://localhost:9094/api/v1/views/hourly_events/refresh

# Query
curl http://localhost:9094/api/v1/views/hourly_events/query
```

## QueryOptions Reference

| Field                | Type         | Default  | Description                          |
|----------------------|-------------|----------|--------------------------------------|
| `max_rows`           | `usize?`    | `10 000` | Maximum rows to return               |
| `enable_cache`       | `bool`      | `true`   | Enable result caching                |
| `cache_ttl_seconds`  | `u64`       | `60`     | Cache TTL in seconds                 |
| `timeout_ms`         | `u64?`      | `30 000` | Query timeout in milliseconds        |
| `offset`             | `usize`     | `0`      | Pagination offset (rows to skip)     |

## Comparison with Alternatives

| Feature                | Streamline Analytics | ksqlDB            | Flink SQL          |
|------------------------|---------------------|-------------------|--------------------|
| Deployment             | Embedded (single binary) | Separate cluster | Separate cluster   |
| Query latency          | Sub-second (in-memory) | Seconds           | Seconds            |
| Dependency             | None (bundled DuckDB)  | Kafka + ksqlDB    | Kafka + Flink      |
| SQL dialect            | DuckDB (PostgreSQL-like) | KSQL             | Flink SQL          |
| Window functions       | ✅ Full DuckDB set    | ✅ Tumbling/hopping | ✅ Full set        |
| Joins                  | ✅ Standard SQL       | ✅ Stream–table    | ✅ Full set        |
| Persistent queries     | Materialised views    | Push queries      | Continuous queries |
| Use case               | Ad-hoc + lightweight  | Real-time streams | Heavy-duty ETL     |

**When to use Streamline Analytics:**
- Quick ad-hoc exploration of topic data
- Single-node deployments where simplicity matters
- Prototyping before moving to a dedicated stream processor

**When to use ksqlDB / Flink SQL:**
- Continuous push-based queries on unbounded streams
- Multi-node distributed processing
- Complex event processing (CEP) patterns
