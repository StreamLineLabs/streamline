# SQLite Query Interface — Design & Prototype

> **Status**: Experiment  
> **Feature flag**: `sqlite-query`  
> **Related ADR**: [ADR-027](../../../streamline-docs/docs/architecture/decisions/adr-027-sqlite-query.md)

## Overview

This document describes the design for a SQLite-compatible query interface that exposes Streamline topics as virtual tables. The goal is to let users run ad-hoc SQL queries against live stream data with zero additional infrastructure.

## Stream-to-Table Mapping

Every Streamline topic is surfaced as a virtual table with a fixed schema:

```sql
CREATE VIRTUAL TABLE <topic_name> USING streamline(
    key TEXT,
    value TEXT,
    partition INTEGER,
    offset INTEGER,
    timestamp DATETIME,
    headers JSON
);
```

| Column | Type | Description |
|---|---|---|
| `key` | `TEXT` | Record key (Base64 for binary keys, UTF-8 otherwise) |
| `value` | `TEXT` | Record value (Base64 for binary, UTF-8 otherwise) |
| `partition` | `INTEGER` | Partition the record belongs to |
| `offset` | `INTEGER` | Offset within the partition |
| `timestamp` | `DATETIME` | Record timestamp (broker or producer timestamp) |
| `headers` | `JSON` | Record headers as a JSON object |

Topics are discovered dynamically — no manual table registration required.

## Example Queries

### Basic exploration

```sql
-- Latest 100 records from a topic
SELECT key, value, partition, offset FROM orders LIMIT 100;

-- Records from the last hour
SELECT * FROM events WHERE timestamp > datetime('now', '-1 hour');

-- Filter by key
SELECT * FROM users WHERE key = 'user-42';
```

### Aggregations

```sql
-- Daily event counts
SELECT date(timestamp), count(*) FROM events GROUP BY 1;

-- Records per partition
SELECT partition, count(*) FROM orders GROUP BY partition;

-- Distinct keys in a time window
SELECT count(DISTINCT key) FROM events
WHERE timestamp BETWEEN '2025-07-01' AND '2025-07-14';
```

### JSON extraction (for JSON-encoded values)

```sql
-- Extract fields from JSON values
SELECT
    json_extract(value, '$.customer_id') AS customer,
    json_extract(value, '$.total') AS total,
    timestamp
FROM orders
WHERE json_extract(value, '$.status') = 'completed'
ORDER BY timestamp DESC
LIMIT 50;
```

### Cross-topic joins

```sql
-- Join events with orders on a shared ID
SELECT
    e.key AS event_key,
    json_extract(e.value, '$.type') AS event_type,
    json_extract(o.value, '$.total') AS order_total,
    e.timestamp
FROM events e
JOIN orders o
    ON json_extract(e.value, '$.order_id') = json_extract(o.value, '$.id')
WHERE e.timestamp > datetime('now', '-1 day');
```

## Implementation Options

### Option 1: SQLite Virtual Table Extension (Recommended for prototype)

Build a C shared library implementing the SQLite virtual table interface (`sqlite3_module`). Streamline loads it as an extension.

**Pros:**
- Full SQLite compatibility — any SQLite client works out of the box.
- Leverages SQLite's battle-tested query planner and execution engine.
- `json_extract`, `datetime`, and all SQLite built-ins are available for free.

**Cons:**
- Requires a C FFI boundary (Rust ↔ C via `libsqlite3-sys`).
- SQLite is single-writer; concurrent queries need connection pooling.

**Architecture:**

```
┌─────────────────────────────────────────────┐
│  Client (CLI / JDBC / sqlite3 shell)        │
└──────────────┬──────────────────────────────┘
               │ SQLite wire protocol / API
┌──────────────▼──────────────────────────────┐
│  SQLite Engine (embedded)                   │
│  ┌────────────────────────────────────────┐ │
│  │  streamline virtual table module       │ │
│  │  xConnect / xBestIndex / xFilter /     │ │
│  │  xNext / xColumn / xEof               │ │
│  └────────────┬───────────────────────────┘ │
└───────────────┼─────────────────────────────┘
                │ Rust FFI
┌───────────────▼─────────────────────────────┐
│  Streamline Storage Engine                  │
│  (log segments, indexes, cache)             │
└─────────────────────────────────────────────┘
```

### Option 2: Custom SQL Parser (SQLite syntax subset)

Parse a subset of SQLite-compatible SQL and translate directly into Streamline storage reads.

**Pros:**
- No C dependency; pure Rust.
- Full control over query planning and pushdown.

**Cons:**
- Large implementation effort to support useful SQL surface area.
- Incompatible with standard SQLite clients.
- Must reimplement functions like `json_extract`, `datetime`, etc.

### Option 3: Translation Layer (SQLite SQL → DuckDB)

Parse incoming SQLite SQL and rewrite it into DuckDB queries, delegating execution to the existing analytics module.

**Pros:**
- Reuses the existing DuckDB analytics infrastructure.
- DuckDB handles query optimization and execution.

**Cons:**
- Requires the `analytics` feature flag — not zero-config.
- SQL dialect differences between SQLite and DuckDB can cause subtle bugs.
- Adds a translation layer that is hard to keep in sync.

### Recommendation

**Option 1** for the prototype. It provides the widest compatibility with the least custom code. Option 3 is a viable follow-up for queries that need DuckDB's analytical performance.

## CLI Integration

New subcommand on `streamline-cli`:

```bash
# One-shot query
streamline-cli sql "SELECT count(*) FROM events WHERE timestamp > datetime('now', '-1 hour')"

# Interactive REPL
streamline-cli sql
streamline> SELECT * FROM orders LIMIT 5;
streamline> .tables
events  orders  users  clicks
streamline> .schema orders
CREATE VIRTUAL TABLE orders USING streamline(key TEXT, value TEXT, partition INTEGER, offset INTEGER, timestamp DATETIME, headers JSON);
streamline> .quit

# Output formats
streamline-cli sql --format json "SELECT key, value FROM events LIMIT 3"
streamline-cli sql --format csv "SELECT date(timestamp), count(*) FROM events GROUP BY 1"
streamline-cli sql --format table "SELECT partition, count(*) FROM orders GROUP BY partition"
```

Connection flags:

```bash
streamline-cli sql --host localhost --port 9095 "SELECT ..."
```

The default port `9095` is chosen to avoid conflicts with Kafka (9092) and HTTP API (9094).

## JDBC/ODBC Driver for BI Tools

A thin JDBC driver wraps the SQLite wire protocol endpoint, enabling BI tools to query Streamline directly:

```
Grafana / Metabase / Tableau
        │
        ▼
  JDBC Driver (streamline-sqlite-jdbc)
        │
        ▼ SQLite wire protocol (port 9095)
  Streamline Server
```

The driver advertises itself as a SQLite-compatible data source, so BI tools can use their existing SQLite connectors with minimal configuration.

## Performance Considerations

### Query pushdown

The virtual table module implements `xBestIndex` to push filters down to the storage engine:

| Filter | Pushdown | Notes |
|---|---|---|
| `timestamp > X` | ✅ | Translates to segment range scan |
| `partition = N` | ✅ | Reads only the target partition |
| `offset BETWEEN A AND B` | ✅ | Direct offset range read |
| `key = X` | ✅ | Key index lookup (if available) |
| `json_extract(value, ...) = Y` | ❌ | Requires full scan + filter |

### Materialized views for frequent queries

For dashboards or recurring queries, support optional materialized views:

```sql
-- Create a materialized view refreshed every 5 minutes
CREATE VIEW daily_counts AS
    SELECT date(timestamp) AS day, count(*) AS cnt
    FROM events
    GROUP BY 1
WITH REFRESH INTERVAL '5 minutes';
```

Materialized views are stored in Streamline's internal state and updated incrementally when possible.

### Lazy loading and limits

- Unbounded `SELECT *` queries are paginated internally (default: 10,000 rows per page).
- Queries without a `WHERE` clause or `LIMIT` trigger a warning in the CLI.
- The server enforces a configurable `max_query_rows` (default: 1,000,000) to prevent accidental full-topic scans.

## Configuration

```toml
[sqlite_query]
enabled = true          # Requires sqlite-query feature flag
port = 9095             # Wire protocol listener port
max_query_rows = 1000000
default_limit = 10000   # Applied when no LIMIT clause is present
read_timeout_ms = 30000
```

## Future Work

- **Schema inference**: Automatically detect JSON schema from recent records and expose typed columns.
- **Write support**: `INSERT INTO topic (key, value) VALUES (...)` as a produce shorthand.
- **Streaming queries**: `SELECT * FROM events WHERE timestamp > now() FOLLOW` for tailing a topic.
- **DuckDB fallback**: Route complex analytical queries to DuckDB when the `analytics` flag is enabled.
