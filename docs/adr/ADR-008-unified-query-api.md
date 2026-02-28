# ADR-008: Unified Query API (StreamQL)

## Status
Proposed

## Date
2026-03-04

## Context
Streamline needs a single, well-defined query API that all SDKs can target. Currently, query capabilities are fragmented across DuckDB analytics, StreamQL, and SQLite modules.

## Decision
Implement a unified HTTP API for all query operations.

### API Design

#### Batch Query
```
POST /api/v1/query
Content-Type: application/json

{
  "sql": "SELECT user_id, COUNT(*) as cnt FROM topic('events') WHERE timestamp > now() - INTERVAL '1 hour' GROUP BY user_id ORDER BY cnt DESC",
  "timeout_ms": 5000,
  "max_rows": 1000,
  "format": "json"
}
```

**Response:**
```json
{
  "columns": [
    { "name": "user_id", "type": "varchar" },
    { "name": "cnt", "type": "bigint" }
  ],
  "rows": [
    ["alice", 142],
    ["bob", 98]
  ],
  "metadata": {
    "execution_time_ms": 42,
    "rows_scanned": 50000,
    "rows_returned": 2,
    "truncated": false
  }
}
```

#### Streaming Query (Server-Sent Events)
```
POST /api/v1/query/stream
Content-Type: application/json

{
  "sql": "SELECT * FROM topic('events') EMIT CHANGES",
  "format": "json"
}
```

**Response (SSE):**
```
event: row
data: {"user_id": "alice", "action": "login", "timestamp": "2026-03-04T10:00:00Z"}

event: row
data: {"user_id": "bob", "action": "purchase", "timestamp": "2026-03-04T10:00:01Z"}

event: heartbeat
data: {"timestamp": "2026-03-04T10:00:05Z"}
```

#### Explain Query
```
POST /api/v1/query/explain
Content-Type: application/json

{
  "sql": "SELECT * FROM topic('events') WHERE user_id = 'alice'"
}
```

### SDK Integration

Every Tier-1 SDK provides:
```
client.query(sql) → QueryResult          // batch
client.queryStream(sql) → AsyncIterator   // streaming
```

### SQL Extensions (StreamQL Syntax)

```sql
-- Standard SQL (batch, executed by DuckDB)
SELECT * FROM topic('events') LIMIT 100;

-- Streaming SQL (continuous, executed by StreamQL operators)
SELECT * FROM topic('events') EMIT CHANGES;

-- Windowed aggregation
SELECT user_id, COUNT(*) as cnt
FROM topic('events')
WINDOW TUMBLE(timestamp, INTERVAL '5 minutes')
GROUP BY user_id
EMIT CHANGES;

-- Stream-stream join
SELECT e.user_id, e.action, u.name
FROM topic('events') e
JOIN topic('users') u ON e.user_id = u.user_id
WITHIN INTERVAL '1 hour'
EMIT CHANGES;

-- Topic function (read from topic)
topic('name')                    -- all partitions
topic('name', partition := 0)    -- specific partition
topic('name', offset := 100)     -- from offset
```

### Error Responses
```json
{
  "error": {
    "code": "SYNTAX_ERROR",
    "message": "Expected FROM clause at line 1, column 8",
    "line": 1,
    "column": 8
  }
}
```

## Consequences
- **Positive:** Single API for all query types (batch + streaming)
- **Positive:** Consistent SDK integration pattern
- **Positive:** StreamQL syntax is a superset of standard SQL
- **Negative:** Migration effort for users of `/api/query` (legacy)
- **Negative:** Two execution engines (DuckDB + StreamQL) behind one API
