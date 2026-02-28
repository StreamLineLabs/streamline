# ADR-007: Query Engine Consolidation

## Status
Proposed

## Date
2026-03-04

## Context
Streamline currently has three overlapping query mechanisms:

1. **DuckDB Analytics** (`analytics` feature) — Embedded DuckDB for batch SQL queries on topic data. ~30MB binary size addition. Stable.
2. **StreamQL** (`streamql` module) — Custom streaming SQL engine with parser, planner, and executor. Supports windowing, CEP, joins. Stable.
3. **SQLite Queries** (`sqlite-queries` feature) — Lightweight SQLite-based query interface. ~1MB. Experimental.

This creates confusion for users:
- Which query engine should they use?
- API endpoints differ (`/api/query` vs StreamQL syntax vs SQLite)
- Documentation must cover three separate systems
- SDK query methods need to target one API

## Decision
**Consolidate into a single unified query API with StreamQL as the primary interface.**

### Architecture
```
User SQL Query
    │
    ▼
StreamQL Parser (parse SQL into AST)
    │
    ├─── Batch queries → DuckDB execution engine
    │    (SELECT ... FROM topic WHERE ... GROUP BY ...)
    │
    └─── Streaming queries → StreamQL operators
         (SELECT ... FROM topic EMIT CHANGES ...)
```

### API Surface
- Single HTTP endpoint: `POST /api/v1/query`
- Single SDK method: `.query(sql)` / `.queryStream(sql)`
- StreamQL syntax is a superset of standard SQL

### Migration Path
1. `sqlite-queries` feature deprecated in v0.3.0, removed in v0.4.0
2. `analytics` feature renamed to `streamql-batch` (DuckDB still used as execution engine)
3. All SDK `.query()` methods target `/api/v1/query`

## Consequences
- **Positive:** Single, clear query story for users and documentation
- **Positive:** StreamQL differentiates from competitors (streaming SQL)
- **Positive:** Reduced binary size for users who don't need DuckDB
- **Negative:** Users of `sqlite-queries` must migrate
- **Negative:** One-time refactoring effort
