# ADR-0012: DuckDB for Embedded Stream Analytics

## Status

Accepted

## Context

Streamline's analytics feature allows users to run SQL queries directly on streaming data without exporting to an external database. We needed an embeddable SQL engine that could:

1. Execute analytical queries (aggregations, window functions, joins) on message data
2. Run in-process without external dependencies
3. Handle JSON message payloads with schema inference
4. Support materialized views with TTL-based caching
5. Integrate with the existing Tokio async runtime

Candidates considered:
- **DuckDB** — Columnar OLAP database, embeddable, C/C++ with Rust bindings
- **SQLite** — Row-oriented, widely used, but not optimized for analytical workloads
- **DataFusion** — Rust-native query engine (Arrow-based)
- **Polars** — Rust DataFrame library with SQL support

## Decision

We chose **DuckDB** (via the `duckdb` Rust crate) for the embedded analytics engine:

- **Analytical performance**: Columnar execution engine optimized for aggregations, window functions, and scan-heavy workloads — exactly what stream analytics needs
- **Zero-config**: Runs fully in-process with no external dependencies
- **JSON support**: Native JSON parsing and schema inference, critical for handling arbitrary message payloads
- **SQL completeness**: Full SQL support including CTEs, window functions (ROW_NUMBER, RANK, LAG, LEAD), and subqueries
- **Memory efficiency**: Vectorized execution with configurable memory limits

The analytics module materializes topic data into temporary DuckDB tables via `streamline_topic('name')` syntax, with regex-based topic scanning for multi-topic queries.

## Consequences

### Positive

- Users can query streaming data with standard SQL without external infrastructure
- Materialized views with TTL caching reduce repeated computation
- REST API integration (`/api/v1/analytics/query`) makes analytics accessible from any client
- Query timeout and pagination support prevent runaway queries

### Negative

- DuckDB is written in C++, requiring FFI through the `duckdb` crate — adds build complexity
- Materializing topic data into DuckDB tables adds memory overhead proportional to query scope
- The `duckdb` crate's Rust bindings may lag behind upstream DuckDB releases

### Neutral

- DataFusion was a close alternative (pure Rust, Arrow-native) but DuckDB's JSON handling and SQL completeness were decisive factors
- The analytics feature is behind the `analytics` feature flag, so it has zero cost for users who don't need it
