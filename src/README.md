# Streamline Source Code Map

This directory contains ~325K lines of Rust organized into the modules below.
Stability levels indicate API change expectations (see [API Stability](../docs/API_STABILITY.md)).

## Core (Stable)

These modules are production-ready. Breaking changes only in major versions.

| Module | Description |
|--------|-------------|
| `config/` | `ServerConfig`, CLI args (`ServerArgs`), TOML config parsing |
| `error.rs` | `StreamlineError` enum, `Result<T>` alias, Kafka error code mapping |
| `server/` | TCP server, connection handling, graceful shutdown |
| `server/http.rs` | HTTP/REST API server (health, management, WebSocket) |
| `server/api.rs` | REST API endpoint handlers |
| `protocol/` | Kafka wire protocol — 50+ API implementations |
| `protocol/kafka.rs` | Main protocol handler (request routing, encoding/decoding) |
| `protocol/handlers/` | Per-API-key request handlers |
| `storage/` | Segment-based persistent storage engine |
| `storage/topic.rs` | `TopicManager`, `Topic` types |
| `storage/partition.rs` | `Partition` — ordered record sequences |
| `storage/segment.rs` | `Segment` — binary file format (magic: `STRM`) |
| `storage/record.rs` | `Record`, `RecordBatch` types |
| `consumer/` | Consumer group coordination, offset management |
| `consumer/coordinator.rs` | `GroupCoordinator` — classic rebalance protocol |
| `consumer/kip848/` | KIP-848 server-side assignment protocol |
| `metrics/` | Prometheus metrics, JMX-compatible metrics |

## Enterprise (Beta)

Feature-complete but APIs may change in minor versions with notice.

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| `auth/` | `auth` | SASL/SCRAM/OAuth authentication, RBAC |
| `audit/` | `auth` | Audit logging |
| `cluster/` | `clustering` | Raft consensus, multi-node management |
| `replication/` | `clustering` | ISR management, data replication |
| `schema/` | `schema-registry` | Avro, Protobuf, JSON Schema registry |
| `transaction/` | — | Transaction coordinator, exactly-once semantics |
| `embedded.rs` | — | `EmbeddedStreamline` SDK for library usage |
| `telemetry/` | `telemetry` | OpenTelemetry distributed tracing |

## Experimental

Work in progress. APIs may change without notice.

| Module | Feature Flag | Description |
|--------|-------------|-------------|
| `analytics/` | `analytics` | Embedded DuckDB SQL queries on streams |
| `sink/` | `iceberg` / `delta-lake` | Lakehouse sink connectors |
| `cdc/` | `cdc` | Change data capture (PostgreSQL, MySQL, etc.) |
| `edge/` | `edge` | Offline-first edge runtime |
| `ai/` | `ai` | Embeddings, semantic search, anomaly detection |
| `lakehouse/` | `lakehouse` | Columnar storage, Arrow/Parquet integration |
| `stateful/` | — | Windows, joins, aggregations |
| `timeseries/` | — | Time-series optimized storage |
| `crdt/` | — | Conflict-free replicated data types |
| `streamql/` | — | SQL-like query language |

## CLI & Developer Tools

| Module | Description |
|--------|-------------|
| `cli.rs` | CLI entry point — all `streamline-cli` subcommands |
| `cli_utils/tui.rs` | Interactive TUI dashboard (`streamline-cli top`) |
| `cli_utils/benchmark.rs` | Performance benchmarking framework |
| `cli_utils/doctor.rs` | System diagnostics and health checks |
| `cli_utils/migrate.rs` | Kafka migration tools |
| `playground/` | Demo/playground mode with sample topics |

## Supporting Infrastructure

| Module | Description |
|--------|-------------|
| `dlq/` | Dead-letter queue support |
| `gateway/` | WebSocket and simple protocol gateway |
| `pubsub/` | Redis-like pub/sub protocol layer |
| `smart_partition/` | Intelligent partition assignment |
| `runtime/` | Sharded thread-per-core runtime |
| `wasm/` | WebAssembly transform runtime |
| `connect/` | Connector framework (sources/sinks) |
| `testing.rs` | Shared test utilities |

## Entry Points

| Binary | File | Description |
|--------|------|-------------|
| `streamline` | `main.rs` | Server binary |
| `streamline-cli` | `cli.rs` | CLI management tool |
| `streamline-ui` | `bin/streamline-ui.rs` | Web dashboard (requires `web-ui` feature) |
