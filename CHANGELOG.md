# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
- **Added**: add CDC SQL Server connector module

### Performance
- Optimize log segment index lookup

### Changed
- Extract protocol decoder into separate module

### Fixed
- Resolve partition rebalance edge case in consumer groups

### Added
- Add configurable retry backoff

### Changed

- **Analytics Module Graduated to Stable**
  - The embedded DuckDB analytics module (`analytics`) has been promoted from
    Experimental to Stable. Breaking changes will now only occur in major versions.
  - Improved error handling: new `InvalidSql`, `TopicNotFound`, `QueryTimeout`,
    and `DuckDb` error variants with user-friendly messages and `From<duckdb::Error>`
    conversion.
  - Added pagination support: `offset` field in `QueryOptions` and `total_rows`,
    `has_more` fields in `QueryResult` for cursor-based result paging.
  - Added query timeout enforcement using `tokio::time::timeout`.
  - Improved HTTP API error responses: errors now include a `code` field and use
    semantically correct HTTP status codes (400 for invalid SQL, 404 for missing
    views/topics, 408 for timeouts).
  - Added comprehensive module-level and function-level documentation with usage
    examples across all analytics source files.
  - Added comprehensive test suite covering basic queries, aggregations (COUNT,
    SUM, AVG), GROUP BY, window functions (ROW_NUMBER), error handling (invalid
    SQL, non-existent topics), caching, pagination, concurrent queries,
    materialized view lifecycle, and large result set handling.

## [0.2.0] - 2026-01-23

### Added

- **Iceberg Sink Enhancements**
  - Retry logic with exponential backoff for catalog commits (configurable `max_retries`, `retry_delay_ms`)
  - Field-based partitioning: extract partition values from JSON record fields
  - Configurable Parquet compression: None, Snappy, Gzip, LZ4, Zstd
  - Schema evolution policy configuration: Strict, AddNewColumns, AddAndPromote
  - Improved error messages for unsupported Hive/Glue catalogs

- **Delta Lake Sink Connector** (new `delta-lake` feature)
  - Write streaming data to Delta Lake tables
  - Write modes: Append, Overwrite, Merge
  - Schema evolution policies: Strict, AddNewColumns, AddAndWiden
  - Partition column configuration
  - Storage options for cloud backends (S3, Azure, GCS)
  - Comprehensive integration test suite

- **Thread-Per-Core Partition Routing (Redpanda-Style Performance)**
  - Routes Produce/Fetch requests to CPU-pinned shards based on partition ownership
  - New `ShardedRuntime` methods for async-to-sync bridging:
    - `submit_to_shard_with_result()`: Submit task to specific shard with result channel
    - `submit_for_partition_with_result()`: Route by partition ID (`partition % shard_count`)
  - `KafkaHandler::with_sharded_runtime()`: Configure handler for shard routing
  - Updated `Server::new()` to accept optional `ShardedRuntime` parameter
  - Achieves cache locality and reduced context switching for partition-heavy workloads
  - Expected improvements: P99 latency -30-50%, throughput +20-40%
  - Enable with `--runtime-mode sharded`

- **ControlledShutdown API (Key 7)**
  - Graceful broker shutdown support for Kafka Streams topology rebalancing
  - Proper partition leadership transfer during shutdown
  - API versions 0-3 supported

- **Delegation Tokens (Keys 38-41)**
  - Full delegation token lifecycle management
  - `CreateDelegationToken` (Key 38): HMAC-based token generation with configurable TTL
  - `RenewDelegationToken` (Key 39): Extend token expiry before expiration
  - `ExpireDelegationToken` (Key 40): Immediate token revocation
  - `DescribeDelegationToken` (Key 41): List and inspect tokens by owner
  - In-memory token storage with persistence support

- **Client Quotas Enforcement (Keys 48-49)**
  - `QuotaManager` for per-client rate limiting
  - `DescribeClientQuotas` (Key 48): Query quota configurations
  - `AlterClientQuotas` (Key 49): Set/modify client quotas
  - Quota types: producer byte rate, consumer byte rate, request rate, connection rate
  - Client matching by user principal and client ID
  - In-memory quota storage with API-based configuration

- **Log Compaction**
  - `LogCompactor` with key-based deduplication
  - `cleanup.policy=compact` topic configuration
  - Tombstone handling with configurable retention
  - Background compaction enforcement via `RetentionEnforcer`
  - Configurable: `min_cleanable_dirty_ratio`, `delete_retention_ms`, `min_compaction_lag_ms`

- **Kafka 3.x API Version Updates**
  - Produce API: v0-3 → v0-9 (flexible versions, improved acks)
  - Fetch API: v0-12 → v0-15 (topic_id support, rack-aware fetch)
  - InitProducerId API: v0-4 → v0-5 (transaction improvements)
  - ConsumerGroupDescribe API: v0 → v0-1 (KIP-848 updates)

- **Build Editions via Feature Flags**
  - Lite edition (default): Core streaming, TLS, compression (~5-8MB binary)
  - Full edition: All enterprise features (~15-20MB binary)
  - Individual feature flags: `auth`, `clustering`, `telemetry`, `metrics`, `cloud-storage`, `schema-registry`, `encryption`
  - Modular dependencies reduce binary size for constrained environments

- **TOML Configuration File Support**
  - Load configuration from TOML file with `-c config.toml`
  - Generate example config with `--generate-config`
  - Merge config file, CLI args, and environment variables

- **Authentication (SASL/OAuth)**
  - SASL/PLAIN for development environments
  - SASL/SCRAM-SHA-256 and SCRAM-SHA-512 for production
  - OAuth 2.0 / OIDC with JWKS validation
  - User management via YAML file or API
  - Session management and delegation tokens

- **Authorization (ACL/RBAC)**
  - ACL-based access control with resource/operation patterns
  - Role-based access control (RBAC) for simplified management
  - Super-user configuration
  - Audit logging for security events

- **Schema Registry**
  - Confluent-compatible Schema Registry API
  - Avro schema support with validation
  - Protobuf schema support
  - JSON Schema support
  - Schema compatibility checking (BACKWARD, FORWARD, FULL, NONE)

- **OpenTelemetry Integration**
  - Distributed tracing with OTLP export
  - Span context propagation
  - Configurable sampling

- **Prometheus Metrics**
  - 60+ metrics covering all subsystems
  - JMX-compatible metric names for Kafka tooling
  - Per-topic, per-partition granularity
  - `/metrics` endpoint

- **Encryption at Rest**
  - AES-256-GCM encryption for stored data
  - Key file configuration

- **HTTP Admin API**
  - RESTful endpoints for cluster, topics, consumers
  - Health check endpoints
  - Schema Registry REST API
  - Alerting API

- **Connection Management**
  - Connection limits (max connections, per-IP limits)
  - Rate limiting
  - Idle timeout handling

- **Consumer Improvements**
  - Consumer lag tracking
  - KIP-848 new consumer protocol (experimental)
  - Offset store improvements

- **Gzip Compression Support** (#59)
  - Added Gzip codec alongside existing LZ4, Snappy, and Zstd
  - Configurable compression at topic and message level
  - Automatic decompression on read

- **Client Quotas and Throttling** (#63)
  - Per-client rate limiting for producers and consumers
  - Configurable byte rate limits via CLI/environment variables
  - Quota metrics exposed via Prometheus endpoint
  - `--quotas-enabled`, `--quota-producer-byte-rate`, `--quota-consumer-byte-rate` options

- **WebSocket Gateway** (#68)
  - Real-time message streaming via WebSocket connections
  - Endpoint: `/ws/topics/{topic}/partitions/{partition}`
  - Client controls: seek, pause, resume
  - JSON message format with offset, key, value, timestamp

- **Simple Protocol Alternative** (#70)
  - Redis RESP-like text protocol for easy integration
  - Commands: PING, TOPICS, CREATE, DELETE, PRODUCE, CONSUME, OFFSETS, INFO, HELP
  - Aliases: PUB/SEND for PRODUCE, SUB/FETCH/GET for CONSUME
  - `--simple-enabled`, `--simple-addr` options (default port: 9095)

- **Tiered Storage** (#65)
  - Offload cold segments to object storage (S3, Azure Blob, GCS)
  - Local filesystem backend for testing
  - Configurable tiering policies (age-based, size-based)
  - Local cache for frequently accessed tiered data
  - Background upload/download with retry logic

- **Zero-Copy Optimization** (#71)
  - BufferPool for reusable byte buffer allocation
  - ZeroCopyReader using Unix pread() for positioned reads
  - FileSlice for lazy file content access
  - Reduced memory copies during I/O operations
  - Statistics tracking for monitoring optimization effectiveness

- **Clustering and Replication**
  - Multi-node clustering with Raft-based consensus (using `openraft`)
  - Cluster membership management with node discovery and heartbeat tracking
  - Partition leadership election and tracking
  - In-Sync Replica (ISR) management for data durability
  - Acks policies support: `acks=0`, `acks=1`, `acks=all`
  - High watermark and log end offset tracking
  - Log truncation for follower resync after leader changes
  - Graceful shutdown with leadership transfer preparation
  - Inter-broker communication protocol (ReplicaFetch, LeaderAndIsr, UpdateMetadata, StopReplica)
  - Replica fetcher for follower partitions
  - Replica sender for leader partitions
  - Failover handling with configurable policies
- **New CLI Arguments for Clustering**
  - `--node-id` - Unique node identifier (enables cluster mode)
  - `--advertised-addr` - Address clients use to connect
  - `--inter-broker-addr` - Internal cluster communication address
  - `--seed-nodes` - Comma-separated seed node addresses for joining
  - `--default-replication-factor` - Default replication factor for new topics
  - `--min-insync-replicas` - Minimum in-sync replicas for acks=all

### Changed

- Single-node mode remains the default (backward compatible)
- Partition now separates log end offset (LEO) from high watermark (HWM)

## [0.1.0] - 2024-12-09

### Added

- Initial release of Streamline
- **Kafka Protocol Support**
  - ApiVersions (API key 18, versions 0-3)
  - Metadata (API key 3, versions 0-12)
  - Produce (API key 0, versions 0-9)
  - Fetch (API key 1, versions 0-13)
  - ListOffsets (API key 2, versions 0-7)
  - CreateTopics (API key 19, versions 0-7)
- **Storage Engine**
  - Persistent, segment-based storage with efficient indexing
  - Binary segment format with 64-byte header (magic bytes: `STRM`)
  - LZ4 compression support
  - CRC32 checksums for data integrity
- **Topic Management**
  - Create, delete, and list topics
  - Multi-partition support
  - Auto-create topics on first produce
  - Topic metadata persistence
- **CLI Tool** (`streamline-cli`)
  - `topics list` - List all topics
  - `topics create <name>` - Create a new topic
  - `topics describe <name>` - Show topic details
  - `topics delete <name>` - Delete a topic
  - `system info` - Show system information
- **Server Features**
  - Single binary deployment
  - Zero configuration required
  - Graceful shutdown handling
  - Structured logging with tracing
- **Configuration**
  - CLI arguments and environment variable support
  - Configurable listen address (`--listen-addr` / `STREAMLINE_LISTEN_ADDR`)
  - Configurable data directory (`--data-dir` / `STREAMLINE_DATA_DIR`)
  - Configurable log level (`--log-level` / `STREAMLINE_LOG_LEVEL`)

### Notes

- This is the initial release focused on core functionality

[Unreleased]: https://github.com/streamlinelabs/streamline/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/streamlinelabs/streamline/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/streamlinelabs/streamline/releases/tag/v0.1.0
