# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).



## [Unreleased]

### Security
- Fixed a runtime panic on every outbound HTTPS request. `reqwest` is built with
  `rustls-tls-webpki-roots-no-provider`, so when it builds its own rustls
  `ClientConfig` it resolves the crypto provider from process-global state and,
  with no `install_default()` anywhere (by design), executes a bare
  `panic!("No provider set")`. Every `reqwest::Client::new()` and plain
  `Client::builder().build()` aborted instead of returning an error — affecting
  OAuth/JWKS validation, Vault KMS, serverless and cloud-function sinks, AI
  providers, edge sync, telemetry, the web UI and the admin CLI. All client
  construction now goes through the new crate-internal `http_client` module,
  which hands reqwest a fully built rustls configuration via
  `use_preconfigured_tls` — explicit AWS-LC provider, explicit TLS 1.2/1.3, the
  WebPKI roots and no client authentication. No process-wide provider is
  installed and the rustls graph stays AWS-LC only.
- Added a new publishable workspace crate, `streamline-serde-wincode` — an
  Apache-2.0 fork of `serde-wincode` 0.1.2 (attributed in its `NOTICE`) whose
  only change is an exact `wincode = "=0.4.9"` requirement. Upstream's
  `wincode = ">=0.4, <1"` spans three mutually semver-incompatible `0.x` lines,
  which Cargo does **not** unify with the root crate's `=0.4.9`: a fresh
  resolution of the published crate selected wincode 0.6 for the bridge
  *alongside* 0.4.9 for `streamline`, mismatching the ABI that
  `src/bincode_compat.rs` configures and requiring Rust 1.89 against an MSRV of
  1.88. A `Cargo.lock` pin could not fix this (it binds only this repository)
  and neither could `[patch]` (patches are not propagated to consumers), so the
  requirement now lives in a manifest that is itself published. The upstream
  package is gone from the graph; the bincode 1 wire format, the bounded
  decoder and the corrupt-length-prefix protections are unchanged.
- Closed the remaining lockfile-only MSRV constraints, which did not survive
  publication. `Cargo.lock` binds this repository and nothing else: a consumer
  of the published `streamline` resolves from scratch against the normalised
  manifests crates.io serves, so the "Cargo.lock pins it" comments documented a
  constraint nobody downstream had. A fresh consumer with `graphql` and
  `cloud-storage` enabled resolved `async-graphql` 7.2.1 (and its
  derive/parser/value crates) plus `crc-fast` 1.10.0 — all of which require Rust
  1.89, above this crate's MSRV of 1.88. Each edge now carries an exact
  requirement in a manifest that is itself published:
  - `async-graphql = "=7.0.17"`, plus new optional direct dependencies
    `async-graphql-derive`/`-parser`/`-value` at `=7.0.17`, activated by the
    `graphql` feature. Pinning the facade alone was not enough: it asks for its
    companion crates with caret requirements, which floated to the 7.2.x line.
  - `crc-fast = "=1.9.0"` and `crc = "=3.3.0"`, new optional direct
    dependencies activated by `cloud-storage` alongside `object_store`, whose
    `crc-fast = "^1.6"` edge was the one that floated.
  These crates are not used by Streamline's own code; they exist solely to bound
  a downstream resolution. `fresh_consumer_resolves_an_msrv_compatible_graph`
  proves it end to end: it runs `cargo package` for all four publishable crates,
  unpacks them, deletes every embedded lockfile, resolves a brand-new consumer
  with the real 1.88 toolchain, and asserts that no package anywhere in the
  resulting graph declares a `rust-version` above the MSRV.
  `permissive_requirements_would_still_float_past_the_msrv` is the non-vacuity
  control, reproducing the defect on demand.
- Removed the unmaintained `rustls-pemfile` dependency (RUSTSEC-2025-0134; the
  repository was archived in August 2025). All PEM certificate and private-key
  parsing — server TLS, inter-broker cluster TLS, QUIC and WebTransport — now
  uses the `PemObject` trait from `rustls::pki_types::pem`, which ships inside
  the `rustls-pki-types` crate that rustls already depends on. `rustls-pemfile`
  was itself only a thin wrapper around that same code, so the accepted key
  formats (PKCS#8, PKCS#1/RSA, SEC1/EC) and all error messages are unchanged.
  The crate is now absent from `Cargo.lock` even under `--all-features`.
- Replaced the unmaintained `bincode` crate entirely
  (RUSTSEC-2025-0141, `patched = []`) with maintained
  `serde-wincode`/`wincode`. The crate-internal `bincode_compat` module
  preserves bincode 1's little-endian, fixed-width wire format, trailing-byte
  acceptance and unlimited collection preallocation. Hard-coded golden vectors
  captured from bincode 1.3.3 cover persisted records, batches, metadata,
  enums, collections and 128-bit integers. Existing segments, raft state,
  time-travel archives and AI vector stores remain byte-compatible, while
  `bincode` is absent from `Cargo.lock`.
- Removed both `rkyv` advisories (RUSTSEC-2026-0001, RUSTSEC-2026-0235) by
  updating `rust_decimal` to 1.43.0, which drops `rkyv` 0.7 from its normal
  dependencies. No code change was required and the MSRV is unaffected.
- Removed all four `quick-xml` advisory hits (RUSTSEC-2026-0194 and
  RUSTSEC-2026-0195, both CVSS 7.5) by upgrading `object_store` from 0.11 to
  0.14.1 — the first line that parses S3/Azure list XML with
  `quick-xml >= 0.41` — and by removing the `iceberg` and `deltalake`
  dependencies, which pinned `quick-xml` 0.36/0.37 transitively through
  `opendal`, `reqsign` and `delta_kernel`.
- Eliminated the `native-tls`/OpenSSL path from all builds, including
  `--all-features`. It entered only via `deltalake` → `delta_kernel` →
  `reqwest` (default-tls) → `hyper-tls` → `native-tls` → `openssl`;
  `delta_kernel` exposed no rustls switch, so Cargo feature unification forced
  OpenSSL in regardless of this crate's rustls-only `reqwest` declaration.
  `cargo deny --all-features check bans` is now clean; Streamline is rustls-only.
- Standardised every direct TLS transport on the AWS-LC rustls provider and
  disabled unused Prometheus exporter defaults. Server and inter-broker TLS
  builders now receive the provider explicitly, so a transitive dependency
  cannot make rustls provider auto-selection panic at startup.
- No advisory was suppressed: `deny.toml` `[advisories] ignore` remains empty
  and the `openssl` ban remains in place. `tests/dependency_security_test.rs`
  fails the build if `quick-xml < 0.41`, `rkyv`, `native-tls`, `hyper-tls` or
  `openssl` re-enter `Cargo.lock`, or if suppressions are added.
- Made RustSec and cargo-deny release checks fail closed and upgraded the
  compatible vulnerable dependency set, including AWS-LC, bytes, h2,
  lz4_flex, PostgreSQL, Quinn, rustls-webpki, tar/time, and Wasmtime.
- Replaced the ineffective C++ CodeQL configuration with Rust and GitHub
  Actions analysis.

### Removed
- **BREAKING**: the Apache Iceberg and Delta Lake sink connectors are no longer
  available in any build configuration. Every upstream release compatible with
  MSRV 1.88 carries the unfixed advisories above (currently released
  `deltalake` <= 0.31.1 still pins `object_store` 0.12 / `quick-xml` < 0.41, and
  MSRV-compatible `iceberg` versions still use `opendal`/`quick-xml` < 0.41), so
  the dependencies were removed rather than shipped vulnerable.
  - The `iceberg` and `delta-lake` Cargo features are retained as compatibility
    no-ops: they resolve and compile but enable no dependencies.
  - `iceberg` was removed from the `full` feature set.
  - `SinkManager::create_sink` now rejects `SinkType::Iceberg` and
    `SinkType::DeltaLake` with an explicit security/upstream message;
    `sink::unavailable::is_available` reports availability programmatically.
  - The implementations are preserved verbatim behind the never-enabled
    `iceberg_backend` / `delta_backend` cfgs (`src/sink/iceberg.rs`,
    `src/sink/delta.rs`, `src/lakehouse/iceberg_topics.rs`) so they can be
    restored once upstream ships fixed releases.
  - `src/lakehouse/iceberg_topics.rs` is no longer compiled. Its flush path
    simulated Iceberg writes — it logged "would write N records", discarded the
    buffer and reported synthetic byte counts as success — so shipping it as a
    working feature would have been inaccurate regardless.

### Changed
- `object_store` upgraded 0.11 → 0.14.1. The convenience methods
  (`get`/`put`/`head`/`delete`/`put_multipart`) moved from the `ObjectStore`
  trait to the new `ObjectStoreExt` extension trait; call sites in
  `src/storage/{backend_factory,diskless,segment_s3,tiering,wal_s3}.rs` now
  import it. No behavioural change to tiered storage.
- The `sink` module is now always compiled instead of being gated behind
  `iceberg`/`delta-lake`, so the Serverless and Cloud Function connectors and
  `SinkManager` remain available under `full` after `iceberg` was dropped from it.
- The `streamline-cli sink` command remains visible in supported builds so
  unavailable Iceberg/Delta creation requests return the explicit
  dependency-security explanation instead of an unrecognised-subcommand error.
- **BREAKING (default builds)**: `SinkType::Serverless` and
  `SinkType::CloudFunction` now require the `serverless` Cargo feature, and
  `sink::unavailable::is_available` reports them accordingly. Both connectors
  deliver over HTTP through `crate::http_client`, which only exists when that
  feature is on; without it the modules compiled but every delivery path was a
  stub. `SinkManager::create_sink` therefore *constructed and registered* a
  sink that failed on every batch — it showed as healthy in `sink list` while
  dropping records. Creation is now rejected up front, before duplicate-name,
  topic-existence and configuration validation, so the caller is told the real
  problem ("rebuild with `--features serverless`") instead of an unrelated
  "topic does not exist", and nothing reaches the registry. The
  `src/sink/serverless.rs` and `src/sink/cloud_functions.rs` modules are gated
  on the same feature. With `--features serverless` the behaviour is unchanged:
  valid configurations construct, register and deliver exactly as before.
- Raised the minimum supported Rust version from 1.80 to 1.88 so patched
  dependency releases can be used consistently across build and release jobs.
- Marketplace HTTP discovery/install now returns `501 Not Implemented` instead
  of creating placeholder `.wasm` files or reporting CRC32 as SHA-256.
- Unsupported contract assertions now fail explicitly rather than passing as
  skipped checks.

### Release engineering
- Sequenced validation, crate publication, binary release creation, SBOM,
  signing, and provenance generation through one gated release path.
- Made the workspace crate graph packageable by declaring registry versions
  for local path dependencies and publishing leaf crates before the root crate.
- A `workflow_dispatch` of `publish-crate.yml` is now unconditionally a dry run.
  The manual trigger previously took a `dry_run` boolean that fed straight into
  the token check and `cargo publish`, so anyone who could dispatch the workflow
  could make an irrevocable crates.io release with none of the release-gate
  checks having run. The manual boolean is gone — there is nothing left to
  supply — and the effective mode is computed by
  `scripts/release/resolve-publish-mode.sh`, which only emits `publish` for a
  `workflow_call` invocation, with an explicit `dry_run: false`, from a caller
  that runs the release gate. (`github.event_name` cannot be used for this: in a
  reusable workflow it is the *caller's* event and is never `workflow_call`.)
- crates.io publication is now resumable and idempotent. Re-running after a
  partial release used to fail with "crate version is already uploaded", leaving
  the release wedged with some crates published and some not.
  `scripts/release/publish-crates.sh` queries crates.io for each exact
  `name@version`, skips what is already published (never accepting a *different*
  version as evidence), and waits for real registry visibility via
  `cargo info <crate>@<version>` — bounded by `VISIBILITY_TIMEOUT_SECONDS` —
  before publishing dependents. This replaces the fixed `sleep 30`. Any
  registry answer that is not a clean 200 or 404 aborts the run rather than
  guessing. The standalone Rust SDK publish uses the same helper.
- Every publishing job in `publish-sdks.yml` now declares
  `needs: [verify-core-version, release-gate, publish-rust-crate]`. The
  ecosystem jobs previously had no dependencies at all, so a manual run pushed
  npm, PyPI, Maven Central, NuGet and a Go tag concurrently with — or before —
  the release gate and the crates.io publish.
- Every SDK publish now verifies the checked-out repository's own authoritative
  version against the release version before building or publishing, in dry runs
  too. The `version` input was previously little more than a label: each job
  checked out its SDK's default branch and published whatever version that
  branch declared, so a 0.4.1 release could ship 0.4.0 artefacts and report
  success. The new `scripts/release/verify-sdk-version.sh` reads the real
  artefact for Rust, Node, Python, Java, .NET, Go, Kotlin and WASM, and fails
  closed on a missing file, an unparsable file or an unknown ecosystem. Go has
  no manifest version, so it is checked against its in-repo `const Version`
  *and* its release-tag state. SDK repositories are checked out into `sdk/` so
  this repository's release-control scripts remain available.
- Added `scripts/release/tests/release-scripts.test.sh`, a hermetic suite for
  the three release helpers that runs against fake `cargo`/`curl` binaries and
  generated SDK fixtures — no network, no registry, no sibling repository. It is
  wired into `cargo test` via `tests/release_scripts_test.rs`, and
  `tests/packaging_metadata_test.rs` adds static guards that enumerate every
  publishing job and assert its dependencies and its version-check ordering.


## [0.3.0] - 2026-04-20

- test: add protocol conformance tests (2026-03-06)
- refactor: optimize segment compaction pipeline (2026-03-06)
- fix: resolve partition rebalance race condition (2026-03-06)
- **Added**: add marketplace web registry integration
- **Changed**: restructure server HTTP API handlers
- **Added**: implement connect runtime framework
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
- feat: add configurable log compaction trigger threshold
- refactor: improve partition assignment algorithm during rebalance
- docs: improve partition retention configuration reference
- fix: resolve consumer group rebalance starvation under load
- chore: update clippy allow/deny configuration for 0.3 release
