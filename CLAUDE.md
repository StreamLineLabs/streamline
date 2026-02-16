# CLAUDE.md - Streamline Project Context

This file provides context to Claude Code for working with the Streamline codebase.

## Project Overview

**Streamline** is a Kafka protocol-compatible streaming solution - "The Redis of Streaming". It's a developer-first, lightweight streaming platform that bridges enterprise streaming (Kafka, Redpanda) and simple messaging (Redis Pub/Sub).

**Key characteristics:**
- Single binary, zero external dependencies, <50MB footprint
- Kafka protocol compatibility (existing Kafka clients work unchanged)
- Zero configuration required (just run `./streamline`)
- Persistent, segment-based storage with efficient indexing

## Organization Structure

Streamline is developed across multiple repositories under the [StreamLine Labs](https://github.com/streamlinelabs) GitHub organization:

| Repository | Description |
|---|---|
| **streamline** (this repo) | Core server, CLI, embedded SDK |
| streamline-operator | Kubernetes operator |
| terraform-provider-streamline | Terraform provider |
| streamline-deploy | Helm charts, K8s manifests, Docker |
| streamline-java-sdk | Java SDK + Spring Boot starter |
| streamline-python-sdk | Python SDK |
| streamline-go-sdk | Go SDK |
| streamline-node-sdk | Node.js/TypeScript SDK |
| streamline-rust-sdk | Rust client SDK |
| streamline-dotnet-sdk | .NET SDK |
| streamline-docs | Documentation website |
| streamline-vscode | VS Code extension |
| homebrew-tap | Homebrew formulae |

This repository contains only the core Rust codebase. SDKs, deployment artifacts, and tooling live in their own repos.

## Quick Reference

### Build & Run Commands

```bash
# Build Editions
cargo build                           # Lite edition (default) - minimal, ~5-8MB
cargo build --features full           # Full edition - enterprise features, ~15-20MB
cargo build --all-features            # All features including dev/test
cargo build --release                 # Release build (optimized)

# Feature Flags
#   lite (default): Core streaming, TLS, compression, basic logging
#   full: Adds auth, clustering, telemetry, cloud-storage, schema-registry, metrics, encryption, analytics
#
# Individual features (can combine):
#   --features auth              # SASL/OAuth authentication, ACLs, RBAC
#   --features clustering        # Raft consensus, multi-node clustering
#   --features telemetry         # OpenTelemetry distributed tracing
#   --features metrics           # Prometheus metrics exposition
#   --features cloud-storage     # S3/Azure/GCS tiered storage
#   --features schema-registry   # Avro/Protobuf/JSON schema management
#   --features encryption        # AES-256-GCM encryption at rest
#   --features analytics         # Embedded DuckDB for SQL queries on stream data
#   --features iceberg           # Apache Iceberg lakehouse sink connector
#   --features delta-lake        # Delta Lake sink connector
#   --features web-ui            # Web dashboard UI (requires streamline-ui binary)

# Run server
cargo run                      # Run server with defaults
cargo run -- --help            # Show server options
cargo run -- --listen-addr 0.0.0.0:9092 --data-dir ./data

# Run CLI
cargo run --bin streamline-cli -- topics list
cargo run --bin streamline-cli -- topics create my-topic --partitions 3
cargo run --bin streamline-cli -- topics describe my-topic
cargo run --bin streamline-cli -- produce my-topic -m "Hello, Streamline!"
cargo run --bin streamline-cli -- consume my-topic --from-beginning

# Testing
cargo test                     # Run all tests
cargo test storage::           # Tests for storage module
cargo test -- --nocapture      # With output visible

# Linting & Formatting
cargo fmt                      # Format code
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps            # Build documentation
```

### Full Validation (run before commits)

```bash
# Validate both editions
cargo fmt && \
cargo clippy --all-targets -- -D warnings && \
cargo clippy --all-targets --all-features -- -D warnings && \
cargo test && \
cargo test --all-features && \
cargo doc --no-deps --all-features
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Binaries                                 │
│  main.rs (server)  │  cli.rs (CLI tool)  │  streamline-ui (UI)  │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────┴──────────────────────────────────┐
│                     Server Layer                                │
│  server/mod.rs - TCP    │  server/http.rs - HTTP/REST/WebSocket │
└──────────────────────────────┬──────────────────────────────────┘
                               │
    ┌──────────────────────────┼──────────────────────────────┐
    │                          │                              │
┌───┴────────┐   ┌─────────────┴───────────┐   ┌──────────────┴────┐
│ Protocol   │   │     Storage Layer       │   │  Enterprise (opt) │
├────────────┤   ├─────────────────────────┤   ├───────────────────┤
│ kafka.rs   │   │ topic.rs, partition.rs  │   │ auth/ (SASL/OAuth)│
│ handlers/  │   │ segment.rs, record.rs   │   │ cluster/ (Raft)   │
│ (32 APIs)  │   │ tiering.rs (S3/Azure)   │   │ schema/ (Registry)│
└────────────┘   │ storage_mode.rs         │   │ metrics/ (Prom)   │
                 └─────────────────────────┘   │ telemetry/ (OTel) │
                               │               │ encryption/       │
┌──────────────────────────────┴───────────────┴───────────────────┐
│                    Supporting Modules                            │
│  consumer/ (groups, KIP-848)  │  replication/  │  transaction/   │
│  cli_utils/ (TUI, benchmark)  │  audit/        │  dlq/           │
└──────────────────────────────────────────────────────────────────┘
                               │
                    ┌──────────┴──────────┐
                    │  config/  error.rs  │
                    └─────────────────────┘
```

## Key Files

| File | Purpose |
|------|---------|
| `src/main.rs` | Server entry point |
| `src/cli.rs` | CLI tool entry point (all CLI commands) |
| `src/lib.rs` | Library exports |
| `src/embedded.rs` | Embedded Rust SDK - `EmbeddedStreamline` for library usage |
| `src/error.rs` | `StreamlineError` type with all error variants |
| `src/config/mod.rs` | `ServerConfig`, `StorageConfig`, CLI args |
| `src/server/mod.rs` | TCP server, connection handling, shutdown |
| `src/server/http.rs` | HTTP API server (metrics, health, management) |
| `src/server/api.rs` | REST API endpoints |
| `src/protocol/kafka.rs` | Kafka wire protocol implementation (32 APIs) |
| `src/protocol/handlers/mod.rs` | Protocol request handlers |
| `src/consumer/coordinator.rs` | `GroupCoordinator` - consumer group management |
| `src/consumer/kip848/mod.rs` | KIP-848 new consumer rebalance protocol |
| `src/consumer/lag.rs` | `LagCalculator` - consumer lag monitoring |
| `src/storage/topic.rs` | `TopicManager`, `Topic` types |
| `src/storage/partition.rs` | `Partition` - ordered record sequences |
| `src/storage/segment.rs` | `Segment` - binary file format (header: `STRM`) |
| `src/storage/record.rs` | `Record`, `RecordBatch` types |
| `src/storage/storage_mode.rs` | Storage modes: local, hybrid, diskless |
| `src/replication/mod.rs` | `ReplicationManager` - replication framework |
| `src/storage/tiering.rs` | `TieringManager` - S3/Azure/GCS tiered storage |
| `src/cluster/mod.rs` | Clustering, Raft consensus, node management |
| `src/auth/mod.rs` | Authentication (SASL, OAuth) and authorization |
| `src/schema/mod.rs` | Schema Registry (Avro, Protobuf, JSON Schema) |
| `src/analytics/mod.rs` | Embedded analytics module for SQL queries |
| `src/analytics/duckdb.rs` | `DuckDBEngine` - SQL query engine on stream data |
| `src/metrics/mod.rs` | Prometheus metrics and JMX-compatible metrics |
| `src/telemetry/mod.rs` | OpenTelemetry distributed tracing |
| `src/cli_utils/tui.rs` | TUI dashboard (interactive terminal UI) |
| `src/cli_utils/benchmark.rs` | Performance benchmarking framework |
| `src/cli_utils/doctor.rs` | System diagnostics and health checks |
| `src/cli_utils/migrate.rs` | Kafka migration tools |
| `src/audit/mod.rs` | Audit logging |
| `src/dlq/mod.rs` | Dead-letter queue support |
| `src/transaction/mod.rs` | Transaction coordinator |
| `src/sink/mod.rs` | Sink connector framework (trait, manager) |
| `src/sink/config.rs` | Sink configuration types |
| `src/sink/iceberg.rs` | Apache Iceberg lakehouse sink |
| `src/sink/delta.rs` | Delta Lake sink connector |

## API Stability

Streamline uses semantic versioning with stability levels:

| Level | Description | Breaking Changes |
|-------|-------------|------------------|
| **Stable** | Production-ready | Only in major versions |
| **Beta** | Feature-complete | In minor versions with notice |
| **Experimental** | Work in progress | Any time without notice |

### Module Stability

| Stability | Modules |
|-----------|---------|
| Stable | `server`, `storage`, `consumer`, `protocol`, `config`, `error`, `analytics` |
| Beta | `embedded`, `transaction`, `cluster`, `replication`, `schema` |
| Experimental | `sink`, `streamql`, `cdc`, `edge` |

### When Working on Code

- **Stable modules**: Avoid breaking changes to public APIs
- **Beta modules**: Can modify APIs but document in CHANGELOG
- **Experimental**: Free to change, mark with stability docs

### Deprecation Process

1. Add `#[deprecated(since = "X.Y.Z", note = "Use X instead")]` attribute
2. Add entry to `docs/DEPRECATIONS.md`
3. Keep functional for 2 minor versions
4. Remove in subsequent minor version

## Coding Conventions

### Error Handling

- Use `StreamlineError` from `src/error.rs`
- Propagate with `?` operator
- **Never** use `.unwrap()` or `.expect()` in production code paths
- Use `.ok_or()` or `.map_err()` to convert `Option`/`Result`

```rust
// Good
let value = optional.ok_or(StreamlineError::Storage("not found".into()))?;

// Bad
let value = optional.unwrap();
```

### Async Code

- Runtime: Tokio with full features
- **Never** block the async runtime
- Use `tokio::time::sleep`, not `std::thread::sleep`

### Naming

- Types: `PascalCase`
- Functions/variables: `snake_case`
- Constants: `SCREAMING_SNAKE_CASE`

### Visibility

- Default to private
- Use `pub(crate)` for internal sharing
- Only `pub` for external API

### Testing

- Unit tests: inline `#[cfg(test)] mod tests { ... }`
- Async tests: `#[tokio::test]`
- Use `tempfile` crate for temp directories

## Dependencies

Key crates used:
- **tokio**: Async runtime (full features)
- **kafka-protocol**: Kafka wire protocol
- **serde/serde_json**: Serialization
- **thiserror**: Error type derivation
- **tracing**: Structured logging
- **clap**: CLI argument parsing
- **bytes**: Zero-copy byte handling
- **lz4_flex/zstd/snap**: Compression (LZ4, Zstd, Snappy)
- **crc32fast**: Checksums
- **axum**: HTTP server (REST API, WebSocket)
- **ratatui/crossterm**: TUI dashboard
- **memmap2**: Memory-mapped I/O for zero-copy reads
- **dashmap**: Concurrent hash maps
- **chrono**: Time/date handling
- **openraft** (clustering): Raft consensus
- **object_store** (cloud-storage): S3/Azure/GCS

## Storage Format

```
data/
└── topics/
    └── {topic-name}/
        ├── metadata.json
        └── partition-{N}/
            └── {offset}.segment
```

Segment files have a 64-byte header starting with magic bytes `STRM`.

## Supported Kafka API Keys

**Core Streaming (7 APIs):**
- ApiVersions (0-3), Metadata (0-12), Produce (0-9), Fetch (0-15), ListOffsets (0-7), CreateTopics (0-7), ControlledShutdown (0-3)
- *ListOffsets supports timestamp-based offset lookup (find offset by timestamp)*

**Topic Management (5 APIs):**
- DeleteTopics (0-6), CreatePartitions (0-3), DescribeConfigs (0-4), AlterConfigs (0-2), IncrementalAlterConfigs (0-1)

**Consumer Groups - Classic Protocol (10 APIs):**
- FindCoordinator (0-4), JoinGroup (0-9), Heartbeat (0-4), LeaveGroup (0-5), SyncGroup (0-5)
- OffsetCommit (0-8), OffsetFetch (0-8), DescribeGroups (0-5), ListGroups (0-4), DeleteGroups (0-2)

**Consumer Groups - KIP-848 Protocol (2 APIs):**
- ConsumerGroupHeartbeat (0-0), ConsumerGroupDescribe (0-1)
- *Server-side partition assignment with incremental rebalancing*

**Authentication & ACLs (5 APIs):**
- SaslHandshake (0-2), SaslAuthenticate (0-2), DescribeAcls (0-3), CreateAcls (0-3), DeleteAcls (0-3)

**Delegation Tokens (4 APIs):**
- CreateDelegationToken (0-3), RenewDelegationToken (0-2), ExpireDelegationToken (0-2), DescribeDelegationToken (0-3)
- *Full HMAC-based delegation token lifecycle management*

**Client Quotas (2 APIs):**
- DescribeClientQuotas (0-1), AlterClientQuotas (0-1)
- *Per-client rate limiting for producer/consumer byte rates*

**Transactions (9 APIs):**
- InitProducerId (0-5), AddPartitionsToTxn (0-4), AddOffsetsToTxn (0-3), EndTxn (0-3), TxnOffsetCommit (0-3)
- WriteTxnMarkers (0-1), DescribeProducers (0-0), DescribeTransactions (0-0), ListTransactions (0-0)

**Admin (10 APIs):**
- DescribeCluster (0-1), OffsetForLeaderEpoch (0-4), DeleteRecords (0-2), DescribeLogDirs (0-4), AlterReplicaLogDirs (0-2)
- ListPartitionReassignments (0-0), AlterPartitionReassignments (0-0), ElectLeaders (0-2), OffsetDelete (0-0), UpdateFeatures (0-1), UnregisterBroker (0-0)

**SCRAM Credentials (2 APIs):**
- DescribeUserScramCredentials (0-0), AlterUserScramCredentials (0-0)

**Quorum (1 API):**
- DescribeQuorum (0-1)

**Total: 50 APIs implemented (production-level Kafka 3.x compatibility)**

## Common Tasks

### Adding a new Kafka API

1. Add handler in `src/protocol/handlers/mod.rs`
2. Update `process_message()` match arm in `src/protocol/kafka.rs`
3. Add tests in `tests/`

### Adding storage functionality

1. Implement in appropriate `src/storage/*.rs` file
2. Update `TopicManager` if needed
3. Expose via `src/lib.rs` if public API

### Adding CLI command

1. Add subcommand enum variant in `src/cli.rs`
2. Implement handler function
3. For complex utilities, add module in `src/cli_utils/`
4. Update help text

### Adding HTTP API endpoint

1. Add handler in `src/server/api.rs` or create new `*_api.rs` file
2. Register route in `src/server/http.rs`
3. Add tests

### Adding a feature flag

1. Add feature in `Cargo.toml` under `[features]`
2. Use `#[cfg(feature = "...")]` for conditional compilation
3. Update `full` feature if enterprise-tier
4. Update CLAUDE.md feature documentation

## Configuration

Streamline supports TOML config files, CLI args, and environment variables.

**Generate example config:**
```bash
./streamline --generate-config > streamline.toml
```

**Core server options (CLI args or env vars):**
- `-c, --config` / `STREAMLINE_CONFIG` - TOML config file path
- `--listen-addr` / `STREAMLINE_LISTEN_ADDR` (default: `0.0.0.0:9092`)
- `--http-addr` / `STREAMLINE_HTTP_ADDR` (default: `0.0.0.0:9094`)
- `--data-dir` / `STREAMLINE_DATA_DIR` (default: `./data`)
- `--log-level` / `STREAMLINE_LOG_LEVEL` (default: `info`)
- `--in-memory` / `STREAMLINE_IN_MEMORY` - Enable in-memory mode
- `--playground` / `STREAMLINE_PLAYGROUND` - Start with demo topics

**Runtime options (performance tuning):**
- `--runtime-mode` - Runtime mode: `tokio` (default) or `sharded` (thread-per-core)
- `--shard-count` - Number of CPU-pinned shards (default: CPU count)
- `--enable-cpu-affinity` - Pin shard threads to CPUs (default: true)
- `--enable-polling` - Use busy-polling instead of blocking (default: false)

**Feature-specific options (require corresponding feature flag):**
- Auth: `--auth-enabled`, `--auth-sasl-mechanisms`, `--auth-users-file`
- ACL: `--acl-enabled`, `--acl-file`, `--acl-super-users`
- Clustering: `--node-id`, `--seed-nodes`, `--inter-broker-addr`
- Encryption: `--encryption-enabled`, `--encryption-key-file`
- Telemetry: `--otel-enabled`, `--otel-endpoint`

See `./streamline --help` for full list.

## CLI Commands Reference

### Produce Messages
```bash
streamline-cli produce <topic> -m "message"           # Single message
streamline-cli produce <topic> --file messages.txt   # From file (one per line)
streamline-cli produce <topic> -m "msg" -k "key"     # With key
streamline-cli produce <topic> -m "msg" -H "k=v"     # With headers
streamline-cli produce <topic> -m "msg" -p 2         # To specific partition
streamline-cli produce <topic> --template '{"id":"{{uuid}}","ts":{{timestamp}}}' -n 100
```

**Message Templates** - Use `--template` with variable substitution:
- `{{uuid}}` - Random UUID
- `{{timestamp}}` - Unix timestamp (ms)
- `{{i}}` - Iteration counter (0-based)
- `{{random:min:max}}` - Random integer in range
- `{{now:format}}` - Formatted datetime (e.g., `{{now:%Y-%m-%d}}`)

### Consume Messages
```bash
streamline-cli consume <topic> --from-beginning      # From start
streamline-cli consume <topic> --offset 100          # From offset
streamline-cli consume <topic> -n 10                 # Limit count
streamline-cli consume <topic> -f                    # Follow (like tail -f)
streamline-cli consume <topic> --show-keys --show-timestamps --show-offsets
```

**Time-Travel Consume** - Start from specific time:
```bash
streamline-cli consume <topic> --at "2024-01-15 14:30:00"
streamline-cli consume <topic> --at "yesterday"
streamline-cli consume <topic> --last "5m"           # Last 5 minutes
streamline-cli consume <topic> --last "2h"           # Last 2 hours
```

**Filtering & Extraction**:
```bash
streamline-cli consume <topic> --grep "error"        # Filter by regex
streamline-cli consume <topic> --grep "error" -i     # Case-insensitive
streamline-cli consume <topic> --grep "error" -v     # Invert match (exclude)
streamline-cli consume <topic> --grep-key "user-"    # Search keys too
streamline-cli consume <topic> --jq "$.user.id"      # JSONPath extraction
streamline-cli consume <topic> --sample "10%"        # Random 10% sample
streamline-cli consume <topic> --sample "100"        # Exact 100 messages
```

### Topic Management
```bash
streamline-cli topics list
streamline-cli topics create <name> --partitions 3
streamline-cli topics create <name> --storage-mode diskless  # In-memory only
streamline-cli topics describe <name>
streamline-cli topics delete <name>
streamline-cli topics alter <name> --retention-ms 86400000
streamline-cli topics watch                          # Real-time metrics
streamline-cli topics export <name> -o backup.jsonl  # Export data
streamline-cli topics import <name> -i backup.jsonl  # Import data
```

### Consumer Groups
```bash
streamline-cli groups list
streamline-cli groups describe <group-id>
streamline-cli groups delete <group-id>
streamline-cli groups reset <group-id> -t <topic> --to-earliest          # Dry run
streamline-cli groups reset <group-id> -t <topic> --to-latest --execute  # Apply
streamline-cli groups reset <group-id> -t <topic> --to-offset 100 --execute
```

### Users & ACLs
```bash
streamline-cli users list --users-file users.yaml
streamline-cli users add --users-file users.yaml <username> -p <password>
streamline-cli acls list
streamline-cli acls add --resource-type topic --resource-name my-topic --principal User:alice --operation read --permission allow
```

### CLI Utilities
```bash
streamline-cli info                    # Show system information
streamline-cli doctor                  # Run diagnostics and health checks
streamline-cli quickstart              # Interactive setup wizard
streamline-cli top                     # TUI dashboard (htop-style)
streamline-cli shell                   # Interactive REPL
streamline-cli benchmark               # Performance benchmarks
streamline-cli benchmark --list-profiles
streamline-cli history                 # Show command history
streamline-cli completions bash        # Generate shell completions
```

### Migration & Profiles
```bash
streamline-cli migrate from-kafka --kafka-servers localhost:9092
streamline-cli migrate from-kafka --topics "topic1,topic2" --execute
streamline-cli profile list            # List connection profiles
streamline-cli profile add prod --data-dir /data/prod
streamline-cli -P prod topics list     # Use profile
```

### Sink Connectors (requires iceberg or delta-lake feature)
```bash
# Create an Iceberg sink
streamline-cli sink create events-iceberg \
  --topics events \
  --catalog-uri http://localhost:8181 \
  --catalog-type rest \
  --namespace default \
  --table events \
  --commit-interval-ms 60000 \
  --partitioning time_based_hour \
  --start

# List all sinks
streamline-cli sink list

# Show sink status and metrics
streamline-cli sink status events-iceberg
streamline-cli sink status events-iceberg --json

# Start/stop sinks
streamline-cli sink start events-iceberg
streamline-cli sink stop events-iceberg

# Delete a sink
streamline-cli sink delete events-iceberg --yes
```

**Supported Catalog Types:**
- `rest` - REST catalog (default, fully supported)
- `hive` - Hive Metastore catalog (not yet available - upstream dependency issue)
- `glue` - AWS Glue catalog (not yet available - upstream dependency issue)

**Partitioning Strategies:**
- `time_based_hour` - Partition by hour (default)
- `time_based_day` - Partition by day
- `time_based_month` - Partition by month
- `field_based` - Partition by a field in the record (e.g., region, tenant_id)
- `none` - No partitioning

**Iceberg Sink Configuration (Programmatic):**
```rust
IcebergSinkConfig {
    catalog_uri: "http://localhost:8181".to_string(),
    catalog_type: CatalogType::Rest,
    namespace: "default".to_string(),
    table: "events".to_string(),
    commit_interval_ms: 60000,
    max_batch_size: 1000,
    partitioning: PartitioningConfig {
        strategy: PartitioningStrategy::TimeBasedHour,
        field: None,  // Set for FieldBased strategy
        time_granularity: TimeGranularity::Hour,
    },
    compression: ParquetCompression::Snappy,  // None, Snappy, Gzip, Lz4, Zstd
    max_retries: 3,                           // Retry attempts for catalog commits
    retry_delay_ms: 1000,                     // Initial delay (exponential backoff)
    schema_evolution: IcebergSchemaEvolution::Strict,  // Strict, AddNewColumns, AddAndPromote
    // ...
}
```

**Delta Lake Sink** (requires `delta-lake` feature):
- Write streaming data to Delta Lake tables
- Supports local filesystem and cloud storage (S3, Azure, GCS)
- Write modes: Append, Overwrite, Merge
- Schema evolution policies: Strict, AddNewColumns, AddAndWiden

### Analytics & SQL Queries (requires analytics feature)
```bash
# Execute SQL queries on streaming data
streamline-cli query "SELECT * FROM streamline_topic('events') LIMIT 10"
streamline-cli query "SELECT user_id, COUNT(*) as cnt FROM streamline_topic('events') GROUP BY user_id" --output-format json
streamline-cli query "SELECT * FROM streamline_topic('logs') WHERE timestamp > now() - interval '1 hour'" --max-rows 100
streamline-cli query "SELECT avg(response_time) FROM streamline_topic('metrics')" --no-cache
```

### Global Options
- `--data-dir <path>` - Data directory (default: ./data)
- `--format json|text` - Output format
- `--no-color` - Disable colored output
- `-y, --yes` - Skip confirmation prompts
- `-P, --profile <name>` - Use named connection profile
- `-o, --output <file>` - Write output to file
