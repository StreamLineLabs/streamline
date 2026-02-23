# Streamline Architecture

This document describes the high-level architecture of Streamline.
If you want to contribute, this is a good place to start.

**Related docs**: [Development Guide](DEVELOPMENT.md) · [API Stability](API_STABILITY.md) · [Benchmarks](BENCHMARKS.md) · [Troubleshooting](TROUBLESHOOTING.md) · [ADRs](adr/)

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Client Connections                             │
│   Kafka Clients  │  MQTT Devices  │  AMQP Services  │  gRPC Services  │
└────────┬─────────┴───────┬────────┴────────┬─────────┴────────┬────────┘
         │                 │                 │                  │
┌────────▼─────────────────▼─────────────────▼──────────────────▼────────┐
│                        Protocol Layer (src/protocol/, src/gateway/)     │
│  ┌──────────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐           │
│  │ Kafka Wire   │  │ MQTT     │  │ AMQP 1.0 │  │ gRPC     │           │
│  │ Protocol     │  │ 3.1.1/5.0│  │          │  │          │           │
│  │ (50+ APIs)   │  │          │  │          │  │          │           │
│  └──────┬───────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘           │
│         └────────────────┴─────────────┴─────────────┘                 │
│                              │                                         │
│                    ┌─────────▼──────────┐                              │
│                    │  Pipeline Router   │                              │
│                    └─────────┬──────────┘                              │
└──────────────────────────────┼──────────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────────┐
│                        Server Layer (src/server/)                       │
│                                                                         │
│  ┌─────────────┐  ┌────────────┐  ┌─────────────┐  ┌───────────────┐  │
│  │ HTTP API    │  │ WebSocket  │  │ Playground  │  │ Schema        │  │
│  │ (46 routes) │  │ Streaming  │  │ API         │  │ Registry      │  │
│  └─────────────┘  └────────────┘  └─────────────┘  └───────────────┘  │
│  ┌─────────────┐  ┌────────────┐  ┌─────────────┐  ┌───────────────┐  │
│  │ GitOps API  │  │ Fleet API  │  │ StreamQL    │  │ Connect API   │  │
│  │             │  │ (edge)     │  │ Engine      │  │               │  │
│  └─────────────┘  └────────────┘  └─────────────┘  └───────────────┘  │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────────┐
│                       Core Engine                                       │
│                                                                         │
│  ┌──────────────────┐  ┌─────────────────┐  ┌───────────────────────┐  │
│  │ TopicManager     │  │ GroupCoordinator │  │ TransactionCoord.     │  │
│  │ (src/storage/)   │  │ (src/consumer/) │  │ (src/transaction/)    │  │
│  │                  │  │                 │  │                       │  │
│  │ Topics           │  │ Consumer Groups │  │ Exactly-once          │  │
│  │ Partitions       │  │ Offsets         │  │ Idempotent Producers  │  │
│  │ Segments         │  │ Rebalancing     │  │ Transaction Log       │  │
│  │ WAL              │  │ KIP-848         │  │                       │  │
│  └──────────────────┘  └─────────────────┘  └───────────────────────┘  │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────────────┐
│                       Storage Layer (src/storage/)                      │
│                                                                         │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌────────────────────┐  │
│  │ Segments   │  │ WAL       │  │ Index     │  │ Compression        │  │
│  │ (STRM fmt) │  │           │  │ (bloom,   │  │ (LZ4, Zstd,       │  │
│  │            │  │           │  │  offset,  │  │  Snappy, Gzip)     │  │
│  │            │  │           │  │  time)    │  │                    │  │
│  └───────────┘  └───────────┘  └───────────┘  └────────────────────┘  │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌────────────────────┐  │
│  │ Zero-copy │  │ io_uring  │  │ mmap      │  │ Encryption         │  │
│  │ sendfile  │  │ (Linux)   │  │           │  │ (AES-256-GCM)      │  │
│  └───────────┘  └───────────┘  └───────────┘  └────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

## Module Map

The codebase is organized into 63 modules in `src/lib.rs`:

### Stable Public API
These modules have backward-compatibility guarantees per `docs/API_STABILITY.md`:

| Module | Purpose | Files |
|--------|---------|-------|
| `config` | Server configuration, CLI args | 1 |
| `consumer` | Consumer groups, offset management | ~5 |
| `embedded` | Library API (`EmbeddedStreamline`, `StreamlineEngine` trait) | 1 |
| `error` | `StreamlineError` via `thiserror`, Kafka error code mapping | 1 |
| `protocol` | Kafka wire protocol (50+ APIs) | ~51 |
| `server` | TCP/HTTP server, 46 API route modules | 46 |
| `storage` | Segment-based persistent storage engine | 38 |
| `transaction` | Exactly-once semantics, coordinator | ~3 |

### Feature-Gated Modules
Controlled by Cargo feature flags (71 total):

| Module | Feature | Purpose |
|--------|---------|---------|
| `auth` | `auth` | SASL/SCRAM/OAuth, ACLs |
| `cluster` | `clustering` | Raft consensus (OpenRaft) |
| `schema` | `schema-registry` | Avro/Protobuf/JSON Schema registry |
| `analytics` | `analytics` | DuckDB embedded SQL engine |
| `sink` | `iceberg`/`delta-lake` | Lakehouse connectors |
| `cdc` | `postgres-cdc` etc. | Change Data Capture |
| `edge` | `edge` | Edge runtime, fleet management, MQTT bridge |
| `featurestore` | `featurestore` | ML feature store, materialized views |
| `graphql` | `graphql` | GraphQL API |

### Internal Modules
Not part of the public API (`pub(crate)`):

`connect` (30 connectors, 16 implemented), `dlq`, `ffi` (C FFI), `gateway` (MQTT/AMQP/gRPC),
`lineage`, `observability` (anomaly detection, auto-instrumentation), `playground`, `plugin`
(extensions SDK), `streamql` (SQL-on-streams, 17 sub-modules), `wasm`, `telemetry`

## Data Flow

### Produce Path
```
Client → TCP Accept → Protocol Decode → Auth Check → Topic Lookup/Create
→ Partition Assignment → WAL Write → Segment Append → ACK to Client
```

### Consume Path
```
Client → TCP Accept → Protocol Decode → Auth Check → Group Coordinator
→ Partition Assignment → Segment Read (zero-copy) → Response Encode → Send
```

### Connector Pipeline
```
Source Connector → poll() → Records → [Transform Chain (WASM)] → Sink Connector → put()
```

## Key Design Decisions

1. **Single binary** — All features compiled into one executable via feature flags.
   No microservices, no external coordinators. See ADR-001.

2. **Segment-based storage** — Custom `STRM` format (64-byte header, magic bytes).
   Not using RocksDB or other embedded databases. See ADR-003.

3. **Kafka protocol as primary interface** — Wire compatibility means existing
   clients work unchanged. Custom protocol extensions via HTTP API.

4. **Feature flags over configuration** — Enterprise features (auth, clustering,
   encryption) are compiled out in the `lite` build, not just disabled at runtime.

5. **Trait-based extensibility** — `StreamlineEngine` for embedding,
   `NativeConnector` / `SinkConnectorTrait` / `SourceConnectorTrait` for connectors,
   `StreamlineTransform` for WASM extensions.

## Build Editions

| Edition | Command | Size | Features |
|---------|---------|------|----------|
| Lite | `cargo build` | ~8 MB | Core streaming + compression |
| Full | `cargo build --features full` | ~20 MB | Everything |

## Directory Structure

```
streamline/
├── src/
│   ├── main.rs              # Server binary entry point
│   ├── cli.rs               # CLI binary entry point
│   ├── lib.rs               # Library root (63 modules)
│   ├── embedded.rs          # Embeddable library API
│   ├── error.rs             # Error types
│   ├── config/              # Configuration
│   ├── server/              # HTTP + WebSocket (46 API modules)
│   ├── protocol/            # Kafka wire protocol
│   │   └── kafka/           # Handlers, codecs, tests
│   ├── storage/             # Segment engine (38 files)
│   ├── consumer/            # Consumer groups
│   ├── transaction/         # Exactly-once
│   ├── connect/             # Connector framework
│   │   ├── hub.rs           # 30-connector catalog
│   │   ├── builtin.rs       # 6 I/O implementations
│   │   ├── builtin_extended.rs  # 10 more implementations
│   │   └── declarative.rs   # YAML pipeline config
│   ├── streamql/            # SQL-on-streams (17 files)
│   ├── gateway/             # MQTT, AMQP, gRPC adapters
│   ├── edge/                # Edge runtime (15 files)
│   ├── observability/       # Metrics, tracing, anomaly detection
│   ├── plugin/              # Extensions SDK (WASM scaffolding)
│   └── ffi/                 # C FFI bindings
├── tests/                   # 58 integration test files
├── benches/                 # 11 benchmark suites
├── fuzz/                    # 5 fuzz targets
├── contrib/
│   └── grafana-datasource/  # Grafana plugin
├── docs/
│   ├── API_STABILITY.md     # Module stability tiers
│   ├── V1_RELEASE_CRITERIA.md
│   └── LAUNCH_CHECKLIST.md
└── scripts/
    ├── check-v1-readiness.sh
    └── record-demo.sh
```

## Contributing

See [CONTRIBUTING.md](../../../.github/CONTRIBUTING.md) for guidelines.
For architecture questions, open a [Discussion](https://github.com/streamlinelabs/streamline/discussions).
