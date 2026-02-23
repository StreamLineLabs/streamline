# API Stability Guide

This document defines the stability tiers for Streamline's public API surface,
compatibility guarantees, and the process for evolving APIs.

## Stability Tiers

Every public module in Streamline is classified into one of three tiers:

### 🟢 Stable

**Guarantee**: No breaking changes within a major version. Backward-compatible additions only.

| Module | Since | Description |
|--------|-------|-------------|
| `server` | 0.1.0 | TCP/HTTP server, connection handling |
| `storage` | 0.1.0 | Topic/partition/segment storage engine |
| `consumer` | 0.1.0 | Consumer group coordinator, offset management |
| `protocol` | 0.1.0 | Kafka wire protocol (50+ APIs) |
| `config` | 0.1.0 | ServerConfig, ServerArgs, environment variables |
| `error` | 0.1.0 | StreamlineError, Result type alias, ErrorHint |
| `analytics` | 0.2.0 | DuckDB-based SQL analytics engine |
| `embedded` | 0.2.0 | EmbeddedStreamline, StreamlineEngine trait |

Stable modules follow [Semantic Versioning 2.0.0](https://semver.org/):
- **PATCH**: Bug fixes, performance improvements
- **MINOR**: Backward-compatible new features, new optional fields
- **MAJOR**: Breaking changes (with 2-version deprecation period)

### 🟡 Beta

**Guarantee**: API is feature-complete but may have minor breaking changes in minor versions. Breaking changes are documented in the CHANGELOG.

| Module | Since | Description |
|--------|-------|-------------|
| `transaction` | 0.2.0 | Exactly-once semantics, transaction coordinator |
| `cluster` | 0.2.0 | Raft-based clustering (OpenRaft) |
| `replication` | 0.2.0 | ISR management, data replication |
| `schema` | 0.2.0 | Schema registry (Avro, Protobuf, JSON Schema) |
| `auth` | 0.2.0 | SASL/SCRAM/OAuth authentication, ACLs |
| `metrics` | 0.2.0 | Prometheus metrics, JMX compatibility |
| `telemetry` | 0.2.0 | OpenTelemetry tracing/metrics |
| `observability` | 0.2.0 | System metrics, anomaly detection, distributed tracing |
| `gateway` | 0.2.0 | Multi-protocol gateway (MQTT, AMQP, gRPC) |
| `connect` | 0.2.0 | Connector framework, declarative pipelines |
| `featurestore` | 0.2.0 | ML feature store, materialized views |
| `ffi` | 0.2.0 | C FFI bindings for embedded use |

### 🔴 Experimental

**Guarantee**: None. API may change or be removed at any time. Use at your own risk in production.

| Module | Since | Description |
|--------|-------|-------------|
| `sink` | 0.2.0 | Iceberg/Delta Lake sinks |
| `streamql` | 0.2.0 | SQL-like stream processing DSL |
| `cdc` | 0.2.0 | Change Data Capture (PostgreSQL, MySQL, etc.) |
| `edge` | 0.2.0 | Edge-first architecture, offline sync |
| `wasm` | 0.2.0 | WebAssembly transform runtime |
| `graphql` | 0.2.0 | GraphQL API |
| `transport` | 0.2.0 | QUIC/WebTransport |
| `timeseries` | 0.2.0 | Time-series native storage |
| `dsl` | 0.2.0 | Stream processing DSL |
| `ai` | 0.2.0 | AI-powered features |

### 🔒 Internal (crate-visible only)

These modules are `pub(crate)` and not part of the public API:
`dlq`, `lineage`, `playground`, `plugin`, `policy`, `pubsub`, `replay`, `smart_partition`, `testing`, `stateful`, `lifecycle`, `obs_pipeline`, `multitenancy`, `network`

## Deprecation Process

1. **Announcement**: Deprecated APIs are marked with `#[deprecated(since = "X.Y.Z", note = "Use ... instead")]`
2. **Grace Period**: Deprecated APIs remain functional for at least **2 minor versions**
3. **Removal**: Deprecated APIs are removed in the next major version
4. **Migration Guide**: Every deprecation includes a migration guide in the CHANGELOG

## Kafka Protocol Compatibility

### Supported Kafka Client Libraries

Streamline aims for compatibility with these Kafka client libraries:

| Client Library | Language | Min Version | Status | Notes |
|---------------|----------|-------------|--------|-------|
| librdkafka | C/C++ | 2.0+ | ✅ Tested | Used by confluent-kafka-python, confluent-kafka-go, node-rdkafka |
| kafka-clients | Java | 3.0+ | ✅ Tested | Official Apache Kafka client |
| kafka-python | Python | 2.0+ | ✅ Tested | Pure Python |
| aiokafka | Python | 0.9+ | ✅ Tested | Async Python |
| confluent-kafka-python | Python | 2.0+ | ✅ Tested | librdkafka-based |
| Sarama | Go | 1.40+ | ✅ Tested | Used by Streamline Go SDK |
| franz-go | Go | 1.15+ | ✅ Tested | Modern Go client |
| kafkajs | Node.js | 2.0+ | ✅ Tested | Used by Streamline Node SDK |
| rdkafka-rust | Rust | 0.34+ | ✅ Tested | librdkafka bindings |
| confluent-kafka-dotnet | C# | 2.0+ | ✅ Tested | .NET librdkafka wrapper |

### Kafka API Version Support

| API | Min Version | Max Version | Status |
|-----|-------------|-------------|--------|
| ApiVersions | 0 | 3 | ✅ Stable |
| Metadata | 0 | 12 | ✅ Stable |
| Produce | 0 | 9 | ✅ Stable |
| Fetch | 0 | 15 | ✅ Stable |
| ListOffsets | 0 | 7 | ✅ Stable |
| CreateTopics | 0 | 7 | ✅ Stable |
| DeleteTopics | 0 | 6 | ✅ Stable |
| FindCoordinator | 0 | 4 | ✅ Stable |
| JoinGroup | 0 | 9 | ✅ Stable |
| SyncGroup | 0 | 5 | ✅ Stable |
| Heartbeat | 0 | 4 | ✅ Stable |
| LeaveGroup | 0 | 5 | ✅ Stable |
| OffsetCommit | 0 | 8 | ✅ Stable |
| OffsetFetch | 0 | 8 | ✅ Stable |
| DescribeGroups | 0 | 5 | ✅ Stable |
| ListGroups | 0 | 4 | ✅ Stable |
| SaslHandshake | 0 | 1 | 🟡 Beta |
| SaslAuthenticate | 0 | 2 | 🟡 Beta |
| CreateAcls | 0 | 3 | 🟡 Beta |
| DescribeAcls | 0 | 3 | 🟡 Beta |
| DeleteAcls | 0 | 3 | 🟡 Beta |
| InitProducerId | 0 | 4 | 🟡 Beta |
| AddPartitionsToTxn | 0 | 4 | 🟡 Beta |
| EndTxn | 0 | 3 | 🟡 Beta |
| DescribeConfigs | 0 | 4 | ✅ Stable |
| AlterConfigs | 0 | 2 | ✅ Stable |
| ConsumerGroupHeartbeat | 0 | 0 | 🔴 Experimental |
| ConsumerGroupDescribe | 0 | 1 | 🔴 Experimental |

## Version Lifecycle

### Release Types

| Type | Branch | Cadence | Support Window |
|------|--------|---------|----------------|
| **Stable Release** | `main` | Every 4-6 weeks | Current + previous minor |
| **LTS Release** | `lts/X.Y` | Every 6 months | 12 months of patch releases |
| **Nightly** | `main` (HEAD) | Daily | No support guarantee |

### Current Version Matrix

| Version | Release Date | Status | End of Support |
|---------|-------------|--------|----------------|
| 0.2.x | 2026-02 | **Current** | Until 0.3.0 release |
| 0.1.x | 2026-01 | Supported | Until 0.3.0 release |

### v1.0 Release Criteria

The following must be met before releasing v1.0:

- [ ] All "Stable" modules pass 3 consecutive release cycles without breaking changes
- [ ] Kafka client compatibility matrix passes for all listed libraries
- [ ] Benchmark dashboard published with reproducible results
- [ ] 10+ external contributors
- [ ] 5+ documented production deployments
- [ ] Security audit completed by external firm
- [ ] CVE response process tested with simulated vulnerability

## CVE Response Policy

| Severity | Acknowledgment | Fix Target | Disclosure |
|----------|---------------|------------|------------|
| **Critical** (CVSS 9.0+) | 24 hours | 72 hours | After fix + 7 days |
| **High** (CVSS 7.0-8.9) | 48 hours | 7 days | After fix + 14 days |
| **Medium** (CVSS 4.0-6.9) | 7 days | 30 days | After fix + 30 days |
| **Low** (CVSS 0.1-3.9) | 14 days | Next release | With release notes |

Report vulnerabilities to: **security@streamline.dev**

## SDK Version Compatibility

All official SDKs target compatibility with the **current** and **previous** minor version of the server:

| SDK | Min Server Version | Max Server Version |
|-----|-------------------|-------------------|
| streamline-java-sdk 0.2.x | 0.1.0 | 0.2.x |
| streamline-python-sdk 0.2.x | 0.1.0 | 0.2.x |
| streamline-go-sdk 0.2.x | 0.1.0 | 0.2.x |
| streamline-node-sdk 0.2.x | 0.1.0 | 0.2.x |
| streamline-rust-sdk 0.2.x | 0.1.0 | 0.2.x |
| streamline-dotnet-sdk 0.2.x | 0.1.0 | 0.2.x |
| streamline-wasm-sdk 0.2.x | 0.2.0 | 0.2.x |
