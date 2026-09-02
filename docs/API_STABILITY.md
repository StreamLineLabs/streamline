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
| `sink` | 0.2.0 | Sink connectors. Serverless/cloud-function sinks require `--features serverless`; **Iceberg and Delta Lake sinks are unavailable** (see notes below) |
| `streamql` | 0.2.0 | SQL-like stream processing DSL |
| `cdc` | 0.2.0 | Change Data Capture (PostgreSQL, MySQL, etc.) |
| `edge` | 0.2.0 | Edge-first architecture, offline sync |
| `wasm` | 0.2.0 | WebAssembly transform runtime |
| `graphql` | 0.2.0 | GraphQL API |
| `transport` | 0.2.0 | QUIC/WebTransport |
| `timeseries` | 0.2.0 | Time-series native storage |
| `dsl` | 0.2.0 | Stream processing DSL |
| `ai` | 0.2.0 | AI-powered features |

#### ⛔ Unavailable: Iceberg and Delta Lake sink connectors

The `sink::iceberg` and `sink::delta` connectors are **not available in any
supported build**, including `--features full` and `--all-features`.

The `iceberg` and `delta-lake` Cargo features are retained as compatibility
no-ops so existing build scripts keep resolving, but they enable no
dependencies. `SinkManager::create_sink` rejects `SinkType::Iceberg` and
`SinkType::DeltaLake` with an explicit error; `sink::unavailable::is_available`
reports `false` for both.

**Why**: every upstream release compatible with our MSRV (Rust 1.88) pulls
`quick-xml < 0.41`, affected by RUSTSEC-2026-0194 and RUSTSEC-2026-0195 (both
CVSS 7.5 and reachable from attacker-influenced S3/Azure list XML — exactly what
these connectors parse). `deltalake` additionally forces
`native-tls` → OpenSSL through `delta_kernel`'s `reqwest` default features.
Streamline does not suppress advisories, so the dependencies were removed rather
than shipped vulnerable.

**Restoration**: the implementations are preserved verbatim behind the
never-enabled `iceberg_backend` / `delta_backend` cfgs and will be re-enabled
once [iceberg-rust](https://github.com/apache/iceberg-rust) and
[delta-rs](https://github.com/delta-io/delta-rs) publish MSRV-compatible
releases built on `quick-xml >= 0.41` without `native-tls`.
`tests/dependency_security_test.rs` and
`tests/sink_connector_availability_test.rs` enforce both halves of this.

#### 🔧 Feature-gated: Serverless and Cloud Function sink connectors

`SinkType::Serverless` and `SinkType::CloudFunction` are available **only** with
`--features serverless`. Unlike the lakehouse connectors this is a build-time
choice, not a removal: the feature is what pulls in the HTTP client
(`crate::http_client`, backed by `reqwest`) that both connectors deliver
through, and `src/sink/serverless.rs` / `src/sink/cloud_functions.rs` are
compiled only under it.

Without the feature, `sink::unavailable::is_available` reports `false` and
`SinkManager::create_sink` rejects both types **before** duplicate-name, topic
and configuration validation, with a reason naming `--features serverless`.
The ordering is part of the contract: the caller must be told the real problem
rather than an unrelated "topic does not exist", and no sink may be registered
that would accept records and drop them on delivery.

With the feature enabled, construction, registration and delivery are unchanged.

`tests/sink_connector_availability_test.rs` covers both halves and is run twice
in CI — once for the default build and once with `--features serverless`.

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

The `Since` column in the tier tables above records when a module was
introduced and does not change. This matrix records which releases are
currently supported.

| Version | Status | End of Support |
|---------|--------|----------------|
| 0.4.x | **Current** | Until 0.6.0 release |
| 0.3.x | Supported (previous minor) | Until 0.5.0 release |
| <= 0.2.x | End of life | Unsupported |

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

`SECURITY.md` is the authoritative statement of the vulnerability-response
policy. The table below mirrors it; if the two ever disagree, `SECURITY.md`
wins.

| Severity | Acknowledgment | Fix Target | Disclosure |
|----------|---------------|------------|------------|
| **Critical** (CVSS 9.0-10.0) | 48 hours | 48 hours | After fix + 7 days |
| **High** (CVSS 7.0-8.9) | 48 hours | 7 days | After fix + 14 days |
| **Medium** (CVSS 4.0-6.9) | 48 hours | 30 days | After fix + 30 days |
| **Low** (CVSS 0.1-3.9) | 48 hours | Next release | With release notes |

Report vulnerabilities to: **security@streamlinelabs.dev** (see `SECURITY.md`).

## SDK Version Compatibility

All official SDKs target compatibility with the **current** and **previous** minor version of the server:

| SDK | Min Server Version | Max Server Version |
|-----|-------------------|-------------------|
| streamline-java-sdk 0.4.x | 0.3.0 | 0.4.x |
| streamline-python-sdk 0.4.x | 0.3.0 | 0.4.x |
| streamline-go-sdk 0.4.x | 0.3.0 | 0.4.x |
| streamline-node-sdk 0.4.x | 0.3.0 | 0.4.x |
| streamline-rust-sdk 0.4.x | 0.3.0 | 0.4.x |
| streamline-dotnet-sdk 0.4.x | 0.3.0 | 0.4.x |
| streamline-wasm-sdk 0.4.x | 0.3.0 | 0.4.x |

> SDK releases live in their own repositories. These rows state the intended
> compatibility window for the 0.4.x line; they are not a claim that every SDK
> has already been tagged 0.4.x.
