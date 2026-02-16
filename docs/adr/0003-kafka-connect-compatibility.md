# ADR-0003: Kafka Connect Compatibility

## Status

Proposed

## Context

Kafka Connect is the standard framework for streaming data between Apache Kafka and external systems, providing 100+ pre-built connectors for databases, cloud services, file systems, and search engines. Users evaluating Kafka-compatible platforms expect access to this connector ecosystem.

Both Redpanda and Confluent Platform fully support Kafka Connect workers. Without connector support, Streamline faces a significant adoption barrier for data integration use cases such as CDC, data lake ingestion, search index synchronization, and cloud service integration.

Building a complete connector ecosystem from scratch is impractical. However, Streamline already has native sink connectors in `src/sink/` for Iceberg and Delta Lake, demonstrating the value of tightly integrated, high-performance connectors.

We need a strategy that provides immediate access to the existing ecosystem while enabling native connectors where performance matters.

## Decision

We will implement a hybrid approach to Kafka Connect compatibility:

1. **External Kafka Connect worker compatibility** — Ensure standard Kafka Connect workers can run against Streamline as a broker, leveraging existing Kafka protocol compatibility (ADR-0001)
2. **Native connector framework** — Extend Streamline's built-in connector capabilities (`src/sink/`) with a registry and management API
3. **Unified management plane** — Expose both external and native connectors through the HTTP API on port 9094

### Approaches Considered

**Option A: Full Kafka Connect API compatibility only** — Low effort but no differentiation; requires separate JVM process for workers.

**Option B: Native connector framework only** — Optimal performance but years to reach connector parity; impractical short-term.

**Option C: Hybrid approach (chosen)** — External workers for breadth (100+ connectors), native connectors for depth (zero-copy I/O, no JVM overhead). Users start with Kafka Connect workers and migrate to native connectors for performance-critical pipelines.

### Protocol Requirements

For external Kafka Connect workers, Streamline must support:

- Consumer Group APIs (JoinGroup, SyncGroup, Heartbeat, LeaveGroup)
- Produce/Fetch APIs (core message production and consumption)
- Admin APIs (CreateTopics, DescribeConfigs for Connect internal topics)
- Transactional APIs (InitProducerId, AddPartitionsToTxn, EndTxn for exactly-once)

Most are already implemented. Primary gaps are transactional APIs and certain admin API features.

### Implementation Plan

**Phase 1: Kafka Connect Worker Compatibility (v0.4)**
- Validate Kafka Connect worker compatibility with Streamline
- Ensure Connect internal topics (`connect-configs`, `connect-offsets`, `connect-status`) work with compacted topic support
- Implement missing transactional API support for exactly-once source connectors
- Integration tests for Debezium, JDBC, S3, Elasticsearch, and BigQuery connectors

**Phase 2: Native Connector Registry (v0.5)**
- Formalize the native connector trait in `src/connector/`
- Connector registry with versioning and configuration schema validation
- REST API for connector management on port 9094
- Migrate existing `src/sink/` Iceberg and Delta Lake connectors to the new framework

**Phase 3: Managed Connector Marketplace (v0.6)**
- WASM-based connector plugins for isolation and portability
- Connector marketplace for discovering and installing connectors
- Monitoring dashboard with per-connector metrics

## Consequences

### Positive

- Immediate access to 100+ existing Kafka Connect connectors
- Easier enterprise adoption — data integration is table stakes
- Native connectors for critical paths outperform JVM-based Connect workers
- Familiar API for teams already using Kafka Connect
- Incremental delivery — each phase delivers standalone value

### Negative

- Kafka Connect REST API must be tracked across Kafka versions
- Connect worker versions may advance features we don't yet support
- Dual runtime (external workers + native connectors) increases testing surface
- Users wanting full connector coverage still need a JVM, partially undermining single-binary value proposition

### Neutral

- Docker Compose templates bundling Streamline + Kafka Connect worker will be provided for easy deployment
- Kafka Connect API compatibility will be pinned to version range 3.4–3.7 initially
- Automated compatibility testing in CI against multiple Kafka Connect versions will help manage version drift
