# ADR-0007: AI/ML Feature Store Mode

## Status

Proposed

## Context

ML/AI workloads need real-time feature computation from event streams with low latency and point-in-time correctness. Existing feature stores (Feast, Tecton) require separate infrastructure — a dedicated online store (Redis/DynamoDB), offline store, and orchestration layer — creating operational complexity that conflicts with Streamline's single-binary philosophy.

Streamline already has the building blocks: streaming ingestion via Kafka protocol, embedded analytics via DuckDB, and a feature flag system. The `featurestore` feature flag exists in `Cargo.toml` as an experimental next-gen feature, and the initial module structure (`FeatureStoreManager`, `OnlineStore`, `OfflineStore`, `FeatureRegistry`, `FeatureServer`) is implemented in `src/featurestore/`.

## Decision

Add a feature store mode activated via the `featurestore` feature flag that provides:

1. **Feature Registry** — Centralized, versioned feature definitions with entity associations, feature groups, and feature views. Supports lineage tracking from source topics through transformations to computed features.

2. **Online Store** — In-memory hash map with WAL-backed persistence for low-latency point lookups (<10ms p99). TTL-based eviction bounds memory usage. Entity-keyed access supports single and batch retrieval.

3. **Offline Store** — Historical feature computation via DuckDB with point-in-time join support for training dataset generation. Materialization from offline to online store via `FeatureStoreManager::materialize()`.

4. **Feature Computation** — Streaming pipelines with tumbling, sliding, and session window aggregations. Features defined using SQL-based DSL leveraging the analytics engine.

5. **Feature Serving** — REST API on port 9094 (`/api/v1/features/`) for online retrieval and historical queries. Feast-compatible gRPC planned for Phase 2.

6. **SDK Integration** — Python SDK as primary ML ecosystem target, with feature logging back to topics for audit trails.

### Implementation Phases

- **Phase 1 (v0.6)**: Feature DSL, windowed aggregations, online store, REST API
- **Phase 2 (v0.7)**: Offline store with PIT joins, materialization, gRPC serving
- **Phase 3 (v0.8)**: Python SDK client, MLflow/Kubeflow connectors

## Consequences

### Positive

- Zero-infrastructure ML features — no separate Feast/Redis deployment
- Stream-native feature computation with no ETL pipeline
- Low-latency online serving (<10ms p99)
- Unified streaming + analytics + feature store in one binary
- Point-in-time correctness prevents training/serving skew

### Negative

- Online store competes with broker memory for hot features
- Adds ~2-3MB to binary size when enabled
- Single-node feature computation (no cluster distribution yet)
- Risk of scope creep toward full ML platform

### Neutral

- Feature flag gating (`featurestore`) keeps the binary lean when the feature is not needed
- Phased delivery limits risk per release cycle
- Builds on existing DuckDB analytics and HTTP API infrastructure
