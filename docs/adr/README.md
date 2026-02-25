# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for the Streamline project.

ADRs document significant architectural decisions along with their context and consequences. They provide a historical record of why certain technical choices were made.

## Format

Each ADR follows this template:

- **Title**: Short descriptive name
- **Status**: Proposed, Accepted, Deprecated, Superseded
- **Context**: The situation and forces at play
- **Decision**: What we decided to do
- **Consequences**: The resulting impact, both positive and negative

## Index

| ADR | Title | Status |
|-----|-------|--------|
| [ADR-0001](0001-kafka-protocol-compatibility.md) | Kafka Protocol Compatibility | Accepted |
| [ADR-0002](0002-single-binary-architecture.md) | Single Binary Architecture | Accepted |
| [ADR-0003](0003-kafka-connect-compatibility.md) | Kafka Connect Compatibility | Accepted |
| [ADR-0004](0004-schema-registry-enhancements.md) | Schema Registry Enhancements | Accepted |
| [ADR-0005](0005-managed-cloud-service.md) | Managed Cloud Service | Accepted |
| [ADR-0006](0006-multi-region-replication.md) | Multi-Region Replication | Accepted |
| [ADR-0007](0007-feature-store.md) | Feature Store | Accepted |
| [ADR-0008](0008-custom-storage-format.md) | Custom STRM Segment Storage Format | Accepted |
| [ADR-0009](0009-feature-flag-architecture.md) | Feature Flag Architecture | Accepted |
| [ADR-0010](0010-embedded-library-mode.md) | Embedded Library Mode | Accepted |
| [ADR-0011](0011-openraft-cluster-consensus.md) | OpenRaft for Cluster Consensus | Accepted |
| [ADR-0012](0012-duckdb-embedded-analytics.md) | DuckDB for Embedded Stream Analytics | Accepted |
| [ADR-0013](0013-wasmtime-transform-runtime.md) | Wasmtime for Stream Transform Runtime | Accepted |

## Creating a New ADR

1. Copy `template.md` to a new file: `NNNN-short-title.md`
2. Fill in all sections
3. Submit a PR for review
4. Update this index
