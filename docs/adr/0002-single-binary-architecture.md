# ADR-0002: Single Binary Architecture

## Status

Accepted

## Context

Traditional streaming platforms like Apache Kafka require multiple components to operate: ZooKeeper (or KRaft controllers), brokers, and often additional tools for management. This multi-component architecture creates significant operational complexity:

- Complex deployment and configuration
- Multiple processes to monitor and manage
- Version compatibility matrices between components
- High resource requirements (Kafka + ZooKeeper typically needs 4+ GB RAM minimum)

Streamline targets developers who need streaming for local development, testing, CI/CD pipelines, and small-to-medium production deployments. These users value simplicity and low resource footprint over enterprise-scale features.

## Decision

Streamline is distributed as a single, statically-linked binary with zero external dependencies. The binary includes:

- The streaming server (Kafka protocol + HTTP API)
- The CLI tool (`streamline-cli`)
- All optional features gated behind compile-time feature flags

The lite edition targets <10MB and the full edition <20MB. The server can start with zero configuration (`./streamline`) using sensible defaults.

Feature flags (`--features full`, `--features auth`, etc.) control which capabilities are compiled in, allowing users to choose between minimal footprint and full functionality.

## Consequences

### Positive

- **Zero-config startup**: `./streamline` works immediately with sensible defaults
- **Simple deployment**: One file to copy, one process to manage
- **Low resource footprint**: Lite edition runs comfortably in 64MB RAM
- **Fast CI/CD**: Download one binary, start server, run tests — no Docker or multi-service orchestration required
- **Feature flags**: Users include only what they need, keeping the binary small

### Negative

- **Vertical scaling only** (single node): Clustering requires the `clustering` feature flag and additional configuration
- **Compile-time feature selection**: Changing features requires a different binary, not a runtime toggle
- **Monolithic codebase**: All server functionality lives in one Rust crate (mitigated by workspace members for WASM and analytics)

### Neutral

- The embedded SDK (`EmbeddedStreamline`) enables library usage without a separate server process — this supports the single-binary philosophy at the integration level
