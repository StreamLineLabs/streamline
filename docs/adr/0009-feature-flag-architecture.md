# ADR-0009: Feature Flag Architecture

## Status
Accepted

## Context
Streamline supports use cases ranging from a <10MB dev tool to a full enterprise
streaming platform. We need a way to include/exclude capabilities without
maintaining separate codebases.

## Decision
Use Cargo feature flags to control compilation of optional modules. The default
build (`lite`) includes only core streaming + compression. The `full` build
includes all 30+ optional features.

## Feature Tiers

| Tier | Features | Binary Size |
|------|----------|-------------|
| `lite` (default) | Core streaming, compression | ~8 MB |
| `full` | Everything below | ~20 MB |
| Enterprise | auth, clustering, encryption, telemetry, cloud-storage, schema-registry, metrics | +5 MB |
| Analytics | analytics, sqlite-queries, iceberg, delta-lake | +4 MB |
| Integration | kafka-connect, cdc, graphql, wasm-transforms | +3 MB |
| Experimental | edge, featurestore, ai, geo-replication, faas | +2 MB |

## Rationale
- **Single codebase** — No forking between "community" and "enterprise"
- **Compile-time elimination** — Unused features have zero runtime cost
- **Dependency isolation** — Optional dependencies (DuckDB, OpenRaft, etc.)
  only pulled in when needed
- **Testing matrix** — CI tests both `lite` and `full` builds

## Consequences
- **Positive**: 8MB binary for simple use cases, 20MB for full enterprise
- **Positive**: CI can test each feature flag independently
- **Positive**: Users pay only for what they use (compile time, binary size)
- **Negative**: 71 feature flags create a complex Cargo.toml
- **Negative**: Some feature combinations may not be tested
- **Mitigation**: `full` meta-feature ensures all combinations compile together
