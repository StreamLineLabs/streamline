# ADR-0006: Multi-Region Geo-Replication

## Status

Proposed

## Context

Global deployments require data locality for low-latency access and disaster recovery across regions. Streamline has existing infrastructure for this: the `geo-replication` feature flag (gated behind `clustering`), `GeoReplicationManager` and `GeoReplicationConfig` structs in `crdt/replication.rs`, CLI subcommands for peer management, and a CRDT module with `LWWRegister`, `ORSet`, `PNCounter`, and other convergent data types.

Kafka's MirrorMaker 2 addresses cross-cluster replication but requires external Kafka Connect workers, contradicting our single-binary architecture (ADR-0002). A native solution is needed.

## Decision

Implement native multi-region geo-replication within the Streamline binary supporting active-passive, active-active, and hub-and-spoke topologies with pluggable conflict resolution (LWW, vector clocks, CRDTs).

Key design points:

- **Within-region consistency**: Strong, via ISR-based replication
- **Cross-region consistency**: Eventual with bounded staleness (`max_replication_lag_ms`)
- **Conflict resolution**: Per-topic strategy selection—LWW (hybrid logical clocks), vector clocks (with dead-letter conflicts topic), or CRDT merge semantics
- **Wire protocol**: Pull-based binary RPC over mTLS, multiplexed per-peer connection, resumable via per-partition replication cursors
- **Bandwidth management**: Zstd compression, configurable batching, per-peer throttling
- **Loop prevention**: Origin region headers prevent re-replication to source

Implementation phases: active-passive in v0.5, active-active with LWW in v0.6, CRDT-based resolution in v0.7.

See the full design in `streamline-docs/docs/architecture/decisions/adr-023-multi-region.md`.

## Consequences

### Positive

- Global low-latency reads without external dependencies
- Automatic disaster recovery with failover
- Transparent to Kafka-compatible clients
- Incremental adoption path from active-passive to active-active

### Negative

- Applications must tolerate eventual consistency across regions
- LWW conflict resolution can silently discard concurrent writes
- Cross-region bandwidth costs for high-throughput topics
- Expanded testing matrix across topologies and failure modes

### Neutral

- MirrorMaker 2 compatibility is not a goal
- Gated behind `geo-replication` feature flag; lite edition unaffected
