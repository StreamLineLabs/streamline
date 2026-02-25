# ADR-0011: OpenRaft for Cluster Consensus

## Status

Accepted

## Context

Streamline needs a consensus protocol for multi-node clustering to coordinate metadata (topic assignments, partition leadership, consumer group state) across nodes. The primary candidates considered were:

1. **OpenRaft** — A Rust-native Raft implementation with async/await support
2. **Custom Raft** — Building a Raft implementation from scratch
3. **etcd embedding** — Using etcd as an embedded consensus store
4. **Paxos variants** — Multi-Paxos or EPaxos

Key requirements:
- Must integrate with Tokio async runtime (Streamline is fully async)
- Must support dynamic membership changes for node scaling
- Must be embeddable (no external process dependencies — "single binary" constraint from ADR-0002)
- Must support snapshotting for state recovery

## Decision

We chose **OpenRaft** (`openraft` crate) for cluster consensus with the following rationale:

- **Rust-native and async**: Integrates directly with our Tokio runtime without FFI or process boundaries
- **Well-maintained**: Active community, battle-tested in TiKV and other production systems
- **Embeddable**: Runs as a library within the Streamline binary, preserving ADR-0002's single-binary constraint
- **Feature-complete**: Supports membership changes, snapshotting, log compaction, and leader lease
- **Separation of concerns**: Raft handles metadata consensus only; data replication uses a separate replication protocol optimized for high-throughput streaming

## Consequences

### Positive

- Zero external dependencies for clustering (consistent with single-binary philosophy)
- Native async integration eliminates FFI overhead and complexity
- Raft's strong consistency guarantees simplify reasoning about metadata correctness
- Dynamic membership changes enable seamless node addition/removal

### Negative

- Raft requires a leader for writes, introducing a single coordination point for metadata operations
- OpenRaft's API surface requires careful integration to avoid blocking the Raft state machine
- Debugging Raft edge cases (split-brain, network partitions) requires specialized testing (addressed via Jepsen tests)

### Neutral

- Data replication is handled separately from Raft consensus, requiring two distinct replication paths
- Raft's leader-based model means metadata write latency depends on quorum acknowledgment
