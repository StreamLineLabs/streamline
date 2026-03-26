# ADR-0015: Semantic Index Storage Layout

## Status

Proposed (M2 P0 deliverable)

## Context

Semantic Topics (M2) attach a vector index to each partition. The on-disk
layout decision affects:

- Recovery time after a crash.
- Compaction interaction with the existing log segment lifecycle in
  `streamline/src/storage/`.
- Whether re-embedding (model upgrade) is a per-segment or per-partition job.
- Memory footprint at steady state (mmap vs heap).

Two layouts were considered:

1. **Per-partition monolithic HNSW** — one `index.bin` per partition. Simpler
   read path; harder to compact and to re-embed incrementally.
2. **Per-segment HNSW with a partition-level merge view** — each closed
   segment carries its own index file; queries fan out and merge results.
   Mirrors the existing log-segment model.

## Decision

- **Per-segment HNSW indices**, written when a segment rolls over.
  - File suffix: `<base-offset>.idx` co-located with `<base-offset>.log`.
  - Format header: 16-byte magic `STREAMLN-IDX0`, version u16, dim u16, M
    u16, ef u16, vector count u64, then HNSW graph.
- **Active-segment index** stays in heap memory; flushes on roll.
- **Reader plane** queries N most-recent segments; older segments are
  searched on-demand via mmap and `madvise(WILLNEED)`.
- **Re-embedding** is a per-segment background job — bounded work units that
  do not block the writer.

## Consequences

### Positive
- Crash recovery only needs to rebuild the active-segment heap index.
- Tier-down (cold-tier) replicates a fixed file set per segment.
- Re-embedding can be throttled and resumed per segment.

### Negative
- Multi-segment fan-out adds query latency; mitigated by parallel scan and a
  small in-memory bloom of segment IDs to skip empties.
- Slightly larger total disk footprint vs a single per-partition graph.

### Neutral
- Aligns with the existing storage refactor described in
  `streamline/docs/STORAGE_REFACTORING.md`.

## References

- HNSW paper: <https://arxiv.org/abs/1603.09320>
- Existing `streamline/src/ai/hnsw.rs`
- `streamline/docs/STORAGE_REFACTORING.md`
