# ADR-0020: Branched Stream Storage (Copy-on-Write Segment Refs)

## Status

Proposed (M5 P0 deliverable)

## Context

M5 (Branched Streams / Time-Travel Replay) lets users `branch` a topic at
an offset, write into the branch, and consume the branch as if it were a
real topic. Naïve full copies are unworkable at TB scale. We need
copy-on-write at the segment level.

## Decision

A branch is a **manifest of segment references** pointing into either:

1. The parent topic's existing segments up to the branch base offset, OR
2. New segments owned by the branch (any data appended on the branch).

Manifest schema (stored in `__branches` system topic, signed via M4):

```rust
struct BranchManifest {
    branch_id: Ulid,
    parent_topic: String,
    parent_partition: u32,
    base_offset: u64,           // exclusive upper bound of inherited data
    created_at: i64,
    created_by: String,
    parent_branch_id: Option<Ulid>,   // for branches-of-branches
    segment_refs: Vec<SegmentRef>,    // owned segments, base-offset ordered
}

enum SegmentRef {
    Inherited { topic: String, partition: u32, base_offset: u64 },
    Owned     { branch_id: Ulid,  base_offset: u64 },
}
```

**Reader plane (`streamline/src/branches/reader.rs`):** for a fetch at
offset X on `topic@branch=B`:

- If `X < base_offset(B)` → read from parent's segment at X.
- Else → read from B's owned segments.
- Merging is just sequential dispatch; no per-record decision.

**Garbage collection:** parent segments referenced by any branch must not be
deleted by retention. A reference-count file per parent segment tracks
branch holders.

**Branch deletion** drops the manifest and any `Owned` segments; parent
refs are decremented.

## Consequences

### Positive
- O(1) branch creation; no data copy.
- Reader path is read-only on parent — branches cannot corrupt the source.
- Plays well with cold-tier (S3) — `Inherited` refs work transparently.

### Negative
- Retention on parent topic now depends on branch lifecycle; long-lived
  branches pin storage. Quotas + warning ops dashboards in M5 P3.
- Refcount file is a new failure mode; rebuild-from-manifest tool required
  for disaster recovery.

### Neutral
- Iceberg integration (M5 P3) maps Streamline branches → Iceberg branch
  refs naturally because both are manifest-based.

## References

- Iceberg branching: <https://iceberg.apache.org/docs/latest/branching/>
- Git's COW pack model — conceptual prior art.
- Existing storage layer: `streamline/src/storage/`
- Internal: `MOONSHOT_PLAN.md` Feature M5
