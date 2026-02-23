# Storage Module Refactoring Guide

## Problem

`src/storage/topic.rs` is 3,223 lines — the largest non-UI file in the codebase.
Combined with `segment.rs` (2,174) and `partition.rs` (1,663), the storage core
is 7,060 lines across 3 files. This makes the code hard to navigate, review, and test.

## Proposed Decomposition

### topic.rs → topic/ directory

```
src/storage/topic.rs (3,223 lines)
  ↓ split into:
src/storage/topic/
  ├── mod.rs          (~200 lines)  — TopicManager struct, public API, re-exports
  ├── metadata.rs     (~400 lines)  — TopicMetadata, TopicConfig, persistence
  ├── write.rs        (~600 lines)  — Produce path: append, batch, WAL integration
  ├── read.rs         (~500 lines)  — Consume path: read, seek, scan
  ├── compaction.rs   (~400 lines)  — Log compaction, segment merging
  ├── retention.rs    (~300 lines)  — TTL, size-based retention, cleanup
  ├── replication.rs  (~400 lines)  — ISR tracking, follower sync (if clustering)
  └── tests.rs        (~400 lines)  — Unit tests
```

### Migration Strategy

1. **Create `topic/` directory** with `mod.rs` that re-exports everything
2. **Move one concern at a time** — start with `metadata.rs` (lowest risk)
3. **Keep all tests passing** at each step
4. **Update imports** — `use crate::storage::topic::*` should work unchanged

### Implementation Steps

```bash
# Step 1: Create directory, move file
mkdir -p src/storage/topic
mv src/storage/topic.rs src/storage/topic/mod.rs

# Step 2: Extract metadata (lowest risk)
# Move TopicMetadata, TopicConfig, TopicStatus types to metadata.rs
# Add: mod metadata; pub use metadata::*; in mod.rs

# Step 3: Extract retention logic
# Move retention_check(), cleanup_expired(), delete_records_before()
# Add: mod retention; pub use retention::*;

# Step 4: Extract compaction
# Move compact(), merge_segments(), compaction_eligible()
# Add: mod compaction; pub use compaction::*;

# Step 5: Extract read path
# Move read(), read_with_ttl(), consume()
# Add: mod read; pub use read::*;

# Step 6: Extract write path
# Move append(), append_with_headers(), produce_batch()
# Add: mod write; pub use write::*;
```

### Similar Refactoring for Other Large Files

| File | Lines | Suggested Split |
|------|-------|----------------|
| `storage/segment.rs` | 2,174 | `segment/write.rs`, `segment/read.rs`, `segment/index.rs` |
| `storage/partition.rs` | 1,663 | `partition/state.rs`, `partition/io.rs`, `partition/rebalance.rs` |
| `transaction/coordinator.rs` | 2,200 | `transaction/state.rs`, `transaction/log.rs`, `transaction/cleanup.rs` |
| `ui/templates.rs` | 9,790 | Externalize to `.html` template files loaded at build time |

### Acceptance Criteria

- [ ] No file in `src/storage/` exceeds 800 lines
- [ ] All existing tests pass without modification
- [ ] `cargo doc` generates correct documentation
- [ ] No new public API surface (all re-exports via mod.rs)
