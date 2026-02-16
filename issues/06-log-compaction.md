# Implement Log Compaction

## Summary

Implement log compaction to retain only the latest value for each key within a topic. This enables using Streamline as a changelog/table store where old values for the same key can be discarded.

## Current State

- `src/storage/topic.rs:46` - `cleanup_policy: String` field exists with value "delete"
- No compaction logic implemented
- All records retained regardless of key

## Requirements

### Compaction Behavior

When `cleanup_policy = "compact"`:
- Retain only the most recent record for each unique key
- Tombstones (null value) mark key for deletion
- Tombstone retained for `delete.retention.ms` before removal
- Keys without values cannot be compacted (skip records with null key)

### Configuration

```rust
pub struct CompactionConfig {
    /// Cleanup policy: "delete", "compact", or "compact,delete"
    pub cleanup_policy: String,
    /// Min time before a segment is eligible for compaction
    pub min_cleanable_dirty_ratio: f64,  // default 0.5
    /// How long to retain tombstones
    pub delete_retention_ms: i64,  // default 24 hours
    /// Min time before compacting a segment
    pub min_compaction_lag_ms: i64,  // default 0
}
```

### Implementation Tasks

1. **Compaction Cleaner** (`src/storage/compaction.rs`):
   ```rust
   pub struct LogCompactor {
       /// Run compaction on a partition
       pub fn compact(&self, partition: &mut Partition) -> Result<CompactionStats>;

       /// Build key -> latest offset map
       fn build_offset_map(&self, segments: &[Segment]) -> HashMap<Bytes, i64>;

       /// Rewrite segment keeping only latest keys
       fn compact_segment(&self, segment: &Segment, offset_map: &HashMap<Bytes, i64>) -> Result<Segment>;
   }

   pub struct CompactionStats {
       pub segments_compacted: u32,
       pub records_before: u64,
       pub records_after: u64,
       pub bytes_reclaimed: u64,
   }
   ```

2. **Key-based Filtering**:
   - Build index of key -> most recent offset
   - Scan from newest to oldest segment
   - Rewrite segments excluding superseded records

3. **Tombstone Handling**:
   - Records with null value are tombstones
   - Track tombstone timestamps
   - Remove tombstones after `delete_retention_ms`

4. **Segment Rewriting**:
   - Create new compacted segment
   - Atomic swap with old segment
   - Preserve offset numbers (gaps allowed)

5. **Background Task**:
   - Run compaction periodically (separate from retention)
   - Only compact segments meeting dirty ratio threshold
   - Lower priority than normal writes

6. **CLI Commands**:
   - `streamline-cli topics create my-table --cleanup-policy compact`
   - `streamline-cli topics compact my-table` (manual trigger)

### Dirty Ratio Calculation

```
dirty_ratio = (total_records - unique_keys) / total_records
```

Only compact when `dirty_ratio >= min_cleanable_dirty_ratio`.

### Acceptance Criteria

- [ ] Topics with `compact` policy retain only latest key values
- [ ] Tombstones respected for key deletion
- [ ] Compaction doesn't affect active segment
- [ ] Offsets preserved (consumers don't miss data)
- [ ] Compaction stats logged
- [ ] Manual compaction trigger via CLI
- [ ] Records without keys skipped (not compacted)

### Example Use Case

```python
# Kafka Streams-style changelog
producer.send('user-profiles', key='user123', value='{"name": "Alice"}')
producer.send('user-profiles', key='user123', value='{"name": "Alice Smith"}')
producer.send('user-profiles', key='user456', value='{"name": "Bob"}')

# After compaction, only 2 records remain:
# user123 -> {"name": "Alice Smith"}
# user456 -> {"name": "Bob"}
```

### Related Files

- `src/storage/topic.rs:46` - cleanup_policy field
- `src/storage/segment.rs` - Segment rewriting
- `src/storage/record.rs` - Key handling
- `src/server/mod.rs` - Background task

## Labels

`enhancement`, `storage`, `production-readiness`, `large`
