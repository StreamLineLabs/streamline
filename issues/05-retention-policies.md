# Implement Retention Policies

## Summary

Implement data retention policies to automatically delete old data based on time or size limits. Currently, `TopicConfig` has retention fields but they are not enforced.

## Current State

- `src/storage/topic.rs:36-47` - `TopicConfig` has retention fields:
  ```rust
  pub retention_ms: i64,     // -1 for infinite (default)
  pub retention_bytes: i64,  // -1 for infinite (default)
  pub segment_bytes: u64,    // 100 MB default
  pub cleanup_policy: String, // "delete" default
  ```
- These fields are stored in `metadata.json` but never enforced
- Segments never deleted, data grows indefinitely
- No background cleanup task exists

## Requirements

### Retention Types

1. **Time-based retention** (`retention_ms`)
   - Delete segments older than N milliseconds
   - Based on `max_timestamp` in segment header

2. **Size-based retention** (`retention_bytes`)
   - Delete oldest segments when topic exceeds size
   - Per-partition enforcement

3. **Segment rolling** (`segment_bytes`)
   - Seal current segment when it exceeds size
   - Create new segment for subsequent writes

### Configuration

Update topic creation to accept retention config:
```bash
streamline-cli topics create my-topic \
  --partitions 3 \
  --retention-ms 604800000 \     # 7 days
  --retention-bytes 10737418240  # 10 GB
  --segment-bytes 104857600      # 100 MB
```

Default behavior: infinite retention (matches current).

### Implementation Tasks

1. **Segment Rolling** (`src/storage/partition.rs`):
   - Check segment size before append
   - Seal segment and create new one when limit exceeded
   - Update `Partition::append()` method

2. **Retention Enforcer** (`src/storage/retention.rs`):
   ```rust
   pub struct RetentionEnforcer {
       interval: Duration,
   }

   impl RetentionEnforcer {
       /// Run retention check for all topics
       pub async fn enforce(&self, topic_manager: &TopicManager) -> Result<()>;

       /// Delete segments older than retention_ms
       fn enforce_time_retention(&self, partition: &mut Partition, retention_ms: i64);

       /// Delete oldest segments to meet retention_bytes
       fn enforce_size_retention(&self, partition: &mut Partition, retention_bytes: i64);
   }
   ```

3. **Background Task** (`src/server/mod.rs`):
   - Run retention enforcer periodically (default: every 5 minutes)
   - Add to `Server::run()` event loop

4. **Segment Deletion** (`src/storage/segment.rs`):
   - Add `Segment::delete()` method
   - Safely remove segment file
   - Update partition offset tracking

5. **CLI Commands**:
   - `streamline-cli topics alter my-topic --retention-ms 86400000`
   - `streamline-cli topics describe my-topic` (show retention config)

6. **Kafka Protocol**:
   - Support retention configs in `CreateTopics` request
   - Add `AlterConfigs` (API Key 33) for runtime changes (optional)

### Edge Cases

- Never delete the active (unsealed) segment
- Ensure at least one segment per partition remains
- Handle race between retention deletion and active reads
- Log deleted segments for audit trail

### Acceptance Criteria

- [ ] Segments seal when exceeding `segment_bytes`
- [ ] Old segments deleted based on `retention_ms`
- [ ] Large topics trimmed based on `retention_bytes`
- [ ] Retention runs in background without blocking writes
- [ ] `earliest_offset()` updates after segment deletion
- [ ] Fetch requests for deleted offsets return appropriate error
- [ ] Topic describe shows retention configuration

### Testing

```rust
#[tokio::test]
async fn test_time_retention() {
    // Create topic with 1 second retention
    // Write records
    // Wait 2 seconds
    // Run retention
    // Verify old segments deleted
}

#[tokio::test]
async fn test_size_retention() {
    // Create topic with 1KB retention
    // Write 5KB of data
    // Run retention
    // Verify total size <= 1KB
}
```

### Related Files

- `src/storage/topic.rs:36-47` - TopicConfig (already has fields)
- `src/storage/partition.rs` - Add segment rolling
- `src/storage/segment.rs` - Add delete method
- `src/server/mod.rs:65-90` - Add background task
- `src/cli.rs` - Add topic alter command

## Labels

`enhancement`, `storage`, `production-readiness`
