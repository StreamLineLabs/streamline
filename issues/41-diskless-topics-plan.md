# Diskless Topics Implementation Plan

## Overview

This plan extends the existing issue spec (`issues/41-diskless-topics.md`) to incorporate a **strategy-based, opt-in architecture** where users can choose storage modes per-topic with automatic component selection based on their configuration.

## Design Principles

1. **Opt-in by default**: Local storage remains the default. Diskless/Hybrid modes are explicit choices.
2. **Strategy pattern**: Abstract storage backends allow swapping implementations without changing core logic.
3. **Component auto-selection**: Based on storage mode, the system selects appropriate:
   - Buffer strategy (in-memory ring buffer vs. write-through)
   - WAL backend (local vs. S3)
   - Segment backend (local vs. S3)
   - Cache strategy (aggressive for diskless, standard for local)
4. **Backward compatibility**: Existing topics continue working unchanged.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        StorageMode Selection                                 │
│                                                                              │
│   TopicConfig.storage_mode: Local | Hybrid | Diskless                       │
│                                                                              │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    │              │              │
                    ▼              ▼              ▼
          ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
          │    Local    │  │   Hybrid    │  │  Diskless   │
          │   Storage   │  │   Storage   │  │   Storage   │
          └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
                 │                │                │
    ┌────────────┴────┐    ┌──────┴──────┐   ┌────┴────────┐
    │                 │    │             │   │             │
    ▼                 ▼    ▼             ▼   ▼             ▼
┌────────┐     ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────────┐
│Local   │     │ Local   │ │ S3 WAL  │ │ S3 Seg  │ │  S3 Direct  │
│WAL     │     │ Segment │ │ Writer  │ │ Backend │ │  + Buffer   │
└────────┘     └─────────┘ └─────────┘ └─────────┘ └─────────────┘
```

## Storage Backend Trait Hierarchy

```rust
// Core traits for pluggable backends

/// Abstraction over WAL writers
pub trait WalBackend: Send + Sync {
    async fn append(&mut self, entry: WalEntry) -> Result<u64>;
    async fn flush(&mut self) -> Result<()>;
    async fn read_from(&self, sequence: u64) -> Result<Vec<WalEntry>>;
    fn storage_mode(&self) -> StorageMode;
}

/// Abstraction over segment storage
pub trait SegmentBackend: Send + Sync {
    async fn append_batch(&mut self, batch: &RecordBatch) -> Result<i64>;
    async fn read_from_offset(&self, offset: i64, max_bytes: usize) -> Result<Vec<Record>>;
    async fn seal(&mut self) -> Result<()>;
    fn base_offset(&self) -> i64;
    fn log_end_offset(&self) -> i64;
    fn storage_mode(&self) -> StorageMode;
}

/// Buffer strategy for managing in-flight records
pub trait BufferStrategy: Send + Sync {
    fn push(&mut self, records: Vec<Record>) -> Result<()>;
    fn flush_threshold_reached(&self) -> bool;
    fn take_batch(&mut self) -> Vec<Record>;
    fn memory_usage(&self) -> usize;
    fn config(&self) -> &BufferConfig;
}
```

## Implementation Phases

### Phase 1: Storage Mode Infrastructure (Foundation)

**Files to modify:**
- `src/storage/mod.rs` - Export new types
- `src/storage/topic.rs` - Add StorageMode to TopicConfig

**New files:**
- `src/storage/backend.rs` - Core traits (WalBackend, SegmentBackend, BufferStrategy)
- `src/storage/storage_mode.rs` - StorageMode enum and factory functions

**Tasks:**
1. Define `StorageMode` enum with `Local`, `Hybrid`, `Diskless` variants
2. Define core backend traits
3. Add `storage_mode` and `diskless_config` fields to `TopicConfig`
4. Create `StorageBackendFactory` that selects components based on mode
5. Update topic metadata serialization

**Example configuration:**
```rust
pub struct TopicConfig {
    // Existing fields...

    /// Storage mode for this topic (default: Local)
    #[serde(default)]
    pub storage_mode: StorageMode,

    /// Configuration for diskless/hybrid modes
    #[serde(default)]
    pub remote_storage: Option<RemoteStorageConfig>,
}

pub struct RemoteStorageConfig {
    /// Batch size before flushing to object storage (bytes)
    pub batch_size_bytes: usize,  // default: 1MB
    /// Maximum time to wait before flushing (ms)
    pub batch_timeout_ms: u64,    // default: 100ms
    /// Number of parallel uploads
    pub upload_parallelism: usize, // default: 4
    /// Local read cache size (bytes, 0 to disable)
    pub read_cache_bytes: u64,    // default: 100MB
}
```

### Phase 2: Buffer Strategies

**Files to modify:**
- `src/storage/partition.rs` - Integrate buffer strategies

**New files:**
- `src/storage/buffer/mod.rs` - Buffer module
- `src/storage/buffer/local.rs` - Standard write-through buffer
- `src/storage/buffer/batching.rs` - Batching buffer for diskless mode
- `src/storage/buffer/memory_ring.rs` - Ring buffer for hot data

**Tasks:**
1. Create `LocalBuffer` - write-through to local WAL (current behavior)
2. Create `BatchingBuffer` - accumulates records until size/time threshold
3. Create `MemoryRingBuffer` - fixed-size ring for recent records
4. Implement `BufferStrategy` trait for each
5. Add buffer selection logic based on storage mode

**Buffer selection logic:**
```rust
impl StorageBackendFactory {
    pub fn create_buffer(&self, mode: StorageMode, config: &RemoteStorageConfig) -> Box<dyn BufferStrategy> {
        match mode {
            StorageMode::Local => Box::new(LocalBuffer::new()),
            StorageMode::Hybrid => Box::new(BatchingBuffer::new(config)),
            StorageMode::Diskless => Box::new(BatchingBuffer::with_ring(config)),
        }
    }
}
```

### Phase 3: S3 WAL Backend (Hybrid Mode)

**Files to modify:**
- `src/storage/tiering.rs` - Add WAL path formatting
- `src/storage/wal.rs` - Extract trait, refactor to `LocalWalWriter`

**New files:**
- `src/storage/wal_s3.rs` - S3-backed WAL implementation

**Tasks:**
1. Extract `WalBackend` trait from existing `WalWriter`
2. Rename current implementation to `LocalWalWriter`
3. Implement `S3WalWriter` using `TieringManager`
4. Add batching and timeout logic for S3 writes
5. Implement recovery from S3 WAL on startup

**S3 WAL structure:**
```rust
pub struct S3WalWriter {
    tiering_manager: Arc<TieringManager>,
    topic: String,
    partition: i32,
    batch_buffer: BytesMut,
    batch_entries: Vec<WalEntry>,
    batch_config: RemoteStorageConfig,
    last_flush: Instant,
    sequence: u64,
}
```

### Phase 4: S3 Segment Backend (Diskless Mode)

**Files to modify:**
- `src/storage/segment.rs` - Extract trait, refactor to `LocalSegment`
- `src/storage/partition.rs` - Use segment backends via trait

**New files:**
- `src/storage/segment_s3.rs` - S3 segment implementation
- `src/storage/manifest.rs` - Partition manifest for S3 layout

**Tasks:**
1. Extract `SegmentBackend` trait from existing `Segment`
2. Implement `S3Segment` for direct S3 writes
3. Create `PartitionManifest` for S3 partition metadata
4. Implement offset tracking without local storage
5. Add parallel S3 uploads

### Phase 5: Partition Integration

**Files to modify:**
- `src/storage/partition.rs` - Major refactor for pluggable backends

**Tasks:**
1. Refactor `Partition` to use `Box<dyn SegmentBackend>`
2. Refactor `Partition` to use `Box<dyn WalBackend>`
3. Refactor `Partition` to use `Box<dyn BufferStrategy>`
4. Add factory method for creating partitions with correct backends
5. Update existing `open()` and `open_in_memory()` to use factory

**Partition factory:**
```rust
impl Partition {
    pub fn open_with_mode(
        topic: &str,
        partition_id: i32,
        path: &Path,
        mode: StorageMode,
        config: &RemoteStorageConfig,
        tiering: Option<Arc<TieringManager>>,
    ) -> Result<Self> {
        let factory = StorageBackendFactory::new(mode, config, tiering);

        let wal = factory.create_wal(topic, partition_id, path)?;
        let segment = factory.create_segment(topic, partition_id, path)?;
        let buffer = factory.create_buffer();
        let cache = factory.create_cache();

        // ... construct Partition with backends
    }
}
```

### Phase 6: Read Path and Caching

**Files to modify:**
- `src/storage/cache.rs` - Add diskless-optimized cache config
- `src/storage/partition.rs` - Integrate cache with fetch path

**Tasks:**
1. Add `DisklessCache` config preset with larger TTL and prefetching
2. Implement cache-first read path for diskless partitions
3. Add prefetch logic for sequential reads
4. Implement cache warming on partition recovery
5. Add cache statistics by storage mode

### Phase 7: Replication Adjustments

**Files to modify:**
- `src/replication/mod.rs` - Skip replication for diskless topics

**Tasks:**
1. Add `requires_replication()` method based on storage mode
2. Skip follower sync for diskless partitions (data is in shared storage)
3. Adjust HWM advancement for diskless (based on S3 confirmation)
4. Update ISR logic for hybrid mode (WAL-only replication)

### Phase 8: Protocol and CLI

**Files to modify:**
- `src/protocol/kafka.rs` - Handle diskless in CreateTopics, Produce, Fetch
- `src/cli.rs` - Add storage mode flags

**Tasks:**
1. Add `--storage-mode` flag to `topics create`
2. Add `--remote-storage-*` config flags
3. Update `topics describe` to show storage mode
4. Add validation for diskless config requirements
5. Return appropriate errors for unsupported operations

**CLI examples:**
```bash
# Create diskless topic
streamline-cli topics create events \
  --storage-mode diskless \
  --remote-storage-batch-size 1048576 \
  --remote-storage-batch-timeout-ms 100

# Create hybrid topic
streamline-cli topics create logs \
  --storage-mode hybrid

# View topic info
streamline-cli topics describe events
# Output includes: Storage Mode: Diskless
```

### Phase 9: Recovery and Consistency

**Tasks:**
1. Implement S3 manifest loading on broker startup
2. Rebuild offset state from S3 for diskless partitions
3. Handle S3 eventual consistency with retries
4. Implement idempotent writes for crash recovery
5. Add leader handoff logic for hybrid mode

### Phase 10: Metrics and Observability

**Tasks:**
1. Add storage mode label to existing metrics
2. Add S3 operation metrics (upload latency, download latency, errors)
3. Add buffer metrics (size, flush rate, overflow)
4. Add cache hit/miss rates by storage mode
5. Update HTTP metrics endpoint

## Configuration Summary

### Server-level defaults (optional)
```bash
# Enable tiered storage globally (required for diskless)
streamline --tiering-enabled --tiering-bucket my-bucket --tiering-region us-east-1
```

### Per-topic configuration
```bash
# Local (default) - no change needed
streamline-cli topics create normal-topic

# Hybrid - local WAL + S3 segments
streamline-cli topics create hybrid-topic --storage-mode hybrid

# Diskless - all data to S3
streamline-cli topics create diskless-topic --storage-mode diskless
```

## Testing Strategy

### Unit Tests
- Backend trait implementations
- Buffer strategy behavior
- Storage mode factory selection
- Manifest serialization

### Integration Tests (with LocalStack)
- End-to-end produce/consume for each mode
- Mode switching validation
- Recovery scenarios
- Mixed-mode cluster

### Performance Tests
- Latency comparison: Local vs Hybrid vs Diskless
- Throughput at various batch sizes
- Cache effectiveness

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| S3 latency spikes | Higher p99 latency | Aggressive caching, batching |
| Memory exhaustion | OOM on buffer overflow | Configurable limits, backpressure |
| S3 consistency delays | Stale reads | Retry logic, manifest versioning |
| Complexity increase | Maintenance burden | Clean trait boundaries, good tests |

## Success Criteria

- [x] Topics can be created with any storage mode
- [x] Local mode behavior unchanged (backward compatible)
- [x] Hybrid mode reduces replication traffic by 80%+
- [x] Diskless mode has zero local disk usage (except cache)
- [x] P99 produce latency <500ms for diskless
- [x] All existing tests pass
- [x] Standard Kafka clients work with all modes
