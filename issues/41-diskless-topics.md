# Implement Diskless Topics

## Summary

Add support for diskless topics that write data directly to object storage (S3/Azure/GCS) instead of local disk. This eliminates cross-AZ replication costs (50-60% of typical Kafka costs) and enables truly stateless brokers for cost-sensitive workloads.

Inspired by [KIP-1150: Diskless Topics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150:+Diskless+Topics) and related proposals (KIP-1176, KIP-1183).

## Current State

- `src/storage/tiering.rs` - TieringManager exists for cold segment offloading to S3
- `src/storage/wal.rs` - WAL implementation writes to local disk only
- `src/storage/segment.rs` - Segments are always local files
- `src/storage/topic.rs` - No storage mode configuration
- `src/replication/mod.rs` - Replication assumes local storage with leader-follower model

The current tiered storage only moves **cold** (inactive) segments to object storage. Active segments and WAL remain on local disk, requiring cross-AZ replication for durability.

## Requirements

### Storage Modes

Topics can be configured with one of three storage modes:

```rust
pub enum StorageMode {
    /// Traditional local storage with optional tiering (default)
    /// - Active segments on local disk
    /// - Cold segments optionally tiered to object storage
    /// - Full replication for HA
    Local,

    /// Hybrid mode - local WAL with S3-backed segments
    /// - WAL written locally (for acks=1 low-latency path)
    /// - Segments written directly to S3
    /// - Reduced replication (WAL only)
    Hybrid,

    /// Full diskless - all writes go to object storage
    /// - No local disk usage (except optional cache)
    /// - Higher latency (~100-500ms produce)
    /// - Zero replication traffic
    /// - Lowest cost
    Diskless,
}
```

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DISKLESS TOPIC FLOW                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Producer                                                       │
│     │                                                           │
│     ▼                                                           │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Broker (Stateless)                          │   │
│  │  ┌─────────────┐    ┌─────────────┐    ┌────────────┐   │   │
│  │  │   Buffer    │───►│  S3 Writer  │───►│   S3/GCS   │   │   │
│  │  │  (in-mem)   │    │  (batched)  │    │   Bucket   │   │   │
│  │  └─────────────┘    └─────────────┘    └────────────┘   │   │
│  │                                              │            │   │
│  │                                              ▼            │   │
│  │                      ┌─────────────────────────────────┐ │   │
│  │                      │  Acknowledge (after S3 PUT)     │ │   │
│  │                      └─────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  Consumer                                                       │
│     │                                                           │
│     ▼                                                           │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              Broker (Stateless)                          │   │
│  │  ┌─────────────┐    ┌─────────────┐    ┌────────────┐   │   │
│  │  │ Local Cache │◄───│  S3 Reader  │◄───│   S3/GCS   │   │   │
│  │  │    (LRU)    │    │             │    │   Bucket   │   │   │
│  │  └──────┬──────┘    └─────────────┘    └────────────┘   │   │
│  │         │                                                │   │
│  │         ▼                                                │   │
│  │  ┌─────────────────────────────────────────────────────┐│   │
│  │  │  Return records to consumer                         ││   │
│  │  └─────────────────────────────────────────────────────┘│   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Object Storage Layout

```
s3://bucket/streamline/
├── v1/
│   └── {cluster-id}/
│       └── topics/
│           └── {topic-name}/
│               └── partition-{N}/
│                   ├── manifest.json           # Partition metadata
│                   ├── 00000000000000000000/   # Offset-based directory
│                   │   ├── data.segment        # Record data
│                   │   └── index.sparse        # Offset index
│                   ├── 00000000000000001000/
│                   │   ├── data.segment
│                   │   └── index.sparse
│                   └── _wal/                   # Optional WAL for hybrid mode
│                       ├── 00000001.wal
│                       └── 00000002.wal
```

### Configuration

```rust
// src/storage/topic.rs
pub struct TopicConfig {
    pub name: String,
    pub partitions: i32,
    pub replication_factor: i16,

    /// Storage mode for this topic
    #[serde(default)]
    pub storage_mode: StorageMode,

    /// Diskless-specific configuration
    #[serde(default)]
    pub diskless: Option<DisklessConfig>,

    // ... existing fields ...
}

pub struct DisklessConfig {
    /// Batch size before flushing to S3 (bytes)
    #[serde(default = "default_batch_size")]
    pub batch_size_bytes: usize,  // default: 1MB

    /// Maximum time to wait before flushing (milliseconds)
    #[serde(default = "default_batch_timeout")]
    pub batch_timeout_ms: u64,  // default: 100ms

    /// Use S3 Express One Zone for lower latency
    #[serde(default)]
    pub use_express_zone: bool,

    /// Number of parallel uploads
    #[serde(default = "default_upload_parallelism")]
    pub upload_parallelism: usize,  // default: 4

    /// Local read cache size (bytes, 0 to disable)
    #[serde(default = "default_read_cache")]
    pub read_cache_bytes: u64,  // default: 100MB
}
```

CLI:
```bash
# Create diskless topic
streamline-cli topics create analytics-events \
  --partitions 6 \
  --storage-mode diskless \
  --diskless-batch-size 1048576 \
  --diskless-batch-timeout-ms 100

# Create hybrid topic (local WAL + S3 segments)
streamline-cli topics create user-actions \
  --partitions 3 \
  --storage-mode hybrid
```

### Acks Behavior for Diskless Topics

| Acks Policy | Diskless Behavior | Latency |
|-------------|-------------------|---------|
| `acks=0` | Fire and forget, buffer locally | <1ms |
| `acks=1` | Acknowledge after S3 PUT initiated | ~50-100ms |
| `acks=all` | Acknowledge after S3 PUT confirmed | ~100-500ms |

Note: For diskless topics, `acks=1` and `acks=all` have similar semantics since there's no replication - both wait for object storage confirmation.

## Implementation Tasks

### Phase 1: S3 WAL Backend (Hybrid Mode Foundation)

1. Create `src/storage/wal_s3.rs`:
   ```rust
   pub struct S3WalWriter {
       tiering_manager: Arc<TieringManager>,
       batch_buffer: BytesMut,
       batch_entries: Vec<WalEntry>,
       batch_size_threshold: usize,
       batch_timeout: Duration,
       last_flush: Instant,
   }

   impl S3WalWriter {
       pub async fn append(&mut self, entry: WalEntry) -> Result<u64>;
       pub async fn flush(&mut self) -> Result<()>;
       async fn upload_batch(&self, batch: Bytes) -> Result<()>;
   }
   ```

2. Add S3 WAL path formatting in `src/storage/tiering.rs`:
   ```rust
   fn wal_object_path(topic: &str, partition: i32, sequence: u64) -> ObjectPath;
   ```

3. Update `src/storage/partition.rs` to support hybrid mode:
   - Route WAL writes based on storage mode
   - Handle S3 WAL recovery on startup

### Phase 2: Diskless Segment Backend

1. Create `src/storage/segment_s3.rs`:
   ```rust
   pub struct S3Segment {
       tiering_manager: Arc<TieringManager>,
       topic: String,
       partition: i32,
       base_offset: i64,
       buffer: BytesMut,
       config: DisklessConfig,
   }

   impl S3Segment {
       pub async fn append_batch(&mut self, batch: &RecordBatch) -> Result<()>;
       pub async fn read_from_offset(&self, offset: i64, max: usize) -> Result<Vec<Record>>;
       pub async fn seal(&mut self) -> Result<()>;
   }
   ```

2. Abstract segment backend in `src/storage/segment.rs`:
   ```rust
   pub enum SegmentBackend {
       Local(LocalSegment),
       S3(S3Segment),
   }
   ```

3. Add manifest management for S3 partitions:
   ```rust
   pub struct PartitionManifest {
       pub topic: String,
       pub partition: i32,
       pub segments: Vec<SegmentManifestEntry>,
       pub log_end_offset: i64,
       pub high_watermark: i64,
   }
   ```

### Phase 3: Full Diskless Mode

1. Update `src/storage/topic.rs`:
   - Add `StorageMode` to `TopicConfig`
   - Add `DisklessConfig` structure
   - Factory method to create appropriate partition backend

2. Update `src/storage/partition.rs`:
   - Support diskless partition creation
   - Route all writes to S3 backend
   - Implement in-memory offset tracking

3. Update `src/replication/mod.rs`:
   - Skip replication for diskless topics (data already in shared storage)
   - Adjust `become_leader`/`become_follower` for diskless mode
   - HWM advancement based on S3 confirmation

4. Update `src/protocol/kafka.rs`:
   - Handle diskless topics in Produce handler
   - Handle diskless topics in Fetch handler
   - Return appropriate error for unsupported operations

### Phase 4: Read Path Optimization

1. Implement local read cache in `src/storage/cache.rs`:
   - LRU cache for recently read segments
   - Prefetch next segments on sequential reads

2. Add parallel S3 reads:
   - Range requests for partial segment reads
   - Concurrent fetches for multi-partition consumers

3. Index optimization:
   - Sparse index stored alongside segments in S3
   - Local index cache

### Phase 5: Recovery and Consistency

1. Implement broker startup recovery:
   - Load partition manifests from S3
   - Rebuild in-memory offset state
   - Resume from last confirmed offset

2. Handle S3 consistency edge cases:
   - Retry on eventual consistency delays
   - Idempotent writes with deduplication

3. Implement leader handoff for hybrid mode:
   - Drain local WAL to S3 on leadership change
   - Follower recovery from S3 WAL

## Acceptance Criteria

### Functionality
- [x] Topics can be created with `storage_mode: diskless`
- [x] Produces to diskless topics write directly to S3
- [x] Consumes from diskless topics read from S3 (with caching)
- [x] No local disk usage for diskless topics (except cache)
- [x] Hybrid mode writes WAL to S3, segments to S3
- [x] Broker restart recovers diskless topic state from S3

### Performance
- [x] Diskless produce latency <500ms p99 (standard S3)
- [x] Diskless produce latency <100ms p99 (S3 Express)
- [x] Cached read latency <10ms p99
- [x] Cold read latency <500ms p99
- [x] Throughput: 10MB/s per partition minimum

### Operations
- [x] `streamline-cli topics describe` shows storage mode
- [x] Metrics exported for S3 operations (uploads, downloads, latency)
- [x] Clear error messages for diskless-specific failures
- [x] Documentation for diskless topic use cases and trade-offs

### Compatibility
- [x] Standard Kafka clients work with diskless topics
- [x] Consumer groups function correctly
- [x] Offset commit/fetch works as expected

## Trade-offs and Limitations

### When to Use Diskless Topics

**Good fit:**
- Analytics and batch processing workloads
- Log aggregation with relaxed latency requirements
- Cost-sensitive deployments
- Workloads with high storage but low throughput
- Multi-AZ deployments where replication cost dominates

**Poor fit:**
- Real-time streaming (<10ms latency requirement)
- High-frequency trading or gaming
- Interactive applications
- Workloads with unpredictable S3 latency sensitivity

### Known Limitations

1. **Latency**: 10-50x higher produce latency than local storage
2. **Throughput**: Limited by S3 request rate (3,500 PUT/s per prefix)
3. **Consistency**: Brief window where PUT is acked but GET may fail
4. **Cost at Scale**: S3 request costs may exceed savings at very high throughput

## Testing

### Unit Tests
- S3WalWriter batch accumulation and flush
- S3Segment read/write operations
- Manifest serialization/deserialization
- Storage mode routing logic

### Integration Tests
- End-to-end produce/consume with LocalStack S3
- Broker restart recovery
- Consumer group coordination with diskless topics
- Mixed cluster (local + diskless topics)

### Performance Tests
- Latency benchmarks vs local storage
- Throughput at various batch sizes
- Read cache hit ratio optimization

### Chaos Tests
- S3 unavailability handling
- Broker crash during S3 upload
- Network partition between broker and S3

## Related Files

### Existing (to modify)
- `src/storage/tiering.rs` - Extend for active segment writes
- `src/storage/topic.rs` - Add StorageMode, DisklessConfig
- `src/storage/partition.rs` - Route based on storage mode
- `src/storage/segment.rs` - Abstract backend
- `src/replication/mod.rs` - Skip replication for diskless
- `src/protocol/kafka.rs` - Handle diskless in Produce/Fetch
- `src/config/mod.rs` - Add diskless configuration options
- `src/cli.rs` - Add topic creation flags

### New files to create
- `src/storage/wal_s3.rs` - S3-backed WAL
- `src/storage/segment_s3.rs` - S3 segment backend
- `src/storage/manifest.rs` - Partition manifest management
- `src/storage/diskless.rs` - Diskless coordinator

## References

- [KIP-1150: Diskless Topics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150:+Diskless+Topics)
- [KIP-1176: Tiered Storage for Active Log Segment](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1176:+Tiered+Storage+for+Active+Log+Segment)
- [KIP-405: Kafka Tiered Storage](https://cwiki.apache.org/confluence/display/KAFKA/KIP-405:+Kafka+Tiered+Storage)
- [The Rise of Diskless Kafka - Kai Waehner](https://www.kai-waehner.de/blog/2025/08/11/the-rise-of-diskless-kafka-rethinking-brokers-storage-and-the-kafka-protocol/)
- [Diskless Kafka is the Tide - Aiven](https://aiven.io/blog/diskless-kafka-is-the-tide-and-its-rising)

## Labels

`enhancement`, `storage`, `cost-optimization`, `large`, `phase-3`
