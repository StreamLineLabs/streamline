//! Storage engine for Streamline
//!
//! The storage engine is responsible for durable storage of records.
//! It provides a topic-based abstraction over segments and partitions.
//!
//! ## Storage Modes
//!
//! Topics can be configured with different storage modes:
//!
//! - **Local**: Traditional local disk storage (default)
//! - **Hybrid**: Local WAL + object storage segments
//! - **Diskless**: All data written to object storage
//!
//! See [`storage_mode`] and [`backend`] modules for details.

pub mod adaptive_batch;
pub mod async_index;
pub mod async_io;
pub mod async_segment;
pub mod async_wal;
pub mod backend;
pub mod backend_factory;
pub mod bloom;
pub mod buffer;
pub mod cache;
pub mod checkpoint;
pub mod compaction;
pub mod compression;
pub mod cross_topic_buffer;
pub mod encryption;
pub mod index;
pub mod io_backend;
pub mod mmap;
pub mod partition;
pub mod partition_maintenance;
pub mod partition_read;
pub mod prefetch;
pub mod producer_state;
pub mod record;
pub mod retention;
pub mod segment;
pub mod sendfile;
pub mod state_store;
pub mod storage_mode;
pub mod deterministic_replay;
pub mod replay_engine;
pub mod timeseries;
pub mod timetravel;
pub mod topic;
pub mod wal;
pub mod hw_acceleration;
pub mod zerocopy;

// Cloud storage modules (require cloud-storage feature)
#[cfg(feature = "cloud-storage")]
pub mod diskless;
#[cfg(feature = "cloud-storage")]
pub mod lifecycle;
#[cfg(feature = "cloud-storage")]
pub mod segment_s3;
#[cfg(feature = "cloud-storage")]
pub mod tiering;
#[cfg(feature = "cloud-storage")]
pub mod tiering_policy;
#[cfg(feature = "cloud-storage")]
pub mod wal_s3;

// Columnar storage with Apache Arrow (requires columnar feature)
#[cfg(feature = "columnar")]
pub mod columnar;

// Public API types
pub use adaptive_batch::{
    AdaptiveBatchConfig, AdaptiveBatchStats, AdaptiveBatcher, AdaptiveBatcherBuilder,
    SharedAdaptiveBatcher,
};
pub use async_segment::AsyncSegment;
pub use async_wal::{AsyncWalReader, AsyncWalWriter};
pub use backend::{BufferStrategy, SegmentBackend, WalBackend};
pub use backend_factory::{BackendFactory, BackendFactoryConfig};
pub use bloom::{
    BloomFilterBuilder, BloomFilterConfig, BloomFilterIndex, BloomFilterStats, ConsumerBloomFilter,
    KeyBloomFilter, DEFAULT_EXPECTED_ITEMS, DEFAULT_FALSE_POSITIVE_RATE,
};
pub use cache::{
    CacheConfig as SegmentCacheConfig, CacheKey, CacheStats, SegmentCache, SegmentCacheBuilder,
    SharedSegmentCache,
};
pub use checkpoint::{Checkpoint, CheckpointManager};
pub use compression::{compress, decompress, CompressionCodec};
pub use encryption::{
    generate_key_hex, DataEncryptionKey, EncryptionAlgorithm, EncryptionConfig, EncryptionManager,
    KeyProvider, KeyProviderType, KEY_SIZE, NONCE_SIZE, TAG_SIZE,
};
pub use mmap::MmapSegmentReader;
pub use partition::Partition;
pub use prefetch::{
    advise_dontneed, advise_noreuse, advise_random, advise_sequential, advise_willneed,
    prefetch_range, PrefetchConfig, SegmentPrefetcher, DEFAULT_PREFETCH_SIZE,
};
pub use producer_state::{
    ProducerEpoch, ProducerId, ProducerMetadata, ProducerPartitionState, ProducerStateManager,
    SequenceNumber, SequenceValidationResult,
};
pub use record::{Header, Record, RecordBatch};
pub use segment::{Segment, SegmentConfig, SegmentSyncMode, SegmentWriter};
pub use storage_mode::{RemoteStorageConfig, StorageMode};
pub use topic::{
    CleanupPolicy, PartitionInfo, Topic, TopicConfig, TopicManager, TopicMetadata,
    MESSAGE_TTL_HEADER,
};
pub use wal::{WalEntry, WalEntryType, WalReader, WalWriter};

// Replay engine re-exports
pub use replay_engine::{
    DivergenceSample, ReplayConfig, ReplayDiff, ReplayEngine, ReplayFilter, ReplayOutput,
    ReplayProgress, ReplaySession, ReplayStats, ReplayStatsSnapshot, ReplayStatus,
};

// Deterministic replay debugger re-exports
pub use deterministic_replay::{
    BreakCondition, Breakpoint, DebugSession, DebugStats, DebugStatus,
    DeterministicReplayEngine, ReplayDebugConfig, StateCapture, StepRecord,
};

// Time-travel re-exports
pub use timetravel::{
    SnapshotMetadata, TimePoint, TimeTravelConfig, TimeTravelManager, TimeTravelStats,
    VersionEntry, VersionManager,
};

// Cloud storage re-exports (require cloud-storage feature)
#[cfg(feature = "cloud-storage")]
pub use lifecycle::{
    AccessPattern, CostEstimate, CostOptimizationConfig, LifecycleConfig,
    LifecycleManager as StorageLifecycleManager, LifecyclePolicy, LifecycleStats,
    LifecycleStatsSnapshot, StorageTier, TieringRecommendation,
};
#[cfg(feature = "cloud-storage")]
pub use segment_s3::{
    LocalSegmentBackend, S3Segment, S3SegmentStateless, S3StatelessConfig, S3StatelessStatsSnapshot,
};
#[cfg(feature = "cloud-storage")]
pub use tiering::{
    CacheConfig, TieringBackend, TieringConfig, TieringManager, TieringPolicy, TieringStats,
};
#[cfg(feature = "cloud-storage")]
pub use wal_s3::{LocalWalBackend, S3WalWriter};

// Columnar storage re-exports (require columnar feature)
#[cfg(feature = "columnar")]
pub use columnar::{
    ColumnarCompression, ColumnarConfig, ColumnarReader, ColumnarStats, ColumnarStorage,
    ColumnarWriter, Projection,
};

// Hardware acceleration engine
pub use hw_acceleration::{
    BatchProcessResult, HwAccelConfig, HwAccelStats, HwAccelStatsSnapshot, HwAccelerationEngine,
    HwCapabilities, ProcessMethod,
};

pub use zerocopy::{
    sendfile_async, sendfile_to_socket, BufferPool, SendfileResult, ZeroCopyConfig, ZeroCopyReader,
    ZeroCopyStats,
};

// State store for stateless agent architecture
pub use state_store::{
    create_state_store, InMemoryStateStore, LeaseHandle, PartitionState, StateStore,
    StateStoreBackend, StateStoreConfig, StateStoreStats,
};

// Cross-topic buffer for efficient S3 batching
pub use cross_topic_buffer::{
    CrossTopicBufferConfig, CrossTopicBufferManager, CrossTopicBufferStats, CtbfHeader,
    CtbfIndexEntry, CTBF_HEADER_SIZE, CTBF_INDEX_ENTRY_SIZE, CTBF_MAGIC, CTBF_VERSION,
};

// Internal crate-level re-exports for types used across modules
pub(crate) use retention::RetentionEnforcer;

// Re-export async I/O utilities for convenient access
pub use async_io::{
    atomic_write_async, create_dir_all_async, exists_async, read_async, read_dir_async,
    read_to_string_async, remove_dir_all_async, remove_file_async, rename_async, sync_dir_async,
    sync_file_async, write_async,
};

// Re-export I/O backend types
pub use io_backend::{
    get_standard_backend, should_use_uring, AsyncFile, AsyncFileSystem, BufferPoolStats,
    IoBackendConfig, IoBufferPool, IoResult, IoStats, StandardFile, StandardFileSystem,
};

// Re-export advanced io_uring types
pub use io_backend::{
    AdvancedUringManager, AdvancedUringManagerStats, Batch, BatchBuilder, BatchConfig, BatchOp,
    BatchOpResult, FixedFileConfig, FixedFileId, FixedFileRegistry, FixedFileStats,
    RegisteredBufferConfig, RegisteredBufferId, RegisteredBufferPool, RegisteredBufferStats,
};

// Re-export io_uring types when available
#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use io_backend::{
    get_uring_backend, is_uring_available, IoWorker, IoWorkerConfig, UringFile, UringFileSystem,
};

// Re-export async index types
pub use async_index::{AsyncIndexBuilder, AsyncSegmentIndex};
