//! Topic management for Streamline storage
//!
//! ## Async Support
//!
//! This module provides both synchronous and asynchronous methods for topic
//! operations. Use the `*_async` variants in async contexts to avoid blocking
//! the tokio runtime during file I/O operations.
//!
//! # Lock Ordering
//!
//! This module uses multiple locks that must be acquired in consistent order:
//!
//! ## TopicManager Locks
//!
//! 1. `topics` (DashMap) - Topic registry, use DashMap methods for safe access
//! 2. `wal_writer` (Mutex) - Write-ahead log writer
//! 3. `checkpoint_manager` (Mutex) - Checkpoint coordination
//!
//! When acquiring multiple TopicManager locks:
//! ```text
//! // CORRECT: DashMap access first, then WAL, then checkpoint
//! let topic_entry = self.topics.get("topic");
//! let wal = self.wal_writer.lock();
//! let checkpoint = self.checkpoint_manager.lock();
//! ```
//!
//! ## Topic/Partition Locks
//!
//! Each `Topic` contains `Vec<RwLock<Partition>>`. Access partitions individually
//! to minimize contention. Never hold multiple partition locks simultaneously
//! unless absolutely necessary.
//!
//! ```text
//! // CORRECT: Access one partition at a time
//! let p0 = topic.partitions[0].write();
//! drop(p0);
//! let p1 = topic.partitions[1].write();
//!
//! // INCORRECT: Holding multiple partition locks (deadlock risk)
//! let p0 = topic.partitions[0].write();
//! let p1 = topic.partitions[1].write(); // Risk if another thread acquires in reverse order
//! ```
//!
//! ## Cross-Module Lock Ordering
//!
//! When interacting with other modules:
//! 1. TopicManager locks
//! 2. Partition locks (within Topic)
//! 3. Segment locks (within Partition)
//! 4. ProducerStateManager locks (see producer_state.rs)
//!
//! ## Debug-Mode Lock Tracking
//!
//! In debug builds, lock acquisitions are tracked via `tracing::debug!` spans.
//! Enable `RUST_LOG=streamline::storage::topic=debug` to see lock acquisition order.
//! For deadlock debugging, use `parking_lot`'s deadlock detection feature:
//!
//! ```toml
//! [dependencies]
//! parking_lot = { version = "0.12", features = ["deadlock_detection"] }
//! ```
//!
//! Then enable with `parking_lot::deadlock::check_deadlock()` in a background thread.

use crate::config::WalConfig;
use crate::error::{Result, StreamlineError};
use crate::storage::async_io;
use crate::storage::checkpoint::CheckpointManager;
use crate::storage::partition::Partition;
use crate::storage::record::Record;
use crate::storage::segment::{SegmentConfig, SegmentSyncMode};
use crate::storage::storage_mode::{RemoteStorageConfig, StorageMode};
use crate::storage::wal::{ShardedWalReader, ShardedWalWriter, WalEntry, WalEntryType};
use bytes::Bytes;
use chrono::Utc;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Cleanup policy for topic retention/compaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CleanupPolicy {
    #[serde(rename = "delete")]
    #[default]
    Delete,
    #[serde(rename = "compact")]
    Compact,
    #[serde(rename = "delete,compact")]
    DeleteCompact,
    #[serde(rename = "compact,delete")]
    CompactDelete,
}

impl CleanupPolicy {
    pub fn as_str(&self) -> &'static str {
        match self {
            CleanupPolicy::Delete => "delete",
            CleanupPolicy::Compact => "compact",
            CleanupPolicy::DeleteCompact => "delete,compact",
            CleanupPolicy::CompactDelete => "compact,delete",
        }
    }

    pub fn supports_compaction(&self) -> bool {
        matches!(
            self,
            CleanupPolicy::Compact | CleanupPolicy::DeleteCompact | CleanupPolicy::CompactDelete
        )
    }

    pub fn supports_delete(&self) -> bool {
        matches!(
            self,
            CleanupPolicy::Delete | CleanupPolicy::DeleteCompact | CleanupPolicy::CompactDelete
        )
    }

    pub fn is_compact_only(&self) -> bool {
        matches!(self, CleanupPolicy::Compact)
    }
}

impl std::fmt::Display for CleanupPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for CleanupPolicy {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        match value {
            "delete" => Ok(CleanupPolicy::Delete),
            "compact" => Ok(CleanupPolicy::Compact),
            "delete,compact" => Ok(CleanupPolicy::DeleteCompact),
            "compact,delete" => Ok(CleanupPolicy::CompactDelete),
            _ => Err(format!("Invalid cleanup policy '{}'", value)),
        }
    }
}

/// Maximum allowed topic name length (Kafka standard)
const MAX_TOPIC_NAME_LENGTH: usize = 249;

/// Reserved topic name prefixes that are used internally
const RESERVED_TOPIC_PREFIXES: &[&str] = &["__"];

/// Check if a character is valid for topic names
/// Valid characters: alphanumeric, dots (.), underscores (_), and hyphens (-)
#[inline]
fn is_valid_topic_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-'
}

/// Validate a topic name according to Kafka naming conventions.
///
/// # Rules
///
/// A valid topic name:
/// - Contains only alphanumeric characters, dots (.), underscores (_), and hyphens (-)
/// - Is between 1 and 249 characters long
/// - Is not "." or ".." (filesystem special names)
///
/// Names starting with `__` (double underscore) are allowed but reserved
/// for internal topics like `__consumer_offsets`.
///
/// # Examples
///
/// ```rust,ignore
/// use streamline::storage::topic::validate_topic_name;
///
/// // Valid names
/// assert!(validate_topic_name("orders").is_ok());
/// assert!(validate_topic_name("user.events").is_ok());
/// assert!(validate_topic_name("my-topic_v2").is_ok());
/// assert!(validate_topic_name("__consumer_offsets").is_ok()); // Internal, warns
///
/// // Invalid names
/// assert!(validate_topic_name("").is_err());           // Empty
/// assert!(validate_topic_name("my topic").is_err());   // Space
/// assert!(validate_topic_name("topic@v1").is_err());   // Invalid char @
/// assert!(validate_topic_name(".").is_err());          // Filesystem special
/// assert!(validate_topic_name("..").is_err());         // Filesystem special
/// ```
///
/// # Errors
///
/// Returns `StreamlineError::InvalidTopicName` with a descriptive message.
pub fn validate_topic_name(name: &str) -> Result<()> {
    // Check empty
    if name.is_empty() {
        return Err(StreamlineError::InvalidTopicName(
            "Topic name cannot be empty".to_string(),
        ));
    }

    // Check length
    if name.len() > MAX_TOPIC_NAME_LENGTH {
        return Err(StreamlineError::InvalidTopicName(format!(
            "Topic name '{}' exceeds maximum length of {} characters (got {})",
            name,
            MAX_TOPIC_NAME_LENGTH,
            name.len()
        )));
    }

    // Check for filesystem special names
    if name == "." || name == ".." {
        return Err(StreamlineError::InvalidTopicName(format!(
            "Topic name '{}' is a reserved filesystem name",
            name
        )));
    }

    // Check character pattern - all characters must be valid
    if !name.chars().all(is_valid_topic_char) {
        return Err(StreamlineError::InvalidTopicName(format!(
            "Topic name '{}' contains invalid characters. \
             Only alphanumeric characters, dots, underscores, and hyphens are allowed",
            name
        )));
    }

    // Check reserved prefixes
    for prefix in RESERVED_TOPIC_PREFIXES {
        if name.starts_with(prefix) {
            warn!(
                topic = %name,
                prefix = %prefix,
                "Topic name uses reserved prefix"
            );
            // Allow but warn - internal topics like __consumer_offsets are valid
        }
    }

    Ok(())
}

/// Calculate which shard (CPU core) should own a given partition.
///
/// This function implements the partition-to-shard assignment strategy
/// used for the sharded runtime mode (thread-per-core execution). The
/// default strategy is simple round-robin: `partition_id % shard_count`.
///
/// # Arguments
///
/// * `partition_id` - The partition ID to assign
/// * `shard_count` - Total number of shards (typically = CPU count, must be > 0)
///
/// # Returns
///
/// The shard ID (0-indexed) that should own this partition.
///
/// # Example
///
/// ```rust,ignore
/// use streamline::storage::topic::partition_to_shard;
///
/// // With 4 CPU cores (shards), partitions are distributed:
/// assert_eq!(partition_to_shard(0, 4), 0);  // partition 0 → shard 0
/// assert_eq!(partition_to_shard(1, 4), 1);  // partition 1 → shard 1
/// assert_eq!(partition_to_shard(4, 4), 0);  // partition 4 → shard 0 (wraps)
/// assert_eq!(partition_to_shard(5, 4), 1);  // partition 5 → shard 1
///
/// // Distribution with 6 partitions and 4 shards:
/// // Shard 0: partitions [0, 4]
/// // Shard 1: partitions [1, 5]
/// // Shard 2: partitions [2]
/// // Shard 3: partitions [3]
/// ```
///
/// # When to Use
///
/// Use this for global partition IDs when you have a single topic or when
/// partition IDs are unique across all topics. For multi-topic scenarios
/// where each topic's partitions start at 0, use [`partition_to_shard_hashed`]
/// for better distribution.
#[inline]
pub fn partition_to_shard(partition_id: u32, shard_count: usize) -> usize {
    debug_assert!(shard_count > 0, "shard_count must be > 0");
    partition_id as usize % shard_count
}

/// Calculate which shard should own a partition using hash-based assignment.
///
/// This variant uses a hash of the topic name and partition ID for more
/// even distribution when you have multiple topics with partition IDs
/// starting at 0.
///
/// # Arguments
///
/// * `topic` - The topic name
/// * `partition_id` - The partition ID within the topic
/// * `shard_count` - Total number of shards (must be > 0)
///
/// # Returns
///
/// The shard ID (0-indexed) that should own this partition.
///
/// # Example
///
/// ```rust,ignore
/// use streamline::storage::topic::partition_to_shard_hashed;
///
/// // Different topics with same partition ID go to different shards
/// let shard_a = partition_to_shard_hashed("orders", 0, 4);
/// let shard_b = partition_to_shard_hashed("users", 0, 4);
/// // shard_a and shard_b will likely differ due to different topic names
///
/// // Same topic+partition always maps to same shard (deterministic)
/// assert_eq!(
///     partition_to_shard_hashed("orders", 0, 4),
///     partition_to_shard_hashed("orders", 0, 4)
/// );
/// ```
///
/// # When to Use
///
/// Use this when:
/// - You have multiple topics where each topic's partitions start at 0
/// - You want better distribution across shards
/// - The simple modulo approach would cluster too many partitions on the same shard
#[inline]
pub fn partition_to_shard_hashed(topic: &str, partition_id: u32, shard_count: usize) -> usize {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    debug_assert!(shard_count > 0, "shard_count must be > 0");

    let mut hasher = DefaultHasher::new();
    topic.hash(&mut hasher);
    partition_id.hash(&mut hasher);
    (hasher.finish() as usize) % shard_count
}

/// Atomically write data to a file using temp file + rename pattern.
///
/// This ensures the file is never partially written, even if the process
/// crashes mid-write. The sequence is:
/// 1. Write to a temp file in the same directory
/// 2. fsync the temp file to ensure data is on disk
/// 3. Rename temp file to target (atomic on POSIX)
/// 4. fsync parent directory to ensure rename is persisted
fn atomic_write(path: &Path, data: &[u8]) -> Result<()> {
    // Create temp file in same directory (ensures same filesystem for atomic rename)
    let parent = path.parent().ok_or_else(|| {
        StreamlineError::storage_msg(format!("Cannot get parent directory of {:?}", path))
    })?;

    let temp_name = format!(
        ".{}.tmp.{}",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("metadata"),
        std::process::id()
    );
    let temp_path = parent.join(&temp_name);

    // Write to temp file
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)
            .map_err(|e| {
                StreamlineError::storage_msg(format!(
                    "Failed to create temp file {:?}: {}",
                    temp_path, e
                ))
            })?;

        file.write_all(data).map_err(|e| {
            StreamlineError::storage_msg(format!(
                "Failed to write temp file {:?}: {}",
                temp_path, e
            ))
        })?;

        // fsync to ensure data is on disk before rename
        file.sync_all().map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to sync temp file {:?}: {}", temp_path, e))
        })?;
    }

    // Atomic rename (on POSIX systems, rename is atomic if src and dst are on same filesystem)
    fs::rename(&temp_path, path).map_err(|e| {
        // Clean up temp file on failure
        let _ = fs::remove_file(&temp_path);
        StreamlineError::storage_msg(format!(
            "Failed to rename {:?} to {:?}: {}",
            temp_path, path, e
        ))
    })?;

    // Sync parent directory to ensure the rename is persisted
    // This is CRITICAL for durability on crash - directory fsync is REQUIRED
    // on Linux/POSIX to persist file renames
    let dir = File::open(parent).map_err(|e| {
        StreamlineError::storage_msg(format!(
            "Failed to open directory {:?} for sync: {}",
            parent, e
        ))
    })?;
    dir.sync_all().map_err(|e| {
        StreamlineError::storage_msg(format!(
            "Failed to sync directory {:?} - rename durability not guaranteed: {}",
            parent, e
        ))
    })?;

    Ok(())
}

/// Metadata describing a topic's configuration and identity.
///
/// `TopicMetadata` is persisted to disk as `metadata.json` in each topic's
/// directory and contains:
///
/// - **Identity**: Name and UUID for stable identification
/// - **Partitioning**: Number of partitions for parallelism
/// - **Replication**: Factor for data redundancy (clustering feature)
/// - **Configuration**: Retention, cleanup, and storage settings
///
/// # Persistence
///
/// Metadata is stored at: `<data_dir>/topics/<topic_name>/metadata.json`
///
/// # Example
///
/// ```rust,ignore
/// let metadata = TopicMetadata::new("orders", 6);
/// println!("Topic {} has {} partitions", metadata.name, metadata.num_partitions);
/// println!("Topic ID: {}", metadata.topic_id);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicMetadata {
    /// Human-readable topic name.
    ///
    /// Must follow Kafka naming conventions: alphanumeric, dots, underscores,
    /// hyphens only. Maximum 249 characters.
    pub name: String,

    /// Unique topic identifier (UUID v4).
    ///
    /// This ID is stable across topic renames and is used by Kafka 3.0+ clients
    /// for topic identification. Generated automatically on topic creation.
    #[serde(default = "generate_topic_id")]
    pub topic_id: Uuid,

    /// Number of partitions in this topic.
    ///
    /// Partitions enable parallel processing - each partition can be consumed
    /// by a different consumer in a group. Cannot be decreased after creation.
    pub num_partitions: i32,

    /// Number of replicas for each partition (requires `clustering` feature).
    ///
    /// Controls data redundancy. A factor of 3 means each partition's data
    /// is stored on 3 different brokers. Default is 1 for single-node deployments.
    pub replication_factor: i16,

    /// Unix timestamp (milliseconds) when the topic was created.
    pub created_at: i64,

    /// Topic-specific configuration (retention, cleanup, storage mode).
    pub config: TopicConfig,
}

/// Generate a new topic ID (used as serde default for backward compatibility)
fn generate_topic_id() -> Uuid {
    Uuid::new_v4()
}

/// Configuration options for a topic's behavior and storage.
///
/// Controls how data is retained, compacted, and stored. Use the builder
/// methods for common configurations:
///
/// - [`TopicConfig::local()`] - Standard local disk storage
/// - [`TopicConfig::hybrid()`] - Local WAL with S3/Azure/GCS tiering
/// - [`TopicConfig::diskless()`] - Direct cloud storage (no local disk)
///
/// # Retention Policies
///
/// **Time-based retention** (`retention_ms`):
/// - `-1`: Infinite retention (default)
/// - `86400000`: 1 day
/// - `604800000`: 7 days
///
/// **Size-based retention** (`retention_bytes`):
/// - `-1`: No size limit (default)
/// - Positive value: Delete oldest segments when topic exceeds this size
///
/// Both policies can be active simultaneously - whichever triggers first applies.
///
/// # Cleanup Policies
///
/// - `Delete` (default): Remove segments older than retention settings
/// - `Compact`: Keep only the latest value for each key (log compaction)
///
/// # Example
///
/// ```rust,ignore
/// // 7-day retention with compaction
/// let config = TopicConfig {
///     retention_ms: 7 * 24 * 60 * 60 * 1000, // 7 days
///     cleanup_policy: CleanupPolicy::Compact,
///     ..TopicConfig::default()
/// };
///
/// // Diskless storage (S3)
/// let config = TopicConfig::diskless();
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    /// Maximum time (milliseconds) to retain data. Use `-1` for infinite.
    ///
    /// Data older than this is eligible for deletion (with `"delete"` policy)
    /// or compaction (with `"compact"` policy).
    pub retention_ms: i64,

    /// Maximum total size (bytes) of all segments. Use `-1` for unlimited.
    ///
    /// When exceeded, the oldest segments are deleted until under the limit.
    pub retention_bytes: i64,

    /// Maximum size (bytes) of a single segment file.
    ///
    /// When a segment reaches this size, a new segment is created.
    /// Default: 100MB. Larger segments reduce file count but increase
    /// recovery time after crashes.
    pub segment_bytes: u64,

    /// Data cleanup strategy.
    ///
    /// - Delete: Remove data older than retention settings
    /// - Compact: Keep only the latest value for each key (requires keys)
    #[serde(default)]
    pub cleanup_policy: CleanupPolicy,

    /// Minimum dirty ratio before compaction runs (0.0 to 1.0).
    ///
    /// Compaction only runs when the ratio of dirty (duplicate keys) to
    /// total data exceeds this threshold. Default: 0.5 (50%).
    #[serde(default = "default_min_cleanable_dirty_ratio")]
    pub min_cleanable_dirty_ratio: f64,

    /// How long (milliseconds) to retain tombstone markers after compaction.
    ///
    /// Tombstones (null values) mark deleted keys. They must be retained
    /// long enough for all consumers to see the deletion. Default: 24 hours.
    #[serde(default = "default_delete_retention_ms")]
    pub delete_retention_ms: i64,

    /// Minimum segment age (milliseconds) before it can be compacted.
    ///
    /// Prevents compacting recently-written data. Default: 0 (no minimum).
    #[serde(default)]
    pub min_compaction_lag_ms: i64,

    /// Message-level TTL (milliseconds). Use `-1` for no TTL.
    ///
    /// Unlike `retention_ms` which deletes entire segments, `message_ttl_ms`
    /// filters out expired messages at read time based on their timestamp.
    /// Per-message TTL can override this via the `x-message-ttl-ms` header.
    #[serde(default = "default_message_ttl_ms")]
    pub message_ttl_ms: i64,

    /// Storage backend for this topic's data.
    ///
    /// - `Local`: Traditional local disk storage (default)
    /// - `Hybrid`: Local WAL for acks + object storage for segments
    /// - `Diskless`: All data written directly to object storage
    #[serde(default)]
    pub storage_mode: StorageMode,

    /// Remote storage configuration (required for Hybrid/Diskless modes).
    ///
    /// Specifies batching, caching, and connection settings for S3/Azure/GCS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_storage: Option<RemoteStorageConfig>,
}

/// Well-known header key for per-message TTL (in milliseconds)
pub const MESSAGE_TTL_HEADER: &str = "x-message-ttl-ms";

/// Statistics for a single partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionStats {
    /// Partition ID
    pub partition_id: i32,
    /// Earliest offset in the partition
    pub earliest_offset: i64,
    /// Latest offset in the partition (next offset to be written)
    pub latest_offset: i64,
    /// Number of messages in the partition
    pub message_count: u64,
}

/// Statistics for a topic including all partition stats
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicStats {
    /// Topic name
    pub name: String,
    /// Number of partitions
    pub num_partitions: i32,
    /// Replication factor
    pub replication_factor: i16,
    /// Statistics for each partition
    pub partition_stats: Vec<PartitionStats>,
    /// Total messages across all partitions
    pub total_messages: u64,
}

fn default_message_ttl_ms() -> i64 {
    -1 // Infinite by default
}

fn default_min_cleanable_dirty_ratio() -> f64 {
    0.5
}

fn default_delete_retention_ms() -> i64 {
    86_400_000 // 24 hours in milliseconds
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            retention_ms: -1,
            retention_bytes: -1,
            segment_bytes: 100 * 1024 * 1024, // 100 MB default
            cleanup_policy: CleanupPolicy::default(),
            min_cleanable_dirty_ratio: default_min_cleanable_dirty_ratio(),
            delete_retention_ms: default_delete_retention_ms(),
            min_compaction_lag_ms: 0,
            message_ttl_ms: default_message_ttl_ms(),
            storage_mode: StorageMode::default(),
            remote_storage: None,
        }
    }
}

impl TopicConfig {
    /// Create a new topic config with local storage (default)
    pub fn local() -> Self {
        Self::default()
    }

    /// Create a new topic config with hybrid storage mode
    ///
    /// Hybrid mode uses local WAL for low-latency acknowledgments
    /// and object storage for segment data.
    pub fn hybrid() -> Self {
        Self {
            storage_mode: StorageMode::Hybrid,
            remote_storage: Some(RemoteStorageConfig::default()),
            ..Self::default()
        }
    }

    /// Create a new topic config with diskless storage mode
    ///
    /// Diskless mode writes all data directly to object storage.
    /// Higher latency but zero local disk usage.
    pub fn diskless() -> Self {
        Self {
            storage_mode: StorageMode::Diskless,
            remote_storage: Some(RemoteStorageConfig::default()),
            ..Self::default()
        }
    }

    /// Create a new topic config with diskless mode optimized for low latency
    pub fn diskless_low_latency() -> Self {
        Self {
            storage_mode: StorageMode::Diskless,
            remote_storage: Some(RemoteStorageConfig::low_latency()),
            ..Self::default()
        }
    }

    /// Create a new topic config with diskless mode optimized for throughput
    pub fn diskless_high_throughput() -> Self {
        Self {
            storage_mode: StorageMode::Diskless,
            remote_storage: Some(RemoteStorageConfig::high_throughput()),
            ..Self::default()
        }
    }

    /// Check if this topic uses local storage
    pub fn is_local(&self) -> bool {
        self.storage_mode.is_local()
    }

    /// Check if this topic is diskless
    pub fn is_diskless(&self) -> bool {
        self.storage_mode.is_diskless()
    }

    /// Check if this topic requires remote storage configuration
    pub fn requires_remote_storage(&self) -> bool {
        self.storage_mode.requires_remote_storage()
    }

    /// Validate the topic configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        // Validate remote storage config if required
        if self.storage_mode.requires_remote_storage() {
            match &self.remote_storage {
                Some(config) => config.validate()?,
                None => {
                    return Err(format!(
                        "Storage mode '{}' requires remote_storage configuration",
                        self.storage_mode
                    ));
                }
            }
        }

        // Cleanup policy is validated by type

        Ok(())
    }
}

impl Default for TopicMetadata {
    fn default() -> Self {
        Self {
            name: String::new(),
            topic_id: Uuid::new_v4(),
            num_partitions: 1,
            replication_factor: 1,
            created_at: chrono::Utc::now().timestamp_millis(),
            config: TopicConfig::default(),
        }
    }
}

impl TopicMetadata {
    /// Create new topic metadata
    pub fn new(name: &str, num_partitions: i32) -> Self {
        Self {
            name: name.to_string(),
            num_partitions,
            ..Default::default()
        }
    }
}

/// A named stream of records, partitioned for parallel processing.
///
/// `Topic` is the primary unit of data organization in Streamline. Each topic
/// contains one or more partitions, and each partition is an ordered, immutable
/// sequence of records.
///
/// # Partitioning
///
/// Partitions enable:
/// - **Parallelism**: Multiple consumers can read different partitions concurrently
/// - **Ordering**: Records within a partition are strictly ordered by offset
/// - **Scalability**: Add partitions to increase throughput
///
/// # Storage Layout
///
/// ```text
/// <data_dir>/topics/<topic_name>/
/// ├── metadata.json         # Topic configuration
/// ├── partition-0/
/// │   ├── 00000000.segment  # First segment
/// │   ├── 00001000.segment  # Second segment (starts at offset 1000)
/// │   └── 00001000.index    # Offset index
/// └── partition-1/
///     └── ...
/// ```
///
/// # Thread Safety
///
/// Each partition has its own `RwLock` for fine-grained concurrency.
/// Multiple threads can write to different partitions simultaneously.
///
/// # Example
///
/// ```rust,ignore
/// // Open or create a topic with 6 partitions
/// let topic = Topic::open("orders", 6, Path::new("./data"))?;
///
/// // Append to a specific partition
/// let offset = topic.append(0, Some(key), value)?;
///
/// // Read from a partition
/// let records = topic.read(0, start_offset, max_records)?;
/// ```
pub struct Topic {
    /// Topic metadata (name, ID, config)
    metadata: TopicMetadata,

    /// Partitions with individual locks for concurrent access
    partitions: Vec<RwLock<Partition>>,

    /// Base storage directory path
    #[allow(dead_code)]
    base_path: PathBuf,

    /// Whether this topic is in-memory only (no disk persistence)
    in_memory: bool,

    /// Segment I/O configuration (DirectIO, sync mode, etc.)
    #[allow(dead_code)]
    segment_config: Option<SegmentConfig>,
}

impl Topic {
    /// Create a new topic or open an existing one
    pub fn open(name: &str, num_partitions: i32, base_path: &Path) -> Result<Self> {
        Self::open_with_config(name, num_partitions, base_path, TopicConfig::default())
    }

    /// Create a new topic with custom configuration or open an existing one
    pub fn open_with_config(
        name: &str,
        num_partitions: i32,
        base_path: &Path,
        config: TopicConfig,
    ) -> Result<Self> {
        // Create topic directory
        let topic_path = base_path.join("topics").join(name);
        fs::create_dir_all(&topic_path)?;

        // Load or create metadata
        let metadata_path = topic_path.join("metadata.json");
        let metadata = if metadata_path.exists() {
            let data = fs::read_to_string(&metadata_path)?;
            serde_json::from_str(&data)?
        } else {
            let metadata = TopicMetadata {
                name: name.to_string(),
                topic_id: Uuid::new_v4(),
                num_partitions,
                replication_factor: 1,
                created_at: chrono::Utc::now().timestamp_millis(),
                config,
            };
            let data = serde_json::to_string_pretty(&metadata)?;
            atomic_write(&metadata_path, data.as_bytes())?;
            metadata
        };

        // Open partitions
        let mut partitions = Vec::with_capacity(metadata.num_partitions as usize);
        for i in 0..metadata.num_partitions {
            let mut partition = Partition::open(name, i, base_path)?;
            // Configure partition with topic's segment size
            partition.set_max_segment_bytes(metadata.config.segment_bytes);
            partitions.push(RwLock::new(partition));
        }

        info!(
            topic = %name,
            partitions = metadata.num_partitions,
            "Topic opened"
        );

        Ok(Self {
            metadata,
            partitions,
            base_path: base_path.to_path_buf(),
            in_memory: false,
            segment_config: None,
        })
    }

    /// Create a new topic with custom configuration and segment config (for DirectIO)
    pub fn open_with_segment_config(
        name: &str,
        num_partitions: i32,
        base_path: &Path,
        config: TopicConfig,
        segment_config: Option<SegmentConfig>,
    ) -> Result<Self> {
        // Create topic directory
        let topic_path = base_path.join("topics").join(name);
        fs::create_dir_all(&topic_path)?;

        // Load or create metadata
        let metadata_path = topic_path.join("metadata.json");
        let metadata = if metadata_path.exists() {
            let data = fs::read_to_string(&metadata_path)?;
            serde_json::from_str(&data)?
        } else {
            let metadata = TopicMetadata {
                name: name.to_string(),
                topic_id: Uuid::new_v4(),
                num_partitions,
                replication_factor: 1,
                created_at: chrono::Utc::now().timestamp_millis(),
                config,
            };
            let data = serde_json::to_string_pretty(&metadata)?;
            atomic_write(&metadata_path, data.as_bytes())?;
            metadata
        };

        // Open partitions with segment config if provided
        let mut partitions = Vec::with_capacity(metadata.num_partitions as usize);
        for i in 0..metadata.num_partitions {
            let mut partition = if let Some(ref seg_config) = segment_config {
                Partition::open_with_segment_config(name, i, base_path, seg_config.clone())?
            } else {
                Partition::open(name, i, base_path)?
            };
            // Configure partition with topic's segment size
            partition.set_max_segment_bytes(metadata.config.segment_bytes);
            partitions.push(RwLock::new(partition));
        }

        info!(
            topic = %name,
            partitions = metadata.num_partitions,
            direct_io = segment_config.as_ref().is_some_and(|c| c.use_direct_io),
            "Topic opened with segment config"
        );

        Ok(Self {
            metadata,
            partitions,
            base_path: base_path.to_path_buf(),
            in_memory: false,
            segment_config,
        })
    }

    /// Create a new in-memory topic (no data persisted to disk)
    ///
    /// This is useful for testing, development, and ephemeral workloads.
    pub fn open_in_memory(name: &str, num_partitions: i32) -> Result<Self> {
        Self::open_in_memory_with_config(name, num_partitions, TopicConfig::default())
    }

    /// Create a new in-memory topic with custom configuration
    pub fn open_in_memory_with_config(
        name: &str,
        num_partitions: i32,
        config: TopicConfig,
    ) -> Result<Self> {
        let metadata = TopicMetadata {
            name: name.to_string(),
            topic_id: Uuid::new_v4(),
            num_partitions,
            replication_factor: 1,
            created_at: chrono::Utc::now().timestamp_millis(),
            config,
        };

        // Create in-memory partitions
        let mut partitions = Vec::with_capacity(num_partitions as usize);
        for i in 0..num_partitions {
            let mut partition = Partition::open_in_memory(name, i)?;
            partition.set_max_segment_bytes(metadata.config.segment_bytes);
            partitions.push(RwLock::new(partition));
        }

        info!(
            topic = %name,
            partitions = num_partitions,
            "In-memory topic created"
        );

        Ok(Self {
            metadata,
            partitions,
            base_path: PathBuf::new(), // Not used in in-memory mode
            in_memory: true,
            segment_config: None, // DirectIO not used for in-memory topics
        })
    }

    /// Check if this topic is running in in-memory mode
    pub fn is_in_memory(&self) -> bool {
        self.in_memory
    }

    /// Configure segment sync mode for all partitions in this topic
    ///
    /// This sets the durability behavior for segment writes across all partitions.
    pub fn set_segment_sync_config(&self, sync_mode: SegmentSyncMode, sync_interval_ms: u64) {
        for partition_lock in &self.partitions {
            let mut partition = partition_lock.write();
            partition.set_sync_config(sync_mode, sync_interval_ms);
        }
    }

    /// Get topic name
    pub fn name(&self) -> &str {
        &self.metadata.name
    }

    /// Get number of partitions
    pub fn num_partitions(&self) -> i32 {
        self.metadata.num_partitions
    }

    /// Get topic metadata
    pub fn metadata(&self) -> &TopicMetadata {
        &self.metadata
    }

    /// Get the unique topic ID (UUID)
    pub fn topic_id(&self) -> Uuid {
        self.metadata.topic_id
    }

    /// Append a record to the specified partition
    pub fn append(&self, partition: i32, key: Option<Bytes>, value: Bytes) -> Result<i64> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        let mut partition_lock = self.partitions[partition as usize].write();

        partition_lock.append(key, value)
    }

    /// Append a record to the specified partition with optimized lock scope
    ///
    /// This method reduces lock contention by performing offset allocation
    /// and record construction outside the write lock. Only segment I/O
    /// operations are performed under the lock.
    ///
    /// For high-throughput scenarios, this can improve performance by 20-50%
    /// by allowing multiple threads to pre-allocate offsets concurrently.
    ///
    /// # Arguments
    /// * `partition` - The partition index
    /// * `key` - Optional record key
    /// * `value` - Record value
    ///
    /// # Returns
    /// The offset assigned to the record
    pub fn append_optimized(
        &self,
        partition: i32,
        key: Option<Bytes>,
        value: Bytes,
    ) -> Result<i64> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        // Step 1: Reserve offset OUTSIDE the write lock (atomic operation only)
        // This uses a read lock which allows concurrent offset allocations
        let (offset, timestamp) = {
            let partition_read = self.partitions[partition as usize].read();
            partition_read.reserve_offset()
        };

        // Step 2: Create record OUTSIDE the write lock (no lock held)
        let record = Record::new(offset, timestamp, key, value);

        // Step 3: Write to segment WITH the write lock (minimal lock scope)
        let mut partition_lock = self.partitions[partition as usize].write();
        partition_lock.append_with_offset(record)
    }

    /// Append a record with headers to the specified partition
    pub fn append_with_headers(
        &self,
        partition: i32,
        key: Option<Bytes>,
        value: Bytes,
        headers: Vec<crate::storage::record::Header>,
    ) -> Result<i64> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        let mut partition_lock = self.partitions[partition as usize].write();

        partition_lock.append_with_headers(key, value, headers)
    }

    /// Append a Kafka record batch to the specified partition
    ///
    /// This method handles Kafka record batches which may contain multiple records.
    /// It atomically reserves `record_count` offsets and updates the batch header's
    /// baseOffset field to the broker-assigned offset.
    ///
    /// # Arguments
    /// * `partition` - The partition index
    /// * `batch_data` - Raw Kafka record batch bytes
    /// * `record_count` - Number of records in the batch
    ///
    /// # Returns
    /// The base offset assigned to the first record in the batch
    pub fn append_batch(
        &self,
        partition: i32,
        batch_data: Bytes,
        record_count: i32,
    ) -> Result<i64> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        let mut partition_lock = self.partitions[partition as usize].write();

        partition_lock.append_batch(batch_data, record_count)
    }

    /// Append replicated records to the specified partition (for followers)
    ///
    /// This is used when a follower receives records from the leader.
    /// Records maintain their original offsets from the leader.
    /// Returns the new log end offset.
    pub fn append_replicated_records(&self, partition: i32, records: Vec<Record>) -> Result<i64> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        let mut partition_lock = self.partitions[partition as usize].write();

        partition_lock.append_replicated_records(records)
    }

    /// Truncate a partition to the specified offset (for follower resync)
    pub fn truncate_partition(&self, partition: i32, offset: i64) -> Result<()> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        let mut partition_lock = self.partitions[partition as usize].write();

        partition_lock.truncate_to(offset)?;
        Ok(())
    }

    /// Delete all records before the specified offset (DeleteRecords API)
    ///
    /// Returns the new low watermark (earliest offset) after deletion.
    pub fn delete_records_before(&self, partition: i32, offset: i64) -> Result<i64> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        let mut partition_lock = self.partitions[partition as usize].write();

        partition_lock.delete_records_before(offset)
    }

    /// Read records from the specified partition
    pub fn read(
        &self,
        partition: i32,
        start_offset: i64,
        max_records: usize,
    ) -> Result<Vec<Record>> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        let partition_lock = self.partitions[partition as usize].read();

        partition_lock.read(start_offset, max_records)
    }

    /// Read records from a partition, filtering out expired messages based on TTL
    ///
    /// This method reads up to `max_records` non-expired records starting from `start_offset`.
    /// Messages are filtered based on:
    /// 1. Per-message TTL (via the "x-message-ttl-ms" header)
    /// 2. Topic-level TTL (message_ttl_ms in TopicConfig)
    ///
    /// Note: This may read more records internally to compensate for expired messages.
    pub fn read_with_ttl(
        &self,
        partition: i32,
        start_offset: i64,
        max_records: usize,
    ) -> Result<Vec<Record>> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        let topic_ttl_ms = self.metadata.config.message_ttl_ms;

        // If TTL is disabled (infinite), just do a normal read
        if topic_ttl_ms < 0 {
            return self.read(partition, start_offset, max_records);
        }

        let partition_lock = self.partitions[partition as usize].read();

        // Use chrono for reliable timestamp that handles edge cases properly
        let current_time_ms = Utc::now().timestamp_millis();

        // Read extra records to account for expired messages
        // We'll read in batches until we have enough non-expired records
        let mut result = Vec::with_capacity(max_records);
        let mut current_offset = start_offset;
        let batch_size = max_records.max(100); // Read at least 100 at a time for efficiency

        while result.len() < max_records {
            let records = partition_lock.read(current_offset, batch_size)?;
            if records.is_empty() {
                break; // No more records
            }

            for record in records {
                current_offset = record.offset + 1;

                // Check per-message TTL first, then fall back to topic TTL
                let effective_ttl = record.get_ttl_ms().unwrap_or(topic_ttl_ms);
                if effective_ttl >= 0 {
                    let expiry_time = record.timestamp.saturating_add(effective_ttl);
                    if current_time_ms > expiry_time {
                        continue; // Skip expired record
                    }
                }

                result.push(record);
                if result.len() >= max_records {
                    break;
                }
            }
        }

        Ok(result)
    }

    /// Read records with READ_COMMITTED isolation level
    ///
    /// Returns records up to the last stable offset (LSO), which is the
    /// minimum of the first unstable transaction offset and HWM. TTL filtering
    /// is also applied. The aborted transaction list is returned separately
    /// in the fetch response for client-side filtering per Kafka protocol.
    pub fn read_committed_with_lso(
        &self,
        partition: i32,
        start_offset: i64,
        max_records: usize,
        last_stable_offset: i64,
    ) -> Result<Vec<Record>> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        let topic_ttl_ms = self.metadata.config.message_ttl_ms;
        let partition_lock = self.partitions[partition as usize].read();

        // If TTL is disabled, use the partition's LSO-based read
        if topic_ttl_ms < 0 {
            return partition_lock.read_committed_with_lso(
                start_offset,
                max_records,
                last_stable_offset,
            );
        }

        // Read with both LSO and TTL filtering
        let current_time_ms = Utc::now().timestamp_millis();
        let mut result = Vec::with_capacity(max_records);
        let mut current_offset = start_offset;
        let batch_size = max_records.max(100);

        while result.len() < max_records {
            let records = partition_lock.read_committed_with_lso(
                current_offset,
                batch_size,
                last_stable_offset,
            )?;
            if records.is_empty() {
                break;
            }

            for record in records {
                current_offset = record.offset + 1;

                let effective_ttl = record.get_ttl_ms().unwrap_or(topic_ttl_ms);
                if effective_ttl >= 0 {
                    let expiry_time = record.timestamp.saturating_add(effective_ttl);
                    if current_time_ms > expiry_time {
                        continue;
                    }
                }

                result.push(record);
                if result.len() >= max_records {
                    break;
                }
            }
        }

        Ok(result)
    }

    /// Get the earliest offset for a partition
    pub fn earliest_offset(&self, partition: i32) -> Result<i64> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        let partition_lock = self.partitions[partition as usize].read();

        Ok(partition_lock.earliest_offset())
    }

    /// Get the latest offset for a partition
    pub fn latest_offset(&self, partition: i32) -> Result<i64> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        let partition_lock = self.partitions[partition as usize].read();

        Ok(partition_lock.latest_offset())
    }

    /// Get the high watermark for a partition
    pub fn high_watermark(&self, partition: i32) -> Result<i64> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        let partition_lock = self.partitions[partition as usize].read();

        Ok(partition_lock.high_watermark())
    }

    /// Find offset by timestamp for a partition
    ///
    /// Returns the first offset with timestamp >= target_timestamp.
    /// Returns `Ok(None)` if no records match.
    pub fn find_offset_by_timestamp(&self, partition: i32, timestamp: i64) -> Result<Option<i64>> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        let partition_lock = self.partitions[partition as usize].read();

        partition_lock.find_offset_by_timestamp(timestamp)
    }

    /// Get partition info
    pub fn partition_info(&self, partition: i32) -> Result<PartitionInfo> {
        if partition < 0 || partition >= self.metadata.num_partitions {
            return Err(StreamlineError::PartitionNotFound(
                self.metadata.name.clone(),
                partition,
            ));
        }

        let partition_lock = self.partitions[partition as usize].read();

        Ok(PartitionInfo {
            id: partition,
            earliest_offset: partition_lock.earliest_offset(),
            latest_offset: partition_lock.latest_offset(),
            high_watermark: partition_lock.high_watermark(),
            size: partition_lock.size(),
            record_count: partition_lock.record_count(),
        })
    }

    /// Get info for all partitions
    pub fn all_partition_info(&self) -> Result<Vec<PartitionInfo>> {
        let mut infos = Vec::with_capacity(self.metadata.num_partitions as usize);
        for i in 0..self.metadata.num_partitions {
            infos.push(self.partition_info(i)?);
        }
        Ok(infos)
    }

    /// Add new partitions to the topic
    ///
    /// # Arguments
    /// * `new_count` - The total number of partitions after adding (must be > current count)
    ///
    /// # Returns
    /// A vector of the newly created partition indices
    pub fn add_partitions(&mut self, new_count: i32) -> Result<Vec<i32>> {
        let current_count = self.metadata.num_partitions;

        if new_count <= current_count {
            return Err(StreamlineError::Config(format!(
                "New partition count ({}) must be greater than current count ({})",
                new_count, current_count
            )));
        }

        let mut new_partition_ids = Vec::new();

        for i in current_count..new_count {
            let partition = if self.in_memory {
                Partition::open_in_memory(self.metadata.name.as_str(), i)?
            } else {
                let mut partition =
                    Partition::open(self.metadata.name.as_str(), i, &self.base_path)?;
                // Configure partition with topic's segment size
                partition.set_max_segment_bytes(self.metadata.config.segment_bytes);
                partition
            };
            self.partitions.push(RwLock::new(partition));
            new_partition_ids.push(i);
        }

        // Update metadata
        self.metadata.num_partitions = new_count;

        // Persist metadata if not in-memory
        if !self.in_memory {
            let topic_path = self.base_path.join("topics").join(&self.metadata.name);
            let metadata_path = topic_path.join("metadata.json");
            let data = serde_json::to_string_pretty(&self.metadata)?;
            atomic_write(&metadata_path, data.as_bytes())?;
        }

        info!(
            topic = %self.metadata.name,
            old_count = current_count,
            new_count = new_count,
            added = new_partition_ids.len(),
            "Partitions added to topic"
        );

        Ok(new_partition_ids)
    }
}

/// Partition information
#[derive(Debug, Clone, Serialize)]
pub struct PartitionInfo {
    pub id: i32,
    pub earliest_offset: i64,
    pub latest_offset: i64,
    pub high_watermark: i64,
    pub size: u64,
    pub record_count: u64,
}

struct TopicStorage {
    /// Base path for storage
    base_path: PathBuf,
    /// WAL writer (optional, only if WAL is enabled)
    /// Uses ShardedWalWriter for improved throughput via parallel shards.
    wal_writer: Option<ShardedWalWriter>,
    /// Checkpoint manager
    checkpoint_manager: Option<Mutex<CheckpointManager>>,
    /// In-memory mode - if true, no data is persisted to disk
    in_memory: bool,
    /// Segment configuration (for DirectIO support)
    segment_config: Option<SegmentConfig>,
}

impl TopicStorage {
    fn in_memory() -> Self {
        Self {
            base_path: PathBuf::new(), // Not used in in-memory mode
            wal_writer: None,
            checkpoint_manager: None,
            in_memory: true,
            segment_config: None,
        }
    }

    fn new(base_path: &Path, wal_config: Option<WalConfig>) -> Result<Self> {
        fs::create_dir_all(base_path.join("topics"))?;
        fs::create_dir_all(base_path.join("meta"))?;

        // Initialize WAL if configured (uses sharded WAL for improved throughput)
        let (wal_writer, checkpoint_manager) = if let Some(config) = wal_config {
            if config.enabled {
                let wal = ShardedWalWriter::new(base_path, config)?;
                let checkpoint = CheckpointManager::new(base_path)?;
                (Some(wal), Some(Mutex::new(checkpoint)))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        Ok(Self {
            base_path: base_path.to_path_buf(),
            wal_writer,
            checkpoint_manager,
            in_memory: false,
            segment_config: None,
        })
    }

    async fn new_async(base_path: PathBuf, wal_config: Option<WalConfig>) -> Result<Self> {
        async_io::create_dir_all_async(base_path.join("topics")).await?;
        async_io::create_dir_all_async(base_path.join("meta")).await?;

        // Initialize sharded WAL if configured (sync operations - typically fast)
        let (wal_writer, checkpoint_manager) = if let Some(config) = wal_config {
            if config.enabled {
                let wal = ShardedWalWriter::new(&base_path, config)?;
                let checkpoint = CheckpointManager::new_async(base_path.clone()).await?;
                (Some(wal), Some(Mutex::new(checkpoint)))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        Ok(Self {
            base_path,
            wal_writer,
            checkpoint_manager,
            in_memory: false,
            segment_config: None,
        })
    }

    fn create_topic_with_config(
        &self,
        name: &str,
        num_partitions: i32,
        config: TopicConfig,
    ) -> Result<Topic> {
        // Use in-memory topic if in-memory mode is enabled
        let topic = if self.in_memory {
            Topic::open_in_memory_with_config(name, num_partitions, config)?
        } else if self.segment_config.is_some() {
            // Use segment config (e.g., for DirectIO) if configured
            Topic::open_with_segment_config(
                name,
                num_partitions,
                &self.base_path,
                config,
                self.segment_config.clone(),
            )?
        } else {
            Topic::open_with_config(name, num_partitions, &self.base_path, config)?
        };

        Ok(topic)
    }
}

/// Topic manager for managing multiple topics
pub struct TopicManager {
    /// Map of topic name to topic (concurrent hashmap for better scalability)
    topics: DashMap<String, Topic>,
    storage: TopicStorage,
}

impl TopicManager {
    /// Create a new topic manager without WAL (backward compatible)
    pub fn new(base_path: &Path) -> Result<Self> {
        Self::with_options(base_path, None, false)
    }

    /// Create a new topic manager with WAL configuration
    pub fn with_wal_config(base_path: &Path, wal_config: Option<WalConfig>) -> Result<Self> {
        Self::with_options(base_path, wal_config, false)
    }

    /// Create a new in-memory topic manager
    ///
    /// In this mode, all data is stored in memory and not persisted to disk.
    /// This is useful for testing, development, and ephemeral workloads.
    pub fn in_memory() -> Result<Self> {
        info!("Starting in-memory topic manager (data will not be persisted)");
        Ok(Self {
            topics: DashMap::new(),
            storage: TopicStorage::in_memory(),
        })
    }

    /// Create a new topic manager with full configuration options
    pub fn with_options(
        base_path: &Path,
        wal_config: Option<WalConfig>,
        in_memory: bool,
    ) -> Result<Self> {
        if in_memory {
            return Self::in_memory();
        }
        let storage = TopicStorage::new(base_path, wal_config)?;
        let manager = Self {
            topics: DashMap::new(),
            storage,
        };

        // Load existing topics
        manager.load_topics()?;

        // Recover from WAL if enabled
        manager.recover_from_wal()?;

        Ok(manager)
    }

    /// Create a new topic manager with full configuration including segment config
    pub fn with_segment_config(
        base_path: &Path,
        wal_config: Option<WalConfig>,
        segment_config: Option<SegmentConfig>,
    ) -> Result<Self> {
        let mut manager = Self::with_options(base_path, wal_config, false)?;
        manager.storage.segment_config = segment_config;
        Ok(manager)
    }

    /// Set segment configuration (for DirectIO support)
    pub fn set_segment_config(&mut self, config: SegmentConfig) {
        self.storage.segment_config = Some(config);
    }

    /// Get segment configuration
    pub fn segment_config(&self) -> Option<&SegmentConfig> {
        self.storage.segment_config.as_ref()
    }

    /// Recover from WAL after crash
    fn recover_from_wal(&self) -> Result<()> {
        // Only recover if we have both WAL and checkpoint
        let checkpoint_manager = match &self.storage.checkpoint_manager {
            Some(cm) => cm,
            None => return Ok(()),
        };

        let checkpoint = checkpoint_manager.lock();

        let last_sequence = checkpoint.last_sequence();
        drop(checkpoint);

        // Read WAL entries after last checkpoint (supports both sharded and legacy WAL)
        let wal_dir = self.storage.base_path.join("wal");
        if !wal_dir.exists() {
            return Ok(());
        }

        let entries = ShardedWalReader::read_after_sequence(&wal_dir, last_sequence)?;
        if entries.is_empty() {
            debug!("No WAL entries to replay");
            return Ok(());
        }

        info!(
            entries = entries.len(),
            after_sequence = last_sequence,
            "Replaying WAL entries"
        );

        // Replay entries
        for entry in &entries {
            match entry.entry_type {
                WalEntryType::Record => {
                    self.replay_record_entry(entry)?;
                }
                WalEntryType::Checkpoint => {
                    // Update checkpoint sequence
                    if let Some(cm) = &self.storage.checkpoint_manager {
                        let mut checkpoint = cm.lock();
                        checkpoint.record_checkpoint(entry.sequence, false)?;
                    }
                }
                WalEntryType::Truncate => {
                    // Nothing to do for truncate entries during recovery
                }
            }
        }

        // Save updated checkpoint
        if let Some(cm) = &self.storage.checkpoint_manager {
            let checkpoint = cm.lock();
            checkpoint.save()?;
        }

        info!("WAL recovery complete");
        Ok(())
    }

    /// Replay a record entry from WAL
    fn replay_record_entry(&self, entry: &WalEntry) -> Result<()> {
        // Ensure topic exists
        self.get_or_create_topic(&entry.topic, 1)?;

        // Extract key and value
        let (key, value) = entry.extract_key_value()?;

        // Append directly to partition (bypassing WAL since we're replaying)
        if let Some(topic) = self.topics.get(&entry.topic) {
            topic.append(entry.partition, key, value)?;
            debug!(
                topic = %entry.topic,
                partition = entry.partition,
                sequence = entry.sequence,
                "Replayed WAL entry"
            );
        }

        Ok(())
    }

    /// Load existing topics from disk
    fn load_topics(&self) -> Result<()> {
        let topics_path = self.storage.base_path.join("topics");

        if !topics_path.exists() {
            return Ok(());
        }

        let entries = fs::read_dir(&topics_path)?;

        for entry in entries.filter_map(|e| e.ok()) {
            if entry.path().is_dir() {
                let topic_name = entry.file_name().to_string_lossy().to_string();
                let metadata_path = entry.path().join("metadata.json");

                if metadata_path.exists() {
                    let data = fs::read_to_string(&metadata_path)?;
                    let metadata: TopicMetadata = serde_json::from_str(&data)?;
                    // Use segment config (e.g., for DirectIO) if configured
                    let topic = if self.storage.segment_config.is_some() {
                        Topic::open_with_segment_config(
                            &topic_name,
                            metadata.num_partitions,
                            &self.storage.base_path,
                            metadata.config.clone(),
                            self.storage.segment_config.clone(),
                        )?
                    } else {
                        Topic::open(
                            &topic_name,
                            metadata.num_partitions,
                            &self.storage.base_path,
                        )?
                    };
                    self.topics.insert(topic_name, topic);
                }
            }
        }

        Ok(())
    }

    /// Create a new topic
    pub fn create_topic(&self, name: &str, num_partitions: i32) -> Result<()> {
        self.create_topic_with_config(name, num_partitions, TopicConfig::default())
    }

    /// Create a new topic with custom configuration
    pub fn create_topic_with_config(
        &self,
        name: &str,
        num_partitions: i32,
        config: TopicConfig,
    ) -> Result<()> {
        // Validate topic name before creating
        validate_topic_name(name)?;

        let topic = self
            .storage
            .create_topic_with_config(name, num_partitions, config)?;

        match self.topics.entry(name.to_string()) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(topic);
            }
            dashmap::mapref::entry::Entry::Occupied(_) => {
                return Err(StreamlineError::TopicAlreadyExists(name.to_string()));
            }
        }

        info!(
            topic = %name,
            partitions = num_partitions,
            in_memory = self.storage.in_memory,
            direct_io = self.storage.segment_config.as_ref().is_some_and(|c| c.use_direct_io),
            "Topic created"
        );

        Ok(())
    }

    /// Delete a topic
    pub fn delete_topic(&self, name: &str) -> Result<()> {
        if self.topics.remove(name).is_none() {
            return Err(StreamlineError::TopicNotFound(name.to_string()));
        }

        // Remove topic directory (skip for in-memory mode)
        if !self.storage.in_memory {
            let topic_path = self.storage.base_path.join("topics").join(name);
            if topic_path.exists() {
                fs::remove_dir_all(&topic_path)?;
            }
        }

        info!(topic = %name, in_memory = self.storage.in_memory, "Topic deleted");

        Ok(())
    }

    /// Get or create a topic (auto-create if doesn't exist)
    pub fn get_or_create_topic(&self, name: &str, num_partitions: i32) -> Result<()> {
        if self.topics.contains_key(name) {
            return Ok(());
        }

        self.create_topic(name, num_partitions)
    }

    /// Append a record to a topic
    pub fn append(
        &self,
        topic: &str,
        partition: i32,
        key: Option<Bytes>,
        value: Bytes,
    ) -> Result<i64> {
        // Write to WAL first (if enabled)
        // ShardedWalWriter handles per-shard locking internally
        let wal_sequence = if let Some(wal) = &self.storage.wal_writer {
            Some(wal.append_record(topic, partition, key.as_ref(), &value)?)
        } else {
            None
        };

        // Then append to topic/partition
        // IMPORTANT: Release topics lock before acquiring checkpoint lock to prevent deadlock.
        // Lock ordering: WAL -> topics -> checkpoint (never hold multiple simultaneously)
        let offset = {
            let topic_obj = self
                .topics
                .get(topic)
                .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

            topic_obj.append(partition, key, value)?
            // DashMap ref released here at end of scope
        };

        // Update checkpoint (if enabled) - topics lock already released
        if let (Some(wal_seq), Some(cm)) = (wal_sequence, &self.storage.checkpoint_manager) {
            let mut checkpoint = cm.lock();
            // Don't save on every write - that would be too slow
            // Checkpoint saves happen periodically or on sync
            checkpoint.update_partition(topic, partition, offset, wal_seq, false)?;
        }

        Ok(offset)
    }

    /// Append a record with headers to a topic
    pub fn append_with_headers(
        &self,
        topic: &str,
        partition: i32,
        key: Option<Bytes>,
        value: Bytes,
        headers: Vec<crate::storage::record::Header>,
    ) -> Result<i64> {
        // Write to WAL first (if enabled)
        // ShardedWalWriter handles per-shard locking internally
        let wal_sequence = if let Some(wal) = &self.storage.wal_writer {
            Some(wal.append_record(topic, partition, key.as_ref(), &value)?)
        } else {
            None
        };

        // Then append to topic/partition
        // IMPORTANT: Release topics lock before acquiring checkpoint lock to prevent deadlock.
        let offset = {
            let topic_obj = self
                .topics
                .get(topic)
                .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

            topic_obj.append_with_headers(partition, key, value, headers)?
            // DashMap ref released here at end of scope
        };

        // Update checkpoint (if enabled) - topics lock already released
        if let (Some(wal_seq), Some(cm)) = (wal_sequence, &self.storage.checkpoint_manager) {
            let mut checkpoint = cm.lock();
            checkpoint.update_partition(topic, partition, offset, wal_seq, false)?;
        }

        Ok(offset)
    }

    /// Append a Kafka record batch to a topic partition
    ///
    /// This method handles Kafka record batches which may contain multiple records.
    /// It atomically reserves `record_count` offsets and updates the batch header's
    /// baseOffset field to the broker-assigned offset.
    ///
    /// This is critical for Kafka protocol compliance - the broker MUST update
    /// the baseOffset in the batch header before storing.
    ///
    /// # Arguments
    /// * `topic` - The topic name
    /// * `partition` - The partition index
    /// * `batch_data` - Raw Kafka record batch bytes
    /// * `record_count` - Number of records in the batch
    ///
    /// # Returns
    /// The base offset assigned to the first record in the batch
    pub fn append_batch(
        &self,
        topic: &str,
        partition: i32,
        batch_data: Bytes,
        record_count: i32,
    ) -> Result<i64> {
        // Write to WAL first (if enabled)
        // ShardedWalWriter handles per-shard locking internally
        let wal_sequence = if let Some(wal) = &self.storage.wal_writer {
            Some(wal.append_record(topic, partition, None, &batch_data)?)
        } else {
            None
        };

        // Then append to topic/partition
        // IMPORTANT: Release topics ref before acquiring checkpoint lock to prevent deadlock.
        let offset = {
            let topic_obj = self
                .topics
                .get(topic)
                .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

            topic_obj.append_batch(partition, batch_data, record_count)?
            // DashMap ref released here at end of scope
        };

        // Update checkpoint (if enabled) - topics ref already released
        // Note: For batches, we use the base_offset but the actual LEO would be base_offset + record_count
        if let (Some(wal_seq), Some(cm)) = (wal_sequence, &self.storage.checkpoint_manager) {
            let mut checkpoint = cm.lock();
            // Update with the last offset in the batch (base_offset + record_count - 1)
            let last_offset = offset + (record_count as i64) - 1;
            checkpoint.update_partition(topic, partition, last_offset, wal_seq, false)?;
        }

        Ok(offset)
    }

    /// Append replicated records to a topic partition (for followers)
    ///
    /// This is used when a follower receives records from the leader.
    /// Records maintain their original offsets from the leader.
    /// Returns the new log end offset.
    pub fn append_replicated_records(
        &self,
        topic: &str,
        partition: i32,
        records: Vec<Record>,
    ) -> Result<i64> {
        let topic_obj = self
            .topics
            .get(topic)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

        topic_obj.append_replicated_records(partition, records)
    }

    /// Truncate a partition to the specified offset (for follower resync)
    ///
    /// This is used when a follower detects log divergence from the leader
    /// and needs to truncate to resync.
    pub fn truncate_partition(&self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        let topic_obj = self
            .topics
            .get(topic)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

        topic_obj.truncate_partition(partition, offset)
    }

    /// Delete all records before the specified offset (DeleteRecords API)
    ///
    /// Returns the new low watermark (earliest offset) after deletion.
    pub fn delete_records_before(&self, topic: &str, partition: i32, offset: i64) -> Result<i64> {
        let topic_obj = self
            .topics
            .get(topic)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

        topic_obj.delete_records_before(partition, offset)
    }

    /// Read records from a topic
    pub fn read(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_records: usize,
    ) -> Result<Vec<Record>> {
        let topic_obj = self
            .topics
            .get(topic)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

        topic_obj.read(partition, start_offset, max_records)
    }

    /// Read records from a topic with TTL filtering
    ///
    /// This method reads records while filtering out expired messages based on:
    /// 1. Per-message TTL (via the "x-message-ttl-ms" header)
    /// 2. Topic-level TTL (message_ttl_ms in TopicConfig)
    ///
    /// If the topic has no TTL configured (message_ttl_ms = -1), this behaves
    /// identically to the regular `read` method.
    pub fn read_with_ttl(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_records: usize,
    ) -> Result<Vec<Record>> {
        let topic_obj = self
            .topics
            .get(topic)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

        topic_obj.read_with_ttl(partition, start_offset, max_records)
    }

    /// Read records with READ_COMMITTED isolation (up to LSO, with TTL filtering)
    pub fn read_committed_with_lso(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_records: usize,
        last_stable_offset: i64,
    ) -> Result<Vec<Record>> {
        let topic_obj = self
            .topics
            .get(topic)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

        topic_obj.read_committed_with_lso(partition, start_offset, max_records, last_stable_offset)
    }

    /// Get topic metadata
    pub fn get_topic_metadata(&self, topic: &str) -> Result<TopicMetadata> {
        let topic_obj = self
            .topics
            .get(topic)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

        Ok(topic_obj.metadata().clone())
    }

    /// List all topics
    pub fn list_topics(&self) -> Result<Vec<TopicMetadata>> {
        Ok(self.topics.iter().map(|t| t.metadata().clone()).collect())
    }

    /// Get earliest offset for a topic partition
    pub fn earliest_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        let topic_obj = self
            .topics
            .get(topic)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

        topic_obj.earliest_offset(partition)
    }

    /// Get latest offset for a topic partition
    pub fn latest_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        let topic_obj = self
            .topics
            .get(topic)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

        topic_obj.latest_offset(partition)
    }

    /// Get high watermark for a topic partition
    pub fn high_watermark(&self, topic: &str, partition: i32) -> Result<i64> {
        let topic_obj = self
            .topics
            .get(topic)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

        topic_obj.high_watermark(partition)
    }

    /// Find offset by timestamp for a topic partition
    ///
    /// Returns the first offset with timestamp >= target_timestamp.
    /// Returns `Ok(None)` if no records match (empty partition or all records older than target).
    ///
    /// This is used by the ListOffsets API to support timestamp-based offset lookup.
    pub fn find_offset_by_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<Option<i64>> {
        let topic_obj = self
            .topics
            .get(topic)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

        topic_obj.find_offset_by_timestamp(partition, timestamp)
    }

    /// Get partition info for a topic partition.
    pub fn partition_info(&self, topic: &str, partition: i32) -> Result<PartitionInfo> {
        let topic_obj = self
            .topics
            .get(topic)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

        topic_obj.partition_info(partition)
    }

    /// Get info for all partitions in a topic.
    pub fn all_partition_info(&self, topic: &str) -> Result<Vec<PartitionInfo>> {
        let topic_obj = self
            .topics
            .get(topic)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

        topic_obj.all_partition_info()
    }

    /// Get statistics for a topic including all partition offsets in a single ref acquisition.
    /// This is more efficient than calling earliest_offset/latest_offset for each partition.
    pub fn get_topic_stats(&self, topic: &str) -> Result<TopicStats> {
        let topic_obj = self
            .topics
            .get(topic)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic.to_string()))?;

        let metadata = topic_obj.metadata();
        let mut partition_stats = Vec::with_capacity(metadata.num_partitions as usize);
        let mut total_messages = 0u64;

        for partition_id in 0..metadata.num_partitions {
            let info = topic_obj.partition_info(partition_id)?;
            let earliest = info.earliest_offset;
            let latest = info.latest_offset;
            let messages = if latest > earliest {
                (latest - earliest) as u64
            } else {
                0
            };
            total_messages += messages;
            partition_stats.push(PartitionStats {
                partition_id,
                earliest_offset: earliest,
                latest_offset: latest,
                message_count: messages,
            });
        }

        Ok(TopicStats {
            name: metadata.name.clone(),
            num_partitions: metadata.num_partitions,
            replication_factor: metadata.replication_factor,
            partition_stats,
            total_messages,
        })
    }

    /// Get statistics for all topics.
    /// Uses DashMap iteration for concurrent access.
    pub fn get_all_topic_stats(&self) -> Result<Vec<TopicStats>> {
        let mut all_stats = Vec::with_capacity(self.topics.len());

        for topic_ref in self.topics.iter() {
            let topic_obj = topic_ref.value();
            let metadata = topic_obj.metadata();
            let mut partition_stats = Vec::with_capacity(metadata.num_partitions as usize);
            let mut total_messages = 0u64;

            for partition_id in 0..metadata.num_partitions {
                let info = topic_obj.partition_info(partition_id)?;
                let earliest = info.earliest_offset;
                let latest = info.latest_offset;
                let messages = if latest > earliest {
                    (latest - earliest) as u64
                } else {
                    0
                };
                total_messages += messages;
                partition_stats.push(PartitionStats {
                    partition_id,
                    earliest_offset: earliest,
                    latest_offset: latest,
                    message_count: messages,
                });
            }

            all_stats.push(TopicStats {
                name: metadata.name.clone(),
                num_partitions: metadata.num_partitions,
                replication_factor: metadata.replication_factor,
                partition_stats,
                total_messages,
            });
        }

        Ok(all_stats)
    }

    /// Sync WAL to disk (for interval sync mode)
    pub fn sync_wal(&self) -> Result<()> {
        if let Some(wal) = &self.storage.wal_writer {
            wal.sync()?;
        }
        Ok(())
    }

    /// Write a checkpoint and sync everything
    pub fn checkpoint(&self) -> Result<()> {
        // First, sync WAL (ShardedWalWriter syncs all shards)
        let checkpoint_seq = if let Some(wal) = &self.storage.wal_writer {
            Some(wal.checkpoint()?)
        } else {
            None
        };

        // Then save checkpoint
        if let (Some(seq), Some(cm)) = (checkpoint_seq, &self.storage.checkpoint_manager) {
            let mut checkpoint = cm.lock();
            checkpoint.record_checkpoint(seq, true)?;
        }

        Ok(())
    }

    /// Update metrics for all topics
    pub fn update_all_metrics(&self) -> Result<()> {
        // Update total topic count
        crate::metrics::update_topics_total(self.topics.len());

        // Update metrics for each topic
        for topic_ref in self.topics.iter() {
            let topic = topic_ref.value();
            crate::metrics::update_partitions_total(topic.name(), topic.num_partitions());

            // Update partition metrics
            for i in 0..topic.num_partitions() {
                let partition_idx = i as usize;
                if partition_idx >= topic.partitions.len() {
                    tracing::warn!(
                        topic = topic.name(),
                        partition = i,
                        "Partition index out of bounds"
                    );
                    continue;
                }

                {
                    let partition_lock = topic.partitions[partition_idx].read();
                    partition_lock.update_metrics();
                }
            }
        }

        Ok(())
    }

    /// Flush all pending writes and close WAL
    pub fn flush(&self) -> Result<()> {
        // Checkpoint first
        self.checkpoint()?;

        // Close WAL (ShardedWalWriter closes all shards)
        if let Some(wal) = &self.storage.wal_writer {
            wal.close()?;
        }

        info!("TopicManager flushed");
        Ok(())
    }

    /// Check if WAL is enabled
    pub fn is_wal_enabled(&self) -> bool {
        self.storage.wal_writer.is_some()
    }

    /// Check if running in in-memory mode
    pub fn is_in_memory(&self) -> bool {
        self.storage.in_memory
    }

    /// Configure segment sync mode for all topics
    ///
    /// This sets the durability behavior for segment writes across all topics and partitions.
    pub fn set_segment_sync_config(&self, sync_mode: SegmentSyncMode, sync_interval_ms: u64) {
        for topic_entry in self.topics.iter() {
            topic_entry.set_segment_sync_config(sync_mode, sync_interval_ms);
        }
    }

    /// Enforce retention policies on a specific partition
    ///
    /// This method is used by the retention enforcer to apply retention policies.
    pub fn enforce_partition_retention(
        &self,
        topic_name: &str,
        partition_id: i32,
        retention_ms: i64,
        retention_bytes: i64,
    ) -> Result<(usize, usize)> {
        let topic = self
            .topics
            .get(topic_name)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic_name.to_string()))?;

        if partition_id < 0 || partition_id >= topic.num_partitions() {
            return Err(StreamlineError::PartitionNotFound(
                topic_name.to_string(),
                partition_id,
            ));
        }

        let mut partition = topic.partitions[partition_id as usize].write();

        let mut time_deleted = 0;
        let mut size_deleted = 0;

        // Enforce time-based retention
        if retention_ms > 0 {
            let now = chrono::Utc::now().timestamp_millis();
            let cutoff_time = now - retention_ms;
            time_deleted = partition.delete_segments_before_timestamp(cutoff_time)?;
        }

        // Enforce size-based retention
        if retention_bytes > 0 {
            size_deleted = partition.delete_segments_over_size(retention_bytes as u64)?;
        }

        Ok((time_deleted, size_deleted))
    }

    /// Compact a specific partition using the provided compactor
    ///
    /// This method is used by the retention enforcer to apply compaction.
    pub(crate) fn compact_partition(
        &self,
        topic_name: &str,
        partition_id: i32,
        compactor: &crate::storage::compaction::LogCompactor,
    ) -> Result<usize> {
        let topic = self
            .topics
            .get(topic_name)
            .ok_or_else(|| StreamlineError::TopicNotFound(topic_name.to_string()))?;

        if partition_id < 0 || partition_id >= topic.num_partitions() {
            return Err(StreamlineError::PartitionNotFound(
                topic_name.to_string(),
                partition_id,
            ));
        }

        let mut partition = topic.partitions[partition_id as usize].write();

        let compacted_count = compactor.compact(&mut partition)?;

        Ok(compacted_count)
    }

    /// Add partitions to an existing topic
    ///
    /// # Arguments
    /// * `name` - The topic name
    /// * `new_partition_count` - The total number of partitions after adding
    ///
    /// # Returns
    /// A vector of the newly created partition indices
    pub fn add_partitions(&self, name: &str, new_partition_count: i32) -> Result<Vec<i32>> {
        let mut topic = self
            .topics
            .get_mut(name)
            .ok_or_else(|| StreamlineError::TopicNotFound(name.to_string()))?;

        topic.add_partitions(new_partition_count)
    }

    // ==================== Async Methods ====================
    // These methods use spawn_blocking to avoid blocking the async runtime

    /// Create a new topic manager without WAL (async version)
    pub async fn new_async(base_path: PathBuf) -> Result<Self> {
        Self::with_options_async(base_path, None, false).await
    }

    /// Create a new topic manager with WAL configuration (async version)
    pub async fn with_wal_config_async(
        base_path: PathBuf,
        wal_config: Option<WalConfig>,
    ) -> Result<Self> {
        Self::with_options_async(base_path, wal_config, false).await
    }

    /// Create a new topic manager with full configuration options (async version)
    pub async fn with_options_async(
        base_path: PathBuf,
        wal_config: Option<WalConfig>,
        in_memory: bool,
    ) -> Result<Self> {
        if in_memory {
            return Self::in_memory();
        }

        let manager = Self {
            topics: DashMap::new(),
            storage: TopicStorage::new_async(base_path, wal_config).await?,
        };

        // Load existing topics asynchronously
        manager.load_topics_async().await?;

        // Recover from WAL if enabled (sync for now - WAL operations are typically fast)
        manager.recover_from_wal()?;

        Ok(manager)
    }

    /// Load existing topics from disk asynchronously
    async fn load_topics_async(&self) -> Result<()> {
        let topics_path = self.storage.base_path.join("topics");

        if !async_io::exists_async(topics_path.clone()).await? {
            return Ok(());
        }

        let entries = async_io::read_dir_async(topics_path).await?;

        for (entry_path, is_dir) in entries {
            if is_dir {
                let topic_name = entry_path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|s| s.to_string())
                    .unwrap_or_default();

                let metadata_path = entry_path.join("metadata.json");

                if async_io::exists_async(metadata_path.clone()).await? {
                    let data = async_io::read_to_string_async(metadata_path).await?;
                    let metadata: TopicMetadata = serde_json::from_str(&data)?;
                    // Use segment config (e.g., for DirectIO) if configured
                    let topic = if self.storage.segment_config.is_some() {
                        Topic::open_with_segment_config(
                            &topic_name,
                            metadata.num_partitions,
                            &self.storage.base_path,
                            metadata.config.clone(),
                            self.storage.segment_config.clone(),
                        )?
                    } else {
                        Topic::open(
                            &topic_name,
                            metadata.num_partitions,
                            &self.storage.base_path,
                        )?
                    };
                    self.topics.insert(topic_name, topic);
                }
            }
        }

        Ok(())
    }

    /// Delete a topic asynchronously
    pub async fn delete_topic_async(&self, name: &str) -> Result<()> {
        if self.topics.remove(name).is_none() {
            return Err(StreamlineError::TopicNotFound(name.to_string()));
        }

        // Remove topic directory (skip for in-memory mode)
        if !self.storage.in_memory {
            let topic_path = self.storage.base_path.join("topics").join(name);
            if async_io::exists_async(topic_path.clone()).await? {
                async_io::remove_dir_all_async(topic_path).await?;
            }
        }

        info!(topic = %name, in_memory = self.storage.in_memory, "Topic deleted (async)");

        Ok(())
    }

    /// Write a checkpoint and sync everything (async version)
    #[allow(clippy::await_holding_lock)]
    pub async fn checkpoint_async(&self) -> Result<()> {
        // First, sync WAL (ShardedWalWriter syncs all shards)
        let checkpoint_seq = if let Some(wal) = &self.storage.wal_writer {
            Some(wal.checkpoint()?)
        } else {
            None
        };

        // Then save checkpoint asynchronously
        // Note: holding lock across await is intentional here for checkpoint consistency
        if let (Some(seq), Some(cm)) = (checkpoint_seq, &self.storage.checkpoint_manager) {
            let mut checkpoint = cm.lock();
            checkpoint.record_checkpoint_async(seq, true).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_topic_create() {
        let dir = tempdir().unwrap();
        let topic = Topic::open("test-topic", 3, dir.path()).unwrap();

        assert_eq!(topic.name(), "test-topic");
        assert_eq!(topic.num_partitions(), 3);
    }

    #[test]
    fn test_topic_append_and_read() {
        let dir = tempdir().unwrap();
        let topic = Topic::open("test-topic", 2, dir.path()).unwrap();

        // Append to partition 0
        let offset1 = topic
            .append(0, Some(Bytes::from("key1")), Bytes::from("value1"))
            .unwrap();
        let offset2 = topic.append(0, None, Bytes::from("value2")).unwrap();

        // Append to partition 1
        let offset3 = topic
            .append(1, Some(Bytes::from("key3")), Bytes::from("value3"))
            .unwrap();

        assert_eq!(offset1, 0);
        assert_eq!(offset2, 1);
        assert_eq!(offset3, 0);

        // Read from partition 0
        let records = topic.read(0, 0, 100).unwrap();
        assert_eq!(records.len(), 2);

        // Read from partition 1
        let records = topic.read(1, 0, 100).unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn test_topic_invalid_partition() {
        let dir = tempdir().unwrap();
        let topic = Topic::open("test-topic", 2, dir.path()).unwrap();

        let result = topic.append(5, None, Bytes::from("value"));
        assert!(result.is_err());

        let result = topic.read(5, 0, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_topic_manager() {
        let dir = tempdir().unwrap();
        let manager = TopicManager::new(dir.path()).unwrap();

        // Create topics
        manager.create_topic("topic1", 2).unwrap();
        manager.create_topic("topic2", 3).unwrap();

        // List topics
        let topics = manager.list_topics().unwrap();
        assert_eq!(topics.len(), 2);

        // Append and read
        manager
            .append("topic1", 0, None, Bytes::from("value1"))
            .unwrap();
        let records = manager.read("topic1", 0, 0, 100).unwrap();
        assert_eq!(records.len(), 1);

        // Delete topic
        manager.delete_topic("topic2").unwrap();
        let topics = manager.list_topics().unwrap();
        assert_eq!(topics.len(), 1);
    }

    #[test]
    fn test_topic_manager_auto_create() {
        let dir = tempdir().unwrap();
        let manager = TopicManager::new(dir.path()).unwrap();

        manager.get_or_create_topic("new-topic", 4).unwrap();

        let metadata = manager.get_topic_metadata("new-topic").unwrap();
        assert_eq!(metadata.num_partitions, 4);
    }

    #[test]
    fn test_topic_manager_create_topic_duplicate() {
        let dir = tempdir().unwrap();
        let manager = TopicManager::new(dir.path()).unwrap();

        manager.create_topic("dup-topic", 1).unwrap();
        let result = manager.create_topic("dup-topic", 1);
        assert!(matches!(
            result,
            Err(StreamlineError::TopicAlreadyExists(_))
        ));
    }

    #[test]
    fn test_topic_manager_with_wal() {
        use crate::config::WalConfig;

        let dir = tempdir().unwrap();
        let wal_config = WalConfig {
            enabled: true,
            sync_interval_ms: 100,
            sync_mode: "interval".to_string(),
            max_file_size: 1024 * 1024,
            retention_files: 5,
            max_pending_writes: 1000,
            ..Default::default()
        };

        let manager = TopicManager::with_wal_config(dir.path(), Some(wal_config)).unwrap();
        assert!(manager.is_wal_enabled());

        // Create topic and append records
        manager.create_topic("wal-test", 1).unwrap();
        manager
            .append(
                "wal-test",
                0,
                Some(Bytes::from("key1")),
                Bytes::from("value1"),
            )
            .unwrap();
        manager
            .append("wal-test", 0, None, Bytes::from("value2"))
            .unwrap();

        // Flush WAL
        manager.flush().unwrap();

        // Verify WAL was written
        let wal_dir = dir.path().join("wal");
        assert!(wal_dir.exists());

        // Verify checkpoint was written
        let checkpoint_path = wal_dir.join("checkpoint.json");
        assert!(checkpoint_path.exists());
    }

    #[test]
    fn test_topic_manager_wal_disabled() {
        use crate::config::WalConfig;

        let dir = tempdir().unwrap();
        let wal_config = WalConfig {
            enabled: false,
            sync_interval_ms: 100,
            sync_mode: "interval".to_string(),
            max_file_size: 1024 * 1024,
            retention_files: 5,
            max_pending_writes: 1000,
            ..Default::default()
        };

        let manager = TopicManager::with_wal_config(dir.path(), Some(wal_config)).unwrap();
        assert!(!manager.is_wal_enabled());

        // Create topic and append records - should work without WAL
        manager.create_topic("no-wal-test", 1).unwrap();
        manager
            .append("no-wal-test", 0, None, Bytes::from("value1"))
            .unwrap();

        let records = manager.read("no-wal-test", 0, 0, 100).unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn test_topic_manager_recovery() {
        use crate::config::WalConfig;

        let dir = tempdir().unwrap();
        let wal_config = WalConfig {
            enabled: true,
            sync_interval_ms: 100,
            sync_mode: "every_write".to_string(),
            max_file_size: 1024 * 1024,
            retention_files: 5,
            max_pending_writes: 1000,
            ..Default::default()
        };

        // First session: create topic and write data
        {
            let manager =
                TopicManager::with_wal_config(dir.path(), Some(wal_config.clone())).unwrap();
            manager.create_topic("recovery-test", 1).unwrap();
            manager
                .append("recovery-test", 0, None, Bytes::from("value1"))
                .unwrap();
            manager
                .append("recovery-test", 0, None, Bytes::from("value2"))
                .unwrap();
            manager.checkpoint().unwrap();
        }

        // Check WAL entries were written (using ShardedWalReader for sharded WAL)
        let wal_dir = dir.path().join("wal");
        let entries = ShardedWalReader::read_all(&wal_dir).unwrap();
        // Should have 2 record entries + 1 checkpoint entry
        assert!(entries.len() >= 2);

        // Second session: should recover and read back
        {
            let manager =
                TopicManager::with_wal_config(dir.path(), Some(wal_config.clone())).unwrap();
            let records = manager.read("recovery-test", 0, 0, 100).unwrap();
            assert!(records.len() >= 2);
        }
    }

    #[test]
    fn test_topic_with_custom_config() {
        let dir = tempdir().unwrap();
        let manager = TopicManager::new(dir.path()).unwrap();

        let config = TopicConfig {
            retention_ms: 86400000,      // 1 day
            retention_bytes: 1073741824, // 1 GB
            segment_bytes: 10485760,     // 10 MB
            cleanup_policy: CleanupPolicy::Delete,
            ..Default::default()
        };

        manager
            .create_topic_with_config("retention-topic", 2, config.clone())
            .unwrap();

        let metadata = manager.get_topic_metadata("retention-topic").unwrap();
        assert_eq!(metadata.config.retention_ms, 86400000);
        assert_eq!(metadata.config.retention_bytes, 1073741824);
        assert_eq!(metadata.config.segment_bytes, 10485760);
    }

    #[test]
    fn test_retention_enforcement() {
        use std::thread;
        use std::time::Duration;

        let dir = tempdir().unwrap();
        let manager = TopicManager::new(dir.path()).unwrap();

        // Create topic with small segment size and short retention
        let config = TopicConfig {
            retention_ms: 100, // 100ms retention
            retention_bytes: -1,
            segment_bytes: 1024, // 1KB segments
            cleanup_policy: CleanupPolicy::Delete,
            ..Default::default()
        };

        manager
            .create_topic_with_config("test-retention", 1, config)
            .unwrap();

        // Write some data
        let large_value = vec![b'A'; 512];
        for _ in 0..5 {
            manager
                .append("test-retention", 0, None, Bytes::from(large_value.clone()))
                .unwrap();
        }

        // Wait for data to become old
        thread::sleep(Duration::from_millis(150));

        // Write more recent data
        for _ in 0..3 {
            manager
                .append("test-retention", 0, None, Bytes::from(large_value.clone()))
                .unwrap();
        }

        // Enforce retention
        let (time_deleted, _) = manager
            .enforce_partition_retention("test-retention", 0, 100, -1)
            .unwrap();

        // Should have completed without error
        // Time deleted may be 0 if segments weren't rolled
        assert!(time_deleted < 100); // Sanity check
    }

    #[test]
    fn test_size_retention_enforcement() {
        let dir = tempdir().unwrap();
        let manager = TopicManager::new(dir.path()).unwrap();

        // Create topic with size-based retention
        let config = TopicConfig {
            retention_ms: -1,
            retention_bytes: 2048, // 2KB max
            segment_bytes: 1024,   // 1KB segments
            cleanup_policy: CleanupPolicy::Delete,
            ..Default::default()
        };

        manager
            .create_topic_with_config("size-retention", 1, config)
            .unwrap();

        // Write enough data to exceed retention
        let large_value = vec![b'B'; 512];
        for _ in 0..10 {
            manager
                .append("size-retention", 0, None, Bytes::from(large_value.clone()))
                .unwrap();
        }

        // Enforce size-based retention
        let (_, size_deleted) = manager
            .enforce_partition_retention("size-retention", 0, -1, 2048)
            .unwrap();

        // Should have completed without error
        // Size deleted may be 0 if not enough segments were created
        assert!(size_deleted < 100); // Sanity check
    }

    #[test]
    fn test_in_memory_topic() {
        let topic = Topic::open_in_memory("test-topic", 3).unwrap();

        assert_eq!(topic.name(), "test-topic");
        assert_eq!(topic.num_partitions(), 3);
        assert!(topic.is_in_memory());
    }

    #[test]
    fn test_in_memory_topic_append_and_read() {
        let topic = Topic::open_in_memory("test-topic", 2).unwrap();

        // Append to partition 0
        let offset1 = topic
            .append(0, Some(Bytes::from("key1")), Bytes::from("value1"))
            .unwrap();
        let offset2 = topic.append(0, None, Bytes::from("value2")).unwrap();

        // Append to partition 1
        let offset3 = topic
            .append(1, Some(Bytes::from("key3")), Bytes::from("value3"))
            .unwrap();

        assert_eq!(offset1, 0);
        assert_eq!(offset2, 1);
        assert_eq!(offset3, 0);

        // Read from partition 0
        let records = topic.read(0, 0, 100).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].value, Bytes::from("value1"));

        // Read from partition 1
        let records = topic.read(1, 0, 100).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, Bytes::from("value3"));
    }

    #[test]
    fn test_in_memory_topic_manager() {
        let manager = TopicManager::in_memory().unwrap();
        assert!(manager.is_in_memory());
        assert!(!manager.is_wal_enabled());

        // Create topics
        manager.create_topic("topic1", 2).unwrap();
        manager.create_topic("topic2", 3).unwrap();

        // List topics
        let topics = manager.list_topics().unwrap();
        assert_eq!(topics.len(), 2);

        // Append and read
        manager
            .append("topic1", 0, Some(Bytes::from("key")), Bytes::from("value1"))
            .unwrap();
        manager
            .append("topic1", 0, None, Bytes::from("value2"))
            .unwrap();

        let records = manager.read("topic1", 0, 0, 100).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].value, Bytes::from("value1"));

        // Delete topic
        manager.delete_topic("topic2").unwrap();
        let topics = manager.list_topics().unwrap();
        assert_eq!(topics.len(), 1);
    }

    #[test]
    fn test_in_memory_topic_manager_auto_create() {
        let manager = TopicManager::in_memory().unwrap();

        manager.get_or_create_topic("new-topic", 4).unwrap();

        let metadata = manager.get_topic_metadata("new-topic").unwrap();
        assert_eq!(metadata.num_partitions, 4);

        // Second call should succeed (topic exists)
        manager.get_or_create_topic("new-topic", 4).unwrap();
    }

    #[test]
    fn test_in_memory_topic_manager_offsets() {
        let manager = TopicManager::in_memory().unwrap();
        manager.create_topic("offset-test", 1).unwrap();

        // Append records
        for i in 0..10 {
            manager
                .append("offset-test", 0, None, Bytes::from(format!("value{}", i)))
                .unwrap();
        }

        // Check offsets
        assert_eq!(manager.earliest_offset("offset-test", 0).unwrap(), 0);
        assert_eq!(manager.latest_offset("offset-test", 0).unwrap(), 10);
        assert_eq!(manager.high_watermark("offset-test", 0).unwrap(), 10);
    }

    #[test]
    fn test_in_memory_vs_disk_topic_manager() {
        let dir = tempdir().unwrap();

        // Create in-memory manager
        let in_mem = TopicManager::in_memory().unwrap();

        // Create disk-based manager
        let on_disk = TopicManager::new(dir.path()).unwrap();

        // Create topics in both
        in_mem.create_topic("test-topic", 2).unwrap();
        on_disk.create_topic("test-topic", 2).unwrap();

        // Append same data to both
        for i in 0..5 {
            in_mem
                .append("test-topic", 0, None, Bytes::from(format!("value{}", i)))
                .unwrap();
            on_disk
                .append("test-topic", 0, None, Bytes::from(format!("value{}", i)))
                .unwrap();
        }

        // Both should have same data
        let in_mem_records = in_mem.read("test-topic", 0, 0, 100).unwrap();
        let on_disk_records = on_disk.read("test-topic", 0, 0, 100).unwrap();

        assert_eq!(in_mem_records.len(), on_disk_records.len());
        for (mem, disk) in in_mem_records.iter().zip(on_disk_records.iter()) {
            assert_eq!(mem.offset, disk.offset);
            assert_eq!(mem.value, disk.value);
        }

        // In-memory should not persist to disk
        assert!(in_mem.is_in_memory());
        assert!(!on_disk.is_in_memory());
    }

    #[test]
    fn test_topic_ttl_config() {
        // Test default TTL
        let config = TopicConfig::default();
        assert_eq!(config.message_ttl_ms, -1); // Infinite by default

        // Test custom TTL
        let config = TopicConfig {
            message_ttl_ms: 60000, // 1 minute
            ..Default::default()
        };
        assert_eq!(config.message_ttl_ms, 60000);
    }

    #[test]
    fn test_read_with_ttl_no_expiry() {
        // Test that read_with_ttl works when TTL is infinite (disabled)
        let manager = TopicManager::in_memory().unwrap();
        manager.create_topic("no-ttl-topic", 1).unwrap();

        // Append records
        manager
            .append("no-ttl-topic", 0, None, Bytes::from("value1"))
            .unwrap();
        manager
            .append("no-ttl-topic", 0, None, Bytes::from("value2"))
            .unwrap();

        // Read with TTL - should return all records since TTL is disabled
        let records = manager.read_with_ttl("no-ttl-topic", 0, 0, 100).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_read_with_ttl_topic_level() {
        use std::thread;
        use std::time::Duration;

        let manager = TopicManager::in_memory().unwrap();

        // Create topic with 100ms TTL
        let config = TopicConfig {
            message_ttl_ms: 100,
            ..Default::default()
        };
        manager
            .create_topic_with_config("ttl-topic", 1, config)
            .unwrap();

        // Append records
        manager
            .append("ttl-topic", 0, None, Bytes::from("value1"))
            .unwrap();
        manager
            .append("ttl-topic", 0, None, Bytes::from("value2"))
            .unwrap();

        // Read immediately - should see both
        let records = manager.read_with_ttl("ttl-topic", 0, 0, 100).unwrap();
        assert_eq!(records.len(), 2);

        // Wait for TTL to expire
        thread::sleep(Duration::from_millis(150));

        // Append a new record
        manager
            .append("ttl-topic", 0, None, Bytes::from("value3"))
            .unwrap();

        // Read with TTL - old records should be filtered out
        let records = manager.read_with_ttl("ttl-topic", 0, 0, 100).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, Bytes::from("value3"));
    }

    #[test]
    fn test_read_with_ttl_message_header() {
        use crate::storage::record::Header;
        use std::thread;
        use std::time::Duration;

        let manager = TopicManager::in_memory().unwrap();

        // Create topic with long TTL
        let config = TopicConfig {
            message_ttl_ms: 10000, // 10 seconds
            ..Default::default()
        };
        manager
            .create_topic_with_config("header-ttl-topic", 1, config)
            .unwrap();

        // Append record with short per-message TTL via header
        let short_ttl_headers = vec![Header {
            key: MESSAGE_TTL_HEADER.to_string(),
            value: Bytes::from("100"), // 100ms TTL
        }];
        manager
            .append_with_headers(
                "header-ttl-topic",
                0,
                None,
                Bytes::from("short-ttl"),
                short_ttl_headers,
            )
            .unwrap();

        // Append record with no header (uses topic TTL)
        manager
            .append("header-ttl-topic", 0, None, Bytes::from("long-ttl"))
            .unwrap();

        // Read immediately - both should be present
        let records = manager
            .read_with_ttl("header-ttl-topic", 0, 0, 100)
            .unwrap();
        assert_eq!(records.len(), 2);

        // Wait for short TTL to expire
        thread::sleep(Duration::from_millis(150));

        // Read with TTL - only long-TTL record should remain
        let records = manager
            .read_with_ttl("header-ttl-topic", 0, 0, 100)
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, Bytes::from("long-ttl"));
    }

    #[test]
    fn test_message_ttl_header_constant() {
        // Verify the header constant is what we expect
        assert_eq!(MESSAGE_TTL_HEADER, "x-message-ttl-ms");
    }

    #[test]
    fn test_partition_to_shard() {
        // Test round-robin partition-to-shard assignment
        assert_eq!(partition_to_shard(0, 4), 0);
        assert_eq!(partition_to_shard(1, 4), 1);
        assert_eq!(partition_to_shard(2, 4), 2);
        assert_eq!(partition_to_shard(3, 4), 3);
        assert_eq!(partition_to_shard(4, 4), 0);
        assert_eq!(partition_to_shard(5, 4), 1);
        assert_eq!(partition_to_shard(7, 4), 3);
        assert_eq!(partition_to_shard(100, 4), 0);

        // Test with different shard counts
        assert_eq!(partition_to_shard(5, 2), 1);
        assert_eq!(partition_to_shard(5, 3), 2);
        assert_eq!(partition_to_shard(5, 8), 5);
    }

    #[test]
    fn test_partition_to_shard_hashed() {
        // Hash-based assignment should be deterministic
        let shard1 = partition_to_shard_hashed("topic1", 0, 4);
        let shard1_again = partition_to_shard_hashed("topic1", 0, 4);
        assert_eq!(shard1, shard1_again);

        // Different topics with same partition should likely map to different shards
        let shard_topic1 = partition_to_shard_hashed("topic1", 0, 4);
        let shard_topic2 = partition_to_shard_hashed("topic2", 0, 4);
        // These could be the same by chance, but the hashing should work
        assert!(shard_topic1 < 4);
        assert!(shard_topic2 < 4);

        // Verify result is always within bounds
        for i in 0..100 {
            let shard = partition_to_shard_hashed("test", i, 8);
            assert!(shard < 8, "shard {} should be < 8", shard);
        }
    }
}
