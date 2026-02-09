//! Time-travel queries for historical data access
//!
//! Enables querying data as it existed at specific points in time.
//! Supports snapshot-based time travel with efficient storage.
//!
//! Features:
//! - Query data at specific timestamps
//! - List available snapshots
//! - Automatic snapshot creation
//! - Snapshot retention policies
//! - Incremental snapshots

use crate::error::{Result, StreamlineError};
use crate::storage::record::Record;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info};

/// Time-travel configuration
#[derive(Debug, Clone)]
pub struct TimeTravelConfig {
    /// Directory for snapshot data
    pub snapshot_dir: PathBuf,
    /// Enable automatic snapshots
    pub auto_snapshot: bool,
    /// Snapshot interval in milliseconds
    pub snapshot_interval_ms: u64,
    /// Maximum number of snapshots to retain
    pub max_snapshots: usize,
    /// Minimum snapshot age before deletion (ms)
    pub min_snapshot_age_ms: u64,
    /// Enable incremental snapshots
    pub incremental: bool,
    /// Snapshot compression
    pub compression: bool,
}

impl Default for TimeTravelConfig {
    fn default() -> Self {
        Self {
            snapshot_dir: PathBuf::from("./data/snapshots"),
            auto_snapshot: true,
            snapshot_interval_ms: 60000,  // 1 minute
            max_snapshots: 168,           // 1 week of hourly snapshots
            min_snapshot_age_ms: 3600000, // 1 hour
            incremental: true,
            compression: true,
        }
    }
}

/// Snapshot metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Unique snapshot ID
    pub id: u64,
    /// Topic name
    pub topic: String,
    /// Partition number
    pub partition: i32,
    /// Snapshot timestamp (ms since epoch)
    pub timestamp: i64,
    /// Start offset in this snapshot
    pub start_offset: i64,
    /// End offset in this snapshot
    pub end_offset: i64,
    /// Record count
    pub record_count: u64,
    /// Size in bytes (compressed if applicable)
    pub size_bytes: u64,
    /// Whether this is an incremental snapshot
    pub incremental: bool,
    /// Parent snapshot ID (for incremental)
    pub parent_id: Option<u64>,
    /// Checksum
    pub checksum: u32,
    /// Creation time
    pub created_at: i64,
}

/// Time-travel manager
pub struct TimeTravelManager {
    /// Configuration
    config: TimeTravelConfig,
    /// Snapshot counter
    snapshot_counter: AtomicU64,
    /// Snapshots by topic/partition
    snapshots: RwLock<HashMap<(String, i32), BTreeMap<i64, SnapshotMetadata>>>,
    /// Statistics
    stats: RwLock<TimeTravelStats>,
}

/// Time-travel statistics
#[derive(Debug, Clone, Default)]
pub struct TimeTravelStats {
    /// Total snapshots created
    pub snapshots_created: u64,
    /// Total snapshots deleted
    pub snapshots_deleted: u64,
    /// Total queries executed
    pub queries_executed: u64,
    /// Total bytes stored
    pub bytes_stored: u64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
}

impl TimeTravelManager {
    /// Create a new time-travel manager
    pub fn new(config: TimeTravelConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.snapshot_dir).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to create snapshot directory: {}", e))
        })?;

        info!(
            "Time-travel manager initialized at {:?}",
            config.snapshot_dir
        );

        Ok(Self {
            config,
            snapshot_counter: AtomicU64::new(0),
            snapshots: RwLock::new(HashMap::new()),
            stats: RwLock::new(TimeTravelStats::default()),
        })
    }

    /// Create a snapshot of current state
    pub fn create_snapshot(
        &self,
        topic: &str,
        partition: i32,
        records: &[Record],
    ) -> Result<SnapshotMetadata> {
        if records.is_empty() {
            return Err(StreamlineError::storage_msg(
                "No records to snapshot".to_string(),
            ));
        }

        let timestamp = chrono::Utc::now().timestamp_millis();
        let id = self.snapshot_counter.fetch_add(1, Ordering::Relaxed);

        let start_offset = records.first().map(|r| r.offset).unwrap_or(0);
        let end_offset = records.last().map(|r| r.offset).unwrap_or(0);

        // Calculate checksum
        let mut hasher = crc32fast::Hasher::new();
        for record in records {
            hasher.update(&record.offset.to_le_bytes());
            hasher.update(&record.timestamp.to_le_bytes());
            hasher.update(&record.value);
        }
        let checksum = hasher.finalize();

        // Serialize and optionally compress
        let data = bincode::serialize(records).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to serialize records: {}", e))
        })?;

        let (final_data, size_bytes) = if self.config.compression {
            let compressed = lz4_flex::compress_prepend_size(&data);
            let size = compressed.len() as u64;
            (compressed, size)
        } else {
            let size = data.len() as u64;
            (data, size)
        };

        // Find parent snapshot for incremental
        let parent_id = if self.config.incremental {
            self.snapshots
                .read()
                .get(&(topic.to_string(), partition))
                .and_then(|snaps| snaps.values().last().map(|s| s.id))
        } else {
            None
        };

        let metadata = SnapshotMetadata {
            id,
            topic: topic.to_string(),
            partition,
            timestamp,
            start_offset,
            end_offset,
            record_count: records.len() as u64,
            size_bytes,
            incremental: parent_id.is_some(),
            parent_id,
            checksum,
            created_at: timestamp,
        };

        // Save snapshot data
        let snapshot_path = self.snapshot_path(topic, partition, id);
        if let Some(parent) = snapshot_path.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(&snapshot_path, &final_data).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to write snapshot: {}", e))
        })?;

        // Save metadata
        let meta_path = self.metadata_path(topic, partition, id);
        let meta_json = serde_json::to_string_pretty(&metadata).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to serialize metadata: {}", e))
        })?;
        std::fs::write(&meta_path, meta_json).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to write metadata: {}", e))
        })?;

        // Register snapshot
        self.snapshots
            .write()
            .entry((topic.to_string(), partition))
            .or_default()
            .insert(timestamp, metadata.clone());

        self.stats.write().snapshots_created += 1;
        self.stats.write().bytes_stored += size_bytes;

        info!(
            "Created snapshot {} for {}/{} with {} records",
            id,
            topic,
            partition,
            records.len()
        );

        // Apply retention
        self.apply_retention(topic, partition)?;

        Ok(metadata)
    }

    /// Query data at a specific timestamp
    pub fn query_at(&self, topic: &str, partition: i32, timestamp: i64) -> Result<Vec<Record>> {
        self.stats.write().queries_executed += 1;

        let snapshots = self.snapshots.read();
        let partition_snapshots = snapshots.get(&(topic.to_string(), partition));

        let snapshot = match partition_snapshots {
            Some(snaps) => {
                // Find the snapshot at or just before the timestamp
                snaps
                    .range(..=timestamp)
                    .next_back()
                    .map(|(_, s)| s.clone())
            }
            None => None,
        };

        match snapshot {
            Some(meta) => {
                self.stats.write().cache_hits += 1;
                self.load_snapshot(&meta)
            }
            None => {
                self.stats.write().cache_misses += 1;
                Ok(Vec::new())
            }
        }
    }

    /// Query data within a time range
    pub fn query_range(
        &self,
        topic: &str,
        partition: i32,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<Record>> {
        self.stats.write().queries_executed += 1;

        let snapshots = self.snapshots.read();
        let partition_snapshots = snapshots.get(&(topic.to_string(), partition));

        let mut all_records = Vec::new();

        if let Some(snaps) = partition_snapshots {
            for (_ts, meta) in snaps.range(start_ts..=end_ts) {
                let records = self.load_snapshot(meta)?;
                all_records.extend(records);
            }
        }

        // Deduplicate by offset
        all_records.sort_by_key(|r| r.offset);
        all_records.dedup_by_key(|r| r.offset);

        Ok(all_records)
    }

    /// Load snapshot data
    fn load_snapshot(&self, meta: &SnapshotMetadata) -> Result<Vec<Record>> {
        let snapshot_path = self.snapshot_path(&meta.topic, meta.partition, meta.id);

        let data = std::fs::read(&snapshot_path)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to read snapshot: {}", e)))?;

        let decompressed = if self.config.compression {
            lz4_flex::decompress_size_prepended(&data).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to decompress snapshot: {}", e))
            })?
        } else {
            data
        };

        let records: Vec<Record> = bincode::deserialize(&decompressed).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to deserialize records: {}", e))
        })?;

        // Verify checksum
        let mut hasher = crc32fast::Hasher::new();
        for record in &records {
            hasher.update(&record.offset.to_le_bytes());
            hasher.update(&record.timestamp.to_le_bytes());
            hasher.update(&record.value);
        }
        let checksum = hasher.finalize();

        if checksum != meta.checksum {
            return Err(StreamlineError::storage_msg(
                "Snapshot checksum mismatch".to_string(),
            ));
        }

        debug!("Loaded snapshot {} with {} records", meta.id, records.len());

        Ok(records)
    }

    /// List available snapshots
    pub fn list_snapshots(&self, topic: &str, partition: i32) -> Vec<SnapshotMetadata> {
        self.snapshots
            .read()
            .get(&(topic.to_string(), partition))
            .map(|snaps| snaps.values().cloned().collect())
            .unwrap_or_default()
    }

    /// Get snapshot at specific timestamp
    pub fn get_snapshot(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Option<SnapshotMetadata> {
        self.snapshots
            .read()
            .get(&(topic.to_string(), partition))
            .and_then(|snaps| snaps.get(&timestamp).cloned())
    }

    /// Delete a snapshot
    pub fn delete_snapshot(&self, topic: &str, partition: i32, id: u64) -> Result<()> {
        let mut snapshots = self.snapshots.write();
        let partition_snapshots = snapshots.get_mut(&(topic.to_string(), partition));

        if let Some(snaps) = partition_snapshots {
            let timestamp = snaps.iter().find(|(_, m)| m.id == id).map(|(ts, _)| *ts);

            if let Some(ts) = timestamp {
                if let Some(meta) = snaps.remove(&ts) {
                    // Delete files
                    let snapshot_path = self.snapshot_path(topic, partition, id);
                    let meta_path = self.metadata_path(topic, partition, id);
                    std::fs::remove_file(&snapshot_path).ok();
                    std::fs::remove_file(&meta_path).ok();

                    let mut stats = self.stats.write();
                    stats.snapshots_deleted += 1;
                    stats.bytes_stored = stats.bytes_stored.saturating_sub(meta.size_bytes);

                    info!("Deleted snapshot {} for {}/{}", id, topic, partition);
                }
            }
        }

        Ok(())
    }

    /// Apply retention policy
    fn apply_retention(&self, topic: &str, partition: i32) -> Result<()> {
        let now = chrono::Utc::now().timestamp_millis();
        let mut to_delete = Vec::new();

        {
            let snapshots = self.snapshots.read();
            if let Some(snaps) = snapshots.get(&(topic.to_string(), partition)) {
                let count = snaps.len();
                if count > self.config.max_snapshots {
                    // Find oldest snapshots beyond retention
                    let excess = count - self.config.max_snapshots;
                    for (_ts, meta) in snaps.iter().take(excess) {
                        let age = now - meta.created_at;
                        if age > self.config.min_snapshot_age_ms as i64 {
                            to_delete.push(meta.id);
                        }
                    }
                }
            }
        }

        for id in to_delete {
            self.delete_snapshot(topic, partition, id)?;
        }

        Ok(())
    }

    /// Get snapshot file path
    fn snapshot_path(&self, topic: &str, partition: i32, id: u64) -> PathBuf {
        self.config
            .snapshot_dir
            .join(topic)
            .join(format!("partition-{}", partition))
            .join(format!("{}.snap", id))
    }

    /// Get metadata file path
    fn metadata_path(&self, topic: &str, partition: i32, id: u64) -> PathBuf {
        self.config
            .snapshot_dir
            .join(topic)
            .join(format!("partition-{}", partition))
            .join(format!("{}.meta.json", id))
    }

    /// Load existing snapshots from disk
    pub fn load_existing(&self) -> Result<usize> {
        let mut count = 0;

        if !self.config.snapshot_dir.exists() {
            return Ok(0);
        }

        for topic_entry in std::fs::read_dir(&self.config.snapshot_dir).map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to read snapshot dir: {}", e))
        })? {
            let topic_entry = topic_entry.map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to read topic entry: {}", e))
            })?;
            let topic = topic_entry.file_name().to_string_lossy().to_string();

            if !topic_entry.path().is_dir() {
                continue;
            }

            for part_entry in std::fs::read_dir(topic_entry.path()).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to read partition dir: {}", e))
            })? {
                let part_entry = part_entry.map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to read partition entry: {}", e))
                })?;
                let part_name = part_entry.file_name().to_string_lossy().to_string();

                if !part_name.starts_with("partition-") {
                    continue;
                }

                let partition: i32 = part_name
                    .strip_prefix("partition-")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);

                // Load metadata files
                for file_entry in std::fs::read_dir(part_entry.path()).map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to read files: {}", e))
                })? {
                    let file_entry = file_entry.map_err(|e| {
                        StreamlineError::storage_msg(format!("Failed to read file entry: {}", e))
                    })?;
                    let file_name = file_entry.file_name().to_string_lossy().to_string();

                    if file_name.ends_with(".meta.json") {
                        let meta_content =
                            std::fs::read_to_string(file_entry.path()).map_err(|e| {
                                StreamlineError::storage_msg(format!(
                                    "Failed to read metadata: {}",
                                    e
                                ))
                            })?;
                        let meta: SnapshotMetadata =
                            serde_json::from_str(&meta_content).map_err(|e| {
                                StreamlineError::storage_msg(format!(
                                    "Failed to parse metadata: {}",
                                    e
                                ))
                            })?;

                        self.snapshots
                            .write()
                            .entry((topic.clone(), partition))
                            .or_default()
                            .insert(meta.timestamp, meta.clone());

                        // Update counter
                        let current = self.snapshot_counter.load(Ordering::Relaxed);
                        if meta.id >= current {
                            self.snapshot_counter.store(meta.id + 1, Ordering::Relaxed);
                        }

                        count += 1;
                    }
                }
            }
        }

        info!("Loaded {} existing snapshots", count);
        Ok(count)
    }

    /// Get statistics
    pub fn stats(&self) -> TimeTravelStats {
        self.stats.read().clone()
    }

    /// Get configuration
    pub fn config(&self) -> &TimeTravelConfig {
        &self.config
    }
}

/// Time point for querying
#[derive(Debug, Clone)]
pub enum TimePoint {
    /// Absolute timestamp in milliseconds
    Timestamp(i64),
    /// Relative time (negative offset from now in ms)
    Relative(i64),
    /// Named point
    Named(String),
    /// Latest available
    Latest,
    /// Earliest available
    Earliest,
}

impl TimePoint {
    /// Resolve to absolute timestamp
    pub fn resolve(&self) -> i64 {
        let now = chrono::Utc::now().timestamp_millis();
        match self {
            TimePoint::Timestamp(ts) => *ts,
            TimePoint::Relative(offset) => now + offset,
            TimePoint::Named(_) => now, // Would lookup named point
            TimePoint::Latest => now,
            TimePoint::Earliest => 0,
        }
    }

    /// Parse from string
    pub fn parse(s: &str) -> Result<Self> {
        if s == "latest" {
            return Ok(TimePoint::Latest);
        }
        if s == "earliest" {
            return Ok(TimePoint::Earliest);
        }

        // Try parsing as duration (e.g., "-1h", "-30m", "-1d")
        if let Some(duration_str) = s.strip_prefix('-') {
            let (num, unit) = if let Some(pos) = duration_str.find(|c: char| !c.is_ascii_digit()) {
                let (n, u) = duration_str.split_at(pos);
                (n.parse::<i64>().unwrap_or(0), u)
            } else {
                (duration_str.parse::<i64>().unwrap_or(0), "ms")
            };

            let ms = match unit {
                "ms" => num,
                "s" => num * 1000,
                "m" => num * 60 * 1000,
                "h" => num * 60 * 60 * 1000,
                "d" => num * 24 * 60 * 60 * 1000,
                _ => num,
            };

            return Ok(TimePoint::Relative(-ms));
        }

        // Try parsing as timestamp
        if let Ok(ts) = s.parse::<i64>() {
            return Ok(TimePoint::Timestamp(ts));
        }

        // Try parsing as ISO 8601 datetime
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return Ok(TimePoint::Timestamp(dt.timestamp_millis()));
        }

        // Treat as named point
        Ok(TimePoint::Named(s.to_string()))
    }
}

/// Version history entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionEntry {
    /// Version number
    pub version: u64,
    /// Timestamp
    pub timestamp: i64,
    /// Snapshot ID
    pub snapshot_id: u64,
    /// Description
    pub description: String,
}

/// Version manager for named versions
pub struct VersionManager {
    /// Versions by topic/partition
    versions: RwLock<HashMap<(String, i32), Vec<VersionEntry>>>,
    /// Version counter
    version_counter: AtomicU64,
}

impl VersionManager {
    /// Create a new version manager
    pub fn new() -> Self {
        Self {
            versions: RwLock::new(HashMap::new()),
            version_counter: AtomicU64::new(1),
        }
    }

    /// Create a new version
    pub fn create_version(
        &self,
        topic: &str,
        partition: i32,
        snapshot_id: u64,
        description: &str,
    ) -> VersionEntry {
        let version = self.version_counter.fetch_add(1, Ordering::Relaxed);
        let entry = VersionEntry {
            version,
            timestamp: chrono::Utc::now().timestamp_millis(),
            snapshot_id,
            description: description.to_string(),
        };

        self.versions
            .write()
            .entry((topic.to_string(), partition))
            .or_default()
            .push(entry.clone());

        entry
    }

    /// Get version by number
    pub fn get_version(&self, topic: &str, partition: i32, version: u64) -> Option<VersionEntry> {
        self.versions
            .read()
            .get(&(topic.to_string(), partition))
            .and_then(|versions| versions.iter().find(|v| v.version == version).cloned())
    }

    /// List all versions
    pub fn list_versions(&self, topic: &str, partition: i32) -> Vec<VersionEntry> {
        self.versions
            .read()
            .get(&(topic.to_string(), partition))
            .cloned()
            .unwrap_or_default()
    }

    /// Get latest version
    pub fn latest_version(&self, topic: &str, partition: i32) -> Option<VersionEntry> {
        self.versions
            .read()
            .get(&(topic.to_string(), partition))
            .and_then(|versions| versions.last().cloned())
    }
}

impl Default for VersionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn create_test_records(count: usize, start_offset: i64) -> Vec<Record> {
        (0..count)
            .map(|i| {
                Record::new(
                    start_offset + i as i64,
                    chrono::Utc::now().timestamp_millis() + i as i64,
                    Some(Bytes::from(format!("key-{}", i))),
                    Bytes::from(format!("value-{}", i)),
                )
            })
            .collect()
    }

    #[test]
    fn test_timetravel_config_default() {
        let config = TimeTravelConfig::default();
        assert!(config.auto_snapshot);
        assert_eq!(config.snapshot_interval_ms, 60000);
        assert_eq!(config.max_snapshots, 168);
    }

    #[test]
    fn test_create_and_query_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let config = TimeTravelConfig {
            snapshot_dir: temp_dir.path().to_path_buf(),
            compression: true,
            ..Default::default()
        };
        let manager = TimeTravelManager::new(config).unwrap();

        let records = create_test_records(100, 0);
        let meta = manager.create_snapshot("test-topic", 0, &records).unwrap();

        assert_eq!(meta.topic, "test-topic");
        assert_eq!(meta.partition, 0);
        assert_eq!(meta.record_count, 100);

        // Query the snapshot
        let result = manager.query_at("test-topic", 0, meta.timestamp).unwrap();
        assert_eq!(result.len(), 100);
        assert_eq!(result[0].offset, 0);
        assert_eq!(result[99].offset, 99);
    }

    #[test]
    fn test_list_snapshots() {
        let temp_dir = TempDir::new().unwrap();
        let config = TimeTravelConfig {
            snapshot_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let manager = TimeTravelManager::new(config).unwrap();

        let records1 = create_test_records(50, 0);
        let records2 = create_test_records(50, 50);

        manager.create_snapshot("test-topic", 0, &records1).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        manager.create_snapshot("test-topic", 0, &records2).unwrap();

        let snapshots = manager.list_snapshots("test-topic", 0);
        assert_eq!(snapshots.len(), 2);
    }

    #[test]
    fn test_delete_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let config = TimeTravelConfig {
            snapshot_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let manager = TimeTravelManager::new(config).unwrap();

        let records = create_test_records(50, 0);
        let meta = manager.create_snapshot("test-topic", 0, &records).unwrap();

        assert_eq!(manager.list_snapshots("test-topic", 0).len(), 1);

        manager.delete_snapshot("test-topic", 0, meta.id).unwrap();
        assert_eq!(manager.list_snapshots("test-topic", 0).len(), 0);
    }

    #[test]
    fn test_timepoint_parse() {
        let tp = TimePoint::parse("latest").unwrap();
        assert!(matches!(tp, TimePoint::Latest));

        let tp = TimePoint::parse("earliest").unwrap();
        assert!(matches!(tp, TimePoint::Earliest));

        let tp = TimePoint::parse("-1h").unwrap();
        if let TimePoint::Relative(ms) = tp {
            assert_eq!(ms, -3600000);
        } else {
            panic!("Expected Relative");
        }

        let tp = TimePoint::parse("-30m").unwrap();
        if let TimePoint::Relative(ms) = tp {
            assert_eq!(ms, -1800000);
        } else {
            panic!("Expected Relative");
        }

        let tp = TimePoint::parse("1234567890000").unwrap();
        if let TimePoint::Timestamp(ts) = tp {
            assert_eq!(ts, 1234567890000);
        } else {
            panic!("Expected Timestamp");
        }
    }

    #[test]
    fn test_version_manager() {
        let vm = VersionManager::new();

        let v1 = vm.create_version("test", 0, 1, "Initial version");
        let v2 = vm.create_version("test", 0, 2, "Second version");

        assert_eq!(v1.version, 1);
        assert_eq!(v2.version, 2);

        let versions = vm.list_versions("test", 0);
        assert_eq!(versions.len(), 2);

        let latest = vm.latest_version("test", 0).unwrap();
        assert_eq!(latest.version, 2);
    }

    #[test]
    fn test_timetravel_stats() {
        let temp_dir = TempDir::new().unwrap();
        let config = TimeTravelConfig {
            snapshot_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let manager = TimeTravelManager::new(config).unwrap();

        let stats = manager.stats();
        assert_eq!(stats.snapshots_created, 0);

        let records = create_test_records(10, 0);
        manager.create_snapshot("test", 0, &records).unwrap();

        let stats = manager.stats();
        assert_eq!(stats.snapshots_created, 1);
        assert!(stats.bytes_stored > 0);
    }
}
