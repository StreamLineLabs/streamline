//! Edge synchronization engine
//!
//! Handles bidirectional sync between edge and cloud.

use crate::error::{Result, StreamlineError};
use crate::storage::{Record, TopicManager};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use super::checkpoint::EdgeSyncCheckpoint;
use super::config::{EdgeSyncConfig, EdgeSyncMode};
use super::conflict::{ConflictResolver, ConflictStats, ResolutionResult};

/// Sync direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncDirection {
    /// Edge to cloud only
    EdgeToCloud,
    /// Cloud to edge only
    CloudToEdge,
    /// Bidirectional sync
    Bidirectional,
}

/// Current sync state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum SyncState {
    /// Sync is idle
    #[default]
    Idle,
    /// Currently syncing
    Syncing,
    /// Waiting for connection
    WaitingForConnection,
    /// Sync completed successfully
    Completed,
    /// Sync failed
    Failed,
    /// Sync paused
    Paused,
}

/// Sync operation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResult {
    /// Whether sync was successful
    pub success: bool,
    /// Number of records synced from edge to cloud
    pub edge_to_cloud_records: u64,
    /// Number of records synced from cloud to edge
    pub cloud_to_edge_records: u64,
    /// Number of conflicts detected
    pub conflicts: u64,
    /// Conflict statistics
    pub conflict_stats: ConflictStats,
    /// Duration in milliseconds
    pub duration_ms: u64,
    /// Error message if failed
    pub error: Option<String>,
    /// Topics synced
    pub topics: Vec<String>,
    /// Timestamp of sync completion
    pub completed_at: i64,
}

/// Sync progress callback
pub type SyncProgressCallback = Box<dyn Fn(SyncProgress) -> Result<()> + Send + Sync>;

/// Sync progress information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncProgress {
    /// Current sync state
    pub state: SyncState,
    /// Current topic being synced
    pub current_topic: Option<String>,
    /// Current partition being synced
    pub current_partition: Option<i32>,
    /// Records processed so far
    pub records_processed: u64,
    /// Total records to process (if known)
    pub total_records: Option<u64>,
    /// Percentage complete (0-100)
    pub percent_complete: f32,
    /// Bytes transferred
    pub bytes_transferred: u64,
    /// Current phase of sync
    pub phase: SyncPhase,
}

/// Sync phase
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum SyncPhase {
    /// Starting sync
    #[default]
    Starting,
    /// Fetching cloud state
    FetchingCloudState,
    /// Detecting conflicts
    DetectingConflicts,
    /// Resolving conflicts
    ResolvingConflicts,
    /// Uploading edge records
    Uploading,
    /// Downloading cloud records
    Downloading,
    /// Finalizing sync
    Finalizing,
    /// Sync complete
    Complete,
}

/// Edge sync engine statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncStats {
    /// Total syncs performed
    pub total_syncs: u64,
    /// Successful syncs
    pub successful_syncs: u64,
    /// Failed syncs
    pub failed_syncs: u64,
    /// Total records synced edge to cloud
    pub total_edge_to_cloud: u64,
    /// Total records synced cloud to edge
    pub total_cloud_to_edge: u64,
    /// Total conflicts resolved
    pub total_conflicts: u64,
    /// Total bytes uploaded
    pub total_bytes_uploaded: u64,
    /// Total bytes downloaded
    pub total_bytes_downloaded: u64,
    /// Last sync timestamp
    pub last_sync_at: Option<i64>,
    /// Last successful sync timestamp
    pub last_success_at: Option<i64>,
    /// Average sync duration in ms
    pub avg_sync_duration_ms: u64,
}

/// Edge sync engine
pub struct EdgeSyncEngine {
    config: EdgeSyncConfig,
    topic_manager: Arc<TopicManager>,
    conflict_resolver: Arc<ConflictResolver>,
    checkpoint: Arc<EdgeSyncCheckpoint>,
    state: parking_lot::RwLock<SyncState>,
    stats: parking_lot::RwLock<SyncStats>,
    running: AtomicBool,
    last_sync_time: AtomicU64,
    #[allow(dead_code)]
    progress_callback: Option<SyncProgressCallback>,
}

impl EdgeSyncEngine {
    /// Create a new sync engine
    pub fn new(
        config: EdgeSyncConfig,
        topic_manager: Arc<TopicManager>,
        conflict_resolver: Arc<ConflictResolver>,
        checkpoint: Arc<EdgeSyncCheckpoint>,
    ) -> Self {
        Self {
            config,
            topic_manager,
            conflict_resolver,
            checkpoint,
            state: parking_lot::RwLock::new(SyncState::Idle),
            stats: parking_lot::RwLock::new(SyncStats::default()),
            running: AtomicBool::new(false),
            last_sync_time: AtomicU64::new(0),
            progress_callback: None,
        }
    }

    /// Set progress callback
    #[allow(dead_code)]
    pub fn with_progress_callback(mut self, callback: SyncProgressCallback) -> Self {
        self.progress_callback = Some(callback);
        self
    }

    /// Get current sync state
    pub fn state(&self) -> SyncState {
        *self.state.read()
    }

    /// Get sync statistics
    pub fn stats(&self) -> SyncStats {
        self.stats.read().clone()
    }

    /// Check if sync is currently running
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Start the sync engine (for continuous/interval modes)
    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(StreamlineError::storage_msg(
                "Sync engine already running".into(),
            ));
        }

        *self.state.write() = SyncState::WaitingForConnection;

        match self.config.mode {
            EdgeSyncMode::Continuous => {
                self.run_continuous().await?;
            }
            EdgeSyncMode::Interval => {
                self.run_interval().await?;
            }
            EdgeSyncMode::Manual | EdgeSyncMode::Batched => {
                // For manual/batched, just mark as ready
                *self.state.write() = SyncState::Idle;
            }
        }

        Ok(())
    }

    /// Stop the sync engine
    pub async fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        *self.state.write() = SyncState::Idle;
        Ok(())
    }

    /// Pause syncing
    pub fn pause(&self) {
        *self.state.write() = SyncState::Paused;
    }

    /// Resume syncing
    pub fn resume(&self) {
        if *self.state.read() == SyncState::Paused {
            *self.state.write() = SyncState::Idle;
        }
    }

    /// Perform a single sync operation
    pub async fn sync_now(&self, direction: SyncDirection) -> Result<SyncResult> {
        let start_time = std::time::Instant::now();
        *self.state.write() = SyncState::Syncing;

        let result = self.do_sync(direction).await;

        let duration_ms = start_time.elapsed().as_millis() as u64;

        match &result {
            Ok(sync_result) => {
                let mut stats = self.stats.write();
                stats.total_syncs += 1;
                stats.successful_syncs += 1;
                stats.total_edge_to_cloud += sync_result.edge_to_cloud_records;
                stats.total_cloud_to_edge += sync_result.cloud_to_edge_records;
                stats.total_conflicts += sync_result.conflicts;
                stats.last_sync_at = Some(sync_result.completed_at);
                stats.last_success_at = Some(sync_result.completed_at);
                stats.avg_sync_duration_ms =
                    (stats.avg_sync_duration_ms + duration_ms) / stats.total_syncs;

                *self.state.write() = SyncState::Completed;
            }
            Err(_) => {
                let mut stats = self.stats.write();
                stats.total_syncs += 1;
                stats.failed_syncs += 1;
                stats.last_sync_at = Some(chrono::Utc::now().timestamp_millis());

                *self.state.write() = SyncState::Failed;
            }
        }

        self.last_sync_time.store(
            chrono::Utc::now().timestamp_millis() as u64,
            Ordering::SeqCst,
        );

        result
    }

    /// Internal sync implementation
    async fn do_sync(&self, direction: SyncDirection) -> Result<SyncResult> {
        let mut edge_to_cloud_records = 0u64;
        let mut cloud_to_edge_records = 0u64;
        let mut total_conflicts = 0u64;
        let mut synced_topics = Vec::new();

        // Get topics to sync
        let topics = self.get_topics_to_sync()?;

        for topic in &topics {
            // Get partitions for this topic
            let partitions = self.get_partition_count(topic)?;

            for partition in 0..partitions {
                // Get checkpoint for this partition
                let checkpoint_offset = self.checkpoint.get_offset(topic, partition);

                // Sync based on direction
                match direction {
                    SyncDirection::EdgeToCloud | SyncDirection::Bidirectional => {
                        let (synced, conflicts) = self
                            .sync_edge_to_cloud(topic, partition, checkpoint_offset)
                            .await?;
                        edge_to_cloud_records += synced;
                        total_conflicts += conflicts;
                    }
                    _ => {}
                }

                match direction {
                    SyncDirection::CloudToEdge | SyncDirection::Bidirectional => {
                        let synced = self.sync_cloud_to_edge(topic, partition).await?;
                        cloud_to_edge_records += synced;
                    }
                    _ => {}
                }
            }

            synced_topics.push(topic.clone());
        }

        let completed_at = chrono::Utc::now().timestamp_millis();

        Ok(SyncResult {
            success: true,
            edge_to_cloud_records,
            cloud_to_edge_records,
            conflicts: total_conflicts,
            conflict_stats: self.conflict_resolver.stats(),
            duration_ms: 0, // Will be set by caller
            error: None,
            topics: synced_topics,
            completed_at,
        })
    }

    /// Sync edge records to cloud
    async fn sync_edge_to_cloud(
        &self,
        topic: &str,
        partition: i32,
        from_offset: i64,
    ) -> Result<(u64, u64)> {
        // Read local records from the checkpoint offset
        let records = self
            .topic_manager
            .read(topic, partition, from_offset, 1000)?;

        if records.is_empty() {
            return Ok((0, 0));
        }

        // In a real implementation, this would send records to cloud
        // For now, we simulate the sync
        let mut synced = 0u64;
        let mut conflicts = 0u64;

        for record in &records {
            // Simulate cloud check for conflicts
            let cloud_record = self
                .fetch_cloud_record(topic, partition, record.offset)
                .await?;

            if let Some(cloud) = cloud_record {
                // Conflict detected
                let resolution = self.conflict_resolver.resolve(record, &cloud);
                conflicts += 1;

                match resolution {
                    ResolutionResult::UseEdge | ResolutionResult::UseMerged => {
                        // Upload edge record
                        self.upload_record(topic, partition, record).await?;
                        synced += 1;
                    }
                    ResolutionResult::UseCloud => {
                        // Cloud wins, no upload needed
                    }
                    ResolutionResult::Skip | ResolutionResult::Unresolved => {
                        // Skip this record
                    }
                }
            } else {
                // No conflict, upload
                self.upload_record(topic, partition, record).await?;
                synced += 1;
            }
        }

        // Update checkpoint
        if let Some(last_record) = records.last() {
            self.checkpoint
                .update(topic, partition, last_record.offset)?;
        }

        Ok((synced, conflicts))
    }

    /// Sync cloud records to edge
    async fn sync_cloud_to_edge(&self, topic: &str, partition: i32) -> Result<u64> {
        // In a real implementation, this would fetch from cloud
        // For now, we simulate the sync
        let cloud_records = self.fetch_cloud_records(topic, partition).await?;

        let mut synced = 0u64;

        for record in cloud_records {
            // Write to local storage (use append_with_headers for headers)
            if record.headers.is_empty() {
                self.topic_manager.append(
                    topic,
                    partition,
                    record.key.clone(),
                    record.value.clone(),
                )?;
            } else {
                self.topic_manager.append_with_headers(
                    topic,
                    partition,
                    record.key.clone(),
                    record.value.clone(),
                    record.headers.clone(),
                )?;
            }
            synced += 1;
        }

        Ok(synced)
    }

    /// Run continuous sync mode
    async fn run_continuous(&self) -> Result<()> {
        while self.running.load(Ordering::SeqCst) {
            if *self.state.read() == SyncState::Paused {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                continue;
            }

            let _ = self.sync_now(SyncDirection::Bidirectional).await;

            // Small delay between syncs
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        Ok(())
    }

    /// Run interval sync mode
    async fn run_interval(&self) -> Result<()> {
        let interval = self.config.interval();

        while self.running.load(Ordering::SeqCst) {
            if *self.state.read() != SyncState::Paused {
                let _ = self.sync_now(SyncDirection::Bidirectional).await;
            }

            tokio::time::sleep(interval).await;
        }
        Ok(())
    }

    /// Get topics to sync
    fn get_topics_to_sync(&self) -> Result<Vec<String>> {
        if self.config.sync_topics.is_empty() {
            // Sync all topics - map TopicMetadata to names
            Ok(self
                .topic_manager
                .list_topics()?
                .into_iter()
                .map(|m| m.name)
                .collect())
        } else {
            Ok(self.config.sync_topics.clone())
        }
    }

    /// Get partition count for a topic
    fn get_partition_count(&self, topic: &str) -> Result<i32> {
        self.topic_manager
            .get_topic_metadata(topic)
            .map(|m| m.num_partitions)
    }

    // Stub methods for cloud operations (would be implemented with actual HTTP client)

    #[allow(dead_code)]
    async fn fetch_cloud_record(
        &self,
        _topic: &str,
        _partition: i32,
        _offset: i64,
    ) -> Result<Option<Record>> {
        // Stub: In real implementation, fetch from cloud endpoint
        Ok(None)
    }

    #[allow(dead_code)]
    async fn fetch_cloud_records(&self, _topic: &str, _partition: i32) -> Result<Vec<Record>> {
        // Stub: In real implementation, fetch from cloud endpoint
        Ok(Vec::new())
    }

    #[allow(dead_code)]
    async fn upload_record(&self, _topic: &str, _partition: i32, _record: &Record) -> Result<()> {
        // Stub: In real implementation, upload to cloud endpoint
        Ok(())
    }
}

/// Batch sync request for efficient bulk operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncBatch {
    /// Edge ID sending the batch
    pub edge_id: String,
    /// Batch sequence number
    pub sequence: u64,
    /// Records grouped by topic and partition
    pub records: HashMap<String, HashMap<i32, Vec<SyncRecord>>>,
    /// Compression used
    pub compression: String,
    /// Whether this is the last batch
    pub is_final: bool,
}

/// Record in a sync batch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncRecord {
    /// Offset in edge storage
    pub offset: i64,
    /// Timestamp
    pub timestamp: i64,
    /// Key (base64 encoded)
    pub key: Option<String>,
    /// Value (base64 encoded)
    pub value: String,
    /// Headers
    pub headers: HashMap<String, String>,
    /// CRC32 checksum
    pub crc32: u32,
}

impl SyncRecord {
    /// Convert from storage Record
    pub fn from_record(record: &Record) -> Self {
        use base64::{engine::general_purpose::STANDARD, Engine as _};

        Self {
            offset: record.offset,
            timestamp: record.timestamp,
            key: record.key.as_ref().map(|k| STANDARD.encode(k)),
            value: STANDARD.encode(&record.value),
            headers: record
                .headers
                .iter()
                .map(|h| (h.key.clone(), STANDARD.encode(&h.value)))
                .collect(),
            crc32: record.crc.unwrap_or(0),
        }
    }

    /// Convert to storage Record
    pub fn to_record(&self) -> Result<Record> {
        use crate::storage::Header;
        use base64::{engine::general_purpose::STANDARD, Engine as _};

        let key = self
            .key
            .as_ref()
            .map(|k| STANDARD.decode(k))
            .transpose()
            .map_err(|e| StreamlineError::storage_msg(format!("Invalid base64 key: {}", e)))?
            .map(Bytes::from);

        let value = STANDARD
            .decode(&self.value)
            .map_err(|e| StreamlineError::storage_msg(format!("Invalid base64 value: {}", e)))?;

        let headers: Result<Vec<Header>> = self
            .headers
            .iter()
            .map(|(k, v)| {
                STANDARD
                    .decode(v)
                    .map(|decoded| Header {
                        key: k.clone(),
                        value: Bytes::from(decoded),
                    })
                    .map_err(|e| {
                        StreamlineError::storage_msg(format!("Invalid base64 header: {}", e))
                    })
            })
            .collect();

        Ok(Record {
            offset: self.offset,
            timestamp: self.timestamp,
            key,
            value: Bytes::from(value),
            headers: headers?,
            crc: Some(self.crc32),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::edge::config::ConflictResolution;
    use tempfile::TempDir;

    fn create_test_engine() -> (EdgeSyncEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());

        let sync_config = EdgeSyncConfig::default();
        let conflict_resolver = Arc::new(ConflictResolver::new(ConflictResolution::LastWriteWins));
        let checkpoint =
            Arc::new(EdgeSyncCheckpoint::new(temp_dir.path().join("checkpoint.json")).unwrap());

        let engine = EdgeSyncEngine::new(sync_config, topic_manager, conflict_resolver, checkpoint);

        (engine, temp_dir)
    }

    #[test]
    fn test_sync_engine_state() {
        let (engine, _temp) = create_test_engine();
        assert_eq!(engine.state(), SyncState::Idle);
        assert!(!engine.is_running());
    }

    #[test]
    fn test_sync_stats_default() {
        let (engine, _temp) = create_test_engine();
        let stats = engine.stats();
        assert_eq!(stats.total_syncs, 0);
        assert_eq!(stats.successful_syncs, 0);
    }

    #[test]
    fn test_pause_resume() {
        let (engine, _temp) = create_test_engine();

        engine.pause();
        assert_eq!(engine.state(), SyncState::Paused);

        engine.resume();
        assert_eq!(engine.state(), SyncState::Idle);
    }

    #[test]
    fn test_sync_record_conversion() {
        let record = Record {
            offset: 42,
            timestamp: 1234567890,
            key: Some(Bytes::from("test-key")),
            value: Bytes::from("test-value"),
            headers: vec![crate::storage::Header {
                key: "header1".to_string(),
                value: Bytes::from("value1"),
            }],
            crc: Some(12345),
        };

        let sync_record = SyncRecord::from_record(&record);
        assert_eq!(sync_record.offset, 42);
        assert_eq!(sync_record.timestamp, 1234567890);
        assert!(sync_record.key.is_some());

        let converted = sync_record.to_record().unwrap();
        assert_eq!(converted.offset, record.offset);
        assert_eq!(converted.timestamp, record.timestamp);
        assert_eq!(converted.value, record.value);
    }

    #[tokio::test]
    async fn test_sync_now() {
        let (engine, temp) = create_test_engine();

        // Create a test topic
        engine.topic_manager.create_topic("test-topic", 1).unwrap();

        // Perform sync
        let result = engine.sync_now(SyncDirection::EdgeToCloud).await;
        assert!(result.is_ok());

        let sync_result = result.unwrap();
        assert!(sync_result.success);

        // Clean up
        drop(engine);
        drop(temp);
    }
}
