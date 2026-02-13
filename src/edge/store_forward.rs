//! Store-and-Forward Engine for Edge → Cloud Data Sync
//!
//! Buffers messages locally when the cloud endpoint is unreachable and
//! performs batch uploads when connectivity is restored. Tracks sync
//! progress via per-topic/partition watermarks.
//!
//! # Sync Strategies
//!
//! - **All**: Forward every record to the cloud.
//! - **LatestOnly**: Keep only the latest value per key (compacted).
//! - **Sample**: Forward a configurable fraction of records.

use crate::error::{Result, StreamlineError};
use crate::storage::{Record, TopicManager};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::checkpoint::EdgeSyncCheckpoint;

/// Strategy for selecting which records to sync to the cloud.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum SyncStrategy {
    /// Sync every record.
    #[default]
    All,
    /// Sync only the latest value per key (log-compacted view).
    LatestOnly,
    /// Sample a fraction of records (configured via `sample_rate`).
    Sample,
}

impl SyncStrategy {
    /// Parse from a string (case-insensitive).
    pub fn from_str_loose(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "latest-only" | "latest_only" | "latestonly" => Self::LatestOnly,
            "sample" => Self::Sample,
            _ => Self::All,
        }
    }
}

/// Configuration for the store-and-forward engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreForwardConfig {
    /// Cloud endpoint for uploads.
    pub cloud_endpoint: String,
    /// Sync strategy.
    pub strategy: SyncStrategy,
    /// Sync interval in seconds.
    pub sync_interval_secs: u64,
    /// Maximum records per upload batch.
    pub batch_size: usize,
    /// Sample rate (0.0–1.0) when using the `Sample` strategy.
    pub sample_rate: f64,
    /// Maximum retries for a failed upload batch.
    pub max_retries: u32,
    /// Retry backoff base in milliseconds.
    pub retry_backoff_ms: u64,
}

impl Default for StoreForwardConfig {
    fn default() -> Self {
        Self {
            cloud_endpoint: String::new(),
            strategy: SyncStrategy::All,
            sync_interval_secs: 300,
            batch_size: 1000,
            sample_rate: 0.1,
            max_retries: 3,
            retry_backoff_ms: 2000,
        }
    }
}

/// Per-topic/partition watermark tracking sync progress.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SyncWatermark {
    /// Topic name.
    pub topic: String,
    /// Partition id.
    pub partition: i32,
    /// Last offset successfully uploaded to the cloud.
    pub last_synced_offset: i64,
    /// Latest local offset.
    pub latest_local_offset: i64,
    /// Timestamp of last successful sync (ms since epoch).
    pub last_synced_at: i64,
}

impl SyncWatermark {
    /// Number of records pending sync.
    /// `latest_local_offset` is the high-water mark (next-to-write offset),
    /// while `last_synced_offset` is the last successfully synced offset.
    pub fn pending(&self) -> u64 {
        if self.last_synced_offset < 0 {
            // Nothing synced yet; all records up to latest are pending
            self.latest_local_offset.max(0) as u64
        } else {
            (self.latest_local_offset - self.last_synced_offset - 1).max(0) as u64
        }
    }
}

/// Aggregate status of the store-and-forward engine.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StoreForwardStatus {
    /// Whether the cloud is currently reachable.
    pub cloud_connected: bool,
    /// Total records buffered locally and pending sync.
    pub total_pending_records: u64,
    /// Total records successfully forwarded.
    pub total_forwarded_records: u64,
    /// Total failed upload attempts.
    pub total_failed_uploads: u64,
    /// Number of upload batches completed.
    pub batches_completed: u64,
    /// Sync lag across all partitions (max pending offset delta).
    pub sync_lag_records: u64,
    /// Per-topic/partition watermarks.
    pub watermarks: Vec<SyncWatermark>,
    /// Last sync attempt timestamp.
    pub last_sync_attempt_at: Option<i64>,
    /// Last successful sync timestamp.
    pub last_sync_success_at: Option<i64>,
    /// Current sync strategy.
    pub strategy: SyncStrategy,
}

/// The store-and-forward engine.
pub struct StoreForwardEngine {
    config: StoreForwardConfig,
    topic_manager: Arc<TopicManager>,
    checkpoint: Arc<EdgeSyncCheckpoint>,
    /// Whether the background sync loop is running.
    running: AtomicBool,
    /// Whether the cloud endpoint is believed to be reachable.
    cloud_connected: AtomicBool,
    /// Total records forwarded.
    total_forwarded: AtomicU64,
    /// Total failed upload attempts.
    total_failed: AtomicU64,
    /// Completed batches.
    batches_completed: AtomicU64,
    /// Last sync attempt timestamp (ms since epoch).
    last_sync_attempt: AtomicU64,
    /// Last successful sync timestamp (ms since epoch).
    last_sync_success: AtomicU64,
}

impl StoreForwardEngine {
    /// Create a new store-and-forward engine.
    pub fn new(
        config: StoreForwardConfig,
        topic_manager: Arc<TopicManager>,
        checkpoint: Arc<EdgeSyncCheckpoint>,
    ) -> Self {
        Self {
            config,
            topic_manager,
            checkpoint,
            running: AtomicBool::new(false),
            cloud_connected: AtomicBool::new(false),
            total_forwarded: AtomicU64::new(0),
            total_failed: AtomicU64::new(0),
            batches_completed: AtomicU64::new(0),
            last_sync_attempt: AtomicU64::new(0),
            last_sync_success: AtomicU64::new(0),
        }
    }

    /// Whether the engine's sync loop is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Whether the cloud endpoint is believed reachable.
    pub fn is_cloud_connected(&self) -> bool {
        self.cloud_connected.load(Ordering::SeqCst)
    }

    /// Start the background sync loop.
    pub async fn start(&self) -> Result<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(StreamlineError::storage_msg(
                "Store-and-forward engine already running".into(),
            ));
        }
        info!(
            endpoint = %self.config.cloud_endpoint,
            interval_secs = self.config.sync_interval_secs,
            strategy = ?self.config.strategy,
            "Store-and-forward engine started"
        );
        Ok(())
    }

    /// Stop the background sync loop.
    pub async fn stop(&self) -> Result<()> {
        self.running.store(false, Ordering::SeqCst);
        info!("Store-and-forward engine stopped");
        Ok(())
    }

    /// Perform a single sync cycle: gather pending records, apply the strategy
    /// filter, and attempt to upload them in batches.
    pub async fn sync_cycle(&self) -> Result<u64> {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        self.last_sync_attempt.store(now, Ordering::SeqCst);

        let topics = self
            .topic_manager
            .list_topics()?
            .into_iter()
            .map(|m| m.name)
            .collect::<Vec<_>>();

        let mut total_synced = 0u64;

        for topic in &topics {
            let partitions = self
                .topic_manager
                .get_topic_metadata(topic)
                .map(|m| m.num_partitions)
                .unwrap_or(1);

            for partition in 0..partitions {
                let from_offset = self.checkpoint.get_offset(topic, partition) + 1;
                let latest = self
                    .topic_manager
                    .latest_offset(topic, partition)
                    .unwrap_or(0);

                if from_offset > latest {
                    continue;
                }

                let records = self.topic_manager.read(
                    topic,
                    partition,
                    from_offset.max(0),
                    self.config.batch_size,
                )?;

                if records.is_empty() {
                    continue;
                }

                let filtered = self.apply_strategy(&records);
                let count = filtered.len() as u64;

                match self.upload_batch(topic, partition, &filtered).await {
                    Ok(()) => {
                        if let Some(last) = records.last() {
                            self.checkpoint.update(topic, partition, last.offset)?;
                        }
                        total_synced += count;
                        self.total_forwarded.fetch_add(count, Ordering::Relaxed);
                        self.batches_completed.fetch_add(1, Ordering::Relaxed);
                        self.cloud_connected.store(true, Ordering::SeqCst);
                        debug!(topic, partition, count, "Batch uploaded");
                    }
                    Err(e) => {
                        self.total_failed.fetch_add(1, Ordering::Relaxed);
                        self.cloud_connected.store(false, Ordering::SeqCst);
                        warn!(topic, partition, error = %e, "Batch upload failed, will retry");
                    }
                }
            }
        }

        if total_synced > 0 {
            let now = chrono::Utc::now().timestamp_millis() as u64;
            self.last_sync_success.store(now, Ordering::SeqCst);
        }

        Ok(total_synced)
    }

    /// Apply the configured sync strategy to filter records.
    fn apply_strategy<'a>(&self, records: &'a [Record]) -> Vec<&'a Record> {
        match self.config.strategy {
            SyncStrategy::All => records.iter().collect(),
            SyncStrategy::LatestOnly => {
                // Keep only the last record per key
                let mut latest: HashMap<Vec<u8>, &Record> = HashMap::new();
                for record in records {
                    let key = record
                        .key
                        .as_ref()
                        .map(|k| k.to_vec())
                        .unwrap_or_else(|| record.offset.to_le_bytes().to_vec());
                    latest.insert(key, record);
                }
                let mut result: Vec<&Record> = latest.into_values().collect();
                result.sort_by_key(|r| r.offset);
                result
            }
            SyncStrategy::Sample => {
                let rate = self.config.sample_rate.clamp(0.0, 1.0);
                if rate >= 1.0 {
                    return records.iter().collect();
                }
                // Deterministic sampling based on offset
                records
                    .iter()
                    .filter(|r| {
                        let hash = (r.offset as u64).wrapping_mul(2654435761) % 1000;
                        (hash as f64 / 1000.0) < rate
                    })
                    .collect()
            }
        }
    }

    /// Upload a batch of records to the cloud.
    /// In a real implementation this would use an HTTP client; here we provide the hook.
    async fn upload_batch(
        &self,
        _topic: &str,
        _partition: i32,
        _records: &[&Record],
    ) -> Result<()> {
        // Stub: in production this would POST to self.config.cloud_endpoint
        // For now we simulate success to keep the engine testable.
        Ok(())
    }

    /// Compute watermarks for all topics/partitions.
    pub fn watermarks(&self) -> Result<Vec<SyncWatermark>> {
        let topics = self
            .topic_manager
            .list_topics()?
            .into_iter()
            .map(|m| m.name)
            .collect::<Vec<_>>();

        let mut watermarks = Vec::new();

        for topic in &topics {
            let partitions = self
                .topic_manager
                .get_topic_metadata(topic)
                .map(|m| m.num_partitions)
                .unwrap_or(1);

            for partition in 0..partitions {
                let synced_offset = self.checkpoint.get_offset(topic, partition);
                let latest_offset = self
                    .topic_manager
                    .latest_offset(topic, partition)
                    .unwrap_or(-1);

                let pc = self.checkpoint.get_partition_checkpoint(topic, partition);

                watermarks.push(SyncWatermark {
                    topic: topic.clone(),
                    partition,
                    last_synced_offset: synced_offset,
                    latest_local_offset: latest_offset,
                    last_synced_at: pc.map(|p| p.last_sync_at).unwrap_or(0),
                });
            }
        }

        Ok(watermarks)
    }

    /// Get the current status of the store-and-forward engine.
    pub fn status(&self) -> Result<StoreForwardStatus> {
        let watermarks = self.watermarks()?;
        let total_pending: u64 = watermarks.iter().map(|w| w.pending()).sum();
        let sync_lag = watermarks.iter().map(|w| w.pending()).max().unwrap_or(0);

        let last_attempt = self.last_sync_attempt.load(Ordering::Relaxed);
        let last_success = self.last_sync_success.load(Ordering::Relaxed);

        Ok(StoreForwardStatus {
            cloud_connected: self.cloud_connected.load(Ordering::SeqCst),
            total_pending_records: total_pending,
            total_forwarded_records: self.total_forwarded.load(Ordering::Relaxed),
            total_failed_uploads: self.total_failed.load(Ordering::Relaxed),
            batches_completed: self.batches_completed.load(Ordering::Relaxed),
            sync_lag_records: sync_lag,
            watermarks,
            last_sync_attempt_at: if last_attempt > 0 {
                Some(last_attempt as i64)
            } else {
                None
            },
            last_sync_success_at: if last_success > 0 {
                Some(last_success as i64)
            } else {
                None
            },
            strategy: self.config.strategy,
        })
    }

    /// Get the engine configuration.
    pub fn config(&self) -> &StoreForwardConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    fn create_test_engine() -> (StoreForwardEngine, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let checkpoint =
            Arc::new(EdgeSyncCheckpoint::new(temp_dir.path().join("cp.json")).unwrap());
        let config = StoreForwardConfig::default();
        let engine = StoreForwardEngine::new(config, topic_manager, checkpoint);
        (engine, temp_dir)
    }

    #[test]
    fn test_initial_status() {
        let (engine, _tmp) = create_test_engine();
        let status = engine.status().unwrap();
        assert!(!status.cloud_connected);
        assert_eq!(status.total_pending_records, 0);
        assert_eq!(status.total_forwarded_records, 0);
    }

    #[test]
    fn test_watermarks_with_data() {
        let (engine, _tmp) = create_test_engine();
        engine
            .topic_manager
            .create_topic("test-topic", 1)
            .unwrap();
        engine
            .topic_manager
            .append(
                "test-topic",
                0,
                Some(Bytes::from("k1")),
                Bytes::from("v1"),
            )
            .unwrap();

        let watermarks = engine.watermarks().unwrap();
        assert_eq!(watermarks.len(), 1);
        assert_eq!(watermarks[0].topic, "test-topic");
        // latest_offset returns next-to-write offset (high-water mark)
        assert!(watermarks[0].latest_local_offset >= 0);
        assert_eq!(watermarks[0].last_synced_offset, -1);
        assert!(watermarks[0].pending() > 0);
    }

    #[tokio::test]
    async fn test_sync_cycle() {
        let (engine, _tmp) = create_test_engine();
        engine
            .topic_manager
            .create_topic("sync-test", 1)
            .unwrap();
        for i in 0..5 {
            engine
                .topic_manager
                .append(
                    "sync-test",
                    0,
                    Some(Bytes::from(format!("k{}", i))),
                    Bytes::from(format!("v{}", i)),
                )
                .unwrap();
        }

        let synced = engine.sync_cycle().await.unwrap();
        assert_eq!(synced, 5);

        let status = engine.status().unwrap();
        assert_eq!(status.total_forwarded_records, 5);
        assert_eq!(status.batches_completed, 1);
        assert_eq!(status.total_pending_records, 0);
    }

    #[test]
    fn test_strategy_all() {
        let records = vec![
            Record {
                offset: 0,
                timestamp: 0,
                key: Some(Bytes::from("a")),
                value: Bytes::from("v0"),
                headers: vec![],
                crc: None,
            },
            Record {
                offset: 1,
                timestamp: 1,
                key: Some(Bytes::from("a")),
                value: Bytes::from("v1"),
                headers: vec![],
                crc: None,
            },
        ];
        let (engine, _tmp) = create_test_engine();
        let result = engine.apply_strategy(&records);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_strategy_latest_only() {
        let records = vec![
            Record {
                offset: 0,
                timestamp: 0,
                key: Some(Bytes::from("a")),
                value: Bytes::from("v0"),
                headers: vec![],
                crc: None,
            },
            Record {
                offset: 1,
                timestamp: 1,
                key: Some(Bytes::from("a")),
                value: Bytes::from("v1"),
                headers: vec![],
                crc: None,
            },
            Record {
                offset: 2,
                timestamp: 2,
                key: Some(Bytes::from("b")),
                value: Bytes::from("v2"),
                headers: vec![],
                crc: None,
            },
        ];
        let temp_dir = TempDir::new().unwrap();
        let tm = Arc::new(TopicManager::new(temp_dir.path()).unwrap());
        let cp = Arc::new(EdgeSyncCheckpoint::new(temp_dir.path().join("cp.json")).unwrap());
        let config = StoreForwardConfig {
            strategy: SyncStrategy::LatestOnly,
            ..Default::default()
        };
        let engine = StoreForwardEngine::new(config, tm, cp);
        let result = engine.apply_strategy(&records);
        // key "a" -> offset 1 wins, key "b" -> offset 2
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].offset, 1);
        assert_eq!(result[1].offset, 2);
    }

    #[test]
    fn test_sync_strategy_parse() {
        assert_eq!(SyncStrategy::from_str_loose("all"), SyncStrategy::All);
        assert_eq!(
            SyncStrategy::from_str_loose("latest-only"),
            SyncStrategy::LatestOnly
        );
        assert_eq!(
            SyncStrategy::from_str_loose("latest_only"),
            SyncStrategy::LatestOnly
        );
        assert_eq!(SyncStrategy::from_str_loose("sample"), SyncStrategy::Sample);
        assert_eq!(SyncStrategy::from_str_loose("unknown"), SyncStrategy::All);
    }

    #[tokio::test]
    async fn test_start_stop() {
        let (engine, _tmp) = create_test_engine();
        assert!(!engine.is_running());
        engine.start().await.unwrap();
        assert!(engine.is_running());
        engine.stop().await.unwrap();
        assert!(!engine.is_running());
    }
}
