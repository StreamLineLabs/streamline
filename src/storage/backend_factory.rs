//! Backend factory for storage mode-aware component creation
//!
//! This module provides factory functions that create the appropriate
//! storage backends based on the configured storage mode (Local, Hybrid, Diskless).
//!
//! ## Usage
//!
//! ```ignore
//! use streamline::storage::backend_factory::{BackendFactory, BackendFactoryConfig};
//!
//! let config = BackendFactoryConfig::new(StorageMode::Diskless)
//!     .with_object_store(store)
//!     .with_remote_config(remote_config);
//!
//! let factory = BackendFactory::new(config);
//!
//! // Create backends for a partition
//! let segment = factory.create_segment("my-topic", 0, 0).await?;
//! let wal = factory.create_wal("my-topic", 0).await?;
//! ```

use crate::config::WalConfig;
use crate::error::{Result, StreamlineError};
#[cfg(feature = "cloud-storage")]
use crate::storage::backend::PartitionManifest;
use crate::storage::backend::{SegmentBackend, WalBackend};
use crate::storage::segment::Segment;
#[cfg(feature = "cloud-storage")]
use crate::storage::segment_s3::S3Segment;
use crate::storage::storage_mode::{RemoteStorageConfig, StorageMode};
use crate::storage::wal::WalWriter;
#[cfg(feature = "cloud-storage")]
use crate::storage::wal_s3::S3WalWriter;
#[cfg(feature = "cloud-storage")]
use futures_util::TryStreamExt;
#[cfg(feature = "cloud-storage")]
use object_store::path::Path as ObjectPath;
#[cfg(feature = "cloud-storage")]
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};
#[cfg(feature = "cloud-storage")]
use std::collections::HashMap;
use std::path::PathBuf;
#[cfg(feature = "cloud-storage")]
use std::sync::Arc;
use tracing::{info, warn};

/// Information about a discovered partition in object storage
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionInfo {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
}

/// Recovery information for a single partition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionRecoveryInfo {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
    /// Number of segments found
    pub segment_count: usize,
    /// Log end offset (next offset to write)
    pub log_end_offset: i64,
    /// High watermark
    pub high_watermark: i64,
    /// Total size in bytes across all segments
    pub total_size_bytes: u64,
    /// Whether WAL data was found
    pub has_wal: bool,
    /// Recovery status
    pub status: RecoveryStatus,
    /// Error message if recovery failed
    pub error: Option<String>,
}

/// Status of partition recovery
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryStatus {
    /// Partition not found in storage
    NotFound,
    /// Recovery successful
    Recovered,
    /// Recovery failed with error
    Failed,
    /// Empty partition (manifest exists but no data)
    Empty,
}

impl std::fmt::Display for RecoveryStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecoveryStatus::NotFound => write!(f, "not_found"),
            RecoveryStatus::Recovered => write!(f, "recovered"),
            RecoveryStatus::Failed => write!(f, "failed"),
            RecoveryStatus::Empty => write!(f, "empty"),
        }
    }
}

/// Summary of cluster-wide recovery
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoverySummary {
    /// Total partitions discovered
    pub total_partitions: usize,
    /// Successfully recovered partitions
    pub recovered: usize,
    /// Failed partitions
    pub failed: usize,
    /// Empty partitions
    pub empty: usize,
    /// Not found partitions
    pub not_found: usize,
    /// Total bytes across all partitions
    pub total_bytes: u64,
    /// Recovery details per partition
    pub partitions: Vec<PartitionRecoveryInfo>,
}

impl RecoverySummary {
    /// Add a partition recovery result to the summary
    pub fn add(&mut self, info: PartitionRecoveryInfo) {
        match info.status {
            RecoveryStatus::Recovered => self.recovered += 1,
            RecoveryStatus::Failed => self.failed += 1,
            RecoveryStatus::Empty => self.empty += 1,
            RecoveryStatus::NotFound => self.not_found += 1,
        }
        self.total_bytes += info.total_size_bytes;
        self.total_partitions += 1;
        self.partitions.push(info);
    }

    /// Check if all partitions recovered successfully
    pub fn is_complete(&self) -> bool {
        self.failed == 0 && self.not_found == 0
    }
}

/// Configuration for the backend factory
#[derive(Debug, Clone)]
pub struct BackendFactoryConfig {
    /// Storage mode to use
    pub storage_mode: StorageMode,

    /// Local storage path (for Local and Hybrid modes)
    pub local_path: Option<PathBuf>,

    /// Object store for remote storage (for Hybrid and Diskless modes)
    #[cfg(feature = "cloud-storage")]
    pub object_store: Option<Arc<dyn ObjectStore>>,

    /// Remote storage configuration
    pub remote_config: RemoteStorageConfig,
}

impl Default for BackendFactoryConfig {
    fn default() -> Self {
        Self {
            storage_mode: StorageMode::Local,
            local_path: None,
            #[cfg(feature = "cloud-storage")]
            object_store: None,
            remote_config: RemoteStorageConfig::default(),
        }
    }
}

impl BackendFactoryConfig {
    /// Create a new configuration for the given storage mode
    pub fn new(storage_mode: StorageMode) -> Self {
        Self {
            storage_mode,
            ..Default::default()
        }
    }

    /// Create a configuration for local storage
    pub fn local(base_path: impl Into<PathBuf>) -> Self {
        Self {
            storage_mode: StorageMode::Local,
            local_path: Some(base_path.into()),
            #[cfg(feature = "cloud-storage")]
            object_store: None,
            remote_config: RemoteStorageConfig::default(),
        }
    }

    /// Create a configuration for hybrid storage
    #[cfg(feature = "cloud-storage")]
    pub fn hybrid(
        base_path: impl Into<PathBuf>,
        object_store: Arc<dyn ObjectStore>,
        remote_config: RemoteStorageConfig,
    ) -> Self {
        Self {
            storage_mode: StorageMode::Hybrid,
            local_path: Some(base_path.into()),
            object_store: Some(object_store),
            remote_config,
        }
    }

    /// Create a configuration for diskless storage
    #[cfg(feature = "cloud-storage")]
    pub fn diskless(
        object_store: Arc<dyn ObjectStore>,
        remote_config: RemoteStorageConfig,
    ) -> Self {
        Self {
            storage_mode: StorageMode::Diskless,
            local_path: None,
            object_store: Some(object_store),
            remote_config,
        }
    }

    /// Set the local path
    pub fn with_local_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.local_path = Some(path.into());
        self
    }

    /// Set the object store
    #[cfg(feature = "cloud-storage")]
    pub fn with_object_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(store);
        self
    }

    /// Set the remote storage configuration
    pub fn with_remote_config(mut self, config: RemoteStorageConfig) -> Self {
        self.remote_config = config;
        self
    }

    /// Validate the configuration for the selected storage mode
    pub fn validate(&self) -> Result<()> {
        match self.storage_mode {
            StorageMode::Local => {
                if self.local_path.is_none() {
                    return Err(StreamlineError::Config(
                        "Local storage mode requires local_path".to_string(),
                    ));
                }
            }
            #[cfg(feature = "cloud-storage")]
            StorageMode::Hybrid => {
                if self.local_path.is_none() {
                    return Err(StreamlineError::Config(
                        "Hybrid storage mode requires local_path".to_string(),
                    ));
                }
                if self.object_store.is_none() {
                    return Err(StreamlineError::Config(
                        "Hybrid storage mode requires object_store".to_string(),
                    ));
                }
            }
            #[cfg(feature = "cloud-storage")]
            StorageMode::Diskless => {
                if self.object_store.is_none() {
                    return Err(StreamlineError::Config(
                        "Diskless storage mode requires object_store".to_string(),
                    ));
                }
            }
            #[cfg(not(feature = "cloud-storage"))]
            StorageMode::Hybrid | StorageMode::Diskless => {
                return Err(StreamlineError::Config(
                    "Hybrid/Diskless storage modes require the 'cloud-storage' feature".to_string(),
                ));
            }
        }
        Ok(())
    }
}

/// Factory for creating storage backends based on configuration
///
/// The factory creates appropriate WAL and Segment backends based on the
/// configured storage mode. This allows transparent switching between
/// local disk, hybrid, and fully diskless storage.
#[derive(Debug)]
pub struct BackendFactory {
    config: BackendFactoryConfig,
}

impl BackendFactory {
    /// Create a new backend factory with the given configuration
    pub fn new(config: BackendFactoryConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { config })
    }

    /// Get the storage mode
    pub fn storage_mode(&self) -> StorageMode {
        self.config.storage_mode
    }

    /// Check if this factory uses local WAL
    pub fn uses_local_wal(&self) -> bool {
        self.config.storage_mode.uses_local_wal()
    }

    /// Check if this factory uses local segments
    pub fn uses_local_segments(&self) -> bool {
        self.config.storage_mode.uses_local_segments()
    }

    /// Check if this factory requires replication
    pub fn requires_replication(&self) -> bool {
        self.config.storage_mode.requires_replication()
    }

    /// Create a WAL backend for a partition
    ///
    /// Returns a boxed trait object for the appropriate WAL backend:
    /// - Local/Hybrid: Local WAL writer
    /// - Diskless: S3 WAL writer (requires cloud-storage feature)
    pub async fn create_wal(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Box<dyn WalBackend + Send + Sync>> {
        match self.config.storage_mode {
            StorageMode::Local => {
                let local_path = self.config.local_path.as_ref().ok_or_else(|| {
                    StreamlineError::Config("local_path required for Local".into())
                })?;
                let wal_path = local_path
                    .join("topics")
                    .join(topic)
                    .join(format!("partition-{}", partition))
                    .join("wal");

                std::fs::create_dir_all(&wal_path)?;

                info!(
                    topic = %topic,
                    partition = partition,
                    mode = %self.config.storage_mode,
                    path = ?wal_path,
                    "Creating local WAL backend"
                );

                // Use local WAL wrapped in trait object
                let wal = WalWriter::new(&wal_path, WalConfig::default())?;
                Ok(Box::new(LocalWalAdapter { inner: wal }))
            }
            #[cfg(feature = "cloud-storage")]
            StorageMode::Hybrid => {
                let local_path = self.config.local_path.as_ref().ok_or_else(|| {
                    StreamlineError::Config("local_path required for Hybrid".into())
                })?;
                let wal_path = local_path
                    .join("topics")
                    .join(topic)
                    .join(format!("partition-{}", partition))
                    .join("wal");

                std::fs::create_dir_all(&wal_path)?;

                info!(
                    topic = %topic,
                    partition = partition,
                    mode = %self.config.storage_mode,
                    path = ?wal_path,
                    "Creating local WAL backend (hybrid mode)"
                );

                let wal = WalWriter::new(&wal_path, WalConfig::default())?;
                Ok(Box::new(LocalWalAdapter { inner: wal }))
            }
            #[cfg(feature = "cloud-storage")]
            StorageMode::Diskless => {
                let store = self.config.object_store.clone().ok_or_else(|| {
                    StreamlineError::Config("object_store required for Diskless".into())
                })?;

                info!(
                    topic = %topic,
                    partition = partition,
                    mode = %self.config.storage_mode,
                    "Creating S3 WAL backend"
                );

                let wal = S3WalWriter::new_with_store(
                    store,
                    topic,
                    partition,
                    self.config.remote_config.batch_size_bytes,
                    self.config.remote_config.batch_timeout_ms,
                )
                .await?;

                Ok(Box::new(wal))
            }
            #[cfg(not(feature = "cloud-storage"))]
            StorageMode::Hybrid | StorageMode::Diskless => Err(StreamlineError::Config(
                "Hybrid/Diskless storage modes require the 'cloud-storage' feature".to_string(),
            )),
        }
    }

    /// Create a segment backend for a partition
    ///
    /// Returns a boxed trait object for the appropriate segment backend:
    /// - Local: Local segment
    /// - Hybrid/Diskless: S3 segment (requires cloud-storage feature)
    pub async fn create_segment(
        &self,
        topic: &str,
        partition: i32,
        base_offset: i64,
    ) -> Result<Box<dyn SegmentBackend + Send + Sync>> {
        match self.config.storage_mode {
            StorageMode::Local => {
                let local_path = self.config.local_path.as_ref().ok_or_else(|| {
                    StreamlineError::Config("local_path required for Local".into())
                })?;
                let partition_path = local_path
                    .join("topics")
                    .join(topic)
                    .join(format!("partition-{}", partition));
                let segment_path = partition_path.join(format!("{:020}.segment", base_offset));

                // Ensure partition directory exists
                std::fs::create_dir_all(&partition_path)?;

                info!(
                    topic = %topic,
                    partition = partition,
                    base_offset = base_offset,
                    mode = %self.config.storage_mode,
                    path = ?segment_path,
                    "Creating local segment backend"
                );

                let segment = Segment::create(&segment_path, base_offset)?;
                Ok(Box::new(LocalSegmentAdapter { inner: segment }))
            }
            #[cfg(feature = "cloud-storage")]
            StorageMode::Hybrid | StorageMode::Diskless => {
                let store = self.config.object_store.clone().ok_or_else(|| {
                    StreamlineError::Config("object_store required for Hybrid/Diskless".into())
                })?;

                info!(
                    topic = %topic,
                    partition = partition,
                    base_offset = base_offset,
                    mode = %self.config.storage_mode,
                    "Creating S3 segment backend"
                );

                let segment = S3Segment::new_with_store(
                    store,
                    topic,
                    partition,
                    base_offset,
                    self.config.remote_config.batch_size_bytes,
                    self.config.remote_config.batch_timeout_ms,
                )
                .await?;

                Ok(Box::new(segment))
            }
            #[cfg(not(feature = "cloud-storage"))]
            StorageMode::Hybrid | StorageMode::Diskless => Err(StreamlineError::Config(
                "Hybrid/Diskless storage modes require the 'cloud-storage' feature".to_string(),
            )),
        }
    }

    /// Open an existing segment from storage
    ///
    /// For local storage, opens the segment file at the given path.
    /// For remote storage, loads segment metadata from the manifest.
    pub async fn open_segment(
        &self,
        topic: &str,
        partition: i32,
        base_offset: i64,
    ) -> Result<Box<dyn SegmentBackend + Send + Sync>> {
        match self.config.storage_mode {
            StorageMode::Local => {
                let local_path = self.config.local_path.as_ref().ok_or_else(|| {
                    StreamlineError::Config("local_path required for Local".into())
                })?;
                let segment_path = local_path
                    .join("topics")
                    .join(topic)
                    .join(format!("partition-{}", partition))
                    .join(format!("{:020}.segment", base_offset));

                if !segment_path.exists() {
                    return Err(StreamlineError::storage_msg(format!(
                        "Segment not found: {:?}",
                        segment_path
                    )));
                }

                let segment = Segment::open(&segment_path)?;
                Ok(Box::new(LocalSegmentAdapter { inner: segment }))
            }
            #[cfg(feature = "cloud-storage")]
            StorageMode::Hybrid | StorageMode::Diskless => {
                let store = self.config.object_store.clone().ok_or_else(|| {
                    StreamlineError::Config("object_store required for Hybrid/Diskless".into())
                })?;

                // For S3, we create a new segment instance that will load from the manifest
                let segment = S3Segment::new_with_store(
                    store,
                    topic,
                    partition,
                    base_offset,
                    self.config.remote_config.batch_size_bytes,
                    self.config.remote_config.batch_timeout_ms,
                )
                .await?;

                Ok(Box::new(segment))
            }
            #[cfg(not(feature = "cloud-storage"))]
            StorageMode::Hybrid | StorageMode::Diskless => Err(StreamlineError::Config(
                "Hybrid/Diskless storage modes require the 'cloud-storage' feature".to_string(),
            )),
        }
    }

    /// Get the local partition path (if applicable)
    pub fn partition_path(&self, topic: &str, partition: i32) -> Option<PathBuf> {
        self.config.local_path.as_ref().map(|base| {
            base.join("topics")
                .join(topic)
                .join(format!("partition-{}", partition))
        })
    }

    /// Get the object store reference (if configured)
    #[cfg(feature = "cloud-storage")]
    pub fn object_store(&self) -> Option<&Arc<dyn ObjectStore>> {
        self.config.object_store.as_ref()
    }

    /// Get the remote storage configuration
    pub fn remote_config(&self) -> &RemoteStorageConfig {
        &self.config.remote_config
    }

    /// Discover all partitions in storage
    ///
    /// Scans the object store (if cloud-storage enabled) or local directories
    /// for partition manifests and returns a list of topic/partition combinations found.
    pub async fn discover_partitions(&self) -> Result<Vec<PartitionInfo>> {
        #[cfg(feature = "cloud-storage")]
        {
            let store = match &self.config.object_store {
                Some(s) => s.clone(),
                None => {
                    // For local mode, scan local directories
                    return self.discover_local_partitions();
                }
            };

            let mut partitions = Vec::new();
            let prefix = ObjectPath::from("v1/topics/");

            // List all objects under the topics prefix
            let list_result = store.list(Some(&prefix));
            let objects: Vec<_> = list_result.try_collect().await.map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to list objects: {}", e))
            })?;

            // Track unique topic/partition combinations by looking for manifest.json files
            let mut seen: HashMap<(String, i32), bool> = HashMap::new();

            for obj in objects {
                let path_str = obj.location.to_string();
                // Path format: v1/topics/{topic}/partition-{N}/manifest.json
                if path_str.ends_with("/manifest.json") {
                    if let Some((topic, partition)) = Self::parse_partition_path(&path_str) {
                        use std::collections::hash_map::Entry;
                        if let Entry::Vacant(e) = seen.entry((topic.clone(), partition)) {
                            e.insert(true);
                            partitions.push(PartitionInfo { topic, partition });
                        }
                    }
                }
            }

            info!(
                partition_count = partitions.len(),
                "Discovered partitions in object storage"
            );

            Ok(partitions)
        }

        #[cfg(not(feature = "cloud-storage"))]
        {
            self.discover_local_partitions()
        }
    }

    /// Parse topic and partition from a manifest path
    #[cfg(feature = "cloud-storage")]
    fn parse_partition_path(path: &str) -> Option<(String, i32)> {
        // Expected format: v1/topics/{topic}/partition-{N}/manifest.json
        let parts: Vec<&str> = path.split('/').collect();
        if parts.len() >= 4 {
            let topic_idx = parts.iter().position(|&p| p == "topics")?;
            if topic_idx + 2 < parts.len() {
                let topic = parts[topic_idx + 1].to_string();
                let partition_str = parts[topic_idx + 2];
                if partition_str.starts_with("partition-") {
                    let partition: i32 = partition_str.strip_prefix("partition-")?.parse().ok()?;
                    return Some((topic, partition));
                }
            }
        }
        None
    }

    /// Discover partitions from local storage
    fn discover_local_partitions(&self) -> Result<Vec<PartitionInfo>> {
        let base_path = match &self.config.local_path {
            Some(p) => p,
            None => return Ok(Vec::new()),
        };

        let topics_dir = base_path.join("topics");
        if !topics_dir.exists() {
            return Ok(Vec::new());
        }

        let mut partitions = Vec::new();

        for topic_entry in std::fs::read_dir(&topics_dir)? {
            let topic_entry = topic_entry?;
            if !topic_entry.file_type()?.is_dir() {
                continue;
            }

            let topic = topic_entry.file_name().to_string_lossy().to_string();
            let topic_path = topic_entry.path();

            for partition_entry in std::fs::read_dir(&topic_path)? {
                let partition_entry = partition_entry?;
                if !partition_entry.file_type()?.is_dir() {
                    continue;
                }

                let dir_name = partition_entry.file_name().to_string_lossy().to_string();
                if let Some(partition_str) = dir_name.strip_prefix("partition-") {
                    if let Ok(partition) = partition_str.parse::<i32>() {
                        partitions.push(PartitionInfo {
                            topic: topic.clone(),
                            partition,
                        });
                    }
                }
            }
        }

        info!(
            partition_count = partitions.len(),
            "Discovered local partitions"
        );

        Ok(partitions)
    }

    /// Get recovery information for a specific partition
    ///
    /// Loads the partition manifest from storage and returns recovery status.
    pub async fn get_partition_recovery_info(
        &self,
        topic: &str,
        partition: i32,
    ) -> PartitionRecoveryInfo {
        match self.config.storage_mode {
            StorageMode::Local => self.get_local_recovery_info(topic, partition),
            #[cfg(feature = "cloud-storage")]
            StorageMode::Hybrid | StorageMode::Diskless => {
                self.get_remote_recovery_info(topic, partition).await
            }
            #[cfg(not(feature = "cloud-storage"))]
            StorageMode::Hybrid | StorageMode::Diskless => PartitionRecoveryInfo {
                topic: topic.to_string(),
                partition,
                segment_count: 0,
                log_end_offset: 0,
                high_watermark: 0,
                total_size_bytes: 0,
                has_wal: false,
                status: RecoveryStatus::Failed,
                error: Some("Hybrid/Diskless modes require 'cloud-storage' feature".to_string()),
            },
        }
    }

    /// Get recovery info from local storage
    fn get_local_recovery_info(&self, topic: &str, partition: i32) -> PartitionRecoveryInfo {
        let base_info = PartitionRecoveryInfo {
            topic: topic.to_string(),
            partition,
            segment_count: 0,
            log_end_offset: 0,
            high_watermark: 0,
            total_size_bytes: 0,
            has_wal: false,
            status: RecoveryStatus::NotFound,
            error: None,
        };

        let partition_path = match self.partition_path(topic, partition) {
            Some(p) => p,
            None => return base_info,
        };

        if !partition_path.exists() {
            return base_info;
        }

        // Count segment files and calculate total size
        let mut segment_count = 0;
        let mut total_size: u64 = 0;
        let mut max_offset: i64 = 0;

        if let Ok(entries) = std::fs::read_dir(&partition_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().map(|e| e == "segment").unwrap_or(false) {
                    segment_count += 1;
                    if let Ok(meta) = std::fs::metadata(&path) {
                        total_size += meta.len();
                    }
                    // Parse offset from filename
                    if let Some(stem) = path.file_stem() {
                        if let Ok(offset) = stem.to_string_lossy().parse::<i64>() {
                            max_offset = max_offset.max(offset);
                        }
                    }
                }
            }
        }

        // Check for WAL directory
        let wal_path = partition_path.join("wal");
        let has_wal = wal_path.exists();

        if segment_count == 0 && !has_wal {
            return PartitionRecoveryInfo {
                status: RecoveryStatus::Empty,
                ..base_info
            };
        }

        PartitionRecoveryInfo {
            topic: topic.to_string(),
            partition,
            segment_count,
            log_end_offset: max_offset,
            high_watermark: max_offset,
            total_size_bytes: total_size,
            has_wal,
            status: RecoveryStatus::Recovered,
            error: None,
        }
    }

    /// Get recovery info from remote storage
    #[cfg(feature = "cloud-storage")]
    async fn get_remote_recovery_info(&self, topic: &str, partition: i32) -> PartitionRecoveryInfo {
        let base_info = PartitionRecoveryInfo {
            topic: topic.to_string(),
            partition,
            segment_count: 0,
            log_end_offset: 0,
            high_watermark: 0,
            total_size_bytes: 0,
            has_wal: false,
            status: RecoveryStatus::NotFound,
            error: None,
        };

        let store = match &self.config.object_store {
            Some(s) => s.clone(),
            None => return base_info,
        };

        // Try to load partition manifest
        let manifest_path = ObjectPath::from(PartitionManifest::manifest_path(topic, partition));

        match store.get(&manifest_path).await {
            Ok(result) => {
                let data = match result.bytes().await {
                    Ok(d) => d,
                    Err(e) => {
                        return PartitionRecoveryInfo {
                            status: RecoveryStatus::Failed,
                            error: Some(format!("Failed to read manifest: {}", e)),
                            ..base_info
                        };
                    }
                };

                match PartitionManifest::from_json(&data) {
                    Ok(manifest) => {
                        let total_size: u64 = manifest.segments.iter().map(|s| s.size_bytes).sum();
                        let segment_count = manifest.segments.len();

                        if segment_count == 0 {
                            return PartitionRecoveryInfo {
                                log_end_offset: manifest.log_end_offset,
                                high_watermark: manifest.high_watermark,
                                status: RecoveryStatus::Empty,
                                ..base_info
                            };
                        }

                        // Check for WAL manifest
                        let wal_manifest_path = ObjectPath::from(format!(
                            "v1/topics/{}/partition-{}/wal/manifest.json",
                            topic, partition
                        ));
                        let has_wal = store.head(&wal_manifest_path).await.is_ok();

                        PartitionRecoveryInfo {
                            topic: topic.to_string(),
                            partition,
                            segment_count,
                            log_end_offset: manifest.log_end_offset,
                            high_watermark: manifest.high_watermark,
                            total_size_bytes: total_size,
                            has_wal,
                            status: RecoveryStatus::Recovered,
                            error: None,
                        }
                    }
                    Err(e) => PartitionRecoveryInfo {
                        status: RecoveryStatus::Failed,
                        error: Some(format!("Failed to parse manifest: {}", e)),
                        ..base_info
                    },
                }
            }
            Err(object_store::Error::NotFound { .. }) => base_info,
            Err(e) => PartitionRecoveryInfo {
                status: RecoveryStatus::Failed,
                error: Some(format!("Failed to get manifest: {}", e)),
                ..base_info
            },
        }
    }

    /// Recover all partitions from storage
    ///
    /// Discovers all partitions and attempts to recover each one,
    /// returning a summary of the recovery process.
    pub async fn recover_all_partitions(&self) -> Result<RecoverySummary> {
        let partitions = self.discover_partitions().await?;
        let mut summary = RecoverySummary::default();

        for partition_info in partitions {
            let recovery_info = self
                .get_partition_recovery_info(&partition_info.topic, partition_info.partition)
                .await;

            match recovery_info.status {
                RecoveryStatus::Recovered => {
                    info!(
                        topic = %partition_info.topic,
                        partition = partition_info.partition,
                        log_end_offset = recovery_info.log_end_offset,
                        segments = recovery_info.segment_count,
                        "Partition recovered successfully"
                    );
                }
                RecoveryStatus::Failed => {
                    warn!(
                        topic = %partition_info.topic,
                        partition = partition_info.partition,
                        error = ?recovery_info.error,
                        "Failed to recover partition"
                    );
                }
                RecoveryStatus::Empty => {
                    info!(
                        topic = %partition_info.topic,
                        partition = partition_info.partition,
                        "Partition is empty"
                    );
                }
                RecoveryStatus::NotFound => {
                    warn!(
                        topic = %partition_info.topic,
                        partition = partition_info.partition,
                        "Partition manifest not found"
                    );
                }
            }

            summary.add(recovery_info);
        }

        info!(
            total = summary.total_partitions,
            recovered = summary.recovered,
            failed = summary.failed,
            empty = summary.empty,
            total_bytes = summary.total_bytes,
            "Partition recovery complete"
        );

        Ok(summary)
    }

    /// Store a partition manifest to object storage
    ///
    /// Used during recovery to persist partition state.
    #[cfg(feature = "cloud-storage")]
    pub async fn save_manifest(&self, manifest: &PartitionManifest) -> Result<()> {
        let store = self.config.object_store.as_ref().ok_or_else(|| {
            StreamlineError::Config("object_store required for saving manifest".into())
        })?;

        let path = ObjectPath::from(PartitionManifest::manifest_path(
            &manifest.topic,
            manifest.partition,
        ));
        let data = manifest.to_json()?;

        store
            .put(&path, PutPayload::from_bytes(data))
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to save manifest: {}", e)))?;

        info!(
            topic = %manifest.topic,
            partition = manifest.partition,
            log_end_offset = manifest.log_end_offset,
            segments = manifest.segments.len(),
            "Saved partition manifest"
        );

        Ok(())
    }

    /// Load a partition manifest from object storage
    #[cfg(feature = "cloud-storage")]
    pub async fn load_manifest(&self, topic: &str, partition: i32) -> Result<PartitionManifest> {
        let store = self.config.object_store.as_ref().ok_or_else(|| {
            StreamlineError::Config("object_store required for loading manifest".into())
        })?;

        let path = ObjectPath::from(PartitionManifest::manifest_path(topic, partition));

        let result = store.get(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => StreamlineError::storage_msg(format!(
                "Manifest not found for {}/{}",
                topic, partition
            )),
            _ => StreamlineError::storage_msg(format!("Failed to get manifest: {}", e)),
        })?;

        let data = result
            .bytes()
            .await
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to read manifest: {}", e)))?;

        PartitionManifest::from_json(&data)
    }
}

/// Adapter to wrap local WalWriter as WalBackend trait object
struct LocalWalAdapter {
    inner: WalWriter,
}

impl std::fmt::Debug for LocalWalAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalWalAdapter")
            .field("next_sequence", &self.inner.next_sequence())
            .finish()
    }
}

#[async_trait::async_trait]
impl WalBackend for LocalWalAdapter {
    async fn append(&mut self, entry: crate::storage::wal::WalEntry) -> Result<u64> {
        let sequence = self.inner.append(&entry)?;
        Ok(sequence)
    }

    async fn flush(&mut self) -> Result<()> {
        self.inner.sync()
    }

    async fn read_from(&self, sequence: u64) -> Result<Vec<crate::storage::wal::WalEntry>> {
        // For local WAL, we need to read from the reader
        // This is a limitation - local WAL doesn't support reading by sequence easily
        // Return empty for now - recovery uses WalReader directly
        let _ = sequence;
        Ok(Vec::new())
    }

    fn storage_mode(&self) -> StorageMode {
        StorageMode::Local
    }

    fn current_sequence(&self) -> u64 {
        self.inner.next_sequence()
    }

    fn has_pending(&self) -> bool {
        false // Local WAL writes synchronously
    }
}

/// Adapter to wrap local Segment as SegmentBackend trait object
struct LocalSegmentAdapter {
    inner: Segment,
}

impl std::fmt::Debug for LocalSegmentAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalSegmentAdapter")
            .field("base_offset", &self.inner.base_offset())
            .field("is_sealed", &self.inner.is_sealed())
            .field("path", &self.inner.path())
            .finish()
    }
}

#[async_trait::async_trait]
impl SegmentBackend for LocalSegmentAdapter {
    async fn append_batch(&mut self, batch: &crate::storage::record::RecordBatch) -> Result<i64> {
        // Append each record in the batch
        let base_offset = batch.base_offset;
        for record in &batch.records {
            self.inner.append_record(record.clone())?;
        }
        Ok(base_offset)
    }

    async fn read_from_offset(
        &self,
        offset: i64,
        max_bytes: usize,
    ) -> Result<Vec<crate::storage::record::Record>> {
        // Local segment read is synchronous
        self.inner.read_from_offset(offset, max_bytes)
    }

    async fn seal(&mut self) -> Result<()> {
        self.inner.seal()
    }

    fn base_offset(&self) -> i64 {
        self.inner.base_offset()
    }

    fn log_end_offset(&self) -> i64 {
        self.inner.max_offset() + 1
    }

    fn storage_mode(&self) -> StorageMode {
        StorageMode::Local
    }

    fn size_bytes(&self) -> u64 {
        self.inner.size()
    }

    fn is_sealed(&self) -> bool {
        self.inner.is_sealed()
    }

    fn identifier(&self) -> String {
        self.inner.path().to_string_lossy().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "cloud-storage")]
    use object_store::memory::InMemory;
    use tempfile::TempDir;

    #[test]
    fn test_config_local() {
        let config = BackendFactoryConfig::local("/tmp/test");
        assert_eq!(config.storage_mode, StorageMode::Local);
        assert!(config.local_path.is_some());
        assert!(config.validate().is_ok());
    }

    #[test]
    #[cfg(feature = "cloud-storage")]
    fn test_config_diskless() {
        let store = Arc::new(InMemory::new());
        let config = BackendFactoryConfig::diskless(store, RemoteStorageConfig::default());
        assert_eq!(config.storage_mode, StorageMode::Diskless);
        assert!(config.object_store.is_some());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_fails_without_local_path() {
        let config = BackendFactoryConfig::new(StorageMode::Local);
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_validation_fails_without_object_store() {
        let config = BackendFactoryConfig::new(StorageMode::Diskless);
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn test_factory_create_local_segment() {
        let temp_dir = TempDir::new().unwrap();
        let config = BackendFactoryConfig::local(temp_dir.path());
        let factory = BackendFactory::new(config).unwrap();

        let segment = factory.create_segment("test-topic", 0, 0).await.unwrap();
        assert_eq!(segment.base_offset(), 0);
        assert_eq!(segment.storage_mode(), StorageMode::Local);
    }

    #[tokio::test]
    #[cfg(feature = "cloud-storage")]
    async fn test_factory_create_diskless_segment() {
        let store = Arc::new(InMemory::new());
        let config = BackendFactoryConfig::diskless(store, RemoteStorageConfig::default());
        let factory = BackendFactory::new(config).unwrap();

        let segment = factory.create_segment("test-topic", 0, 0).await.unwrap();
        assert_eq!(segment.base_offset(), 0);
        assert_eq!(segment.storage_mode(), StorageMode::Diskless);
    }

    #[tokio::test]
    async fn test_factory_create_local_wal() {
        let temp_dir = TempDir::new().unwrap();
        let config = BackendFactoryConfig::local(temp_dir.path());
        let factory = BackendFactory::new(config).unwrap();

        let wal = factory.create_wal("test-topic", 0).await.unwrap();
        assert_eq!(wal.storage_mode(), StorageMode::Local);
    }

    #[tokio::test]
    #[cfg(feature = "cloud-storage")]
    async fn test_factory_create_diskless_wal() {
        let store = Arc::new(InMemory::new());
        let config = BackendFactoryConfig::diskless(store, RemoteStorageConfig::default());
        let factory = BackendFactory::new(config).unwrap();

        let wal = factory.create_wal("test-topic", 0).await.unwrap();
        assert_eq!(wal.storage_mode(), StorageMode::Diskless);
    }

    #[test]
    fn test_factory_properties_local() {
        let temp_dir = TempDir::new().unwrap();

        // Local factory
        let config = BackendFactoryConfig::local(temp_dir.path());
        let factory = BackendFactory::new(config).unwrap();
        assert!(factory.uses_local_wal());
        assert!(factory.uses_local_segments());
        assert!(factory.requires_replication());
    }

    #[test]
    #[cfg(feature = "cloud-storage")]
    fn test_factory_properties_diskless() {
        // Diskless factory
        let store = Arc::new(InMemory::new());
        let config = BackendFactoryConfig::diskless(store, RemoteStorageConfig::default());
        let factory = BackendFactory::new(config).unwrap();
        assert!(!factory.uses_local_wal());
        assert!(!factory.uses_local_segments());
        assert!(!factory.requires_replication());
    }

    #[test]
    fn test_partition_path_local() {
        let temp_dir = TempDir::new().unwrap();
        let config = BackendFactoryConfig::local(temp_dir.path());
        let factory = BackendFactory::new(config).unwrap();

        let path = factory.partition_path("my-topic", 5).unwrap();
        assert!(path.ends_with("topics/my-topic/partition-5"));
    }

    #[test]
    #[cfg(feature = "cloud-storage")]
    fn test_partition_path_diskless() {
        // Diskless has no local path
        let store = Arc::new(InMemory::new());
        let config = BackendFactoryConfig::diskless(store, RemoteStorageConfig::default());
        let factory = BackendFactory::new(config).unwrap();
        assert!(factory.partition_path("my-topic", 5).is_none());
    }

    #[test]
    #[cfg(feature = "cloud-storage")]
    fn test_parse_partition_path() {
        // Valid path with manifest
        let result =
            BackendFactory::parse_partition_path("v1/topics/my-topic/partition-0/manifest.json");
        assert_eq!(result, Some(("my-topic".to_string(), 0)));

        // Valid path with higher partition number
        let result =
            BackendFactory::parse_partition_path("v1/topics/orders/partition-42/manifest.json");
        assert_eq!(result, Some(("orders".to_string(), 42)));

        // Valid path - function parses topic/partition structure regardless of file
        let result =
            BackendFactory::parse_partition_path("v1/topics/my-topic/partition-0/data.segment");
        assert_eq!(result, Some(("my-topic".to_string(), 0)));

        // Invalid: no partition directory
        let result = BackendFactory::parse_partition_path("v1/topics/my-topic/file.txt");
        assert_eq!(result, None);

        // Invalid: malformed partition name
        let result =
            BackendFactory::parse_partition_path("v1/topics/my-topic/part-0/manifest.json");
        assert_eq!(result, None);
    }

    #[test]
    fn test_recovery_summary() {
        let mut summary = RecoverySummary::default();

        summary.add(PartitionRecoveryInfo {
            topic: "topic1".to_string(),
            partition: 0,
            segment_count: 5,
            log_end_offset: 1000,
            high_watermark: 1000,
            total_size_bytes: 50000,
            has_wal: true,
            status: RecoveryStatus::Recovered,
            error: None,
        });

        summary.add(PartitionRecoveryInfo {
            topic: "topic1".to_string(),
            partition: 1,
            segment_count: 0,
            log_end_offset: 0,
            high_watermark: 0,
            total_size_bytes: 0,
            has_wal: false,
            status: RecoveryStatus::Empty,
            error: None,
        });

        summary.add(PartitionRecoveryInfo {
            topic: "topic2".to_string(),
            partition: 0,
            segment_count: 0,
            log_end_offset: 0,
            high_watermark: 0,
            total_size_bytes: 0,
            has_wal: false,
            status: RecoveryStatus::Failed,
            error: Some("test error".to_string()),
        });

        assert_eq!(summary.total_partitions, 3);
        assert_eq!(summary.recovered, 1);
        assert_eq!(summary.empty, 1);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.total_bytes, 50000);
        assert!(!summary.is_complete());
    }

    #[test]
    fn test_recovery_status_display() {
        assert_eq!(RecoveryStatus::NotFound.to_string(), "not_found");
        assert_eq!(RecoveryStatus::Recovered.to_string(), "recovered");
        assert_eq!(RecoveryStatus::Failed.to_string(), "failed");
        assert_eq!(RecoveryStatus::Empty.to_string(), "empty");
    }

    #[tokio::test]
    #[cfg(feature = "cloud-storage")]
    async fn test_discover_partitions_empty_store() {
        let store = Arc::new(InMemory::new());
        let config = BackendFactoryConfig::diskless(store, RemoteStorageConfig::default());
        let factory = BackendFactory::new(config).unwrap();

        let partitions = factory.discover_partitions().await.unwrap();
        assert!(partitions.is_empty());
    }

    #[tokio::test]
    async fn test_discover_local_partitions() {
        let temp_dir = TempDir::new().unwrap();

        // Create some partition directories
        let topic1_p0 = temp_dir.path().join("topics/topic1/partition-0");
        let topic1_p1 = temp_dir.path().join("topics/topic1/partition-1");
        let topic2_p0 = temp_dir.path().join("topics/topic2/partition-0");
        std::fs::create_dir_all(&topic1_p0).unwrap();
        std::fs::create_dir_all(&topic1_p1).unwrap();
        std::fs::create_dir_all(&topic2_p0).unwrap();

        let config = BackendFactoryConfig::local(temp_dir.path());
        let factory = BackendFactory::new(config).unwrap();

        let partitions = factory.discover_partitions().await.unwrap();
        assert_eq!(partitions.len(), 3);

        // Check all partitions were discovered
        let has_t1p0 = partitions
            .iter()
            .any(|p| p.topic == "topic1" && p.partition == 0);
        let has_t1p1 = partitions
            .iter()
            .any(|p| p.topic == "topic1" && p.partition == 1);
        let has_t2p0 = partitions
            .iter()
            .any(|p| p.topic == "topic2" && p.partition == 0);
        assert!(has_t1p0);
        assert!(has_t1p1);
        assert!(has_t2p0);
    }

    #[tokio::test]
    #[cfg(feature = "cloud-storage")]
    async fn test_get_partition_recovery_info_not_found() {
        let store = Arc::new(InMemory::new());
        let config = BackendFactoryConfig::diskless(store, RemoteStorageConfig::default());
        let factory = BackendFactory::new(config).unwrap();

        let info = factory.get_partition_recovery_info("nonexistent", 0).await;
        assert_eq!(info.status, RecoveryStatus::NotFound);
        assert_eq!(info.topic, "nonexistent");
        assert_eq!(info.partition, 0);
    }

    #[tokio::test]
    #[cfg(feature = "cloud-storage")]
    async fn test_save_and_load_manifest() {
        let store = Arc::new(InMemory::new());
        let config = BackendFactoryConfig::diskless(store, RemoteStorageConfig::default());
        let factory = BackendFactory::new(config).unwrap();

        // Create a manifest
        let mut manifest = PartitionManifest::new("test-topic", 0);
        manifest.log_end_offset = 100;
        manifest.high_watermark = 100;

        // Save it
        factory.save_manifest(&manifest).await.unwrap();

        // Load it back
        let loaded = factory.load_manifest("test-topic", 0).await.unwrap();
        assert_eq!(loaded.topic, "test-topic");
        assert_eq!(loaded.partition, 0);
        assert_eq!(loaded.log_end_offset, 100);
        assert_eq!(loaded.high_watermark, 100);
    }

    #[tokio::test]
    #[cfg(feature = "cloud-storage")]
    async fn test_recover_all_partitions_with_manifest() {
        use crate::storage::backend::SegmentManifestEntry;

        let store = Arc::new(InMemory::new());

        // Pre-populate the store with a manifest
        let mut manifest = PartitionManifest::new("test-topic", 0);
        manifest.add_segment(SegmentManifestEntry {
            base_offset: 0,
            end_offset: 99,
            size_bytes: 1024,
            object_path: "v1/topics/test-topic/partition-0/00000000000000000000/data.segment"
                .to_string(),
            sealed: true,
            created_at: 0,
            index_path: None,
        });

        let manifest_path = ObjectPath::from("v1/topics/test-topic/partition-0/manifest.json");
        let data = manifest.to_json().unwrap();
        store
            .put(&manifest_path, PutPayload::from_bytes(data))
            .await
            .unwrap();

        let config = BackendFactoryConfig::diskless(store, RemoteStorageConfig::default());
        let factory = BackendFactory::new(config).unwrap();

        let summary = factory.recover_all_partitions().await.unwrap();
        assert_eq!(summary.total_partitions, 1);
        assert_eq!(summary.recovered, 1);
        assert_eq!(summary.failed, 0);
        assert!(summary.is_complete());
    }

    #[tokio::test]
    async fn test_get_local_recovery_info_with_segments() {
        let temp_dir = TempDir::new().unwrap();

        // Create partition directory with a segment file
        let partition_path = temp_dir.path().join("topics/test-topic/partition-0");
        std::fs::create_dir_all(&partition_path).unwrap();

        // Create a fake segment file
        let segment_path = partition_path.join("00000000000000000000.segment");
        std::fs::write(&segment_path, vec![0u8; 1024]).unwrap();

        // Create WAL directory
        let wal_path = partition_path.join("wal");
        std::fs::create_dir_all(&wal_path).unwrap();

        let config = BackendFactoryConfig::local(temp_dir.path());
        let factory = BackendFactory::new(config).unwrap();

        let info = factory.get_partition_recovery_info("test-topic", 0).await;
        assert_eq!(info.status, RecoveryStatus::Recovered);
        assert_eq!(info.segment_count, 1);
        assert_eq!(info.total_size_bytes, 1024);
        assert!(info.has_wal);
    }
}
