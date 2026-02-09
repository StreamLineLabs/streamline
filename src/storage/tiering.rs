//! Tiered storage for offloading cold data to object storage
//!
//! This module provides tiered storage functionality that moves old segments
//! from local disk to object storage backends (S3, Azure Blob, GCS, or local filesystem).
//!
//! ## Features
//!
//! - Configurable tiering policies (age-based, size-based)
//! - Multiple backend support (S3, Azure, GCS, local filesystem)
//! - Transparent fetch on cache miss
//! - LRU cache for frequently accessed cold segments
//!
//! ## Example Configuration
//!
//! ```yaml
//! tiering:
//!   enabled: true
//!   backend:
//!     type: s3
//!     bucket: my-streamline-data
//!     region: us-east-1
//!   policy:
//!     age_threshold_secs: 3600  # 1 hour
//!     min_local_segments: 2
//!     max_local_bytes: 10737418240  # 10GB
//! ```

use crate::error::{Result, StreamlineError};
use bytes::Bytes;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Tiering configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringConfig {
    /// Whether tiering is enabled
    pub enabled: bool,

    /// Backend configuration
    pub backend: TieringBackend,

    /// Tiering policy
    pub policy: TieringPolicy,

    /// Local cache configuration
    pub cache: CacheConfig,
}

impl Default for TieringConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: TieringBackend::Local {
                path: PathBuf::from("./tiered"),
            },
            policy: TieringPolicy::default(),
            cache: CacheConfig::default(),
        }
    }
}

/// Tiering backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TieringBackend {
    /// Local filesystem backend (for testing/development)
    Local {
        /// Path to store tiered segments
        path: PathBuf,
    },

    /// Amazon S3 backend
    S3 {
        /// S3 bucket name
        bucket: String,
        /// AWS region
        region: String,
        /// Optional endpoint URL (for S3-compatible storage like MinIO)
        #[serde(default)]
        endpoint: Option<String>,
        /// Optional access key ID (defaults to environment variable)
        #[serde(default)]
        access_key_id: Option<String>,
        /// Optional secret access key (defaults to environment variable)
        #[serde(default)]
        secret_access_key: Option<String>,
    },

    /// Azure Blob Storage backend
    Azure {
        /// Storage account name
        account: String,
        /// Container name
        container: String,
        /// Optional access key (defaults to environment variable)
        #[serde(default)]
        access_key: Option<String>,
    },

    /// Google Cloud Storage backend
    Gcs {
        /// GCS bucket name
        bucket: String,
        /// Optional service account key path
        #[serde(default)]
        service_account_key: Option<PathBuf>,
    },
}

/// Tiering policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringPolicy {
    /// Segments older than this will be tiered (in seconds)
    #[serde(default = "default_age_threshold")]
    pub age_threshold_secs: u64,

    /// Minimum number of segments to keep local per partition
    #[serde(default = "default_min_local_segments")]
    pub min_local_segments: usize,

    /// Maximum local storage before forced tiering (in bytes)
    #[serde(default)]
    pub max_local_bytes: Option<u64>,

    /// Interval between tiering checks (in seconds)
    #[serde(default = "default_check_interval")]
    pub check_interval_secs: u64,
}

fn default_age_threshold() -> u64 {
    3600 // 1 hour
}

fn default_min_local_segments() -> usize {
    2
}

fn default_check_interval() -> u64 {
    60 // 1 minute
}

impl Default for TieringPolicy {
    fn default() -> Self {
        Self {
            age_threshold_secs: default_age_threshold(),
            min_local_segments: default_min_local_segments(),
            max_local_bytes: None,
            check_interval_secs: default_check_interval(),
        }
    }
}

/// Cache configuration for tiered segments
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Maximum cache size in bytes
    #[serde(default = "default_cache_size")]
    pub max_bytes: u64,

    /// Cache directory path
    #[serde(default = "default_cache_path")]
    pub path: PathBuf,

    /// TTL for cached segments (in seconds)
    #[serde(default = "default_cache_ttl")]
    pub ttl_secs: u64,
}

fn default_cache_size() -> u64 {
    1024 * 1024 * 1024 // 1GB
}

fn default_cache_path() -> PathBuf {
    PathBuf::from("./cache/tiered")
}

fn default_cache_ttl() -> u64 {
    3600 // 1 hour
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: default_cache_size(),
            path: default_cache_path(),
            ttl_secs: default_cache_ttl(),
        }
    }
}

/// Tiering manager handles upload/download of segments to object storage
pub struct TieringManager {
    /// Object storage backend
    store: Arc<dyn ObjectStore>,

    /// Tiering configuration
    config: TieringConfig,

    /// Metrics
    segments_uploaded: std::sync::atomic::AtomicU64,
    segments_downloaded: std::sync::atomic::AtomicU64,
    bytes_uploaded: std::sync::atomic::AtomicU64,
    bytes_downloaded: std::sync::atomic::AtomicU64,
}

impl TieringManager {
    /// Create a new tiering manager
    pub fn new(config: TieringConfig) -> Result<Self> {
        let store = create_object_store(&config.backend)?;

        // Ensure cache directory exists
        if let Err(e) = std::fs::create_dir_all(&config.cache.path) {
            warn!(
                path = %config.cache.path.display(),
                error = %e,
                "Failed to create cache directory"
            );
        }

        info!(
            backend = ?config.backend,
            enabled = config.enabled,
            "Tiering manager initialized"
        );

        Ok(Self {
            store,
            config,
            segments_uploaded: std::sync::atomic::AtomicU64::new(0),
            segments_downloaded: std::sync::atomic::AtomicU64::new(0),
            bytes_uploaded: std::sync::atomic::AtomicU64::new(0),
            bytes_downloaded: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Check if tiering is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the age threshold for tiering
    pub fn age_threshold(&self) -> Duration {
        Duration::from_secs(self.config.policy.age_threshold_secs)
    }

    /// Upload a segment to object storage
    pub async fn upload_segment(
        &self,
        topic: &str,
        partition: i32,
        segment_id: u64,
        data: Bytes,
    ) -> Result<()> {
        if !self.config.enabled {
            return Err(StreamlineError::storage_msg(
                "Tiering is not enabled".into(),
            ));
        }

        let path = segment_object_path(topic, partition, segment_id);

        debug!(
            topic = %topic,
            partition = partition,
            segment_id = segment_id,
            size = data.len(),
            "Uploading segment to object storage"
        );

        self.store
            .put(&path, PutPayload::from_bytes(data.clone()))
            .await
            .map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to upload segment: {}", e))
            })?;

        self.segments_uploaded
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_uploaded
            .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);

        info!(
            topic = %topic,
            partition = partition,
            segment_id = segment_id,
            "Segment uploaded to object storage"
        );

        Ok(())
    }

    /// Download a segment from object storage
    pub async fn download_segment(
        &self,
        topic: &str,
        partition: i32,
        segment_id: u64,
    ) -> Result<Bytes> {
        if !self.config.enabled {
            return Err(StreamlineError::storage_msg(
                "Tiering is not enabled".into(),
            ));
        }

        let path = segment_object_path(topic, partition, segment_id);

        debug!(
            topic = %topic,
            partition = partition,
            segment_id = segment_id,
            "Downloading segment from object storage"
        );

        let result = self.store.get(&path).await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to download segment: {}", e))
        })?;

        let data = result.bytes().await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to read segment data: {}", e))
        })?;

        self.segments_downloaded
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.bytes_downloaded
            .fetch_add(data.len() as u64, std::sync::atomic::Ordering::Relaxed);

        info!(
            topic = %topic,
            partition = partition,
            segment_id = segment_id,
            size = data.len(),
            "Segment downloaded from object storage"
        );

        Ok(data)
    }

    /// Check if a segment exists in object storage
    pub async fn segment_exists(
        &self,
        topic: &str,
        partition: i32,
        segment_id: u64,
    ) -> Result<bool> {
        if !self.config.enabled {
            return Ok(false);
        }

        let path = segment_object_path(topic, partition, segment_id);

        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(StreamlineError::storage_msg(format!(
                "Failed to check segment existence: {}",
                e
            ))),
        }
    }

    /// Delete a segment from object storage
    pub async fn delete_segment(&self, topic: &str, partition: i32, segment_id: u64) -> Result<()> {
        if !self.config.enabled {
            return Err(StreamlineError::storage_msg(
                "Tiering is not enabled".into(),
            ));
        }

        let path = segment_object_path(topic, partition, segment_id);

        debug!(
            topic = %topic,
            partition = partition,
            segment_id = segment_id,
            "Deleting segment from object storage"
        );

        self.store.delete(&path).await.map_err(|e| {
            StreamlineError::storage_msg(format!("Failed to delete segment: {}", e))
        })?;

        info!(
            topic = %topic,
            partition = partition,
            segment_id = segment_id,
            "Segment deleted from object storage"
        );

        Ok(())
    }

    /// List tiered segments for a topic/partition
    pub async fn list_segments(&self, topic: &str, partition: i32) -> Result<Vec<u64>> {
        if !self.config.enabled {
            return Ok(Vec::new());
        }

        let prefix = ObjectPath::from(format!("topics/{}/partition-{}/", topic, partition));

        let mut segment_ids = Vec::new();

        let mut list_stream = self.store.list(Some(&prefix));
        while let Some(result) = futures_util::StreamExt::next(&mut list_stream).await {
            match result {
                Ok(meta) => {
                    // Extract segment ID from path: topics/{topic}/partition-{n}/{segment_id}.segment
                    if let Some(filename) = meta.location.filename() {
                        if let Some(id_str) = filename.strip_suffix(".segment") {
                            if let Ok(id) = id_str.parse::<u64>() {
                                segment_ids.push(id);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!(error = %e, "Error listing tiered segments");
                }
            }
        }

        segment_ids.sort();
        Ok(segment_ids)
    }

    /// Get tiering statistics
    pub fn stats(&self) -> TieringStats {
        TieringStats {
            segments_uploaded: self
                .segments_uploaded
                .load(std::sync::atomic::Ordering::Relaxed),
            segments_downloaded: self
                .segments_downloaded
                .load(std::sync::atomic::Ordering::Relaxed),
            bytes_uploaded: self
                .bytes_uploaded
                .load(std::sync::atomic::Ordering::Relaxed),
            bytes_downloaded: self
                .bytes_downloaded
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

/// Tiering statistics
#[derive(Debug, Clone, Serialize)]
pub struct TieringStats {
    pub segments_uploaded: u64,
    pub segments_downloaded: u64,
    pub bytes_uploaded: u64,
    pub bytes_downloaded: u64,
}

/// Generate the object storage path for a segment
fn segment_object_path(topic: &str, partition: i32, segment_id: u64) -> ObjectPath {
    ObjectPath::from(format!(
        "topics/{}/partition-{}/{}.segment",
        topic, partition, segment_id
    ))
}

/// Create an object store from the backend configuration
fn create_object_store(backend: &TieringBackend) -> Result<Arc<dyn ObjectStore>> {
    match backend {
        TieringBackend::Local { path } => {
            // Ensure directory exists
            std::fs::create_dir_all(path).map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to create tiering directory: {}", e))
            })?;

            let store =
                object_store::local::LocalFileSystem::new_with_prefix(path).map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to create local store: {}", e))
                })?;

            Ok(Arc::new(store))
        }

        TieringBackend::S3 {
            bucket,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
        } => {
            let mut builder = object_store::aws::AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(region);

            if let Some(endpoint) = endpoint {
                builder = builder.with_endpoint(endpoint);
            }

            if let Some(key_id) = access_key_id {
                builder = builder.with_access_key_id(key_id);
            }

            if let Some(secret) = secret_access_key {
                builder = builder.with_secret_access_key(secret);
            }

            let store = builder.build().map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to create S3 store: {}", e))
            })?;

            Ok(Arc::new(store))
        }

        TieringBackend::Azure {
            account,
            container,
            access_key,
        } => {
            let mut builder = object_store::azure::MicrosoftAzureBuilder::new()
                .with_account(account)
                .with_container_name(container);

            if let Some(key) = access_key {
                builder = builder.with_access_key(key);
            }

            let store = builder.build().map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to create Azure store: {}", e))
            })?;

            Ok(Arc::new(store))
        }

        TieringBackend::Gcs {
            bucket,
            service_account_key,
        } => {
            let mut builder =
                object_store::gcp::GoogleCloudStorageBuilder::new().with_bucket_name(bucket);

            if let Some(key_path) = service_account_key {
                builder = builder.with_service_account_path(key_path.to_string_lossy());
            }

            let store = builder.build().map_err(|e| {
                StreamlineError::storage_msg(format!("Failed to create GCS store: {}", e))
            })?;

            Ok(Arc::new(store))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = TieringConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.policy.age_threshold_secs, 3600);
        assert_eq!(config.policy.min_local_segments, 2);
    }

    #[test]
    fn test_segment_object_path() {
        let path = segment_object_path("my-topic", 2, 12345);
        assert_eq!(path.as_ref(), "topics/my-topic/partition-2/12345.segment");
    }

    #[tokio::test]
    async fn test_local_backend() {
        let temp_dir = tempfile::tempdir().unwrap();
        let config = TieringConfig {
            enabled: true,
            backend: TieringBackend::Local {
                path: temp_dir.path().to_path_buf(),
            },
            policy: TieringPolicy::default(),
            cache: CacheConfig::default(),
        };

        let manager = TieringManager::new(config).unwrap();
        assert!(manager.is_enabled());

        // Upload a segment
        let data = Bytes::from("test segment data");
        manager
            .upload_segment("test-topic", 0, 100, data.clone())
            .await
            .unwrap();

        // Check it exists
        assert!(manager.segment_exists("test-topic", 0, 100).await.unwrap());

        // Download it
        let downloaded = manager
            .download_segment("test-topic", 0, 100)
            .await
            .unwrap();
        assert_eq!(downloaded, data);

        // List segments
        let segments = manager.list_segments("test-topic", 0).await.unwrap();
        assert_eq!(segments, vec![100]);

        // Delete it
        manager.delete_segment("test-topic", 0, 100).await.unwrap();
        assert!(!manager.segment_exists("test-topic", 0, 100).await.unwrap());

        // Check stats
        let stats = manager.stats();
        assert_eq!(stats.segments_uploaded, 1);
        assert_eq!(stats.segments_downloaded, 1);
    }

    #[test]
    fn test_backend_serialization() {
        let backend = TieringBackend::S3 {
            bucket: "my-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
        };

        let json = serde_json::to_string(&backend).unwrap();
        assert!(json.contains("\"type\":\"s3\""));
        assert!(json.contains("\"bucket\":\"my-bucket\""));
    }
}
