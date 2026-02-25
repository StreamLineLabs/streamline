//! Edge-First Architecture Module
//!
//! Provides edge computing capabilities for Streamline with:
//! - Minimal edge binary (<5MB footprint target)
//! - Offline-first with sync-when-connected
//! - Edge-to-cloud federation
//! - Conflict resolution for distributed writes
//! - Resource-constrained optimizations
//!
//! # Overview
//!
//! The edge module enables deploying Streamline on resource-constrained devices
//! at the network edge while maintaining seamless synchronization with cloud
//! infrastructure.
//!
//! # Key Components
//!
//! - [`EdgeConfig`]: Configuration for edge deployments
//! - [`EdgeSyncEngine`]: Bidirectional synchronization engine
//! - [`ConflictResolver`]: Handles merge conflicts during sync
//! - [`FederationManager`]: Manages edge-to-cloud relationships
//! - [`EdgeSyncCheckpoint`]: Tracks sync progress for resumability
//!
//! # Example
//!
//! ```no_run
//! use streamline::edge::{EdgeConfig, EdgeRuntime};
//!
//! # fn main() -> streamline::Result<()> {
//! // Create an edge configuration
//! let config = EdgeConfig::minimal()
//!     .with_cloud_endpoint("https://cloud.example.com:9092");
//!
//! // Create and start the edge runtime
//! let runtime = EdgeRuntime::new(config)?;
//!
//! // Produce messages locally (works offline)
//! runtime.produce("events", b"sensor-1", b"temperature=23.5")?;
//!
//! // Sync will happen automatically when connected to cloud
//! # Ok(())
//! # }
//! ```
//!
//! # Offline-First Design
//!
//! The edge module is designed for intermittent connectivity:
//!
//! 1. **Local Storage**: All writes go to local storage first
//! 2. **Checkpoint Tracking**: Sync progress is checkpointed for resumability
//! 3. **Conflict Resolution**: When connectivity returns, conflicts are resolved
//! 4. **Bidirectional Sync**: Data flows both edge→cloud and cloud→edge
//!
//! # Resource Optimization
//!
//! For resource-constrained devices:
//!
//! - Use `EdgeConfig::minimal()` for smallest footprint
//! - Configure `EdgeRetentionPolicy::RingBuffer` for bounded storage
//! - Enable maximum compression with `EdgeCompressionStrategy::Maximum`
//! - Disable WAL for lowest latency (at cost of durability)

pub mod checkpoint;
pub mod config;
pub mod conflict;
pub mod discovery;
pub mod federation;
pub mod http_sync;
pub mod mesh;
pub mod multicloud;
pub mod optimizations;
pub mod power;
pub mod resource_monitor;
pub mod routing;
pub mod service_mesh;
pub mod store_forward;
pub mod sync;

// Re-export main types
pub use checkpoint::{CheckpointState, EdgeSyncCheckpoint, PartitionCheckpoint, TopicCheckpoint};
pub use config::{
    ConflictResolution, EdgeCompressionStrategy, EdgeConfig, EdgeNetworkConfig, EdgeResourceConfig,
    EdgeRetentionPolicy, EdgeStorageConfig, EdgeSyncConfig, EdgeSyncMode,
};
pub use conflict::{ConflictInfo, ConflictResolver, ConflictStats, ResolutionResult};
pub use federation::{
    EdgeCapabilities, EdgeCluster, EdgeNodeInfo, FederationCommand, FederationManager,
    FederationState, FederationStats, HeartbeatRequest, HeartbeatResponse,
};
pub use sync::{
    EdgeSyncEngine, SyncBatch, SyncDirection, SyncPhase, SyncProgress, SyncRecord, SyncResult,
    SyncState, SyncStats,
};
pub use discovery::{
    DiscoveryConfig, DiscoveryProtocol, DiscoveryStatsSnapshot, EdgeDiscovery, EdgeNode,
    EdgeNodeCapabilities, EdgeNodeStatus,
};
pub use mesh::{MeshTopology, PeerConnection, PeerConnectionState, SyncPolicy};
pub use resource_monitor::{
    AdaptiveConfig, ResourceAlert, ResourceMonitor, ResourceMonitorConfig, ResourceSnapshot,
    ResourceThresholds,
};
pub use optimizations::{
    CompactionResult, EdgeOptimizationConfig, EdgeOptimizer, EvictionResult, MemoryRecommendation,
    OptimizationStats, TopicStorageUsage,
};
pub use store_forward::{
    StoreForwardConfig, StoreForwardEngine, StoreForwardStatus, SyncStrategy, SyncWatermark,
};
pub use power::{
    BatteryStatus, EdgeOperation, PowerConfig, PowerManager, PowerProfile,
    PowerSource, PowerThresholds, ProfileChange,
};

use crate::error::{Result, StreamlineError};
use crate::storage::TopicManager;
use bytes::Bytes;
use std::sync::Arc;

/// Edge runtime - high-level API for edge deployments
pub struct EdgeRuntime {
    config: EdgeConfig,
    topic_manager: Arc<TopicManager>,
    sync_engine: Option<Arc<EdgeSyncEngine>>,
    federation: Option<Arc<FederationManager>>,
    checkpoint: Arc<EdgeSyncCheckpoint>,
}

impl EdgeRuntime {
    /// Create a new edge runtime
    pub fn new(config: EdgeConfig) -> Result<Self> {
        let data_path = std::path::PathBuf::from(&config.storage.data_dir);

        // Create topic manager based on storage config
        let topic_manager = if config.storage.in_memory {
            Arc::new(TopicManager::in_memory()?)
        } else {
            // Create data directory if it doesn't exist
            if !data_path.exists() {
                std::fs::create_dir_all(&data_path).map_err(|e| {
                    StreamlineError::storage_msg(format!("Failed to create data dir: {}", e))
                })?;
            }

            if config.storage.wal_enabled {
                let wal_config = crate::config::WalConfig::default();
                Arc::new(TopicManager::with_wal_config(&data_path, Some(wal_config))?)
            } else {
                Arc::new(TopicManager::new(&data_path)?)
            }
        };

        let checkpoint_path = data_path.join("edge_checkpoint.json");
        let checkpoint = Arc::new(EdgeSyncCheckpoint::new(checkpoint_path)?);
        checkpoint.set_edge_id(&config.edge_id);

        let mut runtime = Self {
            config: config.clone(),
            topic_manager,
            sync_engine: None,
            federation: None,
            checkpoint,
        };

        // Initialize sync engine if cloud endpoint is configured
        if config.has_cloud_sync() {
            let conflict_resolver =
                Arc::new(ConflictResolver::new(config.sync.conflict_resolution));
            let sync_engine = Arc::new(EdgeSyncEngine::new(
                config.sync.clone(),
                runtime.topic_manager.clone(),
                conflict_resolver,
                runtime.checkpoint.clone(),
            ));
            runtime.sync_engine = Some(sync_engine);

            let federation = Arc::new(FederationManager::new(config));
            runtime.federation = Some(federation);
        }

        Ok(runtime)
    }

    /// Create an in-memory edge runtime (for testing)
    pub fn in_memory() -> Result<Self> {
        let config = EdgeConfig {
            storage: EdgeStorageConfig {
                in_memory: true,
                ..Default::default()
            },
            ..Default::default()
        };
        Self::new(config)
    }

    /// Get the edge ID
    pub fn edge_id(&self) -> &str {
        &self.config.edge_id
    }

    /// Check if cloud sync is enabled
    pub fn has_cloud_sync(&self) -> bool {
        self.sync_engine.is_some()
    }

    /// Get current sync state
    pub fn sync_state(&self) -> SyncState {
        self.sync_engine
            .as_ref()
            .map(|e| e.state())
            .unwrap_or(SyncState::Idle)
    }

    /// Get federation state
    pub fn federation_state(&self) -> FederationState {
        self.federation
            .as_ref()
            .map(|f| f.state())
            .unwrap_or(FederationState::Disconnected)
    }

    /// Produce a message to a topic
    pub fn produce(
        &self,
        topic: &str,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<i64> {
        self.produce_to_partition(topic, 0, key, value)
    }

    /// Produce a message to a specific partition
    pub fn produce_to_partition(
        &self,
        topic: &str,
        partition: i32,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<i64> {
        // Auto-create topic if needed
        if self.topic_manager.get_topic_metadata(topic).is_err() {
            self.topic_manager.create_topic(topic, 1)?;
        }

        let key_bytes = Some(Bytes::copy_from_slice(key.as_ref()));
        let value_bytes = Bytes::copy_from_slice(value.as_ref());

        self.topic_manager
            .append(topic, partition, key_bytes, value_bytes)
    }

    /// Consume messages from a topic
    pub fn consume(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_records: usize,
    ) -> Result<Vec<crate::storage::Record>> {
        self.topic_manager
            .read(topic, partition, offset, max_records)
    }

    /// Create a topic
    pub fn create_topic(&self, name: &str, partitions: i32) -> Result<()> {
        self.topic_manager.create_topic(name, partitions)
    }

    /// List topics
    pub fn list_topics(&self) -> Result<Vec<String>> {
        Ok(self
            .topic_manager
            .list_topics()?
            .into_iter()
            .map(|m| m.name)
            .collect())
    }

    /// Get latest offset for a partition
    pub fn latest_offset(&self, topic: &str, partition: i32) -> Result<i64> {
        self.topic_manager.latest_offset(topic, partition)
    }

    /// Trigger immediate sync (if connected)
    pub async fn sync_now(&self) -> Result<SyncResult> {
        match &self.sync_engine {
            Some(engine) => engine.sync_now(SyncDirection::Bidirectional).await,
            None => Err(StreamlineError::storage_msg(
                "Cloud sync not configured".into(),
            )),
        }
    }

    /// Start background sync
    pub async fn start_sync(&self) -> Result<()> {
        if let Some(ref engine) = self.sync_engine {
            engine.start().await?;
        }
        if let Some(ref federation) = self.federation {
            federation.start().await?;
        }
        Ok(())
    }

    /// Stop background sync
    pub async fn stop_sync(&self) -> Result<()> {
        if let Some(ref engine) = self.sync_engine {
            engine.stop().await?;
        }
        if let Some(ref federation) = self.federation {
            federation.stop().await?;
        }
        Ok(())
    }

    /// Get sync statistics
    pub fn sync_stats(&self) -> Option<SyncStats> {
        self.sync_engine.as_ref().map(|e| e.stats())
    }

    /// Get federation statistics
    pub fn federation_stats(&self) -> Option<FederationStats> {
        self.federation.as_ref().map(|f| f.stats())
    }

    /// Get checkpoint state
    pub fn checkpoint(&self) -> CheckpointState {
        self.checkpoint.get_all()
    }

    /// Save checkpoint
    pub fn save_checkpoint(&self) -> Result<()> {
        self.checkpoint.save()
    }

    /// Get topic manager (for advanced use)
    pub fn topic_manager(&self) -> Arc<TopicManager> {
        self.topic_manager.clone()
    }
}

/// Builder for EdgeRuntime
pub struct EdgeRuntimeBuilder {
    config: EdgeConfig,
}

impl EdgeRuntimeBuilder {
    /// Create a new builder with default config
    pub fn new() -> Self {
        Self {
            config: EdgeConfig::default(),
        }
    }

    /// Create a builder for minimal resource usage
    pub fn minimal() -> Self {
        Self {
            config: EdgeConfig::minimal(),
        }
    }

    /// Create a builder for offline-first operation
    pub fn offline_first() -> Self {
        Self {
            config: EdgeConfig::offline_first(),
        }
    }

    /// Set edge ID
    pub fn edge_id(mut self, id: impl Into<String>) -> Self {
        self.config.edge_id = id.into();
        self
    }

    /// Set cloud endpoint
    pub fn cloud_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.config.cloud_endpoint = Some(endpoint.into());
        self
    }

    /// Set data directory
    pub fn data_dir(mut self, dir: impl Into<String>) -> Self {
        self.config.storage.data_dir = dir.into();
        self
    }

    /// Enable in-memory mode
    pub fn in_memory(mut self, enabled: bool) -> Self {
        self.config.storage.in_memory = enabled;
        self
    }

    /// Set sync mode
    pub fn sync_mode(mut self, mode: EdgeSyncMode) -> Self {
        self.config.sync.mode = mode;
        self
    }

    /// Set conflict resolution strategy
    pub fn conflict_resolution(mut self, strategy: ConflictResolution) -> Self {
        self.config.sync.conflict_resolution = strategy;
        self
    }

    /// Set retention policy
    pub fn retention_policy(mut self, policy: EdgeRetentionPolicy) -> Self {
        self.config.storage.retention_policy = policy;
        self
    }

    /// Set maximum storage size
    pub fn max_storage_bytes(mut self, bytes: u64) -> Self {
        self.config.storage.max_storage_bytes = bytes;
        self
    }

    /// Set compression strategy
    pub fn compression(mut self, strategy: EdgeCompressionStrategy) -> Self {
        self.config.sync.compression = strategy;
        self.config.storage.compression = strategy;
        self
    }

    /// Build the runtime
    pub fn build(self) -> Result<EdgeRuntime> {
        EdgeRuntime::new(self.config)
    }
}

impl Default for EdgeRuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_edge_runtime_in_memory() {
        let runtime = EdgeRuntime::in_memory().unwrap();
        assert!(!runtime.has_cloud_sync());
        assert_eq!(runtime.sync_state(), SyncState::Idle);
    }

    #[test]
    fn test_edge_runtime_produce_consume() {
        let runtime = EdgeRuntime::in_memory().unwrap();

        // Create topic and produce
        runtime.create_topic("test-topic", 1).unwrap();
        let offset = runtime.produce("test-topic", b"key1", b"value1").unwrap();
        assert_eq!(offset, 0);

        // Consume
        let records = runtime.consume("test-topic", 0, 0, 10).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value.as_ref(), b"value1");
    }

    #[test]
    fn test_edge_runtime_auto_create_topic() {
        let runtime = EdgeRuntime::in_memory().unwrap();

        // Produce to non-existent topic (should auto-create)
        let offset = runtime.produce("auto-topic", b"key", b"value").unwrap();
        assert_eq!(offset, 0);

        // Verify topic exists
        let topics = runtime.list_topics().unwrap();
        assert!(topics.contains(&"auto-topic".to_string()));
    }

    #[test]
    fn test_edge_runtime_builder() {
        let runtime = EdgeRuntimeBuilder::minimal()
            .edge_id("test-edge")
            .in_memory(true)
            .sync_mode(EdgeSyncMode::Manual)
            .conflict_resolution(ConflictResolution::EdgeWins)
            .build()
            .unwrap();

        assert_eq!(runtime.edge_id(), "test-edge");
    }

    #[test]
    fn test_edge_runtime_checkpoint() {
        let runtime = EdgeRuntime::in_memory().unwrap();
        let checkpoint = runtime.checkpoint();

        assert_eq!(checkpoint.version, 1);
        assert!(checkpoint.topics.is_empty());
    }
}
