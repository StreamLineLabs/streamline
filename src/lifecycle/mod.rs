//! Smart Data Lifecycle Management
//!
//! Provides intelligent data lifecycle management including:
//! - Tiered storage (hot/warm/cold/archive)
//! - Automatic data aging and migration
//! - Cost-optimized storage policies
//! - Compliance-aware retention
//! - Automatic cleanup and compaction

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

pub mod policies;
pub mod storage_tiers;
pub mod transitions;

pub use policies::{LifecyclePolicy, PolicyEvaluator};
pub use storage_tiers::{StorageTier, TierManager};
pub use transitions::{MigrationJob, MigrationManager};

/// Data age classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataAge {
    /// Recently written data (< 1 hour)
    Hot,
    /// Recent data (1 hour - 1 day)
    Warm,
    /// Older data (1 day - 30 days)
    Cool,
    /// Historical data (30 days - 1 year)
    Cold,
    /// Archive data (> 1 year)
    Archive,
    /// Data marked for deletion
    Expired,
}

impl DataAge {
    /// Get the minimum age in seconds for this classification
    pub fn min_age_seconds(&self) -> u64 {
        match self {
            DataAge::Hot => 0,
            DataAge::Warm => 3600,           // 1 hour
            DataAge::Cool => 86400,          // 1 day
            DataAge::Cold => 30 * 86400,     // 30 days
            DataAge::Archive => 365 * 86400, // 1 year
            DataAge::Expired => u64::MAX,
        }
    }

    /// Get classification from age in seconds
    pub fn from_age(age_seconds: u64) -> Self {
        if age_seconds < 3600 {
            DataAge::Hot
        } else if age_seconds < 86400 {
            DataAge::Warm
        } else if age_seconds < 30 * 86400 {
            DataAge::Cool
        } else if age_seconds < 365 * 86400 {
            DataAge::Cold
        } else {
            DataAge::Archive
        }
    }

    /// Get recommended storage tier
    pub fn recommended_tier(&self) -> StorageTier {
        match self {
            DataAge::Hot => StorageTier::Memory,
            DataAge::Warm => StorageTier::Ssd,
            DataAge::Cool => StorageTier::Ssd,
            DataAge::Cold => StorageTier::Hdd,
            DataAge::Archive => StorageTier::ObjectStorage,
            DataAge::Expired => StorageTier::ObjectStorage,
        }
    }
}

/// Data segment information for lifecycle management
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSegment {
    /// Segment identifier
    pub id: String,
    /// Topic this segment belongs to
    pub topic: String,
    /// Partition number
    pub partition: u32,
    /// Start offset
    pub start_offset: i64,
    /// End offset
    pub end_offset: i64,
    /// Segment creation time (Unix timestamp ms)
    pub created_at: i64,
    /// Last access time (Unix timestamp ms)
    pub last_accessed: i64,
    /// Size in bytes
    pub size_bytes: u64,
    /// Current storage tier
    pub current_tier: StorageTier,
    /// Current file path
    pub path: PathBuf,
    /// Is segment sealed (no more writes)
    pub sealed: bool,
    /// Access count
    pub access_count: u64,
    /// Tags for policy matching
    pub tags: HashMap<String, String>,
}

impl DataSegment {
    /// Create a new data segment
    pub fn new(
        id: impl Into<String>,
        topic: impl Into<String>,
        partition: u32,
        start_offset: i64,
    ) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            id: id.into(),
            topic: topic.into(),
            partition,
            start_offset,
            end_offset: start_offset,
            created_at: now,
            last_accessed: now,
            size_bytes: 0,
            current_tier: StorageTier::Memory,
            path: PathBuf::new(),
            sealed: false,
            access_count: 0,
            tags: HashMap::new(),
        }
    }

    /// Get age in seconds
    pub fn age_seconds(&self) -> u64 {
        let now = chrono::Utc::now().timestamp_millis();
        ((now - self.created_at) / 1000) as u64
    }

    /// Get data age classification
    pub fn age_classification(&self) -> DataAge {
        DataAge::from_age(self.age_seconds())
    }

    /// Record an access
    pub fn record_access(&mut self) {
        self.last_accessed = chrono::Utc::now().timestamp_millis();
        self.access_count += 1;
    }

    /// Get time since last access in seconds
    pub fn idle_seconds(&self) -> u64 {
        let now = chrono::Utc::now().timestamp_millis();
        ((now - self.last_accessed) / 1000) as u64
    }

    /// Add a tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }
}

/// Lifecycle manager configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleConfig {
    /// Enable automatic lifecycle management
    pub enabled: bool,
    /// Evaluation interval
    pub evaluation_interval: Duration,
    /// Maximum concurrent migrations
    pub max_concurrent_migrations: usize,
    /// Enable cost optimization
    pub cost_optimization_enabled: bool,
    /// Default retention period (days)
    pub default_retention_days: u32,
    /// Enable compression for cold data
    pub compress_cold_data: bool,
    /// Compression algorithm for cold data
    pub cold_compression: CompressionType,
    /// Archive storage path
    pub archive_path: PathBuf,
    /// Enable deletion of expired data
    pub delete_expired: bool,
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            evaluation_interval: Duration::from_secs(300), // 5 minutes
            max_concurrent_migrations: 4,
            cost_optimization_enabled: true,
            default_retention_days: 365,
            compress_cold_data: true,
            cold_compression: CompressionType::Zstd,
            archive_path: PathBuf::from("./archive"),
            delete_expired: true,
        }
    }
}

/// Compression type for data storage
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompressionType {
    /// No compression
    None,
    /// LZ4 (fast)
    Lz4,
    /// Zstd (balanced)
    Zstd,
    /// Gzip (compatible)
    Gzip,
}

/// Lifecycle management statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LifecycleStats {
    /// Total segments managed
    pub total_segments: u64,
    /// Segments by tier
    pub segments_by_tier: HashMap<StorageTier, u64>,
    /// Bytes by tier
    pub bytes_by_tier: HashMap<StorageTier, u64>,
    /// Total migrations performed
    pub total_migrations: u64,
    /// Successful migrations
    pub successful_migrations: u64,
    /// Failed migrations
    pub failed_migrations: u64,
    /// Bytes migrated
    pub bytes_migrated: u64,
    /// Total deletions
    pub total_deletions: u64,
    /// Bytes deleted
    pub bytes_deleted: u64,
    /// Bytes saved by compression
    pub compression_savings_bytes: u64,
    /// Estimated cost savings (dollars)
    pub estimated_cost_savings: f64,
    /// Last evaluation time
    pub last_evaluation: i64,
}

/// Lifecycle action to be taken
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LifecycleAction {
    /// No action needed
    None,
    /// Migrate to a different tier
    Migrate {
        segment_id: String,
        from_tier: StorageTier,
        to_tier: StorageTier,
        reason: String,
    },
    /// Compress the segment
    Compress {
        segment_id: String,
        algorithm: CompressionType,
    },
    /// Delete the segment
    Delete { segment_id: String, reason: String },
    /// Archive the segment
    Archive {
        segment_id: String,
        destination: PathBuf,
    },
}

/// Lifecycle manager for intelligent data lifecycle management
pub struct LifecycleManager {
    config: LifecycleConfig,
    policy_evaluator: PolicyEvaluator,
    tier_manager: Arc<RwLock<TierManager>>,
    migration_manager: Arc<RwLock<MigrationManager>>,
    segments: Arc<RwLock<HashMap<String, DataSegment>>>,
    stats: Arc<RwLock<LifecycleStats>>,
}

impl LifecycleManager {
    /// Create a new lifecycle manager
    pub fn new(config: LifecycleConfig) -> Self {
        let tier_manager = Arc::new(RwLock::new(TierManager::new()));
        let migration_manager = Arc::new(RwLock::new(MigrationManager::new(
            config.max_concurrent_migrations,
        )));

        Self {
            config,
            policy_evaluator: PolicyEvaluator::new(),
            tier_manager,
            migration_manager,
            segments: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(LifecycleStats::default())),
        }
    }

    /// Register a data segment
    pub async fn register_segment(&self, segment: DataSegment) {
        let mut segments = self.segments.write().await;
        segments.insert(segment.id.clone(), segment);
    }

    /// Unregister a data segment
    pub async fn unregister_segment(&self, segment_id: &str) -> Option<DataSegment> {
        let mut segments = self.segments.write().await;
        segments.remove(segment_id)
    }

    /// Get segment by ID
    pub async fn get_segment(&self, segment_id: &str) -> Option<DataSegment> {
        let segments = self.segments.read().await;
        segments.get(segment_id).cloned()
    }

    /// Update segment information
    pub async fn update_segment(&self, segment: DataSegment) {
        let mut segments = self.segments.write().await;
        segments.insert(segment.id.clone(), segment);
    }

    /// Add a lifecycle policy
    pub fn add_policy(&mut self, policy: LifecyclePolicy) {
        self.policy_evaluator.add_policy(policy);
    }

    /// Evaluate all segments and determine actions
    pub async fn evaluate(&self) -> Vec<LifecycleAction> {
        let segments = self.segments.read().await;
        let mut actions = Vec::new();

        for segment in segments.values() {
            // Evaluate policies
            if let Some(action) = self.policy_evaluator.evaluate(segment) {
                actions.push(action);
                continue;
            }

            // Default age-based evaluation
            if let Some(action) = self.evaluate_age_based(segment) {
                actions.push(action);
            }
        }

        // Update last evaluation time
        let mut stats = self.stats.write().await;
        stats.last_evaluation = chrono::Utc::now().timestamp_millis();

        actions
    }

    /// Evaluate segment based on age
    fn evaluate_age_based(&self, segment: &DataSegment) -> Option<LifecycleAction> {
        let age = segment.age_classification();
        let recommended_tier = age.recommended_tier();

        // Check if migration is needed
        if segment.current_tier != recommended_tier && segment.sealed {
            return Some(LifecycleAction::Migrate {
                segment_id: segment.id.clone(),
                from_tier: segment.current_tier,
                to_tier: recommended_tier,
                reason: format!("Age-based migration: data is {:?}", age),
            });
        }

        // Check if compression is needed
        if self.config.compress_cold_data
            && matches!(age, DataAge::Cold | DataAge::Archive)
            && !segment.path.to_string_lossy().ends_with(".zst")
        {
            return Some(LifecycleAction::Compress {
                segment_id: segment.id.clone(),
                algorithm: self.config.cold_compression,
            });
        }

        // Check for expiration
        let retention_days = self.config.default_retention_days;
        let age_days = segment.age_seconds() / 86400;
        if age_days > retention_days as u64 && self.config.delete_expired {
            return Some(LifecycleAction::Delete {
                segment_id: segment.id.clone(),
                reason: format!(
                    "Retention expired: {} days old, retention is {} days",
                    age_days, retention_days
                ),
            });
        }

        None
    }

    /// Execute a lifecycle action
    pub async fn execute_action(&self, action: LifecycleAction) -> Result<(), String> {
        match action {
            LifecycleAction::None => Ok(()),

            LifecycleAction::Migrate {
                segment_id,
                from_tier,
                to_tier,
                reason,
            } => {
                let mut migration_manager = self.migration_manager.write().await;
                let job = MigrationJob::new(&segment_id, from_tier, to_tier, &reason);
                migration_manager.submit(job);

                let mut stats = self.stats.write().await;
                stats.total_migrations += 1;
                Ok(())
            }

            LifecycleAction::Compress {
                segment_id,
                algorithm,
            } => {
                // Update segment metadata (actual compression done by storage layer)
                let mut segments = self.segments.write().await;
                if let Some(segment) = segments.get_mut(&segment_id) {
                    let ext = match algorithm {
                        CompressionType::Lz4 => ".lz4",
                        CompressionType::Zstd => ".zst",
                        CompressionType::Gzip => ".gz",
                        CompressionType::None => "",
                    };
                    let new_path = format!("{}{}", segment.path.to_string_lossy(), ext);
                    segment.path = PathBuf::from(new_path);
                }
                Ok(())
            }

            LifecycleAction::Delete { segment_id, reason } => {
                let segment = {
                    let mut segments = self.segments.write().await;
                    segments.remove(&segment_id)
                };

                if let Some(seg) = segment {
                    let mut stats = self.stats.write().await;
                    stats.total_deletions += 1;
                    stats.bytes_deleted += seg.size_bytes;
                    tracing::info!(
                        "Deleted segment {} ({}): {}",
                        segment_id,
                        seg.size_bytes,
                        reason
                    );
                }
                Ok(())
            }

            LifecycleAction::Archive {
                segment_id,
                destination,
            } => {
                let mut segments = self.segments.write().await;
                if let Some(segment) = segments.get_mut(&segment_id) {
                    segment.current_tier = StorageTier::ObjectStorage;
                    segment.path = destination;
                }
                Ok(())
            }
        }
    }

    /// Run a single evaluation cycle
    pub async fn run_cycle(&self) {
        let actions = self.evaluate().await;

        for action in actions {
            if let Err(e) = self.execute_action(action).await {
                tracing::error!("Failed to execute lifecycle action: {}", e);

                let mut stats = self.stats.write().await;
                stats.failed_migrations += 1;
            }
        }
    }

    /// Get statistics
    pub async fn stats(&self) -> LifecycleStats {
        let stats = self.stats.read().await;
        stats.clone()
    }

    /// Get segments by tier
    pub async fn segments_by_tier(&self, tier: StorageTier) -> Vec<DataSegment> {
        let segments = self.segments.read().await;
        segments
            .values()
            .filter(|s| s.current_tier == tier)
            .cloned()
            .collect()
    }

    /// Get segments older than specified age
    pub async fn segments_older_than(&self, age_seconds: u64) -> Vec<DataSegment> {
        let segments = self.segments.read().await;
        segments
            .values()
            .filter(|s| s.age_seconds() > age_seconds)
            .cloned()
            .collect()
    }

    /// Get total storage used by tier
    pub async fn storage_by_tier(&self) -> HashMap<StorageTier, u64> {
        let segments = self.segments.read().await;
        let mut storage: HashMap<StorageTier, u64> = HashMap::new();

        for segment in segments.values() {
            *storage.entry(segment.current_tier).or_default() += segment.size_bytes;
        }

        storage
    }

    /// Calculate estimated cost for current storage distribution
    pub async fn estimate_storage_cost(&self) -> f64 {
        let storage = self.storage_by_tier().await;
        let tier_manager = self.tier_manager.read().await;

        let mut total_cost = 0.0;
        for (tier, bytes) in storage {
            if let Some(config) = tier_manager.get_tier_config(tier) {
                let gb = bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                total_cost += gb * config.cost_per_gb_month;
            }
        }

        total_cost
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_age_classification() {
        assert_eq!(DataAge::from_age(0), DataAge::Hot);
        assert_eq!(DataAge::from_age(3599), DataAge::Hot);
        assert_eq!(DataAge::from_age(3600), DataAge::Warm);
        assert_eq!(DataAge::from_age(86399), DataAge::Warm);
        assert_eq!(DataAge::from_age(86400), DataAge::Cool);
        assert_eq!(DataAge::from_age(30 * 86400 - 1), DataAge::Cool);
        assert_eq!(DataAge::from_age(30 * 86400), DataAge::Cold);
        assert_eq!(DataAge::from_age(365 * 86400), DataAge::Archive);
    }

    #[test]
    fn test_data_segment_creation() {
        let segment = DataSegment::new("seg-001", "my-topic", 0, 0);

        assert_eq!(segment.id, "seg-001");
        assert_eq!(segment.topic, "my-topic");
        assert_eq!(segment.partition, 0);
        assert_eq!(segment.current_tier, StorageTier::Memory);
        assert!(!segment.sealed);
    }

    #[test]
    fn test_data_segment_with_tags() {
        let segment = DataSegment::new("seg-001", "my-topic", 0, 0)
            .with_tag("env", "production")
            .with_tag("team", "platform");

        assert_eq!(segment.tags.get("env"), Some(&"production".to_string()));
        assert_eq!(segment.tags.get("team"), Some(&"platform".to_string()));
    }

    #[test]
    fn test_lifecycle_config_default() {
        let config = LifecycleConfig::default();

        assert!(config.enabled);
        assert_eq!(config.max_concurrent_migrations, 4);
        assert_eq!(config.default_retention_days, 365);
        assert!(config.compress_cold_data);
    }

    #[tokio::test]
    async fn test_lifecycle_manager() {
        let config = LifecycleConfig::default();
        let manager = LifecycleManager::new(config);

        let segment = DataSegment::new("seg-001", "my-topic", 0, 0);
        manager.register_segment(segment).await;

        let retrieved = manager.get_segment("seg-001").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, "seg-001");
    }

    #[tokio::test]
    async fn test_segments_by_tier() {
        let config = LifecycleConfig::default();
        let manager = LifecycleManager::new(config);

        // Register segments with different tiers
        let mut seg1 = DataSegment::new("seg-001", "topic", 0, 0);
        seg1.current_tier = StorageTier::Memory;
        manager.register_segment(seg1).await;

        let mut seg2 = DataSegment::new("seg-002", "topic", 0, 100);
        seg2.current_tier = StorageTier::Ssd;
        manager.register_segment(seg2).await;

        let memory_segments = manager.segments_by_tier(StorageTier::Memory).await;
        assert_eq!(memory_segments.len(), 1);

        let ssd_segments = manager.segments_by_tier(StorageTier::Ssd).await;
        assert_eq!(ssd_segments.len(), 1);
    }
}
