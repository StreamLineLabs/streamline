//! Intelligent Data Lifecycle Management for Tiered Storage
//!
//! This module provides cost-optimized tiered storage with:
//! - Access pattern tracking for predictive tiering
//! - Cold storage class support (Glacier, Azure Cool, GCS Archive)
//! - Cost estimation and optimization recommendations
//! - Automatic tier transitions based on policies
//!
//! ## Storage Tiers
//!
//! | Tier | Use Case | Cost | Latency |
//! |------|----------|------|---------|
//! | Hot (Local) | Active data | $$$$ | <1ms |
//! | Warm (S3 Standard) | Recent data | $$$ | 10-100ms |
//! | Cold (S3 Glacier IR) | Archive | $$ | 1-5 min |
//! | Frozen (S3 Deep Archive) | Compliance | $ | 12+ hours |
//!
//! ## Example Configuration
//!
//! ```yaml
//! lifecycle:
//!   enabled: true
//!   policies:
//!     - name: default
//!       hot_retention_days: 7
//!       warm_retention_days: 30
//!       cold_retention_days: 365
//!       delete_after_days: 730
//!   cost_optimization:
//!     target_monthly_cost: 100.00
//!     prefer_retrieval_latency: true
//! ```

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tracing::info;

/// Storage tier levels
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord, Default,
)]
#[serde(rename_all = "lowercase")]
pub enum StorageTier {
    /// Hot tier - local SSD/NVMe storage
    #[default]
    Hot,
    /// Warm tier - standard object storage (S3 Standard, Azure Hot)
    Warm,
    /// Cold tier - infrequent access (S3 Glacier IR, Azure Cool)
    Cold,
    /// Frozen tier - archive (S3 Deep Archive, Azure Archive)
    Frozen,
}

impl StorageTier {
    /// Get the typical retrieval latency for this tier
    pub fn typical_latency(&self) -> Duration {
        match self {
            StorageTier::Hot => Duration::from_millis(1),
            StorageTier::Warm => Duration::from_millis(100),
            StorageTier::Cold => Duration::from_secs(180), // 3 minutes
            StorageTier::Frozen => Duration::from_secs(43200), // 12 hours
        }
    }

    /// Get the cost factor relative to hot storage (per GB/month)
    pub fn cost_factor(&self) -> f64 {
        match self {
            StorageTier::Hot => 1.0,     // Reference: local SSD
            StorageTier::Warm => 0.15,   // ~$0.023/GB vs ~$0.15/GB local
            StorageTier::Cold => 0.05,   // ~$0.008/GB
            StorageTier::Frozen => 0.01, // ~$0.001/GB
        }
    }

    /// Get the next colder tier
    pub fn colder(&self) -> Option<StorageTier> {
        match self {
            StorageTier::Hot => Some(StorageTier::Warm),
            StorageTier::Warm => Some(StorageTier::Cold),
            StorageTier::Cold => Some(StorageTier::Frozen),
            StorageTier::Frozen => None,
        }
    }

    /// Get the next warmer tier
    pub fn warmer(&self) -> Option<StorageTier> {
        match self {
            StorageTier::Hot => None,
            StorageTier::Warm => Some(StorageTier::Hot),
            StorageTier::Cold => Some(StorageTier::Warm),
            StorageTier::Frozen => Some(StorageTier::Cold),
        }
    }
}

/// Lifecycle policy for a topic or partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecyclePolicy {
    /// Policy name
    pub name: String,
    /// Days to keep data in hot tier
    #[serde(default = "default_hot_days")]
    pub hot_retention_days: u32,
    /// Days to keep data in warm tier
    #[serde(default = "default_warm_days")]
    pub warm_retention_days: u32,
    /// Days to keep data in cold tier
    #[serde(default = "default_cold_days")]
    pub cold_retention_days: u32,
    /// Days after which data is deleted (0 = never)
    #[serde(default)]
    pub delete_after_days: u32,
    /// Minimum access count to keep in hot tier
    #[serde(default = "default_hot_access_threshold")]
    pub hot_access_threshold: u32,
    /// Enable predictive tiering based on access patterns
    #[serde(default = "default_predictive")]
    pub predictive_tiering: bool,
}

fn default_hot_days() -> u32 {
    7
}
fn default_warm_days() -> u32 {
    30
}
fn default_cold_days() -> u32 {
    365
}
fn default_hot_access_threshold() -> u32 {
    10
}
fn default_predictive() -> bool {
    true
}

impl Default for LifecyclePolicy {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            hot_retention_days: default_hot_days(),
            warm_retention_days: default_warm_days(),
            cold_retention_days: default_cold_days(),
            delete_after_days: 0,
            hot_access_threshold: default_hot_access_threshold(),
            predictive_tiering: default_predictive(),
        }
    }
}

impl LifecyclePolicy {
    /// Determine the target tier for data based on age
    pub fn target_tier(&self, age_days: u32) -> StorageTier {
        if age_days <= self.hot_retention_days {
            StorageTier::Hot
        } else if age_days <= self.hot_retention_days + self.warm_retention_days {
            StorageTier::Warm
        } else if age_days
            <= self.hot_retention_days + self.warm_retention_days + self.cold_retention_days
        {
            StorageTier::Cold
        } else {
            StorageTier::Frozen
        }
    }

    /// Check if data should be deleted based on policy
    pub fn should_delete(&self, age_days: u32) -> bool {
        self.delete_after_days > 0 && age_days > self.delete_after_days
    }
}

/// Cost optimization configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostOptimizationConfig {
    /// Target monthly cost in USD
    #[serde(default)]
    pub target_monthly_cost: Option<f64>,
    /// Prefer faster retrieval over cost savings
    #[serde(default)]
    pub prefer_retrieval_latency: bool,
    /// Estimated cost per GB for hot storage (local SSD)
    #[serde(default = "default_hot_cost")]
    pub hot_cost_per_gb: f64,
    /// Estimated cost per GB for warm storage
    #[serde(default = "default_warm_cost")]
    pub warm_cost_per_gb: f64,
    /// Estimated cost per GB for cold storage
    #[serde(default = "default_cold_cost")]
    pub cold_cost_per_gb: f64,
    /// Estimated cost per GB for frozen storage
    #[serde(default = "default_frozen_cost")]
    pub frozen_cost_per_gb: f64,
}

fn default_hot_cost() -> f64 {
    0.15
} // Local SSD
fn default_warm_cost() -> f64 {
    0.023
} // S3 Standard
fn default_cold_cost() -> f64 {
    0.0125
} // S3 Glacier IR
fn default_frozen_cost() -> f64 {
    0.001
} // S3 Deep Archive

impl Default for CostOptimizationConfig {
    fn default() -> Self {
        Self {
            target_monthly_cost: None,
            prefer_retrieval_latency: false,
            hot_cost_per_gb: default_hot_cost(),
            warm_cost_per_gb: default_warm_cost(),
            cold_cost_per_gb: default_cold_cost(),
            frozen_cost_per_gb: default_frozen_cost(),
        }
    }
}

/// Lifecycle configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleConfig {
    /// Enable lifecycle management
    pub enabled: bool,
    /// Default policy for topics without specific policy
    pub default_policy: LifecyclePolicy,
    /// Topic-specific policies
    #[serde(default)]
    pub topic_policies: HashMap<String, LifecyclePolicy>,
    /// Cost optimization settings
    #[serde(default)]
    pub cost_optimization: CostOptimizationConfig,
    /// Check interval in seconds
    #[serde(default = "default_check_interval")]
    pub check_interval_secs: u64,
}

fn default_check_interval() -> u64 {
    3600
} // 1 hour

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_policy: LifecyclePolicy::default(),
            topic_policies: HashMap::new(),
            cost_optimization: CostOptimizationConfig::default(),
            check_interval_secs: default_check_interval(),
        }
    }
}

/// Access pattern tracker for a segment
#[derive(Debug, Clone)]
pub struct AccessPattern {
    /// Segment ID
    pub segment_id: u64,
    /// Topic name
    pub topic: String,
    /// Partition
    pub partition: i32,
    /// Segment age in days
    pub age_days: u32,
    /// Segment size in bytes
    pub size_bytes: u64,
    /// Current tier
    pub current_tier: StorageTier,
    /// Total access count
    pub access_count: u64,
    /// Access count in last 24 hours
    pub access_count_24h: u64,
    /// Access count in last 7 days
    pub access_count_7d: u64,
    /// Last access time
    pub last_access: Option<Instant>,
    /// Created time
    pub created_at: SystemTime,
}

impl AccessPattern {
    /// Check if segment is "hot" based on access pattern
    pub fn is_hot(&self, threshold: u32) -> bool {
        self.access_count_24h >= threshold as u64
    }

    /// Calculate access frequency (accesses per day)
    pub fn access_frequency(&self) -> f64 {
        if self.age_days == 0 {
            return self.access_count as f64;
        }
        self.access_count as f64 / self.age_days as f64
    }

    /// Predict if segment will be accessed in next period
    pub fn predict_access(&self) -> bool {
        // Simple heuristic: if accessed recently and frequently, likely to be accessed again
        if self.access_count_24h > 0 {
            return true;
        }
        if self.access_count_7d > 2 {
            return true;
        }
        self.access_frequency() > 0.1
    }
}

/// Tiering recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TieringRecommendation {
    /// Segment identifier
    pub segment_id: u64,
    /// Topic name
    pub topic: String,
    /// Partition
    pub partition: i32,
    /// Current tier
    pub current_tier: StorageTier,
    /// Recommended tier
    pub recommended_tier: StorageTier,
    /// Reason for recommendation
    pub reason: String,
    /// Estimated monthly savings in USD
    pub estimated_savings: f64,
    /// Priority (higher = more urgent)
    pub priority: u32,
}

/// Cost estimate for storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostEstimate {
    /// Total data size in bytes
    pub total_bytes: u64,
    /// Bytes per tier
    pub bytes_by_tier: HashMap<StorageTier, u64>,
    /// Current monthly cost
    pub current_monthly_cost: f64,
    /// Projected cost if optimized
    pub optimized_monthly_cost: f64,
    /// Potential savings
    pub potential_savings: f64,
    /// Recommendations count
    pub recommendation_count: usize,
}

/// Lifecycle manager for intelligent tiered storage
pub struct LifecycleManager {
    /// Configuration
    config: LifecycleConfig,
    /// Access patterns by segment key (topic/partition/segment_id)
    access_patterns: RwLock<HashMap<String, AccessPattern>>,
    /// Pending transitions
    pending_transitions: RwLock<Vec<TieringRecommendation>>,
    /// Statistics
    stats: LifecycleStats,
}

/// Lifecycle statistics
#[derive(Debug, Default)]
pub struct LifecycleStats {
    /// Segments tracked
    pub segments_tracked: AtomicU64,
    /// Transitions completed
    pub transitions_completed: AtomicU64,
    /// Bytes moved to colder tiers
    pub bytes_moved_cold: AtomicU64,
    /// Bytes moved to warmer tiers
    pub bytes_moved_warm: AtomicU64,
    /// Estimated cost savings
    pub estimated_savings: AtomicU64, // Stored as cents
    /// Last check time (reserved for future periodic check tracking)
    #[allow(dead_code)]
    last_check: RwLock<Option<Instant>>,
}

impl LifecycleManager {
    /// Create a new lifecycle manager
    pub fn new(config: LifecycleConfig) -> Self {
        info!(
            enabled = config.enabled,
            default_hot_days = config.default_policy.hot_retention_days,
            "Lifecycle manager initialized"
        );

        Self {
            config,
            access_patterns: RwLock::new(HashMap::new()),
            pending_transitions: RwLock::new(Vec::new()),
            stats: LifecycleStats::default(),
        }
    }

    /// Check if lifecycle management is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the policy for a topic
    pub fn get_policy(&self, topic: &str) -> &LifecyclePolicy {
        self.config
            .topic_policies
            .get(topic)
            .unwrap_or(&self.config.default_policy)
    }

    /// Record an access to a segment
    pub fn record_access(
        &self,
        topic: &str,
        partition: i32,
        segment_id: u64,
        size_bytes: u64,
        current_tier: StorageTier,
        created_at: SystemTime,
    ) {
        let key = format!("{}/{}/{}", topic, partition, segment_id);
        let now = Instant::now();

        let mut patterns = self.access_patterns.write();

        let age_days = created_at
            .elapsed()
            .map(|d| (d.as_secs() / 86400) as u32)
            .unwrap_or(0);

        if let Some(pattern) = patterns.get_mut(&key) {
            pattern.access_count += 1;
            pattern.access_count_24h += 1;
            pattern.access_count_7d += 1;
            pattern.last_access = Some(now);
            pattern.age_days = age_days;
        } else {
            patterns.insert(
                key,
                AccessPattern {
                    segment_id,
                    topic: topic.to_string(),
                    partition,
                    age_days,
                    size_bytes,
                    current_tier,
                    access_count: 1,
                    access_count_24h: 1,
                    access_count_7d: 1,
                    last_access: Some(now),
                    created_at,
                },
            );
            self.stats.segments_tracked.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Update access counts (call periodically to decay 24h/7d counters)
    pub fn decay_access_counts(&self) {
        let mut patterns = self.access_patterns.write();
        for pattern in patterns.values_mut() {
            // Decay counters (simple exponential decay)
            pattern.access_count_24h = pattern.access_count_24h.saturating_sub(1);
            pattern.access_count_7d = (pattern.access_count_7d as f64 * 0.9) as u64;
        }
    }

    /// Analyze all segments and generate tiering recommendations
    pub fn analyze(&self) -> Vec<TieringRecommendation> {
        let patterns = self.access_patterns.read();
        let mut recommendations = Vec::new();

        for pattern in patterns.values() {
            if let Some(rec) = self.analyze_segment(pattern) {
                recommendations.push(rec);
            }
        }

        // Sort by priority (highest first)
        recommendations.sort_by(|a, b| b.priority.cmp(&a.priority));

        // Store for later retrieval
        *self.pending_transitions.write() = recommendations.clone();

        recommendations
    }

    /// Analyze a single segment and generate recommendation
    fn analyze_segment(&self, pattern: &AccessPattern) -> Option<TieringRecommendation> {
        let policy = self.get_policy(&pattern.topic);

        // Determine target tier based on age
        let age_target_tier = policy.target_tier(pattern.age_days);

        // Adjust based on access patterns if predictive tiering is enabled
        let recommended_tier = if policy.predictive_tiering {
            if pattern.is_hot(policy.hot_access_threshold) && age_target_tier > StorageTier::Hot {
                // Segment is being accessed frequently, keep it hot
                StorageTier::Hot
            } else if pattern.predict_access() && age_target_tier > StorageTier::Warm {
                // Likely to be accessed, keep it warm
                StorageTier::Warm
            } else {
                age_target_tier
            }
        } else {
            age_target_tier
        };

        // Don't recommend if already at target tier
        if pattern.current_tier == recommended_tier {
            return None;
        }

        // Calculate estimated savings
        let current_cost = self.tier_cost(pattern.current_tier, pattern.size_bytes);
        let recommended_cost = self.tier_cost(recommended_tier, pattern.size_bytes);
        let estimated_savings = (current_cost - recommended_cost).max(0.0);

        // Calculate priority
        let priority = self.calculate_priority(pattern, recommended_tier, estimated_savings);

        let reason = if recommended_tier < pattern.current_tier {
            format!(
                "Segment age ({} days) exceeds {} tier threshold",
                pattern.age_days,
                format!("{:?}", pattern.current_tier).to_lowercase()
            )
        } else {
            format!(
                "High access frequency ({:.2}/day) - keeping in warmer tier",
                pattern.access_frequency()
            )
        };

        Some(TieringRecommendation {
            segment_id: pattern.segment_id,
            topic: pattern.topic.clone(),
            partition: pattern.partition,
            current_tier: pattern.current_tier,
            recommended_tier,
            reason,
            estimated_savings,
            priority,
        })
    }

    /// Calculate monthly cost for a tier
    fn tier_cost(&self, tier: StorageTier, size_bytes: u64) -> f64 {
        let gb = size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        let cost_per_gb = match tier {
            StorageTier::Hot => self.config.cost_optimization.hot_cost_per_gb,
            StorageTier::Warm => self.config.cost_optimization.warm_cost_per_gb,
            StorageTier::Cold => self.config.cost_optimization.cold_cost_per_gb,
            StorageTier::Frozen => self.config.cost_optimization.frozen_cost_per_gb,
        };
        gb * cost_per_gb
    }

    /// Calculate priority for a transition
    fn calculate_priority(
        &self,
        pattern: &AccessPattern,
        recommended_tier: StorageTier,
        estimated_savings: f64,
    ) -> u32 {
        let mut priority = 0u32;

        // Higher savings = higher priority
        priority += (estimated_savings * 100.0) as u32;

        // Larger segments = higher priority
        let gb = pattern.size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        priority += (gb * 10.0) as u32;

        // Moving to colder tier = higher priority (cost savings)
        if recommended_tier > pattern.current_tier {
            priority += 50;
        }

        // Older data = higher priority for cold storage
        priority += pattern.age_days.min(100);

        priority
    }

    /// Estimate total storage costs
    pub fn estimate_costs(&self) -> CostEstimate {
        let patterns = self.access_patterns.read();
        let recommendations = self.pending_transitions.read();

        let mut bytes_by_tier: HashMap<StorageTier, u64> = HashMap::new();
        let mut total_bytes = 0u64;

        for pattern in patterns.values() {
            total_bytes += pattern.size_bytes;
            *bytes_by_tier.entry(pattern.current_tier).or_default() += pattern.size_bytes;
        }

        // Calculate current cost
        let current_monthly_cost: f64 = bytes_by_tier
            .iter()
            .map(|(tier, bytes)| self.tier_cost(*tier, *bytes))
            .sum();

        // Calculate potential savings
        let potential_savings: f64 = recommendations.iter().map(|r| r.estimated_savings).sum();
        let optimized_monthly_cost = current_monthly_cost - potential_savings;

        CostEstimate {
            total_bytes,
            bytes_by_tier,
            current_monthly_cost,
            optimized_monthly_cost,
            potential_savings,
            recommendation_count: recommendations.len(),
        }
    }

    /// Get pending transitions
    pub fn get_pending_transitions(&self) -> Vec<TieringRecommendation> {
        self.pending_transitions.read().clone()
    }

    /// Mark a transition as complete
    pub fn complete_transition(
        &self,
        topic: &str,
        partition: i32,
        segment_id: u64,
        new_tier: StorageTier,
    ) {
        let key = format!("{}/{}/{}", topic, partition, segment_id);

        let mut patterns = self.access_patterns.write();
        if let Some(pattern) = patterns.get_mut(&key) {
            let bytes_moved = pattern.size_bytes;
            let was_warming = new_tier < pattern.current_tier;

            pattern.current_tier = new_tier;

            // Update stats
            self.stats
                .transitions_completed
                .fetch_add(1, Ordering::Relaxed);
            if was_warming {
                self.stats
                    .bytes_moved_warm
                    .fetch_add(bytes_moved, Ordering::Relaxed);
            } else {
                self.stats
                    .bytes_moved_cold
                    .fetch_add(bytes_moved, Ordering::Relaxed);
            }
        }

        // Remove from pending transitions
        let mut pending = self.pending_transitions.write();
        pending.retain(|r| {
            !(r.topic == topic && r.partition == partition && r.segment_id == segment_id)
        });

        info!(
            topic = %topic,
            partition = partition,
            segment_id = segment_id,
            new_tier = ?new_tier,
            "Tier transition completed"
        );
    }

    /// Get lifecycle statistics
    pub fn get_stats(&self) -> LifecycleStatsSnapshot {
        LifecycleStatsSnapshot {
            segments_tracked: self.stats.segments_tracked.load(Ordering::Relaxed),
            transitions_completed: self.stats.transitions_completed.load(Ordering::Relaxed),
            bytes_moved_cold: self.stats.bytes_moved_cold.load(Ordering::Relaxed),
            bytes_moved_warm: self.stats.bytes_moved_warm.load(Ordering::Relaxed),
            estimated_savings_cents: self.stats.estimated_savings.load(Ordering::Relaxed),
        }
    }
}

impl Default for LifecycleManager {
    fn default() -> Self {
        Self::new(LifecycleConfig::default())
    }
}

/// Snapshot of lifecycle statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleStatsSnapshot {
    pub segments_tracked: u64,
    pub transitions_completed: u64,
    pub bytes_moved_cold: u64,
    pub bytes_moved_warm: u64,
    pub estimated_savings_cents: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_tier_ordering() {
        assert!(StorageTier::Hot < StorageTier::Warm);
        assert!(StorageTier::Warm < StorageTier::Cold);
        assert!(StorageTier::Cold < StorageTier::Frozen);
    }

    #[test]
    fn test_storage_tier_transitions() {
        assert_eq!(StorageTier::Hot.colder(), Some(StorageTier::Warm));
        assert_eq!(StorageTier::Warm.warmer(), Some(StorageTier::Hot));
        assert_eq!(StorageTier::Frozen.colder(), None);
        assert_eq!(StorageTier::Hot.warmer(), None);
    }

    #[test]
    fn test_lifecycle_policy_target_tier() {
        let policy = LifecyclePolicy {
            hot_retention_days: 7,
            warm_retention_days: 30,
            cold_retention_days: 365,
            ..Default::default()
        };

        assert_eq!(policy.target_tier(1), StorageTier::Hot);
        assert_eq!(policy.target_tier(7), StorageTier::Hot);
        assert_eq!(policy.target_tier(8), StorageTier::Warm);
        assert_eq!(policy.target_tier(37), StorageTier::Warm);
        assert_eq!(policy.target_tier(38), StorageTier::Cold);
        assert_eq!(policy.target_tier(400), StorageTier::Cold);
        assert_eq!(policy.target_tier(403), StorageTier::Frozen);
    }

    #[test]
    fn test_lifecycle_manager_record_access() {
        let manager = LifecycleManager::new(LifecycleConfig {
            enabled: true,
            ..Default::default()
        });

        let created_at = SystemTime::now() - Duration::from_secs(86400 * 5); // 5 days ago

        manager.record_access(
            "test-topic",
            0,
            100,
            1024 * 1024,
            StorageTier::Hot,
            created_at,
        );
        manager.record_access(
            "test-topic",
            0,
            100,
            1024 * 1024,
            StorageTier::Hot,
            created_at,
        );

        let patterns = manager.access_patterns.read();
        let pattern = patterns.get("test-topic/0/100").unwrap();

        assert_eq!(pattern.access_count, 2);
        assert_eq!(pattern.access_count_24h, 2);
    }

    #[test]
    fn test_lifecycle_manager_analyze() {
        let config = LifecycleConfig {
            enabled: true,
            default_policy: LifecyclePolicy {
                hot_retention_days: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        let manager = LifecycleManager::new(config);

        // Record an old segment that hasn't been accessed
        let created_at = SystemTime::now() - Duration::from_secs(86400 * 10); // 10 days ago
        manager.record_access(
            "test-topic",
            0,
            100,
            1024 * 1024,
            StorageTier::Hot,
            created_at,
        );

        let recommendations = manager.analyze();

        // Should recommend moving to warmer tier
        assert!(!recommendations.is_empty());
        assert_eq!(recommendations[0].current_tier, StorageTier::Hot);
        assert!(recommendations[0].recommended_tier > StorageTier::Hot);
    }

    #[test]
    fn test_cost_estimation() {
        let manager = LifecycleManager::new(LifecycleConfig {
            enabled: true,
            ..Default::default()
        });

        let created_at = SystemTime::now();

        // Add 1GB to hot tier
        manager.record_access(
            "topic1",
            0,
            1,
            1024 * 1024 * 1024,
            StorageTier::Hot,
            created_at,
        );
        // Add 1GB to warm tier
        manager.record_access(
            "topic2",
            0,
            2,
            1024 * 1024 * 1024,
            StorageTier::Warm,
            created_at,
        );

        let estimate = manager.estimate_costs();

        assert_eq!(estimate.total_bytes, 2 * 1024 * 1024 * 1024);
        assert!(estimate.current_monthly_cost > 0.0);
    }

    #[test]
    fn test_access_pattern_hot_detection() {
        let pattern = AccessPattern {
            segment_id: 1,
            topic: "test".to_string(),
            partition: 0,
            age_days: 30,
            size_bytes: 1024,
            current_tier: StorageTier::Warm,
            access_count: 100,
            access_count_24h: 15,
            access_count_7d: 50,
            last_access: Some(Instant::now()),
            created_at: SystemTime::now(),
        };

        assert!(pattern.is_hot(10));
        assert!(!pattern.is_hot(20));
        assert!(pattern.predict_access());
    }
}
