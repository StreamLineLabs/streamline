//! Storage Tiers
//!
//! Provides tiered storage management with different performance and cost characteristics.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Storage tier types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum StorageTier {
    /// In-memory storage (fastest, most expensive)
    #[default]
    Memory,
    /// SSD storage (fast, moderate cost)
    Ssd,
    /// HDD storage (moderate speed, lower cost)
    Hdd,
    /// Object storage like S3 (slow, cheapest)
    ObjectStorage,
    /// Glacier-like cold storage (very slow, very cheap)
    ColdArchive,
}

impl StorageTier {
    /// Get relative speed factor (higher is faster)
    pub fn speed_factor(&self) -> f64 {
        match self {
            StorageTier::Memory => 100.0,
            StorageTier::Ssd => 10.0,
            StorageTier::Hdd => 1.0,
            StorageTier::ObjectStorage => 0.1,
            StorageTier::ColdArchive => 0.01,
        }
    }

    /// Get relative cost factor (higher is more expensive)
    pub fn cost_factor(&self) -> f64 {
        match self {
            StorageTier::Memory => 100.0,
            StorageTier::Ssd => 10.0,
            StorageTier::Hdd => 1.0,
            StorageTier::ObjectStorage => 0.1,
            StorageTier::ColdArchive => 0.01,
        }
    }

    /// Is this a local tier?
    pub fn is_local(&self) -> bool {
        matches!(
            self,
            StorageTier::Memory | StorageTier::Ssd | StorageTier::Hdd
        )
    }

    /// Is this a remote tier?
    pub fn is_remote(&self) -> bool {
        matches!(self, StorageTier::ObjectStorage | StorageTier::ColdArchive)
    }

    /// Get default path prefix
    pub fn default_path_prefix(&self) -> &'static str {
        match self {
            StorageTier::Memory => "/dev/shm/streamline",
            StorageTier::Ssd => "/data/ssd/streamline",
            StorageTier::Hdd => "/data/hdd/streamline",
            StorageTier::ObjectStorage => "s3://streamline-archive",
            StorageTier::ColdArchive => "glacier://streamline-cold",
        }
    }
}

/// Storage tier capabilities
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierCapabilities {
    /// Supports random reads
    pub random_read: bool,
    /// Supports append writes
    pub append_write: bool,
    /// Supports in-place updates
    pub in_place_update: bool,
    /// Supports atomic operations
    pub atomic_ops: bool,
    /// Maximum file size (bytes)
    pub max_file_size: u64,
    /// Minimum file size (bytes)
    pub min_file_size: u64,
    /// Read latency (milliseconds, typical)
    pub read_latency_ms: u32,
    /// Write latency (milliseconds, typical)
    pub write_latency_ms: u32,
}

impl Default for TierCapabilities {
    fn default() -> Self {
        Self {
            random_read: true,
            append_write: true,
            in_place_update: true,
            atomic_ops: true,
            max_file_size: u64::MAX,
            min_file_size: 0,
            read_latency_ms: 1,
            write_latency_ms: 1,
        }
    }
}

impl TierCapabilities {
    /// Create capabilities for memory tier
    pub fn memory() -> Self {
        Self {
            random_read: true,
            append_write: true,
            in_place_update: true,
            atomic_ops: true,
            max_file_size: 1024 * 1024 * 1024, // 1GB
            min_file_size: 0,
            read_latency_ms: 0,
            write_latency_ms: 0,
        }
    }

    /// Create capabilities for SSD tier
    pub fn ssd() -> Self {
        Self {
            random_read: true,
            append_write: true,
            in_place_update: true,
            atomic_ops: true,
            max_file_size: 100 * 1024 * 1024 * 1024, // 100GB
            min_file_size: 0,
            read_latency_ms: 1,
            write_latency_ms: 1,
        }
    }

    /// Create capabilities for HDD tier
    pub fn hdd() -> Self {
        Self {
            random_read: true,
            append_write: true,
            in_place_update: true,
            atomic_ops: true,
            max_file_size: 1024 * 1024 * 1024 * 1024, // 1TB
            min_file_size: 0,
            read_latency_ms: 10,
            write_latency_ms: 10,
        }
    }

    /// Create capabilities for object storage
    pub fn object_storage() -> Self {
        Self {
            random_read: false, // Range reads are expensive
            append_write: false,
            in_place_update: false,
            atomic_ops: false,
            max_file_size: 5 * 1024 * 1024 * 1024 * 1024, // 5TB
            min_file_size: 0,
            read_latency_ms: 100,
            write_latency_ms: 200,
        }
    }

    /// Create capabilities for cold archive
    pub fn cold_archive() -> Self {
        Self {
            random_read: false,
            append_write: false,
            in_place_update: false,
            atomic_ops: false,
            max_file_size: u64::MAX,
            min_file_size: 1024 * 1024, // 1MB minimum
            read_latency_ms: 3600000,   // Hours to retrieve
            write_latency_ms: 1000,
        }
    }
}

/// Storage tier configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageTierConfig {
    /// Tier type
    pub tier: StorageTier,
    /// Base path for this tier
    pub path: PathBuf,
    /// Maximum capacity (bytes)
    pub max_capacity: u64,
    /// Reserved capacity (bytes)
    pub reserved_capacity: u64,
    /// Cost per GB per month (dollars)
    pub cost_per_gb_month: f64,
    /// Tier capabilities
    pub capabilities: TierCapabilities,
    /// Enable compression for this tier
    pub compression_enabled: bool,
    /// Enable encryption for this tier
    pub encryption_enabled: bool,
}

impl StorageTierConfig {
    /// Create default configuration for a tier
    pub fn default_for(tier: StorageTier) -> Self {
        let (path, max_capacity, cost) = match tier {
            StorageTier::Memory => (
                PathBuf::from("/dev/shm/streamline"),
                8 * 1024 * 1024 * 1024, // 8GB
                100.0,
            ),
            StorageTier::Ssd => (
                PathBuf::from("/data/ssd/streamline"),
                500 * 1024 * 1024 * 1024, // 500GB
                0.20,
            ),
            StorageTier::Hdd => (
                PathBuf::from("/data/hdd/streamline"),
                2 * 1024 * 1024 * 1024 * 1024, // 2TB
                0.02,
            ),
            StorageTier::ObjectStorage => (PathBuf::from("s3://streamline-data"), u64::MAX, 0.023),
            StorageTier::ColdArchive => (
                PathBuf::from("glacier://streamline-archive"),
                u64::MAX,
                0.004,
            ),
        };

        let capabilities = match tier {
            StorageTier::Memory => TierCapabilities::memory(),
            StorageTier::Ssd => TierCapabilities::ssd(),
            StorageTier::Hdd => TierCapabilities::hdd(),
            StorageTier::ObjectStorage => TierCapabilities::object_storage(),
            StorageTier::ColdArchive => TierCapabilities::cold_archive(),
        };

        Self {
            tier,
            path,
            max_capacity,
            reserved_capacity: 0,
            cost_per_gb_month: cost,
            capabilities,
            compression_enabled: !matches!(tier, StorageTier::Memory),
            encryption_enabled: false,
        }
    }

    /// Set path
    pub fn with_path(mut self, path: PathBuf) -> Self {
        self.path = path;
        self
    }

    /// Set max capacity
    pub fn with_max_capacity(mut self, capacity: u64) -> Self {
        self.max_capacity = capacity;
        self
    }

    /// Enable encryption
    pub fn with_encryption(mut self) -> Self {
        self.encryption_enabled = true;
        self
    }
}

/// Storage tier metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TierMetrics {
    /// Total reads
    pub reads: u64,
    /// Total writes
    pub writes: u64,
    /// Bytes read
    pub bytes_read: u64,
    /// Bytes written
    pub bytes_written: u64,
    /// Average read latency (microseconds)
    pub avg_read_latency_us: u64,
    /// Average write latency (microseconds)
    pub avg_write_latency_us: u64,
    /// Read errors
    pub read_errors: u64,
    /// Write errors
    pub write_errors: u64,
}

/// Storage tier statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierStats {
    /// Tier type
    pub tier: StorageTier,
    /// Current used capacity (bytes)
    pub used_capacity: u64,
    /// Maximum capacity (bytes)
    pub max_capacity: u64,
    /// Number of files/objects
    pub file_count: u64,
    /// Usage percentage
    pub usage_percent: f64,
    /// Estimated monthly cost
    pub estimated_monthly_cost: f64,
    /// Metrics
    pub metrics: TierMetrics,
}

impl TierStats {
    /// Create new tier stats
    pub fn new(tier: StorageTier, max_capacity: u64) -> Self {
        Self {
            tier,
            used_capacity: 0,
            max_capacity,
            file_count: 0,
            usage_percent: 0.0,
            estimated_monthly_cost: 0.0,
            metrics: TierMetrics::default(),
        }
    }

    /// Update usage statistics
    pub fn update_usage(&mut self, used_bytes: u64, file_count: u64, cost_per_gb_month: f64) {
        self.used_capacity = used_bytes;
        self.file_count = file_count;
        self.usage_percent = if self.max_capacity > 0 {
            (used_bytes as f64 / self.max_capacity as f64) * 100.0
        } else {
            0.0
        };
        let gb = used_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        self.estimated_monthly_cost = gb * cost_per_gb_month;
    }

    /// Check if tier is near capacity
    pub fn is_near_capacity(&self, threshold_percent: f64) -> bool {
        self.usage_percent >= threshold_percent
    }
}

/// Tier manager for managing storage tiers
pub struct TierManager {
    configs: HashMap<StorageTier, StorageTierConfig>,
    stats: HashMap<StorageTier, TierStats>,
}

impl TierManager {
    /// Create a new tier manager
    pub fn new() -> Self {
        let mut configs = HashMap::new();
        let mut stats = HashMap::new();

        // Initialize default tiers
        for tier in [
            StorageTier::Memory,
            StorageTier::Ssd,
            StorageTier::Hdd,
            StorageTier::ObjectStorage,
        ] {
            let config = StorageTierConfig::default_for(tier);
            stats.insert(tier, TierStats::new(tier, config.max_capacity));
            configs.insert(tier, config);
        }

        Self { configs, stats }
    }

    /// Configure a tier
    pub fn configure_tier(&mut self, config: StorageTierConfig) {
        let tier = config.tier;
        self.stats
            .insert(tier, TierStats::new(tier, config.max_capacity));
        self.configs.insert(tier, config);
    }

    /// Get tier configuration
    pub fn get_tier_config(&self, tier: StorageTier) -> Option<&StorageTierConfig> {
        self.configs.get(&tier)
    }

    /// Get tier statistics
    pub fn get_tier_stats(&self, tier: StorageTier) -> Option<&TierStats> {
        self.stats.get(&tier)
    }

    /// Update tier usage
    pub fn update_usage(&mut self, tier: StorageTier, used_bytes: u64, file_count: u64) {
        if let Some(config) = self.configs.get(&tier) {
            if let Some(stats) = self.stats.get_mut(&tier) {
                stats.update_usage(used_bytes, file_count, config.cost_per_gb_month);
            }
        }
    }

    /// Record a read operation
    pub fn record_read(&mut self, tier: StorageTier, bytes: u64, latency_us: u64) {
        if let Some(stats) = self.stats.get_mut(&tier) {
            let m = &mut stats.metrics;
            m.reads += 1;
            m.bytes_read += bytes;
            // Update rolling average
            m.avg_read_latency_us = (m.avg_read_latency_us * (m.reads - 1) + latency_us) / m.reads;
        }
    }

    /// Record a write operation
    pub fn record_write(&mut self, tier: StorageTier, bytes: u64, latency_us: u64) {
        if let Some(stats) = self.stats.get_mut(&tier) {
            let m = &mut stats.metrics;
            m.writes += 1;
            m.bytes_written += bytes;
            m.avg_write_latency_us =
                (m.avg_write_latency_us * (m.writes - 1) + latency_us) / m.writes;
        }
    }

    /// Get all tier stats
    pub fn all_stats(&self) -> Vec<&TierStats> {
        self.stats.values().collect()
    }

    /// Find best tier for data based on access pattern
    pub fn recommend_tier(&self, access_frequency: AccessFrequency, data_size: u64) -> StorageTier {
        match access_frequency {
            AccessFrequency::Realtime => StorageTier::Memory,
            AccessFrequency::Frequent => StorageTier::Ssd,
            AccessFrequency::Occasional => StorageTier::Hdd,
            AccessFrequency::Rare => StorageTier::ObjectStorage,
            AccessFrequency::Archive => {
                if data_size > 1024 * 1024 {
                    // > 1MB
                    StorageTier::ColdArchive
                } else {
                    StorageTier::ObjectStorage
                }
            }
        }
    }

    /// Calculate total storage cost
    pub fn total_monthly_cost(&self) -> f64 {
        self.stats.values().map(|s| s.estimated_monthly_cost).sum()
    }

    /// Get tiers near capacity
    pub fn tiers_near_capacity(&self, threshold_percent: f64) -> Vec<StorageTier> {
        self.stats
            .iter()
            .filter(|(_, s)| s.is_near_capacity(threshold_percent))
            .map(|(t, _)| *t)
            .collect()
    }
}

impl Default for TierManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Access frequency classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessFrequency {
    /// Real-time access required
    Realtime,
    /// Accessed frequently (multiple times per day)
    Frequent,
    /// Accessed occasionally (weekly)
    Occasional,
    /// Rarely accessed (monthly or less)
    Rare,
    /// Archive - almost never accessed
    Archive,
}

impl AccessFrequency {
    /// Classify based on accesses per day
    pub fn from_accesses_per_day(accesses: f64) -> Self {
        if accesses > 100.0 {
            AccessFrequency::Realtime
        } else if accesses > 10.0 {
            AccessFrequency::Frequent
        } else if accesses > 1.0 {
            AccessFrequency::Occasional
        } else if accesses > 0.03 {
            // ~1/month
            AccessFrequency::Rare
        } else {
            AccessFrequency::Archive
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_tier_properties() {
        assert!(StorageTier::Memory.is_local());
        assert!(StorageTier::Ssd.is_local());
        assert!(StorageTier::Hdd.is_local());
        assert!(StorageTier::ObjectStorage.is_remote());
        assert!(StorageTier::ColdArchive.is_remote());
    }

    #[test]
    fn test_storage_tier_speed_cost_tradeoff() {
        // Faster tiers should be more expensive
        assert!(StorageTier::Memory.speed_factor() > StorageTier::Ssd.speed_factor());
        assert!(StorageTier::Memory.cost_factor() > StorageTier::Ssd.cost_factor());

        assert!(StorageTier::Ssd.speed_factor() > StorageTier::Hdd.speed_factor());
        assert!(StorageTier::Ssd.cost_factor() > StorageTier::Hdd.cost_factor());
    }

    #[test]
    fn test_tier_capabilities() {
        let memory_caps = TierCapabilities::memory();
        assert!(memory_caps.random_read);
        assert!(memory_caps.in_place_update);
        assert_eq!(memory_caps.read_latency_ms, 0);

        let object_caps = TierCapabilities::object_storage();
        assert!(!object_caps.random_read);
        assert!(!object_caps.in_place_update);
        assert!(object_caps.read_latency_ms > 0);
    }

    #[test]
    fn test_tier_config_default() {
        let ssd_config = StorageTierConfig::default_for(StorageTier::Ssd);
        assert_eq!(ssd_config.tier, StorageTier::Ssd);
        assert!(ssd_config.compression_enabled);
        assert!(!ssd_config.encryption_enabled);
    }

    #[test]
    fn test_tier_stats() {
        let mut stats = TierStats::new(StorageTier::Ssd, 100 * 1024 * 1024 * 1024);
        stats.update_usage(50 * 1024 * 1024 * 1024, 100, 0.20);

        assert_eq!(stats.usage_percent, 50.0);
        assert!(!stats.is_near_capacity(80.0));
        assert!(stats.is_near_capacity(40.0));
    }

    #[test]
    fn test_tier_manager() {
        let mut manager = TierManager::new();

        // Update usage
        manager.update_usage(StorageTier::Ssd, 100 * 1024 * 1024 * 1024, 1000);

        let stats = manager.get_tier_stats(StorageTier::Ssd).unwrap();
        assert_eq!(stats.file_count, 1000);

        // Record operations
        manager.record_read(StorageTier::Ssd, 1024 * 1024, 100);
        manager.record_write(StorageTier::Ssd, 2048 * 1024, 200);

        let stats = manager.get_tier_stats(StorageTier::Ssd).unwrap();
        assert_eq!(stats.metrics.reads, 1);
        assert_eq!(stats.metrics.writes, 1);
    }

    #[test]
    fn test_tier_recommendation() {
        let manager = TierManager::new();

        assert_eq!(
            manager.recommend_tier(AccessFrequency::Realtime, 1024),
            StorageTier::Memory
        );
        assert_eq!(
            manager.recommend_tier(AccessFrequency::Frequent, 1024),
            StorageTier::Ssd
        );
        assert_eq!(
            manager.recommend_tier(AccessFrequency::Occasional, 1024),
            StorageTier::Hdd
        );
        assert_eq!(
            manager.recommend_tier(AccessFrequency::Archive, 10 * 1024 * 1024),
            StorageTier::ColdArchive
        );
    }

    #[test]
    fn test_access_frequency_classification() {
        assert_eq!(
            AccessFrequency::from_accesses_per_day(200.0),
            AccessFrequency::Realtime
        );
        assert_eq!(
            AccessFrequency::from_accesses_per_day(50.0),
            AccessFrequency::Frequent
        );
        assert_eq!(
            AccessFrequency::from_accesses_per_day(5.0),
            AccessFrequency::Occasional
        );
        assert_eq!(
            AccessFrequency::from_accesses_per_day(0.1),
            AccessFrequency::Rare
        );
        assert_eq!(
            AccessFrequency::from_accesses_per_day(0.01),
            AccessFrequency::Archive
        );
    }
}
