//! Multi-Cloud Federation Manager
//!
//! Provides cross-region and cross-cloud topic federation with:
//! - Active-active replication
//! - Conflict-free replicated data types (CRDTs)
//! - Cross-cloud routing
//! - Latency-aware data placement
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │                    Multi-Cloud Federation                           │
//! │                                                                     │
//! │   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐        │
//! │   │ AWS Region  │◄────►│ GCP Region  │◄────►│ Azure Region│        │
//! │   │  US-East    │      │  US-West    │      │  EU-West    │        │
//! │   └─────────────┘      └─────────────┘      └─────────────┘        │
//! │          │                    │                    │                │
//! │          └────────────────────┴────────────────────┘                │
//! │                            │                                        │
//! │                    Federation Mesh                                  │
//! │                                                                     │
//! └─────────────────────────────────────────────────────────────────────┘
//! ```

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Cloud provider type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[allow(clippy::upper_case_acronyms)]
pub enum CloudProvider {
    /// Amazon Web Services
    AWS,
    /// Google Cloud Platform
    GCP,
    /// Microsoft Azure
    Azure,
    /// Private/On-premise
    #[default]
    Private,
    /// Other cloud providers
    Other,
}

/// Federation region
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederationRegion {
    /// Region ID (e.g., "us-east-1", "europe-west1")
    pub id: String,
    /// Human-readable name
    pub name: String,
    /// Cloud provider
    pub provider: CloudProvider,
    /// Region endpoint URL
    pub endpoint: String,
    /// Is this region active
    pub active: bool,
    /// Is this the primary region
    pub primary: bool,
    /// Region priority (lower = higher priority)
    pub priority: u32,
    /// Region metadata
    pub metadata: HashMap<String, String>,
    /// Health status
    pub health: RegionHealth,
}

/// Region health status
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RegionHealth {
    /// Region is healthy
    Healthy,
    /// Region has elevated latency
    Degraded,
    /// Region is unreachable
    Unhealthy,
    /// Health unknown (not yet checked)
    #[default]
    Unknown,
}

/// Replication mode for federated topics
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplicationMode {
    /// Active-passive: One primary, others receive replicated data
    #[default]
    ActivePassive,
    /// Active-active: All regions can write, conflicts resolved
    ActiveActive,
    /// Read-only: Region only receives replicated data
    ReadOnly,
}

/// Conflict resolution strategy for active-active replication
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictStrategy {
    /// Last writer wins (based on timestamp)
    #[default]
    LastWriterWins,
    /// First writer wins
    FirstWriterWins,
    /// Primary region wins
    PrimaryWins,
    /// Use CRDT merge semantics
    CrdtMerge,
    /// Custom resolution (requires handler)
    Custom,
}

/// Federated topic configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FederatedTopicConfig {
    /// Topic name
    pub topic: String,
    /// Regions where topic is replicated
    pub regions: Vec<String>,
    /// Replication mode
    pub replication_mode: ReplicationMode,
    /// Conflict resolution strategy
    pub conflict_strategy: ConflictStrategy,
    /// Minimum replicas across regions
    pub min_replicas: u32,
    /// Maximum lag before alerting (milliseconds)
    pub max_lag_ms: u64,
    /// Enable compression for cross-region traffic
    pub compression_enabled: bool,
    /// Enable encryption for cross-region traffic
    pub encryption_enabled: bool,
}

impl Default for FederatedTopicConfig {
    fn default() -> Self {
        Self {
            topic: String::new(),
            regions: Vec::new(),
            replication_mode: ReplicationMode::ActivePassive,
            conflict_strategy: ConflictStrategy::LastWriterWins,
            min_replicas: 2,
            max_lag_ms: 10_000, // 10 seconds
            compression_enabled: true,
            encryption_enabled: true,
        }
    }
}

/// Replication lag information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationLag {
    /// Source region
    pub source_region: String,
    /// Target region
    pub target_region: String,
    /// Lag in milliseconds
    pub lag_ms: u64,
    /// Pending records
    pub pending_records: u64,
    /// Last update timestamp
    pub updated_at: i64,
}

/// Federation statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MultiCloudStats {
    /// Records replicated
    pub records_replicated: u64,
    /// Bytes replicated
    pub bytes_replicated: u64,
    /// Conflicts detected
    pub conflicts_detected: u64,
    /// Conflicts resolved
    pub conflicts_resolved: u64,
    /// Failed replications
    pub replication_failures: u64,
    /// Average replication latency (ms)
    pub avg_replication_latency_ms: f64,
}

/// Multi-cloud federation manager
pub struct MultiCloudFederation {
    /// Configuration
    config: MultiCloudConfig,
    /// Registered regions
    regions: Arc<RwLock<HashMap<String, FederationRegion>>>,
    /// Federated topics
    topics: Arc<RwLock<HashMap<String, FederatedTopicConfig>>>,
    /// Replication lag per topic per region pair
    lag: Arc<RwLock<HashMap<String, Vec<ReplicationLag>>>>,
    /// Statistics
    stats: Arc<MultiCloudStatsInner>,
    /// Running flag
    running: Arc<std::sync::atomic::AtomicBool>,
}

struct MultiCloudStatsInner {
    records_replicated: AtomicU64,
    bytes_replicated: AtomicU64,
    conflicts_detected: AtomicU64,
    conflicts_resolved: AtomicU64,
    replication_failures: AtomicU64,
}

/// Multi-cloud configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiCloudConfig {
    /// Enable multi-cloud federation
    pub enabled: bool,
    /// Local region ID
    pub local_region: String,
    /// Replication batch size
    pub batch_size: usize,
    /// Replication interval (milliseconds)
    pub replication_interval_ms: u64,
    /// Heartbeat interval (milliseconds)
    pub heartbeat_interval_ms: u64,
    /// Connection timeout (milliseconds)
    pub connection_timeout_ms: u64,
    /// Enable latency-aware routing
    pub latency_aware_routing: bool,
    /// Enable automatic failover
    pub auto_failover: bool,
    /// Failover threshold (missed heartbeats)
    pub failover_threshold: u32,
}

impl Default for MultiCloudConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            local_region: "local".to_string(),
            batch_size: 1000,
            replication_interval_ms: 100,
            heartbeat_interval_ms: 5000,
            connection_timeout_ms: 30_000,
            latency_aware_routing: true,
            auto_failover: true,
            failover_threshold: 3,
        }
    }
}

impl MultiCloudFederation {
    /// Create a new multi-cloud federation manager
    pub fn new(config: MultiCloudConfig) -> Self {
        Self {
            config,
            regions: Arc::new(RwLock::new(HashMap::new())),
            topics: Arc::new(RwLock::new(HashMap::new())),
            lag: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(MultiCloudStatsInner {
                records_replicated: AtomicU64::new(0),
                bytes_replicated: AtomicU64::new(0),
                conflicts_detected: AtomicU64::new(0),
                conflicts_resolved: AtomicU64::new(0),
                replication_failures: AtomicU64::new(0),
            }),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Register a region
    pub async fn register_region(&self, region: FederationRegion) -> Result<()> {
        let mut regions = self.regions.write().await;
        regions.insert(region.id.clone(), region);
        Ok(())
    }

    /// Unregister a region
    pub async fn unregister_region(&self, region_id: &str) -> Result<()> {
        let mut regions = self.regions.write().await;
        if regions.remove(region_id).is_none() {
            return Err(StreamlineError::Config(format!(
                "Region '{}' not found",
                region_id
            )));
        }
        Ok(())
    }

    /// Get a region by ID
    pub async fn get_region(&self, region_id: &str) -> Option<FederationRegion> {
        let regions = self.regions.read().await;
        regions.get(region_id).cloned()
    }

    /// List all regions
    pub async fn list_regions(&self) -> Vec<FederationRegion> {
        let regions = self.regions.read().await;
        regions.values().cloned().collect()
    }

    /// Register a federated topic
    pub async fn federate_topic(&self, config: FederatedTopicConfig) -> Result<()> {
        // Validate regions exist
        let regions = self.regions.read().await;
        for region_id in &config.regions {
            if !regions.contains_key(region_id) {
                return Err(StreamlineError::Config(format!(
                    "Region '{}' not registered",
                    region_id
                )));
            }
        }
        drop(regions);

        let mut topics = self.topics.write().await;
        topics.insert(config.topic.clone(), config);

        Ok(())
    }

    /// Remove topic from federation
    pub async fn unfederate_topic(&self, topic: &str) -> Result<()> {
        let mut topics = self.topics.write().await;
        if topics.remove(topic).is_none() {
            return Err(StreamlineError::Config(format!(
                "Topic '{}' not federated",
                topic
            )));
        }
        Ok(())
    }

    /// Get federated topic config
    pub async fn get_topic(&self, topic: &str) -> Option<FederatedTopicConfig> {
        let topics = self.topics.read().await;
        topics.get(topic).cloned()
    }

    /// List all federated topics
    pub async fn list_topics(&self) -> Vec<FederatedTopicConfig> {
        let topics = self.topics.read().await;
        topics.values().cloned().collect()
    }

    /// Update region health
    pub async fn update_region_health(&self, region_id: &str, health: RegionHealth) -> Result<()> {
        let mut regions = self.regions.write().await;
        if let Some(region) = regions.get_mut(region_id) {
            region.health = health;
            Ok(())
        } else {
            Err(StreamlineError::Config(format!(
                "Region '{}' not found",
                region_id
            )))
        }
    }

    /// Get replication lag for a topic
    pub async fn get_replication_lag(&self, topic: &str) -> Vec<ReplicationLag> {
        let lag = self.lag.read().await;
        lag.get(topic).cloned().unwrap_or_default()
    }

    /// Update replication lag
    pub async fn update_replication_lag(
        &self,
        topic: &str,
        source: &str,
        target: &str,
        lag_ms: u64,
        pending_records: u64,
    ) -> Result<()> {
        let mut lags = self.lag.write().await;
        let topic_lags = lags.entry(topic.to_string()).or_default();

        // Find and update existing or add new
        let mut found = false;
        for lag in topic_lags.iter_mut() {
            if lag.source_region == source && lag.target_region == target {
                lag.lag_ms = lag_ms;
                lag.pending_records = pending_records;
                lag.updated_at = chrono::Utc::now().timestamp_millis();
                found = true;
                break;
            }
        }

        if !found {
            topic_lags.push(ReplicationLag {
                source_region: source.to_string(),
                target_region: target.to_string(),
                lag_ms,
                pending_records,
                updated_at: chrono::Utc::now().timestamp_millis(),
            });
        }

        Ok(())
    }

    /// Select best region for routing based on latency
    pub async fn select_region(&self, topic: &str) -> Option<String> {
        let topics = self.topics.read().await;
        let config = topics.get(topic)?;

        if !self.config.latency_aware_routing {
            // Return primary region
            let regions = self.regions.read().await;
            return config.regions.iter().find_map(|id| {
                regions
                    .get(id)
                    .and_then(|r| if r.primary { Some(id.clone()) } else { None })
            });
        }

        // Find region with lowest lag and good health
        let regions = self.regions.read().await;
        let lags = self.lag.read().await;

        let local = &self.config.local_region;
        let topic_lags = lags.get(topic);

        config
            .regions
            .iter()
            .filter_map(|id| {
                let region = regions.get(id)?;
                if region.health == RegionHealth::Unhealthy {
                    return None;
                }

                let lag = topic_lags
                    .and_then(|lags| {
                        lags.iter()
                            .find(|l| &l.source_region == local && &l.target_region == id)
                            .map(|l| l.lag_ms)
                    })
                    .unwrap_or(0);

                Some((id.clone(), region.priority, lag))
            })
            .min_by_key(|(_, priority, lag)| (*priority, *lag))
            .map(|(id, _, _)| id)
    }

    /// Start federation (replication loops, heartbeats)
    pub async fn start(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        self.running
            .store(true, std::sync::atomic::Ordering::SeqCst);

        // Start heartbeat task
        let running = self.running.clone();
        let interval = std::time::Duration::from_millis(self.config.heartbeat_interval_ms);

        tokio::spawn(async move {
            while running.load(std::sync::atomic::Ordering::SeqCst) {
                // Send heartbeats to all regions
                // In production, this would actually ping remote regions
                tokio::time::sleep(interval).await;
            }
        });

        Ok(())
    }

    /// Stop federation
    pub fn stop(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// Get statistics
    pub fn stats(&self) -> MultiCloudStats {
        MultiCloudStats {
            records_replicated: self.stats.records_replicated.load(Ordering::Relaxed),
            bytes_replicated: self.stats.bytes_replicated.load(Ordering::Relaxed),
            conflicts_detected: self.stats.conflicts_detected.load(Ordering::Relaxed),
            conflicts_resolved: self.stats.conflicts_resolved.load(Ordering::Relaxed),
            replication_failures: self.stats.replication_failures.load(Ordering::Relaxed),
            avg_replication_latency_ms: 0.0, // Would be calculated from samples
        }
    }

    /// Get configuration
    pub fn config(&self) -> &MultiCloudConfig {
        &self.config
    }
}

impl Default for MultiCloudFederation {
    fn default() -> Self {
        Self::new(MultiCloudConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_region() {
        let federation = MultiCloudFederation::default();

        let region = FederationRegion {
            id: "us-east-1".to_string(),
            name: "US East".to_string(),
            provider: CloudProvider::AWS,
            endpoint: "https://us-east-1.streamline.example.com".to_string(),
            active: true,
            primary: true,
            priority: 1,
            metadata: HashMap::new(),
            health: RegionHealth::Healthy,
        };

        federation.register_region(region).await.unwrap();

        let regions = federation.list_regions().await;
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].id, "us-east-1");
    }

    #[tokio::test]
    async fn test_federate_topic() {
        let federation = MultiCloudFederation::default();

        // Register regions first
        for (id, name) in [("us-east", "US East"), ("us-west", "US West")] {
            let region = FederationRegion {
                id: id.to_string(),
                name: name.to_string(),
                provider: CloudProvider::AWS,
                endpoint: format!("https://{}.example.com", id),
                active: true,
                primary: id == "us-east",
                priority: 1,
                metadata: HashMap::new(),
                health: RegionHealth::Healthy,
            };
            federation.register_region(region).await.unwrap();
        }

        // Federate topic
        let config = FederatedTopicConfig {
            topic: "events".to_string(),
            regions: vec!["us-east".to_string(), "us-west".to_string()],
            replication_mode: ReplicationMode::ActiveActive,
            ..Default::default()
        };

        federation.federate_topic(config).await.unwrap();

        let topics = federation.list_topics().await;
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].topic, "events");
    }

    #[tokio::test]
    async fn test_region_selection() {
        let federation = MultiCloudFederation::new(MultiCloudConfig {
            local_region: "local".to_string(),
            latency_aware_routing: false,
            ..Default::default()
        });

        // Register regions
        let primary = FederationRegion {
            id: "primary".to_string(),
            name: "Primary".to_string(),
            provider: CloudProvider::AWS,
            endpoint: "https://primary.example.com".to_string(),
            active: true,
            primary: true,
            priority: 1,
            metadata: HashMap::new(),
            health: RegionHealth::Healthy,
        };

        let secondary = FederationRegion {
            id: "secondary".to_string(),
            name: "Secondary".to_string(),
            provider: CloudProvider::GCP,
            endpoint: "https://secondary.example.com".to_string(),
            active: true,
            primary: false,
            priority: 2,
            metadata: HashMap::new(),
            health: RegionHealth::Healthy,
        };

        federation.register_region(primary).await.unwrap();
        federation.register_region(secondary).await.unwrap();

        // Federate topic
        let config = FederatedTopicConfig {
            topic: "events".to_string(),
            regions: vec!["primary".to_string(), "secondary".to_string()],
            ..Default::default()
        };
        federation.federate_topic(config).await.unwrap();

        // Should select primary
        let selected = federation.select_region("events").await;
        assert_eq!(selected, Some("primary".to_string()));
    }
}
