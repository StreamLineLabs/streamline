//! Bandwidth Throttling & Region-Aware Consumer Routing
//!
//! Provides WAN-friendly replication controls for multi-region deployments:
//!
//! - **Bandwidth Throttling**: Token-bucket rate limiter for inter-region
//!   replication traffic, preventing network saturation.
//! - **Region-Aware Routing**: Routes consumer fetch requests to the nearest
//!   replica, reducing cross-region traffic and latency.
//! - **Priority-Based Replication**: Assigns priorities to topics for bandwidth
//!   allocation during congestion.

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Bandwidth Throttler (Token Bucket)
// ---------------------------------------------------------------------------

/// Configuration for bandwidth throttling.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThrottleConfig {
    /// Maximum bytes per second for replication traffic.
    pub max_bytes_per_sec: u64,
    /// Burst capacity in bytes (allows short bursts above the rate).
    pub burst_bytes: u64,
    /// Per-topic bandwidth limits (overrides global).
    pub topic_limits: HashMap<String, u64>,
    /// Priority tiers: topics in higher tiers get bandwidth first.
    pub priority_tiers: Vec<PriorityTier>,
}

impl Default for ThrottleConfig {
    fn default() -> Self {
        Self {
            max_bytes_per_sec: 100 * 1024 * 1024, // 100 MB/s
            burst_bytes: 10 * 1024 * 1024,        // 10 MB burst
            topic_limits: HashMap::new(),
            priority_tiers: vec![
                PriorityTier {
                    name: "critical".to_string(),
                    bandwidth_pct: 60,
                    topics: Vec::new(),
                },
                PriorityTier {
                    name: "normal".to_string(),
                    bandwidth_pct: 30,
                    topics: Vec::new(),
                },
                PriorityTier {
                    name: "background".to_string(),
                    bandwidth_pct: 10,
                    topics: Vec::new(),
                },
            ],
        }
    }
}

/// Priority tier for bandwidth allocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityTier {
    /// Tier name.
    pub name: String,
    /// Percentage of total bandwidth allocated to this tier.
    pub bandwidth_pct: u32,
    /// Topics assigned to this tier.
    pub topics: Vec<String>,
}

/// Token-bucket bandwidth throttler.
pub struct BandwidthThrottler {
    config: ThrottleConfig,
    /// Available tokens (bytes).
    tokens: AtomicI64,
    /// Last refill timestamp in nanoseconds.
    last_refill_ns: AtomicU64,
    /// Total bytes throttled (delayed).
    total_throttled_bytes: AtomicU64,
    /// Total bytes passed through.
    total_passed_bytes: AtomicU64,
    /// Per-region statistics.
    region_stats: RwLock<HashMap<String, RegionThrottleStats>>,
}

/// Per-region throttle statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegionThrottleStats {
    pub bytes_sent: u64,
    pub bytes_throttled: u64,
    pub throttle_count: u64,
    pub avg_delay_ms: f64,
}

/// Snapshot of throttler state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThrottleSnapshot {
    pub max_bytes_per_sec: u64,
    pub current_tokens: i64,
    pub total_passed_bytes: u64,
    pub total_throttled_bytes: u64,
    pub utilization_pct: f64,
    pub region_stats: HashMap<String, RegionThrottleStats>,
}

impl BandwidthThrottler {
    /// Create a new throttler.
    pub fn new(config: ThrottleConfig) -> Self {
        Self {
            tokens: AtomicI64::new(config.burst_bytes as i64),
            last_refill_ns: AtomicU64::new(Self::now_ns()),
            total_throttled_bytes: AtomicU64::new(0),
            total_passed_bytes: AtomicU64::new(0),
            region_stats: RwLock::new(HashMap::new()),
            config,
        }
    }

    /// Request permission to send `bytes` of replication data.
    ///
    /// Returns the number of milliseconds to wait before sending. Returns 0
    /// if the data can be sent immediately.
    pub fn acquire(&self, bytes: u64, region: Option<&str>) -> u64 {
        self.refill_tokens();

        let current = self.tokens.fetch_sub(bytes as i64, Ordering::Relaxed);
        self.total_passed_bytes.fetch_add(bytes, Ordering::Relaxed);

        if current >= bytes as i64 {
            // Enough tokens — no delay
            return 0;
        }

        // Calculate delay
        let deficit = (bytes as i64 - current).max(0) as u64;
        let delay_ms = (deficit * 1000) / self.config.max_bytes_per_sec.max(1);

        self.total_throttled_bytes
            .fetch_add(bytes, Ordering::Relaxed);

        // Record per-region stats
        if let Some(r) = region {
            if let Ok(mut stats) = self.region_stats.try_write() {
                let entry = stats.entry(r.to_string()).or_default();
                entry.bytes_throttled += bytes;
                entry.throttle_count += 1;
                entry.avg_delay_ms = entry.avg_delay_ms
                    + (delay_ms as f64 - entry.avg_delay_ms) / entry.throttle_count as f64;
            }
        }

        delay_ms
    }

    /// Check bandwidth limit for a specific topic.
    pub fn topic_limit(&self, topic: &str) -> Option<u64> {
        self.config.topic_limits.get(topic).copied()
    }

    /// Get the priority tier for a topic.
    pub fn topic_tier(&self, topic: &str) -> Option<&PriorityTier> {
        self.config
            .priority_tiers
            .iter()
            .find(|t| t.topics.contains(&topic.to_string()))
    }

    /// Get a snapshot of current throttle state.
    pub async fn snapshot(&self) -> ThrottleSnapshot {
        self.refill_tokens();
        let passed = self.total_passed_bytes.load(Ordering::Relaxed);
        let throttled = self.total_throttled_bytes.load(Ordering::Relaxed);
        let total = passed + throttled;

        ThrottleSnapshot {
            max_bytes_per_sec: self.config.max_bytes_per_sec,
            current_tokens: self.tokens.load(Ordering::Relaxed),
            total_passed_bytes: passed,
            total_throttled_bytes: throttled,
            utilization_pct: if total > 0 {
                passed as f64 / total as f64 * 100.0
            } else {
                0.0
            },
            region_stats: self.region_stats.read().await.clone(),
        }
    }

    /// Update the maximum rate at runtime.
    pub fn set_max_rate(&mut self, bytes_per_sec: u64) {
        self.config.max_bytes_per_sec = bytes_per_sec;
    }

    fn refill_tokens(&self) {
        let now = Self::now_ns();
        let last = self.last_refill_ns.load(Ordering::Relaxed);
        let elapsed_ns = now.saturating_sub(last);

        if elapsed_ns == 0 {
            return;
        }

        let refill = (self.config.max_bytes_per_sec as u128 * elapsed_ns as u128
            / 1_000_000_000u128) as i64;

        if refill > 0 && self.last_refill_ns.compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            let new_tokens = self.tokens.fetch_add(refill, Ordering::Relaxed) + refill;
            // Cap at burst size
            let burst = self.config.burst_bytes as i64;
            if new_tokens > burst {
                self.tokens.store(burst, Ordering::Relaxed);
            }
        }
    }

    fn now_ns() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}

// ---------------------------------------------------------------------------
// Region-Aware Consumer Routing
// ---------------------------------------------------------------------------

/// Configuration for region-aware consumer routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionRoutingConfig {
    /// Map of region ID → list of broker addresses in that region.
    pub regions: HashMap<String, Vec<String>>,
    /// Local region ID.
    pub local_region: String,
    /// Prefer local replicas for reads.
    pub prefer_local: bool,
    /// Maximum cross-region latency before routing to local (ms).
    pub max_cross_region_latency_ms: u64,
    /// Follower fetch enabled (read from ISR followers in local region).
    pub follower_fetch_enabled: bool,
}

impl Default for RegionRoutingConfig {
    fn default() -> Self {
        Self {
            regions: HashMap::new(),
            local_region: "default".to_string(),
            prefer_local: true,
            max_cross_region_latency_ms: 100,
            follower_fetch_enabled: true,
        }
    }
}

/// A routing decision for a consumer fetch request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchRoutingDecision {
    /// Target region for the fetch.
    pub target_region: String,
    /// Target broker address.
    pub target_broker: String,
    /// Whether this is a local or cross-region fetch.
    pub is_local: bool,
    /// Estimated latency in milliseconds.
    pub estimated_latency_ms: u64,
    /// Reason for the routing decision.
    pub reason: String,
}

/// Region-aware consumer router.
pub struct RegionRouter {
    config: RegionRoutingConfig,
    /// Measured latencies to each region (ms).
    region_latencies: RwLock<HashMap<String, u64>>,
    /// Routing statistics.
    stats: Arc<RegionRoutingStats>,
}

/// Routing statistics.
#[derive(Debug, Default)]
pub struct RegionRoutingStats {
    pub local_fetches: AtomicU64,
    pub cross_region_fetches: AtomicU64,
    pub follower_fetches: AtomicU64,
}

impl RegionRouter {
    pub fn new(config: RegionRoutingConfig) -> Self {
        Self {
            config,
            region_latencies: RwLock::new(HashMap::new()),
            stats: Arc::new(RegionRoutingStats::default()),
        }
    }

    /// Route a fetch request to the best region/broker.
    pub async fn route_fetch(
        &self,
        topic: &str,
        partition: i32,
        consumer_region: &str,
    ) -> FetchRoutingDecision {
        // Prefer local region
        if self.config.prefer_local {
            if let Some(brokers) = self.config.regions.get(consumer_region) {
                if !brokers.is_empty() {
                    self.stats.local_fetches.fetch_add(1, Ordering::Relaxed);
                    let broker = &brokers[partition as usize % brokers.len()];
                    return FetchRoutingDecision {
                        target_region: consumer_region.to_string(),
                        target_broker: broker.clone(),
                        is_local: true,
                        estimated_latency_ms: 1,
                        reason: "Local region preferred".to_string(),
                    };
                }
            }
        }

        // Find lowest-latency region with available brokers
        let latencies = self.region_latencies.read().await;
        let mut best_region = self.config.local_region.clone();
        let mut best_latency = u64::MAX;

        for (region, brokers) in &self.config.regions {
            if brokers.is_empty() {
                continue;
            }
            let latency = latencies.get(region).copied().unwrap_or(50);
            if latency < best_latency {
                best_latency = latency;
                best_region = region.clone();
            }
        }

        let is_local = best_region == consumer_region;
        if is_local {
            self.stats.local_fetches.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats
                .cross_region_fetches
                .fetch_add(1, Ordering::Relaxed);
        }

        let brokers = self.config.regions.get(&best_region);
        let broker = brokers
            .and_then(|b| b.get(partition as usize % b.len()))
            .cloned()
            .unwrap_or_else(|| "localhost:9092".to_string());

        FetchRoutingDecision {
            target_region: best_region,
            target_broker: broker,
            is_local,
            estimated_latency_ms: best_latency,
            reason: if is_local {
                "Local region".to_string()
            } else {
                format!("Lowest latency region ({}ms)", best_latency)
            },
        }
    }

    /// Update measured latency for a region.
    pub async fn update_latency(&self, region: &str, latency_ms: u64) {
        self.region_latencies
            .write()
            .await
            .insert(region.to_string(), latency_ms);
    }

    /// Get routing statistics.
    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.stats.local_fetches.load(Ordering::Relaxed),
            self.stats.cross_region_fetches.load(Ordering::Relaxed),
            self.stats.follower_fetches.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_throttler_no_delay_under_limit() {
        let config = ThrottleConfig {
            max_bytes_per_sec: 1_000_000,
            burst_bytes: 100_000,
            ..Default::default()
        };
        let throttler = BandwidthThrottler::new(config);
        let delay = throttler.acquire(1000, None);
        assert_eq!(delay, 0);
    }

    #[test]
    fn test_throttler_delay_when_burst_exceeded() {
        let config = ThrottleConfig {
            max_bytes_per_sec: 1000,
            burst_bytes: 100,
            ..Default::default()
        };
        let throttler = BandwidthThrottler::new(config);
        // First call drains burst
        let _ = throttler.acquire(100, None);
        // Second call exceeds burst — should delay
        let delay = throttler.acquire(1000, Some("us-east"));
        assert!(delay > 0);
    }

    #[tokio::test]
    async fn test_throttler_snapshot() {
        let throttler = BandwidthThrottler::new(ThrottleConfig::default());
        throttler.acquire(1024, Some("eu-west"));
        let snap = throttler.snapshot().await;
        assert!(snap.total_passed_bytes > 0);
    }

    #[tokio::test]
    async fn test_region_router_local_preference() {
        let config = RegionRoutingConfig {
            regions: HashMap::from([
                ("us-east".to_string(), vec!["broker1:9092".to_string()]),
                ("eu-west".to_string(), vec!["broker2:9092".to_string()]),
            ]),
            local_region: "us-east".to_string(),
            prefer_local: true,
            ..Default::default()
        };

        let router = RegionRouter::new(config);
        let decision = router.route_fetch("events", 0, "us-east").await;
        assert!(decision.is_local);
        assert_eq!(decision.target_region, "us-east");
    }

    #[tokio::test]
    async fn test_region_router_cross_region() {
        let config = RegionRoutingConfig {
            regions: HashMap::from([
                ("us-east".to_string(), vec!["broker1:9092".to_string()]),
            ]),
            local_region: "us-east".to_string(),
            prefer_local: true,
            ..Default::default()
        };

        let router = RegionRouter::new(config);
        // Consumer in eu-west, but no eu-west brokers
        let decision = router.route_fetch("events", 0, "eu-west").await;
        assert!(!decision.is_local);
    }

    #[tokio::test]
    async fn test_region_router_stats() {
        let config = RegionRoutingConfig {
            regions: HashMap::from([
                ("us-east".to_string(), vec!["broker1:9092".to_string()]),
            ]),
            local_region: "us-east".to_string(),
            prefer_local: true,
            ..Default::default()
        };

        let router = RegionRouter::new(config);
        router.route_fetch("t1", 0, "us-east").await;
        router.route_fetch("t2", 0, "us-east").await;

        let (local, cross, _) = router.stats();
        assert_eq!(local, 2);
        assert_eq!(cross, 0);
    }
}
