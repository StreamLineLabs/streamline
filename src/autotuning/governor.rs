//! Resource Governor for Streamline
//!
//! Manages system resources adaptively:
//! - Memory pressure detection with automatic response
//! - Auto-partitioning based on throughput
//! - Disk space monitoring with auto-tiering triggers
//! - Dynamic buffer pool sizing

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Resource governor configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceGovernorConfig {
    /// Memory high watermark (percentage, 0-100)
    pub memory_high_watermark_pct: u8,
    /// Memory critical watermark (percentage, 0-100)
    pub memory_critical_watermark_pct: u8,
    /// Disk high watermark (percentage, 0-100)
    pub disk_high_watermark_pct: u8,
    /// Disk critical watermark (percentage, 0-100)
    pub disk_critical_watermark_pct: u8,
    /// Enable auto-partitioning
    pub auto_partition_enabled: bool,
    /// Throughput threshold (msg/s) to trigger partition split
    pub partition_split_threshold: u64,
    /// Minimum partition split cooldown
    pub partition_split_cooldown: Duration,
    /// Enable adaptive buffer sizing
    pub adaptive_buffers: bool,
    /// Monitoring interval
    pub check_interval: Duration,
    /// Enable auto-tiering to cloud storage on disk pressure
    pub auto_tier_on_disk_pressure: bool,
}

impl Default for ResourceGovernorConfig {
    fn default() -> Self {
        Self {
            memory_high_watermark_pct: 75,
            memory_critical_watermark_pct: 90,
            disk_high_watermark_pct: 80,
            disk_critical_watermark_pct: 95,
            auto_partition_enabled: false,
            partition_split_threshold: 100_000,
            partition_split_cooldown: Duration::from_secs(300),
            adaptive_buffers: true,
            check_interval: Duration::from_secs(10),
            auto_tier_on_disk_pressure: false,
        }
    }
}

/// Memory pressure levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PressureLevel {
    /// Normal operation
    Normal,
    /// High usage - start shedding caches
    High,
    /// Critical - reject new writes, force compaction
    Critical,
}

impl std::fmt::Display for PressureLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Normal => write!(f, "normal"),
            Self::High => write!(f, "high"),
            Self::Critical => write!(f, "critical"),
        }
    }
}

/// Resource snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    /// Memory usage percentage
    pub memory_usage_pct: f64,
    /// Memory pressure level
    pub memory_pressure: PressureLevel,
    /// Disk usage percentage
    pub disk_usage_pct: f64,
    /// Disk pressure level
    pub disk_pressure: PressureLevel,
    /// Current buffer pool size in bytes
    pub buffer_pool_bytes: u64,
    /// Recommended buffer pool size
    pub recommended_buffer_bytes: u64,
    /// Active compaction tasks
    pub active_compactions: u32,
    /// Pending tier-out bytes
    pub pending_tier_out_bytes: u64,
    /// Timestamp
    pub timestamp: std::time::SystemTime,
}

/// Auto-partition recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionRecommendation {
    pub topic: String,
    pub current_partitions: u32,
    pub recommended_partitions: u32,
    pub reason: String,
    pub throughput_per_partition: f64,
}

/// Actions taken by the resource governor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GovernorAction {
    /// Reduced buffer pool size
    ReducedBuffers { from_bytes: u64, to_bytes: u64 },
    /// Triggered compaction
    TriggeredCompaction { topic: String },
    /// Triggered tiering to cloud storage
    TriggeredTiering { topic: String, bytes: u64 },
    /// Rejected write due to resource pressure
    RejectedWrite { reason: String },
    /// Recommended partition split
    RecommendedPartitionSplit(PartitionRecommendation),
    /// Increased buffer pool size
    IncreasedBuffers { from_bytes: u64, to_bytes: u64 },
}

impl std::fmt::Display for GovernorAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReducedBuffers {
                from_bytes,
                to_bytes,
            } => {
                write!(f, "reduced buffers {}→{} bytes", from_bytes, to_bytes)
            }
            Self::TriggeredCompaction { topic } => {
                write!(f, "triggered compaction on '{}'", topic)
            }
            Self::TriggeredTiering { topic, bytes } => {
                write!(f, "tiering {} bytes from '{}'", bytes, topic)
            }
            Self::RejectedWrite { reason } => {
                write!(f, "rejected write: {}", reason)
            }
            Self::RecommendedPartitionSplit(rec) => {
                write!(
                    f,
                    "recommend splitting '{}' {}→{} partitions",
                    rec.topic, rec.current_partitions, rec.recommended_partitions
                )
            }
            Self::IncreasedBuffers {
                from_bytes,
                to_bytes,
            } => {
                write!(f, "increased buffers {}→{} bytes", from_bytes, to_bytes)
            }
        }
    }
}

/// Resource Governor
pub struct ResourceGovernor {
    config: ResourceGovernorConfig,
    active: AtomicBool,
    buffer_pool_bytes: AtomicU64,
    actions_taken: Arc<parking_lot::Mutex<Vec<GovernorAction>>>,
    last_partition_check: Arc<parking_lot::Mutex<Option<Instant>>>,
    topic_throughputs: Arc<parking_lot::Mutex<std::collections::HashMap<String, f64>>>,
}

impl ResourceGovernor {
    pub fn new(config: ResourceGovernorConfig) -> Self {
        let default_buffer = 64 * 1024 * 1024; // 64MB default
        Self {
            config,
            active: AtomicBool::new(false),
            buffer_pool_bytes: AtomicU64::new(default_buffer),
            actions_taken: Arc::new(parking_lot::Mutex::new(Vec::new())),
            last_partition_check: Arc::new(parking_lot::Mutex::new(None)),
            topic_throughputs: Arc::new(parking_lot::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Start the resource governor
    pub fn start(&self) {
        self.active.store(true, Ordering::SeqCst);
        info!("Resource governor started");
    }

    /// Stop the resource governor
    pub fn stop(&self) {
        self.active.store(false, Ordering::SeqCst);
        info!("Resource governor stopped");
    }

    /// Check if governor is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Evaluate current resource state and take action
    pub fn evaluate(&self, memory_pct: f64, disk_pct: f64) -> Vec<GovernorAction> {
        let mut actions = Vec::new();

        // Memory pressure handling
        let memory_pressure = if memory_pct >= self.config.memory_critical_watermark_pct as f64 {
            PressureLevel::Critical
        } else if memory_pct >= self.config.memory_high_watermark_pct as f64 {
            PressureLevel::High
        } else {
            PressureLevel::Normal
        };

        match memory_pressure {
            PressureLevel::Critical => {
                let current = self.buffer_pool_bytes.load(Ordering::Relaxed);
                let new_size = current / 2;
                self.buffer_pool_bytes.store(new_size, Ordering::Relaxed);
                let action = GovernorAction::ReducedBuffers {
                    from_bytes: current,
                    to_bytes: new_size,
                };
                warn!("Memory critical: {}", action);
                actions.push(action);
            }
            PressureLevel::High => {
                let current = self.buffer_pool_bytes.load(Ordering::Relaxed);
                let new_size = current * 3 / 4;
                if new_size < current {
                    self.buffer_pool_bytes.store(new_size, Ordering::Relaxed);
                    actions.push(GovernorAction::ReducedBuffers {
                        from_bytes: current,
                        to_bytes: new_size,
                    });
                }
            }
            PressureLevel::Normal => {
                if self.config.adaptive_buffers {
                    let current = self.buffer_pool_bytes.load(Ordering::Relaxed);
                    let max_buffer = 256 * 1024 * 1024; // 256MB max
                    if current < max_buffer && memory_pct < 50.0 {
                        let new_size = std::cmp::min(current * 5 / 4, max_buffer);
                        if new_size > current {
                            self.buffer_pool_bytes.store(new_size, Ordering::Relaxed);
                            actions.push(GovernorAction::IncreasedBuffers {
                                from_bytes: current,
                                to_bytes: new_size,
                            });
                        }
                    }
                }
            }
        }

        // Disk pressure handling
        if disk_pct >= self.config.disk_critical_watermark_pct as f64 {
            actions.push(GovernorAction::RejectedWrite {
                reason: format!("disk usage critical at {:.1}%", disk_pct),
            });
        } else if disk_pct >= self.config.disk_high_watermark_pct as f64
            && self.config.auto_tier_on_disk_pressure
        {
            actions.push(GovernorAction::TriggeredTiering {
                topic: "__oldest__".to_string(),
                bytes: 0,
            });
        }

        // Record actions
        if !actions.is_empty() {
            self.actions_taken.lock().extend(actions.clone());
        }

        actions
    }

    /// Record throughput for a topic (for auto-partition decisions)
    pub fn record_throughput(&self, topic: &str, messages_per_sec: f64) {
        self.topic_throughputs
            .lock()
            .insert(topic.to_string(), messages_per_sec);
    }

    /// Get partition recommendations based on throughput
    pub fn partition_recommendations(&self) -> Vec<PartitionRecommendation> {
        if !self.config.auto_partition_enabled {
            return Vec::new();
        }

        // Check cooldown
        let mut last_check = self.last_partition_check.lock();
        if let Some(last) = *last_check {
            if last.elapsed() < self.config.partition_split_cooldown {
                return Vec::new();
            }
        }
        *last_check = Some(Instant::now());

        let throughputs = self.topic_throughputs.lock();
        let threshold = self.config.partition_split_threshold as f64;

        throughputs
            .iter()
            .filter_map(|(topic, &throughput)| {
                if throughput > threshold {
                    // Recommend doubling partitions
                    let current = 3u32; // Would query TopicManager in production
                    let recommended = current * 2;
                    Some(PartitionRecommendation {
                        topic: topic.clone(),
                        current_partitions: current,
                        recommended_partitions: recommended,
                        reason: format!(
                            "throughput {:.0} msg/s exceeds threshold {:.0}",
                            throughput, threshold
                        ),
                        throughput_per_partition: throughput / current as f64,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    /// Get current resource snapshot
    pub fn snapshot(&self, memory_pct: f64, disk_pct: f64) -> ResourceSnapshot {
        let memory_pressure = if memory_pct >= self.config.memory_critical_watermark_pct as f64 {
            PressureLevel::Critical
        } else if memory_pct >= self.config.memory_high_watermark_pct as f64 {
            PressureLevel::High
        } else {
            PressureLevel::Normal
        };

        let disk_pressure = if disk_pct >= self.config.disk_critical_watermark_pct as f64 {
            PressureLevel::Critical
        } else if disk_pct >= self.config.disk_high_watermark_pct as f64 {
            PressureLevel::High
        } else {
            PressureLevel::Normal
        };

        let buffer = self.buffer_pool_bytes.load(Ordering::Relaxed);

        ResourceSnapshot {
            memory_usage_pct: memory_pct,
            memory_pressure,
            disk_usage_pct: disk_pct,
            disk_pressure,
            buffer_pool_bytes: buffer,
            recommended_buffer_bytes: buffer,
            active_compactions: 0,
            pending_tier_out_bytes: 0,
            timestamp: std::time::SystemTime::now(),
        }
    }

    /// Get current buffer pool size
    pub fn buffer_pool_bytes(&self) -> u64 {
        self.buffer_pool_bytes.load(Ordering::Relaxed)
    }

    /// Get all actions taken
    pub fn actions_history(&self) -> Vec<GovernorAction> {
        self.actions_taken.lock().clone()
    }

    /// Check if writes should be accepted
    pub fn should_accept_write(&self, memory_pct: f64, disk_pct: f64) -> Result<()> {
        if disk_pct >= self.config.disk_critical_watermark_pct as f64 {
            return Err(StreamlineError::ResourceExhausted(format!(
                "disk usage at {:.1}% exceeds critical threshold {}%",
                disk_pct, self.config.disk_critical_watermark_pct
            )));
        }
        if memory_pct >= self.config.memory_critical_watermark_pct as f64 {
            return Err(StreamlineError::ResourceExhausted(format!(
                "memory usage at {:.1}% exceeds critical threshold {}%",
                memory_pct, self.config.memory_critical_watermark_pct
            )));
        }
        Ok(())
    }
}

impl Default for ResourceGovernor {
    fn default() -> Self {
        Self::new(ResourceGovernorConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normal_operation() {
        let gov = ResourceGovernor::default();
        let actions = gov.evaluate(50.0, 50.0);
        // Should potentially increase buffers if adaptive
        assert!(
            actions.is_empty()
                || actions
                    .iter()
                    .any(|a| matches!(a, GovernorAction::IncreasedBuffers { .. }))
        );
    }

    #[test]
    fn test_memory_high_watermark() {
        let gov = ResourceGovernor::new(ResourceGovernorConfig {
            memory_high_watermark_pct: 75,
            ..Default::default()
        });
        let initial_buffer = gov.buffer_pool_bytes();
        let actions = gov.evaluate(80.0, 50.0);
        assert!(!actions.is_empty());
        assert!(gov.buffer_pool_bytes() < initial_buffer);
    }

    #[test]
    fn test_memory_critical() {
        let gov = ResourceGovernor::new(ResourceGovernorConfig {
            memory_critical_watermark_pct: 90,
            ..Default::default()
        });
        let initial = gov.buffer_pool_bytes();
        let actions = gov.evaluate(95.0, 50.0);
        assert!(actions
            .iter()
            .any(|a| matches!(a, GovernorAction::ReducedBuffers { .. })));
        assert_eq!(gov.buffer_pool_bytes(), initial / 2);
    }

    #[test]
    fn test_disk_critical_rejects_writes() {
        let gov = ResourceGovernor::default();
        let result = gov.should_accept_write(50.0, 96.0);
        assert!(result.is_err());
    }

    #[test]
    fn test_disk_normal_accepts_writes() {
        let gov = ResourceGovernor::default();
        assert!(gov.should_accept_write(50.0, 50.0).is_ok());
    }

    #[test]
    fn test_partition_recommendations() {
        let gov = ResourceGovernor::new(ResourceGovernorConfig {
            auto_partition_enabled: true,
            partition_split_threshold: 1000,
            partition_split_cooldown: Duration::from_millis(0),
            ..Default::default()
        });
        gov.record_throughput("events", 5000.0);
        gov.record_throughput("logs", 500.0);

        let recs = gov.partition_recommendations();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].topic, "events");
    }

    #[test]
    fn test_snapshot() {
        let gov = ResourceGovernor::default();
        let snap = gov.snapshot(60.0, 40.0);
        assert_eq!(snap.memory_pressure, PressureLevel::Normal);
        assert_eq!(snap.disk_pressure, PressureLevel::Normal);
    }

    #[test]
    fn test_pressure_level_display() {
        assert_eq!(PressureLevel::Normal.to_string(), "normal");
        assert_eq!(PressureLevel::High.to_string(), "high");
        assert_eq!(PressureLevel::Critical.to_string(), "critical");
    }

    #[test]
    fn test_start_stop() {
        let gov = ResourceGovernor::default();
        assert!(!gov.is_active());
        gov.start();
        assert!(gov.is_active());
        gov.stop();
        assert!(!gov.is_active());
    }

    #[test]
    fn test_actions_history() {
        let gov = ResourceGovernor::default();
        gov.evaluate(95.0, 50.0); // Should trigger critical actions
        assert!(!gov.actions_history().is_empty());
    }

    #[test]
    fn test_governor_action_display() {
        let action = GovernorAction::TriggeredCompaction {
            topic: "test".to_string(),
        };
        assert!(action.to_string().contains("test"));
    }
}
