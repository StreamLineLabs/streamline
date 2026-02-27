//! Automated performance advisor for Streamline.
//!
//! Analyzes [`PerformanceSnapshot`]s and produces [`Recommendation`]s with
//! suggested actions to improve cluster health, throughput, and resource usage.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum AdvisorError {
    #[error("recommendation not found: {0}")]
    NotFound(String),
    #[error("recommendation already applied: {0}")]
    AlreadyApplied(String),
    #[error("insufficient data points: need {need}, have {have}")]
    InsufficientData { need: usize, have: usize },
}

pub type Result<T> = std::result::Result<T, AdvisorError>;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvisorConfig {
    pub enable_auto_analysis: bool,
    pub analysis_interval_secs: u64,
    pub min_data_points: usize,
    pub enable_auto_apply: bool,
}

impl Default for AdvisorConfig {
    fn default() -> Self {
        Self {
            enable_auto_analysis: true,
            analysis_interval_secs: 300,
            min_data_points: 10,
            enable_auto_apply: false,
        }
    }
}

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RecommendationCategory {
    PartitionBalance,
    ConsumerLag,
    Throughput,
    MemoryUsage,
    DiskUsage,
    Replication,
    Configuration,
    Scaling,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RecommendedAction {
    AddPartitions { topic: String, count: u32 },
    RebalanceConsumers { group: String },
    IncreaseRetention { topic: String, hours: u64 },
    ScaleUp { replicas: i32 },
    TuneConfig { key: String, value: String, reason: String },
    CompactTopics { topics: Vec<String> },
    NoAction { reason: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RecommendationStatus {
    Pending,
    Applied,
    Dismissed,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recommendation {
    pub id: String,
    pub category: RecommendationCategory,
    pub severity: Severity,
    pub title: String,
    pub description: String,
    pub action: RecommendedAction,
    pub impact: String,
    pub confidence: f64,
    pub created_at: String,
    pub status: RecommendationStatus,
    pub applied_at: Option<String>,
}

// ---------------------------------------------------------------------------
// Performance snapshot types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPerf {
    pub name: String,
    pub partitions: u32,
    pub msg_rate: f64,
    pub size_bytes: u64,
    pub hottest_partition_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerPerf {
    pub group: String,
    pub lag: u64,
    pub throughput: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemPerf {
    pub cpu_pct: f64,
    pub memory_pct: f64,
    pub disk_pct: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSnapshot {
    pub topics: Vec<TopicPerf>,
    pub consumers: Vec<ConsumerPerf>,
    pub system: SystemPerf,
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct AdvisorStats {
    pub analyses_run: AtomicU64,
    pub recommendations_generated: AtomicU64,
    pub recommendations_applied: AtomicU64,
    pub recommendations_dismissed: AtomicU64,
}

// ---------------------------------------------------------------------------
// PerformanceAdvisor
// ---------------------------------------------------------------------------

pub struct PerformanceAdvisor {
    config: AdvisorConfig,
    recommendations: Arc<RwLock<Vec<Recommendation>>>,
    stats: Arc<AdvisorStats>,
}

impl PerformanceAdvisor {
    pub fn new(config: AdvisorConfig) -> Self {
        info!(
            auto_analysis = config.enable_auto_analysis,
            interval = config.analysis_interval_secs,
            "performance advisor initialized"
        );
        Self {
            config,
            recommendations: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(AdvisorStats::default()),
        }
    }

    /// Analyze a performance snapshot and return new recommendations.
    pub fn analyze(&self, snapshot: &PerformanceSnapshot) -> Vec<Recommendation> {
        self.stats.analyses_run.fetch_add(1, Ordering::Relaxed);
        let mut recs = Vec::new();

        // Partition imbalance check
        for topic in &snapshot.topics {
            if topic.hottest_partition_pct > 60.0 {
                let severity = if topic.hottest_partition_pct > 85.0 {
                    Severity::Critical
                } else {
                    Severity::Warning
                };
                let confidence = (topic.hottest_partition_pct / 100.0).min(1.0);
                recs.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    category: RecommendationCategory::PartitionBalance,
                    severity,
                    title: format!("Partition imbalance on topic '{}'", topic.name),
                    description: format!(
                        "Hottest partition handles {:.1}% of traffic for topic '{}'. \
                         Consider adding partitions to distribute load.",
                        topic.hottest_partition_pct, topic.name
                    ),
                    action: RecommendedAction::AddPartitions {
                        topic: topic.name.clone(),
                        count: topic.partitions.max(1),
                    },
                    impact: "Improved throughput distribution across partitions".into(),
                    confidence,
                    created_at: chrono::Utc::now().to_rfc3339(),
                    status: RecommendationStatus::Pending,
                    applied_at: None,
                });
                debug!(topic = %topic.name, pct = topic.hottest_partition_pct, "partition imbalance detected");
            }
        }

        // Consumer lag check
        for consumer in &snapshot.consumers {
            if consumer.lag > 100_000 {
                let severity = if consumer.lag > 1_000_000 {
                    Severity::Critical
                } else {
                    Severity::Warning
                };
                let confidence = (consumer.lag as f64 / 1_000_000.0).min(1.0).max(0.5);
                recs.push(Recommendation {
                    id: Uuid::new_v4().to_string(),
                    category: RecommendationCategory::ConsumerLag,
                    severity,
                    title: format!("High consumer lag for group '{}'", consumer.group),
                    description: format!(
                        "Consumer group '{}' has a lag of {} messages. \
                         Consider scaling consumers or rebalancing.",
                        consumer.group, consumer.lag
                    ),
                    action: RecommendedAction::RebalanceConsumers {
                        group: consumer.group.clone(),
                    },
                    impact: "Reduced message processing latency".into(),
                    confidence,
                    created_at: chrono::Utc::now().to_rfc3339(),
                    status: RecommendationStatus::Pending,
                    applied_at: None,
                });
                debug!(group = %consumer.group, lag = consumer.lag, "consumer lag detected");
            }
        }

        // Memory usage check
        if snapshot.system.memory_pct > 80.0 {
            let severity = if snapshot.system.memory_pct > 90.0 {
                Severity::Critical
            } else {
                Severity::Warning
            };
            let topic_names: Vec<String> = snapshot.topics.iter().map(|t| t.name.clone()).collect();
            recs.push(Recommendation {
                id: Uuid::new_v4().to_string(),
                category: RecommendationCategory::MemoryUsage,
                severity,
                title: "High memory usage detected".into(),
                description: format!(
                    "Memory usage is at {:.1}%. Consider compacting topics to free memory.",
                    snapshot.system.memory_pct
                ),
                action: RecommendedAction::CompactTopics { topics: topic_names },
                impact: "Reduced memory footprint through log compaction".into(),
                confidence: (snapshot.system.memory_pct / 100.0).min(1.0),
                created_at: chrono::Utc::now().to_rfc3339(),
                status: RecommendationStatus::Pending,
                applied_at: None,
            });
            warn!(memory_pct = snapshot.system.memory_pct, "high memory usage");
        }

        // Disk usage check
        if snapshot.system.disk_pct > 85.0 {
            let severity = if snapshot.system.disk_pct > 95.0 {
                Severity::Critical
            } else {
                Severity::Warning
            };
            let largest_topic = snapshot
                .topics
                .iter()
                .max_by_key(|t| t.size_bytes)
                .map(|t| t.name.clone())
                .unwrap_or_default();
            recs.push(Recommendation {
                id: Uuid::new_v4().to_string(),
                category: RecommendationCategory::DiskUsage,
                severity,
                title: "Disk usage exceeds threshold".into(),
                description: format!(
                    "Disk usage is at {:.1}%. Reduce retention on large topics to reclaim space.",
                    snapshot.system.disk_pct
                ),
                action: RecommendedAction::IncreaseRetention {
                    topic: largest_topic,
                    hours: 24,
                },
                impact: "Reclaimed disk space by reducing retention period".into(),
                confidence: (snapshot.system.disk_pct / 100.0).min(1.0),
                created_at: chrono::Utc::now().to_rfc3339(),
                status: RecommendationStatus::Pending,
                applied_at: None,
            });
            warn!(disk_pct = snapshot.system.disk_pct, "high disk usage");
        }

        // CPU pressure → scaling recommendation
        if snapshot.system.cpu_pct > 85.0 {
            recs.push(Recommendation {
                id: Uuid::new_v4().to_string(),
                category: RecommendationCategory::Scaling,
                severity: if snapshot.system.cpu_pct > 95.0 {
                    Severity::Critical
                } else {
                    Severity::Warning
                },
                title: "High CPU utilization".into(),
                description: format!(
                    "CPU usage at {:.1}%. Consider scaling up the cluster.",
                    snapshot.system.cpu_pct
                ),
                action: RecommendedAction::ScaleUp { replicas: 1 },
                impact: "Better request handling capacity".into(),
                confidence: (snapshot.system.cpu_pct / 100.0).min(1.0),
                created_at: chrono::Utc::now().to_rfc3339(),
                status: RecommendationStatus::Pending,
                applied_at: None,
            });
        }

        // Store recommendations
        self.stats
            .recommendations_generated
            .fetch_add(recs.len() as u64, Ordering::Relaxed);

        if let Ok(mut stored) = self.recommendations.write() {
            stored.extend(recs.clone());
        }

        info!(count = recs.len(), "analysis complete");
        recs
    }

    /// Return all current recommendations.
    pub fn get_recommendations(&self) -> Vec<Recommendation> {
        self.recommendations.read().map_or_else(|_| Vec::new(), |r| r.clone())
    }

    /// Dismiss a recommendation by id.
    pub fn dismiss(&self, id: &str) -> Result<()> {
        let mut recs = self.recommendations.write().map_err(|_| {
            AdvisorError::NotFound(id.to_string())
        })?;
        let rec = recs
            .iter_mut()
            .find(|r| r.id == id)
            .ok_or_else(|| AdvisorError::NotFound(id.to_string()))?;
        rec.status = RecommendationStatus::Dismissed;
        self.stats.recommendations_dismissed.fetch_add(1, Ordering::Relaxed);
        info!(id = %id, "recommendation dismissed");
        Ok(())
    }

    /// Apply a recommendation by id.
    pub fn apply(&self, id: &str) -> Result<()> {
        let mut recs = self.recommendations.write().map_err(|_| {
            AdvisorError::NotFound(id.to_string())
        })?;
        let rec = recs
            .iter_mut()
            .find(|r| r.id == id)
            .ok_or_else(|| AdvisorError::NotFound(id.to_string()))?;
        if rec.status == RecommendationStatus::Applied {
            return Err(AdvisorError::AlreadyApplied(id.to_string()));
        }
        rec.status = RecommendationStatus::Applied;
        rec.applied_at = Some(chrono::Utc::now().to_rfc3339());
        self.stats.recommendations_applied.fetch_add(1, Ordering::Relaxed);
        info!(id = %id, "recommendation applied");
        Ok(())
    }

    /// Return a reference to the advisor stats.
    pub fn stats(&self) -> &Arc<AdvisorStats> {
        &self.stats
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_system() -> SystemPerf {
        SystemPerf {
            cpu_pct: 30.0,
            memory_pct: 50.0,
            disk_pct: 40.0,
        }
    }

    fn make_snapshot(
        topics: Vec<TopicPerf>,
        consumers: Vec<ConsumerPerf>,
        system: SystemPerf,
    ) -> PerformanceSnapshot {
        PerformanceSnapshot { topics, consumers, system }
    }

    fn make_topic(name: &str, hottest: f64, partitions: u32) -> TopicPerf {
        TopicPerf {
            name: name.to_string(),
            partitions,
            msg_rate: 1000.0,
            size_bytes: 1_000_000,
            hottest_partition_pct: hottest,
        }
    }

    fn make_consumer(group: &str, lag: u64) -> ConsumerPerf {
        ConsumerPerf {
            group: group.to_string(),
            lag,
            throughput: 500.0,
        }
    }

    #[test]
    fn test_default_config() {
        let cfg = AdvisorConfig::default();
        assert!(cfg.enable_auto_analysis);
        assert_eq!(cfg.analysis_interval_secs, 300);
        assert_eq!(cfg.min_data_points, 10);
        assert!(!cfg.enable_auto_apply);
    }

    #[test]
    fn test_new_advisor() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        assert!(advisor.get_recommendations().is_empty());
        assert_eq!(advisor.stats().analyses_run.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_partition_imbalance_warning() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![make_topic("events", 70.0, 4)],
            vec![],
            default_system(),
        );
        let recs = advisor.analyze(&snapshot);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].category, RecommendationCategory::PartitionBalance);
        assert_eq!(recs[0].severity, Severity::Warning);
    }

    #[test]
    fn test_partition_imbalance_critical() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![make_topic("events", 90.0, 4)],
            vec![],
            default_system(),
        );
        let recs = advisor.analyze(&snapshot);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].severity, Severity::Critical);
    }

    #[test]
    fn test_no_partition_imbalance_below_threshold() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![make_topic("events", 50.0, 4)],
            vec![],
            default_system(),
        );
        let recs = advisor.analyze(&snapshot);
        assert!(recs.is_empty());
    }

    #[test]
    fn test_consumer_lag_warning() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![],
            vec![make_consumer("group-a", 200_000)],
            default_system(),
        );
        let recs = advisor.analyze(&snapshot);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].category, RecommendationCategory::ConsumerLag);
        assert_eq!(recs[0].severity, Severity::Warning);
    }

    #[test]
    fn test_consumer_lag_critical() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![],
            vec![make_consumer("group-b", 2_000_000)],
            default_system(),
        );
        let recs = advisor.analyze(&snapshot);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].severity, Severity::Critical);
    }

    #[test]
    fn test_no_consumer_lag_below_threshold() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![],
            vec![make_consumer("group-c", 50_000)],
            default_system(),
        );
        let recs = advisor.analyze(&snapshot);
        assert!(recs.is_empty());
    }

    #[test]
    fn test_high_memory_usage() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![make_topic("logs", 30.0, 2)],
            vec![],
            SystemPerf { cpu_pct: 20.0, memory_pct: 85.0, disk_pct: 40.0 },
        );
        let recs = advisor.analyze(&snapshot);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].category, RecommendationCategory::MemoryUsage);
    }

    #[test]
    fn test_high_disk_usage() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![make_topic("big-topic", 30.0, 2)],
            vec![],
            SystemPerf { cpu_pct: 20.0, memory_pct: 50.0, disk_pct: 90.0 },
        );
        let recs = advisor.analyze(&snapshot);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].category, RecommendationCategory::DiskUsage);
        assert_eq!(recs[0].severity, Severity::Warning);
    }

    #[test]
    fn test_high_cpu_usage() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![],
            vec![],
            SystemPerf { cpu_pct: 88.0, memory_pct: 50.0, disk_pct: 40.0 },
        );
        let recs = advisor.analyze(&snapshot);
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0].category, RecommendationCategory::Scaling);
    }

    #[test]
    fn test_multiple_recommendations() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![make_topic("hot-topic", 75.0, 4)],
            vec![make_consumer("slow-group", 500_000)],
            SystemPerf { cpu_pct: 20.0, memory_pct: 85.0, disk_pct: 90.0 },
        );
        let recs = advisor.analyze(&snapshot);
        assert_eq!(recs.len(), 4); // partition + consumer + memory + disk
    }

    #[test]
    fn test_dismiss_recommendation() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![make_topic("t", 70.0, 2)],
            vec![],
            default_system(),
        );
        let recs = advisor.analyze(&snapshot);
        let id = recs[0].id.clone();
        advisor.dismiss(&id).unwrap();
        let stored = advisor.get_recommendations();
        let r = stored.iter().find(|r| r.id == id).unwrap();
        assert_eq!(r.status, RecommendationStatus::Dismissed);
    }

    #[test]
    fn test_apply_recommendation() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![make_topic("t", 70.0, 2)],
            vec![],
            default_system(),
        );
        let recs = advisor.analyze(&snapshot);
        let id = recs[0].id.clone();
        advisor.apply(&id).unwrap();
        let stored = advisor.get_recommendations();
        let r = stored.iter().find(|r| r.id == id).unwrap();
        assert_eq!(r.status, RecommendationStatus::Applied);
        assert!(r.applied_at.is_some());
    }

    #[test]
    fn test_apply_already_applied() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![make_topic("t", 70.0, 2)],
            vec![],
            default_system(),
        );
        let recs = advisor.analyze(&snapshot);
        let id = recs[0].id.clone();
        advisor.apply(&id).unwrap();
        let err = advisor.apply(&id).unwrap_err();
        assert!(matches!(err, AdvisorError::AlreadyApplied(_)));
    }

    #[test]
    fn test_dismiss_not_found() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let err = advisor.dismiss("nonexistent").unwrap_err();
        assert!(matches!(err, AdvisorError::NotFound(_)));
    }

    #[test]
    fn test_stats_tracking() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![make_topic("t", 70.0, 2)],
            vec![make_consumer("g", 200_000)],
            default_system(),
        );
        let recs = advisor.analyze(&snapshot);
        assert_eq!(advisor.stats().analyses_run.load(Ordering::Relaxed), 1);
        assert_eq!(
            advisor.stats().recommendations_generated.load(Ordering::Relaxed),
            recs.len() as u64
        );
        let id = recs[0].id.clone();
        advisor.apply(&id).unwrap();
        assert_eq!(advisor.stats().recommendations_applied.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_confidence_bounded() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![make_topic("t", 99.0, 2)],
            vec![make_consumer("g", 10_000_000)],
            default_system(),
        );
        let recs = advisor.analyze(&snapshot);
        for rec in &recs {
            assert!(rec.confidence >= 0.0 && rec.confidence <= 1.0);
        }
    }

    #[test]
    fn test_healthy_system_no_recommendations() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snapshot = make_snapshot(
            vec![make_topic("t", 25.0, 4)],
            vec![make_consumer("g", 100)],
            SystemPerf { cpu_pct: 30.0, memory_pct: 40.0, disk_pct: 50.0 },
        );
        let recs = advisor.analyze(&snapshot);
        assert!(recs.is_empty());
    }

    #[test]
    fn test_accumulates_across_analyses() {
        let advisor = PerformanceAdvisor::new(AdvisorConfig::default());
        let snap1 = make_snapshot(
            vec![make_topic("t1", 70.0, 2)],
            vec![],
            default_system(),
        );
        let snap2 = make_snapshot(
            vec![make_topic("t2", 75.0, 4)],
            vec![],
            default_system(),
        );
        advisor.analyze(&snap1);
        advisor.analyze(&snap2);
        assert_eq!(advisor.get_recommendations().len(), 2);
        assert_eq!(advisor.stats().analyses_run.load(Ordering::Relaxed), 2);
    }
}
