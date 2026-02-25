//! Cluster health monitoring
//!
//! Provides comprehensive health checks for cluster components including
//! broker connectivity, partition health, ISR status, replication lag,
//! and overall cluster readiness.

use crate::cluster::node::{NodeId, NodeState};
use crate::cluster::raft::state_machine::ClusterMetadata;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Overall cluster health status
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ClusterHealthStatus {
    /// All components healthy
    Green,
    /// Degraded but operational (e.g., some replicas down)
    Yellow,
    /// Critical issues (e.g., under-replicated partitions, no leader)
    Red,
}

impl std::fmt::Display for ClusterHealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Green => write!(f, "GREEN"),
            Self::Yellow => write!(f, "YELLOW"),
            Self::Red => write!(f, "RED"),
        }
    }
}

/// Individual health check result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    pub name: String,
    pub status: ClusterHealthStatus,
    pub message: String,
    pub details: HashMap<String, String>,
}

/// Complete cluster health report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHealthReport {
    /// Overall status (worst of all checks)
    pub status: ClusterHealthStatus,
    /// Individual check results
    pub checks: Vec<HealthCheck>,
    /// Summary statistics
    pub stats: ClusterStats,
    /// Timestamp of this report
    pub timestamp_ms: u64,
}

/// Cluster-wide statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClusterStats {
    pub total_brokers: usize,
    pub alive_brokers: usize,
    pub dead_brokers: usize,
    pub total_topics: usize,
    pub total_partitions: usize,
    pub total_replicas: usize,
    pub under_replicated_partitions: usize,
    pub offline_partitions: usize,
    pub leaderless_partitions: usize,
    pub avg_replicas_per_broker: f64,
    pub min_isr_violations: usize,
}

/// Cluster health monitor that evaluates cluster state
pub struct ClusterHealthMonitor {
    /// Minimum ISR count for partition health
    min_insync_replicas: usize,
    /// History of health reports for trend analysis
    history: Vec<(Instant, ClusterHealthStatus)>,
    /// Maximum history entries to retain
    max_history: usize,
}

impl ClusterHealthMonitor {
    pub fn new(min_insync_replicas: usize) -> Self {
        Self {
            min_insync_replicas,
            history: Vec::new(),
            max_history: 100,
        }
    }

    /// Run all health checks and produce a report
    pub fn evaluate(&mut self, metadata: &ClusterMetadata) -> ClusterHealthReport {
        let mut checks = Vec::new();

        checks.push(self.check_broker_health(metadata));
        checks.push(self.check_controller_health(metadata));
        checks.push(self.check_partition_health(metadata));
        checks.push(self.check_isr_health(metadata));
        checks.push(self.check_replication_coverage(metadata));
        checks.push(self.check_load_distribution(metadata));

        let status = checks
            .iter()
            .map(|c| c.status)
            .max_by_key(|s| match s {
                ClusterHealthStatus::Green => 0,
                ClusterHealthStatus::Yellow => 1,
                ClusterHealthStatus::Red => 2,
            })
            .unwrap_or(ClusterHealthStatus::Green);

        let stats = self.compute_stats(metadata);

        self.history.push((Instant::now(), status));
        if self.history.len() > self.max_history {
            self.history.remove(0);
        }

        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        ClusterHealthReport {
            status,
            checks,
            stats,
            timestamp_ms,
        }
    }

    /// Check broker availability
    fn check_broker_health(&self, metadata: &ClusterMetadata) -> HealthCheck {
        let total = metadata.brokers.len();
        let alive = metadata.alive_brokers().len();
        let dead = total - alive;

        let (status, message) = if dead == 0 {
            (
                ClusterHealthStatus::Green,
                format!("All {} brokers healthy", total),
            )
        } else if alive == 0 {
            (
                ClusterHealthStatus::Red,
                format!("All {} brokers offline", total),
            )
        } else if dead as f64 / total as f64 > 0.5 {
            (
                ClusterHealthStatus::Red,
                format!("{}/{} brokers offline (majority lost)", dead, total),
            )
        } else {
            (
                ClusterHealthStatus::Yellow,
                format!("{}/{} brokers offline", dead, total),
            )
        };

        let mut details = HashMap::new();
        details.insert("total_brokers".to_string(), total.to_string());
        details.insert("alive_brokers".to_string(), alive.to_string());
        details.insert("dead_brokers".to_string(), dead.to_string());

        for (id, broker) in &metadata.brokers {
            details.insert(
                format!("broker_{}", id),
                format!("{} ({})", broker.state, broker.advertised_addr),
            );
        }

        HealthCheck {
            name: "broker_health".to_string(),
            status,
            message,
            details,
        }
    }

    /// Check if a controller is elected
    fn check_controller_health(&self, metadata: &ClusterMetadata) -> HealthCheck {
        let (status, message) = match metadata.controller_id {
            Some(id) => {
                let alive = metadata
                    .brokers
                    .get(&id)
                    .map(|b| b.is_alive())
                    .unwrap_or(false);
                if alive {
                    (
                        ClusterHealthStatus::Green,
                        format!("Controller node {} is healthy", id),
                    )
                } else {
                    (
                        ClusterHealthStatus::Red,
                        format!("Controller node {} is not alive", id),
                    )
                }
            }
            None => (
                ClusterHealthStatus::Yellow,
                "No controller elected".to_string(),
            ),
        };

        let mut details = HashMap::new();
        details.insert(
            "controller_id".to_string(),
            metadata
                .controller_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "none".to_string()),
        );

        HealthCheck {
            name: "controller_health".to_string(),
            status,
            message,
            details,
        }
    }

    /// Check partition leader availability
    fn check_partition_health(&self, metadata: &ClusterMetadata) -> HealthCheck {
        let alive_ids: Vec<NodeId> = metadata.alive_brokers().iter().map(|b| b.node_id).collect();
        let mut total_partitions = 0;
        let mut leaderless = 0;
        let mut offline = 0;

        for topic in metadata.topics.values() {
            for partition in topic.partitions.values() {
                total_partitions += 1;

                match partition.leader {
                    None => {
                        leaderless += 1;
                        offline += 1;
                    }
                    Some(leader) => {
                        if !alive_ids.contains(&leader) {
                            offline += 1;
                        }
                    }
                }
            }
        }

        let (status, message) = if total_partitions == 0 {
            (ClusterHealthStatus::Green, "No partitions".to_string())
        } else if offline > 0 {
            (
                ClusterHealthStatus::Red,
                format!(
                    "{} offline partitions ({} leaderless) out of {}",
                    offline, leaderless, total_partitions
                ),
            )
        } else {
            (
                ClusterHealthStatus::Green,
                format!("All {} partitions have active leaders", total_partitions),
            )
        };

        let mut details = HashMap::new();
        details.insert("total_partitions".to_string(), total_partitions.to_string());
        details.insert("offline_partitions".to_string(), offline.to_string());
        details.insert("leaderless_partitions".to_string(), leaderless.to_string());

        HealthCheck {
            name: "partition_health".to_string(),
            status,
            message,
            details,
        }
    }

    /// Check ISR health
    fn check_isr_health(&self, metadata: &ClusterMetadata) -> HealthCheck {
        let alive_ids: Vec<NodeId> = metadata.alive_brokers().iter().map(|b| b.node_id).collect();
        let mut total_partitions = 0;
        let mut under_min_isr = 0;
        let mut shrunk_isr = 0;

        for topic in metadata.topics.values() {
            for partition in topic.partitions.values() {
                total_partitions += 1;

                let alive_isr: Vec<NodeId> = partition
                    .isr
                    .iter()
                    .filter(|r| alive_ids.contains(r))
                    .copied()
                    .collect();

                if alive_isr.len() < self.min_insync_replicas {
                    under_min_isr += 1;
                }

                if alive_isr.len() < partition.isr.len() {
                    shrunk_isr += 1;
                }
            }
        }

        let (status, message) = if under_min_isr > 0 {
            (
                ClusterHealthStatus::Red,
                format!(
                    "{} partitions below min.insync.replicas ({})",
                    under_min_isr, self.min_insync_replicas
                ),
            )
        } else if shrunk_isr > 0 {
            (
                ClusterHealthStatus::Yellow,
                format!("{} partitions with shrunk ISR", shrunk_isr),
            )
        } else {
            (
                ClusterHealthStatus::Green,
                format!("All {} partitions have full ISR", total_partitions),
            )
        };

        let mut details = HashMap::new();
        details.insert(
            "min_insync_replicas".to_string(),
            self.min_insync_replicas.to_string(),
        );
        details.insert("under_min_isr".to_string(), under_min_isr.to_string());
        details.insert("shrunk_isr".to_string(), shrunk_isr.to_string());

        HealthCheck {
            name: "isr_health".to_string(),
            status,
            message,
            details,
        }
    }

    /// Check replication coverage
    fn check_replication_coverage(&self, metadata: &ClusterMetadata) -> HealthCheck {
        let alive_ids: Vec<NodeId> = metadata.alive_brokers().iter().map(|b| b.node_id).collect();
        let mut total = 0;
        let mut under_replicated = 0;

        for topic in metadata.topics.values() {
            for partition in topic.partitions.values() {
                total += 1;
                let alive_replicas = partition
                    .replicas
                    .iter()
                    .filter(|r| alive_ids.contains(r))
                    .count();

                if alive_replicas < topic.replication_factor as usize {
                    under_replicated += 1;
                }
            }
        }

        let (status, message) = if under_replicated > 0 {
            (
                ClusterHealthStatus::Yellow,
                format!("{}/{} partitions under-replicated", under_replicated, total),
            )
        } else if total > 0 {
            (
                ClusterHealthStatus::Green,
                format!("All {} partitions fully replicated", total),
            )
        } else {
            (ClusterHealthStatus::Green, "No partitions".to_string())
        };

        let mut details = HashMap::new();
        details.insert("under_replicated".to_string(), under_replicated.to_string());

        HealthCheck {
            name: "replication_coverage".to_string(),
            status,
            message,
            details,
        }
    }

    /// Check load distribution across brokers
    fn check_load_distribution(&self, metadata: &ClusterMetadata) -> HealthCheck {
        let alive_brokers = metadata.alive_brokers();
        if alive_brokers.is_empty() {
            return HealthCheck {
                name: "load_distribution".to_string(),
                status: ClusterHealthStatus::Green,
                message: "No brokers to check".to_string(),
                details: HashMap::new(),
            };
        }

        let alive_ids: Vec<NodeId> = alive_brokers.iter().map(|b| b.node_id).collect();
        let mut load_map: HashMap<NodeId, usize> = alive_ids.iter().map(|&id| (id, 0)).collect();

        for topic in metadata.topics.values() {
            for partition in topic.partitions.values() {
                if let Some(leader) = partition.leader {
                    if let Some(count) = load_map.get_mut(&leader) {
                        *count += 1;
                    }
                }
            }
        }

        let loads: Vec<usize> = load_map.values().copied().collect();
        let max = loads.iter().copied().max().unwrap_or(0);
        let min = loads.iter().copied().min().unwrap_or(0);
        let avg = loads.iter().sum::<usize>() as f64 / loads.len().max(1) as f64;

        let skew = if avg > 0.0 {
            (max as f64 - min as f64) / avg
        } else {
            0.0
        };

        let (status, message) = if skew > 0.5 {
            (
                ClusterHealthStatus::Yellow,
                format!(
                    "Leader skew {:.1}% — broker load range [{}, {}], avg {:.1}",
                    skew * 100.0,
                    min,
                    max,
                    avg
                ),
            )
        } else {
            (
                ClusterHealthStatus::Green,
                format!(
                    "Leaders well distributed — range [{}, {}], avg {:.1}",
                    min, max, avg
                ),
            )
        };

        let mut details = HashMap::new();
        details.insert("leader_skew".to_string(), format!("{:.2}", skew));
        for (&id, &count) in &load_map {
            details.insert(format!("broker_{}_leaders", id), count.to_string());
        }

        HealthCheck {
            name: "load_distribution".to_string(),
            status,
            message,
            details,
        }
    }

    /// Compute cluster statistics
    fn compute_stats(&self, metadata: &ClusterMetadata) -> ClusterStats {
        let alive_ids: Vec<NodeId> = metadata.alive_brokers().iter().map(|b| b.node_id).collect();
        let mut stats = ClusterStats {
            total_brokers: metadata.brokers.len(),
            alive_brokers: alive_ids.len(),
            dead_brokers: metadata.brokers.len() - alive_ids.len(),
            total_topics: metadata.topics.len(),
            ..Default::default()
        };

        for topic in metadata.topics.values() {
            for partition in topic.partitions.values() {
                stats.total_partitions += 1;
                stats.total_replicas += partition.replicas.len();

                let alive_replicas = partition
                    .replicas
                    .iter()
                    .filter(|r| alive_ids.contains(r))
                    .count();

                if alive_replicas < topic.replication_factor as usize {
                    stats.under_replicated_partitions += 1;
                }

                match partition.leader {
                    None => {
                        stats.leaderless_partitions += 1;
                        stats.offline_partitions += 1;
                    }
                    Some(leader) if !alive_ids.contains(&leader) => {
                        stats.offline_partitions += 1;
                    }
                    _ => {}
                }

                let alive_isr = partition
                    .isr
                    .iter()
                    .filter(|r| alive_ids.contains(r))
                    .count();
                if alive_isr < self.min_insync_replicas {
                    stats.min_isr_violations += 1;
                }
            }
        }

        stats.avg_replicas_per_broker = if stats.alive_brokers > 0 {
            stats.total_replicas as f64 / stats.alive_brokers as f64
        } else {
            0.0
        };

        stats
    }

    /// Get recent health trend (last N reports)
    pub fn health_trend(&self, count: usize) -> Vec<ClusterHealthStatus> {
        self.history
            .iter()
            .rev()
            .take(count)
            .map(|(_, status)| *status)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::node::BrokerInfo;
    use crate::cluster::raft::types::{PartitionAssignment, TopicAssignment};

    fn make_broker(id: NodeId, state: NodeState) -> BrokerInfo {
        BrokerInfo {
            node_id: id,
            advertised_addr: format!("127.0.0.1:{}", 9092 + id).parse().unwrap(),
            inter_broker_addr: format!("127.0.0.1:{}", 9192 + id).parse().unwrap(),
            rack: None,
            datacenter: None,
            state,
        }
    }

    #[test]
    fn test_healthy_cluster_is_green() {
        let mut monitor = ClusterHealthMonitor::new(1);
        let mut metadata = ClusterMetadata::new("test".to_string());
        metadata.controller_id = Some(1);
        metadata.register_broker(make_broker(1, NodeState::Running));
        metadata.register_broker(make_broker(2, NodeState::Running));

        let topic = TopicAssignment::new("t1".to_string(), 2, 2)
            .with_partition(PartitionAssignment::new(0, vec![1, 2]).with_leader(1))
            .with_partition(PartitionAssignment::new(1, vec![2, 1]).with_leader(2));
        metadata.topics.insert("t1".to_string(), topic);

        let report = monitor.evaluate(&metadata);
        assert_eq!(report.status, ClusterHealthStatus::Green);
        assert_eq!(report.stats.total_partitions, 2);
        assert_eq!(report.stats.offline_partitions, 0);
    }

    #[test]
    fn test_dead_broker_triggers_yellow_or_red() {
        let mut monitor = ClusterHealthMonitor::new(1);
        let mut metadata = ClusterMetadata::new("test".to_string());
        metadata.controller_id = Some(1);
        metadata.register_broker(make_broker(1, NodeState::Running));
        metadata.register_broker(make_broker(2, NodeState::Dead));

        let topic = TopicAssignment::new("t1".to_string(), 1, 2)
            .with_partition(PartitionAssignment::new(0, vec![1, 2]).with_leader(1));
        metadata.topics.insert("t1".to_string(), topic);

        let report = monitor.evaluate(&metadata);
        assert_ne!(report.status, ClusterHealthStatus::Green);
        assert_eq!(report.stats.dead_brokers, 1);
    }

    #[test]
    fn test_leaderless_partition_is_red() {
        let mut monitor = ClusterHealthMonitor::new(1);
        let mut metadata = ClusterMetadata::new("test".to_string());
        metadata.controller_id = Some(1);
        metadata.register_broker(make_broker(1, NodeState::Running));

        let topic = TopicAssignment::new("t1".to_string(), 1, 1)
            .with_partition(PartitionAssignment::new(0, vec![1])); // no leader set
        metadata.topics.insert("t1".to_string(), topic);

        let report = monitor.evaluate(&metadata);
        assert_eq!(report.status, ClusterHealthStatus::Red);
        assert_eq!(report.stats.leaderless_partitions, 1);
    }

    #[test]
    fn test_empty_cluster_is_green() {
        let mut monitor = ClusterHealthMonitor::new(1);
        let metadata = ClusterMetadata::new("test".to_string());

        let report = monitor.evaluate(&metadata);
        assert_eq!(report.status, ClusterHealthStatus::Green);
    }

    #[test]
    fn test_health_trend_tracking() {
        let mut monitor = ClusterHealthMonitor::new(1);
        let metadata = ClusterMetadata::new("test".to_string());

        monitor.evaluate(&metadata);
        monitor.evaluate(&metadata);
        monitor.evaluate(&metadata);

        let trend = monitor.health_trend(3);
        assert_eq!(trend.len(), 3);
    }
}
