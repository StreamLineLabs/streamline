//! Smart rebalancer — computes and executes partition rebalancing plans.

use super::analyzer::{SkewReport, SkewSeverity};
use serde::{Deserialize, Serialize};

/// Rebalancing mode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RebalanceMode {
    /// Fully automatic — rebalance without human approval
    AutoPilot,
    /// Human-in-the-loop — generate plan, wait for approval
    Approval,
    /// Dry-run — generate plan but don't execute
    DryRun,
}

/// Configuration for the smart rebalancer.
#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    /// Rebalancing mode
    pub mode: RebalanceMode,
    /// Minimum skew coefficient to trigger rebalancing
    pub trigger_threshold: f64,
    /// Maximum percentage of data to move in a single rebalance
    pub max_movement_pct: f64,
    /// Cooldown period between rebalances (seconds)
    pub cooldown_secs: u64,
    /// Maximum concurrent partition moves
    pub max_concurrent_moves: usize,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            mode: RebalanceMode::Approval,
            trigger_threshold: 0.5,
            max_movement_pct: 25.0,
            cooldown_secs: 300,
            max_concurrent_moves: 2,
        }
    }
}

/// A single rebalancing action.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceAction {
    /// Topic
    pub topic: String,
    /// Action type
    pub action_type: ActionType,
    /// Estimated data movement in bytes
    pub estimated_movement_bytes: u64,
    /// Expected improvement description
    pub expected_improvement: String,
}

/// Types of rebalancing actions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionType {
    /// Split a hot partition into two
    SplitPartition { partition: i32 },
    /// Merge two cold partitions
    MergePartitions { from: i32, into: i32 },
    /// Reassign partition to a different broker
    ReassignPartition {
        partition: i32,
        from_broker: i32,
        to_broker: i32,
    },
    /// Suggest key redistribution strategy
    SuggestKeyStrategy { partition: i32, strategy: String },
    /// Add new partitions to distribute load
    AddPartitions { count: i32 },
}

/// A rebalancing plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancePlan {
    /// Topic being rebalanced
    pub topic: String,
    /// Actions to take
    pub actions: Vec<RebalanceAction>,
    /// Current skew coefficient
    pub current_skew: f64,
    /// Estimated skew after rebalancing
    pub estimated_skew_after: f64,
    /// Total data movement
    pub total_movement_bytes: u64,
    /// Summary description
    pub summary: String,
}

/// Result of executing a rebalancing plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct RebalanceResult {
    pub plan: RebalancePlan,
    pub executed: bool,
    pub actions_completed: usize,
    pub actions_failed: usize,
    pub duration_ms: u64,
    pub error: Option<String>,
}

/// Smart rebalancer that generates and executes partition rebalancing plans.
pub struct SmartRebalancer {
    config: RebalanceConfig,
    last_rebalance_ts: Option<i64>,
}

impl SmartRebalancer {
    /// Create a new smart rebalancer.
    pub fn new(config: RebalanceConfig) -> Self {
        Self {
            config,
            last_rebalance_ts: None,
        }
    }

    /// Check if rebalancing should be triggered.
    pub fn should_rebalance(&self, report: &SkewReport) -> bool {
        if report.severity == SkewSeverity::None {
            return false;
        }

        if report.skew_coefficient < self.config.trigger_threshold {
            return false;
        }

        // Check cooldown
        if let Some(last_ts) = self.last_rebalance_ts {
            let now = chrono::Utc::now().timestamp();
            if (now - last_ts) < self.config.cooldown_secs as i64 {
                return false;
            }
        }

        true
    }

    /// Generate a rebalancing plan from a skew report.
    pub fn plan(&self, report: &SkewReport) -> RebalancePlan {
        let mut actions = Vec::new();

        let hot_partitions: Vec<_> = report.partitions.iter().filter(|p| p.is_hot).collect();

        let cold_partitions: Vec<_> = report.partitions.iter().filter(|p| p.is_cold).collect();

        // Strategy: If there are hot partitions, recommend adding partitions
        // or reassigning keys
        if !hot_partitions.is_empty() {
            // If many cold partitions exist, suggest reassignment
            if cold_partitions.len() >= 2 {
                for hot in &hot_partitions {
                    actions.push(RebalanceAction {
                        topic: report.topic.clone(),
                        action_type: ActionType::SuggestKeyStrategy {
                            partition: hot.partition,
                            strategy: format!(
                                "Redistribute keys from partition {} ({}% load) to underutilized partitions",
                                hot.partition, hot.throughput_share_pct as i32
                            ),
                        },
                        estimated_movement_bytes: 0,
                        expected_improvement: format!(
                            "Reduce partition {} load from {:.0}% to ~{:.0}%",
                            hot.partition,
                            hot.throughput_share_pct,
                            hot.expected_share_pct
                        ),
                    });
                }
            }

            // If skew is critical, recommend adding partitions
            if report.severity == SkewSeverity::Critical || report.severity == SkewSeverity::High {
                let additional = (report.num_partitions as f64 * 0.5).ceil() as i32;
                actions.push(RebalanceAction {
                    topic: report.topic.clone(),
                    action_type: ActionType::AddPartitions {
                        count: additional.max(1),
                    },
                    estimated_movement_bytes: 0,
                    expected_improvement: format!(
                        "Adding {} partitions to spread load more evenly",
                        additional.max(1)
                    ),
                });
            }
        }

        // If many cold partitions and few hot, suggest merging
        if cold_partitions.len() >= 2 && hot_partitions.is_empty() {
            for pair in cold_partitions.windows(2) {
                actions.push(RebalanceAction {
                    topic: report.topic.clone(),
                    action_type: ActionType::MergePartitions {
                        from: pair[1].partition,
                        into: pair[0].partition,
                    },
                    estimated_movement_bytes: 0,
                    expected_improvement: format!(
                        "Merge cold partitions {} and {} to reduce overhead",
                        pair[0].partition, pair[1].partition
                    ),
                });
            }
        }

        let total_movement: u64 = actions.iter().map(|a| a.estimated_movement_bytes).sum();
        let estimated_skew_after = report.skew_coefficient * 0.5;

        let summary = if actions.is_empty() {
            "No rebalancing actions needed".to_string()
        } else {
            format!(
                "{} actions planned to reduce skew from {:.2} to ~{:.2}",
                actions.len(),
                report.skew_coefficient,
                estimated_skew_after
            )
        };

        RebalancePlan {
            topic: report.topic.clone(),
            actions,
            current_skew: report.skew_coefficient,
            estimated_skew_after,
            total_movement_bytes: total_movement,
            summary,
        }
    }

    /// Mark the last rebalance timestamp.
    pub fn record_rebalance(&mut self) {
        self.last_rebalance_ts = Some(chrono::Utc::now().timestamp());
    }

    /// Get configuration.
    pub fn config(&self) -> &RebalanceConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::smart_partition::analyzer::*;

    #[test]
    fn test_plan_for_skewed_topic() {
        let report = SkewReport {
            topic: "events".to_string(),
            num_partitions: 4,
            severity: SkewSeverity::High,
            skew_coefficient: 1.2,
            partitions: vec![
                PartitionSkew {
                    partition: 0,
                    throughput_share_pct: 80.0,
                    expected_share_pct: 25.0,
                    deviation_pct: 55.0,
                    is_hot: true,
                    is_cold: false,
                },
                PartitionSkew {
                    partition: 1,
                    throughput_share_pct: 10.0,
                    expected_share_pct: 25.0,
                    deviation_pct: -15.0,
                    is_hot: false,
                    is_cold: false,
                },
                PartitionSkew {
                    partition: 2,
                    throughput_share_pct: 6.0,
                    expected_share_pct: 25.0,
                    deviation_pct: -19.0,
                    is_hot: false,
                    is_cold: false,
                },
                PartitionSkew {
                    partition: 3,
                    throughput_share_pct: 4.0,
                    expected_share_pct: 25.0,
                    deviation_pct: -21.0,
                    is_hot: false,
                    is_cold: true,
                },
            ],
            total_throughput_mps: 1000.0,
            recommendation: "Rebalance recommended".to_string(),
        };

        let rebalancer = SmartRebalancer::new(RebalanceConfig::default());
        assert!(rebalancer.should_rebalance(&report));

        let plan = rebalancer.plan(&report);
        assert!(!plan.actions.is_empty());
        assert!(plan.estimated_skew_after < plan.current_skew);
    }

    #[test]
    fn test_no_rebalance_when_balanced() {
        let report = SkewReport {
            topic: "events".to_string(),
            num_partitions: 4,
            severity: SkewSeverity::None,
            skew_coefficient: 0.05,
            partitions: Vec::new(),
            total_throughput_mps: 400.0,
            recommendation: "Well balanced".to_string(),
        };

        let rebalancer = SmartRebalancer::new(RebalanceConfig::default());
        assert!(!rebalancer.should_rebalance(&report));
    }
}
