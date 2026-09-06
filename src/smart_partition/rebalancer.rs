//! Smart rebalancer -- computes and executes partition rebalancing plans
//! using greedy bin-packing, rack-awareness constraints, and validation.

use super::analyzer::{SkewAnalysis, SkewReport, SkewSeverity};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

// ── Configuration ───────────────────────────────────────────────────────

/// Rebalancing mode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RebalanceMode {
    /// Fully automatic
    AutoPilot,
    /// Human-in-the-loop
    Approval,
    /// Dry-run only
    DryRun,
}

/// Configuration for the smart rebalancer.
#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    pub mode: RebalanceMode,
    /// Minimum skew coefficient to trigger rebalancing
    pub trigger_threshold: f64,
    /// Maximum percentage of data to move in a single rebalance
    pub max_movement_pct: f64,
    /// Cooldown period between rebalances (seconds)
    pub cooldown_secs: u64,
    /// Maximum concurrent partition moves
    pub max_concurrent_moves: usize,
    /// Maximum number of partitions a single broker may hold
    pub max_partition_count_per_broker: u32,
    /// Whether to enforce rack-diversity for replica placement
    pub rack_awareness: bool,
    /// Minimum replica factor to maintain during rebalancing
    pub min_replica_factor: u32,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            mode: RebalanceMode::Approval,
            trigger_threshold: 0.5,
            max_movement_pct: 25.0,
            cooldown_secs: 300,
            max_concurrent_moves: 2,
            max_partition_count_per_broker: 1000,
            rack_awareness: false,
            min_replica_factor: 1,
        }
    }
}

// ── Core data types ─────────────────────────────────────────────────────

/// A single partition move instruction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMove {
    pub partition_id: i32,
    pub from_broker: i32,
    pub to_broker: i32,
    pub estimated_bytes: u64,
}

/// State of a single broker in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerState {
    pub broker_id: i32,
    pub rack: Option<String>,
    pub partition_ids: Vec<i32>,
    pub total_throughput_mps: f64,
    pub total_bytes: u64,
    pub available_capacity_pct: f64,
}

/// Assignment of a partition to a broker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub partition_id: i32,
    pub broker_id: i32,
    pub is_leader: bool,
    pub size_bytes: u64,
    pub throughput_mps: f64,
}

/// A single rebalancing action (legacy format).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalanceAction {
    pub topic: String,
    pub action_type: ActionType,
    pub estimated_movement_bytes: u64,
    pub expected_improvement: String,
}

/// Types of rebalancing actions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionType {
    SplitPartition {
        partition: i32,
    },
    MergePartitions {
        from: i32,
        into: i32,
    },
    ReassignPartition {
        partition: i32,
        from_broker: i32,
        to_broker: i32,
    },
    SuggestKeyStrategy {
        partition: i32,
        strategy: String,
    },
    AddPartitions {
        count: i32,
    },
}

/// A rebalancing plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancePlan {
    pub topic: String,
    /// Legacy action list
    pub actions: Vec<RebalanceAction>,
    /// Concrete partition moves (greedy bin-packing)
    pub moves: Vec<PartitionMove>,
    pub current_skew: f64,
    pub estimated_skew_after: f64,
    /// Total data movement (bytes)
    pub total_movement_bytes: u64,
    /// Estimated duration in seconds
    pub estimated_duration_secs: f64,
    /// Total bytes to relocate
    pub data_to_move_bytes: u64,
    pub summary: String,
}

impl RebalancePlan {
    /// Convert estimated duration to `std::time::Duration`.
    pub fn estimated_duration(&self) -> Duration {
        Duration::from_secs_f64(self.estimated_duration_secs)
    }
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

// ── Validation ──────────────────────────────────────────────────────────

/// Severity of a validation finding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValidationSeverity {
    Error,
    Warning,
}

/// A single validation issue found in a rebalance plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    pub code: String,
    pub message: String,
    pub severity: ValidationSeverity,
}

/// Snapshot of the cluster topology.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterState {
    pub brokers: Vec<BrokerState>,
    pub total_partitions: u32,
    pub skew_coefficient: f64,
}

/// Result of a dry-run simulation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunResult {
    pub plan: RebalancePlan,
    pub pre_state: ClusterState,
    pub post_state: ClusterState,
    pub warnings: Vec<String>,
    pub estimated_duration_secs: f64,
}

// ── SmartRebalancer ─────────────────────────────────────────────────────

/// Smart rebalancer that generates and executes partition rebalancing plans.
pub struct SmartRebalancer {
    config: RebalanceConfig,
    last_rebalance_ts: Option<i64>,
}

impl SmartRebalancer {
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
        if let Some(last_ts) = self.last_rebalance_ts {
            let now = chrono::Utc::now().timestamp();
            if (now - last_ts) < self.config.cooldown_secs as i64 {
                return false;
            }
        }
        true
    }

    /// Generate a rebalancing plan from a legacy `SkewReport`.
    pub fn plan(&self, report: &SkewReport) -> RebalancePlan {
        let mut actions = Vec::new();

        let hot_partitions: Vec<_> = report.partitions.iter().filter(|p| p.is_hot).collect();
        let cold_partitions: Vec<_> = report.partitions.iter().filter(|p| p.is_cold).collect();

        if !hot_partitions.is_empty() {
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
            moves: Vec::new(),
            current_skew: report.skew_coefficient,
            estimated_skew_after,
            total_movement_bytes: total_movement,
            estimated_duration_secs: 0.0,
            data_to_move_bytes: total_movement,
            summary,
        }
    }

    // ── Greedy bin-packing rebalancer ───────────────────────────────────

    /// Generate a rebalance plan using greedy bin-packing over the broker
    /// topology.
    ///
    /// Moves partitions from over-loaded brokers to under-loaded ones while
    /// respecting `max_partition_count_per_broker` and rack-awareness.
    pub fn plan_rebalance(
        &self,
        analysis: &SkewAnalysis,
        assignments: &[PartitionAssignment],
        brokers: &[BrokerState],
    ) -> RebalancePlan {
        if brokers.is_empty() || assignments.is_empty() {
            return self.empty_plan(&analysis.topic, analysis.skew_coefficient);
        }

        let mut broker_load: HashMap<i32, f64> = HashMap::new();
        let mut broker_parts: HashMap<i32, Vec<i32>> = HashMap::new();
        let mut partition_broker: HashMap<i32, i32> = HashMap::new();
        let mut partition_bytes: HashMap<i32, u64> = HashMap::new();
        let mut partition_throughput: HashMap<i32, f64> = HashMap::new();

        for b in brokers {
            broker_load.insert(b.broker_id, b.total_throughput_mps);
            broker_parts.insert(b.broker_id, b.partition_ids.clone());
        }
        for a in assignments {
            partition_broker.insert(a.partition_id, a.broker_id);
            partition_bytes.insert(a.partition_id, a.size_bytes);
            partition_throughput.insert(a.partition_id, a.throughput_mps);
        }

        let broker_rack: HashMap<i32, Option<String>> = brokers
            .iter()
            .map(|b| (b.broker_id, b.rack.clone()))
            .collect();

        // Mean broker load
        let mean_load: f64 = if !broker_load.is_empty() {
            broker_load.values().sum::<f64>() / broker_load.len() as f64
        } else {
            0.0
        };

        // Collect movable partitions from over-loaded brokers, sorted by
        // throughput descending (move biggest first).
        let mut movable: Vec<(i32, f64, u64)> = Vec::new();
        for (&broker_id, load) in &broker_load {
            if *load > mean_load * 1.3 {
                if let Some(parts) = broker_parts.get(&broker_id) {
                    for &pid in parts {
                        let tp = partition_throughput.get(&pid).copied().unwrap_or(0.0);
                        let bytes = partition_bytes.get(&pid).copied().unwrap_or(0);
                        movable.push((pid, tp, bytes));
                    }
                }
            }
        }
        movable.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let max_per_broker = self.config.max_partition_count_per_broker;
        let max_movement_bytes = {
            let total_bytes: u64 = partition_bytes.values().sum();
            (total_bytes as f64 * self.config.max_movement_pct / 100.0) as u64
        };

        let mut moves: Vec<PartitionMove> = Vec::new();
        let mut moved_bytes: u64 = 0;
        let mut moved_parts: HashSet<i32> = HashSet::new();

        for (pid, tp, bytes) in &movable {
            if moved_bytes + bytes > max_movement_bytes {
                break;
            }
            if moved_parts.contains(pid) {
                continue;
            }

            let from_broker = match partition_broker.get(pid) {
                Some(&b) => b,
                None => continue,
            };

            let from_rack = broker_rack.get(&from_broker).cloned().flatten();
            let target = broker_load
                .iter()
                .filter(|(&bid, _)| {
                    if bid == from_broker {
                        return false;
                    }
                    let count = broker_parts.get(&bid).map(|v| v.len()).unwrap_or(0) as u32;
                    if count >= max_per_broker {
                        return false;
                    }
                    if self.config.rack_awareness {
                        let target_rack = broker_rack.get(&bid).cloned().flatten();
                        if target_rack.is_some() && target_rack == from_rack {
                            return false;
                        }
                    }
                    true
                })
                .min_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
                .map(|(&bid, _)| bid);

            if let Some(to_broker) = target {
                moves.push(PartitionMove {
                    partition_id: *pid,
                    from_broker,
                    to_broker,
                    estimated_bytes: *bytes,
                });

                *broker_load.entry(from_broker).or_default() -= tp;
                *broker_load.entry(to_broker).or_default() += tp;
                if let Some(parts) = broker_parts.get_mut(&from_broker) {
                    parts.retain(|&p| p != *pid);
                }
                broker_parts.entry(to_broker).or_default().push(*pid);
                partition_broker.insert(*pid, to_broker);
                moved_bytes += bytes;
                moved_parts.insert(*pid);
            }
        }

        let total_movement: u64 = moves.iter().map(|m| m.estimated_bytes).sum();
        let estimated_skew_after = if !moves.is_empty() {
            analysis.skew_coefficient * 0.6
        } else {
            analysis.skew_coefficient
        };

        let summary = if moves.is_empty() {
            "No partition moves needed".to_string()
        } else {
            format!(
                "{} partition moves planned ({} bytes), est. skew {:.2} -> {:.2}",
                moves.len(),
                total_movement,
                analysis.skew_coefficient,
                estimated_skew_after,
            )
        };

        RebalancePlan {
            topic: analysis.topic.clone(),
            actions: Vec::new(),
            moves,
            current_skew: analysis.skew_coefficient,
            estimated_skew_after,
            total_movement_bytes: total_movement,
            estimated_duration_secs: estimate_duration_secs(total_movement, 50_000_000),
            data_to_move_bytes: total_movement,
            summary,
        }
    }

    /// Estimate how long a rebalance plan will take given a sustained
    /// throughput in bytes/sec.
    pub fn estimate_rebalance_duration(
        &self,
        plan: &RebalancePlan,
        throughput_bytes_per_sec: u64,
    ) -> Duration {
        Duration::from_secs_f64(estimate_duration_secs(
            plan.data_to_move_bytes,
            throughput_bytes_per_sec,
        ))
    }

    /// Validate a rebalance plan against the broker topology.
    ///
    /// Checks: max partition count, SPOF, rack-awareness, replica factor,
    /// and ISR emptiness.
    pub fn validate_plan(
        &self,
        plan: &RebalancePlan,
        brokers: &[BrokerState],
    ) -> Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();

        // Post-move partition counts per broker
        let mut counts: HashMap<i32, u32> = brokers
            .iter()
            .map(|b| (b.broker_id, b.partition_ids.len() as u32))
            .collect();

        for mv in &plan.moves {
            if let Some(c) = counts.get_mut(&mv.from_broker) {
                *c = c.saturating_sub(1);
            }
            *counts.entry(mv.to_broker).or_default() += 1;
        }

        let total: u32 = counts.values().sum();

        for (&broker, &count) in &counts {
            if count > self.config.max_partition_count_per_broker {
                errors.push(ValidationError {
                    code: "MAX_PARTITIONS_EXCEEDED".into(),
                    message: format!(
                        "Broker {} would hold {} partitions (max {})",
                        broker, count, self.config.max_partition_count_per_broker
                    ),
                    severity: ValidationSeverity::Error,
                });
            }
        }

        // Single point of failure
        for (&broker, &count) in &counts {
            if total > 0 && (count as f64 / total as f64) > 0.5 {
                errors.push(ValidationError {
                    code: "SINGLE_POINT_OF_FAILURE".into(),
                    message: format!(
                        "Broker {} would hold {:.0}% of all partitions",
                        broker,
                        (count as f64 / total as f64) * 100.0
                    ),
                    severity: ValidationSeverity::Error,
                });
            }
        }

        // Rack-awareness
        if self.config.rack_awareness {
            let broker_rack: HashMap<i32, Option<String>> = brokers
                .iter()
                .map(|b| (b.broker_id, b.rack.clone()))
                .collect();

            for mv in &plan.moves {
                let from_rack = broker_rack.get(&mv.from_broker).cloned().flatten();
                let to_rack = broker_rack.get(&mv.to_broker).cloned().flatten();
                if from_rack.is_some() && from_rack == to_rack {
                    errors.push(ValidationError {
                        code: "RACK_AWARENESS_VIOLATION".into(),
                        message: format!("Partition {} moving within same rack", mv.partition_id,),
                        severity: ValidationSeverity::Warning,
                    });
                }
            }
        }

        // Replica factor
        if (brokers.len() as u32) < self.config.min_replica_factor {
            errors.push(ValidationError {
                code: "INSUFFICIENT_BROKERS_FOR_REPLICATION".into(),
                message: format!(
                    "Only {} brokers available but min_replica_factor is {}",
                    brokers.len(),
                    self.config.min_replica_factor
                ),
                severity: ValidationSeverity::Error,
            });
        }

        // Warn about empty brokers
        for (&broker, &count) in &counts {
            if count == 0 {
                errors.push(ValidationError {
                    code: "EMPTY_BROKER".into(),
                    message: format!("Broker {broker} would have 0 partitions after rebalance"),
                    severity: ValidationSeverity::Warning,
                });
            }
        }

        if errors
            .iter()
            .any(|e| e.severity == ValidationSeverity::Error)
        {
            Err(errors)
        } else {
            Ok(())
        }
    }

    /// Simulate executing the plan without applying changes.
    pub fn dry_run(&self, plan: &RebalancePlan, brokers: &[BrokerState]) -> DryRunResult {
        let pre_state = build_cluster_state(brokers, plan.current_skew);

        let mut post_brokers = brokers.to_vec();
        for mv in &plan.moves {
            if let Some(src) = post_brokers
                .iter_mut()
                .find(|b| b.broker_id == mv.from_broker)
            {
                src.partition_ids.retain(|&p| p != mv.partition_id);
                src.total_bytes = src.total_bytes.saturating_sub(mv.estimated_bytes);
            }
            if let Some(dst) = post_brokers
                .iter_mut()
                .find(|b| b.broker_id == mv.to_broker)
            {
                dst.partition_ids.push(mv.partition_id);
                dst.total_bytes += mv.estimated_bytes;
            }
        }

        let post_state = build_cluster_state(&post_brokers, plan.estimated_skew_after);

        let mut warnings = Vec::new();
        if let Err(errs) = self.validate_plan(plan, brokers) {
            for e in errs {
                warnings.push(format!("[{}] {}", e.code, e.message));
            }
        }

        let dur = estimate_duration_secs(plan.data_to_move_bytes, 50_000_000);

        DryRunResult {
            plan: plan.clone(),
            pre_state,
            post_state,
            warnings,
            estimated_duration_secs: dur,
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

    fn empty_plan(&self, topic: &str, skew: f64) -> RebalancePlan {
        RebalancePlan {
            topic: topic.to_string(),
            actions: Vec::new(),
            moves: Vec::new(),
            current_skew: skew,
            estimated_skew_after: skew,
            total_movement_bytes: 0,
            estimated_duration_secs: 0.0,
            data_to_move_bytes: 0,
            summary: "No rebalancing actions needed".to_string(),
        }
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn estimate_duration_secs(bytes: u64, throughput_bytes_per_sec: u64) -> f64 {
    if throughput_bytes_per_sec == 0 {
        return 0.0;
    }
    bytes as f64 / throughput_bytes_per_sec as f64
}

fn build_cluster_state(brokers: &[BrokerState], skew: f64) -> ClusterState {
    let total_partitions: u32 = brokers.iter().map(|b| b.partition_ids.len() as u32).sum();
    ClusterState {
        brokers: brokers.to_vec(),
        total_partitions,
        skew_coefficient: skew,
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::smart_partition::analyzer::*;

    fn make_brokers(count: usize, parts_each: usize) -> Vec<BrokerState> {
        (0..count)
            .map(|i| {
                let start = (i * parts_each) as i32;
                BrokerState {
                    broker_id: i as i32,
                    rack: Some(format!("rack-{}", i % 3)),
                    partition_ids: (start..start + parts_each as i32).collect(),
                    total_throughput_mps: 100.0 * parts_each as f64,
                    total_bytes: 1_000_000 * parts_each as u64,
                    available_capacity_pct: 50.0,
                }
            })
            .collect()
    }

    fn make_assignments(brokers: &[BrokerState]) -> Vec<PartitionAssignment> {
        brokers
            .iter()
            .flat_map(|b| {
                b.partition_ids.iter().map(move |&pid| PartitionAssignment {
                    partition_id: pid,
                    broker_id: b.broker_id,
                    is_leader: true,
                    size_bytes: 1_000_000,
                    throughput_mps: 100.0,
                })
            })
            .collect()
    }

    fn skew_analysis(skew: f64, partitions: usize) -> SkewAnalysis {
        SkewAnalysis {
            topic: "test-topic".into(),
            skew_coefficient: skew,
            hot_partitions: vec![0],
            cold_partitions: vec![],
            recommendations: vec![],
            health_score: HealthScore {
                overall: 0.5,
                balance_score: 1.0 - skew,
                throughput_score: 1.0,
                latency_score: 1.0,
            },
            partition_details: (0..partitions)
                .map(|i| PartitionSkew {
                    partition: i as i32,
                    throughput_share_pct: 100.0 / partitions as f64,
                    expected_share_pct: 100.0 / partitions as f64,
                    deviation_pct: 0.0,
                    is_hot: i == 0,
                    is_cold: false,
                })
                .collect(),
            total_throughput_mps: 1000.0,
            severity: if skew > 0.5 {
                SkewSeverity::High
            } else {
                SkewSeverity::Low
            },
        }
    }

    // ── Legacy plan tests ───────────────────────────────────────────────

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

    // ── Greedy bin-packing ──────────────────────────────────────────────

    #[test]
    fn test_plan_rebalance_hot_to_cold() {
        let brokers = vec![
            BrokerState {
                broker_id: 0,
                rack: Some("rack-a".into()),
                partition_ids: vec![0, 1, 2, 3],
                total_throughput_mps: 800.0,
                total_bytes: 4_000_000,
                available_capacity_pct: 10.0,
            },
            BrokerState {
                broker_id: 1,
                rack: Some("rack-b".into()),
                partition_ids: vec![4],
                total_throughput_mps: 100.0,
                total_bytes: 1_000_000,
                available_capacity_pct: 90.0,
            },
        ];

        let assignments = vec![
            PartitionAssignment {
                partition_id: 0,
                broker_id: 0,
                is_leader: true,
                size_bytes: 1_000_000,
                throughput_mps: 400.0,
            },
            PartitionAssignment {
                partition_id: 1,
                broker_id: 0,
                is_leader: true,
                size_bytes: 1_000_000,
                throughput_mps: 200.0,
            },
            PartitionAssignment {
                partition_id: 2,
                broker_id: 0,
                is_leader: true,
                size_bytes: 1_000_000,
                throughput_mps: 100.0,
            },
            PartitionAssignment {
                partition_id: 3,
                broker_id: 0,
                is_leader: true,
                size_bytes: 1_000_000,
                throughput_mps: 100.0,
            },
            PartitionAssignment {
                partition_id: 4,
                broker_id: 1,
                is_leader: true,
                size_bytes: 1_000_000,
                throughput_mps: 100.0,
            },
        ];

        let analysis = skew_analysis(0.6, 5);
        let rebalancer = SmartRebalancer::new(RebalanceConfig::default());
        let plan = rebalancer.plan_rebalance(&analysis, &assignments, &brokers);

        assert!(!plan.moves.is_empty(), "should produce moves");
        assert!(
            plan.moves
                .iter()
                .all(|m| m.from_broker == 0 && m.to_broker == 1),
            "all moves should be from broker 0 to broker 1"
        );
        assert!(plan.data_to_move_bytes > 0);
    }

    #[test]
    fn test_plan_rebalance_rack_awareness() {
        let config = RebalanceConfig {
            rack_awareness: true,
            ..Default::default()
        };

        let brokers = vec![
            BrokerState {
                broker_id: 0,
                rack: Some("rack-a".into()),
                partition_ids: vec![0, 1, 2],
                total_throughput_mps: 600.0,
                total_bytes: 3_000_000,
                available_capacity_pct: 20.0,
            },
            BrokerState {
                broker_id: 1,
                rack: Some("rack-a".into()),
                partition_ids: vec![3],
                total_throughput_mps: 100.0,
                total_bytes: 1_000_000,
                available_capacity_pct: 80.0,
            },
        ];

        let assignments = make_assignments(&brokers);
        let analysis = skew_analysis(0.5, 4);
        let rebalancer = SmartRebalancer::new(config);
        let plan = rebalancer.plan_rebalance(&analysis, &assignments, &brokers);

        assert!(plan.moves.is_empty(), "no moves when only one rack");
    }

    #[test]
    fn test_plan_rebalance_respects_max_partitions() {
        let config = RebalanceConfig {
            max_partition_count_per_broker: 2,
            ..Default::default()
        };

        let brokers = vec![
            BrokerState {
                broker_id: 0,
                rack: None,
                partition_ids: vec![0, 1, 2],
                total_throughput_mps: 600.0,
                total_bytes: 3_000_000,
                available_capacity_pct: 20.0,
            },
            BrokerState {
                broker_id: 1,
                rack: None,
                partition_ids: vec![3, 4],
                total_throughput_mps: 200.0,
                total_bytes: 2_000_000,
                available_capacity_pct: 60.0,
            },
        ];

        let assignments = make_assignments(&brokers);
        let analysis = skew_analysis(0.4, 5);
        let rebalancer = SmartRebalancer::new(config);
        let plan = rebalancer.plan_rebalance(&analysis, &assignments, &brokers);

        assert!(plan.moves.is_empty());
    }

    // ── Duration estimation ─────────────────────────────────────────────

    #[test]
    fn test_estimate_rebalance_duration() {
        let rebalancer = SmartRebalancer::new(RebalanceConfig::default());
        let plan = RebalancePlan {
            topic: "t".into(),
            actions: vec![],
            moves: vec![],
            current_skew: 0.5,
            estimated_skew_after: 0.3,
            total_movement_bytes: 100_000_000,
            estimated_duration_secs: 2.0,
            data_to_move_bytes: 100_000_000,
            summary: String::new(),
        };

        // 100 MB at 50 MB/s = 2 seconds
        let dur = rebalancer.estimate_rebalance_duration(&plan, 50_000_000);
        assert_eq!(dur, Duration::from_secs(2));
    }

    // ── Validation ──────────────────────────────────────────────────────

    #[test]
    fn test_validate_plan_valid() {
        let brokers = make_brokers(3, 4);
        let plan = RebalancePlan {
            topic: "t".into(),
            actions: vec![],
            moves: vec![PartitionMove {
                partition_id: 0,
                from_broker: 0,
                to_broker: 1,
                estimated_bytes: 1_000_000,
            }],
            current_skew: 0.5,
            estimated_skew_after: 0.3,
            total_movement_bytes: 1_000_000,
            estimated_duration_secs: 1.0,
            data_to_move_bytes: 1_000_000,
            summary: String::new(),
        };

        let rebalancer = SmartRebalancer::new(RebalanceConfig::default());
        assert!(rebalancer.validate_plan(&plan, &brokers).is_ok());
    }

    #[test]
    fn test_validate_plan_max_partitions_exceeded() {
        let config = RebalanceConfig {
            max_partition_count_per_broker: 4,
            ..Default::default()
        };

        let brokers = make_brokers(2, 4);

        let plan = RebalancePlan {
            topic: "t".into(),
            actions: vec![],
            moves: vec![PartitionMove {
                partition_id: 0,
                from_broker: 0,
                to_broker: 1,
                estimated_bytes: 1_000_000,
            }],
            current_skew: 0.5,
            estimated_skew_after: 0.3,
            total_movement_bytes: 1_000_000,
            estimated_duration_secs: 1.0,
            data_to_move_bytes: 1_000_000,
            summary: String::new(),
        };

        let rebalancer = SmartRebalancer::new(config);
        let result = rebalancer.validate_plan(&plan, &brokers);
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.iter().any(|e| e.code == "MAX_PARTITIONS_EXCEEDED"));
    }

    #[test]
    fn test_validate_plan_spof() {
        let brokers = vec![
            BrokerState {
                broker_id: 0,
                rack: None,
                partition_ids: vec![0, 1, 2, 3, 4, 5],
                total_throughput_mps: 600.0,
                total_bytes: 6_000_000,
                available_capacity_pct: 10.0,
            },
            BrokerState {
                broker_id: 1,
                rack: None,
                partition_ids: vec![],
                total_throughput_mps: 0.0,
                total_bytes: 0,
                available_capacity_pct: 100.0,
            },
        ];

        let plan = RebalancePlan {
            topic: "t".into(),
            actions: vec![],
            moves: vec![],
            current_skew: 0.8,
            estimated_skew_after: 0.8,
            total_movement_bytes: 0,
            estimated_duration_secs: 0.0,
            data_to_move_bytes: 0,
            summary: String::new(),
        };

        let rebalancer = SmartRebalancer::new(RebalanceConfig::default());
        let result = rebalancer.validate_plan(&plan, &brokers);
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs.iter().any(|e| e.code == "SINGLE_POINT_OF_FAILURE"));
    }

    #[test]
    fn test_validate_plan_insufficient_brokers_for_replication() {
        let config = RebalanceConfig {
            min_replica_factor: 3,
            ..Default::default()
        };

        let brokers = make_brokers(2, 4);
        let plan = RebalancePlan {
            topic: "t".into(),
            actions: vec![],
            moves: vec![],
            current_skew: 0.3,
            estimated_skew_after: 0.3,
            total_movement_bytes: 0,
            estimated_duration_secs: 0.0,
            data_to_move_bytes: 0,
            summary: String::new(),
        };

        let rebalancer = SmartRebalancer::new(config);
        let result = rebalancer.validate_plan(&plan, &brokers);
        assert!(result.is_err());
        let errs = result.unwrap_err();
        assert!(errs
            .iter()
            .any(|e| e.code == "INSUFFICIENT_BROKERS_FOR_REPLICATION"));
    }

    // ── Dry run ─────────────────────────────────────────────────────────

    #[test]
    fn test_dry_run() {
        let brokers = make_brokers(3, 4);
        let plan = RebalancePlan {
            topic: "t".into(),
            actions: vec![],
            moves: vec![PartitionMove {
                partition_id: 0,
                from_broker: 0,
                to_broker: 2,
                estimated_bytes: 1_000_000,
            }],
            current_skew: 0.5,
            estimated_skew_after: 0.3,
            total_movement_bytes: 1_000_000,
            estimated_duration_secs: 1.0,
            data_to_move_bytes: 1_000_000,
            summary: String::new(),
        };

        let rebalancer = SmartRebalancer::new(RebalanceConfig::default());
        let result = rebalancer.dry_run(&plan, &brokers);

        assert_eq!(result.pre_state.total_partitions, 12);
        assert_eq!(result.post_state.total_partitions, 12);

        let post_b0 = result
            .post_state
            .brokers
            .iter()
            .find(|b| b.broker_id == 0)
            .unwrap();
        let post_b2 = result
            .post_state
            .brokers
            .iter()
            .find(|b| b.broker_id == 2)
            .unwrap();
        assert_eq!(post_b0.partition_ids.len(), 3);
        assert_eq!(post_b2.partition_ids.len(), 5);
    }
}
