//! Smart Partitioning & Auto-Rebalancing
//!
//! ML-driven partition management that monitors throughput skew,
//! consumer lag, and message key distribution, then automatically
//! rebalances partitions to eliminate hot spots.

pub mod analyzer;
pub mod rebalancer;

pub use analyzer::{
    PartitionMetrics, SkewAnalysis, SkewAnalyzer, SkewAnalyzerConfig, SkewReport, SkewSeverity,
};
pub use rebalancer::{
    BrokerState, ClusterState, DryRunResult, PartitionAssignment, RebalanceConfig, RebalanceMode,
    RebalancePlan, SmartRebalancer, ValidationError,
};

use serde::{Deserialize, Serialize};

// ── Coordinator ─────────────────────────────────────────────────────────

/// Configuration for the `SmartPartitionManager` coordinator.
#[derive(Debug, Clone)]
pub struct SmartPartitionConfig {
    /// How often to run analysis (seconds)
    pub analysis_interval_secs: u64,
    /// Automatically apply rebalance plans when skew exceeds threshold
    pub auto_rebalance: bool,
    /// Gini skew threshold that triggers action recommendations
    pub skew_threshold: f64,
    /// Minimum time between rebalances (seconds)
    pub cooldown_period_secs: u64,
    /// Underlying analyzer configuration
    pub analyzer_config: SkewAnalyzerConfig,
    /// Underlying rebalancer configuration
    pub rebalancer_config: RebalanceConfig,
}

impl Default for SmartPartitionConfig {
    fn default() -> Self {
        Self {
            analysis_interval_secs: 60,
            auto_rebalance: false,
            skew_threshold: 0.3,
            cooldown_period_secs: 300,
            analyzer_config: SkewAnalyzerConfig::default(),
            rebalancer_config: RebalanceConfig::default(),
        }
    }
}

/// High-level partition management actions.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionAction {
    /// Split a hot partition into two
    Split { topic: String, partition: i32 },
    /// Merge two cold partitions
    Merge { topic: String, from: i32, into: i32 },
    /// Move a partition between brokers
    Move {
        topic: String,
        partition: i32,
        from_broker: i32,
        to_broker: i32,
    },
    /// Add partitions to a topic
    AddPartitions { topic: String, count: u32 },
}

/// Coordinator that periodically analyses partition metrics and optionally
/// triggers automatic rebalancing.
pub struct SmartPartitionManager {
    analyzer: SkewAnalyzer,
    rebalancer: SmartRebalancer,
    config: SmartPartitionConfig,
}

impl SmartPartitionManager {
    /// Create a new manager with the given configuration.
    pub fn new(config: SmartPartitionConfig) -> Self {
        let analyzer = SkewAnalyzer::new(config.analyzer_config.clone());
        let rebalancer = SmartRebalancer::new(config.rebalancer_config.clone());
        Self {
            analyzer,
            rebalancer,
            config,
        }
    }

    /// Ingest a metrics sample for a topic.
    pub fn record_sample(&mut self, topic: &str, metrics: Vec<PartitionMetrics>) {
        self.analyzer.record_sample(topic, metrics);
    }

    /// Run a deep analysis on the latest sample for a topic.
    pub fn run_analysis(&self, topic: &str) -> Option<SkewAnalysis> {
        self.analyzer.analyze_deep(topic)
    }

    /// Suggest concrete partition actions based on the latest analysis.
    pub fn suggest_actions(&self, analysis: &SkewAnalysis) -> Vec<PartitionAction> {
        let mut actions = Vec::new();

        if analysis.skew_coefficient < self.config.skew_threshold {
            return actions;
        }

        // Hot partitions: split if extremely hot, otherwise move
        for &pid in &analysis.hot_partitions {
            let detail = analysis
                .partition_details
                .iter()
                .find(|d| d.partition == pid);

            if let Some(d) = detail {
                if d.throughput_share_pct > 50.0 {
                    actions.push(PartitionAction::Split {
                        topic: analysis.topic.clone(),
                        partition: pid,
                    });
                } else {
                    actions.push(PartitionAction::Move {
                        topic: analysis.topic.clone(),
                        partition: pid,
                        from_broker: -1, // to be resolved by caller
                        to_broker: -1,
                    });
                }
            }
        }

        // Cold partitions: merge pairs
        let cold = &analysis.cold_partitions;
        for pair in cold.windows(2) {
            actions.push(PartitionAction::Merge {
                topic: analysis.topic.clone(),
                from: pair[1],
                into: pair[0],
            });
        }

        // Many hot partitions: also suggest adding partitions
        if analysis.hot_partitions.len() >= 2 {
            let additional = (analysis.partition_details.len() as f64 * 0.5).ceil() as u32;
            actions.push(PartitionAction::AddPartitions {
                topic: analysis.topic.clone(),
                count: additional.max(1),
            });
        }

        actions
    }

    /// Whether auto-rebalance is enabled.
    pub fn auto_rebalance_enabled(&self) -> bool {
        self.config.auto_rebalance
    }

    /// Access the underlying analyzer.
    pub fn analyzer(&self) -> &SkewAnalyzer {
        &self.analyzer
    }

    /// Access the underlying rebalancer.
    pub fn rebalancer(&self) -> &SmartRebalancer {
        &self.rebalancer
    }

    /// Get the manager configuration.
    pub fn config(&self) -> &SmartPartitionConfig {
        &self.config
    }
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_metrics(topic: &str, rates: &[f64]) -> Vec<PartitionMetrics> {
        rates
            .iter()
            .enumerate()
            .map(|(i, &tp)| PartitionMetrics {
                topic: topic.to_string(),
                partition: i as i32,
                throughput_mps: tp,
                throughput_bps: tp * 100.0,
                consumer_lag: 0,
                unique_keys: 50,
                size_bytes: 0,
                hot_key_ratio: 0.0,
                p99_latency_ms: 5.0,
            })
            .collect()
    }

    #[test]
    fn test_manager_run_analysis() {
        let mut mgr = SmartPartitionManager::new(SmartPartitionConfig::default());
        mgr.record_sample("topic-a", sample_metrics("topic-a", &[100.0, 100.0, 100.0]));

        let analysis = mgr.run_analysis("topic-a").unwrap();
        assert_eq!(analysis.severity, SkewSeverity::None);
    }

    #[test]
    fn test_manager_suggest_actions_balanced() {
        let mut mgr = SmartPartitionManager::new(SmartPartitionConfig::default());
        mgr.record_sample("t", sample_metrics("t", &[100.0, 100.0, 100.0, 100.0]));

        let analysis = mgr.run_analysis("t").unwrap();
        let actions = mgr.suggest_actions(&analysis);
        assert!(
            actions.is_empty(),
            "balanced topic should produce no actions"
        );
    }

    #[test]
    fn test_manager_suggest_actions_skewed() {
        let mut mgr = SmartPartitionManager::new(SmartPartitionConfig {
            skew_threshold: 0.1,
            ..SmartPartitionConfig::default()
        });
        mgr.record_sample("t", sample_metrics("t", &[900.0, 30.0, 20.0, 10.0]));

        let analysis = mgr.run_analysis("t").unwrap();
        let actions = mgr.suggest_actions(&analysis);
        assert!(!actions.is_empty(), "skewed topic should produce actions");

        assert!(actions.iter().any(|a| matches!(
            a,
            PartitionAction::Split { .. } | PartitionAction::Move { .. }
        )));
    }

    #[test]
    fn test_manager_suggest_actions_cold_merge() {
        let mut mgr = SmartPartitionManager::new(SmartPartitionConfig {
            skew_threshold: 0.1,
            ..SmartPartitionConfig::default()
        });
        mgr.record_sample("t", sample_metrics("t", &[500.0, 5.0, 3.0, 2.0]));

        let analysis = mgr.run_analysis("t").unwrap();
        let actions = mgr.suggest_actions(&analysis);

        let merges: Vec<_> = actions
            .iter()
            .filter(|a| matches!(a, PartitionAction::Merge { .. }))
            .collect();
        assert!(
            !merges.is_empty(),
            "cold partitions should trigger merge suggestions"
        );
    }
}
