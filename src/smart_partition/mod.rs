//! Smart Partitioning & Auto-Rebalancing
//!
//! ML-driven partition management that monitors throughput skew,
//! consumer lag, and message key distribution, then automatically
//! rebalances partitions to eliminate hot spots.

pub mod analyzer;
pub mod rebalancer;

pub use analyzer::{PartitionMetrics, SkewAnalyzer, SkewAnalyzerConfig, SkewReport, SkewSeverity};
pub use rebalancer::{RebalanceConfig, RebalanceMode, RebalancePlan, SmartRebalancer};
