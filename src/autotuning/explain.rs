//! Human-readable explanations for auto-tuning recommendations
//!
//! Provides an "explain" mode that describes tuning decisions,
//! parameter changes, and workload classifications in plain language.

use super::{AutoTuner, TunableParameters, WorkloadType};
use serde::{Deserialize, Serialize};

/// Impact level of a parameter recommendation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecommendationImpact {
    /// Large effect on performance
    High,
    /// Moderate effect on performance
    Medium,
    /// Minor effect on performance
    Low,
}

/// A single parameter change recommendation with rationale
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterRecommendation {
    /// Name of the parameter being tuned
    pub parameter_name: String,
    /// Current value in human-readable form (e.g., "16 KB", "128 MB")
    pub current_value: String,
    /// Recommended value in human-readable form
    pub recommended_value: String,
    /// Percentage change from current to recommended
    pub change_percent: f64,
    /// Why this change is recommended
    pub reason: String,
    /// Expected impact on performance
    pub impact: RecommendationImpact,
}

/// Explanation of a detected workload pattern
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadExplanation {
    /// The classified workload type
    pub workload_type: WorkloadType,
    /// What this workload pattern looks like
    pub description: String,
    /// Key characteristics of this workload
    pub characteristics: Vec<String>,
    /// What the tuner optimizes for with this workload
    pub optimization_goals: Vec<String>,
}

/// Full explain report for the current tuning state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainReport {
    /// 1-2 sentence overview of the tuning state
    pub summary: String,
    /// Description of the detected workload
    pub workload_description: String,
    /// Individual parameter recommendations
    pub recommendations: Vec<ParameterRecommendation>,
    /// Confidence in recommendations (0.0–1.0)
    pub confidence: f64,
    /// Number of data points used for analysis
    pub data_points: usize,
}

/// Generates human-readable explanations of tuning state and recommendations
pub struct TuningExplainer;

impl TuningExplainer {
    /// Explain the current tuning state of an `AutoTuner`
    pub fn explain_current(tuner: &AutoTuner) -> ExplainReport {
        let stats = tuner.stats();
        let current = tuner.current_params();
        let best = tuner.best_params();
        let workload = tuner.workload_type();

        let workload_explanation = Self::explain_workload(workload);
        let recommendations = Self::explain_recommendation(&current, &best, workload);

        let confidence = Self::calculate_confidence(stats.history_size, stats.iterations);

        let status = if stats.is_active {
            "active"
        } else {
            "inactive"
        };
        let summary = format!(
            "Auto-tuning is {} with {} data points over {} iterations. \
             Detected workload: {:?} (confidence {:.0}%).",
            status,
            stats.history_size,
            stats.iterations,
            workload,
            confidence * 100.0,
        );

        ExplainReport {
            summary,
            workload_description: workload_explanation.description,
            recommendations,
            confidence,
            data_points: stats.history_size,
        }
    }

    /// Compare two parameter sets and explain every meaningful difference
    pub fn explain_recommendation(
        current: &TunableParameters,
        recommended: &TunableParameters,
        workload: WorkloadType,
    ) -> Vec<ParameterRecommendation> {
        let mut recs = Vec::new();

        struct ParamInfo {
            name: &'static str,
            current: f64,
            recommended: f64,
            is_bytes: bool,
            is_ms: bool,
        }

        let params = vec![
            ParamInfo {
                name: "batch_size",
                current: current.batch_size as f64,
                recommended: recommended.batch_size as f64,
                is_bytes: true,
                is_ms: false,
            },
            ParamInfo {
                name: "buffer_pool_size",
                current: current.buffer_pool_size as f64,
                recommended: recommended.buffer_pool_size as f64,
                is_bytes: true,
                is_ms: false,
            },
            ParamInfo {
                name: "io_threads",
                current: current.io_threads as f64,
                recommended: recommended.io_threads as f64,
                is_bytes: false,
                is_ms: false,
            },
            ParamInfo {
                name: "flush_interval_ms",
                current: current.flush_interval_ms as f64,
                recommended: recommended.flush_interval_ms as f64,
                is_bytes: false,
                is_ms: true,
            },
            ParamInfo {
                name: "compression_level",
                current: current.compression_level as f64,
                recommended: recommended.compression_level as f64,
                is_bytes: false,
                is_ms: false,
            },
            ParamInfo {
                name: "cache_size",
                current: current.cache_size as f64,
                recommended: recommended.cache_size as f64,
                is_bytes: true,
                is_ms: false,
            },
            ParamInfo {
                name: "max_connections",
                current: current.max_connections as f64,
                recommended: recommended.max_connections as f64,
                is_bytes: false,
                is_ms: false,
            },
            ParamInfo {
                name: "queue_depth",
                current: current.queue_depth as f64,
                recommended: recommended.queue_depth as f64,
                is_bytes: false,
                is_ms: false,
            },
            ParamInfo {
                name: "prefetch_size",
                current: current.prefetch_size as f64,
                recommended: recommended.prefetch_size as f64,
                is_bytes: true,
                is_ms: false,
            },
        ];

        for p in &params {
            if (p.current - p.recommended).abs() < 1e-6 {
                continue;
            }

            let change_percent = if p.current.abs() > 1e-10 {
                ((p.recommended - p.current) / p.current) * 100.0
            } else {
                0.0
            };

            let current_str = if p.is_bytes {
                format_bytes(p.current as usize)
            } else if p.is_ms {
                format!("{} ms", p.current as u64)
            } else {
                format!("{}", p.current as u64)
            };

            let recommended_str = if p.is_bytes {
                format_bytes(p.recommended as usize)
            } else if p.is_ms {
                format!("{} ms", p.recommended as u64)
            } else {
                format!("{}", p.recommended as u64)
            };

            let reason = Self::reason_for_change(p.name, change_percent, workload);
            let impact = Self::assess_impact(p.name, change_percent);

            recs.push(ParameterRecommendation {
                parameter_name: p.name.to_string(),
                current_value: current_str,
                recommended_value: recommended_str,
                change_percent,
                reason,
                impact,
            });
        }

        recs
    }

    /// Explain what a workload type means in plain language
    pub fn explain_workload(workload: WorkloadType) -> WorkloadExplanation {
        match workload {
            WorkloadType::WriteHeavy => WorkloadExplanation {
                workload_type: workload,
                description: "The system is handling predominantly write operations with high ingestion rates.".into(),
                characteristics: vec![
                    "High producer throughput".into(),
                    "Low cache hit rate".into(),
                    "Frequent disk flushes".into(),
                ],
                optimization_goals: vec![
                    "Maximize write throughput".into(),
                    "Batch writes for efficiency".into(),
                    "Minimize flush overhead".into(),
                ],
            },
            WorkloadType::ReadHeavy => WorkloadExplanation {
                workload_type: workload,
                description: "The system is serving mostly read/consume operations with data frequently re-read.".into(),
                characteristics: vec![
                    "High consumer throughput".into(),
                    "High cache hit rate".into(),
                    "Large prefetch benefit".into(),
                ],
                optimization_goals: vec![
                    "Maximize cache utilization".into(),
                    "Increase prefetch size".into(),
                    "Support many concurrent consumers".into(),
                ],
            },
            WorkloadType::Balanced => WorkloadExplanation {
                workload_type: workload,
                description: "The system has a balanced mix of read and write operations.".into(),
                characteristics: vec![
                    "Even producer/consumer ratio".into(),
                    "Moderate cache hit rate".into(),
                    "Steady resource usage".into(),
                ],
                optimization_goals: vec![
                    "Balance read and write performance".into(),
                    "Maintain moderate resource allocation".into(),
                    "Avoid over-optimizing for one direction".into(),
                ],
            },
            WorkloadType::LatencySensitive => WorkloadExplanation {
                workload_type: workload,
                description: "The system serves latency-critical requests where response time matters most.".into(),
                characteristics: vec![
                    "Low average latency".into(),
                    "Tight P99 requirements".into(),
                    "Low error rate".into(),
                ],
                optimization_goals: vec![
                    "Minimize end-to-end latency".into(),
                    "Reduce batching delays".into(),
                    "Disable or minimize compression overhead".into(),
                ],
            },
            WorkloadType::ThroughputOriented => WorkloadExplanation {
                workload_type: workload,
                description: "The system prioritizes maximum data throughput over individual request latency.".into(),
                characteristics: vec![
                    "Very high record rates".into(),
                    "Large batch sizes are beneficial".into(),
                    "High I/O parallelism".into(),
                ],
                optimization_goals: vec![
                    "Maximize records per second".into(),
                    "Use aggressive batching and compression".into(),
                    "Increase I/O thread count".into(),
                ],
            },
            WorkloadType::Bursty => WorkloadExplanation {
                workload_type: workload,
                description: "The system experiences highly variable load with intermittent traffic spikes.".into(),
                characteristics: vec![
                    "High throughput variance".into(),
                    "Unpredictable traffic patterns".into(),
                    "Periodic idle periods".into(),
                ],
                optimization_goals: vec![
                    "Handle sudden traffic spikes".into(),
                    "Maintain headroom for bursts".into(),
                    "Use moderate defaults to avoid over-provisioning".into(),
                ],
            },
            WorkloadType::Unknown => WorkloadExplanation {
                workload_type: workload,
                description: "Not enough data to classify the workload pattern yet.".into(),
                characteristics: vec![
                    "Insufficient metrics collected".into(),
                    "Workload pattern still emerging".into(),
                ],
                optimization_goals: vec![
                    "Gather more performance data".into(),
                    "Use safe default parameters".into(),
                ],
            },
        }
    }

    /// Compute a confidence value based on available data
    fn calculate_confidence(data_points: usize, iterations: u64) -> f64 {
        // More data and iterations → higher confidence, capped at 1.0
        let data_factor = (data_points as f64 / 500.0).min(1.0);
        let iter_factor = (iterations as f64 / 50.0).min(1.0);
        (data_factor * 0.7 + iter_factor * 0.3).min(1.0)
    }

    /// Generate a human-readable reason for a parameter change
    fn reason_for_change(param: &str, change_percent: f64, workload: WorkloadType) -> String {
        let direction = if change_percent > 0.0 {
            "Increasing"
        } else {
            "Decreasing"
        };

        match param {
            "batch_size" => format!(
                "{direction} batch size to better match {:?} workload throughput requirements.",
                workload,
            ),
            "buffer_pool_size" => format!(
                "{direction} buffer pool to improve write buffering for {:?} workload.",
                workload,
            ),
            "io_threads" => format!(
                "{direction} I/O threads to match the concurrency demands of {:?} workload.",
                workload,
            ),
            "flush_interval_ms" => {
                if change_percent > 0.0 {
                    format!(
                        "Relaxing flush interval to batch more writes for {:?} workload.",
                        workload,
                    )
                } else {
                    format!(
                        "Tightening flush interval to reduce latency for {:?} workload.",
                        workload,
                    )
                }
            }
            "compression_level" => format!(
                "{direction} compression level to balance CPU usage and I/O for {:?} workload.",
                workload,
            ),
            "cache_size" => format!(
                "{direction} cache size to improve hit rate for {:?} workload.",
                workload,
            ),
            "max_connections" => format!(
                "{direction} connection limit to handle {:?} workload concurrency.",
                workload,
            ),
            "queue_depth" => format!(
                "{direction} queue depth to manage request buffering for {:?} workload.",
                workload,
            ),
            "prefetch_size" => format!(
                "{direction} prefetch size to optimize sequential read performance for {:?} workload.",
                workload,
            ),
            _ => format!(
                "{direction} {param} for {:?} workload optimization.",
                workload,
            ),
        }
    }

    /// Assess the impact level of a parameter change
    fn assess_impact(param: &str, change_percent: f64) -> RecommendationImpact {
        let abs_change = change_percent.abs();

        // High-impact parameters with large changes
        if matches!(param, "cache_size" | "batch_size" | "io_threads") && abs_change > 50.0 {
            return RecommendationImpact::High;
        }

        if abs_change > 100.0 {
            RecommendationImpact::High
        } else if abs_change > 25.0 {
            RecommendationImpact::Medium
        } else {
            RecommendationImpact::Low
        }
    }
}

/// Format a byte count into a human-readable string (e.g., "16 KB", "128 MB")
pub fn format_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = 1024 * KB;
    const GB: usize = 1024 * MB;

    if bytes >= GB {
        let value = bytes as f64 / GB as f64;
        if (value - value.round()).abs() < 0.01 {
            format!("{} GB", value.round() as usize)
        } else {
            format!("{:.1} GB", value)
        }
    } else if bytes >= MB {
        let value = bytes as f64 / MB as f64;
        if (value - value.round()).abs() < 0.01 {
            format!("{} MB", value.round() as usize)
        } else {
            format!("{:.1} MB", value)
        }
    } else if bytes >= KB {
        let value = bytes as f64 / KB as f64;
        if (value - value.round()).abs() < 0.01 {
            format!("{} KB", value.round() as usize)
        } else {
            format!("{:.1} KB", value)
        }
    } else {
        format!("{} B", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::autotuning::AutoTuningConfig;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512 B");
        assert_eq!(format_bytes(1024), "1 KB");
        assert_eq!(format_bytes(16384), "16 KB");
        assert_eq!(format_bytes(64 * 1024 * 1024), "64 MB");
        assert_eq!(format_bytes(128 * 1024 * 1024), "128 MB");
        assert_eq!(format_bytes(2 * 1024 * 1024 * 1024), "2 GB");
    }

    #[test]
    fn test_explain_workload_all_variants() {
        let variants = [
            WorkloadType::WriteHeavy,
            WorkloadType::ReadHeavy,
            WorkloadType::Balanced,
            WorkloadType::LatencySensitive,
            WorkloadType::ThroughputOriented,
            WorkloadType::Bursty,
            WorkloadType::Unknown,
        ];

        for wt in &variants {
            let explanation = TuningExplainer::explain_workload(*wt);
            assert_eq!(explanation.workload_type, *wt);
            assert!(!explanation.description.is_empty());
            assert!(!explanation.characteristics.is_empty());
            assert!(!explanation.optimization_goals.is_empty());
        }
    }

    #[test]
    fn test_explain_recommendation_detects_changes() {
        let current = TunableParameters::default();
        let recommended = WorkloadType::WriteHeavy.recommended_params();

        let recs = TuningExplainer::explain_recommendation(
            &current,
            &recommended,
            WorkloadType::WriteHeavy,
        );

        assert!(
            !recs.is_empty(),
            "Should produce recommendations when params differ"
        );

        for rec in &recs {
            assert!(!rec.parameter_name.is_empty());
            assert!(!rec.current_value.is_empty());
            assert!(!rec.recommended_value.is_empty());
            assert!(!rec.reason.is_empty());
        }
    }

    #[test]
    fn test_explain_recommendation_no_changes() {
        let params = TunableParameters::default();
        let recs =
            TuningExplainer::explain_recommendation(&params, &params, WorkloadType::Balanced);

        assert!(
            recs.is_empty(),
            "Identical params should produce no recommendations"
        );
    }

    #[test]
    fn test_explain_current_report() {
        let config = AutoTuningConfig::default();
        let tuner = AutoTuner::new(config);
        tuner.start();

        let report = TuningExplainer::explain_current(&tuner);

        assert!(!report.summary.is_empty());
        assert!(!report.workload_description.is_empty());
        assert!(report.confidence >= 0.0 && report.confidence <= 1.0);
    }

    #[test]
    fn test_recommendation_impact_levels() {
        let current = TunableParameters {
            cache_size: 32 * 1024 * 1024,
            ..TunableParameters::default()
        };
        let recommended = TunableParameters {
            cache_size: 512 * 1024 * 1024,
            ..TunableParameters::default()
        };

        let recs = TuningExplainer::explain_recommendation(
            &current,
            &recommended,
            WorkloadType::ReadHeavy,
        );

        let cache_rec = recs
            .iter()
            .find(|r| r.parameter_name == "cache_size")
            .unwrap();
        assert_eq!(cache_rec.impact, RecommendationImpact::High);
        assert!(cache_rec.change_percent > 100.0);
    }
}
