//! Benchmark report generator for comparative analysis
//!
//! Generates structured benchmark reports comparing Streamline against
//! competing streaming platforms. Supports markdown and JSON output formats.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Category of benchmark test
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkCategory {
    Throughput,
    Latency,
    ResourceUsage,
    Startup,
    EndToEnd,
}

impl fmt::Display for BenchmarkCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Throughput => write!(f, "Throughput"),
            Self::Latency => write!(f, "Latency"),
            Self::ResourceUsage => write!(f, "Resource Usage"),
            Self::Startup => write!(f, "Startup"),
            Self::EndToEnd => write!(f, "End-to-End"),
        }
    }
}

/// System information captured at report generation time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    pub os: String,
    pub arch: String,
    pub cpus: usize,
    pub memory_gb: f64,
    pub rust_version: String,
}

impl SystemInfo {
    /// Auto-detect system information from the current environment.
    pub fn detect() -> Self {
        let cpus = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);

        let memory_gb = Self::detect_memory_gb();

        Self {
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            cpus,
            memory_gb,
            rust_version: env!("CARGO_PKG_RUST_VERSION").to_string(),
        }
    }

    /// Attempt to detect system memory. Falls back to 0.0 if unavailable.
    fn detect_memory_gb() -> f64 {
        #[cfg(target_os = "linux")]
        {
            if let Ok(contents) = std::fs::read_to_string("/proc/meminfo") {
                for line in contents.lines() {
                    if let Some(rest) = line.strip_prefix("MemTotal:") {
                        let kb_str = rest.trim().trim_end_matches("kB").trim();
                        if let Ok(kb) = kb_str.parse::<u64>() {
                            return kb as f64 / 1_048_576.0;
                        }
                    }
                }
            }
            0.0
        }
        #[cfg(target_os = "macos")]
        {
            // Use sysctl on macOS
            let output = std::process::Command::new("sysctl")
                .arg("-n")
                .arg("hw.memsize")
                .output();
            if let Ok(output) = output {
                if let Ok(s) = String::from_utf8(output.stdout) {
                    if let Ok(bytes) = s.trim().parse::<u64>() {
                        return bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                    }
                }
            }
            0.0
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            0.0
        }
    }
}

/// A single platform's metric for a benchmark comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlatformMetric {
    pub platform: String,
    pub value: f64,
    pub unit: String,
    pub is_best: bool,
}

impl PlatformMetric {
    pub fn new(platform: impl Into<String>, value: f64, unit: impl Into<String>) -> Self {
        Self {
            platform: platform.into(),
            value,
            unit: unit.into(),
            is_best: false,
        }
    }
}

/// A single benchmark comparison across platforms
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkComparison {
    pub test_name: String,
    pub category: BenchmarkCategory,
    pub metrics: Vec<PlatformMetric>,
}

/// Summary statistics for the report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportSummary {
    pub total_tests: usize,
    pub streamline_wins: usize,
    pub streamline_win_rate: f64,
    pub key_advantages: Vec<String>,
    pub key_disadvantages: Vec<String>,
}

/// A complete benchmark report comparing Streamline against competitors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub title: String,
    pub timestamp: String,
    pub system_info: SystemInfo,
    pub results: Vec<BenchmarkComparison>,
    pub summary: ReportSummary,
}

impl BenchmarkReport {
    /// Create a new report with auto-detected system info.
    pub fn new(title: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            system_info: SystemInfo::detect(),
            results: Vec::new(),
            summary: ReportSummary {
                total_tests: 0,
                streamline_wins: 0,
                streamline_win_rate: 0.0,
                key_advantages: Vec::new(),
                key_disadvantages: Vec::new(),
            },
        }
    }

    /// Add a benchmark comparison to the report.
    pub fn add_comparison(
        &mut self,
        test_name: impl Into<String>,
        category: BenchmarkCategory,
        metrics: Vec<PlatformMetric>,
    ) {
        self.results.push(BenchmarkComparison {
            test_name: test_name.into(),
            category,
            metrics,
        });
    }

    /// Compute summary statistics from the current results.
    ///
    /// For each comparison, determines the "best" metric using category-specific
    /// logic: higher is better for throughput, lower is better for latency/startup/
    /// resource usage/end-to-end. Marks `is_best` on winning metrics and tallies
    /// Streamline wins.
    pub fn generate_summary(&mut self) {
        let total = self.results.len();
        let mut wins = 0usize;
        let mut advantages: Vec<String> = Vec::new();
        let mut disadvantages: Vec<String> = Vec::new();

        for comparison in &mut self.results {
            if comparison.metrics.is_empty() {
                continue;
            }

            let higher_is_better = matches!(comparison.category, BenchmarkCategory::Throughput);

            // Find the best value
            let best_value = if higher_is_better {
                comparison
                    .metrics
                    .iter()
                    .map(|m| m.value)
                    .fold(f64::NEG_INFINITY, f64::max)
            } else {
                comparison
                    .metrics
                    .iter()
                    .map(|m| m.value)
                    .fold(f64::INFINITY, f64::min)
            };

            // Mark the best metric(s) and check if Streamline won
            let mut streamline_won = false;
            for metric in &mut comparison.metrics {
                metric.is_best = (metric.value - best_value).abs() < f64::EPSILON;
                if metric.is_best && metric.platform == "Streamline" {
                    streamline_won = true;
                }
            }

            if streamline_won {
                wins += 1;
                advantages.push(format!(
                    "Best {} in {}",
                    comparison.category, comparison.test_name
                ));
            } else if comparison
                .metrics
                .iter()
                .any(|m| m.platform == "Streamline")
            {
                disadvantages.push(format!(
                    "Not leading in {} ({})",
                    comparison.test_name, comparison.category
                ));
            }
        }

        let win_rate = if total > 0 {
            wins as f64 / total as f64
        } else {
            0.0
        };

        self.summary = ReportSummary {
            total_tests: total,
            streamline_wins: wins,
            streamline_win_rate: win_rate,
            key_advantages: advantages,
            key_disadvantages: disadvantages,
        };
    }

    /// Format a slice of comparisons as an ASCII table.
    pub fn format_table(comparisons: &[BenchmarkComparison]) -> String {
        if comparisons.is_empty() {
            return String::from("(no comparisons)");
        }

        // Collect all platforms across all comparisons
        let mut platforms: Vec<String> = Vec::new();
        for comp in comparisons {
            for m in &comp.metrics {
                if !platforms.contains(&m.platform) {
                    platforms.push(m.platform.clone());
                }
            }
        }

        // Column widths: test name + one per platform
        let test_col_width = comparisons
            .iter()
            .map(|c| c.test_name.len())
            .max()
            .unwrap_or(4)
            .max(4);
        let platform_col_width = 18;

        let mut out = String::new();

        // Header
        out.push_str(&format!("| {:<width$} |", "Test", width = test_col_width));
        for p in &platforms {
            out.push_str(&format!(" {:<width$} |", p, width = platform_col_width));
        }
        out.push('\n');

        // Separator
        out.push_str(&format!("|{:-<width$}--|", "", width = test_col_width));
        for _ in &platforms {
            out.push_str(&format!("{:-<width$}--|", "", width = platform_col_width));
        }
        out.push('\n');

        // Rows
        for comp in comparisons {
            out.push_str(&format!(
                "| {:<width$} |",
                comp.test_name,
                width = test_col_width
            ));
            for p in &platforms {
                let cell = comp
                    .metrics
                    .iter()
                    .find(|m| m.platform == *p)
                    .map(|m| {
                        let val = format!("{:.2} {}", m.value, m.unit);
                        if m.is_best {
                            format!("{}*", val)
                        } else {
                            val
                        }
                    })
                    .unwrap_or_else(|| "-".to_string());
                out.push_str(&format!(" {:<width$} |", cell, width = platform_col_width));
            }
            out.push('\n');
        }

        out
    }

    /// Generate a full markdown report.
    pub fn to_markdown(&self) -> String {
        let mut md = String::new();

        md.push_str(&format!("# {}\n\n", self.title));
        md.push_str(&format!("**Generated:** {}\n\n", self.timestamp));

        // System info
        md.push_str("## System Information\n\n");
        md.push_str(&format!("- **OS:** {}\n", self.system_info.os));
        md.push_str(&format!("- **Architecture:** {}\n", self.system_info.arch));
        md.push_str(&format!("- **CPUs:** {}\n", self.system_info.cpus));
        md.push_str(&format!(
            "- **Memory:** {:.1} GB\n",
            self.system_info.memory_gb
        ));
        md.push_str(&format!(
            "- **Rust Version:** {}\n\n",
            self.system_info.rust_version
        ));

        // Group results by category
        let categories = [
            BenchmarkCategory::Startup,
            BenchmarkCategory::Throughput,
            BenchmarkCategory::Latency,
            BenchmarkCategory::ResourceUsage,
            BenchmarkCategory::EndToEnd,
        ];

        md.push_str("## Results\n\n");

        for cat in &categories {
            let comparisons: Vec<&BenchmarkComparison> =
                self.results.iter().filter(|r| r.category == *cat).collect();

            if comparisons.is_empty() {
                continue;
            }

            md.push_str(&format!("### {}\n\n", cat));

            // Build a table for this category
            let owned: Vec<BenchmarkComparison> = comparisons.into_iter().cloned().collect();
            md.push_str(&Self::format_table(&owned));
            md.push('\n');
        }

        // Summary
        md.push_str("## Summary\n\n");
        md.push_str(&format!(
            "- **Total Tests:** {}\n",
            self.summary.total_tests
        ));
        md.push_str(&format!(
            "- **Streamline Wins:** {}\n",
            self.summary.streamline_wins
        ));
        md.push_str(&format!(
            "- **Win Rate:** {:.1}%\n\n",
            self.summary.streamline_win_rate * 100.0
        ));

        if !self.summary.key_advantages.is_empty() {
            md.push_str("### Key Advantages\n\n");
            for adv in &self.summary.key_advantages {
                md.push_str(&format!("- ✅ {}\n", adv));
            }
            md.push('\n');
        }

        if !self.summary.key_disadvantages.is_empty() {
            md.push_str("### Areas for Improvement\n\n");
            for dis in &self.summary.key_disadvantages {
                md.push_str(&format!("- ⚠️ {}\n", dis));
            }
            md.push('\n');
        }

        md.push_str("---\n");
        md.push_str("*Report generated by Streamline Benchmark Suite*\n");

        md
    }

    /// Generate a JSON representation of the report.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self)
            .unwrap_or_else(|_| serde_json::json!({"error": "serialization failed"}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_report() -> BenchmarkReport {
        let mut report = BenchmarkReport {
            title: "Test Report".to_string(),
            timestamp: "2025-01-01T00:00:00Z".to_string(),
            system_info: SystemInfo {
                os: "linux".to_string(),
                arch: "x86_64".to_string(),
                cpus: 8,
                memory_gb: 16.0,
                rust_version: "1.80".to_string(),
            },
            results: Vec::new(),
            summary: ReportSummary {
                total_tests: 0,
                streamline_wins: 0,
                streamline_win_rate: 0.0,
                key_advantages: Vec::new(),
                key_disadvantages: Vec::new(),
            },
        };

        report.add_comparison(
            "cold_start",
            BenchmarkCategory::Startup,
            vec![
                PlatformMetric::new("Streamline", 0.05, "seconds"),
                PlatformMetric::new("Apache Kafka", 8.5, "seconds"),
                PlatformMetric::new("Redpanda", 1.2, "seconds"),
            ],
        );

        report.add_comparison(
            "throughput_1kb",
            BenchmarkCategory::Throughput,
            vec![
                PlatformMetric::new("Streamline", 950_000.0, "msg/s"),
                PlatformMetric::new("Apache Kafka", 1_200_000.0, "msg/s"),
                PlatformMetric::new("Redpanda", 1_100_000.0, "msg/s"),
            ],
        );

        report.add_comparison(
            "p99_latency",
            BenchmarkCategory::Latency,
            vec![
                PlatformMetric::new("Streamline", 0.8, "ms"),
                PlatformMetric::new("Apache Kafka", 5.2, "ms"),
                PlatformMetric::new("Redpanda", 1.5, "ms"),
            ],
        );

        report.add_comparison(
            "memory_idle",
            BenchmarkCategory::ResourceUsage,
            vec![
                PlatformMetric::new("Streamline", 12.0, "MB"),
                PlatformMetric::new("Apache Kafka", 512.0, "MB"),
                PlatformMetric::new("Redpanda", 256.0, "MB"),
            ],
        );

        report
    }

    #[test]
    fn test_report_creation() {
        let report = BenchmarkReport::new("My Benchmark");
        assert_eq!(report.title, "My Benchmark");
        assert!(report.results.is_empty());
        assert!(!report.timestamp.is_empty());
    }

    #[test]
    fn test_system_info_detect() {
        let info = SystemInfo::detect();
        assert!(!info.os.is_empty());
        assert!(!info.arch.is_empty());
        assert!(info.cpus >= 1);
    }

    #[test]
    fn test_add_comparison() {
        let mut report = BenchmarkReport::new("Test");
        report.add_comparison(
            "test_bench",
            BenchmarkCategory::Throughput,
            vec![PlatformMetric::new("Streamline", 100.0, "msg/s")],
        );
        assert_eq!(report.results.len(), 1);
        assert_eq!(report.results[0].test_name, "test_bench");
        assert_eq!(report.results[0].metrics.len(), 1);
    }

    #[test]
    fn test_generate_summary_throughput_win() {
        let mut report = BenchmarkReport::new("Test");
        report.add_comparison(
            "throughput_test",
            BenchmarkCategory::Throughput,
            vec![
                PlatformMetric::new("Streamline", 500_000.0, "msg/s"),
                PlatformMetric::new("Apache Kafka", 300_000.0, "msg/s"),
            ],
        );
        report.generate_summary();

        assert_eq!(report.summary.total_tests, 1);
        assert_eq!(report.summary.streamline_wins, 1);
        assert!((report.summary.streamline_win_rate - 1.0).abs() < f64::EPSILON);
        assert!(report.results[0].metrics[0].is_best); // Streamline
        assert!(!report.results[0].metrics[1].is_best); // Kafka
    }

    #[test]
    fn test_generate_summary_latency_win() {
        let mut report = BenchmarkReport::new("Test");
        // Lower latency is better
        report.add_comparison(
            "latency_test",
            BenchmarkCategory::Latency,
            vec![
                PlatformMetric::new("Streamline", 0.5, "ms"),
                PlatformMetric::new("Apache Kafka", 3.0, "ms"),
            ],
        );
        report.generate_summary();

        assert_eq!(report.summary.streamline_wins, 1);
        assert!(report.results[0].metrics[0].is_best);
    }

    #[test]
    fn test_generate_summary_loss() {
        let mut report = BenchmarkReport::new("Test");
        // Higher throughput is better — Kafka wins
        report.add_comparison(
            "throughput_test",
            BenchmarkCategory::Throughput,
            vec![
                PlatformMetric::new("Streamline", 300_000.0, "msg/s"),
                PlatformMetric::new("Apache Kafka", 500_000.0, "msg/s"),
            ],
        );
        report.generate_summary();

        assert_eq!(report.summary.streamline_wins, 0);
        assert!((report.summary.streamline_win_rate).abs() < f64::EPSILON);
        assert!(!report.summary.key_disadvantages.is_empty());
    }

    #[test]
    fn test_generate_summary_mixed() {
        let mut report = sample_report();
        report.generate_summary();

        assert_eq!(report.summary.total_tests, 4);
        // Streamline wins: cold_start (lower), p99_latency (lower), memory_idle (lower)
        // Streamline loses: throughput_1kb (higher is better, Kafka has 1.2M)
        assert_eq!(report.summary.streamline_wins, 3);
        assert!((report.summary.streamline_win_rate - 0.75).abs() < f64::EPSILON);
    }

    #[test]
    fn test_generate_summary_empty() {
        let mut report = BenchmarkReport::new("Empty");
        report.generate_summary();

        assert_eq!(report.summary.total_tests, 0);
        assert_eq!(report.summary.streamline_wins, 0);
        assert!((report.summary.streamline_win_rate).abs() < f64::EPSILON);
    }

    #[test]
    fn test_format_table_empty() {
        let table = BenchmarkReport::format_table(&[]);
        assert_eq!(table, "(no comparisons)");
    }

    #[test]
    fn test_format_table_content() {
        let comparisons = vec![BenchmarkComparison {
            test_name: "cold_start".to_string(),
            category: BenchmarkCategory::Startup,
            metrics: vec![
                PlatformMetric {
                    platform: "Streamline".to_string(),
                    value: 0.05,
                    unit: "seconds".to_string(),
                    is_best: true,
                },
                PlatformMetric {
                    platform: "Kafka".to_string(),
                    value: 8.5,
                    unit: "seconds".to_string(),
                    is_best: false,
                },
            ],
        }];

        let table = BenchmarkReport::format_table(&comparisons);
        assert!(table.contains("cold_start"));
        assert!(table.contains("Streamline"));
        assert!(table.contains("Kafka"));
        assert!(table.contains("0.05 seconds*")); // best marker
        assert!(table.contains("8.50 seconds"));
        // Should NOT have a star on non-best
        assert!(!table.contains("8.50 seconds*"));
    }

    #[test]
    fn test_to_markdown() {
        let mut report = sample_report();
        report.generate_summary();
        let md = report.to_markdown();

        assert!(md.contains("# Test Report"));
        assert!(md.contains("## System Information"));
        assert!(md.contains("**OS:** linux"));
        assert!(md.contains("## Results"));
        assert!(md.contains("### Startup"));
        assert!(md.contains("### Throughput"));
        assert!(md.contains("### Latency"));
        assert!(md.contains("### Resource Usage"));
        assert!(md.contains("## Summary"));
        assert!(md.contains("**Total Tests:** 4"));
        assert!(md.contains("**Win Rate:** 75.0%"));
        assert!(md.contains("✅"));
    }

    #[test]
    fn test_to_json() {
        let mut report = sample_report();
        report.generate_summary();
        let json = report.to_json();

        assert_eq!(json["title"], "Test Report");
        assert!(json["system_info"]["cpus"].is_number());
        assert!(json["results"].is_array());
        assert_eq!(json["results"].as_array().unwrap().len(), 4);
        assert_eq!(json["summary"]["total_tests"], 4);
        assert_eq!(json["summary"]["streamline_wins"], 3);
    }

    #[test]
    fn test_benchmark_category_display() {
        assert_eq!(BenchmarkCategory::Throughput.to_string(), "Throughput");
        assert_eq!(BenchmarkCategory::Latency.to_string(), "Latency");
        assert_eq!(
            BenchmarkCategory::ResourceUsage.to_string(),
            "Resource Usage"
        );
        assert_eq!(BenchmarkCategory::Startup.to_string(), "Startup");
        assert_eq!(BenchmarkCategory::EndToEnd.to_string(), "End-to-End");
    }

    #[test]
    fn test_platform_metric_new() {
        let m = PlatformMetric::new("Streamline", 42.0, "msg/s");
        assert_eq!(m.platform, "Streamline");
        assert!((m.value - 42.0).abs() < f64::EPSILON);
        assert_eq!(m.unit, "msg/s");
        assert!(!m.is_best);
    }

    #[test]
    fn test_report_roundtrip_json() {
        let mut report = sample_report();
        report.generate_summary();
        let json = report.to_json();
        let json_str = serde_json::to_string(&json).expect("serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("deserialize");
        assert_eq!(parsed["title"], "Test Report");
        assert_eq!(parsed["summary"]["total_tests"], 4);
    }
}
