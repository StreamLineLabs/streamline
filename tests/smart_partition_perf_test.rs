//! Performance and correctness tests for the smart partition rebalancer.
//!
//! Run with: cargo test --test smart_partition_perf_test
//!
//! These tests exercise the analyzer and rebalancer across realistic cluster
//! topologies (small/medium/large) with various skew distributions. Each test
//! asserts both correctness and performance within CI-friendly time budgets.

use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

use streamline::{
    analyze_key_distribution, analyze_throughput_skew, gini_coefficient,
    predict_rebalance_benefit, BrokerState, PartitionAssignment, PartitionMetrics, RebalanceConfig,
    RebalanceMode, SkewAnalyzer, SkewAnalyzerConfig, SkewSeverity, SmartRebalancer,
};

// ── Data generators ─────────────────────────────────────────────────────

fn make_partition_metrics(topic: &str, throughputs: &[f64]) -> Vec<PartitionMetrics> {
    throughputs
        .iter()
        .enumerate()
        .map(|(i, &tp)| PartitionMetrics {
            topic: topic.to_string(),
            partition: i as i32,
            throughput_mps: tp,
            throughput_bps: tp * 256.0,
            consumer_lag: (tp * 0.1) as i64,
            unique_keys: 100 + (tp as u64 / 10),
            size_bytes: (tp * 1024.0) as u64,
            hot_key_ratio: if tp > 500.0 { 0.4 } else { 0.05 },
            p99_latency_ms: 5.0 + (tp / 200.0),
        })
        .collect()
}

fn uniform_throughputs(count: usize, base: f64) -> Vec<f64> {
    vec![base; count]
}

fn mild_skew_throughputs(count: usize, base: f64) -> Vec<f64> {
    let mut v = vec![base; count];
    if !v.is_empty() {
        v[0] = base * 1.5;
    }
    v
}

fn extreme_skew_throughputs(count: usize, base: f64) -> Vec<f64> {
    let mut v = vec![base; count];
    if !v.is_empty() {
        v[0] = base * 10.0;
    }
    v
}

fn hot_partitions_throughputs(count: usize, base: f64, hot_count: usize) -> Vec<f64> {
    let total_budget = base * count as f64;
    let hot_share = total_budget * 0.5;
    let cold_share = total_budget * 0.5;
    let hot_each = hot_share / hot_count as f64;
    let cold_each = if count > hot_count {
        cold_share / (count - hot_count) as f64
    } else {
        base
    };
    let mut v = Vec::with_capacity(count);
    for i in 0..count {
        v.push(if i < hot_count { hot_each } else { cold_each });
    }
    v
}

fn progressive_degradation(count: usize, base: f64) -> Vec<f64> {
    (0..count)
        .map(|i| base * (1.0 + 0.1 * i as f64))
        .collect()
}

fn zipfian_throughputs(count: usize, base: f64) -> Vec<f64> {
    let harmonic: f64 = (1..=count).map(|r| 1.0 / r as f64).sum();
    let total = base * count as f64;
    (1..=count)
        .map(|r| total * (1.0 / r as f64) / harmonic)
        .collect()
}

fn top_one_pct_throughputs(count: usize, base: f64) -> Vec<f64> {
    let hot_count = (count as f64 * 0.01).ceil().max(1.0) as usize;
    let total = base * count as f64;
    let hot_share = total * 0.90;
    let cold_share = total * 0.10;
    let hot_each = hot_share / hot_count as f64;
    let cold_each = if count > hot_count {
        cold_share / (count - hot_count) as f64
    } else {
        base
    };
    let mut v = Vec::with_capacity(count);
    for i in 0..count {
        v.push(if i < hot_count { hot_each } else { cold_each });
    }
    v
}

fn make_brokers(count: usize, partitions_each: usize) -> Vec<BrokerState> {
    let mut pid = 0;
    (0..count)
        .map(|i| {
            let ids: Vec<i32> = (pid..pid + partitions_each as i32).collect();
            pid += partitions_each as i32;
            BrokerState {
                broker_id: i as i32,
                rack: Some(format!("rack-{}", i % 3)),
                partition_ids: ids,
                total_throughput_mps: 100.0 * partitions_each as f64,
                total_bytes: 1_000_000 * partitions_each as u64,
                available_capacity_pct: 50.0,
            }
        })
        .collect()
}

fn make_brokers_skewed(
    count: usize,
    partitions_each: usize,
    broker_throughputs: &[f64],
) -> Vec<BrokerState> {
    let mut pid = 0;
    (0..count)
        .map(|i| {
            let ids: Vec<i32> = (pid..pid + partitions_each as i32).collect();
            pid += partitions_each as i32;
            let tp = broker_throughputs.get(i).copied().unwrap_or(100.0);
            BrokerState {
                broker_id: i as i32,
                rack: Some(format!("rack-{}", i % 3)),
                partition_ids: ids,
                total_throughput_mps: tp,
                total_bytes: (tp * 10_000.0) as u64,
                available_capacity_pct: 50.0,
            }
        })
        .collect()
}

fn make_assignments(brokers: &[BrokerState]) -> Vec<PartitionAssignment> {
    brokers
        .iter()
        .flat_map(|b| {
            let per_part_tp = if b.partition_ids.is_empty() {
                0.0
            } else {
                b.total_throughput_mps / b.partition_ids.len() as f64
            };
            b.partition_ids.iter().map(move |&pid| PartitionAssignment {
                partition_id: pid,
                broker_id: b.broker_id,
                is_leader: true,
                size_bytes: 1_000_000,
                throughput_mps: per_part_tp,
            })
        })
        .collect()
}

fn make_uniform_keys(count: usize) -> HashMap<String, u64> {
    (0..count)
        .map(|i| (format!("key-{:08}", i), 10))
        .collect()
}

fn make_zipfian_keys(count: usize) -> HashMap<String, u64> {
    let mut map = HashMap::with_capacity(count);
    for i in 0..count {
        let rank = i + 1;
        let freq = (1_000_000.0 / (rank as f64).powf(1.0)) as u64;
        map.insert(format!("key-{:08}", i), freq.max(1));
    }
    map
}

fn make_sequential_keys(count: usize) -> HashMap<String, u64> {
    (0..count)
        .map(|i| (format!("{}", 1_000_000 + i), 5))
        .collect()
}

// ── Small cluster correctness ───────────────────────────────────────────

#[test]
fn small_uniform_no_skew() {
    let metrics = make_partition_metrics("t", &uniform_throughputs(30, 100.0));
    let analysis = analyze_throughput_skew(&metrics);

    assert!(
        analysis.skew_coefficient < 0.05,
        "Uniform distribution should have near-zero Gini (got {})",
        analysis.skew_coefficient
    );
    assert_eq!(analysis.severity, SkewSeverity::None);
    assert!(analysis.hot_partitions.is_empty());
}

#[test]
fn small_mild_skew_detected() {
    let metrics = make_partition_metrics("t", &mild_skew_throughputs(30, 100.0));
    let analysis = analyze_throughput_skew(&metrics);

    assert!(
        analysis.skew_coefficient > 0.0,
        "Mild skew should be detected"
    );
}

#[test]
fn small_extreme_skew_critical() {
    let metrics = make_partition_metrics("t", &extreme_skew_throughputs(30, 100.0));
    let analysis = analyze_throughput_skew(&metrics);

    assert!(
        analysis.skew_coefficient > 0.15,
        "Extreme skew Gini should be elevated (got {})",
        analysis.skew_coefficient
    );
    assert!(
        !analysis.hot_partitions.is_empty(),
        "Should identify hot partitions"
    );
    assert!(
        analysis.hot_partitions.contains(&0),
        "Partition 0 should be hot"
    );
}

// ── Medium cluster correctness ──────────────────────────────────────────

#[test]
fn medium_3_hot_partitions() {
    let metrics = make_partition_metrics("t", &hot_partitions_throughputs(300, 100.0, 3));
    let analysis = analyze_throughput_skew(&metrics);

    assert!(
        analysis.skew_coefficient > 0.2,
        "3-hot-partition workload should show significant skew (got {})",
        analysis.skew_coefficient
    );
    assert!(
        !analysis.hot_partitions.is_empty(),
        "Should detect hot partitions"
    );
}

#[test]
fn medium_progressive_degradation() {
    let metrics = make_partition_metrics("t", &progressive_degradation(300, 50.0));
    let analysis = analyze_throughput_skew(&metrics);

    assert!(
        analysis.skew_coefficient > 0.1,
        "Progressive degradation should produce moderate skew (got {})",
        analysis.skew_coefficient
    );
}

// ── Large cluster correctness ───────────────────────────────────────────

#[test]
fn large_zipfian_has_hot_partitions() {
    let metrics = make_partition_metrics("t", &zipfian_throughputs(2000, 100.0));
    let analysis = analyze_throughput_skew(&metrics);

    assert!(
        analysis.skew_coefficient > 0.3,
        "Zipfian distribution should be skewed (got {})",
        analysis.skew_coefficient
    );
    assert!(
        !analysis.hot_partitions.is_empty(),
        "Zipfian should identify hot partitions"
    );
}

#[test]
fn large_top_1pct_extreme_skew() {
    let metrics = make_partition_metrics("t", &top_one_pct_throughputs(2000, 100.0));
    let analysis = analyze_throughput_skew(&metrics);

    assert!(
        analysis.skew_coefficient > 0.5,
        "1% hot partitions carrying 90% traffic should show extreme skew (got {})",
        analysis.skew_coefficient
    );
    assert!(
        matches!(
            analysis.severity,
            SkewSeverity::High | SkewSeverity::Critical
        ),
        "Severity should be High or Critical, got {:?}",
        analysis.severity
    );
}

// ── Key distribution correctness ────────────────────────────────────────

#[test]
fn key_distribution_uniform_1k() {
    let keys = make_uniform_keys(1_000);
    let analysis = analyze_key_distribution(&keys);

    assert!(
        analysis.estimated_cardinality >= 500,
        "HLL should estimate ≥500 for 1K keys (got {})",
        analysis.estimated_cardinality
    );
}

#[test]
fn key_distribution_zipfian_detects_hot_keys() {
    let keys = make_zipfian_keys(100_000);
    let analysis = analyze_key_distribution(&keys);

    assert!(
        !analysis.top_k_keys.is_empty(),
        "Should find top-K keys in Zipfian distribution"
    );
    if analysis.top_k_keys.len() >= 2 {
        assert!(
            analysis.top_k_keys[0].1 >= analysis.top_k_keys[1].1,
            "Top-K should be sorted by frequency"
        );
    }
}

#[test]
fn key_distribution_sequential_monotonic() {
    let keys = make_sequential_keys(100_000);
    let analysis = analyze_key_distribution(&keys);

    assert!(
        analysis.estimated_cardinality >= 10_000,
        "HLL should estimate reasonable cardinality for 100K sequential keys (got {})",
        analysis.estimated_cardinality
    );
}

// ── Rebalance planning correctness ──────────────────────────────────────

#[test]
fn rebalance_reduces_skew_small() {
    let rebalancer = SmartRebalancer::new(RebalanceConfig::default());
    let brokers = make_brokers_skewed(3, 10, &[800.0, 100.0, 100.0]);
    let assignments = make_assignments(&brokers);
    let metrics = make_partition_metrics("t", &extreme_skew_throughputs(30, 100.0));
    let analysis = analyze_throughput_skew(&metrics);

    let plan = rebalancer.plan_rebalance(&analysis, &assignments, &brokers);

    assert!(
        plan.estimated_skew_after <= plan.current_skew,
        "Rebalance should reduce skew: before={} after={}",
        plan.current_skew,
        plan.estimated_skew_after
    );
}

#[test]
fn rebalance_reduces_skew_large() {
    let rebalancer = SmartRebalancer::new(RebalanceConfig::default());
    let brokers = make_brokers(50, 40);
    let assignments = make_assignments(&brokers);
    let metrics = make_partition_metrics("t", &zipfian_throughputs(2000, 100.0));
    let analysis = analyze_throughput_skew(&metrics);

    let plan = rebalancer.plan_rebalance(&analysis, &assignments, &brokers);

    assert!(
        plan.estimated_skew_after <= plan.current_skew,
        "Large rebalance should reduce skew: before={} after={}",
        plan.current_skew,
        plan.estimated_skew_after
    );
}

#[test]
fn rebalance_with_rack_awareness() {
    let rebalancer = SmartRebalancer::new(RebalanceConfig {
        rack_awareness: true,
        ..RebalanceConfig::default()
    });
    let brokers = make_brokers(50, 40);
    let assignments = make_assignments(&brokers);
    let metrics = make_partition_metrics("t", &top_one_pct_throughputs(2000, 100.0));
    let analysis = analyze_throughput_skew(&metrics);

    let plan = rebalancer.plan_rebalance(&analysis, &assignments, &brokers);

    for mv in &plan.moves {
        let from_rack = brokers
            .iter()
            .find(|b| b.broker_id == mv.from_broker)
            .and_then(|b| b.rack.as_ref());
        let to_rack = brokers
            .iter()
            .find(|b| b.broker_id == mv.to_broker)
            .and_then(|b| b.rack.as_ref());
        if let (Some(fr), Some(tr)) = (from_rack, to_rack) {
            assert_ne!(
                fr, tr,
                "Rack-aware rebalance should not move partition {} within same rack ({})",
                mv.partition_id, fr
            );
        }
    }
}

#[test]
fn validate_plan_correctness() {
    let rebalancer = SmartRebalancer::new(RebalanceConfig::default());
    let brokers = make_brokers_skewed(3, 10, &[800.0, 100.0, 100.0]);
    let assignments = make_assignments(&brokers);
    let metrics = make_partition_metrics("t", &extreme_skew_throughputs(30, 100.0));
    let analysis = analyze_throughput_skew(&metrics);

    let plan = rebalancer.plan_rebalance(&analysis, &assignments, &brokers);
    let validation = rebalancer.validate_plan(&plan, &brokers);

    assert!(
        validation.is_ok(),
        "Valid rebalance plan should pass validation: {:?}",
        validation.err()
    );
}

#[test]
fn dry_run_produces_pre_post_state() {
    let rebalancer = SmartRebalancer::new(RebalanceConfig {
        mode: RebalanceMode::DryRun,
        ..RebalanceConfig::default()
    });
    let brokers = make_brokers(10, 30);
    let assignments = make_assignments(&brokers);
    let metrics = make_partition_metrics("t", &hot_partitions_throughputs(300, 100.0, 3));
    let analysis = analyze_throughput_skew(&metrics);

    let plan = rebalancer.plan_rebalance(&analysis, &assignments, &brokers);
    let dry = rebalancer.dry_run(&plan, &brokers);

    assert_eq!(dry.pre_state.brokers.len(), brokers.len());
    assert_eq!(dry.post_state.brokers.len(), brokers.len());
    assert!(
        dry.post_state.skew_coefficient <= dry.pre_state.skew_coefficient,
        "Dry-run post-state skew should not increase: pre={} post={}",
        dry.pre_state.skew_coefficient,
        dry.post_state.skew_coefficient
    );
}

#[test]
fn predict_benefit_doubles_partitions() {
    let metrics = make_partition_metrics("t", &extreme_skew_throughputs(30, 100.0));
    let analysis = analyze_throughput_skew(&metrics);

    let benefit = predict_rebalance_benefit(&analysis, 60);
    assert!(
        benefit.estimated_skew_improvement > 0.0,
        "Doubling partitions should improve skew (got {})",
        benefit.estimated_skew_improvement
    );
}

// ── Performance guardrails (timed tests for CI) ─────────────────────────

#[test]
fn perf_analyze_throughput_skew_small() {
    let metrics = make_partition_metrics("perf", &uniform_throughputs(30, 100.0));

    let start = Instant::now();
    for _ in 0..1_000 {
        let _ = analyze_throughput_skew(black_box(&metrics));
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() < 1_000,
        "1000 analyses of 30 partitions should complete in <1s (took {:?})",
        elapsed
    );
}

#[test]
fn perf_analyze_throughput_skew_large() {
    let metrics = make_partition_metrics("perf", &zipfian_throughputs(2000, 100.0));

    let start = Instant::now();
    for _ in 0..100 {
        let _ = analyze_throughput_skew(black_box(&metrics));
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() < 2_000,
        "100 analyses of 2000 partitions should complete in <2s (took {:?})",
        elapsed
    );
}

#[test]
fn perf_key_distribution_100k() {
    let keys = make_uniform_keys(100_000);

    let start = Instant::now();
    for _ in 0..10 {
        let _ = analyze_key_distribution(black_box(&keys));
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_secs() < 10,
        "10 key-distribution analyses of 100K keys should complete in <10s (took {:?})",
        elapsed
    );
}

#[test]
fn perf_plan_rebalance_large() {
    let rebalancer = SmartRebalancer::new(RebalanceConfig::default());
    let brokers = make_brokers(50, 40);
    let assignments = make_assignments(&brokers);
    let metrics = make_partition_metrics("perf", &zipfian_throughputs(2000, 100.0));
    let analysis = analyze_throughput_skew(&metrics);

    let start = Instant::now();
    for _ in 0..100 {
        let _ = rebalancer.plan_rebalance(
            black_box(&analysis),
            black_box(&assignments),
            black_box(&brokers),
        );
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() < 5_000,
        "100 rebalance plans for 2000 partitions should complete in <5s (took {:?})",
        elapsed
    );
}

#[test]
fn perf_validate_plan_large() {
    let rebalancer = SmartRebalancer::new(RebalanceConfig::default());
    let brokers = make_brokers(50, 40);
    let assignments = make_assignments(&brokers);
    let metrics = make_partition_metrics("perf", &zipfian_throughputs(2000, 100.0));
    let analysis = analyze_throughput_skew(&metrics);
    let plan = rebalancer.plan_rebalance(&analysis, &assignments, &brokers);

    let start = Instant::now();
    for _ in 0..1_000 {
        let _ = rebalancer.validate_plan(black_box(&plan), black_box(&brokers));
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() < 2_000,
        "1000 validations for 2000-partition plan should complete in <2s (took {:?})",
        elapsed
    );
}
