//! Smart Partition & Auto-Rebalancing benchmarks for Streamline
//!
//! Run with: cargo bench --bench smart_partition_bench
//!
//! Measures analysis and rebalancing performance across realistic cluster
//! topologies: small (3-broker), medium (10-broker), and large (50-broker).
//! Includes throughput-skew analysis, key-distribution analysis, rebalance
//! planning, plan validation, and dry-run simulation.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::collections::HashMap;

use streamline::{
    analyze_key_distribution, analyze_throughput_skew, gini_coefficient,
    predict_rebalance_benefit, BrokerState, PartitionAssignment, PartitionMetrics, RebalanceConfig,
    RebalanceMode, SkewAnalyzer, SkewAnalyzerConfig, SmartRebalancer,
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

/// Generate partition throughputs for `n` partitions across `num_brokers`
/// brokers, with a configurable skew factor on the first broker.
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
    // Zipf: rank r gets base / r^s with s~1
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

// ── Benchmarks: Throughput-skew analysis ────────────────────────────────

fn bench_analyze_throughput_skew(c: &mut Criterion) {
    let mut group = c.benchmark_group("analyze_throughput_skew");

    // Small cluster: 3 brokers × 10 partitions each
    let small_uniform = make_partition_metrics("small", &uniform_throughputs(30, 100.0));
    let small_mild = make_partition_metrics("small", &mild_skew_throughputs(30, 100.0));
    let small_extreme = make_partition_metrics("small", &extreme_skew_throughputs(30, 100.0));

    group.bench_with_input(
        BenchmarkId::new("small_uniform", 30),
        &small_uniform,
        |b, m| b.iter(|| analyze_throughput_skew(black_box(m))),
    );
    group.bench_with_input(
        BenchmarkId::new("small_mild_skew", 30),
        &small_mild,
        |b, m| b.iter(|| analyze_throughput_skew(black_box(m))),
    );
    group.bench_with_input(
        BenchmarkId::new("small_extreme_skew", 30),
        &small_extreme,
        |b, m| b.iter(|| analyze_throughput_skew(black_box(m))),
    );

    // Medium cluster: 10 brokers × 30 partitions each
    let med_uniform = make_partition_metrics("medium", &uniform_throughputs(300, 100.0));
    let med_hot3 = make_partition_metrics("medium", &hot_partitions_throughputs(300, 100.0, 3));
    let med_progressive = make_partition_metrics("medium", &progressive_degradation(300, 50.0));

    group.bench_with_input(
        BenchmarkId::new("medium_uniform", 300),
        &med_uniform,
        |b, m| b.iter(|| analyze_throughput_skew(black_box(m))),
    );
    group.bench_with_input(
        BenchmarkId::new("medium_3_hot_partitions", 300),
        &med_hot3,
        |b, m| b.iter(|| analyze_throughput_skew(black_box(m))),
    );
    group.bench_with_input(
        BenchmarkId::new("medium_progressive_degradation", 300),
        &med_progressive,
        |b, m| b.iter(|| analyze_throughput_skew(black_box(m))),
    );

    // Large cluster: 50 brokers × 40 partitions each
    let large_zipfian = make_partition_metrics("large", &zipfian_throughputs(2000, 100.0));
    let large_top1pct = make_partition_metrics("large", &top_one_pct_throughputs(2000, 100.0));

    group.bench_with_input(
        BenchmarkId::new("large_zipfian", 2000),
        &large_zipfian,
        |b, m| b.iter(|| analyze_throughput_skew(black_box(m))),
    );
    group.bench_with_input(
        BenchmarkId::new("large_top_1pct", 2000),
        &large_top1pct,
        |b, m| b.iter(|| analyze_throughput_skew(black_box(m))),
    );

    group.finish();
}

// ── Benchmarks: Key-distribution analysis ───────────────────────────────

fn bench_analyze_key_distribution(c: &mut Criterion) {
    let mut group = c.benchmark_group("analyze_key_distribution");

    let keys_1k = make_uniform_keys(1_000);
    let keys_100k = make_uniform_keys(100_000);
    let keys_1m = make_uniform_keys(1_000_000);
    let keys_zipf_100k = make_zipfian_keys(100_000);
    let keys_seq_100k = make_sequential_keys(100_000);

    group.bench_with_input(
        BenchmarkId::new("uniform_1k", 1_000),
        &keys_1k,
        |b, k| b.iter(|| analyze_key_distribution(black_box(k))),
    );
    group.bench_with_input(
        BenchmarkId::new("uniform_100k", 100_000),
        &keys_100k,
        |b, k| b.iter(|| analyze_key_distribution(black_box(k))),
    );
    group.bench_with_input(
        BenchmarkId::new("uniform_1m", 1_000_000),
        &keys_1m,
        |b, k| b.iter(|| analyze_key_distribution(black_box(k))),
    );
    group.bench_with_input(
        BenchmarkId::new("zipfian_100k", 100_000),
        &keys_zipf_100k,
        |b, k| b.iter(|| analyze_key_distribution(black_box(k))),
    );
    group.bench_with_input(
        BenchmarkId::new("sequential_100k", 100_000),
        &keys_seq_100k,
        |b, k| b.iter(|| analyze_key_distribution(black_box(k))),
    );

    group.finish();
}

// ── Benchmarks: Rebalance planning ──────────────────────────────────────

fn bench_plan_rebalance(c: &mut Criterion) {
    let mut group = c.benchmark_group("plan_rebalance");

    let rebalancer = SmartRebalancer::new(RebalanceConfig::default());
    let rack_rebalancer = SmartRebalancer::new(RebalanceConfig {
        rack_awareness: true,
        ..RebalanceConfig::default()
    });

    // Small: 3 brokers, skewed (broker 0 has 80% throughput)
    {
        let brokers = make_brokers_skewed(3, 10, &[800.0, 100.0, 100.0]);
        let assignments = make_assignments(&brokers);
        let metrics = make_partition_metrics("small-skew", &extreme_skew_throughputs(30, 100.0));
        let analysis = analyze_throughput_skew(&metrics);

        group.bench_function(BenchmarkId::new("small_skewed", 30), |b| {
            b.iter(|| {
                rebalancer.plan_rebalance(
                    black_box(&analysis),
                    black_box(&assignments),
                    black_box(&brokers),
                )
            })
        });
    }

    // Medium: 10 brokers, hot-partition workload
    {
        let brokers = make_brokers(10, 30);
        let assignments = make_assignments(&brokers);
        let metrics =
            make_partition_metrics("medium-hot", &hot_partitions_throughputs(300, 100.0, 3));
        let analysis = analyze_throughput_skew(&metrics);

        group.bench_function(BenchmarkId::new("medium_hot_partitions", 300), |b| {
            b.iter(|| {
                rebalancer.plan_rebalance(
                    black_box(&analysis),
                    black_box(&assignments),
                    black_box(&brokers),
                )
            })
        });
    }

    // Large: 50 brokers, Zipfian workload
    {
        let brokers = make_brokers(50, 40);
        let assignments = make_assignments(&brokers);
        let metrics = make_partition_metrics("large-zipf", &zipfian_throughputs(2000, 100.0));
        let analysis = analyze_throughput_skew(&metrics);

        group.bench_function(BenchmarkId::new("large_zipfian", 2000), |b| {
            b.iter(|| {
                rebalancer.plan_rebalance(
                    black_box(&analysis),
                    black_box(&assignments),
                    black_box(&brokers),
                )
            })
        });
    }

    // Large with rack awareness
    {
        let brokers = make_brokers(50, 40);
        let assignments = make_assignments(&brokers);
        let metrics =
            make_partition_metrics("large-rack", &top_one_pct_throughputs(2000, 100.0));
        let analysis = analyze_throughput_skew(&metrics);

        group.bench_function(BenchmarkId::new("large_rack_aware", 2000), |b| {
            b.iter(|| {
                rack_rebalancer.plan_rebalance(
                    black_box(&analysis),
                    black_box(&assignments),
                    black_box(&brokers),
                )
            })
        });
    }

    group.finish();
}

// ── Benchmarks: Plan validation ─────────────────────────────────────────

fn bench_validate_plan(c: &mut Criterion) {
    let mut group = c.benchmark_group("validate_plan");

    let rebalancer = SmartRebalancer::new(RebalanceConfig::default());

    // Small
    {
        let brokers = make_brokers_skewed(3, 10, &[800.0, 100.0, 100.0]);
        let assignments = make_assignments(&brokers);
        let metrics = make_partition_metrics("val-s", &extreme_skew_throughputs(30, 100.0));
        let analysis = analyze_throughput_skew(&metrics);
        let plan = rebalancer.plan_rebalance(&analysis, &assignments, &brokers);

        group.bench_function(BenchmarkId::new("small", 30), |b| {
            b.iter(|| rebalancer.validate_plan(black_box(&plan), black_box(&brokers)))
        });
    }

    // Large
    {
        let brokers = make_brokers(50, 40);
        let assignments = make_assignments(&brokers);
        let metrics = make_partition_metrics("val-l", &zipfian_throughputs(2000, 100.0));
        let analysis = analyze_throughput_skew(&metrics);
        let plan = rebalancer.plan_rebalance(&analysis, &assignments, &brokers);

        group.bench_function(BenchmarkId::new("large", 2000), |b| {
            b.iter(|| rebalancer.validate_plan(black_box(&plan), black_box(&brokers)))
        });
    }

    group.finish();
}

// ── Benchmarks: Dry-run simulation ──────────────────────────────────────

fn bench_dry_run(c: &mut Criterion) {
    let mut group = c.benchmark_group("dry_run");

    let rebalancer = SmartRebalancer::new(RebalanceConfig {
        mode: RebalanceMode::DryRun,
        ..RebalanceConfig::default()
    });

    // Medium
    {
        let brokers = make_brokers(10, 30);
        let assignments = make_assignments(&brokers);
        let metrics =
            make_partition_metrics("dry-m", &hot_partitions_throughputs(300, 100.0, 3));
        let analysis = analyze_throughput_skew(&metrics);
        let plan = rebalancer.plan_rebalance(&analysis, &assignments, &brokers);

        group.bench_function(BenchmarkId::new("medium", 300), |b| {
            b.iter(|| rebalancer.dry_run(black_box(&plan), black_box(&brokers)))
        });
    }

    // Large
    {
        let brokers = make_brokers(50, 40);
        let assignments = make_assignments(&brokers);
        let metrics = make_partition_metrics("dry-l", &zipfian_throughputs(2000, 100.0));
        let analysis = analyze_throughput_skew(&metrics);
        let plan = rebalancer.plan_rebalance(&analysis, &assignments, &brokers);

        group.bench_function(BenchmarkId::new("large", 2000), |b| {
            b.iter(|| rebalancer.dry_run(black_box(&plan), black_box(&brokers)))
        });
    }

    group.finish();
}

// ── Benchmarks: Gini coefficient scaling ────────────────────────────────

fn bench_gini_coefficient(c: &mut Criterion) {
    let mut group = c.benchmark_group("gini_coefficient");

    for &n in &[30, 300, 2000] {
        let values: Vec<f64> = zipfian_throughputs(n, 100.0);
        group.bench_with_input(BenchmarkId::new("zipfian", n), &values, |b, v| {
            b.iter(|| gini_coefficient(black_box(v)))
        });
    }

    group.finish();
}

// ── Benchmarks: Predict rebalance benefit ───────────────────────────────

fn bench_predict_rebalance_benefit(c: &mut Criterion) {
    let mut group = c.benchmark_group("predict_rebalance_benefit");

    let metrics_small = make_partition_metrics("pred-s", &extreme_skew_throughputs(30, 100.0));
    let analysis_small = analyze_throughput_skew(&metrics_small);

    let metrics_large = make_partition_metrics("pred-l", &zipfian_throughputs(2000, 100.0));
    let analysis_large = analyze_throughput_skew(&metrics_large);

    group.bench_function("small_double_partitions", |b| {
        b.iter(|| predict_rebalance_benefit(black_box(&analysis_small), black_box(60)))
    });
    group.bench_function("large_double_partitions", |b| {
        b.iter(|| predict_rebalance_benefit(black_box(&analysis_large), black_box(4000)))
    });

    group.finish();
}

// ── Benchmarks: SkewAnalyzer stateful workflow ──────────────────────────

fn bench_analyzer_workflow(c: &mut Criterion) {
    let mut group = c.benchmark_group("analyzer_workflow");

    // Record + analyze_deep for various sizes
    for &(label, n) in &[("small", 30usize), ("medium", 300), ("large", 2000)] {
        let throughputs = zipfian_throughputs(n, 100.0);
        let metrics = make_partition_metrics("workflow", &throughputs);

        group.bench_function(BenchmarkId::new("record_and_analyze", label), |b| {
            b.iter(|| {
                let mut analyzer = SkewAnalyzer::new(SkewAnalyzerConfig::default());
                analyzer.record_sample("workflow", metrics.clone());
                let result = analyzer.analyze_deep("workflow");
                black_box(result)
            })
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_gini_coefficient,
    bench_analyze_throughput_skew,
    bench_analyze_key_distribution,
    bench_plan_rebalance,
    bench_validate_plan,
    bench_dry_run,
    bench_predict_rebalance_benefit,
    bench_analyzer_workflow,
);

criterion_main!(benches);
