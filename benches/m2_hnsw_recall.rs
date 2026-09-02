//! # M2 P0 — HNSW Recall Benchmark Scaffold (`m2-p0-hnsw-recall`)
//!
//! Sweeps HNSW hyperparameters `M` and `ef_search` against a fixed
//! ground-truth nearest-neighbor set computed by brute-force. Reports
//! recall@10 and query latency.
//!
//! Tuning targets pinned in ADR-0014 and ADR-0015:
//! - **recall@10 ≥ 0.85** for the chosen defaults.
//! - **p99 query < 5 ms** at 100K vectors on a single core.
//!
//! ## Status
//!
//! Pure-Rust harness exercising the existing `streamline::ai::hnsw`. If
//! the API evolves, update [`build_index`] and [`search_index`] below;
//! the recall computation is API-agnostic. Until those hooks are wired,
//! the scaffold falls back to a brute-force-vs-brute-force comparison
//! that documents the harness shape and keeps regressions to it
//! visible.
//!
//! Run:
//! ```bash
//! cargo bench --bench m2_hnsw_recall
//! ```

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

const DIM: usize = 128;
const N_VECTORS: usize = 5_000;
const N_QUERIES: usize = 100;
const K: usize = 10;

fn rand_vec(seed: u64, dim: usize) -> Vec<f32> {
    let mut s = seed;
    (0..dim)
        .map(|_| {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((s >> 11) as f64 / (1u64 << 53) as f64) as f32 - 0.5
        })
        .collect()
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0f32;
    let mut na = 0f32;
    let mut nb = 0f32;
    for (x, y) in a.iter().zip(b) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na.sqrt() * nb.sqrt())
    }
}

fn brute_topk(corpus: &[Vec<f32>], q: &[f32], k: usize) -> Vec<usize> {
    let mut scored: Vec<(usize, f32)> = corpus
        .iter()
        .enumerate()
        .map(|(i, v)| (i, cosine(v, q)))
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.into_iter().take(k).map(|(i, _)| i).collect()
}

/// Placeholder — wire to `streamline::ai::hnsw` builder under M2 P1.
/// Note: HnswIndex::insert/search are async; would need tokio runtime
/// in the bench harness. For now we measure harness overhead and validate
/// the recall computation shape. The real HNSW recall is tested in
/// `streamline/tests/hnsw_integration.rs`.
struct StubIndex {
    corpus: Vec<Vec<f32>>,
}

fn build_index(corpus: Vec<Vec<f32>>, _m: usize, _ef_construction: usize) -> StubIndex {
    StubIndex { corpus }
}

fn search_index(idx: &StubIndex, q: &[f32], k: usize, _ef_search: usize) -> Vec<usize> {
    brute_topk(&idx.corpus, q, k)
}

fn bench_hnsw_recall(c: &mut Criterion) {
    let corpus: Vec<Vec<f32>> = (0..N_VECTORS)
        .map(|i| rand_vec(i as u64 + 1, DIM))
        .collect();
    let queries: Vec<Vec<f32>> = (0..N_QUERIES)
        .map(|i| rand_vec((i + 999) as u64, DIM))
        .collect();

    // Ground truth (brute force).
    let gt: Vec<Vec<usize>> = queries.iter().map(|q| brute_topk(&corpus, q, K)).collect();

    let mut group = c.benchmark_group("m2_hnsw_recall");
    for &m in &[16usize, 32, 64] {
        for &ef in &[50usize, 100, 200] {
            let idx = build_index(corpus.clone(), m, m * 2);
            // Recall @ ef.
            let mut hits = 0;
            let mut total = 0;
            for (q, gt) in queries.iter().zip(gt.iter()) {
                let res = search_index(&idx, q, K, ef);
                hits += res.iter().filter(|x| gt.contains(x)).count();
                total += K;
            }
            let recall = hits as f64 / total as f64;
            eprintln!("[recall] M={m} ef_search={ef} recall@{K}={recall:.3}");

            group.bench_with_input(
                BenchmarkId::new(format!("M={m}"), ef),
                &(idx, queries.clone()),
                |b, (idx, qs)| {
                    let mut iter = qs.iter().cycle();
                    b.iter(|| {
                        let q = iter.next().unwrap();
                        let _ = search_index(idx, q, K, ef);
                    });
                },
            );
        }
    }
    group.finish();
}

criterion_group!(m2_hnsw, bench_hnsw_recall);
criterion_main!(m2_hnsw);
