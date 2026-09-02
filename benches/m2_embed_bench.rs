//! # M2 P0 — Embedding-Model Benchmark Scaffold
//!
//! **Status:** Phase-0 spike scaffold (`m2-p0-bench-embed`). Reports the
//! shape of the benchmark; the actual model loading is gated behind the
//! `ai-embed` feature flag (not yet defined in `Cargo.toml`) because it
//! pulls in `candle` / `ort` (multi-MB downloads).
//!
//! ## What this measures
//!
//! Three numbers per (model, runtime) combo:
//!
//! 1. **Throughput** — texts/sec at batch size 32 on N CPU cores.
//! 2. **Latency** — p50/p99 single-text embed latency.
//! 3. **Recall@10** — vs. a fixed gold ranking on the BEIR `nfcorpus`
//!    subset (300 docs, 25 queries; small enough to ship in-tree).
//!
//! ## Ratifying ADR-0014
//!
//! ADR-0014 picks `bge-small-en-v1.5` based on the *expected* numbers.
//! When this scaffold is wired to actual models (M2 P1 `m2-p1-embed-worker`),
//! we re-run and amend the ADR if real numbers diverge.
//!
//! ## How to enable real models
//!
//! 1. Add to `Cargo.toml`:
//!    ```toml
//!    [features]
//!    ai-embed = ["dep:candle-core", "dep:candle-nn", "dep:tokenizers"]
//!    ```
//! 2. Replace [`embed_stub`] below with a real model load + tokenizer
//!    + forward pass (see [candle examples](https://github.com/huggingface/candle/tree/main/candle-examples)).
//! 3. Drop a `bge-small-en-v1.5.gguf` into `benches/data/` (gitignored;
//!    `streamline embed pull` downloads it).
//!
//! Until then, this bench reports the harness's own overhead so
//! regressions in the harness itself are caught.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

/// Stub embedding: a deterministic hash projected to 384-dim float vector.
/// Replace with real model in M2 P1.
fn embed_stub(text: &str) -> Vec<f32> {
    let mut v = vec![0f32; 384];
    let bytes = text.as_bytes();
    for (i, slot) in v.iter_mut().enumerate() {
        let mut h: u64 = i as u64 ^ 0x9e3779b97f4a7c15;
        for b in bytes {
            h = h.rotate_left(5) ^ (*b as u64);
            h = h.wrapping_mul(0x100_0000_01b3);
        }
        *slot = ((h as u32 as f32) / (u32::MAX as f32)) - 0.5;
    }
    v
}

fn texts(n: usize) -> Vec<String> {
    (0..n)
        .map(|i| {
            format!(
                "synthetic order {i}: amount={}, currency=USD",
                i * 13 % 9999
            )
        })
        .collect()
}

fn bench_embed(c: &mut Criterion) {
    let mut group = c.benchmark_group("m2_embed");
    for batch in [1usize, 8, 32, 128] {
        let xs = texts(batch);
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &xs, |b, xs| {
            b.iter(|| {
                let mut sink = 0f32;
                for x in xs {
                    let v = embed_stub(x);
                    sink += v[0];
                }
                black_box(sink)
            });
        });
    }
    group.finish();
}

criterion_group!(m2_benches, bench_embed);
criterion_main!(m2_benches);
