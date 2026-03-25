//! # Streamline Bench — unified throughput / latency / size harness.
//!
//! **Status:** Phase-1 prototype scaffold (`pre-bench-harness` deliverable).
//!
//! This is the canonical place to add new "moonshot NFR" benchmarks. The
//! file is intentionally split into small modules so each moonshot's
//! Phase-0 spike can drop a `<moonshot>_bench.rs` next to it without
//! touching the others.
//!
//! Run:
//! ```bash
//! cargo bench --bench streamline_bench
//! cargo bench --bench streamline_bench -- semantic   # only m2 benches
//! ```
//!
//! All benches use [`SyntheticDataset`] so numbers are reproducible across
//! machines and across moonshots. Seed defaults to `42`; override with
//! `STREAMLINE_BENCH_SEED=<n>`.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

/// A deterministic, in-memory record stream. Used by every harness so
/// throughput numbers are comparable.
pub struct SyntheticDataset {
    seed: u64,
    avg_value_bytes: usize,
    text_corpus: &'static [&'static str],
}

impl SyntheticDataset {
    pub fn new(seed: u64, avg_value_bytes: usize) -> Self {
        Self {
            seed,
            avg_value_bytes,
            text_corpus: TEXT_CORPUS,
        }
    }

    /// Generate `n` records (key, value bytes). Deterministic per seed.
    pub fn records(&self, n: usize) -> Vec<(String, Vec<u8>)> {
        let mut out = Vec::with_capacity(n);
        let mut s = self.seed;
        for i in 0..n {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let key = format!("k{i}");
            let mut value = Vec::with_capacity(self.avg_value_bytes);
            let txt = self.text_corpus[(s as usize) % self.text_corpus.len()];
            value.extend_from_slice(txt.as_bytes());
            while value.len() < self.avg_value_bytes {
                value.push(((s >> (value.len() % 56)) & 0xff) as u8);
            }
            value.truncate(self.avg_value_bytes);
            out.push((key, value));
        }
        out
    }

    /// Generate `n` text snippets (for embedding/recall benches).
    pub fn texts(&self, n: usize) -> Vec<String> {
        let mut out = Vec::with_capacity(n);
        let mut s = self.seed;
        for _ in 0..n {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            out.push(self.text_corpus[(s as usize) % self.text_corpus.len()].to_string());
        }
        out
    }
}

/// Tiny representative corpus. Real moonshots can opt into BEIR /
/// Streamline-Bench by setting `STREAMLINE_BENCH_CORPUS=<path>` and adding
/// a loader. Held inline so the scaffold has no I/O dependencies.
const TEXT_CORPUS: &[&str] = &[
    "Order #4221 totalled 122.45 EUR and shipped to Berlin yesterday.",
    "Customer support ticket 88: refund issued for SKU XL-204.",
    "Server alpha-3 reported elevated CPU between 14:00 and 14:07 UTC.",
    "User Alice logged in from 192.0.2.41 using OAuth 2.1 PKCE flow.",
    "Inventory snapshot: warehouse #7 has 1240 units of product P-9912.",
    "Schema for orders.v3 added optional discount_code: string field.",
    "Pipeline orders.normalize@v2 emitted 4.2M records in 60 seconds.",
    "GDPR delete request processed for user 7c1a-b29b in 1.4s.",
];

/// Baseline throughput: produce N records into a no-op sink.
/// Sanity-check the harness machinery.
fn bench_baseline_produce(c: &mut Criterion) {
    let mut group = c.benchmark_group("baseline_produce");
    let ds = SyntheticDataset::new(seed_from_env(), 128);
    for n in [1_000usize, 10_000, 100_000] {
        let records = ds.records(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &records, |b, recs| {
            b.iter(|| {
                let mut sink = 0u64;
                for (k, v) in recs.iter() {
                    sink ^= k.len() as u64 ^ v.len() as u64;
                }
                black_box(sink)
            });
        });
    }
    group.finish();
}

fn seed_from_env() -> u64 {
    std::env::var("STREAMLINE_BENCH_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(42)
}

criterion_group!(benches, bench_baseline_produce);
criterion_main!(benches);
