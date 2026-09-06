//! Semantic topics (M2): per-partition vector index built by an async
//! embedder worker that consumes the append path.
//!
//! Stability tier: **Experimental**. Behind feature `semantic-topics` (not
//! yet declared in `Cargo.toml`). Enable via `TopicConfig.semantic.embed`.
//!
//! See `MOONSHOT_PLAN.md` §M2 and `docs/adr/0014-default-embedding-model.md`.
//!
//! # Architecture
//!
//! ```text
//! Producer ─► Append (storage::log) ─► [tap] ─► EmbedQueue ─► EmbedWorker
//!                                                                  │
//!                                                                  ▼
//!                                                          Per-partition HNSW
//! Consumer.search(query, k) ─► search_api::search(topic, query, k)
//! ```
//!
//! The `EmbedQueue` is a bounded MPSC channel; backpressure on overflow is
//! reported via `metrics::counter!("semantic.embed.dropped")`.

pub mod cost_ledger;
pub mod embedder;
pub mod index;
pub mod reembed;
pub mod registry;
pub mod worker;

pub use embedder::{HashEmbedder, DEFAULT_DIM};
pub use index::InMemoryIndex;
pub use worker::{EmbedError, EmbedJob, EmbedWorker, Embedder, EmbedderHandle};

/// A single search hit returned by `consumer.search()`.
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub partition: i32,
    pub offset: i64,
    pub score: f32,
}

/// API surface that producers and consumers indirectly call.
/// Real implementation lands in M2 P1; today this is a typed scaffold.
pub trait SemanticIndex: Send + Sync {
    /// Insert a record's embedding under `(partition, offset)`.
    fn insert(&self, partition: i32, offset: i64, embedding: &[f32]);

    /// Top-k nearest neighbors for `query`.
    fn search(&self, query: &[f32], k: usize) -> Vec<SearchHit>;
}
