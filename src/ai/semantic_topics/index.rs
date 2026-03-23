//! In-memory implementation of [`super::SemanticIndex`] — a flat array of
//! `(partition, offset, vector)` rows that does brute-force cosine NN
//! search.
//!
//! Stability tier: **Experimental**. M2 P2 will swap this for an HNSW
//! index per partition (`crate::ai::hnsw`); the trait stays stable.

use std::sync::RwLock;

use super::{SearchHit, SemanticIndex};

#[derive(Default)]
pub struct InMemoryIndex {
    rows: RwLock<Vec<Row>>,
    dim: RwLock<Option<usize>>,
}

#[derive(Debug, Clone)]
struct Row {
    partition: i32,
    offset: i64,
    vector: Vec<f32>,
}

impl InMemoryIndex {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.rows.read().map(|r| r.len()).unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return a snapshot of all `(partition, offset)` pairs stored in the index.
    /// Used by the re-embed migration to enumerate existing records.
    pub fn entries(&self) -> Vec<(i32, i64)> {
        self.rows
            .read()
            .map(|r| r.iter().map(|row| (row.partition, row.offset)).collect())
            .unwrap_or_default()
    }
}

/// Cosine similarity for unit-length vectors is just the dot product;
/// we don't assume unit-length here so we normalize defensively.
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "dim mismatch in cosine()");
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na * nb)
    }
}

impl SemanticIndex for InMemoryIndex {
    fn insert(&self, partition: i32, offset: i64, embedding: &[f32]) {
        if let Ok(mut d) = self.dim.write() {
            match *d {
                None => *d = Some(embedding.len()),
                Some(existing) if existing == embedding.len() => {}
                Some(existing) => {
                    debug_assert!(
                        false,
                        "embedding dim {} != existing index dim {}",
                        embedding.len(),
                        existing
                    );
                    return;
                }
            }
        }
        if let Ok(mut rows) = self.rows.write() {
            rows.push(Row {
                partition,
                offset,
                vector: embedding.to_vec(),
            });
        }
    }

    fn search(&self, query: &[f32], k: usize) -> Vec<SearchHit> {
        let rows = match self.rows.read() {
            Ok(r) => r,
            Err(_) => return Vec::new(),
        };
        if rows.is_empty() || k == 0 {
            return Vec::new();
        }
        let mut scored: Vec<SearchHit> = rows
            .iter()
            .map(|r| SearchHit {
                partition: r.partition,
                offset: r.offset,
                score: cosine(&r.vector, query),
            })
            .collect();
        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        scored.truncate(k);
        scored
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_index_returns_no_hits() {
        let idx = InMemoryIndex::new();
        assert!(idx.is_empty());
        assert!(idx.search(&[0.1, 0.2, 0.3], 5).is_empty());
    }

    #[test]
    fn search_ranks_by_cosine_similarity() {
        let idx = InMemoryIndex::new();
        // Three distinct unit vectors:
        idx.insert(0, 1, &[1.0, 0.0, 0.0]);
        idx.insert(0, 2, &[0.0, 1.0, 0.0]);
        idx.insert(0, 3, &[0.5, 0.5, 0.0]);
        let hits = idx.search(&[1.0, 0.0, 0.0], 3);
        assert_eq!(hits.len(), 3);
        assert_eq!(hits[0].offset, 1, "exact match should be first");
        assert_eq!(hits[1].offset, 3, "blended vector should be second");
        assert_eq!(hits[2].offset, 2);
    }

    #[test]
    fn k_zero_returns_empty() {
        let idx = InMemoryIndex::new();
        idx.insert(0, 1, &[1.0, 0.0]);
        assert!(idx.search(&[1.0, 0.0], 0).is_empty());
    }

    #[test]
    fn k_larger_than_corpus_returns_all() {
        let idx = InMemoryIndex::new();
        idx.insert(0, 1, &[1.0, 0.0]);
        idx.insert(0, 2, &[0.0, 1.0]);
        let hits = idx.search(&[1.0, 1.0], 100);
        assert_eq!(hits.len(), 2);
    }
}
