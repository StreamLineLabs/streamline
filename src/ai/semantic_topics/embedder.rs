//! Deterministic, dependency-free embedder for the semantic-topics scaffold.
//!
//! Uses the **feature-hashing** trick (a.k.a. the "hashing trick"): each
//! whitespace-separated token is FNV-hashed into a fixed-dim bucket and
//! contributes +1 to that dimension. The resulting vector is L2-normalized
//! so cosine-similarity is well-defined.
//!
//! Stability tier: **Experimental**. Deliberately stupid — we want zero
//! model deps in unit tests. Production deployments swap in a real
//! `Embedder` (sentence-transformers, OpenAI, BGE, etc.) without changing
//! the surrounding wiring.

use super::worker::{EmbedError, Embedder};

/// Default dimensionality. Small enough for fast tests, large enough to
/// keep collisions rare for short texts.
pub const DEFAULT_DIM: usize = 64;

#[derive(Clone)]
pub struct HashEmbedder {
    dim: usize,
}

impl HashEmbedder {
    pub const fn new(dim: usize) -> Self {
        Self { dim }
    }
}

impl Default for HashEmbedder {
    fn default() -> Self {
        Self::new(DEFAULT_DIM)
    }
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    h
}

impl Embedder for HashEmbedder {
    fn embed(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let mut v = vec![0.0f32; self.dim];
        for token in text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|t| !t.is_empty())
        {
            let lowered = token.to_ascii_lowercase();
            let bucket = (fnv1a64(lowered.as_bytes()) as usize) % self.dim;
            v[bucket] += 1.0;
        }
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for x in &mut v {
                *x /= norm;
            }
        }
        Ok(v)
    }

    fn dim(&self) -> usize {
        self.dim
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embed_dim_matches() {
        let e = HashEmbedder::new(32);
        let v = e.embed("hello world").expect("embed");
        assert_eq!(v.len(), 32);
    }

    #[test]
    fn empty_text_returns_zero_vector() {
        let v = HashEmbedder::default().embed("   ").expect("embed");
        assert!(v.iter().all(|x| *x == 0.0));
    }

    #[test]
    fn shared_tokens_produce_similar_vectors() {
        let e = HashEmbedder::default();
        let a = e.embed("payment failed for user").expect("a");
        let b = e.embed("payment failed for customer").expect("b");
        let c = e.embed("user logged into account").expect("c");
        let cos = |x: &[f32], y: &[f32]| -> f32 {
            x.iter().zip(y.iter()).map(|(p, q)| p * q).sum::<f32>()
        };
        let sim_ab = cos(&a, &b);
        let sim_ac = cos(&a, &c);
        assert!(
            sim_ab > sim_ac,
            "expected payment-payment > payment-login (got {sim_ab} vs {sim_ac})"
        );
    }

    #[test]
    fn case_insensitive() {
        let e = HashEmbedder::default();
        let a = e.embed("Hello World").expect("a");
        let b = e.embed("HELLO world").expect("b");
        assert_eq!(a, b);
    }
}
