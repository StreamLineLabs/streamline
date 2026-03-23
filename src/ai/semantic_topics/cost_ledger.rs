//! Cost ledger for semantic topic embedding (M2 P3).
//!
//! Tracks bytes embedded, records embedded, and estimated cost per topic.

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

/// Returns the global `CostLedger` singleton, protected by a `Mutex`.
pub fn global() -> &'static Mutex<CostLedger> {
    static INSTANCE: OnceLock<Mutex<CostLedger>> = OnceLock::new();
    INSTANCE.get_or_init(|| Mutex::new(CostLedger::new()))
}

/// Per-topic embedding statistics.
#[derive(Debug, Clone)]
pub struct TopicEmbedStats {
    pub topic: String,
    pub total_records_embedded: u64,
    pub total_bytes_embedded: u64,
    pub total_errors: u64,
    pub model: String,
    pub estimated_cost_usd: f64,
}

impl TopicEmbedStats {
    fn new(topic: &str, model: &str) -> Self {
        Self {
            topic: topic.to_string(),
            total_records_embedded: 0,
            total_bytes_embedded: 0,
            total_errors: 0,
            model: model.to_string(),
            estimated_cost_usd: 0.0,
        }
    }
}

/// Estimated cost per 1 000 tokens by model name.
///
/// Tokens are approximated as `ceil(bytes / 4)` (a rough byte-to-token ratio
/// for English text).
fn cost_per_1k_tokens(model: &str) -> f64 {
    match model {
        "bge-small" => 0.00001,
        "openai" => 0.00002,
        _ => 0.0,
    }
}

/// Approximate token count from raw bytes.
fn estimate_tokens(bytes: u64) -> f64 {
    // ~4 bytes per token for English text.
    (bytes as f64 / 4.0).ceil()
}

/// Global per-topic cost ledger.
#[derive(Debug, Default)]
pub struct CostLedger {
    stats: HashMap<String, TopicEmbedStats>,
}

impl CostLedger {
    /// Creates an empty cost ledger.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful embedding for `topic`.
    pub fn record_embed(&mut self, topic: &str, bytes_len: u64, model: &str) {
        let entry = self
            .stats
            .entry(topic.to_string())
            .or_insert_with(|| TopicEmbedStats::new(topic, model));

        // If the model changed mid-stream, update the stored model name.
        if entry.model != model {
            entry.model = model.to_string();
        }

        entry.total_records_embedded += 1;
        entry.total_bytes_embedded += bytes_len;

        let tokens = estimate_tokens(bytes_len);
        entry.estimated_cost_usd += tokens / 1000.0 * cost_per_1k_tokens(model);
    }

    /// Record an embedding failure for `topic`.
    pub fn record_error(&mut self, topic: &str) {
        self.stats
            .entry(topic.to_string())
            .or_insert_with(|| TopicEmbedStats::new(topic, ""))
            .total_errors += 1;
    }

    /// Get stats for a single topic.
    pub fn get_stats(&self, topic: &str) -> Option<TopicEmbedStats> {
        self.stats.get(topic).cloned()
    }

    /// Snapshot of stats for all tracked topics.
    pub fn get_all_stats(&self) -> Vec<TopicEmbedStats> {
        self.stats.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_ledger_returns_none() {
        let ledger = CostLedger::new();
        assert!(ledger.get_stats("nope").is_none());
        assert!(ledger.get_all_stats().is_empty());
    }

    #[test]
    fn record_embed_increments_stats() {
        let mut ledger = CostLedger::new();
        ledger.record_embed("orders", 400, "bge-small");
        ledger.record_embed("orders", 800, "bge-small");

        let stats = ledger.get_stats("orders").unwrap();
        assert_eq!(stats.total_records_embedded, 2);
        assert_eq!(stats.total_bytes_embedded, 1200);
        assert_eq!(stats.total_errors, 0);
        assert_eq!(stats.model, "bge-small");
        assert!(stats.estimated_cost_usd > 0.0);
    }

    #[test]
    fn record_error_increments_error_count() {
        let mut ledger = CostLedger::new();
        ledger.record_embed("events", 100, "openai");
        ledger.record_error("events");
        ledger.record_error("events");

        let stats = ledger.get_stats("events").unwrap();
        assert_eq!(stats.total_records_embedded, 1);
        assert_eq!(stats.total_errors, 2);
    }

    #[test]
    fn get_all_stats_returns_all_topics() {
        let mut ledger = CostLedger::new();
        ledger.record_embed("alpha", 100, "bge-small");
        ledger.record_embed("beta", 200, "openai");
        ledger.record_embed("gamma", 300, "bge-small");

        let all = ledger.get_all_stats();
        assert_eq!(all.len(), 3);
        let topics: Vec<&str> = all.iter().map(|s| s.topic.as_str()).collect();
        assert!(topics.contains(&"alpha"));
        assert!(topics.contains(&"beta"));
        assert!(topics.contains(&"gamma"));
    }

    #[test]
    fn cost_estimation_uses_model_table() {
        let mut ledger = CostLedger::new();
        // 4000 bytes ≈ 1000 tokens
        ledger.record_embed("t1", 4000, "bge-small");
        ledger.record_embed("t2", 4000, "openai");

        let bge = ledger.get_stats("t1").unwrap();
        let oai = ledger.get_stats("t2").unwrap();

        // bge-small: 1000 tokens / 1000 * 0.00001 = 0.00001
        assert!((bge.estimated_cost_usd - 0.00001).abs() < 1e-10);
        // openai:    1000 tokens / 1000 * 0.00002 = 0.00002
        assert!((oai.estimated_cost_usd - 0.00002).abs() < 1e-10);
    }

    #[test]
    fn unknown_model_has_zero_cost() {
        let mut ledger = CostLedger::new();
        ledger.record_embed("t", 4000, "custom-local");
        let stats = ledger.get_stats("t").unwrap();
        assert_eq!(stats.estimated_cost_usd, 0.0);
    }

    #[test]
    fn model_change_updates_stored_model() {
        let mut ledger = CostLedger::new();
        ledger.record_embed("t", 100, "bge-small");
        assert_eq!(ledger.get_stats("t").unwrap().model, "bge-small");

        ledger.record_embed("t", 100, "openai");
        assert_eq!(ledger.get_stats("t").unwrap().model, "openai");
    }
}
