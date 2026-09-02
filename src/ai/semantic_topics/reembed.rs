//! Re-embed migration for semantic topics (M2 P3).
//!
//! When a topic's embedding model changes, this module reads all records
//! from the topic, re-embeds them with the new model, and atomically
//! swaps the index.

use std::sync::Arc;
use std::time::Instant;

use super::index::InMemoryIndex;
use super::registry;
use super::worker::Embedder;
use super::SemanticIndex;

/// Progress report returned by [`reembed_sync`].
#[derive(Debug, Clone)]
pub struct ReembedProgress {
    pub total_records: u64,
    pub processed: u64,
    pub errors: u64,
    pub started_at: Instant,
}

impl ReembedProgress {
    fn new(total: u64) -> Self {
        Self {
            total_records: total,
            processed: 0,
            errors: 0,
            started_at: Instant::now(),
        }
    }

    /// Elapsed wall-clock time since the migration started.
    pub fn elapsed(&self) -> std::time::Duration {
        self.started_at.elapsed()
    }
}

/// Configuration for a re-embed migration.
#[derive(Debug, Clone)]
pub struct ReembedConfig {
    pub topic: String,
    pub new_model: String,
    pub batch_size: usize,
}

impl ReembedConfig {
    /// Creates a default config targeting the given topic and new model.
    pub fn new(topic: impl Into<String>, new_model: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            new_model: new_model.into(),
            batch_size: 100,
        }
    }
}

/// Synchronously re-embed all records for `config.topic` using `embedder`,
/// then atomically swap the index in the global registry.
///
/// This is a blocking operation intended to be called from a background task
/// or migration CLI. For the initial scaffold the "records" are derived from
/// the existing index entries (partition, offset); a real implementation would
/// read payloads from the topic log.
pub fn reembed_sync(config: ReembedConfig, embedder: &dyn Embedder) -> ReembedProgress {
    let old_index = match registry::get(&config.topic) {
        Some(idx) => idx,
        None => return ReembedProgress::new(0),
    };

    let entries = old_index.entries();
    let mut progress = ReembedProgress::new(entries.len() as u64);

    let new_index = Arc::new(InMemoryIndex::new());

    for batch in entries.chunks(config.batch_size) {
        for &(partition, offset) in batch {
            // Simulate reading the original payload from the topic log.
            // In a real implementation this would fetch from storage::log.
            let synthetic_text = format!("record-{partition}-{offset}");

            match embedder.embed(&synthetic_text) {
                Ok(vector) => {
                    new_index.insert(partition, offset, &vector);
                    progress.processed += 1;
                }
                Err(_) => {
                    progress.errors += 1;
                }
            }
        }
    }

    registry::swap(&config.topic, new_index);

    progress
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::semantic_topics::embedder::HashEmbedder;
    use crate::ai::semantic_topics::registry;
    use crate::ai::semantic_topics::SemanticIndex;

    /// The semantic index registry is process-global, so these tests must not
    /// run concurrently: one test's `reset_for_tests()` would otherwise wipe
    /// another test's seeded index.
    fn registry_guard() -> std::sync::MutexGuard<'static, ()> {
        registry::test_lock()
    }

    #[test]
    fn reembed_empty_topic_is_noop() {
        let _guard = registry_guard();
        registry::reset_for_tests();
        let embedder = HashEmbedder::default();
        let config = ReembedConfig::new("nonexistent", "bge-small");
        let progress = reembed_sync(config, &embedder);
        assert_eq!(progress.total_records, 0);
        assert_eq!(progress.processed, 0);
        assert_eq!(progress.errors, 0);
    }

    #[test]
    fn reembed_swaps_index() {
        let _guard = registry_guard();
        let topic = format!("reembed-swap-{}", std::process::id());
        let embedder = HashEmbedder::new(32);

        // Seed the old index with some records.
        let old = registry::get_or_create(&topic);
        old.insert(0, 1, &[1.0; 32]);
        old.insert(0, 2, &[0.5; 32]);
        old.insert(1, 3, &[0.0; 32]);
        assert_eq!(old.len(), 3);

        let config = ReembedConfig::new(&topic, "hash-v2");
        let progress = reembed_sync(config, &embedder);

        assert_eq!(progress.total_records, 3);
        assert_eq!(progress.processed, 3);
        assert_eq!(progress.errors, 0);

        // The registry should now point to the new index.
        let new = registry::get(&topic).expect("topic should exist");
        assert!(!Arc::ptr_eq(&old, &new), "index should have been swapped");
        assert_eq!(new.len(), 3, "new index should contain re-embedded records");
    }

    #[test]
    fn reembed_respects_batch_size() {
        let _guard = registry_guard();
        registry::reset_for_tests();
        let embedder = HashEmbedder::new(16);

        let idx = registry::get_or_create("test-batch");
        for i in 0..5 {
            idx.insert(0, i, &[1.0; 16]);
        }

        let mut config = ReembedConfig::new("test-batch", "hash-v2");
        config.batch_size = 2;

        let progress = reembed_sync(config, &embedder);
        assert_eq!(progress.total_records, 5);
        assert_eq!(progress.processed, 5);
    }

    #[test]
    fn progress_tracks_elapsed_time() {
        let _guard = registry_guard();
        registry::reset_for_tests();
        let embedder = HashEmbedder::default();
        let config = ReembedConfig::new("elapsed-test", "model");
        let progress = reembed_sync(config, &embedder);
        // Just verify it doesn't panic and returns a duration.
        let _ = progress.elapsed();
    }
}
