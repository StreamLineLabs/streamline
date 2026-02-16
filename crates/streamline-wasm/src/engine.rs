//! TransformationEngine - central coordinator for WASM message transformations
//!
//! The engine ties together the [`TransformationRegistry`] and [`WasmRuntime`]
//! to apply registered transformations during message production. It supports
//! single-message and batch processing, chaining of multiple transformations,
//! and collection of processing metrics.

use super::runtime::WasmRuntime;
use super::{TransformInput, TransformOutput, TransformationRegistry};
use crate::error::Result;
use serde::Serialize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

/// Central coordinator that applies WASM transformations during message production.
///
/// Holds references to a [`TransformationRegistry`] (which maps topics to
/// transformation configs) and a [`WasmRuntime`] (which executes individual
/// WASM modules). It provides single-message and batch processing with
/// chaining support, where the output of one transformation feeds into the
/// next.
pub struct TransformationEngine {
    /// Transformation registry (topic → config mapping)
    registry: Arc<TransformationRegistry>,

    /// WASM runtime for executing modules
    runtime: Arc<WasmRuntime>,

    /// Whether the engine is enabled
    enabled: AtomicBool,

    /// Aggregate processing statistics
    stats: EngineStats,
}

/// Aggregate engine-level statistics backed by atomic counters.
struct EngineStats {
    messages_processed: AtomicU64,
    messages_passed: AtomicU64,
    messages_dropped: AtomicU64,
    messages_transformed: AtomicU64,
    messages_routed: AtomicU64,
    errors: AtomicU64,
    total_time_us: AtomicU64,
}

impl EngineStats {
    fn new() -> Self {
        Self {
            messages_processed: AtomicU64::new(0),
            messages_passed: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
            messages_transformed: AtomicU64::new(0),
            messages_routed: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            total_time_us: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> EngineStatsSnapshot {
        let processed = self.messages_processed.load(Ordering::Relaxed);
        let total_us = self.total_time_us.load(Ordering::Relaxed);
        let avg_us = if processed > 0 {
            total_us as f64 / processed as f64
        } else {
            0.0
        };

        EngineStatsSnapshot {
            messages_processed: processed,
            messages_passed: self.messages_passed.load(Ordering::Relaxed),
            messages_dropped: self.messages_dropped.load(Ordering::Relaxed),
            messages_transformed: self.messages_transformed.load(Ordering::Relaxed),
            messages_routed: self.messages_routed.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            total_time_us: total_us,
            avg_time_us: avg_us,
        }
    }
}

/// Point-in-time snapshot of engine processing statistics.
#[derive(Debug, Clone, Serialize)]
pub struct EngineStatsSnapshot {
    /// Total messages processed
    pub messages_processed: u64,
    /// Messages passed through unchanged
    pub messages_passed: u64,
    /// Messages dropped by filters
    pub messages_dropped: u64,
    /// Messages transformed
    pub messages_transformed: u64,
    /// Messages routed to different topics
    pub messages_routed: u64,
    /// Errors encountered during processing
    pub errors: u64,
    /// Total processing time in microseconds
    pub total_time_us: u64,
    /// Average processing time per message in microseconds
    pub avg_time_us: f64,
}

impl TransformationEngine {
    /// Create a new `TransformationEngine`.
    pub fn new(registry: Arc<TransformationRegistry>, runtime: Arc<WasmRuntime>) -> Self {
        info!("TransformationEngine initialized");
        Self {
            registry,
            runtime,
            enabled: AtomicBool::new(true),
            stats: EngineStats::new(),
        }
    }

    /// Returns `true` if the engine is currently enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Enable or disable the engine.
    pub fn set_enabled(&self, enabled: bool) {
        let prev = self.enabled.swap(enabled, Ordering::Relaxed);
        if prev != enabled {
            info!(enabled, "TransformationEngine enabled state changed");
        }
    }

    /// Process a single message through all registered transformations for its topic.
    ///
    /// Transformations are applied in order (chained): the output of one
    /// transformation becomes the input for the next. If any transformation
    /// drops the message or returns an error, the chain short-circuits.
    pub async fn process_message(&self, input: TransformInput) -> Result<TransformOutput> {
        if !self.is_enabled() {
            debug!(topic = %input.topic, "Engine disabled, passing message through");
            return Ok(TransformOutput::Pass(input));
        }

        let start = Instant::now();
        let topic = input.topic.clone();

        let configs = self.registry.get_for_topic(&topic).await;
        if configs.is_empty() {
            debug!(topic = %topic, "No transformations registered for topic");
            self.stats
                .messages_processed
                .fetch_add(1, Ordering::Relaxed);
            self.stats.messages_passed.fetch_add(1, Ordering::Relaxed);
            let elapsed = start.elapsed().as_micros() as u64;
            self.stats
                .total_time_us
                .fetch_add(elapsed, Ordering::Relaxed);
            return Ok(TransformOutput::Pass(input));
        }

        debug!(topic = %topic, count = configs.len(), "Applying transformations");

        let mut current_input = input;

        for config in &configs {
            let result = self
                .runtime
                .execute(&config.id, current_input.clone())
                .await;

            match result {
                Ok(output) => {
                    // Update per-transformation stats in the registry
                    match &output {
                        TransformOutput::Pass(_) => {
                            self.registry
                                .update_stats(&config.id, 1, 1, 0, 0, 0, 0, 0)
                                .await;
                        }
                        TransformOutput::Drop => {
                            self.registry
                                .update_stats(&config.id, 1, 0, 1, 0, 0, 0, 0)
                                .await;
                            let elapsed = start.elapsed().as_micros() as u64;
                            self.stats
                                .messages_processed
                                .fetch_add(1, Ordering::Relaxed);
                            self.stats.messages_dropped.fetch_add(1, Ordering::Relaxed);
                            self.stats
                                .total_time_us
                                .fetch_add(elapsed, Ordering::Relaxed);
                            return Ok(TransformOutput::Drop);
                        }
                        TransformOutput::Transformed(results) => {
                            self.registry
                                .update_stats(&config.id, 1, 0, 0, 1, 0, 0, 0)
                                .await;

                            // Chain: use the first transformed result as next input
                            if let Some(first) = results.first() {
                                current_input = TransformInput {
                                    topic: current_input.topic.clone(),
                                    partition: current_input.partition,
                                    key: first.key.clone(),
                                    value: first.value.clone(),
                                    headers: first.headers.clone(),
                                    timestamp_ms: current_input.timestamp_ms,
                                };
                            }
                        }
                        TransformOutput::Route(_) => {
                            self.registry
                                .update_stats(&config.id, 1, 0, 0, 0, 1, 0, 0)
                                .await;
                            // Route terminates the chain – return immediately
                            let elapsed = start.elapsed().as_micros() as u64;
                            self.stats
                                .messages_processed
                                .fetch_add(1, Ordering::Relaxed);
                            self.stats.messages_routed.fetch_add(1, Ordering::Relaxed);
                            self.stats
                                .total_time_us
                                .fetch_add(elapsed, Ordering::Relaxed);
                            return Ok(output);
                        }
                        TransformOutput::Error(msg) => {
                            self.registry
                                .update_stats(&config.id, 1, 0, 0, 0, 0, 1, 0)
                                .await;
                            warn!(
                                transform_id = %config.id,
                                error = %msg,
                                "Transformation returned error, short-circuiting chain"
                            );
                            let elapsed = start.elapsed().as_micros() as u64;
                            self.stats
                                .messages_processed
                                .fetch_add(1, Ordering::Relaxed);
                            self.stats.errors.fetch_add(1, Ordering::Relaxed);
                            self.stats
                                .total_time_us
                                .fetch_add(elapsed, Ordering::Relaxed);
                            return Ok(output);
                        }
                    }
                }
                Err(e) => {
                    self.registry
                        .update_stats(&config.id, 1, 0, 0, 0, 0, 1, 0)
                        .await;
                    error!(
                        transform_id = %config.id,
                        error = %e,
                        "Runtime error executing transformation"
                    );
                    let elapsed = start.elapsed().as_micros() as u64;
                    self.stats
                        .messages_processed
                        .fetch_add(1, Ordering::Relaxed);
                    self.stats.errors.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .total_time_us
                        .fetch_add(elapsed, Ordering::Relaxed);
                    return Err(e);
                }
            }
        }

        // All transformations applied; determine final output category
        let elapsed = start.elapsed().as_micros() as u64;
        self.stats
            .messages_processed
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .messages_transformed
            .fetch_add(1, Ordering::Relaxed);
        self.stats
            .total_time_us
            .fetch_add(elapsed, Ordering::Relaxed);

        Ok(TransformOutput::Pass(current_input))
    }

    /// Process a batch of messages, returning one output per input.
    ///
    /// Each message is processed independently through the full transformation
    /// chain. Errors on individual messages do not abort the batch; instead the
    /// corresponding entry is set to [`TransformOutput::Error`].
    pub async fn process_batch(&self, inputs: Vec<TransformInput>) -> Vec<Result<TransformOutput>> {
        let mut results = Vec::with_capacity(inputs.len());

        for input in inputs {
            results.push(self.process_message(input).await);
        }

        results
    }

    /// Return a point-in-time snapshot of the engine's aggregate statistics.
    pub fn stats(&self) -> EngineStatsSnapshot {
        self.stats.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Helper: build a minimal engine with a fresh registry and runtime.
    fn make_engine() -> (TransformationEngine, Arc<TransformationRegistry>) {
        let registry = Arc::new(TransformationRegistry::new());
        let runtime = Arc::new(WasmRuntime::new().expect("WasmRuntime::new"));
        let engine = TransformationEngine::new(Arc::clone(&registry), runtime);
        (engine, registry)
    }

    /// Helper: build a [`TransformInput`] for `topic`.
    fn make_input(topic: &str) -> TransformInput {
        TransformInput {
            topic: topic.to_string(),
            partition: 0,
            key: None,
            value: b"hello".to_vec(),
            headers: HashMap::new(),
            timestamp_ms: 0,
        }
    }

    #[tokio::test]
    async fn test_disabled_engine_passes_through() {
        let (engine, _registry) = make_engine();
        engine.set_enabled(false);
        assert!(!engine.is_enabled());

        let output = engine.process_message(make_input("topic-a")).await.unwrap();
        assert!(matches!(output, TransformOutput::Pass(_)));

        let stats = engine.stats();
        // Disabled path skips stat recording
        assert_eq!(stats.messages_processed, 0);
    }

    #[tokio::test]
    async fn test_no_transforms_passes_through() {
        let (engine, _registry) = make_engine();

        let output = engine.process_message(make_input("topic-a")).await.unwrap();
        assert!(matches!(output, TransformOutput::Pass(_)));

        let stats = engine.stats();
        assert_eq!(stats.messages_processed, 1);
        assert_eq!(stats.messages_passed, 1);
    }

    #[tokio::test]
    async fn test_process_batch_returns_per_message_results() {
        let (engine, _registry) = make_engine();

        let inputs = vec![
            make_input("topic-a"),
            make_input("topic-b"),
            make_input("topic-c"),
        ];

        let results = engine.process_batch(inputs).await;
        assert_eq!(results.len(), 3);
        for r in &results {
            assert!(r.is_ok());
            assert!(matches!(r.as_ref().unwrap(), TransformOutput::Pass(_)));
        }

        let stats = engine.stats();
        assert_eq!(stats.messages_processed, 3);
        assert_eq!(stats.messages_passed, 3);
    }

    #[tokio::test]
    async fn test_stats_snapshot_accuracy() {
        let (engine, _registry) = make_engine();

        // Process several messages to accumulate stats
        for _ in 0..5 {
            engine.process_message(make_input("topic-x")).await.unwrap();
        }

        let stats = engine.stats();
        assert_eq!(stats.messages_processed, 5);
        assert_eq!(stats.messages_passed, 5);
        assert_eq!(stats.messages_dropped, 0);
        assert_eq!(stats.messages_transformed, 0);
        assert_eq!(stats.errors, 0);
        assert!(stats.avg_time_us >= 0.0);
    }

    #[tokio::test]
    async fn test_enable_disable_toggle() {
        let (engine, _registry) = make_engine();

        assert!(engine.is_enabled());
        engine.set_enabled(false);
        assert!(!engine.is_enabled());
        engine.set_enabled(true);
        assert!(engine.is_enabled());
    }
}
