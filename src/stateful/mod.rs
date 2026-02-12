//! Stateful Stream Operators
//!
//! Provides stateful operators for stream processing including:
//! - Windowing (tumbling, sliding, session, hopping)
//! - Aggregations (count, sum, avg, min, max, custom)
//! - Joins (inner, left, outer, temporal)
//! - State management (local, distributed, checkpointing)

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

pub mod aggregations;
pub mod checkpoints;
pub mod joins;
pub mod state;
pub mod windows;

pub use checkpoints::{Checkpoint, CheckpointConfig, CheckpointManager};
pub use state::{StateBackend, StateConfig, StateManager};
pub use windows::{Window, WindowConfig, WindowManager, WindowOutput};

/// Stateful operator error
#[derive(Debug, Clone, thiserror::Error)]
pub enum OperatorError {
    /// State error
    #[error("State error: {0}")]
    StateError(String),
    /// Window error
    #[error("Window error: {0}")]
    WindowError(String),
    /// Aggregation error
    #[error("Aggregation error: {0}")]
    AggregationError(String),
    /// Join error
    #[error("Join error: {0}")]
    JoinError(String),
    /// Checkpoint error
    #[error("Checkpoint error: {0}")]
    CheckpointError(String),
    /// Serialization error
    #[error("Serialization error: {0}")]
    SerializationError(String),
    /// Timeout error
    #[error("Timeout: {0}")]
    Timeout(String),
}

/// Stateful operator configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperatorConfig {
    /// Operator name
    pub name: String,
    /// Parallelism level
    pub parallelism: usize,
    /// State backend type
    pub state_backend: StateBackendType,
    /// Checkpoint interval (ms)
    pub checkpoint_interval_ms: u64,
    /// Processing time vs event time
    pub time_semantics: TimeSemantics,
    /// Watermark strategy
    pub watermark_strategy: WatermarkStrategy,
    /// Late data handling
    pub late_data_handling: LateDataHandling,
}

impl Default for OperatorConfig {
    fn default() -> Self {
        Self {
            name: "stateful-operator".to_string(),
            parallelism: 1,
            state_backend: StateBackendType::Memory,
            checkpoint_interval_ms: 60000, // 1 minute
            time_semantics: TimeSemantics::ProcessingTime,
            watermark_strategy: WatermarkStrategy::BoundedOutOfOrderness {
                max_out_of_orderness_ms: 5000,
            },
            late_data_handling: LateDataHandling::Drop,
        }
    }
}

/// State backend type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum StateBackendType {
    /// In-memory state
    #[default]
    Memory,
    /// RocksDB-backed state
    RocksDb,
    /// Distributed state (requires clustering)
    Distributed,
}

/// Time semantics
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TimeSemantics {
    /// Processing time (wall clock)
    #[default]
    ProcessingTime,
    /// Event time (from record timestamp)
    EventTime,
    /// Ingestion time (when received)
    IngestionTime,
}

/// Watermark strategy
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum WatermarkStrategy {
    /// No watermarks (processing time only)
    #[default]
    None,
    /// Periodic watermarks
    Periodic { interval_ms: u64 },
    /// Bounded out-of-orderness
    BoundedOutOfOrderness { max_out_of_orderness_ms: u64 },
    /// Punctuated (from special records)
    Punctuated,
}

/// Late data handling
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum LateDataHandling {
    /// Drop late data
    #[default]
    Drop,
    /// Include in current window
    Include,
    /// Side output
    SideOutput,
    /// Recompute window
    Recompute,
}

/// Stream element with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamElement<T> {
    /// Element value
    pub value: T,
    /// Event timestamp (milliseconds)
    pub timestamp: i64,
    /// Key for keyed streams
    pub key: Option<String>,
    /// Partition
    pub partition: u32,
    /// Offset
    pub offset: i64,
    /// Headers
    pub headers: HashMap<String, String>,
}

impl<T> StreamElement<T> {
    /// Create a new stream element
    pub fn new(value: T, timestamp: i64) -> Self {
        Self {
            value,
            timestamp,
            key: None,
            partition: 0,
            offset: 0,
            headers: HashMap::new(),
        }
    }

    /// Create with key
    pub fn with_key(mut self, key: impl Into<String>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Create with partition
    pub fn with_partition(mut self, partition: u32) -> Self {
        self.partition = partition;
        self
    }

    /// Create with offset
    pub fn with_offset(mut self, offset: i64) -> Self {
        self.offset = offset;
        self
    }

    /// Add header
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

/// Watermark for event time processing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Watermark {
    /// Watermark timestamp
    pub timestamp: i64,
    /// Source partition
    pub partition: u32,
}

impl Watermark {
    /// Create a new watermark
    pub fn new(timestamp: i64) -> Self {
        Self {
            timestamp,
            partition: 0,
        }
    }

    /// Create with partition
    pub fn with_partition(mut self, partition: u32) -> Self {
        self.partition = partition;
        self
    }
}

/// Stateful operator processor
pub struct StatefulProcessor<T: Clone + Send + Sync + 'static> {
    /// Configuration
    config: OperatorConfig,
    /// State manager
    state_manager: StateManager,
    /// Window manager
    window_manager: WindowManager<T>,
    /// Checkpoint manager
    checkpoint_manager: CheckpointManager,
    /// Current watermark per partition
    watermarks: HashMap<u32, i64>,
    /// Late data buffer
    late_data: VecDeque<StreamElement<T>>,
    /// Statistics
    stats: ProcessorStats,
}

/// Processor statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProcessorStats {
    /// Elements processed
    pub elements_processed: u64,
    /// Late elements
    pub late_elements: u64,
    /// Windows triggered
    pub windows_triggered: u64,
    /// Checkpoints completed
    pub checkpoints_completed: u64,
    /// State size (bytes)
    pub state_size_bytes: u64,
    /// Last checkpoint time
    pub last_checkpoint_ms: i64,
    /// Processing latency (ms)
    pub avg_latency_ms: f64,
}

impl<T: Clone + Send + Sync + 'static> StatefulProcessor<T> {
    /// Create a new stateful processor
    pub fn new(config: OperatorConfig) -> Self {
        let state_config = StateConfig {
            backend: config.state_backend,
            max_state_size: 100 * 1024 * 1024, // 100MB
            ttl_ms: None,
        };

        let checkpoint_config = CheckpointConfig {
            interval_ms: config.checkpoint_interval_ms,
            min_pause_between_checkpoints_ms: 1000,
            timeout_ms: 300000, // 5 minutes
            max_concurrent_checkpoints: 1,
        };

        let window_config = WindowConfig::default();

        Self {
            config,
            state_manager: StateManager::new(state_config),
            window_manager: WindowManager::new(window_config),
            checkpoint_manager: CheckpointManager::new(checkpoint_config),
            watermarks: HashMap::new(),
            late_data: VecDeque::new(),
            stats: ProcessorStats::default(),
        }
    }

    /// Process an element
    pub fn process(
        &mut self,
        element: StreamElement<T>,
    ) -> Result<Vec<WindowOutput<T>>, OperatorError> {
        let process_start = chrono::Utc::now().timestamp_millis();

        // Check for late data
        if self.is_late(&element) {
            self.handle_late_data(element)?;
            return Ok(vec![]);
        }

        // Update watermark
        self.update_watermark(&element);

        // Add to window
        let outputs = self.window_manager.add(element)?;

        // Update stats
        self.stats.elements_processed += 1;
        let latency = chrono::Utc::now().timestamp_millis() - process_start;
        self.update_avg_latency(latency as f64);

        // Trigger windows based on watermark
        let mut triggered_outputs = self.trigger_windows()?;
        triggered_outputs.extend(outputs);

        self.stats.windows_triggered += triggered_outputs.len() as u64;

        Ok(triggered_outputs)
    }

    /// Process watermark
    pub fn process_watermark(
        &mut self,
        watermark: Watermark,
    ) -> Result<Vec<WindowOutput<T>>, OperatorError> {
        self.watermarks
            .insert(watermark.partition, watermark.timestamp);

        // Get minimum watermark across all partitions
        let min_watermark = *self.watermarks.values().min().unwrap_or(&0);

        // Trigger windows that are complete
        self.window_manager.advance_watermark(min_watermark)
    }

    /// Check if element is late
    fn is_late(&self, element: &StreamElement<T>) -> bool {
        if matches!(self.config.time_semantics, TimeSemantics::ProcessingTime) {
            return false;
        }

        let watermark = self
            .watermarks
            .get(&element.partition)
            .copied()
            .unwrap_or(0);
        element.timestamp < watermark
    }

    /// Handle late data
    fn handle_late_data(&mut self, element: StreamElement<T>) -> Result<(), OperatorError> {
        self.stats.late_elements += 1;

        match self.config.late_data_handling {
            LateDataHandling::Drop => {
                // Just drop it
            }
            LateDataHandling::Include => {
                // Add to current window
                self.window_manager.add(element)?;
            }
            LateDataHandling::SideOutput => {
                // Buffer for side output
                self.late_data.push_back(element);
                if self.late_data.len() > 10000 {
                    self.late_data.pop_front();
                }
            }
            LateDataHandling::Recompute => {
                // Mark window for recomputation
                self.window_manager.mark_for_recompute(element.timestamp)?;
            }
        }

        Ok(())
    }

    /// Update watermark based on element
    fn update_watermark(&mut self, element: &StreamElement<T>) {
        if matches!(self.config.time_semantics, TimeSemantics::ProcessingTime) {
            return;
        }

        let current_watermark = self
            .watermarks
            .get(&element.partition)
            .copied()
            .unwrap_or(0);
        let max_ooo = match &self.config.watermark_strategy {
            WatermarkStrategy::BoundedOutOfOrderness {
                max_out_of_orderness_ms,
            } => *max_out_of_orderness_ms as i64,
            _ => 0,
        };

        let new_watermark = element.timestamp - max_ooo;
        if new_watermark > current_watermark {
            self.watermarks.insert(element.partition, new_watermark);
        }
    }

    /// Trigger windows
    fn trigger_windows(&mut self) -> Result<Vec<WindowOutput<T>>, OperatorError> {
        let min_watermark = *self.watermarks.values().min().unwrap_or(&0);
        self.window_manager.trigger(min_watermark)
    }

    /// Update rolling average latency
    fn update_avg_latency(&mut self, latency: f64) {
        let n = self.stats.elements_processed as f64;
        self.stats.avg_latency_ms = ((self.stats.avg_latency_ms * (n - 1.0)) + latency) / n;
    }

    /// Create checkpoint
    pub fn checkpoint(&mut self) -> Result<Checkpoint, OperatorError> {
        let checkpoint = self.checkpoint_manager.create_checkpoint(
            &self.state_manager,
            &self.window_manager,
            &self.watermarks,
        )?;

        self.stats.checkpoints_completed += 1;
        self.stats.last_checkpoint_ms = chrono::Utc::now().timestamp_millis();

        Ok(checkpoint)
    }

    /// Restore from checkpoint
    pub fn restore(&mut self, checkpoint: &Checkpoint) -> Result<(), OperatorError> {
        self.checkpoint_manager.restore_checkpoint(
            checkpoint,
            &mut self.state_manager,
            &mut self.window_manager,
            &mut self.watermarks,
        )
    }

    /// Get statistics
    pub fn stats(&self) -> &ProcessorStats {
        &self.stats
    }

    /// Get late data (for side output)
    pub fn drain_late_data(&mut self) -> Vec<StreamElement<T>> {
        self.late_data.drain(..).collect()
    }

    /// Get current watermarks
    pub fn watermarks(&self) -> &HashMap<u32, i64> {
        &self.watermarks
    }
}

/// Keyed stream processor for per-key state
pub struct KeyedProcessor<K: Clone + Eq + std::hash::Hash, V: Clone + Send + Sync + 'static> {
    /// Per-key processors
    processors: HashMap<K, StatefulProcessor<V>>,
    /// Configuration
    config: OperatorConfig,
    /// Global stats
    global_stats: ProcessorStats,
}

impl<K: Clone + Eq + std::hash::Hash, V: Clone + Send + Sync + 'static> KeyedProcessor<K, V> {
    /// Create a new keyed processor
    pub fn new(config: OperatorConfig) -> Self {
        Self {
            processors: HashMap::new(),
            config,
            global_stats: ProcessorStats::default(),
        }
    }

    /// Process keyed element
    pub fn process(
        &mut self,
        key: K,
        element: StreamElement<V>,
    ) -> Result<Vec<WindowOutput<V>>, OperatorError> {
        let processor = self
            .processors
            .entry(key)
            .or_insert_with(|| StatefulProcessor::new(self.config.clone()));

        let outputs = processor.process(element)?;

        // Update global stats
        self.global_stats.elements_processed += 1;
        self.global_stats.windows_triggered += outputs.len() as u64;

        Ok(outputs)
    }

    /// Process watermark for all keys
    #[allow(clippy::type_complexity)]
    pub fn process_watermark(
        &mut self,
        watermark: Watermark,
    ) -> Result<Vec<(K, Vec<WindowOutput<V>>)>, OperatorError> {
        let mut results = Vec::new();

        for (key, processor) in &mut self.processors {
            let outputs = processor.process_watermark(watermark)?;
            if !outputs.is_empty() {
                results.push((key.clone(), outputs));
            }
        }

        Ok(results)
    }

    /// Get processor for key
    pub fn get(&self, key: &K) -> Option<&StatefulProcessor<V>> {
        self.processors.get(key)
    }

    /// Get mutable processor for key
    pub fn get_mut(&mut self, key: &K) -> Option<&mut StatefulProcessor<V>> {
        self.processors.get_mut(key)
    }

    /// Get global stats
    pub fn stats(&self) -> &ProcessorStats {
        &self.global_stats
    }

    /// Number of active keys
    pub fn key_count(&self) -> usize {
        self.processors.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operator_config_default() {
        let config = OperatorConfig::default();
        assert_eq!(config.parallelism, 1);
        assert_eq!(config.state_backend, StateBackendType::Memory);
    }

    #[test]
    fn test_stream_element() {
        let element = StreamElement::new("test", 1000)
            .with_key("key1")
            .with_partition(1)
            .with_offset(100)
            .with_header("h1", "v1");

        assert_eq!(element.value, "test");
        assert_eq!(element.timestamp, 1000);
        assert_eq!(element.key, Some("key1".to_string()));
        assert_eq!(element.partition, 1);
        assert_eq!(element.offset, 100);
        assert_eq!(element.headers.get("h1"), Some(&"v1".to_string()));
    }

    #[test]
    fn test_watermark() {
        let wm = Watermark::new(5000).with_partition(2);
        assert_eq!(wm.timestamp, 5000);
        assert_eq!(wm.partition, 2);
    }

    #[test]
    fn test_watermark_strategy_default() {
        let strategy = WatermarkStrategy::default();
        assert!(matches!(strategy, WatermarkStrategy::None));
    }

    #[test]
    fn test_late_data_handling_default() {
        let handling = LateDataHandling::default();
        assert_eq!(handling, LateDataHandling::Drop);
    }

    #[test]
    fn test_time_semantics_default() {
        let semantics = TimeSemantics::default();
        assert_eq!(semantics, TimeSemantics::ProcessingTime);
    }

    #[test]
    fn test_state_backend_type_default() {
        let backend = StateBackendType::default();
        assert_eq!(backend, StateBackendType::Memory);
    }
}
