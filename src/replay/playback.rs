//! Traffic playback/replay functionality

use crate::error::{Result, StreamlineError};
use crate::replay::storage::Recording;
use crate::replay::transform::MutatorChain;
use crate::storage::TopicManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Replay mode
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayMode {
    /// Replay at original timing
    #[default]
    RealTime,
    /// Replay as fast as possible
    Fast,
    /// Step through messages one at a time
    StepByStep,
    /// Paused (manual step only)
    Paused,
}

/// Configuration for traffic replay
#[derive(Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    /// Path to the recording
    pub recording_path: String,

    /// Target topic manager (where to replay to)
    #[serde(skip)]
    pub topic_manager: Option<Arc<TopicManager>>,

    /// Replay mode
    #[serde(default)]
    pub mode: ReplayMode,

    /// Speed multiplier (for RealTime mode, 1.0 = normal speed)
    #[serde(default = "default_speed")]
    pub speed: f64,

    /// Topic mapping (original topic -> target topic)
    #[serde(default)]
    pub topic_mapping: HashMap<String, String>,

    /// Topics to replay (empty = all topics in recording)
    #[serde(default)]
    pub topics: Vec<String>,

    /// Start from specific offset (per topic-partition)
    #[serde(default)]
    pub start_offsets: HashMap<String, i64>,

    /// Stop at specific offset (per topic-partition)
    #[serde(default)]
    pub stop_offsets: HashMap<String, i64>,

    /// Loop replay when finished
    #[serde(default)]
    pub loop_replay: bool,

    /// Apply mutations during replay
    #[serde(default)]
    pub apply_mutations: bool,

    /// Breakpoint offsets (pause at these offsets)
    #[serde(default)]
    pub breakpoints: HashMap<String, Vec<i64>>,
}

fn default_speed() -> f64 {
    1.0
}

impl std::fmt::Debug for ReplayConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayConfig")
            .field("recording_path", &self.recording_path)
            .field(
                "topic_manager",
                &self.topic_manager.as_ref().map(|_| "<TopicManager>"),
            )
            .field("mode", &self.mode)
            .field("speed", &self.speed)
            .field("topic_mapping", &self.topic_mapping)
            .field("topics", &self.topics)
            .field("start_offsets", &self.start_offsets)
            .field("stop_offsets", &self.stop_offsets)
            .field("loop_replay", &self.loop_replay)
            .field("apply_mutations", &self.apply_mutations)
            .field("breakpoints", &self.breakpoints)
            .finish()
    }
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            recording_path: String::new(),
            topic_manager: None,
            mode: ReplayMode::default(),
            speed: 1.0,
            topic_mapping: HashMap::new(),
            topics: Vec::new(),
            start_offsets: HashMap::new(),
            stop_offsets: HashMap::new(),
            loop_replay: false,
            apply_mutations: false,
            breakpoints: HashMap::new(),
        }
    }
}

impl ReplayConfig {
    /// Create a new replay config
    pub fn new(recording_path: impl Into<String>) -> Self {
        Self {
            recording_path: recording_path.into(),
            ..Default::default()
        }
    }

    /// Set replay mode
    pub fn with_mode(mut self, mode: ReplayMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set replay speed
    pub fn with_speed(mut self, speed: f64) -> Self {
        self.speed = speed.max(0.01);
        self
    }

    /// Add topic mapping
    pub fn with_topic_mapping(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.topic_mapping.insert(from.into(), to.into());
        self
    }

    /// Set topics to replay
    pub fn with_topics(mut self, topics: Vec<String>) -> Self {
        self.topics = topics;
        self
    }

    /// Enable loop replay
    pub fn with_loop(mut self) -> Self {
        self.loop_replay = true;
        self
    }

    /// Add a breakpoint
    pub fn with_breakpoint(mut self, topic_partition: impl Into<String>, offset: i64) -> Self {
        let key = topic_partition.into();
        self.breakpoints.entry(key).or_default().push(offset);
        self
    }

    /// Set the topic manager
    pub fn with_topic_manager(mut self, tm: Arc<TopicManager>) -> Self {
        self.topic_manager = Some(tm);
        self
    }
}

/// Statistics about replay
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplayerStats {
    /// Total records replayed
    pub records_replayed: u64,
    /// Total bytes replayed
    pub bytes_replayed: u64,
    /// Current position (record index)
    pub current_position: u64,
    /// Total records in recording
    pub total_records: u64,
    /// Progress percentage
    pub progress_percent: f64,
    /// Errors encountered
    pub errors: u64,
    /// Whether replay is active
    pub active: bool,
    /// Current mode
    pub mode: ReplayMode,
    /// Current speed
    pub speed: f64,
    /// Replay start time
    pub start_time: i64,
    /// Elapsed time in milliseconds
    pub elapsed_ms: i64,
}

/// Traffic replayer for replaying recorded traffic
pub struct TrafficReplayer {
    config: ReplayConfig,
    recording: Option<Recording>,
    active: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    stats: Arc<RwLock<ReplayerStats>>,
    mutator_chain: Option<MutatorChain>,
    shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
    /// Channel for step-by-step replay mode (planned feature)
    #[allow(dead_code)]
    step_tx: Option<tokio::sync::mpsc::Sender<()>>,
    /// Receiver for step-by-step replay mode (planned feature)
    #[allow(dead_code)]
    step_rx: Option<tokio::sync::mpsc::Receiver<()>>,
}

impl TrafficReplayer {
    /// Create a new traffic replayer
    pub fn new(config: ReplayConfig) -> Result<Self> {
        // Open the recording
        let recording = Recording::open(&config.recording_path)?;

        let (step_tx, step_rx) = tokio::sync::mpsc::channel(1);

        Ok(Self {
            config,
            recording: Some(recording),
            active: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(RwLock::new(ReplayerStats::default())),
            mutator_chain: None,
            shutdown_tx: None,
            step_tx: Some(step_tx),
            step_rx: Some(step_rx),
        })
    }

    /// Set a mutator chain for message transformation
    pub fn with_mutators(mut self, chain: MutatorChain) -> Self {
        self.mutator_chain = Some(chain);
        self
    }

    /// Start replay
    pub async fn start(&mut self) -> Result<()> {
        if self.active.load(Ordering::SeqCst) {
            return Err(StreamlineError::storage_msg(
                "Replay already active".to_string(),
            ));
        }

        let Some(ref recording) = self.recording else {
            return Err(StreamlineError::storage_msg(
                "No recording loaded".to_string(),
            ));
        };

        info!(
            recording = %self.config.recording_path,
            mode = ?self.config.mode,
            speed = self.config.speed,
            "Starting replay"
        );

        // Initialize stats
        {
            let mut stats = self.stats.write().await;
            stats.total_records = recording.metadata.record_count;
            stats.mode = self.config.mode;
            stats.speed = self.config.speed;
            stats.start_time = chrono::Utc::now().timestamp_millis();
            stats.active = true;
        }

        self.active.store(true, Ordering::SeqCst);

        // Set up shutdown channel
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        self.shutdown_tx = Some(shutdown_tx);

        // Start replay task
        self.run_replay().await
    }

    /// Run the replay loop
    async fn run_replay(&mut self) -> Result<()> {
        let Some(ref recording) = self.recording else {
            return Err(StreamlineError::storage_msg(
                "No recording loaded".to_string(),
            ));
        };

        let messages = recording.messages()?;
        let mut last_timestamp: Option<i64> = None;

        for msg_result in messages {
            // Check if stopped
            if !self.active.load(Ordering::SeqCst) {
                break;
            }

            // Check if paused
            while self.paused.load(Ordering::SeqCst) {
                if !self.active.load(Ordering::SeqCst) {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }

            let mut msg = match msg_result {
                Ok(m) => m,
                Err(e) => {
                    error!(error = %e, "Error reading recorded message");
                    let mut stats = self.stats.write().await;
                    stats.errors += 1;
                    continue;
                }
            };

            // Check topic filter
            if !self.config.topics.is_empty() && !self.config.topics.contains(&msg.topic) {
                continue;
            }

            // Check stop offset
            let tp_key = format!("{}-{}", msg.topic, msg.partition);
            if let Some(&stop_offset) = self.config.stop_offsets.get(&tp_key) {
                if msg.offset >= stop_offset {
                    continue;
                }
            }

            // Check start offset
            if let Some(&start_offset) = self.config.start_offsets.get(&tp_key) {
                if msg.offset < start_offset {
                    continue;
                }
            }

            // Apply timing
            if self.config.mode == ReplayMode::RealTime {
                if let Some(last_ts) = last_timestamp {
                    let delay_ms = ((msg.timestamp - last_ts) as f64 / self.config.speed) as u64;
                    if delay_ms > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }
                }
                last_timestamp = Some(msg.timestamp);
            }

            // Check breakpoints
            if let Some(breakpoints) = self.config.breakpoints.get(&tp_key) {
                if breakpoints.contains(&msg.offset) {
                    info!(
                        topic = %msg.topic,
                        partition = msg.partition,
                        offset = msg.offset,
                        "Hit breakpoint"
                    );
                    self.paused.store(true, Ordering::SeqCst);

                    // Wait for step or resume
                    while self.paused.load(Ordering::SeqCst) {
                        if !self.active.load(Ordering::SeqCst) {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }

            // Apply mutations if configured
            if self.config.apply_mutations {
                if let Some(ref chain) = self.mutator_chain {
                    msg = chain.apply(msg)?;
                }
            }

            // Get target topic
            let target_topic = self
                .config
                .topic_mapping
                .get(&msg.topic)
                .cloned()
                .unwrap_or_else(|| msg.topic.clone());

            // Replay to target
            if let Some(ref tm) = self.config.topic_manager {
                let record = msg.to_record()?;
                let _ = tm.get_or_create_topic(&target_topic, 1);

                match tm.append_with_headers(
                    &target_topic,
                    msg.partition,
                    record.key,
                    record.value.clone(),
                    record.headers,
                ) {
                    Ok(offset) => {
                        debug!(
                            topic = %target_topic,
                            partition = msg.partition,
                            offset = offset,
                            "Replayed message"
                        );
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to replay message");
                        let mut stats = self.stats.write().await;
                        stats.errors += 1;
                    }
                }
            }

            // Update stats
            {
                let mut stats = self.stats.write().await;
                stats.records_replayed += 1;
                stats.current_position += 1;
                stats.progress_percent = if stats.total_records > 0 {
                    (stats.records_replayed as f64 / stats.total_records as f64) * 100.0
                } else {
                    0.0
                };
            }

            // Step-by-step mode - wait for next step
            if self.config.mode == ReplayMode::StepByStep {
                self.paused.store(true, Ordering::SeqCst);
                while self.paused.load(Ordering::SeqCst) && self.active.load(Ordering::SeqCst) {
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }

        // Mark complete
        {
            let mut stats = self.stats.write().await;
            stats.active = false;
            stats.elapsed_ms = chrono::Utc::now().timestamp_millis() - stats.start_time;
        }

        self.active.store(false, Ordering::SeqCst);

        info!(
            records = self.stats.read().await.records_replayed,
            "Replay complete"
        );

        Ok(())
    }

    /// Stop replay
    pub async fn stop(&mut self) -> Result<ReplayerStats> {
        info!("Stopping replay");

        // Signal shutdown
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        self.active.store(false, Ordering::SeqCst);
        self.paused.store(false, Ordering::SeqCst);

        let stats = self.stats.read().await.clone();
        Ok(stats)
    }

    /// Pause replay
    pub fn pause(&self) {
        info!("Pausing replay");
        self.paused.store(true, Ordering::SeqCst);
    }

    /// Resume replay
    pub fn resume(&self) {
        info!("Resuming replay");
        self.paused.store(false, Ordering::SeqCst);
    }

    /// Step to next message (when paused or in step mode)
    pub fn step(&self) {
        if self.paused.load(Ordering::SeqCst) {
            // Briefly unpause to allow one message
            self.paused.store(false, Ordering::SeqCst);
        }
    }

    /// Change replay speed
    pub async fn set_speed(&self, speed: f64) {
        let speed = speed.max(0.01);
        let mut stats = self.stats.write().await;
        stats.speed = speed;
    }

    /// Change replay mode
    pub async fn set_mode(&self, mode: ReplayMode) {
        let mut stats = self.stats.write().await;
        stats.mode = mode;

        if mode == ReplayMode::Paused {
            self.paused.store(true, Ordering::SeqCst);
        }
    }

    /// Check if replay is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Check if replay is paused
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// Get current stats
    pub async fn stats(&self) -> ReplayerStats {
        let mut stats = self.stats.read().await.clone();
        if stats.active {
            stats.elapsed_ms = chrono::Utc::now().timestamp_millis() - stats.start_time;
        }
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::storage::RecordingMetadata;
    use crate::storage::Record;
    use bytes::Bytes;
    use tempfile::TempDir;

    #[test]
    fn test_replay_config_default() {
        let config = ReplayConfig::default();
        assert_eq!(config.speed, 1.0);
        assert_eq!(config.mode, ReplayMode::RealTime);
        assert!(!config.loop_replay);
    }

    #[test]
    fn test_replay_config_builder() {
        let config = ReplayConfig::new("./recording")
            .with_mode(ReplayMode::Fast)
            .with_speed(2.0)
            .with_topic_mapping("events", "events-replay")
            .with_loop()
            .with_breakpoint("events-0", 100);

        assert_eq!(config.recording_path, "./recording");
        assert_eq!(config.mode, ReplayMode::Fast);
        assert_eq!(config.speed, 2.0);
        assert!(config.loop_replay);
        assert!(config.topic_mapping.contains_key("events"));
        assert!(config.breakpoints.contains_key("events-0"));
    }

    #[tokio::test]
    async fn test_replayer_creation() {
        let temp_dir = TempDir::new().unwrap();
        let recording_dir = temp_dir.path().join("test-recording");

        // Create a recording first
        let metadata = RecordingMetadata::new("test");
        let mut recording = Recording::create(&recording_dir, metadata).unwrap();

        for i in 0..5 {
            let record = Record::new(
                i,
                1234567890 + i * 1000,
                None,
                Bytes::from(format!("value-{}", i)),
            );
            let msg = crate::replay::storage::RecordedMessage::from_record(&record, "events", 0);
            recording.write(&msg).unwrap();
        }
        recording.close().unwrap();

        // Create replayer
        let config = ReplayConfig::new(recording_dir.to_str().unwrap());
        let replayer = TrafficReplayer::new(config).unwrap();

        assert!(!replayer.is_active());
    }

    #[test]
    fn test_replay_mode_default() {
        let mode = ReplayMode::default();
        assert_eq!(mode, ReplayMode::RealTime);
    }
}
