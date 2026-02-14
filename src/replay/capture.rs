//! Traffic capture/recording for replay

use crate::error::{Result, StreamlineError};
use crate::replay::storage::{RecordedMessage, Recording, RecordingMetadata};
use crate::storage::Record;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// Configuration for traffic recording
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordConfig {
    /// Topics to record (empty = all topics)
    #[serde(default)]
    pub topics: Vec<String>,

    /// Output directory for recordings
    pub output_dir: String,

    /// Recording name/description
    #[serde(default)]
    pub name: Option<String>,

    /// Environment label
    #[serde(default)]
    pub environment: Option<String>,

    /// Maximum duration in seconds (0 = unlimited)
    #[serde(default)]
    pub max_duration_secs: u64,

    /// Maximum records to capture (0 = unlimited)
    #[serde(default)]
    pub max_records: u64,

    /// Maximum bytes to capture (0 = unlimited)
    #[serde(default)]
    pub max_bytes: u64,

    /// Filter expression (topic pattern)
    #[serde(default)]
    pub filter: Option<String>,

    /// Sample rate (0.0-1.0, 1.0 = capture all)
    #[serde(default = "default_sample_rate")]
    pub sample_rate: f64,

    /// Include headers in recording
    #[serde(default = "default_include_headers")]
    pub include_headers: bool,
}

fn default_sample_rate() -> f64 {
    1.0
}

fn default_include_headers() -> bool {
    true
}

impl Default for RecordConfig {
    fn default() -> Self {
        Self {
            topics: Vec::new(),
            output_dir: "./recordings".to_string(),
            name: None,
            environment: None,
            max_duration_secs: 0,
            max_records: 0,
            max_bytes: 0,
            filter: None,
            sample_rate: 1.0,
            include_headers: true,
        }
    }
}

impl RecordConfig {
    /// Create a new config for specific topics
    pub fn for_topics(topics: Vec<String>) -> Self {
        Self {
            topics,
            ..Default::default()
        }
    }

    /// Set output directory
    pub fn with_output_dir(mut self, dir: impl Into<String>) -> Self {
        self.output_dir = dir.into();
        self
    }

    /// Set recording name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set sample rate
    pub fn with_sample_rate(mut self, rate: f64) -> Self {
        self.sample_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Set maximum duration
    pub fn with_max_duration(mut self, secs: u64) -> Self {
        self.max_duration_secs = secs;
        self
    }
}

/// Statistics about recording
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RecorderStats {
    /// Total records captured
    pub records_captured: u64,
    /// Total bytes captured
    pub bytes_captured: u64,
    /// Records skipped (due to sampling)
    pub records_skipped: u64,
    /// Errors encountered
    pub errors: u64,
    /// Recording start time
    pub start_time: i64,
    /// Current duration in milliseconds
    pub duration_ms: i64,
    /// Whether recording is active
    pub active: bool,
}

/// Traffic recorder for capturing production traffic
pub struct TrafficRecorder {
    config: RecordConfig,
    recording: Arc<RwLock<Option<Recording>>>,
    active: Arc<AtomicBool>,
    stats: Arc<RwLock<RecorderStats>>,
    topic_filter: HashSet<String>,
    shutdown_tx: Option<tokio::sync::broadcast::Sender<()>>,
}

impl TrafficRecorder {
    /// Create a new traffic recorder
    pub fn new(config: RecordConfig) -> Self {
        let topic_filter: HashSet<String> = config.topics.iter().cloned().collect();

        Self {
            config,
            recording: Arc::new(RwLock::new(None)),
            active: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(RwLock::new(RecorderStats::default())),
            topic_filter,
            shutdown_tx: None,
        }
    }

    /// Start recording
    pub async fn start(&mut self) -> Result<String> {
        if self.active.load(Ordering::SeqCst) {
            return Err(StreamlineError::storage_msg(
                "Recording already active".to_string(),
            ));
        }

        // Generate recording ID based on timestamp
        let recording_id = chrono::Utc::now().format("%Y%m%d_%H%M%S").to_string();
        let recording_dir = PathBuf::from(&self.config.output_dir).join(&recording_id);

        info!(
            recording_id = %recording_id,
            dir = %recording_dir.display(),
            topics = ?self.config.topics,
            "Starting traffic recording"
        );

        // Create recording
        let mut metadata =
            RecordingMetadata::new(&recording_id).with_topics(self.config.topics.clone());

        if let Some(ref name) = self.config.name {
            metadata = metadata.with_name(name);
        }

        if let Some(ref env) = self.config.environment {
            metadata = metadata.with_environment(env);
        }

        let recording = Recording::create(&recording_dir, metadata)?;

        // Store recording and activate
        {
            let mut rec = self.recording.write().await;
            *rec = Some(recording);
        }

        // Reset stats
        {
            let mut stats = self.stats.write().await;
            *stats = RecorderStats {
                start_time: chrono::Utc::now().timestamp_millis(),
                active: true,
                ..Default::default()
            };
        }

        self.active.store(true, Ordering::SeqCst);

        // Set up shutdown channel
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        self.shutdown_tx = Some(shutdown_tx.clone());

        // Start duration check task if max_duration is set
        if self.config.max_duration_secs > 0 {
            let active = self.active.clone();
            let max_duration = self.config.max_duration_secs;
            let mut shutdown_rx = shutdown_tx.subscribe();

            tokio::spawn(async move {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(max_duration)) => {
                        info!("Recording max duration reached");
                        active.store(false, Ordering::SeqCst);
                    }
                    _ = shutdown_rx.recv() => {}
                }
            });
        }

        Ok(recording_id)
    }

    /// Stop recording
    pub async fn stop(&mut self) -> Result<RecorderStats> {
        if !self.active.load(Ordering::SeqCst) {
            return Err(StreamlineError::storage_msg(
                "No active recording".to_string(),
            ));
        }

        info!("Stopping traffic recording");

        // Signal shutdown
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }

        self.active.store(false, Ordering::SeqCst);

        // Close recording
        {
            let mut rec = self.recording.write().await;
            if let Some(ref mut recording) = *rec {
                recording.close()?;
            }
        }

        // Update stats
        let stats = {
            let mut stats = self.stats.write().await;
            stats.active = false;
            stats.duration_ms = chrono::Utc::now().timestamp_millis() - stats.start_time;
            stats.clone()
        };

        info!(
            records = stats.records_captured,
            bytes = stats.bytes_captured,
            duration_ms = stats.duration_ms,
            "Recording stopped"
        );

        Ok(stats)
    }

    /// Capture a record (called by the server)
    pub async fn capture(&self, record: &Record, topic: &str, partition: i32) -> Result<bool> {
        if !self.active.load(Ordering::SeqCst) {
            return Ok(false);
        }

        // Check topic filter
        if !self.topic_filter.is_empty() && !self.matches_topic(topic) {
            return Ok(false);
        }

        // Apply sampling
        if self.config.sample_rate < 1.0 {
            let should_sample = rand::random::<f64>() < self.config.sample_rate;
            if !should_sample {
                let mut stats = self.stats.write().await;
                stats.records_skipped += 1;
                return Ok(false);
            }
        }

        // Check limits
        {
            let stats = self.stats.read().await;
            if self.config.max_records > 0 && stats.records_captured >= self.config.max_records {
                self.active.store(false, Ordering::SeqCst);
                return Ok(false);
            }
            if self.config.max_bytes > 0 && stats.bytes_captured >= self.config.max_bytes {
                self.active.store(false, Ordering::SeqCst);
                return Ok(false);
            }
        }

        // Create recorded message
        let mut recorded = RecordedMessage::from_record(record, topic, partition);

        // Strip headers if not configured to include them
        if !self.config.include_headers {
            recorded.headers.clear();
        }

        // Write to recording
        {
            let mut rec = self.recording.write().await;
            if let Some(ref mut recording) = *rec {
                if let Err(e) = recording.write(&recorded) {
                    error!(error = %e, "Failed to write recorded message");
                    let mut stats = self.stats.write().await;
                    stats.errors += 1;
                    return Err(e);
                }
            }
        }

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.records_captured += 1;
            stats.bytes_captured += record.value.len() as u64;
        }

        debug!(topic = %topic, partition = partition, offset = record.offset, "Captured record");
        Ok(true)
    }

    /// Check if a topic matches the filter
    fn matches_topic(&self, topic: &str) -> bool {
        if self.topic_filter.is_empty() {
            return true;
        }

        if self.topic_filter.contains(topic) {
            return true;
        }

        // Check wildcard patterns
        for pattern in &self.topic_filter {
            if pattern.contains('*') {
                let regex_pattern = pattern.replace('*', ".*");
                if regex::Regex::new(&format!("^{}$", regex_pattern))
                    .map(|r| r.is_match(topic))
                    .unwrap_or(false)
                {
                    return true;
                }
            }
        }

        false
    }

    /// Check if recording is active
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Get current stats
    pub async fn stats(&self) -> RecorderStats {
        let mut stats = self.stats.read().await.clone();
        if stats.active {
            stats.duration_ms = chrono::Utc::now().timestamp_millis() - stats.start_time;
        }
        stats
    }

    /// Get the recording path
    pub async fn recording_path(&self) -> Option<PathBuf> {
        let rec = self.recording.read().await;
        rec.as_ref().map(|r| r.path().to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tempfile::TempDir;

    #[test]
    fn test_record_config_default() {
        let config = RecordConfig::default();
        assert!(config.topics.is_empty());
        assert_eq!(config.sample_rate, 1.0);
        assert!(config.include_headers);
    }

    #[test]
    fn test_record_config_builder() {
        let config = RecordConfig::for_topics(vec!["events".to_string()])
            .with_output_dir("./test-recordings")
            .with_name("Test Recording")
            .with_sample_rate(0.5)
            .with_max_duration(3600);

        assert_eq!(config.topics, vec!["events"]);
        assert_eq!(config.output_dir, "./test-recordings");
        assert_eq!(config.name, Some("Test Recording".to_string()));
        assert_eq!(config.sample_rate, 0.5);
        assert_eq!(config.max_duration_secs, 3600);
    }

    #[tokio::test]
    async fn test_recorder_start_stop() {
        let temp_dir = TempDir::new().unwrap();
        let config = RecordConfig::default().with_output_dir(temp_dir.path().to_str().unwrap());

        let mut recorder = TrafficRecorder::new(config);

        assert!(!recorder.is_active());

        let recording_id = recorder.start().await.unwrap();
        assert!(recorder.is_active());
        assert!(!recording_id.is_empty());

        let stats = recorder.stop().await.unwrap();
        assert!(!recorder.is_active());
        assert!(!stats.active);
    }

    #[tokio::test]
    async fn test_recorder_capture() {
        let temp_dir = TempDir::new().unwrap();
        let config = RecordConfig::for_topics(vec!["events".to_string()])
            .with_output_dir(temp_dir.path().to_str().unwrap());

        let mut recorder = TrafficRecorder::new(config);
        recorder.start().await.unwrap();

        // Capture some records
        for i in 0..5 {
            let record = Record::new(
                i,
                chrono::Utc::now().timestamp_millis(),
                None,
                Bytes::from(format!("value-{}", i)),
            );
            let captured = recorder.capture(&record, "events", 0).await.unwrap();
            assert!(captured);
        }

        let stats = recorder.stop().await.unwrap();
        assert_eq!(stats.records_captured, 5);
    }

    #[tokio::test]
    async fn test_recorder_topic_filter() {
        let temp_dir = TempDir::new().unwrap();
        let config = RecordConfig::for_topics(vec!["events".to_string()])
            .with_output_dir(temp_dir.path().to_str().unwrap());

        let mut recorder = TrafficRecorder::new(config);
        recorder.start().await.unwrap();

        // Should capture
        let record = Record::new(0, 0, None, Bytes::from("test"));
        assert!(recorder.capture(&record, "events", 0).await.unwrap());

        // Should not capture (wrong topic)
        assert!(!recorder.capture(&record, "logs", 0).await.unwrap());

        let stats = recorder.stop().await.unwrap();
        assert_eq!(stats.records_captured, 1);
    }

    #[test]
    fn test_matches_topic_wildcard() {
        let config = RecordConfig::for_topics(vec!["events.*".to_string()]);
        let recorder = TrafficRecorder::new(config);

        assert!(recorder.matches_topic("events.clicks"));
        assert!(recorder.matches_topic("events.views"));
        assert!(!recorder.matches_topic("logs.errors"));
    }
}
