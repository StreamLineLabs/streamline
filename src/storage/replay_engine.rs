//! Time-Travel Replay Engine
//!
//! Provides replay capabilities for streaming data, allowing users to
//! re-process historical messages with speed control, filtering, and
//! divergence detection.
//!
//! Features:
//! - Create replay sessions for topics with time range selection
//! - Adjustable playback speed (pause, resume, fast-forward)
//! - Message filtering by key pattern, value content, headers, and sampling
//! - Output to another topic, in-memory collection, or discard
//! - Divergence diff computation between original and replayed streams

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Time-travel replay engine managing concurrent replay sessions.
pub struct ReplayEngine {
    sessions: Arc<RwLock<HashMap<String, ReplaySession>>>,
    config: ReplayConfig,
    stats: Arc<ReplayStats>,
}

/// Configuration for the replay engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayConfig {
    /// Maximum number of concurrent replay sessions.
    pub max_concurrent_sessions: usize,
    /// Maximum allowed replay duration in seconds.
    pub max_replay_duration_secs: u64,
    /// Default playback speed multiplier.
    pub default_speed: f64,
    /// Maximum playback speed multiplier.
    pub max_speed: f64,
    /// Internal buffer size for replay messages.
    pub buffer_size: usize,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            max_concurrent_sessions: 10,
            max_replay_duration_secs: 3600,
            default_speed: 1.0,
            max_speed: 100.0,
            buffer_size: 10000,
        }
    }
}

/// A single replay session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaySession {
    /// Unique session identifier.
    pub id: String,
    /// Source topic to replay from.
    pub topic: String,
    /// Optional partition filter.
    pub partition: Option<i32>,
    /// Replay window start (ms since epoch).
    pub start_time: u64,
    /// Replay window end (ms since epoch).
    pub end_time: u64,
    /// Current playback speed multiplier.
    pub speed: f64,
    /// Current session status.
    pub status: ReplayStatus,
    /// Replay progress tracking.
    pub progress: ReplayProgress,
    /// Optional message filter.
    pub filter: Option<ReplayFilter>,
    /// Output destination for replayed messages.
    pub output: ReplayOutput,
    /// ISO-8601 creation timestamp.
    pub created_at: String,
}

/// Status of a replay session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ReplayStatus {
    Pending,
    Running,
    Paused,
    Completed,
    Failed(String),
    Cancelled,
}

/// Progress information for a replay session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayProgress {
    pub messages_replayed: u64,
    pub messages_total_estimate: u64,
    pub current_offset: i64,
    pub current_timestamp: u64,
    pub percent_complete: f64,
    pub elapsed_secs: f64,
}

impl Default for ReplayProgress {
    fn default() -> Self {
        Self {
            messages_replayed: 0,
            messages_total_estimate: 0,
            current_offset: -1,
            current_timestamp: 0,
            percent_complete: 0.0,
            elapsed_secs: 0.0,
        }
    }
}

/// Filter criteria for replayed messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayFilter {
    /// Regex pattern applied to message keys.
    pub key_pattern: Option<String>,
    /// Substring match on message values.
    pub value_contains: Option<String>,
    /// Required header key-value pairs.
    pub headers: Option<HashMap<String, String>>,
    /// Random sampling rate in `[0.0, 1.0]`.
    pub sample_rate: Option<f64>,
}

/// Destination for replayed messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplayOutput {
    /// Write replayed messages to a target topic.
    Topic { target_topic: String },
    /// Collect messages in memory up to a limit.
    Collect { max_messages: usize },
    /// Discard replayed messages (useful for validation).
    Discard,
}

/// Result of comparing original and replayed message streams.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayDiff {
    pub session_id: String,
    pub original_count: u64,
    pub replayed_count: u64,
    pub matched: u64,
    pub diverged: u64,
    pub missing: u64,
    pub extra: u64,
    pub divergence_samples: Vec<DivergenceSample>,
}

/// A single divergence between original and replayed data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DivergenceSample {
    pub offset: i64,
    pub original_value: Option<String>,
    pub replayed_value: Option<String>,
    pub diff_type: String,
}

/// Atomic statistics for the replay engine.
pub struct ReplayStats {
    pub sessions_created: AtomicU64,
    pub messages_replayed: AtomicU64,
    pub bytes_replayed: AtomicU64,
    pub diffs_computed: AtomicU64,
}

impl Default for ReplayStats {
    fn default() -> Self {
        Self {
            sessions_created: AtomicU64::new(0),
            messages_replayed: AtomicU64::new(0),
            bytes_replayed: AtomicU64::new(0),
            diffs_computed: AtomicU64::new(0),
        }
    }
}

/// Serialisable snapshot of [`ReplayStats`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayStatsSnapshot {
    pub sessions_created: u64,
    pub messages_replayed: u64,
    pub bytes_replayed: u64,
    pub diffs_computed: u64,
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl ReplayEngine {
    /// Create a new replay engine with the given configuration.
    pub fn new(config: ReplayConfig) -> Self {
        info!(
            max_concurrent = config.max_concurrent_sessions,
            max_speed = config.max_speed,
            "Initialising replay engine"
        );
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(ReplayStats::default()),
        }
    }

    /// Create a new replay session. Returns the session ID.
    pub async fn create_session(
        &self,
        topic: String,
        partition: Option<i32>,
        start_time: u64,
        end_time: u64,
        filter: Option<ReplayFilter>,
        output: ReplayOutput,
    ) -> Result<String> {
        if start_time >= end_time {
            return Err(StreamlineError::Storage(
                "start_time must be before end_time".into(),
            ));
        }

        let sessions = self.sessions.read().await;
        let active = sessions
            .values()
            .filter(|s| matches!(s.status, ReplayStatus::Running | ReplayStatus::Paused))
            .count();
        if active >= self.config.max_concurrent_sessions {
            return Err(StreamlineError::Storage(format!(
                "Maximum concurrent sessions ({}) reached",
                self.config.max_concurrent_sessions
            )));
        }
        drop(sessions);

        if let Some(ref f) = filter {
            if let Some(rate) = f.sample_rate {
                if !(0.0..=1.0).contains(&rate) {
                    return Err(StreamlineError::Storage(
                        "sample_rate must be between 0.0 and 1.0".into(),
                    ));
                }
            }
        }

        let id = Uuid::new_v4().to_string();
        let session = ReplaySession {
            id: id.clone(),
            topic,
            partition,
            start_time,
            end_time,
            speed: self.config.default_speed,
            status: ReplayStatus::Pending,
            progress: ReplayProgress::default(),
            filter,
            output,
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        self.sessions.write().await.insert(id.clone(), session);
        self.stats.sessions_created.fetch_add(1, Ordering::Relaxed);
        info!(session_id = %id, "Replay session created");
        Ok(id)
    }

    /// Transition a pending session to running.
    pub async fn start_session(&self, session_id: &str) -> Result<()> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            StreamlineError::Storage(format!("Replay session '{}' not found", session_id))
        })?;

        match &session.status {
            ReplayStatus::Pending => {
                session.status = ReplayStatus::Running;
                debug!(session_id, "Replay session started");
                Ok(())
            }
            other => Err(StreamlineError::Storage(format!(
                "Cannot start session in {:?} state",
                other
            ))),
        }
    }

    /// Pause a running session.
    pub async fn pause_session(&self, session_id: &str) -> Result<()> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            StreamlineError::Storage(format!("Replay session '{}' not found", session_id))
        })?;

        match &session.status {
            ReplayStatus::Running => {
                session.status = ReplayStatus::Paused;
                debug!(session_id, "Replay session paused");
                Ok(())
            }
            other => Err(StreamlineError::Storage(format!(
                "Cannot pause session in {:?} state",
                other
            ))),
        }
    }

    /// Resume a paused session.
    pub async fn resume_session(&self, session_id: &str) -> Result<()> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            StreamlineError::Storage(format!("Replay session '{}' not found", session_id))
        })?;

        match &session.status {
            ReplayStatus::Paused => {
                session.status = ReplayStatus::Running;
                debug!(session_id, "Replay session resumed");
                Ok(())
            }
            other => Err(StreamlineError::Storage(format!(
                "Cannot resume session in {:?} state",
                other
            ))),
        }
    }

    /// Cancel a running or paused session.
    pub async fn cancel_session(&self, session_id: &str) -> Result<()> {
        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            StreamlineError::Storage(format!("Replay session '{}' not found", session_id))
        })?;

        match &session.status {
            ReplayStatus::Running | ReplayStatus::Paused | ReplayStatus::Pending => {
                session.status = ReplayStatus::Cancelled;
                warn!(session_id, "Replay session cancelled");
                Ok(())
            }
            other => Err(StreamlineError::Storage(format!(
                "Cannot cancel session in {:?} state",
                other
            ))),
        }
    }

    /// Retrieve a clone of a session by ID.
    pub async fn get_session(&self, session_id: &str) -> Result<ReplaySession> {
        self.sessions
            .read()
            .await
            .get(session_id)
            .cloned()
            .ok_or_else(|| {
                StreamlineError::Storage(format!("Replay session '{}' not found", session_id))
            })
    }

    /// List all sessions, optionally filtered by status.
    pub async fn list_sessions(
        &self,
        status_filter: Option<&ReplayStatus>,
    ) -> Vec<ReplaySession> {
        let sessions = self.sessions.read().await;
        sessions
            .values()
            .filter(|s| status_filter.map_or(true, |f| s.status == *f))
            .cloned()
            .collect()
    }

    /// Adjust playback speed for a running or paused session.
    pub async fn set_speed(&self, session_id: &str, speed: f64) -> Result<()> {
        if speed <= 0.0 || speed > self.config.max_speed {
            return Err(StreamlineError::Storage(format!(
                "Speed must be in (0.0, {}]",
                self.config.max_speed
            )));
        }

        let mut sessions = self.sessions.write().await;
        let session = sessions.get_mut(session_id).ok_or_else(|| {
            StreamlineError::Storage(format!("Replay session '{}' not found", session_id))
        })?;

        match &session.status {
            ReplayStatus::Running | ReplayStatus::Paused => {
                session.speed = speed;
                debug!(session_id, speed, "Replay speed updated");
                Ok(())
            }
            other => Err(StreamlineError::Storage(format!(
                "Cannot set speed on session in {:?} state",
                other
            ))),
        }
    }

    /// Compute a divergence diff for a completed session.
    pub async fn compute_diff(
        &self,
        session_id: &str,
        original_messages: &[(i64, Option<String>)],
        replayed_messages: &[(i64, Option<String>)],
    ) -> Result<ReplayDiff> {
        // Verify the session exists
        let session = self.get_session(session_id).await?;
        if session.status != ReplayStatus::Completed {
            return Err(StreamlineError::Storage(
                "Diff can only be computed for completed sessions".into(),
            ));
        }

        let orig_map: HashMap<i64, Option<String>> =
            original_messages.iter().cloned().collect();
        let replay_map: HashMap<i64, Option<String>> =
            replayed_messages.iter().cloned().collect();

        let mut matched: u64 = 0;
        let mut diverged: u64 = 0;
        let mut missing: u64 = 0;
        let mut extra: u64 = 0;
        let mut samples = Vec::new();

        for (offset, orig_val) in &orig_map {
            match replay_map.get(offset) {
                Some(replay_val) if replay_val == orig_val => matched += 1,
                Some(replay_val) => {
                    diverged += 1;
                    if samples.len() < 10 {
                        samples.push(DivergenceSample {
                            offset: *offset,
                            original_value: orig_val.clone(),
                            replayed_value: replay_val.clone(),
                            diff_type: "value_mismatch".into(),
                        });
                    }
                }
                None => {
                    missing += 1;
                    if samples.len() < 10 {
                        samples.push(DivergenceSample {
                            offset: *offset,
                            original_value: orig_val.clone(),
                            replayed_value: None,
                            diff_type: "missing_in_replay".into(),
                        });
                    }
                }
            }
        }

        for offset in replay_map.keys() {
            if !orig_map.contains_key(offset) {
                extra += 1;
                if samples.len() < 10 {
                    samples.push(DivergenceSample {
                        offset: *offset,
                        original_value: None,
                        replayed_value: replay_map[offset].clone(),
                        diff_type: "extra_in_replay".into(),
                    });
                }
            }
        }

        self.stats.diffs_computed.fetch_add(1, Ordering::Relaxed);

        Ok(ReplayDiff {
            session_id: session_id.to_string(),
            original_count: original_messages.len() as u64,
            replayed_count: replayed_messages.len() as u64,
            matched,
            diverged,
            missing,
            extra,
            divergence_samples: samples,
        })
    }

    /// Return an atomic stats snapshot.
    pub fn stats(&self) -> ReplayStatsSnapshot {
        ReplayStatsSnapshot {
            sessions_created: self.stats.sessions_created.load(Ordering::Relaxed),
            messages_replayed: self.stats.messages_replayed.load(Ordering::Relaxed),
            bytes_replayed: self.stats.bytes_replayed.load(Ordering::Relaxed),
            diffs_computed: self.stats.diffs_computed.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_engine() -> ReplayEngine {
        ReplayEngine::new(ReplayConfig::default())
    }

    #[test]
    fn test_default_config() {
        let cfg = ReplayConfig::default();
        assert_eq!(cfg.max_concurrent_sessions, 10);
        assert_eq!(cfg.max_replay_duration_secs, 3600);
        assert!((cfg.default_speed - 1.0).abs() < f64::EPSILON);
        assert!((cfg.max_speed - 100.0).abs() < f64::EPSILON);
        assert_eq!(cfg.buffer_size, 10000);
    }

    #[tokio::test]
    async fn test_create_session() {
        let engine = default_engine();
        let id = engine
            .create_session(
                "events".into(),
                None,
                1000,
                2000,
                None,
                ReplayOutput::Discard,
            )
            .await
            .unwrap();
        assert!(!id.is_empty());

        let session = engine.get_session(&id).await.unwrap();
        assert_eq!(session.topic, "events");
        assert_eq!(session.status, ReplayStatus::Pending);
    }

    #[tokio::test]
    async fn test_create_session_invalid_time_range() {
        let engine = default_engine();
        let err = engine
            .create_session("t".into(), None, 2000, 1000, None, ReplayOutput::Discard)
            .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_create_session_equal_times() {
        let engine = default_engine();
        let err = engine
            .create_session("t".into(), None, 1000, 1000, None, ReplayOutput::Discard)
            .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_session_lifecycle_start_pause_resume_cancel() {
        let engine = default_engine();
        let id = engine
            .create_session("t".into(), Some(0), 0, 100, None, ReplayOutput::Discard)
            .await
            .unwrap();

        engine.start_session(&id).await.unwrap();
        assert_eq!(engine.get_session(&id).await.unwrap().status, ReplayStatus::Running);

        engine.pause_session(&id).await.unwrap();
        assert_eq!(engine.get_session(&id).await.unwrap().status, ReplayStatus::Paused);

        engine.resume_session(&id).await.unwrap();
        assert_eq!(engine.get_session(&id).await.unwrap().status, ReplayStatus::Running);

        engine.cancel_session(&id).await.unwrap();
        assert_eq!(engine.get_session(&id).await.unwrap().status, ReplayStatus::Cancelled);
    }

    #[tokio::test]
    async fn test_cannot_start_running_session() {
        let engine = default_engine();
        let id = engine
            .create_session("t".into(), None, 0, 100, None, ReplayOutput::Discard)
            .await
            .unwrap();
        engine.start_session(&id).await.unwrap();
        assert!(engine.start_session(&id).await.is_err());
    }

    #[tokio::test]
    async fn test_cannot_pause_pending_session() {
        let engine = default_engine();
        let id = engine
            .create_session("t".into(), None, 0, 100, None, ReplayOutput::Discard)
            .await
            .unwrap();
        assert!(engine.pause_session(&id).await.is_err());
    }

    #[tokio::test]
    async fn test_cannot_resume_pending_session() {
        let engine = default_engine();
        let id = engine
            .create_session("t".into(), None, 0, 100, None, ReplayOutput::Discard)
            .await
            .unwrap();
        assert!(engine.resume_session(&id).await.is_err());
    }

    #[tokio::test]
    async fn test_get_nonexistent_session() {
        let engine = default_engine();
        assert!(engine.get_session("no-such-id").await.is_err());
    }

    #[tokio::test]
    async fn test_list_sessions_empty() {
        let engine = default_engine();
        assert!(engine.list_sessions(None).await.is_empty());
    }

    #[tokio::test]
    async fn test_list_sessions_with_filter() {
        let engine = default_engine();
        let id1 = engine
            .create_session("a".into(), None, 0, 10, None, ReplayOutput::Discard)
            .await
            .unwrap();
        let _id2 = engine
            .create_session("b".into(), None, 0, 10, None, ReplayOutput::Discard)
            .await
            .unwrap();
        engine.start_session(&id1).await.unwrap();

        let running = engine.list_sessions(Some(&ReplayStatus::Running)).await;
        assert_eq!(running.len(), 1);
        assert_eq!(running[0].id, id1);

        let pending = engine.list_sessions(Some(&ReplayStatus::Pending)).await;
        assert_eq!(pending.len(), 1);
    }

    #[tokio::test]
    async fn test_set_speed() {
        let engine = default_engine();
        let id = engine
            .create_session("t".into(), None, 0, 100, None, ReplayOutput::Discard)
            .await
            .unwrap();
        engine.start_session(&id).await.unwrap();

        engine.set_speed(&id, 5.0).await.unwrap();
        let s = engine.get_session(&id).await.unwrap();
        assert!((s.speed - 5.0).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_set_speed_invalid_zero() {
        let engine = default_engine();
        let id = engine
            .create_session("t".into(), None, 0, 100, None, ReplayOutput::Discard)
            .await
            .unwrap();
        engine.start_session(&id).await.unwrap();
        assert!(engine.set_speed(&id, 0.0).await.is_err());
    }

    #[tokio::test]
    async fn test_set_speed_exceeds_max() {
        let engine = default_engine();
        let id = engine
            .create_session("t".into(), None, 0, 100, None, ReplayOutput::Discard)
            .await
            .unwrap();
        engine.start_session(&id).await.unwrap();
        assert!(engine.set_speed(&id, 200.0).await.is_err());
    }

    #[tokio::test]
    async fn test_set_speed_on_pending_session() {
        let engine = default_engine();
        let id = engine
            .create_session("t".into(), None, 0, 100, None, ReplayOutput::Discard)
            .await
            .unwrap();
        assert!(engine.set_speed(&id, 2.0).await.is_err());
    }

    #[tokio::test]
    async fn test_max_concurrent_sessions() {
        let config = ReplayConfig {
            max_concurrent_sessions: 2,
            ..Default::default()
        };
        let engine = ReplayEngine::new(config);

        let id1 = engine
            .create_session("a".into(), None, 0, 10, None, ReplayOutput::Discard)
            .await
            .unwrap();
        let id2 = engine
            .create_session("b".into(), None, 0, 10, None, ReplayOutput::Discard)
            .await
            .unwrap();
        engine.start_session(&id1).await.unwrap();
        engine.start_session(&id2).await.unwrap();

        let err = engine
            .create_session("c".into(), None, 0, 10, None, ReplayOutput::Discard)
            .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_compute_diff_all_match() {
        let engine = default_engine();
        let id = engine
            .create_session("t".into(), None, 0, 100, None, ReplayOutput::Discard)
            .await
            .unwrap();
        // Manually complete the session for diff testing
        {
            let mut sessions = engine.sessions.write().await;
            sessions.get_mut(&id).unwrap().status = ReplayStatus::Completed;
        }

        let original = vec![(0, Some("a".into())), (1, Some("b".into()))];
        let replayed = vec![(0, Some("a".into())), (1, Some("b".into()))];
        let diff = engine.compute_diff(&id, &original, &replayed).await.unwrap();

        assert_eq!(diff.matched, 2);
        assert_eq!(diff.diverged, 0);
        assert_eq!(diff.missing, 0);
        assert_eq!(diff.extra, 0);
    }

    #[tokio::test]
    async fn test_compute_diff_with_divergence() {
        let engine = default_engine();
        let id = engine
            .create_session("t".into(), None, 0, 100, None, ReplayOutput::Discard)
            .await
            .unwrap();
        {
            let mut sessions = engine.sessions.write().await;
            sessions.get_mut(&id).unwrap().status = ReplayStatus::Completed;
        }

        let original = vec![
            (0, Some("a".into())),
            (1, Some("b".into())),
            (2, Some("c".into())),
        ];
        let replayed = vec![
            (0, Some("a".into())),
            (1, Some("CHANGED".into())),
            (3, Some("extra".into())),
        ];
        let diff = engine.compute_diff(&id, &original, &replayed).await.unwrap();

        assert_eq!(diff.matched, 1);
        assert_eq!(diff.diverged, 1);
        assert_eq!(diff.missing, 1);
        assert_eq!(diff.extra, 1);
        assert!(!diff.divergence_samples.is_empty());
    }

    #[tokio::test]
    async fn test_compute_diff_requires_completed() {
        let engine = default_engine();
        let id = engine
            .create_session("t".into(), None, 0, 100, None, ReplayOutput::Discard)
            .await
            .unwrap();
        let err = engine.compute_diff(&id, &[], &[]).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_stats_initial() {
        let engine = default_engine();
        let s = engine.stats();
        assert_eq!(s.sessions_created, 0);
        assert_eq!(s.messages_replayed, 0);
        assert_eq!(s.bytes_replayed, 0);
        assert_eq!(s.diffs_computed, 0);
    }

    #[tokio::test]
    async fn test_stats_after_operations() {
        let engine = default_engine();
        engine
            .create_session("t".into(), None, 0, 100, None, ReplayOutput::Discard)
            .await
            .unwrap();
        let s = engine.stats();
        assert_eq!(s.sessions_created, 1);
    }

    #[tokio::test]
    async fn test_filter_with_invalid_sample_rate() {
        let engine = default_engine();
        let filter = ReplayFilter {
            key_pattern: None,
            value_contains: None,
            headers: None,
            sample_rate: Some(1.5),
        };
        let err = engine
            .create_session("t".into(), None, 0, 100, Some(filter), ReplayOutput::Discard)
            .await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn test_session_with_topic_output() {
        let engine = default_engine();
        let id = engine
            .create_session(
                "source".into(),
                None,
                0,
                100,
                None,
                ReplayOutput::Topic {
                    target_topic: "sink".into(),
                },
            )
            .await
            .unwrap();
        let session = engine.get_session(&id).await.unwrap();
        match &session.output {
            ReplayOutput::Topic { target_topic } => assert_eq!(target_topic, "sink"),
            _ => panic!("Expected Topic output"),
        }
    }

    #[tokio::test]
    async fn test_session_with_collect_output() {
        let engine = default_engine();
        let id = engine
            .create_session(
                "source".into(),
                None,
                0,
                100,
                None,
                ReplayOutput::Collect { max_messages: 500 },
            )
            .await
            .unwrap();
        let session = engine.get_session(&id).await.unwrap();
        match &session.output {
            ReplayOutput::Collect { max_messages } => assert_eq!(*max_messages, 500),
            _ => panic!("Expected Collect output"),
        }
    }

    #[tokio::test]
    async fn test_cancel_completed_session_fails() {
        let engine = default_engine();
        let id = engine
            .create_session("t".into(), None, 0, 100, None, ReplayOutput::Discard)
            .await
            .unwrap();
        {
            let mut sessions = engine.sessions.write().await;
            sessions.get_mut(&id).unwrap().status = ReplayStatus::Completed;
        }
        assert!(engine.cancel_session(&id).await.is_err());
    }

    #[test]
    fn test_replay_status_serde_roundtrip() {
        let statuses = vec![
            ReplayStatus::Pending,
            ReplayStatus::Running,
            ReplayStatus::Paused,
            ReplayStatus::Completed,
            ReplayStatus::Failed("oops".into()),
            ReplayStatus::Cancelled,
        ];
        for s in statuses {
            let json = serde_json::to_string(&s).unwrap();
            let back: ReplayStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(s, back);
        }
    }

    #[test]
    fn test_replay_config_serde_roundtrip() {
        let cfg = ReplayConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let back: ReplayConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.max_concurrent_sessions, cfg.max_concurrent_sessions);
    }
}
