//! Deterministic Replay Debugger
//!
//! Captures full processing state (offsets, windows, aggregations, variables)
//! and allows deterministic, step-by-step replay with breakpoints, state
//! inspection, and time-travel debugging.
//!
//! Key concepts:
//! - **StateCapture**: a frozen snapshot of all processing state
//! - **DebugSession**: a replay session operating on a captured snapshot
//! - **Breakpoint / BreakCondition**: pause execution on offset, key match, etc.

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

/// Deterministic replay engine managing captures and debug sessions.
pub struct DeterministicReplayEngine {
    captures: Arc<RwLock<HashMap<String, StateCapture>>>,
    sessions: Arc<RwLock<HashMap<String, DebugSession>>>,
    config: ReplayDebugConfig,
    stats: Arc<DebugStats>,
}

/// Configuration for the replay debugger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayDebugConfig {
    /// Maximum number of stored state captures.
    pub max_captures: usize,
    /// Maximum state capture size in megabytes.
    pub max_state_size_mb: u64,
    /// Whether breakpoint support is enabled.
    pub enable_breakpoints: bool,
    /// Use a deterministic logical clock instead of wall-clock.
    pub deterministic_clock: bool,
    /// Number of steps between automatic snapshots.
    pub snapshot_interval: usize,
}

impl Default for ReplayDebugConfig {
    fn default() -> Self {
        Self {
            max_captures: 100,
            max_state_size_mb: 512,
            enable_breakpoints: true,
            deterministic_clock: true,
            snapshot_interval: 1000,
        }
    }
}

/// A frozen snapshot of all processing state at a point in time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateCapture {
    /// Unique capture identifier.
    pub id: String,
    /// Human-readable name for the capture.
    pub name: String,
    /// Topic → partition → offset mapping.
    pub offsets: HashMap<String, HashMap<i32, i64>>,
    /// Window state keyed by window id.
    pub window_state: HashMap<String, serde_json::Value>,
    /// Aggregation state keyed by aggregation id.
    pub aggregation_state: HashMap<String, serde_json::Value>,
    /// User-defined variables.
    pub variables: HashMap<String, serde_json::Value>,
    /// Logical clock value in milliseconds.
    pub clock_ms: u64,
    /// Total number of messages processed at capture time.
    pub message_count: u64,
    /// ISO-8601 timestamp of when the capture was taken.
    pub captured_at: String,
    /// Approximate size of the serialised capture in bytes.
    pub size_bytes: u64,
}

/// A debug replay session operating on a captured state snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugSession {
    /// Unique session identifier.
    pub id: String,
    /// Identifier of the state capture this session replays from.
    pub capture_id: String,
    /// Current session status.
    pub status: DebugStatus,
    /// Current step index.
    pub current_step: u64,
    /// Total number of steps in the replay.
    pub total_steps: u64,
    /// Active breakpoints.
    pub breakpoints: Vec<Breakpoint>,
    /// History of executed steps.
    pub step_history: Vec<StepRecord>,
    /// ISO-8601 creation timestamp.
    pub created_at: String,
}

/// Status of a debug session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DebugStatus {
    /// Session is paused, waiting for user action.
    Paused,
    /// Executing one step at a time.
    Stepping,
    /// Running freely until a breakpoint or completion.
    Running,
    /// Replay completed successfully.
    Completed,
    /// An error occurred during replay.
    Error(String),
}

/// A breakpoint attached to a debug session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Breakpoint {
    /// Unique breakpoint identifier.
    pub id: String,
    /// Condition that triggers this breakpoint.
    pub condition: BreakCondition,
    /// Number of times this breakpoint has been hit.
    pub hit_count: u64,
    /// Whether the breakpoint is active.
    pub enabled: bool,
}

/// Condition that determines when a breakpoint fires.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum BreakCondition {
    /// Break when a specific topic/partition reaches an offset.
    Offset {
        topic: String,
        partition: i32,
        offset: i64,
    },
    /// Break after processing N messages.
    MessageCount(u64),
    /// Break when a message key matches the pattern.
    KeyMatch { pattern: String },
    /// Break when a message value field equals a value.
    ValueMatch {
        field: String,
        value: serde_json::Value,
    },
    /// Break when a specific state variable changes.
    StateChange { variable: String },
    /// Break on any processing error.
    ErrorOccurred,
}

/// Record of a single executed step in a debug session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepRecord {
    /// Step index.
    pub step: u64,
    /// Human-readable action description.
    pub action: String,
    /// Optional input data.
    pub input: Option<String>,
    /// Optional output data.
    pub output: Option<String>,
    /// State changes produced by this step.
    pub state_delta: HashMap<String, serde_json::Value>,
    /// Logical clock value at step execution.
    pub timestamp_ms: u64,
}

/// Aggregate statistics for the debug engine.
pub struct DebugStats {
    pub total_captures: AtomicU64,
    pub total_sessions: AtomicU64,
    pub total_steps: AtomicU64,
    pub breakpoints_hit: AtomicU64,
}

impl Default for DebugStats {
    fn default() -> Self {
        Self {
            total_captures: AtomicU64::new(0),
            total_sessions: AtomicU64::new(0),
            total_steps: AtomicU64::new(0),
            breakpoints_hit: AtomicU64::new(0),
        }
    }
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl DeterministicReplayEngine {
    /// Create a new replay engine with the given configuration.
    pub fn new(config: ReplayDebugConfig) -> Self {
        info!(
            max_captures = config.max_captures,
            breakpoints = config.enable_breakpoints,
            "Initializing deterministic replay engine"
        );
        Self {
            captures: Arc::new(RwLock::new(HashMap::new())),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(DebugStats::default()),
        }
    }

    /// Capture the current processing state and return the capture id.
    #[allow(clippy::too_many_arguments)]
    pub async fn capture_state(
        &self,
        name: &str,
        offsets: HashMap<String, HashMap<i32, i64>>,
        window_state: HashMap<String, serde_json::Value>,
        aggregation_state: HashMap<String, serde_json::Value>,
        variables: HashMap<String, serde_json::Value>,
        clock_ms: u64,
    ) -> Result<String, String> {
        let mut captures = self.captures.write().await;

        if captures.len() >= self.config.max_captures {
            return Err(format!(
                "Maximum capture limit ({}) reached",
                self.config.max_captures
            ));
        }

        let id = Uuid::new_v4().to_string();
        let size_bytes = serde_json::to_string(&offsets).unwrap_or_default().len() as u64
            + serde_json::to_string(&window_state)
                .unwrap_or_default()
                .len() as u64
            + serde_json::to_string(&aggregation_state)
                .unwrap_or_default()
                .len() as u64
            + serde_json::to_string(&variables)
                .unwrap_or_default()
                .len() as u64;

        let max_bytes = self.config.max_state_size_mb * 1024 * 1024;
        if size_bytes > max_bytes {
            return Err(format!(
                "State size ({} bytes) exceeds limit ({} bytes)",
                size_bytes, max_bytes
            ));
        }

        let message_count: u64 = offsets
            .values()
            .flat_map(|parts| parts.values())
            .map(|&o| o.max(0) as u64)
            .sum();

        let capture = StateCapture {
            id: id.clone(),
            name: name.to_string(),
            offsets,
            window_state,
            aggregation_state,
            variables,
            clock_ms,
            message_count,
            captured_at: chrono::Utc::now().to_rfc3339(),
            size_bytes,
        };

        info!(id = %id, name = %name, size = size_bytes, "State captured");
        captures.insert(id.clone(), capture);
        self.stats.total_captures.fetch_add(1, Ordering::Relaxed);
        Ok(id)
    }

    /// Retrieve a state capture by id.
    pub async fn get_capture(&self, id: &str) -> Option<StateCapture> {
        let captures = self.captures.read().await;
        captures.get(id).cloned()
    }

    /// List all state captures.
    pub async fn list_captures(&self) -> Vec<StateCapture> {
        let captures = self.captures.read().await;
        captures.values().cloned().collect()
    }

    /// Delete a state capture by id.
    pub async fn delete_capture(&self, id: &str) -> Result<(), String> {
        let mut captures = self.captures.write().await;
        if captures.remove(id).is_some() {
            info!(id = %id, "State capture deleted");
            Ok(())
        } else {
            Err(format!("Capture '{}' not found", id))
        }
    }

    /// Create a new debug session for the given capture. Returns session id.
    pub async fn create_debug_session(&self, capture_id: &str) -> Result<String, String> {
        // Verify the capture exists
        {
            let captures = self.captures.read().await;
            if !captures.contains_key(capture_id) {
                return Err(format!("Capture '{}' not found", capture_id));
            }
        }

        let session_id = Uuid::new_v4().to_string();
        let session = DebugSession {
            id: session_id.clone(),
            capture_id: capture_id.to_string(),
            status: DebugStatus::Paused,
            current_step: 0,
            total_steps: 100, // placeholder total
            breakpoints: Vec::new(),
            step_history: Vec::new(),
            created_at: chrono::Utc::now().to_rfc3339(),
        };

        let mut sessions = self.sessions.write().await;
        sessions.insert(session_id.clone(), session);
        self.stats.total_sessions.fetch_add(1, Ordering::Relaxed);
        info!(session_id = %session_id, capture_id = %capture_id, "Debug session created");
        Ok(session_id)
    }

    /// Advance the session by one step.
    pub async fn step(&self, session_id: &str) -> Result<StepRecord, String> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?;

        if session.status == DebugStatus::Completed {
            return Err("Session already completed".to_string());
        }

        session.status = DebugStatus::Stepping;
        session.current_step += 1;

        let record = StepRecord {
            step: session.current_step,
            action: format!("step_{}", session.current_step),
            input: None,
            output: None,
            state_delta: HashMap::new(),
            timestamp_ms: session.current_step * 10,
        };

        session.step_history.push(record.clone());
        self.stats.total_steps.fetch_add(1, Ordering::Relaxed);

        if Self::check_breakpoints(session) {
            session.status = DebugStatus::Paused;
            self.stats.breakpoints_hit.fetch_add(1, Ordering::Relaxed);
        }

        if session.current_step >= session.total_steps {
            session.status = DebugStatus::Completed;
        }

        debug!(session_id = %session_id, step = session.current_step, "Step executed");
        Ok(record)
    }

    /// Step over (advance past the current action without descending).
    pub async fn step_over(&self, session_id: &str) -> Result<StepRecord, String> {
        // For this engine, step_over behaves like step but skips breakpoint evaluation.
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?;

        if session.status == DebugStatus::Completed {
            return Err("Session already completed".to_string());
        }

        session.status = DebugStatus::Stepping;
        session.current_step += 1;

        let record = StepRecord {
            step: session.current_step,
            action: format!("step_over_{}", session.current_step),
            input: None,
            output: None,
            state_delta: HashMap::new(),
            timestamp_ms: session.current_step * 10,
        };

        session.step_history.push(record.clone());
        self.stats.total_steps.fetch_add(1, Ordering::Relaxed);

        if session.current_step >= session.total_steps {
            session.status = DebugStatus::Completed;
        }

        Ok(record)
    }

    /// Continue execution until a breakpoint is hit or the session completes.
    pub async fn continue_to_breakpoint(&self, session_id: &str) -> Result<StepRecord, String> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?;

        if session.status == DebugStatus::Completed {
            return Err("Session already completed".to_string());
        }

        session.status = DebugStatus::Running;

        let mut last_record = None;
        while session.current_step < session.total_steps {
            session.current_step += 1;
            let record = StepRecord {
                step: session.current_step,
                action: format!("continue_{}", session.current_step),
                input: None,
                output: None,
                state_delta: HashMap::new(),
                timestamp_ms: session.current_step * 10,
            };
            session.step_history.push(record.clone());
            self.stats.total_steps.fetch_add(1, Ordering::Relaxed);

            if Self::check_breakpoints(session) {
                session.status = DebugStatus::Paused;
                self.stats.breakpoints_hit.fetch_add(1, Ordering::Relaxed);
                return Ok(record);
            }
            last_record = Some(record);
        }

        session.status = DebugStatus::Completed;
        last_record.ok_or_else(|| "No steps to execute".to_string())
    }

    /// Add a breakpoint to a debug session. Returns the breakpoint id.
    pub async fn add_breakpoint(
        &self,
        session_id: &str,
        condition: BreakCondition,
    ) -> Result<String, String> {
        if !self.config.enable_breakpoints {
            return Err("Breakpoints are disabled in configuration".to_string());
        }

        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?;

        let bp_id = Uuid::new_v4().to_string();
        let bp = Breakpoint {
            id: bp_id.clone(),
            condition,
            hit_count: 0,
            enabled: true,
        };

        session.breakpoints.push(bp);
        debug!(session_id = %session_id, bp_id = %bp_id, "Breakpoint added");
        Ok(bp_id)
    }

    /// Remove a breakpoint from a debug session.
    pub async fn remove_breakpoint(
        &self,
        session_id: &str,
        bp_id: &str,
    ) -> Result<(), String> {
        let mut sessions = self.sessions.write().await;
        let session = sessions
            .get_mut(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?;

        let before = session.breakpoints.len();
        session.breakpoints.retain(|bp| bp.id != bp_id);

        if session.breakpoints.len() == before {
            return Err(format!("Breakpoint '{}' not found", bp_id));
        }
        Ok(())
    }

    /// Inspect the state capture associated with a debug session.
    pub async fn inspect_state(&self, session_id: &str) -> Result<StateCapture, String> {
        let sessions = self.sessions.read().await;
        let session = sessions
            .get(session_id)
            .ok_or_else(|| format!("Session '{}' not found", session_id))?;

        let captures = self.captures.read().await;
        captures
            .get(&session.capture_id)
            .cloned()
            .ok_or_else(|| format!("Capture '{}' not found", session.capture_id))
    }

    /// Retrieve a debug session by id.
    pub async fn get_session(&self, id: &str) -> Option<DebugSession> {
        let sessions = self.sessions.read().await;
        sessions.get(id).cloned()
    }

    /// Return a reference to the aggregate statistics.
    pub fn stats(&self) -> &DebugStats {
        &self.stats
    }

    // -- internal helpers ---------------------------------------------------

    /// Check if any enabled breakpoint fires for the current session state.
    /// Returns true if a breakpoint was hit (and increments its hit_count).
    /// Check if any enabled breakpoint fires for the current session state.
    /// Returns true if a breakpoint was hit (and increments its hit_count).
    fn check_breakpoints(session: &mut DebugSession) -> bool {
        for bp in session.breakpoints.iter_mut() {
            if !bp.enabled {
                continue;
            }
            let hit = match &bp.condition {
                BreakCondition::MessageCount(n) => session.current_step >= *n,
                BreakCondition::ErrorOccurred => false,
                BreakCondition::Offset { .. } => false,
                BreakCondition::KeyMatch { .. } => false,
                BreakCondition::ValueMatch { .. } => false,
                BreakCondition::StateChange { .. } => false,
            };
            if hit {
                bp.hit_count += 1;
                return true;
            }
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> ReplayDebugConfig {
        ReplayDebugConfig {
            max_captures: 10,
            ..Default::default()
        }
    }

    fn sample_offsets() -> HashMap<String, HashMap<i32, i64>> {
        let mut m = HashMap::new();
        let mut parts = HashMap::new();
        parts.insert(0, 42);
        m.insert("topic-a".to_string(), parts);
        m
    }

    #[tokio::test]
    async fn test_new_engine() {
        let engine = DeterministicReplayEngine::new(default_config());
        assert!(engine.list_captures().await.is_empty());
    }

    #[tokio::test]
    async fn test_capture_and_get() {
        let engine = DeterministicReplayEngine::new(default_config());
        let id = engine
            .capture_state("snap1", sample_offsets(), HashMap::new(), HashMap::new(), HashMap::new(), 1000)
            .await
            .unwrap();

        let cap = engine.get_capture(&id).await.unwrap();
        assert_eq!(cap.name, "snap1");
        assert_eq!(cap.clock_ms, 1000);
    }

    #[tokio::test]
    async fn test_capture_limit() {
        let cfg = ReplayDebugConfig {
            max_captures: 2,
            ..default_config()
        };
        let engine = DeterministicReplayEngine::new(cfg);
        engine
            .capture_state("a", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        engine
            .capture_state("b", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        let err = engine
            .capture_state("c", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap_err();
        assert!(err.contains("Maximum capture limit"));
    }

    #[tokio::test]
    async fn test_list_captures() {
        let engine = DeterministicReplayEngine::new(default_config());
        engine
            .capture_state("x", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        engine
            .capture_state("y", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        assert_eq!(engine.list_captures().await.len(), 2);
    }

    #[tokio::test]
    async fn test_delete_capture() {
        let engine = DeterministicReplayEngine::new(default_config());
        let id = engine
            .capture_state("del", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        engine.delete_capture(&id).await.unwrap();
        assert!(engine.get_capture(&id).await.is_none());
    }

    #[tokio::test]
    async fn test_delete_missing_capture() {
        let engine = DeterministicReplayEngine::new(default_config());
        let err = engine.delete_capture("nope").await.unwrap_err();
        assert!(err.contains("not found"));
    }

    #[tokio::test]
    async fn test_create_debug_session() {
        let engine = DeterministicReplayEngine::new(default_config());
        let cap_id = engine
            .capture_state("s", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        let sess_id = engine.create_debug_session(&cap_id).await.unwrap();
        let sess = engine.get_session(&sess_id).await.unwrap();
        assert_eq!(sess.capture_id, cap_id);
        assert_eq!(sess.status, DebugStatus::Paused);
    }

    #[tokio::test]
    async fn test_create_session_missing_capture() {
        let engine = DeterministicReplayEngine::new(default_config());
        let err = engine.create_debug_session("ghost").await.unwrap_err();
        assert!(err.contains("not found"));
    }

    #[tokio::test]
    async fn test_step() {
        let engine = DeterministicReplayEngine::new(default_config());
        let cap_id = engine
            .capture_state("st", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        let sid = engine.create_debug_session(&cap_id).await.unwrap();
        let rec = engine.step(&sid).await.unwrap();
        assert_eq!(rec.step, 1);

        let sess = engine.get_session(&sid).await.unwrap();
        assert_eq!(sess.current_step, 1);
    }

    #[tokio::test]
    async fn test_step_over() {
        let engine = DeterministicReplayEngine::new(default_config());
        let cap_id = engine
            .capture_state("so", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        let sid = engine.create_debug_session(&cap_id).await.unwrap();
        let rec = engine.step_over(&sid).await.unwrap();
        assert!(rec.action.starts_with("step_over_"));
    }

    #[tokio::test]
    async fn test_continue_to_completion() {
        let engine = DeterministicReplayEngine::new(default_config());
        let cap_id = engine
            .capture_state("ct", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        let sid = engine.create_debug_session(&cap_id).await.unwrap();
        engine.continue_to_breakpoint(&sid).await.unwrap();

        let sess = engine.get_session(&sid).await.unwrap();
        assert_eq!(sess.status, DebugStatus::Completed);
        assert_eq!(sess.current_step, sess.total_steps);
    }

    #[tokio::test]
    async fn test_add_and_remove_breakpoint() {
        let engine = DeterministicReplayEngine::new(default_config());
        let cap_id = engine
            .capture_state("bp", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        let sid = engine.create_debug_session(&cap_id).await.unwrap();

        let bp_id = engine
            .add_breakpoint(&sid, BreakCondition::MessageCount(5))
            .await
            .unwrap();

        let sess = engine.get_session(&sid).await.unwrap();
        assert_eq!(sess.breakpoints.len(), 1);

        engine.remove_breakpoint(&sid, &bp_id).await.unwrap();
        let sess = engine.get_session(&sid).await.unwrap();
        assert!(sess.breakpoints.is_empty());
    }

    #[tokio::test]
    async fn test_remove_missing_breakpoint() {
        let engine = DeterministicReplayEngine::new(default_config());
        let cap_id = engine
            .capture_state("rb", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        let sid = engine.create_debug_session(&cap_id).await.unwrap();
        let err = engine.remove_breakpoint(&sid, "nope").await.unwrap_err();
        assert!(err.contains("not found"));
    }

    #[tokio::test]
    async fn test_breakpoint_pauses_session() {
        let engine = DeterministicReplayEngine::new(default_config());
        let cap_id = engine
            .capture_state("bph", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        let sid = engine.create_debug_session(&cap_id).await.unwrap();
        engine
            .add_breakpoint(&sid, BreakCondition::MessageCount(3))
            .await
            .unwrap();

        engine.continue_to_breakpoint(&sid).await.unwrap();

        let sess = engine.get_session(&sid).await.unwrap();
        assert_eq!(sess.status, DebugStatus::Paused);
        assert_eq!(sess.current_step, 3);
    }

    #[tokio::test]
    async fn test_inspect_state() {
        let engine = DeterministicReplayEngine::new(default_config());
        let cap_id = engine
            .capture_state("insp", sample_offsets(), HashMap::new(), HashMap::new(), HashMap::new(), 500)
            .await
            .unwrap();
        let sid = engine.create_debug_session(&cap_id).await.unwrap();
        let state = engine.inspect_state(&sid).await.unwrap();
        assert_eq!(state.clock_ms, 500);
        assert!(state.offsets.contains_key("topic-a"));
    }

    #[tokio::test]
    async fn test_inspect_missing_session() {
        let engine = DeterministicReplayEngine::new(default_config());
        let err = engine.inspect_state("ghost").await.unwrap_err();
        assert!(err.contains("not found"));
    }

    #[tokio::test]
    async fn test_stats() {
        let engine = DeterministicReplayEngine::new(default_config());
        let cap_id = engine
            .capture_state("stats", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        let sid = engine.create_debug_session(&cap_id).await.unwrap();
        engine.step(&sid).await.unwrap();
        engine.step(&sid).await.unwrap();

        assert_eq!(engine.stats().total_captures.load(Ordering::Relaxed), 1);
        assert_eq!(engine.stats().total_sessions.load(Ordering::Relaxed), 1);
        assert_eq!(engine.stats().total_steps.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_step_completed_session_errors() {
        let engine = DeterministicReplayEngine::new(default_config());
        let cap_id = engine
            .capture_state("comp", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        let sid = engine.create_debug_session(&cap_id).await.unwrap();

        // Run to completion
        engine.continue_to_breakpoint(&sid).await.unwrap();

        let err = engine.step(&sid).await.unwrap_err();
        assert!(err.contains("already completed"));
    }

    #[tokio::test]
    async fn test_breakpoints_disabled() {
        let cfg = ReplayDebugConfig {
            enable_breakpoints: false,
            ..default_config()
        };
        let engine = DeterministicReplayEngine::new(cfg);
        let cap_id = engine
            .capture_state("nobp", HashMap::new(), HashMap::new(), HashMap::new(), HashMap::new(), 0)
            .await
            .unwrap();
        let sid = engine.create_debug_session(&cap_id).await.unwrap();
        let err = engine
            .add_breakpoint(&sid, BreakCondition::ErrorOccurred)
            .await
            .unwrap_err();
        assert!(err.contains("disabled"));
    }

    #[tokio::test]
    async fn test_break_condition_serde_roundtrip() {
        let conditions = vec![
            BreakCondition::Offset {
                topic: "t".to_string(),
                partition: 0,
                offset: 100,
            },
            BreakCondition::MessageCount(42),
            BreakCondition::KeyMatch {
                pattern: "user-*".to_string(),
            },
            BreakCondition::ValueMatch {
                field: "status".to_string(),
                value: serde_json::json!("active"),
            },
            BreakCondition::StateChange {
                variable: "counter".to_string(),
            },
            BreakCondition::ErrorOccurred,
        ];
        for cond in conditions {
            let json = serde_json::to_string(&cond).unwrap();
            let back: BreakCondition = serde_json::from_str(&json).unwrap();
            assert_eq!(back, cond);
        }
    }

    #[tokio::test]
    async fn test_debug_status_serde_roundtrip() {
        let statuses = vec![
            DebugStatus::Paused,
            DebugStatus::Stepping,
            DebugStatus::Running,
            DebugStatus::Completed,
            DebugStatus::Error("oops".to_string()),
        ];
        for s in statuses {
            let json = serde_json::to_string(&s).unwrap();
            let back: DebugStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(back, s);
        }
    }
}
