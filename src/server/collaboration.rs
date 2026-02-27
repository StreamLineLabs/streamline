//! Real-Time Collaboration Engine
//!
//! Enables multiple users to simultaneously edit shared resources (notebooks,
//! pipelines, dashboards, queries, schemas) with live cursor tracking,
//! conflict resolution, and operation history.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::{debug, info, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Strategy used when concurrent edits conflict.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConflictStrategy {
    LastWriterWins,
    CrdtMerge,
    ManualResolve,
}

impl Default for ConflictStrategy {
    fn default() -> Self {
        Self::LastWriterWins
    }
}

/// Configuration for the collaboration engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollabConfig {
    /// Maximum participants allowed in a single session.
    #[serde(default = "default_max_users")]
    pub max_users_per_session: usize,
    /// Maximum concurrent sessions.
    #[serde(default = "default_max_sessions")]
    pub max_sessions: usize,
    /// Seconds of inactivity before a session is considered idle.
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,
    /// Milliseconds between sync broadcasts.
    #[serde(default = "default_sync_interval")]
    pub sync_interval_ms: u64,
    /// Strategy for resolving conflicting edits.
    #[serde(default)]
    pub conflict_resolution: ConflictStrategy,
}

fn default_max_users() -> usize { 20 }
fn default_max_sessions() -> usize { 1000 }
fn default_idle_timeout() -> u64 { 1800 }
fn default_sync_interval() -> u64 { 100 }

impl Default for CollabConfig {
    fn default() -> Self {
        Self {
            max_users_per_session: default_max_users(),
            max_sessions: default_max_sessions(),
            idle_timeout_secs: default_idle_timeout(),
            sync_interval_ms: default_sync_interval(),
            conflict_resolution: ConflictStrategy::default(),
        }
    }
}

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/// Kind of resource being collaboratively edited.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ResourceType {
    Notebook,
    Pipeline,
    Dashboard,
    Query,
    Schema,
}

/// Role a participant holds within a session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CollabRole {
    Owner,
    Editor,
    Viewer,
}

/// Cursor / selection state for a participant.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CursorPosition {
    pub line: u32,
    pub column: u32,
    pub selection_start: Option<(u32, u32)>,
    pub selection_end: Option<(u32, u32)>,
}

/// A single participant in a collaboration session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Participant {
    pub user_id: String,
    pub display_name: String,
    /// Hex colour used for cursor rendering.
    pub color: String,
    pub cursor_position: Option<CursorPosition>,
    pub role: CollabRole,
    pub joined_at: String,
    pub last_active_at: String,
    pub online: bool,
}

/// Type of collaborative operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OpType {
    Insert,
    Delete,
    Update,
    Move,
    SetCursor,
}

/// A single collaborative operation applied to a session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollabOperation {
    pub id: String,
    pub user_id: String,
    pub op_type: OpType,
    pub path: String,
    pub value: Option<serde_json::Value>,
    pub timestamp: String,
    pub version: u64,
}

/// A collaboration session that groups participants and operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollabSession {
    pub id: String,
    pub resource_type: ResourceType,
    pub resource_id: String,
    pub participants: Vec<Participant>,
    pub operations: Vec<CollabOperation>,
    pub created_at: String,
    pub last_activity_at: String,
}

// ---------------------------------------------------------------------------
// Stats (lock-free)
// ---------------------------------------------------------------------------

/// Atomic counters exposed for observability.
#[derive(Debug, Default)]
pub struct CollabStats {
    pub sessions: AtomicU64,
    pub participants: AtomicU64,
    pub operations: AtomicU64,
    pub conflicts_resolved: AtomicU64,
}

/// Snapshot of [`CollabStats`] for serialisation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollabStatsSnapshot {
    pub sessions: u64,
    pub participants: u64,
    pub operations: u64,
    pub conflicts_resolved: u64,
}

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

/// Thread-safe collaboration engine managing sessions and operations.
pub struct CollaborationEngine {
    sessions: Arc<RwLock<HashMap<String, CollabSession>>>,
    config: CollabConfig,
    stats: Arc<CollabStats>,
}

impl CollaborationEngine {
    /// Create a new engine with the given configuration.
    pub fn new(config: CollabConfig) -> Self {
        info!(
            max_sessions = config.max_sessions,
            max_users = config.max_users_per_session,
            "collaboration engine initialised"
        );
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            config,
            stats: Arc::new(CollabStats::default()),
        }
    }

    /// Create a new session for a resource and return its id.
    pub fn create_session(
        &self,
        resource_type: ResourceType,
        resource_id: &str,
        creator: Participant,
    ) -> Result<String, CollabError> {
        let mut sessions = self.sessions.write();
        if sessions.len() >= self.config.max_sessions {
            warn!(limit = self.config.max_sessions, "session limit reached");
            return Err(CollabError::SessionLimitReached);
        }

        let now = chrono_now();
        let session_id = generate_id();
        let session = CollabSession {
            id: session_id.clone(),
            resource_type,
            resource_id: resource_id.to_string(),
            participants: vec![creator],
            operations: Vec::new(),
            created_at: now.clone(),
            last_activity_at: now,
        };

        sessions.insert(session_id.clone(), session);
        self.stats.sessions.fetch_add(1, Ordering::Relaxed);
        self.stats.participants.fetch_add(1, Ordering::Relaxed);

        debug!(session_id = %session_id, "collaboration session created");
        Ok(session_id)
    }

    /// Add a participant to an existing session.
    pub fn join_session(
        &self,
        session_id: &str,
        user: Participant,
    ) -> Result<(), CollabError> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(session_id)
            .ok_or(CollabError::SessionNotFound)?;

        if session.participants.len() >= self.config.max_users_per_session {
            warn!(session_id, limit = self.config.max_users_per_session, "participant limit reached");
            return Err(CollabError::ParticipantLimitReached);
        }

        if session.participants.iter().any(|p| p.user_id == user.user_id) {
            return Err(CollabError::AlreadyInSession);
        }

        debug!(session_id, user_id = %user.user_id, "user joined session");
        session.participants.push(user);
        session.last_activity_at = chrono_now();
        self.stats.participants.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Remove a participant from a session.
    pub fn leave_session(
        &self,
        session_id: &str,
        user_id: &str,
    ) -> Result<(), CollabError> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(session_id)
            .ok_or(CollabError::SessionNotFound)?;

        let before = session.participants.len();
        session.participants.retain(|p| p.user_id != user_id);
        if session.participants.len() == before {
            return Err(CollabError::UserNotInSession);
        }

        session.last_activity_at = chrono_now();
        self.stats.participants.fetch_sub(1, Ordering::Relaxed);
        debug!(session_id, user_id, "user left session");
        Ok(())
    }

    /// Update the cursor position for a participant.
    pub fn update_cursor(
        &self,
        session_id: &str,
        user_id: &str,
        pos: CursorPosition,
    ) -> Result<(), CollabError> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(session_id)
            .ok_or(CollabError::SessionNotFound)?;

        let participant = session
            .participants
            .iter_mut()
            .find(|p| p.user_id == user_id)
            .ok_or(CollabError::UserNotInSession)?;

        participant.cursor_position = Some(pos);
        participant.last_active_at = chrono_now();
        session.last_activity_at = chrono_now();
        Ok(())
    }

    /// Apply a collaborative operation to a session.
    pub fn apply_operation(
        &self,
        session_id: &str,
        mut op: CollabOperation,
    ) -> Result<u64, CollabError> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(session_id)
            .ok_or(CollabError::SessionNotFound)?;

        // Verify the user is a participant with edit rights.
        let role = session
            .participants
            .iter()
            .find(|p| p.user_id == op.user_id)
            .map(|p| &p.role)
            .ok_or(CollabError::UserNotInSession)?;

        if *role == CollabRole::Viewer {
            return Err(CollabError::InsufficientRole);
        }

        // Assign monotonic version.
        let version = session.operations.len() as u64 + 1;
        op.version = version;
        op.timestamp = chrono_now();

        // Simple last-writer-wins conflict detection: if the incoming op
        // targets the same path as the previous op by a different user we
        // count it as a resolved conflict.
        if self.config.conflict_resolution == ConflictStrategy::LastWriterWins {
            if let Some(prev) = session.operations.last() {
                if prev.path == op.path && prev.user_id != op.user_id {
                    self.stats.conflicts_resolved.fetch_add(1, Ordering::Relaxed);
                    debug!(session_id, path = %op.path, "conflict resolved (last-writer-wins)");
                }
            }
        }

        session.operations.push(op);
        session.last_activity_at = chrono_now();
        self.stats.operations.fetch_add(1, Ordering::Relaxed);
        Ok(version)
    }

    /// Return a clone of the session, if it exists.
    pub fn get_session(&self, session_id: &str) -> Option<CollabSession> {
        self.sessions.read().get(session_id).cloned()
    }

    /// List all active sessions (lightweight summaries).
    pub fn list_sessions(&self) -> Vec<CollabSession> {
        self.sessions.read().values().cloned().collect()
    }

    /// Get the participants for a session.
    pub fn get_participants(&self, session_id: &str) -> Result<Vec<Participant>, CollabError> {
        let sessions = self.sessions.read();
        let session = sessions
            .get(session_id)
            .ok_or(CollabError::SessionNotFound)?;
        Ok(session.participants.clone())
    }

    /// Return an atomic snapshot of engine statistics.
    pub fn stats(&self) -> CollabStatsSnapshot {
        CollabStatsSnapshot {
            sessions: self.stats.sessions.load(Ordering::Relaxed),
            participants: self.stats.participants.load(Ordering::Relaxed),
            operations: self.stats.operations.load(Ordering::Relaxed),
            conflicts_resolved: self.stats.conflicts_resolved.load(Ordering::Relaxed),
        }
    }
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors returned by the collaboration engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CollabError {
    SessionNotFound,
    SessionLimitReached,
    ParticipantLimitReached,
    AlreadyInSession,
    UserNotInSession,
    InsufficientRole,
}

impl std::fmt::Display for CollabError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SessionNotFound => write!(f, "session not found"),
            Self::SessionLimitReached => write!(f, "session limit reached"),
            Self::ParticipantLimitReached => write!(f, "participant limit reached"),
            Self::AlreadyInSession => write!(f, "user already in session"),
            Self::UserNotInSession => write!(f, "user not in session"),
            Self::InsufficientRole => write!(f, "insufficient role for this operation"),
        }
    }
}

impl std::error::Error for CollabError {}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn chrono_now() -> String {
    // Simple monotonic-friendly timestamp; production would use chrono/time.
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_default()
}

fn generate_id() -> String {
    use std::sync::atomic::AtomicU64;
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    format!("collab-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn default_engine() -> CollaborationEngine {
        CollaborationEngine::new(CollabConfig::default())
    }

    fn make_participant(id: &str, role: CollabRole) -> Participant {
        Participant {
            user_id: id.to_string(),
            display_name: id.to_string(),
            color: "#ff0000".to_string(),
            cursor_position: None,
            role,
            joined_at: chrono_now(),
            last_active_at: chrono_now(),
            online: true,
        }
    }

    fn make_op(user_id: &str, op_type: OpType, path: &str) -> CollabOperation {
        CollabOperation {
            id: generate_id(),
            user_id: user_id.to_string(),
            op_type,
            path: path.to_string(),
            value: None,
            timestamp: String::new(),
            version: 0,
        }
    }

    // -- Config defaults ----------------------------------------------------

    #[test]
    fn test_default_config_values() {
        let cfg = CollabConfig::default();
        assert_eq!(cfg.max_users_per_session, 20);
        assert_eq!(cfg.max_sessions, 1000);
        assert_eq!(cfg.idle_timeout_secs, 1800);
        assert_eq!(cfg.sync_interval_ms, 100);
        assert_eq!(cfg.conflict_resolution, ConflictStrategy::LastWriterWins);
    }

    // -- Engine creation ----------------------------------------------------

    #[test]
    fn test_new_engine_empty() {
        let engine = default_engine();
        assert!(engine.list_sessions().is_empty());
        let s = engine.stats();
        assert_eq!(s.sessions, 0);
        assert_eq!(s.participants, 0);
    }

    // -- Session lifecycle --------------------------------------------------

    #[test]
    fn test_create_session() {
        let engine = default_engine();
        let creator = make_participant("alice", CollabRole::Owner);
        let sid = engine
            .create_session(ResourceType::Notebook, "nb-1", creator)
            .unwrap();
        assert!(!sid.is_empty());
        assert_eq!(engine.list_sessions().len(), 1);
        assert_eq!(engine.stats().sessions, 1);
        assert_eq!(engine.stats().participants, 1);
    }

    #[test]
    fn test_session_limit_reached() {
        let config = CollabConfig {
            max_sessions: 1,
            ..Default::default()
        };
        let engine = CollaborationEngine::new(config);
        let _ = engine
            .create_session(ResourceType::Pipeline, "p1", make_participant("a", CollabRole::Owner))
            .unwrap();
        let err = engine
            .create_session(ResourceType::Pipeline, "p2", make_participant("b", CollabRole::Owner))
            .unwrap_err();
        assert_eq!(err, CollabError::SessionLimitReached);
    }

    #[test]
    fn test_get_session() {
        let engine = default_engine();
        let sid = engine
            .create_session(ResourceType::Dashboard, "d-1", make_participant("alice", CollabRole::Owner))
            .unwrap();
        let session = engine.get_session(&sid).unwrap();
        assert_eq!(session.resource_type, ResourceType::Dashboard);
        assert_eq!(session.resource_id, "d-1");
    }

    #[test]
    fn test_get_session_not_found() {
        let engine = default_engine();
        assert!(engine.get_session("nonexistent").is_none());
    }

    // -- Join / leave -------------------------------------------------------

    #[test]
    fn test_join_session() {
        let engine = default_engine();
        let sid = engine
            .create_session(ResourceType::Query, "q1", make_participant("alice", CollabRole::Owner))
            .unwrap();
        engine
            .join_session(&sid, make_participant("bob", CollabRole::Editor))
            .unwrap();
        let parts = engine.get_participants(&sid).unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(engine.stats().participants, 2);
    }

    #[test]
    fn test_join_session_duplicate() {
        let engine = default_engine();
        let sid = engine
            .create_session(ResourceType::Schema, "s1", make_participant("alice", CollabRole::Owner))
            .unwrap();
        let err = engine
            .join_session(&sid, make_participant("alice", CollabRole::Editor))
            .unwrap_err();
        assert_eq!(err, CollabError::AlreadyInSession);
    }

    #[test]
    fn test_join_session_participant_limit() {
        let config = CollabConfig {
            max_users_per_session: 1,
            ..Default::default()
        };
        let engine = CollaborationEngine::new(config);
        let sid = engine
            .create_session(ResourceType::Notebook, "nb", make_participant("alice", CollabRole::Owner))
            .unwrap();
        let err = engine
            .join_session(&sid, make_participant("bob", CollabRole::Editor))
            .unwrap_err();
        assert_eq!(err, CollabError::ParticipantLimitReached);
    }

    #[test]
    fn test_leave_session() {
        let engine = default_engine();
        let sid = engine
            .create_session(ResourceType::Notebook, "nb", make_participant("alice", CollabRole::Owner))
            .unwrap();
        engine.join_session(&sid, make_participant("bob", CollabRole::Editor)).unwrap();
        engine.leave_session(&sid, "bob").unwrap();
        assert_eq!(engine.get_participants(&sid).unwrap().len(), 1);
        assert_eq!(engine.stats().participants, 1);
    }

    #[test]
    fn test_leave_session_user_not_found() {
        let engine = default_engine();
        let sid = engine
            .create_session(ResourceType::Notebook, "nb", make_participant("alice", CollabRole::Owner))
            .unwrap();
        let err = engine.leave_session(&sid, "ghost").unwrap_err();
        assert_eq!(err, CollabError::UserNotInSession);
    }

    // -- Cursor updates -----------------------------------------------------

    #[test]
    fn test_update_cursor() {
        let engine = default_engine();
        let sid = engine
            .create_session(ResourceType::Notebook, "nb", make_participant("alice", CollabRole::Owner))
            .unwrap();
        let pos = CursorPosition {
            line: 10,
            column: 5,
            selection_start: None,
            selection_end: None,
        };
        engine.update_cursor(&sid, "alice", pos.clone()).unwrap();
        let parts = engine.get_participants(&sid).unwrap();
        assert_eq!(parts[0].cursor_position.as_ref().unwrap().line, 10);
    }

    #[test]
    fn test_update_cursor_user_not_in_session() {
        let engine = default_engine();
        let sid = engine
            .create_session(ResourceType::Notebook, "nb", make_participant("alice", CollabRole::Owner))
            .unwrap();
        let pos = CursorPosition { line: 0, column: 0, selection_start: None, selection_end: None };
        let err = engine.update_cursor(&sid, "ghost", pos).unwrap_err();
        assert_eq!(err, CollabError::UserNotInSession);
    }

    // -- Operations ---------------------------------------------------------

    #[test]
    fn test_apply_operation() {
        let engine = default_engine();
        let sid = engine
            .create_session(ResourceType::Notebook, "nb", make_participant("alice", CollabRole::Owner))
            .unwrap();
        let op = make_op("alice", OpType::Insert, "/cells/0");
        let ver = engine.apply_operation(&sid, op).unwrap();
        assert_eq!(ver, 1);
        assert_eq!(engine.stats().operations, 1);
    }

    #[test]
    fn test_apply_operation_viewer_rejected() {
        let engine = default_engine();
        let sid = engine
            .create_session(ResourceType::Notebook, "nb", make_participant("alice", CollabRole::Owner))
            .unwrap();
        engine.join_session(&sid, make_participant("viewer", CollabRole::Viewer)).unwrap();
        let op = make_op("viewer", OpType::Update, "/cells/0");
        let err = engine.apply_operation(&sid, op).unwrap_err();
        assert_eq!(err, CollabError::InsufficientRole);
    }

    #[test]
    fn test_apply_operation_increments_version() {
        let engine = default_engine();
        let sid = engine
            .create_session(ResourceType::Pipeline, "p1", make_participant("alice", CollabRole::Owner))
            .unwrap();
        let v1 = engine.apply_operation(&sid, make_op("alice", OpType::Insert, "/a")).unwrap();
        let v2 = engine.apply_operation(&sid, make_op("alice", OpType::Update, "/a")).unwrap();
        assert_eq!(v1, 1);
        assert_eq!(v2, 2);
    }

    // -- Conflict resolution ------------------------------------------------

    #[test]
    fn test_conflict_detected_last_writer_wins() {
        let engine = default_engine();
        let sid = engine
            .create_session(ResourceType::Notebook, "nb", make_participant("alice", CollabRole::Owner))
            .unwrap();
        engine.join_session(&sid, make_participant("bob", CollabRole::Editor)).unwrap();
        engine.apply_operation(&sid, make_op("alice", OpType::Update, "/title")).unwrap();
        engine.apply_operation(&sid, make_op("bob", OpType::Update, "/title")).unwrap();
        assert_eq!(engine.stats().conflicts_resolved, 1);
    }

    // -- Serialisation round-trip -------------------------------------------

    #[test]
    fn test_config_serde_roundtrip() {
        let cfg = CollabConfig::default();
        let json = serde_json::to_string(&cfg).unwrap();
        let parsed: CollabConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.max_sessions, cfg.max_sessions);
        assert_eq!(parsed.sync_interval_ms, cfg.sync_interval_ms);
    }
}
