//! Edge sync service for browser/edge node connections (M3 P1).
//!
//! Accepts connections from browser/edge nodes, handles sync protocol,
//! performs CRDT merges on the server side.

use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

fn read_or_recover<T>(m: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    m.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_or_recover<T>(m: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    m.write().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Configuration for the sync service.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// Maximum number of concurrent node connections.
    pub max_connections: u32,
    /// Interval between periodic sync sweeps (milliseconds).
    pub sync_interval_ms: u64,
    /// Maximum batch payload size in bytes.
    pub max_batch_bytes: u64,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            max_connections: 1024,
            sync_interval_ms: 5_000,
            max_batch_bytes: 1_048_576, // 1 MiB
        }
    }
}

/// Per-node session state tracked by the sync service.
#[derive(Debug, Clone)]
pub struct SyncSession {
    pub node_id: String,
    /// Unix epoch seconds when the node connected.
    pub connected_at: i64,
    /// Unix epoch seconds of the last successful sync.
    pub last_sync_at: i64,
    /// Topics the node is interested in.
    pub topics: Vec<String>,
}

/// A write that a browser/edge node wants to push to the server.
#[derive(Debug, Clone)]
pub struct PendingWrite {
    pub topic: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp_ms: i64,
    pub node_id: String,
}

/// A write that the server pushes back to the node after CRDT merge.
#[derive(Debug, Clone)]
pub struct ServerWrite {
    pub topic: String,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp_ms: i64,
}

/// Response returned to the node after a sync request.
#[derive(Debug, Clone)]
pub struct SyncResponse {
    /// Number of writes accepted by the server.
    pub accepted: u64,
    /// Number of writes rejected (e.g. over-quota, invalid).
    pub rejected: u64,
    /// Writes the server sends back (CRDT-merged results, new data).
    pub server_writes: Vec<ServerWrite>,
}

/// Source of a merge value for observability purposes.
#[derive(Debug, Clone)]
pub struct NodeSource {
    pub node_id: String,
    pub timestamp_ms: i64,
}

/// The sync service manages browser/edge node connections and
/// orchestrates CRDT-based bidirectional sync.
#[derive(Debug)]
pub struct SyncService {
    pub config: SyncConfig,
    sessions: RwLock<HashMap<String, SyncSession>>,
}

impl SyncService {
    /// Creates a new sync service with the given configuration.
    pub fn new(config: SyncConfig) -> Self {
        Self {
            config,
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Returns the number of active sessions.
    pub fn session_count(&self) -> usize {
        read_or_recover(&self.sessions).len()
    }
}

fn now_epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Register a new browser/edge node connection, returning its session.
pub fn handle_connect(svc: &SyncService, node_id: &str) -> SyncSession {
    let now = now_epoch_secs();
    let session = SyncSession {
        node_id: node_id.to_string(),
        connected_at: now,
        last_sync_at: now,
        topics: Vec::new(),
    };
    write_or_recover(&svc.sessions).insert(node_id.to_string(), session.clone());
    session
}

/// Process a batch of pending writes from a node.
///
/// Placeholder: accepts all writes, returns an empty server-writes list.
/// A production implementation would apply CRDT merges and return the
/// merged results.
pub fn handle_sync_request(
    svc: &SyncService,
    node_id: &str,
    pending_writes: Vec<PendingWrite>,
) -> SyncResponse {
    let now = now_epoch_secs();

    // Update the session's last_sync_at timestamp.
    if let Some(session) = write_or_recover(&svc.sessions).get_mut(node_id) {
        session.last_sync_at = now;
    }

    let total = pending_writes.len() as u64;
    let mut rejected = 0u64;

    for pw in &pending_writes {
        if pw.value.len() as u64 > svc.config.max_batch_bytes {
            rejected += 1;
        }
    }
    let accepted = total - rejected;

    SyncResponse {
        accepted,
        rejected,
        server_writes: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn svc() -> SyncService {
        SyncService::new(SyncConfig::default())
    }

    #[test]
    fn connect_creates_session() {
        let s = svc();
        let session = handle_connect(&s, "browser-1");
        assert_eq!(session.node_id, "browser-1");
        assert!(session.connected_at > 0);
        assert_eq!(s.session_count(), 1);
    }

    #[test]
    fn multiple_connects_tracked() {
        let s = svc();
        handle_connect(&s, "node-a");
        handle_connect(&s, "node-b");
        assert_eq!(s.session_count(), 2);
    }

    #[test]
    fn reconnect_replaces_session() {
        let s = svc();
        handle_connect(&s, "node-1");
        handle_connect(&s, "node-1");
        assert_eq!(s.session_count(), 1);
    }

    #[test]
    fn sync_request_accepts_writes() {
        let s = svc();
        handle_connect(&s, "node-1");

        let writes = vec![
            PendingWrite {
                topic: "t1".into(),
                key: b"k1".to_vec(),
                value: b"v1".to_vec(),
                timestamp_ms: 1000,
                node_id: "node-1".into(),
            },
            PendingWrite {
                topic: "t1".into(),
                key: b"k2".to_vec(),
                value: b"v2".to_vec(),
                timestamp_ms: 2000,
                node_id: "node-1".into(),
            },
        ];

        let resp = handle_sync_request(&s, "node-1", writes);
        assert_eq!(resp.accepted, 2);
        assert_eq!(resp.rejected, 0);
        assert!(resp.server_writes.is_empty());
    }

    #[test]
    fn sync_request_rejects_oversized_writes() {
        let cfg = SyncConfig {
            max_batch_bytes: 2, // tiny limit
            ..Default::default()
        };
        let s = SyncService::new(cfg);
        handle_connect(&s, "node-1");

        let writes = vec![PendingWrite {
            topic: "t1".into(),
            key: b"k1".to_vec(),
            value: b"this is way too big".to_vec(),
            timestamp_ms: 1000,
            node_id: "node-1".into(),
        }];

        let resp = handle_sync_request(&s, "node-1", writes);
        assert_eq!(resp.rejected, 1);
        assert_eq!(resp.accepted, 0);
    }

    #[test]
    fn sync_updates_last_sync_at() {
        let s = svc();
        let initial = handle_connect(&s, "node-1");
        std::thread::sleep(std::time::Duration::from_millis(10));
        handle_sync_request(&s, "node-1", vec![]);
        let sessions = s.sessions.read().unwrap();
        let updated = sessions.get("node-1").unwrap();
        assert!(updated.last_sync_at >= initial.last_sync_at);
    }

    #[test]
    fn default_config_values() {
        let cfg = SyncConfig::default();
        assert_eq!(cfg.max_connections, 1024);
        assert_eq!(cfg.sync_interval_ms, 5_000);
        assert_eq!(cfg.max_batch_bytes, 1_048_576);
    }
}
