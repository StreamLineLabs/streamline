//! Per-agent ACL guard for memory access (M1 P2).
//!
//! Each agent is a principal `agent:<id>`. By default, agents can only
//! access their own memory topics. Share grants provide scoped read access.

use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

fn read_or_recover<T>(m: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    m.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_or_recover<T>(m: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    m.write().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A time-bounded grant allowing one agent to read another's memories.
#[derive(Debug, Clone)]
pub struct ShareGrant {
    pub from_agent: String,
    pub to_agent: String,
    /// Topic prefix the grantee may read (e.g. `"__mem.alice.semantic"`).
    pub prefix: String,
    /// Unix epoch seconds when the grant was created.
    pub granted_at: i64,
    /// Optional time-to-live in seconds; `None` means no expiry.
    pub ttl_secs: Option<u64>,
}

/// Registry of share grants, protected by a `RwLock` for concurrent access.
#[derive(Debug, Default)]
pub struct MemoryAcl {
    grants: RwLock<Vec<ShareGrant>>,
}

impl MemoryAcl {
    /// Creates an empty ACL with no grants.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Check whether `agent_id` may access `target_topic`.
///
/// Access is allowed when:
///   1. The topic belongs to the agent (`__mem.<tenant>.<agent_id>`), **or**
///   2. A valid (non-expired) share grant covers the topic.
pub fn check_access(acl: &MemoryAcl, agent_id: &str, target_topic: &str) -> bool {
    // Own-topic: `__mem.<agent_id>.` anywhere in the topic string.
    let own_marker = format!("__mem.{}", agent_id);
    if target_topic.contains(&own_marker) {
        return true;
    }

    let grants = read_or_recover(&acl.grants);
    grants.iter().any(|g| {
        g.to_agent == agent_id
            && target_topic.starts_with(&g.prefix)
            && !is_expired(g)
    })
}

/// Record a share grant from one agent to another.
pub fn grant(acl: &MemoryAcl, from: &str, to: &str, prefix: &str, ttl_secs: Option<u64>) {
    let now = now_epoch_secs();
    let g = ShareGrant {
        from_agent: from.to_string(),
        to_agent: to.to_string(),
        prefix: prefix.to_string(),
        granted_at: now,
        ttl_secs,
    };
    write_or_recover(&acl.grants).push(g);
}

/// Remove all matching grants.
pub fn revoke(acl: &MemoryAcl, from: &str, to: &str, prefix: &str) {
    let mut grants = write_or_recover(&acl.grants);
    grants.retain(|g| !(g.from_agent == from && g.to_agent == to && g.prefix == prefix));
}

/// Returns `true` when a grant's TTL has elapsed.
pub fn is_expired(grant: &ShareGrant) -> bool {
    match grant.ttl_secs {
        None => false,
        Some(ttl) => {
            let deadline = grant.granted_at + ttl as i64;
            now_epoch_secs() > deadline
        }
    }
}

fn now_epoch_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn own_topic_always_allowed() {
        let acl = MemoryAcl::new();
        assert!(check_access(&acl, "alice", "__mem.alice.semantic"));
        assert!(check_access(&acl, "alice", "__mem.alice.episodic"));
    }

    #[test]
    fn other_agent_denied_by_default() {
        let acl = MemoryAcl::new();
        assert!(!check_access(&acl, "bob", "__mem.alice.semantic"));
    }

    #[test]
    fn shared_access_works() {
        let acl = MemoryAcl::new();
        grant(&acl, "alice", "bob", "__mem.alice.semantic", None);
        assert!(check_access(&acl, "bob", "__mem.alice.semantic"));
    }

    #[test]
    fn shared_access_wrong_prefix_denied() {
        let acl = MemoryAcl::new();
        grant(&acl, "alice", "bob", "__mem.alice.semantic", None);
        // Grant is for semantic, not procedural
        assert!(!check_access(&acl, "bob", "__mem.alice.procedural"));
    }

    #[test]
    fn expired_grant_denied() {
        let acl = MemoryAcl::new();
        // Manually insert a grant that expired in the past
        let g = ShareGrant {
            from_agent: "alice".to_string(),
            to_agent: "bob".to_string(),
            prefix: "__mem.alice.semantic".to_string(),
            granted_at: 0, // epoch = 1970
            ttl_secs: Some(1),
        };
        acl.grants.write().unwrap().push(g);
        assert!(!check_access(&acl, "bob", "__mem.alice.semantic"));
    }

    #[test]
    fn revoke_removes_grant() {
        let acl = MemoryAcl::new();
        grant(&acl, "alice", "bob", "__mem.alice.semantic", None);
        assert!(check_access(&acl, "bob", "__mem.alice.semantic"));

        revoke(&acl, "alice", "bob", "__mem.alice.semantic");
        assert!(!check_access(&acl, "bob", "__mem.alice.semantic"));
    }

    #[test]
    fn no_ttl_never_expires() {
        let g = ShareGrant {
            from_agent: "a".into(),
            to_agent: "b".into(),
            prefix: "p".into(),
            granted_at: 0,
            ttl_secs: None,
        };
        assert!(!is_expired(&g));
    }

    #[test]
    fn grant_with_future_deadline_not_expired() {
        let g = ShareGrant {
            from_agent: "a".into(),
            to_agent: "b".into(),
            prefix: "p".into(),
            granted_at: now_epoch_secs(),
            ttl_secs: Some(86_400), // 1 day from now
        };
        assert!(!is_expired(&g));
    }
}
