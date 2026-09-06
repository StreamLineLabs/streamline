//! Multi-agent shared memory namespaces (M1 P2).
//!
//! Shared namespaces like `__mem.<tenant>.shared.<namespace>.*` allow
//! multiple agents to read/write to common memory.

/// A shared memory namespace scoped to a tenant.
#[derive(Debug, Clone)]
pub struct SharedNamespace {
    pub tenant: String,
    pub namespace: String,
    /// Agent IDs permitted to access this namespace. An empty list means
    /// all agents within the tenant may access it.
    pub allowed_agents: Vec<String>,
}

/// Create a new shared namespace with no agent restrictions (all agents
/// within the tenant may access it).
pub fn create_shared_namespace(tenant: &str, namespace: &str) -> SharedNamespace {
    SharedNamespace {
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        allowed_agents: Vec::new(),
    }
}

/// Returns `true` when `topic` matches the shared-namespace convention
/// `__mem.*.shared.*`.
pub fn is_shared_topic(topic: &str) -> bool {
    let rest = match topic.strip_prefix("__mem.") {
        Some(r) => r,
        None => return false,
    };
    // Expect at least `<tenant>.shared.<something>`
    let mut parts = rest.splitn(3, '.');
    let _tenant = match parts.next() {
        Some(t) if !t.is_empty() => t,
        _ => return false,
    };
    match parts.next() {
        Some("shared") => {}
        _ => return false,
    }
    // Must have at least one more segment
    parts.next().is_some_and(|s| !s.is_empty())
}

/// Check whether `agent_id` is allowed to access the shared namespace.
///
/// If `allowed_agents` is empty, access is open to all agents in the
/// tenant. Otherwise the agent must be in the allow-list.
pub fn check_shared_access(ns: &SharedNamespace, agent_id: &str) -> bool {
    if ns.allowed_agents.is_empty() {
        return true;
    }
    ns.allowed_agents.iter().any(|a| a == agent_id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_shared_namespace_defaults() {
        let ns = create_shared_namespace("acme", "project-x");
        assert_eq!(ns.tenant, "acme");
        assert_eq!(ns.namespace, "project-x");
        assert!(ns.allowed_agents.is_empty());
    }

    #[test]
    fn is_shared_topic_valid() {
        assert!(is_shared_topic("__mem.acme.shared.project-x"));
        assert!(is_shared_topic("__mem.globex.shared.ns1.sub"));
    }

    #[test]
    fn is_shared_topic_non_shared() {
        assert!(!is_shared_topic("__mem.acme.agent1.episodic"));
        assert!(!is_shared_topic("regular-topic"));
        assert!(!is_shared_topic("__mem.acme.shared"));
        assert!(!is_shared_topic("__mem..shared.x"));
    }

    #[test]
    fn check_shared_access_open() {
        let ns = create_shared_namespace("acme", "project-x");
        assert!(check_shared_access(&ns, "any-agent"));
    }

    #[test]
    fn check_shared_access_restricted() {
        let mut ns = create_shared_namespace("acme", "project-x");
        ns.allowed_agents = vec!["agent-a".into(), "agent-b".into()];
        assert!(check_shared_access(&ns, "agent-a"));
        assert!(check_shared_access(&ns, "agent-b"));
        assert!(!check_shared_access(&ns, "agent-c"));
    }

    #[test]
    fn is_shared_topic_empty_segments() {
        assert!(!is_shared_topic("__mem."));
        assert!(!is_shared_topic("__mem.acme.shared."));
        assert!(!is_shared_topic(""));
    }
}
