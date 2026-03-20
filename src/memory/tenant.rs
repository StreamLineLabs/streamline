//! Multi-tenant memory isolation (M1 P3).
//!
//! Ensures complete tenant isolation: cross-tenant reads are impossible
//! even with valid agent IDs.

/// Context extracted from the MCP session token identifying the tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantContext {
    pub tenant_id: String,
}

impl TenantContext {
    /// Creates a context for the given tenant.
    pub fn new(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
        }
    }
}

/// Validates that `topic` belongs to the tenant in `ctx`.
///
/// A topic is accessible when it contains the segment `__mem.<tenant_id>.`
/// matching the context's tenant.
pub fn validate_tenant_access(ctx: &TenantContext, topic: &str) -> bool {
    let marker = format!("__mem.{}.", ctx.tenant_id);
    topic.contains(&marker)
}

/// Extracts the tenant segment from a topic following the convention
/// `__mem.<tenant>.agent.*` (or any sub-path after the tenant segment).
///
/// Returns `None` for malformed topics that don't start with `__mem.` or
/// have fewer than three dot-separated segments.
pub fn extract_tenant_from_topic(topic: &str) -> Option<&str> {
    let rest = topic.strip_prefix("__mem.")?;
    let dot = rest.find('.')?;
    if dot == 0 {
        return None;
    }
    Some(&rest[..dot])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_tenant_access_allowed() {
        let ctx = TenantContext::new("acme");
        assert!(validate_tenant_access(&ctx, "__mem.acme.agent1.episodic"));
        assert!(validate_tenant_access(&ctx, "__mem.acme.agent2.semantic"));
    }

    #[test]
    fn cross_tenant_access_denied() {
        let ctx = TenantContext::new("acme");
        assert!(!validate_tenant_access(&ctx, "__mem.globex.agent1.episodic"));
        assert!(!validate_tenant_access(&ctx, "__mem.initech.agent2.semantic"));
    }

    #[test]
    fn malformed_topic_denied() {
        let ctx = TenantContext::new("acme");
        assert!(!validate_tenant_access(&ctx, "random-topic"));
        assert!(!validate_tenant_access(&ctx, "__mem."));
        assert!(!validate_tenant_access(&ctx, ""));
    }

    #[test]
    fn tenant_id_substring_not_confused() {
        let ctx = TenantContext::new("acme");
        // "acme-corp" is a different tenant
        assert!(!validate_tenant_access(&ctx, "__mem.acme-corp.agent.episodic"));
    }

    #[test]
    fn extract_tenant_valid() {
        assert_eq!(
            extract_tenant_from_topic("__mem.acme.agent1.episodic"),
            Some("acme")
        );
        assert_eq!(
            extract_tenant_from_topic("__mem.globex.shared.ns1"),
            Some("globex")
        );
    }

    #[test]
    fn extract_tenant_malformed() {
        assert_eq!(extract_tenant_from_topic("random-topic"), None);
        assert_eq!(extract_tenant_from_topic("__mem."), None);
        assert_eq!(extract_tenant_from_topic("__mem..agent"), None);
        assert_eq!(extract_tenant_from_topic(""), None);
    }

    #[test]
    fn extract_tenant_no_sub_path() {
        // Only two segments (no sub-path after tenant) → still valid
        assert_eq!(extract_tenant_from_topic("__mem.acme.x"), Some("acme"));
    }
}
