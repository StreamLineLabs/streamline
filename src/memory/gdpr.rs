//! GDPR delete-by-agent implementation (M1 P2).
//!
//! Deletes all memory data for an agent within the SLA (1 hour).

use super::Tier;

/// A request to delete all memory data for a specific agent.
#[derive(Debug, Clone)]
pub struct GdprDeleteRequest {
    pub agent_id: String,
    pub tenant_id: String,
    pub requested_by: String,
}

/// Result of a GDPR delete operation.
#[derive(Debug, Clone)]
pub struct GdprDeleteResult {
    /// Topic names that were (or would be) deleted.
    pub topics_deleted: Vec<String>,
    /// Total number of records purged across all topics.
    pub records_purged: u64,
    /// Unix epoch seconds when the operation completed.
    pub completed_at: i64,
}

/// Maximum SLA for completing a GDPR delete (seconds).
pub const GDPR_DELETE_SLA_SECS: u64 = 3600;

/// All memory tiers that must be scanned for deletion.
const ALL_TIERS: &[Tier] = &[Tier::Episodic, Tier::Semantic, Tier::Procedural];

/// Placeholder implementation: computes the list of topics that would be
/// deleted for the given agent, without performing actual deletion.
///
/// A production implementation would iterate the topic log, delete records,
/// and compact the underlying storage.
pub fn delete_agent_data(request: &GdprDeleteRequest) -> Result<GdprDeleteResult, GdprDeleteError> {
    if request.agent_id.is_empty() {
        return Err(GdprDeleteError::InvalidRequest(
            "agent_id must not be empty".into(),
        ));
    }
    if request.tenant_id.is_empty() {
        return Err(GdprDeleteError::InvalidRequest(
            "tenant_id must not be empty".into(),
        ));
    }

    let topics: Vec<String> = ALL_TIERS
        .iter()
        .map(|tier| {
            format!(
                "__mem.{}.{}.{}",
                request.tenant_id,
                request.agent_id,
                tier.topic_suffix()
            )
        })
        .collect();

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;

    Ok(GdprDeleteResult {
        topics_deleted: topics,
        records_purged: 0, // placeholder — real impl counts purged records
        completed_at: now,
    })
}

/// Errors that can occur during a GDPR delete operation.
#[derive(Debug, thiserror::Error)]
pub enum GdprDeleteError {
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("storage error: {0}")]
    Storage(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req() -> GdprDeleteRequest {
        GdprDeleteRequest {
            agent_id: "agent-1".into(),
            tenant_id: "acme".into(),
            requested_by: "admin@acme.com".into(),
        }
    }

    #[test]
    fn delete_returns_all_tier_topics() {
        let result = delete_agent_data(&req()).unwrap();
        assert_eq!(result.topics_deleted.len(), 3);
        assert!(result
            .topics_deleted
            .contains(&"__mem.acme.agent-1.episodic".to_string()));
        assert!(result
            .topics_deleted
            .contains(&"__mem.acme.agent-1.semantic".to_string()));
        assert!(result
            .topics_deleted
            .contains(&"__mem.acme.agent-1.procedural".to_string()));
    }

    #[test]
    fn delete_sets_completed_at() {
        let result = delete_agent_data(&req()).unwrap();
        assert!(result.completed_at > 0);
    }

    #[test]
    fn delete_placeholder_purges_zero() {
        let result = delete_agent_data(&req()).unwrap();
        assert_eq!(result.records_purged, 0);
    }

    #[test]
    fn empty_agent_id_rejected() {
        let mut r = req();
        r.agent_id = String::new();
        assert!(delete_agent_data(&r).is_err());
    }

    #[test]
    fn empty_tenant_id_rejected() {
        let mut r = req();
        r.tenant_id = String::new();
        assert!(delete_agent_data(&r).is_err());
    }

    #[test]
    fn sla_constant_is_one_hour() {
        assert_eq!(GDPR_DELETE_SLA_SECS, 3600);
    }
}
