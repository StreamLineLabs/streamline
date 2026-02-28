//! Comprehensive audit event types for SOC2 compliance.
//!
//! Covers all administrative operations that must be logged for audit trails.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

/// Categories of audit events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AuditCategory {
    Authentication,
    Authorization,
    TopicManagement,
    UserManagement,
    AclManagement,
    SchemaManagement,
    ClusterManagement,
    ConfigManagement,
    DataAccess,
    ConnectorManagement,
}

/// Outcome of an audited operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AuditOutcome {
    Success,
    Failure,
    Denied,
}

/// A comprehensive audit event record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Unique event ID
    pub id: String,
    /// ISO 8601 timestamp
    pub timestamp: DateTime<Utc>,
    /// Event category
    pub category: AuditCategory,
    /// Specific action (e.g., "TOPIC_CREATE", "USER_DELETE")
    pub action: String,
    /// Outcome of the operation
    pub outcome: AuditOutcome,
    /// Principal who performed the action
    pub principal: Option<String>,
    /// Client IP address
    pub client_ip: Option<IpAddr>,
    /// Resource type affected (e.g., "topic", "user", "acl")
    pub resource_type: Option<String>,
    /// Resource name affected
    pub resource_name: Option<String>,
    /// Additional details as key-value pairs
    pub details: std::collections::HashMap<String, String>,
}

impl AuditEvent {
    /// Create a new audit event with the current timestamp.
    pub fn new(category: AuditCategory, action: impl Into<String>, outcome: AuditOutcome) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            timestamp: Utc::now(),
            category,
            action: action.into(),
            outcome,
            principal: None,
            client_ip: None,
            resource_type: None,
            resource_name: None,
            details: std::collections::HashMap::new(),
        }
    }

    pub fn with_principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    pub fn with_client_ip(mut self, ip: IpAddr) -> Self {
        self.client_ip = Some(ip);
        self
    }

    pub fn with_resource(mut self, resource_type: impl Into<String>, resource_name: impl Into<String>) -> Self {
        self.resource_type = Some(resource_type.into());
        self.resource_name = Some(resource_name.into());
        self
    }

    pub fn with_detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.details.insert(key.into(), value.into());
        self
    }
}

// Pre-defined action constants for type safety
pub mod actions {
    // Authentication
    pub const AUTH_LOGIN: &str = "AUTH_LOGIN";
    pub const AUTH_LOGOUT: &str = "AUTH_LOGOUT";
    pub const AUTH_TOKEN_REFRESH: &str = "AUTH_TOKEN_REFRESH";
    pub const AUTH_TOKEN_CREATE: &str = "AUTH_TOKEN_CREATE";
    pub const AUTH_TOKEN_REVOKE: &str = "AUTH_TOKEN_REVOKE";

    // Topics
    pub const TOPIC_CREATE: &str = "TOPIC_CREATE";
    pub const TOPIC_DELETE: &str = "TOPIC_DELETE";
    pub const TOPIC_ALTER: &str = "TOPIC_ALTER";
    pub const TOPIC_DESCRIBE: &str = "TOPIC_DESCRIBE";

    // Users
    pub const USER_CREATE: &str = "USER_CREATE";
    pub const USER_DELETE: &str = "USER_DELETE";
    pub const USER_UPDATE: &str = "USER_UPDATE";
    pub const USER_PASSWORD_CHANGE: &str = "USER_PASSWORD_CHANGE";

    // ACLs
    pub const ACL_CREATE: &str = "ACL_CREATE";
    pub const ACL_DELETE: &str = "ACL_DELETE";

    // Schema
    pub const SCHEMA_REGISTER: &str = "SCHEMA_REGISTER";
    pub const SCHEMA_DELETE: &str = "SCHEMA_DELETE";
    pub const SCHEMA_COMPAT_CHECK: &str = "SCHEMA_COMPAT_CHECK";

    // Cluster
    pub const CLUSTER_NODE_JOIN: &str = "CLUSTER_NODE_JOIN";
    pub const CLUSTER_NODE_LEAVE: &str = "CLUSTER_NODE_LEAVE";
    pub const CLUSTER_LEADER_CHANGE: &str = "CLUSTER_LEADER_CHANGE";
    pub const CLUSTER_REBALANCE: &str = "CLUSTER_REBALANCE";

    // Config
    pub const CONFIG_UPDATE: &str = "CONFIG_UPDATE";
    pub const CONFIG_RESET: &str = "CONFIG_RESET";

    // Data Access
    pub const DATA_PRODUCE: &str = "DATA_PRODUCE";
    pub const DATA_CONSUME: &str = "DATA_CONSUME";
    pub const DATA_DELETE: &str = "DATA_DELETE";

    // Connectors
    pub const CONNECTOR_CREATE: &str = "CONNECTOR_CREATE";
    pub const CONNECTOR_DELETE: &str = "CONNECTOR_DELETE";
    pub const CONNECTOR_START: &str = "CONNECTOR_START";
    pub const CONNECTOR_STOP: &str = "CONNECTOR_STOP";
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_audit_event_new() {
        let event = AuditEvent::new(
            AuditCategory::Authentication,
            actions::AUTH_LOGIN,
            AuditOutcome::Success,
        );
        assert_eq!(event.category, AuditCategory::Authentication);
        assert_eq!(event.action, "AUTH_LOGIN");
        assert_eq!(event.outcome, AuditOutcome::Success);
        assert!(!event.id.is_empty());
        assert!(event.principal.is_none());
    }

    #[test]
    fn test_audit_event_builder() {
        let event = AuditEvent::new(
            AuditCategory::TopicManagement,
            actions::TOPIC_CREATE,
            AuditOutcome::Success,
        )
        .with_principal("alice")
        .with_client_ip(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)))
        .with_resource("topic", "orders")
        .with_detail("partitions", "3");

        assert_eq!(event.principal.as_deref(), Some("alice"));
        assert_eq!(event.resource_type.as_deref(), Some("topic"));
        assert_eq!(event.resource_name.as_deref(), Some("orders"));
        assert_eq!(event.details.get("partitions").map(|s| s.as_str()), Some("3"));
    }

    #[test]
    fn test_audit_event_serialization() {
        let event = AuditEvent::new(
            AuditCategory::Authorization,
            actions::ACL_CREATE,
            AuditOutcome::Denied,
        );
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("AUTHORIZATION"));
        assert!(json.contains("ACL_CREATE"));
        assert!(json.contains("DENIED"));
    }

    #[test]
    fn test_audit_event_deserialization_roundtrip() {
        let event = AuditEvent::new(
            AuditCategory::DataAccess,
            actions::DATA_PRODUCE,
            AuditOutcome::Success,
        )
        .with_principal("bob");

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: AuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.category, AuditCategory::DataAccess);
        assert_eq!(deserialized.action, "DATA_PRODUCE");
        assert_eq!(deserialized.principal.as_deref(), Some("bob"));
    }
}
