//! Tenant types and configuration

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Unique identifier for a tenant
pub type TenantId = String;

/// Tenant state
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TenantState {
    /// Tenant is active and can perform operations
    Active,
    /// Tenant is suspended (operations blocked)
    Suspended,
    /// Tenant is being provisioned
    Provisioning,
    /// Tenant is being deprovisioned
    Deprovisioning,
}

impl std::fmt::Display for TenantState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TenantState::Active => write!(f, "active"),
            TenantState::Suspended => write!(f, "suspended"),
            TenantState::Provisioning => write!(f, "provisioning"),
            TenantState::Deprovisioning => write!(f, "deprovisioning"),
        }
    }
}

/// Configuration for a tenant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantConfig {
    /// Maximum number of topics
    pub max_topics: u32,
    /// Maximum number of partitions across all topics
    pub max_partitions: u32,
    /// Maximum producer bytes per second
    pub max_producer_bytes_per_sec: u64,
    /// Maximum consumer bytes per second
    pub max_consumer_bytes_per_sec: u64,
    /// Maximum message size in bytes
    pub max_message_size: u32,
    /// Maximum retention in milliseconds
    pub max_retention_ms: i64,
    /// Maximum storage bytes
    pub max_storage_bytes: u64,
    /// Whether transactions are enabled
    pub transactions_enabled: bool,
    /// Custom metadata
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl Default for TenantConfig {
    fn default() -> Self {
        Self {
            max_topics: 100,
            max_partitions: 500,
            max_producer_bytes_per_sec: 50 * 1024 * 1024, // 50 MB/s
            max_consumer_bytes_per_sec: 100 * 1024 * 1024, // 100 MB/s
            max_message_size: 1024 * 1024,                 // 1 MB
            max_retention_ms: 7 * 24 * 60 * 60 * 1000,    // 7 days
            max_storage_bytes: 10 * 1024 * 1024 * 1024,   // 10 GB
            transactions_enabled: true,
            metadata: HashMap::new(),
        }
    }
}

/// Represents a tenant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    /// Unique tenant identifier
    pub id: TenantId,
    /// Display name
    pub name: String,
    /// Tenant state
    pub state: TenantState,
    /// Tenant configuration and quotas
    pub config: TenantConfig,
    /// Creation timestamp (unix ms)
    pub created_at_ms: i64,
    /// Last update timestamp (unix ms)
    pub updated_at_ms: i64,
}

impl Tenant {
    /// Create a new tenant
    pub fn new(id: impl Into<TenantId>, name: impl Into<String>, config: TenantConfig) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            id: id.into(),
            name: name.into(),
            state: TenantState::Active,
            config,
            created_at_ms: now,
            updated_at_ms: now,
        }
    }

    /// Check if the tenant is active
    pub fn is_active(&self) -> bool {
        self.state == TenantState::Active
    }

    /// Map an external topic name to the internal namespaced name
    pub fn namespace_topic(&self, topic: &str) -> String {
        format!("{}.{}", self.id, topic)
    }

    /// Map an external consumer group ID to the internal namespaced group
    pub fn namespace_group(&self, group_id: &str) -> String {
        format!("{}.{}", self.id, group_id)
    }

    /// Strip the namespace prefix from an internal topic name
    pub fn strip_namespace<'a>(&self, internal_topic: &'a str) -> Option<&'a str> {
        let prefix = format!("{}.", self.id);
        internal_topic.strip_prefix(&prefix)
    }

    /// Strip the namespace prefix from an internal consumer group ID
    pub fn strip_group_namespace<'a>(&self, internal_group: &'a str) -> Option<&'a str> {
        let prefix = format!("{}.", self.id);
        internal_group.strip_prefix(&prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_creation() {
        let tenant = Tenant::new("tenant-1", "Test Tenant", TenantConfig::default());
        assert_eq!(tenant.id, "tenant-1");
        assert_eq!(tenant.name, "Test Tenant");
        assert_eq!(tenant.state, TenantState::Active);
        assert!(tenant.is_active());
    }

    #[test]
    fn test_tenant_namespace_topic() {
        let tenant = Tenant::new("acme", "ACME Corp", TenantConfig::default());
        assert_eq!(tenant.namespace_topic("events"), "acme.events");
        assert_eq!(tenant.namespace_topic("logs"), "acme.logs");
    }

    #[test]
    fn test_tenant_strip_namespace() {
        let tenant = Tenant::new("acme", "ACME Corp", TenantConfig::default());
        assert_eq!(tenant.strip_namespace("acme.events"), Some("events"));
        assert_eq!(tenant.strip_namespace("other.events"), None);
    }

    #[test]
    fn test_tenant_state_display() {
        assert_eq!(TenantState::Active.to_string(), "active");
        assert_eq!(TenantState::Suspended.to_string(), "suspended");
        assert_eq!(TenantState::Provisioning.to_string(), "provisioning");
        assert_eq!(TenantState::Deprovisioning.to_string(), "deprovisioning");
    }

    #[test]
    fn test_tenant_config_defaults() {
        let config = TenantConfig::default();
        assert_eq!(config.max_topics, 100);
        assert_eq!(config.max_partitions, 500);
        assert!(config.transactions_enabled);
    }

    #[test]
    fn test_tenant_config_custom() {
        let config = TenantConfig {
            max_topics: 10,
            max_partitions: 50,
            max_producer_bytes_per_sec: 1024 * 1024,
            max_consumer_bytes_per_sec: 2 * 1024 * 1024,
            max_message_size: 512 * 1024,
            max_retention_ms: 3600000,
            max_storage_bytes: 1024 * 1024 * 1024,
            transactions_enabled: false,
            metadata: HashMap::from([("tier".to_string(), "free".to_string())]),
        };
        assert_eq!(config.max_topics, 10);
        assert!(!config.transactions_enabled);
        assert_eq!(config.metadata.get("tier").unwrap(), "free");
    }

    #[test]
    fn test_tenant_serialization_roundtrip() {
        let tenant = Tenant::new("test", "Test", TenantConfig::default());
        let json = serde_json::to_string(&tenant).unwrap();
        let parsed: Tenant = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, "test");
        assert_eq!(parsed.state, TenantState::Active);
    }

    #[test]
    fn test_tenant_not_active_when_suspended() {
        let mut tenant = Tenant::new("test", "Test", TenantConfig::default());
        tenant.state = TenantState::Suspended;
        assert!(!tenant.is_active());
    }
}
