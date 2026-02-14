//! Multi-tenant management for Streamline Cloud
//!
//! Provides tenant lifecycle management, quota enforcement,
//! and prefix-based topic isolation for the managed cloud offering.
//!
//! # Stability: Experimental

use crate::error::{Result, StreamlineError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Billing plan tiers for managed cloud tenants
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum CloudPlan {
    /// Free tier — limited resources, ideal for experimentation
    #[default]
    Free,
    /// Starter tier — small workloads with basic support
    Starter,
    /// Pro tier — production workloads with priority support
    Pro,
    /// Enterprise tier — dedicated resources, SLA guarantees, custom limits
    Enterprise,
}

impl std::fmt::Display for CloudPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CloudPlan::Free => write!(f, "free"),
            CloudPlan::Starter => write!(f, "starter"),
            CloudPlan::Pro => write!(f, "pro"),
            CloudPlan::Enterprise => write!(f, "enterprise"),
        }
    }
}

/// Per-tenant resource quotas
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantQuota {
    /// Maximum number of topics
    pub max_topics: u32,
    /// Maximum total partitions across all topics
    pub max_partitions: u32,
    /// Maximum throughput in bytes per second (ingress + egress)
    pub max_throughput_bytes: u64,
    /// Maximum data retention period in hours
    pub retention_limit_hours: u64,
}

impl TenantQuota {
    /// Default quotas for a given plan
    pub fn for_plan(plan: &CloudPlan) -> Self {
        match plan {
            CloudPlan::Free => Self {
                max_topics: 5,
                max_partitions: 10,
                max_throughput_bytes: 1_048_576, // 1 MB/s
                retention_limit_hours: 24,
            },
            CloudPlan::Starter => Self {
                max_topics: 25,
                max_partitions: 100,
                max_throughput_bytes: 10_485_760, // 10 MB/s
                retention_limit_hours: 168,       // 7 days
            },
            CloudPlan::Pro => Self {
                max_topics: 100,
                max_partitions: 500,
                max_throughput_bytes: 104_857_600, // 100 MB/s
                retention_limit_hours: 720,        // 30 days
            },
            CloudPlan::Enterprise => Self {
                max_topics: 1000,
                max_partitions: 5000,
                max_throughput_bytes: 1_073_741_824, // 1 GB/s
                retention_limit_hours: 8760,         // 365 days
            },
        }
    }
}

/// A managed cloud tenant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudTenant {
    /// Unique tenant identifier
    pub id: String,
    /// Human-readable tenant name
    pub name: String,
    /// Billing plan
    pub plan: CloudPlan,
    /// Resource limits enforced for this tenant
    pub resource_limits: TenantQuota,
    /// Timestamp when the tenant was created
    pub created_at: DateTime<Utc>,
}

/// Enforces prefix-based topic isolation between tenants.
///
/// Each tenant's topics are namespaced under `{tenant_id}.` to prevent
/// cross-tenant access.
pub struct TenantIsolation;

impl TenantIsolation {
    /// Returns the namespaced topic name for a tenant.
    pub fn namespaced_topic(tenant_id: &str, topic: &str) -> String {
        format!("{}.{}", tenant_id, topic)
    }

    /// Validates that a tenant is allowed to access the given topic.
    ///
    /// A tenant may only access topics prefixed with its own ID.
    pub fn validate_access(tenant_id: &str, topic: &str) -> Result<()> {
        let prefix = format!("{}.", tenant_id);
        if topic.starts_with(&prefix) {
            Ok(())
        } else {
            Err(StreamlineError::Config(format!(
                "Tenant '{}' is not allowed to access topic '{}'",
                tenant_id, topic
            )))
        }
    }
}

/// Manages the lifecycle of managed cloud tenants
pub struct CloudTenantManager {
    tenants: Arc<RwLock<HashMap<String, CloudTenant>>>,
}

impl CloudTenantManager {
    /// Create a new tenant manager
    pub fn new() -> Self {
        Self {
            tenants: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a new managed cloud tenant
    pub async fn create_tenant(
        &self,
        name: &str,
        plan: CloudPlan,
    ) -> Result<CloudTenant> {
        let id = uuid::Uuid::new_v4().to_string();
        let resource_limits = TenantQuota::for_plan(&plan);

        let tenant = CloudTenant {
            id: id.clone(),
            name: name.to_string(),
            plan,
            resource_limits,
            created_at: Utc::now(),
        };

        let mut tenants = self.tenants.write().await;
        tenants.insert(id, tenant.clone());

        tracing::info!(tenant_id = %tenant.id, name = %tenant.name, plan = %tenant.plan, "Cloud tenant created");
        Ok(tenant)
    }

    /// Retrieve a tenant by ID
    pub async fn get_tenant(&self, tenant_id: &str) -> Option<CloudTenant> {
        let tenants = self.tenants.read().await;
        tenants.get(tenant_id).cloned()
    }

    /// List all tenants
    pub async fn list_tenants(&self) -> Vec<CloudTenant> {
        let tenants = self.tenants.read().await;
        tenants.values().cloned().collect()
    }

    /// Update the quota for an existing tenant
    pub async fn update_quota(
        &self,
        tenant_id: &str,
        quota: TenantQuota,
    ) -> Result<CloudTenant> {
        let mut tenants = self.tenants.write().await;
        let tenant = tenants.get_mut(tenant_id).ok_or_else(|| {
            StreamlineError::Config(format!("Tenant not found: {}", tenant_id))
        })?;

        tenant.resource_limits = quota;
        tracing::info!(tenant_id = %tenant_id, "Tenant quota updated");
        Ok(tenant.clone())
    }

    /// Validate that a tenant has access to a topic (prefix-based isolation)
    pub async fn validate_tenant_access(
        &self,
        tenant_id: &str,
        topic: &str,
    ) -> Result<()> {
        // Ensure the tenant exists
        let tenants = self.tenants.read().await;
        if !tenants.contains_key(tenant_id) {
            return Err(StreamlineError::Config(format!(
                "Tenant not found: {}",
                tenant_id
            )));
        }
        drop(tenants);

        TenantIsolation::validate_access(tenant_id, topic)
    }
}

impl Default for CloudTenantManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_for_plan() {
        let free = TenantQuota::for_plan(&CloudPlan::Free);
        assert_eq!(free.max_topics, 5);

        let enterprise = TenantQuota::for_plan(&CloudPlan::Enterprise);
        assert_eq!(enterprise.max_topics, 1000);
    }

    #[test]
    fn test_cloud_plan_display() {
        assert_eq!(CloudPlan::Free.to_string(), "free");
        assert_eq!(CloudPlan::Pro.to_string(), "pro");
    }

    #[test]
    fn test_tenant_isolation_namespaced_topic() {
        let topic = TenantIsolation::namespaced_topic("tenant-abc", "orders");
        assert_eq!(topic, "tenant-abc.orders");
    }

    #[test]
    fn test_tenant_isolation_validate_access() {
        assert!(TenantIsolation::validate_access("t1", "t1.orders").is_ok());
        assert!(TenantIsolation::validate_access("t1", "t2.orders").is_err());
        assert!(TenantIsolation::validate_access("t1", "orders").is_err());
    }

    #[tokio::test]
    async fn test_create_and_get_tenant() {
        let mgr = CloudTenantManager::new();
        let tenant = mgr.create_tenant("Acme Corp", CloudPlan::Pro).await.unwrap();

        assert_eq!(tenant.name, "Acme Corp");
        assert_eq!(tenant.plan, CloudPlan::Pro);
        assert_eq!(tenant.resource_limits.max_topics, 100);

        let fetched = mgr.get_tenant(&tenant.id).await.unwrap();
        assert_eq!(fetched.id, tenant.id);
    }

    #[tokio::test]
    async fn test_list_tenants() {
        let mgr = CloudTenantManager::new();
        mgr.create_tenant("A", CloudPlan::Free).await.unwrap();
        mgr.create_tenant("B", CloudPlan::Starter).await.unwrap();

        let list = mgr.list_tenants().await;
        assert_eq!(list.len(), 2);
    }

    #[tokio::test]
    async fn test_update_quota() {
        let mgr = CloudTenantManager::new();
        let tenant = mgr.create_tenant("Test", CloudPlan::Free).await.unwrap();
        assert_eq!(tenant.resource_limits.max_topics, 5);

        let updated = mgr
            .update_quota(
                &tenant.id,
                TenantQuota {
                    max_topics: 50,
                    max_partitions: 200,
                    max_throughput_bytes: 50_000_000,
                    retention_limit_hours: 72,
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.resource_limits.max_topics, 50);
    }

    #[tokio::test]
    async fn test_validate_tenant_access() {
        let mgr = CloudTenantManager::new();
        let tenant = mgr.create_tenant("Test", CloudPlan::Free).await.unwrap();

        let namespaced = TenantIsolation::namespaced_topic(&tenant.id, "events");
        assert!(mgr.validate_tenant_access(&tenant.id, &namespaced).await.is_ok());
        assert!(mgr.validate_tenant_access(&tenant.id, "other.events").await.is_err());
        assert!(mgr.validate_tenant_access("nonexistent", "x.y").await.is_err());
    }
}
