//! Multi-Tenancy Framework
//!
//! Provides comprehensive multi-tenancy support including:
//! - Tenant isolation and management
//! - Resource quotas and limits
//! - Namespace isolation
//! - Usage tracking and billing
//! - Tenant-aware authentication

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub mod isolation;
pub mod namespaces;
pub mod quotas;
pub mod usage;

pub use isolation::{IsolationLevel, NamespaceManager};
pub use quotas::{QuotaEnforcer, ResourceQuota, ResourceType};
pub use usage::{BillingPeriod, UsageMetric, UsageRecord, UsageTracker};

/// Tenant status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TenantStatus {
    /// Tenant is active
    #[default]
    Active,
    /// Tenant is suspended (quota exceeded, payment issues, etc.)
    Suspended,
    /// Tenant is being provisioned
    Provisioning,
    /// Tenant is being decommissioned
    Decommissioning,
    /// Tenant is archived
    Archived,
}

/// Tenant tier for resource allocation
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TenantTier {
    /// Free tier with limited resources
    Free,
    /// Basic tier
    #[default]
    Basic,
    /// Professional tier
    Professional,
    /// Enterprise tier
    Enterprise,
    /// Custom tier
    Custom,
}

impl TenantTier {
    /// Get default quotas for this tier
    pub fn default_quotas(&self) -> ResourceQuota {
        match self {
            TenantTier::Free => ResourceQuota {
                max_topics: Some(5),
                max_partitions_per_topic: Some(3),
                max_storage_bytes: Some(100 * 1024 * 1024), // 100MB
                max_message_rate: Some(100),                // 100 msg/s
                max_bandwidth_bytes: Some(1024 * 1024),     // 1MB/s
                max_connections: Some(10),
                max_consumer_groups: Some(5),
                retention_days: Some(1),
            },
            TenantTier::Basic => ResourceQuota {
                max_topics: Some(50),
                max_partitions_per_topic: Some(10),
                max_storage_bytes: Some(10 * 1024 * 1024 * 1024), // 10GB
                max_message_rate: Some(1000),
                max_bandwidth_bytes: Some(10 * 1024 * 1024), // 10MB/s
                max_connections: Some(100),
                max_consumer_groups: Some(50),
                retention_days: Some(7),
            },
            TenantTier::Professional => ResourceQuota {
                max_topics: Some(500),
                max_partitions_per_topic: Some(50),
                max_storage_bytes: Some(100 * 1024 * 1024 * 1024), // 100GB
                max_message_rate: Some(10000),
                max_bandwidth_bytes: Some(100 * 1024 * 1024), // 100MB/s
                max_connections: Some(1000),
                max_consumer_groups: Some(500),
                retention_days: Some(30),
            },
            TenantTier::Enterprise => ResourceQuota {
                max_topics: None, // Unlimited
                max_partitions_per_topic: None,
                max_storage_bytes: None,
                max_message_rate: None,
                max_bandwidth_bytes: None,
                max_connections: None,
                max_consumer_groups: None,
                retention_days: Some(365),
            },
            TenantTier::Custom => ResourceQuota::default(),
        }
    }
}

/// Tenant configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantConfig {
    /// Tenant tier
    pub tier: TenantTier,
    /// Custom quotas (overrides tier defaults)
    pub custom_quotas: Option<ResourceQuota>,
    /// Isolation level
    pub isolation_level: IsolationLevel,
    /// Allowed namespaces
    pub namespaces: Vec<String>,
    /// Admin users
    pub admins: Vec<String>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl Default for TenantConfig {
    fn default() -> Self {
        Self {
            tier: TenantTier::Basic,
            custom_quotas: None,
            isolation_level: IsolationLevel::Namespace,
            namespaces: vec!["default".to_string()],
            admins: Vec::new(),
            metadata: HashMap::new(),
        }
    }
}

/// Tenant information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    /// Unique tenant ID
    pub id: String,
    /// Display name
    pub name: String,
    /// Description
    pub description: Option<String>,
    /// Status
    pub status: TenantStatus,
    /// Configuration
    pub config: TenantConfig,
    /// Creation timestamp
    pub created_at: i64,
    /// Last modified timestamp
    pub updated_at: i64,
    /// Contact email
    pub contact_email: Option<String>,
    /// Tags for organization
    pub tags: Vec<String>,
}

impl Tenant {
    /// Create a new tenant
    pub fn new(id: impl Into<String>, name: impl Into<String>) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            id: id.into(),
            name: name.into(),
            description: None,
            status: TenantStatus::Active,
            config: TenantConfig::default(),
            created_at: now,
            updated_at: now,
            contact_email: None,
            tags: Vec::new(),
        }
    }

    /// Set tier
    pub fn with_tier(mut self, tier: TenantTier) -> Self {
        self.config.tier = tier;
        self
    }

    /// Set description
    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Set isolation level
    pub fn with_isolation(mut self, level: IsolationLevel) -> Self {
        self.config.isolation_level = level;
        self
    }

    /// Add namespace
    pub fn with_namespace(mut self, ns: impl Into<String>) -> Self {
        self.config.namespaces.push(ns.into());
        self
    }

    /// Add admin
    pub fn with_admin(mut self, admin: impl Into<String>) -> Self {
        self.config.admins.push(admin.into());
        self
    }

    /// Get effective quotas (custom or tier default)
    pub fn effective_quotas(&self) -> ResourceQuota {
        self.config
            .custom_quotas
            .clone()
            .unwrap_or_else(|| self.config.tier.default_quotas())
    }

    /// Is tenant active?
    pub fn is_active(&self) -> bool {
        self.status == TenantStatus::Active
    }

    /// Suspend tenant
    pub fn suspend(&mut self, reason: &str) {
        self.status = TenantStatus::Suspended;
        self.config
            .metadata
            .insert("suspend_reason".to_string(), reason.to_string());
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    /// Activate tenant
    pub fn activate(&mut self) {
        self.status = TenantStatus::Active;
        self.config.metadata.remove("suspend_reason");
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }
}

/// Tenant manager configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantManagerConfig {
    /// Enable tenant isolation
    pub isolation_enabled: bool,
    /// Default tenant tier
    pub default_tier: TenantTier,
    /// Default isolation level
    pub default_isolation: IsolationLevel,
    /// Enable usage tracking
    pub usage_tracking_enabled: bool,
    /// Enable quota enforcement
    pub quota_enforcement_enabled: bool,
    /// Maximum tenants
    pub max_tenants: Option<usize>,
}

impl Default for TenantManagerConfig {
    fn default() -> Self {
        Self {
            isolation_enabled: true,
            default_tier: TenantTier::Basic,
            default_isolation: IsolationLevel::Namespace,
            usage_tracking_enabled: true,
            quota_enforcement_enabled: true,
            max_tenants: None,
        }
    }
}

/// Tenant manager statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TenantManagerStats {
    /// Total tenants
    pub total_tenants: usize,
    /// Active tenants
    pub active_tenants: usize,
    /// Suspended tenants
    pub suspended_tenants: usize,
    /// Tenants by tier
    pub tenants_by_tier: HashMap<String, usize>,
    /// Total namespaces
    pub total_namespaces: usize,
    /// Quota violations
    pub quota_violations: u64,
}

/// Multi-tenancy manager
pub struct TenantManager {
    config: TenantManagerConfig,
    tenants: Arc<RwLock<HashMap<String, Tenant>>>,
    namespace_manager: Arc<RwLock<NamespaceManager>>,
    quota_enforcer: Arc<RwLock<QuotaEnforcer>>,
    usage_tracker: Arc<RwLock<UsageTracker>>,
    stats: Arc<RwLock<TenantManagerStats>>,
}

impl TenantManager {
    /// Create a new tenant manager
    pub fn new(config: TenantManagerConfig) -> Self {
        Self {
            config,
            tenants: Arc::new(RwLock::new(HashMap::new())),
            namespace_manager: Arc::new(RwLock::new(NamespaceManager::new())),
            quota_enforcer: Arc::new(RwLock::new(QuotaEnforcer::new())),
            usage_tracker: Arc::new(RwLock::new(UsageTracker::new())),
            stats: Arc::new(RwLock::new(TenantManagerStats::default())),
        }
    }

    /// Get configuration
    pub fn config(&self) -> &TenantManagerConfig {
        &self.config
    }

    /// Create a new tenant
    pub async fn create_tenant(&self, tenant: Tenant) -> Result<(), TenantError> {
        // Check max tenants limit
        if let Some(max) = self.config.max_tenants {
            let tenants = self.tenants.read().await;
            if tenants.len() >= max {
                return Err(TenantError::LimitExceeded("Maximum tenants reached".into()));
            }
        }

        let mut tenants = self.tenants.write().await;

        if tenants.contains_key(&tenant.id) {
            return Err(TenantError::AlreadyExists(tenant.id.clone()));
        }

        let tenant_id = tenant.id.clone();
        let namespaces = tenant.config.namespaces.clone();
        let quotas = tenant.effective_quotas();

        tenants.insert(tenant_id.clone(), tenant);
        drop(tenants);

        // Setup namespaces
        {
            let mut ns_manager = self.namespace_manager.write().await;
            for ns in namespaces {
                ns_manager.create_namespace(&tenant_id, &ns)?;
            }
        }

        // Setup quotas
        {
            let mut enforcer = self.quota_enforcer.write().await;
            enforcer.set_quotas(&tenant_id, quotas);
        }

        // Initialize usage tracking
        {
            let mut tracker = self.usage_tracker.write().await;
            tracker.init_tenant(&tenant_id);
        }

        // Update stats
        self.update_stats().await;

        Ok(())
    }

    /// Get a tenant by ID
    pub async fn get_tenant(&self, id: &str) -> Option<Tenant> {
        let tenants = self.tenants.read().await;
        tenants.get(id).cloned()
    }

    /// List all tenants
    pub async fn list_tenants(&self) -> Vec<Tenant> {
        let tenants = self.tenants.read().await;
        tenants.values().cloned().collect()
    }

    /// Update a tenant
    pub async fn update_tenant(&self, id: &str, update: TenantUpdate) -> Result<(), TenantError> {
        let mut tenants = self.tenants.write().await;

        let tenant = tenants
            .get_mut(id)
            .ok_or_else(|| TenantError::NotFound(id.to_string()))?;

        if let Some(name) = update.name {
            tenant.name = name;
        }
        if let Some(desc) = update.description {
            tenant.description = Some(desc);
        }
        if let Some(tier) = update.tier {
            tenant.config.tier = tier;
        }
        if let Some(status) = update.status {
            tenant.status = status;
        }
        if let Some(isolation) = update.isolation_level {
            tenant.config.isolation_level = isolation;
        }
        if let Some(quotas) = update.custom_quotas {
            tenant.config.custom_quotas = Some(quotas.clone());

            // Update quota enforcer
            let mut enforcer = self.quota_enforcer.write().await;
            enforcer.set_quotas(id, quotas);
        }

        tenant.updated_at = chrono::Utc::now().timestamp_millis();

        // Update stats
        drop(tenants);
        self.update_stats().await;

        Ok(())
    }

    /// Delete a tenant
    pub async fn delete_tenant(&self, id: &str) -> Result<(), TenantError> {
        let mut tenants = self.tenants.write().await;

        if !tenants.contains_key(id) {
            return Err(TenantError::NotFound(id.to_string()));
        }

        tenants.remove(id);
        drop(tenants);

        // Cleanup namespaces
        {
            let mut ns_manager = self.namespace_manager.write().await;
            ns_manager.delete_tenant_namespaces(id);
        }

        // Cleanup quotas
        {
            let mut enforcer = self.quota_enforcer.write().await;
            enforcer.remove_quotas(id);
        }

        // Cleanup usage
        {
            let mut tracker = self.usage_tracker.write().await;
            tracker.remove_tenant(id);
        }

        // Update stats
        self.update_stats().await;

        Ok(())
    }

    /// Suspend a tenant
    pub async fn suspend_tenant(&self, id: &str, reason: &str) -> Result<(), TenantError> {
        let mut tenants = self.tenants.write().await;
        let tenant = tenants
            .get_mut(id)
            .ok_or_else(|| TenantError::NotFound(id.to_string()))?;
        tenant.suspend(reason);

        drop(tenants);
        self.update_stats().await;
        Ok(())
    }

    /// Activate a tenant
    pub async fn activate_tenant(&self, id: &str) -> Result<(), TenantError> {
        let mut tenants = self.tenants.write().await;
        let tenant = tenants
            .get_mut(id)
            .ok_or_else(|| TenantError::NotFound(id.to_string()))?;
        tenant.activate();

        drop(tenants);
        self.update_stats().await;
        Ok(())
    }

    /// Check if a tenant can perform an operation
    pub async fn check_quota(
        &self,
        tenant_id: &str,
        resource: ResourceType,
        amount: u64,
    ) -> Result<(), TenantError> {
        if !self.config.quota_enforcement_enabled {
            return Ok(());
        }

        // Check if tenant exists and is active
        let tenants = self.tenants.read().await;
        let tenant = tenants
            .get(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        if !tenant.is_active() {
            return Err(TenantError::Suspended(tenant_id.to_string()));
        }
        drop(tenants);

        // Check quota
        let enforcer = self.quota_enforcer.read().await;
        enforcer
            .check_quota(tenant_id, resource, amount)
            .map_err(|e| {
                // Update stats
                let stats = self.stats.clone();
                tokio::spawn(async move {
                    let mut s = stats.write().await;
                    s.quota_violations += 1;
                });
                e
            })
    }

    /// Record resource usage
    pub async fn record_usage(&self, tenant_id: &str, metric: UsageMetric, amount: u64) {
        if !self.config.usage_tracking_enabled {
            return;
        }

        let mut tracker = self.usage_tracker.write().await;
        tracker.record(tenant_id, metric, amount);
    }

    /// Get usage for a tenant
    pub async fn get_usage(
        &self,
        tenant_id: &str,
        period: BillingPeriod,
    ) -> Option<Vec<UsageRecord>> {
        let tracker = self.usage_tracker.read().await;
        tracker.get_usage(tenant_id, period)
    }

    /// Get manager statistics
    pub async fn stats(&self) -> TenantManagerStats {
        let stats = self.stats.read().await;
        stats.clone()
    }

    /// Update statistics
    async fn update_stats(&self) {
        let tenants = self.tenants.read().await;
        let ns_manager = self.namespace_manager.read().await;

        let mut stats = self.stats.write().await;
        stats.total_tenants = tenants.len();
        stats.active_tenants = tenants.values().filter(|t| t.is_active()).count();
        stats.suspended_tenants = tenants
            .values()
            .filter(|t| t.status == TenantStatus::Suspended)
            .count();
        stats.total_namespaces = ns_manager.namespace_count();

        // Count by tier
        stats.tenants_by_tier.clear();
        for tenant in tenants.values() {
            let tier_name = format!("{:?}", tenant.config.tier);
            *stats.tenants_by_tier.entry(tier_name).or_default() += 1;
        }
    }

    /// Validate access to a resource
    pub async fn validate_access(
        &self,
        tenant_id: &str,
        namespace: &str,
        resource: &str,
    ) -> Result<(), TenantError> {
        // Check tenant exists and is active
        let tenants = self.tenants.read().await;
        let tenant = tenants
            .get(tenant_id)
            .ok_or_else(|| TenantError::NotFound(tenant_id.to_string()))?;

        if !tenant.is_active() {
            return Err(TenantError::Suspended(tenant_id.to_string()));
        }

        // Check namespace access
        if !tenant.config.namespaces.contains(&namespace.to_string()) {
            return Err(TenantError::AccessDenied(format!(
                "Tenant {} does not have access to namespace {}",
                tenant_id, namespace
            )));
        }

        // Check isolation
        let ns_manager = self.namespace_manager.read().await;
        if !ns_manager.can_access(tenant_id, namespace, resource) {
            return Err(TenantError::AccessDenied(format!(
                "Access denied to resource {} in namespace {}",
                resource, namespace
            )));
        }

        Ok(())
    }
}

impl Default for TenantManager {
    fn default() -> Self {
        Self::new(TenantManagerConfig::default())
    }
}

/// Tenant update request
#[derive(Debug, Clone, Default)]
pub struct TenantUpdate {
    /// New name
    pub name: Option<String>,
    /// New description
    pub description: Option<String>,
    /// New tier
    pub tier: Option<TenantTier>,
    /// New status
    pub status: Option<TenantStatus>,
    /// New isolation level
    pub isolation_level: Option<IsolationLevel>,
    /// New custom quotas
    pub custom_quotas: Option<ResourceQuota>,
}

impl TenantUpdate {
    /// Create a new update builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set name
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set tier
    pub fn tier(mut self, tier: TenantTier) -> Self {
        self.tier = Some(tier);
        self
    }

    /// Set status
    pub fn status(mut self, status: TenantStatus) -> Self {
        self.status = Some(status);
        self
    }

    /// Set quotas
    pub fn quotas(mut self, quotas: ResourceQuota) -> Self {
        self.custom_quotas = Some(quotas);
        self
    }
}

/// Tenant error
#[derive(Debug, Clone, thiserror::Error)]
pub enum TenantError {
    /// Tenant not found
    #[error("Tenant not found: {0}")]
    NotFound(String),
    /// Tenant already exists
    #[error("Tenant already exists: {0}")]
    AlreadyExists(String),
    /// Tenant is suspended
    #[error("Tenant is suspended: {0}")]
    Suspended(String),
    /// Access denied
    #[error("Access denied: {0}")]
    AccessDenied(String),
    /// Quota exceeded
    #[error("Quota exceeded: {0}")]
    QuotaExceeded(String),
    /// Limit exceeded
    #[error("Limit exceeded: {0}")]
    LimitExceeded(String),
    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
    /// Namespace error
    #[error("Namespace error: {0}")]
    NamespaceError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tenant_tier_quotas() {
        let free_quotas = TenantTier::Free.default_quotas();
        assert_eq!(free_quotas.max_topics, Some(5));

        let enterprise_quotas = TenantTier::Enterprise.default_quotas();
        assert_eq!(enterprise_quotas.max_topics, None); // Unlimited
    }

    #[test]
    fn test_tenant_creation() {
        let tenant = Tenant::new("test-tenant", "Test Tenant")
            .with_tier(TenantTier::Professional)
            .with_description("A test tenant");

        assert_eq!(tenant.id, "test-tenant");
        assert_eq!(tenant.name, "Test Tenant");
        assert_eq!(tenant.config.tier, TenantTier::Professional);
        assert!(tenant.is_active());
    }

    #[test]
    fn test_tenant_suspend_activate() {
        let mut tenant = Tenant::new("test", "Test");

        tenant.suspend("Payment overdue");
        assert_eq!(tenant.status, TenantStatus::Suspended);
        assert!(!tenant.is_active());

        tenant.activate();
        assert_eq!(tenant.status, TenantStatus::Active);
        assert!(tenant.is_active());
    }

    #[test]
    fn test_tenant_effective_quotas() {
        let tenant = Tenant::new("test", "Test").with_tier(TenantTier::Basic);
        let quotas = tenant.effective_quotas();
        assert_eq!(quotas.max_topics, Some(50));

        // With custom quotas
        let mut tenant2 = Tenant::new("test2", "Test 2");
        tenant2.config.custom_quotas = Some(ResourceQuota {
            max_topics: Some(100),
            ..Default::default()
        });
        let quotas2 = tenant2.effective_quotas();
        assert_eq!(quotas2.max_topics, Some(100));
    }

    #[tokio::test]
    async fn test_tenant_manager_create() {
        let manager = TenantManager::new(TenantManagerConfig::default());

        let tenant = Tenant::new("tenant1", "Tenant 1");
        manager.create_tenant(tenant).await.unwrap();

        let retrieved = manager.get_tenant("tenant1").await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().name, "Tenant 1");
    }

    #[tokio::test]
    async fn test_tenant_manager_list() {
        let manager = TenantManager::new(TenantManagerConfig::default());

        manager
            .create_tenant(Tenant::new("t1", "Tenant 1"))
            .await
            .unwrap();
        manager
            .create_tenant(Tenant::new("t2", "Tenant 2"))
            .await
            .unwrap();

        let tenants = manager.list_tenants().await;
        assert_eq!(tenants.len(), 2);
    }

    #[tokio::test]
    async fn test_tenant_manager_update() {
        let manager = TenantManager::new(TenantManagerConfig::default());

        manager
            .create_tenant(Tenant::new("t1", "Tenant 1"))
            .await
            .unwrap();

        let update = TenantUpdate::new()
            .name("Updated Tenant 1")
            .tier(TenantTier::Professional);

        manager.update_tenant("t1", update).await.unwrap();

        let tenant = manager.get_tenant("t1").await.unwrap();
        assert_eq!(tenant.name, "Updated Tenant 1");
        assert_eq!(tenant.config.tier, TenantTier::Professional);
    }

    #[tokio::test]
    async fn test_tenant_manager_delete() {
        let manager = TenantManager::new(TenantManagerConfig::default());

        manager
            .create_tenant(Tenant::new("t1", "Tenant 1"))
            .await
            .unwrap();
        assert!(manager.get_tenant("t1").await.is_some());

        manager.delete_tenant("t1").await.unwrap();
        assert!(manager.get_tenant("t1").await.is_none());
    }

    #[tokio::test]
    async fn test_tenant_manager_suspend() {
        let manager = TenantManager::new(TenantManagerConfig::default());

        manager
            .create_tenant(Tenant::new("t1", "Tenant 1"))
            .await
            .unwrap();
        manager
            .suspend_tenant("t1", "Test suspension")
            .await
            .unwrap();

        let tenant = manager.get_tenant("t1").await.unwrap();
        assert_eq!(tenant.status, TenantStatus::Suspended);
    }

    #[tokio::test]
    async fn test_tenant_manager_stats() {
        let manager = TenantManager::new(TenantManagerConfig::default());

        manager
            .create_tenant(Tenant::new("t1", "Tenant 1").with_tier(TenantTier::Basic))
            .await
            .unwrap();
        manager
            .create_tenant(Tenant::new("t2", "Tenant 2").with_tier(TenantTier::Professional))
            .await
            .unwrap();

        let stats = manager.stats().await;
        assert_eq!(stats.total_tenants, 2);
        assert_eq!(stats.active_tenants, 2);
    }

    #[tokio::test]
    async fn test_tenant_duplicate_error() {
        let manager = TenantManager::new(TenantManagerConfig::default());

        manager
            .create_tenant(Tenant::new("t1", "Tenant 1"))
            .await
            .unwrap();

        let result = manager.create_tenant(Tenant::new("t1", "Duplicate")).await;
        assert!(matches!(result, Err(TenantError::AlreadyExists(_))));
    }

    #[tokio::test]
    async fn test_tenant_not_found_error() {
        let manager = TenantManager::new(TenantManagerConfig::default());

        let result = manager.get_tenant("nonexistent").await;
        assert!(result.is_none());

        let result = manager
            .update_tenant("nonexistent", TenantUpdate::new())
            .await;
        assert!(matches!(result, Err(TenantError::NotFound(_))));
    }
}
