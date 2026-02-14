//! Resource Quotas
//!
//! Provides quota management and enforcement for multi-tenant deployments.

use super::TenantError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Resource type for quota tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ResourceType {
    /// Number of topics
    Topics,
    /// Number of partitions per topic
    PartitionsPerTopic,
    /// Total storage in bytes
    Storage,
    /// Messages per second
    MessageRate,
    /// Bandwidth in bytes per second
    Bandwidth,
    /// Number of connections
    Connections,
    /// Number of consumer groups
    ConsumerGroups,
    /// Retention period in days
    RetentionDays,
    /// Number of producers
    Producers,
    /// Number of consumers
    Consumers,
    /// Custom resource
    Custom(u32),
}

/// Resource quota definition
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceQuota {
    /// Maximum topics
    pub max_topics: Option<u64>,
    /// Maximum partitions per topic
    pub max_partitions_per_topic: Option<u64>,
    /// Maximum storage in bytes
    pub max_storage_bytes: Option<u64>,
    /// Maximum message rate (messages/second)
    pub max_message_rate: Option<u64>,
    /// Maximum bandwidth (bytes/second)
    pub max_bandwidth_bytes: Option<u64>,
    /// Maximum connections
    pub max_connections: Option<u64>,
    /// Maximum consumer groups
    pub max_consumer_groups: Option<u64>,
    /// Maximum retention days
    pub retention_days: Option<u64>,
}

impl ResourceQuota {
    /// Create a new quota with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Set max topics
    pub fn with_max_topics(mut self, max: u64) -> Self {
        self.max_topics = Some(max);
        self
    }

    /// Set max storage
    pub fn with_max_storage(mut self, bytes: u64) -> Self {
        self.max_storage_bytes = Some(bytes);
        self
    }

    /// Set max message rate
    pub fn with_max_message_rate(mut self, rate: u64) -> Self {
        self.max_message_rate = Some(rate);
        self
    }

    /// Set max connections
    pub fn with_max_connections(mut self, max: u64) -> Self {
        self.max_connections = Some(max);
        self
    }

    /// Get limit for a resource type
    pub fn get_limit(&self, resource: ResourceType) -> Option<u64> {
        match resource {
            ResourceType::Topics => self.max_topics,
            ResourceType::PartitionsPerTopic => self.max_partitions_per_topic,
            ResourceType::Storage => self.max_storage_bytes,
            ResourceType::MessageRate => self.max_message_rate,
            ResourceType::Bandwidth => self.max_bandwidth_bytes,
            ResourceType::Connections => self.max_connections,
            ResourceType::ConsumerGroups => self.max_consumer_groups,
            ResourceType::RetentionDays => self.retention_days,
            _ => None,
        }
    }

    /// Check if a resource has unlimited quota
    pub fn is_unlimited(&self, resource: ResourceType) -> bool {
        self.get_limit(resource).is_none()
    }
}

/// Quota limit with current usage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaLimit {
    /// Maximum allowed value (None = unlimited)
    pub limit: Option<u64>,
    /// Current usage
    pub current: u64,
    /// Warning threshold (percentage)
    pub warning_threshold: f64,
}

impl QuotaLimit {
    /// Create a new quota limit
    pub fn new(limit: Option<u64>) -> Self {
        Self {
            limit,
            current: 0,
            warning_threshold: 0.8, // 80% warning
        }
    }

    /// Check if quota is exceeded
    pub fn is_exceeded(&self) -> bool {
        if let Some(limit) = self.limit {
            self.current > limit
        } else {
            false
        }
    }

    /// Check if quota would be exceeded by adding amount
    pub fn would_exceed(&self, amount: u64) -> bool {
        if let Some(limit) = self.limit {
            self.current + amount > limit
        } else {
            false
        }
    }

    /// Check if at warning threshold
    pub fn is_warning(&self) -> bool {
        if let Some(limit) = self.limit {
            let threshold = (limit as f64 * self.warning_threshold) as u64;
            self.current >= threshold
        } else {
            false
        }
    }

    /// Get usage percentage
    pub fn usage_percent(&self) -> Option<f64> {
        self.limit.map(|limit| {
            if limit == 0 {
                0.0
            } else {
                (self.current as f64 / limit as f64) * 100.0
            }
        })
    }

    /// Add to current usage
    pub fn add(&mut self, amount: u64) {
        self.current = self.current.saturating_add(amount);
    }

    /// Subtract from current usage
    pub fn subtract(&mut self, amount: u64) {
        self.current = self.current.saturating_sub(amount);
    }

    /// Set current usage
    pub fn set(&mut self, value: u64) {
        self.current = value;
    }

    /// Reset current usage
    pub fn reset(&mut self) {
        self.current = 0;
    }

    /// Available capacity
    pub fn available(&self) -> Option<u64> {
        self.limit.map(|limit| limit.saturating_sub(self.current))
    }
}

impl Default for QuotaLimit {
    fn default() -> Self {
        Self::new(None)
    }
}

/// Quota policy for enforcement
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaPolicy {
    /// Enforcement mode
    pub enforcement: EnforcementMode,
    /// Grace period in seconds
    pub grace_period_secs: u64,
    /// Burst allowance percentage
    pub burst_allowance: f64,
    /// Notification on warning
    pub notify_on_warning: bool,
    /// Notification on exceeded
    pub notify_on_exceeded: bool,
}

impl Default for QuotaPolicy {
    fn default() -> Self {
        Self {
            enforcement: EnforcementMode::Soft,
            grace_period_secs: 300, // 5 minutes
            burst_allowance: 0.1,   // 10% burst
            notify_on_warning: true,
            notify_on_exceeded: true,
        }
    }
}

/// Enforcement mode for quotas
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum EnforcementMode {
    /// No enforcement (monitoring only)
    None,
    /// Soft enforcement (warn but allow)
    #[default]
    Soft,
    /// Hard enforcement (reject when exceeded)
    Hard,
    /// Throttle (slow down when approaching limit)
    Throttle,
}

/// Tenant quota status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantQuotaStatus {
    /// Tenant ID
    pub tenant_id: String,
    /// Quota limits by resource type
    pub limits: HashMap<ResourceType, QuotaLimit>,
    /// Policy
    pub policy: QuotaPolicy,
    /// Last updated
    pub last_updated: i64,
}

impl TenantQuotaStatus {
    /// Create new quota status
    pub fn new(tenant_id: impl Into<String>, quota: ResourceQuota) -> Self {
        let mut limits = HashMap::new();

        // Initialize limits from quota
        if let Some(limit) = quota.max_topics {
            limits.insert(ResourceType::Topics, QuotaLimit::new(Some(limit)));
        }
        if let Some(limit) = quota.max_partitions_per_topic {
            limits.insert(
                ResourceType::PartitionsPerTopic,
                QuotaLimit::new(Some(limit)),
            );
        }
        if let Some(limit) = quota.max_storage_bytes {
            limits.insert(ResourceType::Storage, QuotaLimit::new(Some(limit)));
        }
        if let Some(limit) = quota.max_message_rate {
            limits.insert(ResourceType::MessageRate, QuotaLimit::new(Some(limit)));
        }
        if let Some(limit) = quota.max_bandwidth_bytes {
            limits.insert(ResourceType::Bandwidth, QuotaLimit::new(Some(limit)));
        }
        if let Some(limit) = quota.max_connections {
            limits.insert(ResourceType::Connections, QuotaLimit::new(Some(limit)));
        }
        if let Some(limit) = quota.max_consumer_groups {
            limits.insert(ResourceType::ConsumerGroups, QuotaLimit::new(Some(limit)));
        }

        Self {
            tenant_id: tenant_id.into(),
            limits,
            policy: QuotaPolicy::default(),
            last_updated: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Check if a resource operation is allowed
    pub fn check(&self, resource: ResourceType, amount: u64) -> bool {
        if let Some(limit) = self.limits.get(&resource) {
            match self.policy.enforcement {
                EnforcementMode::None | EnforcementMode::Soft => true,
                EnforcementMode::Hard => !limit.would_exceed(amount),
                EnforcementMode::Throttle => {
                    // Allow but may throttle
                    !limit.would_exceed(amount)
                }
            }
        } else {
            // No limit for this resource
            true
        }
    }

    /// Record usage
    pub fn record(&mut self, resource: ResourceType, amount: u64) {
        let limit = self.limits.entry(resource).or_default();
        limit.add(amount);
        self.last_updated = chrono::Utc::now().timestamp_millis();
    }

    /// Get usage for a resource
    pub fn usage(&self, resource: ResourceType) -> Option<&QuotaLimit> {
        self.limits.get(&resource)
    }

    /// Check if any quota is exceeded
    pub fn any_exceeded(&self) -> bool {
        self.limits.values().any(|l| l.is_exceeded())
    }

    /// Check if any quota is at warning
    pub fn any_warning(&self) -> bool {
        self.limits.values().any(|l| l.is_warning())
    }

    /// Get exceeded quotas
    pub fn exceeded_quotas(&self) -> Vec<ResourceType> {
        self.limits
            .iter()
            .filter(|(_, l)| l.is_exceeded())
            .map(|(r, _)| *r)
            .collect()
    }
}

/// Quota enforcer
pub struct QuotaEnforcer {
    /// Quota status by tenant
    quotas: HashMap<String, TenantQuotaStatus>,
    /// Global policy
    global_policy: QuotaPolicy,
}

impl QuotaEnforcer {
    /// Create a new quota enforcer
    pub fn new() -> Self {
        Self {
            quotas: HashMap::new(),
            global_policy: QuotaPolicy::default(),
        }
    }

    /// Set quotas for a tenant
    pub fn set_quotas(&mut self, tenant_id: &str, quota: ResourceQuota) {
        let status = TenantQuotaStatus::new(tenant_id, quota);
        self.quotas.insert(tenant_id.to_string(), status);
    }

    /// Remove quotas for a tenant
    pub fn remove_quotas(&mut self, tenant_id: &str) {
        self.quotas.remove(tenant_id);
    }

    /// Update quota for a tenant
    pub fn update_quota(&mut self, tenant_id: &str, resource: ResourceType, limit: Option<u64>) {
        if let Some(status) = self.quotas.get_mut(tenant_id) {
            status.limits.insert(resource, QuotaLimit::new(limit));
            status.last_updated = chrono::Utc::now().timestamp_millis();
        }
    }

    /// Check if operation is allowed
    pub fn check_quota(
        &self,
        tenant_id: &str,
        resource: ResourceType,
        amount: u64,
    ) -> Result<(), TenantError> {
        if let Some(status) = self.quotas.get(tenant_id) {
            if !status.check(resource, amount) {
                return Err(TenantError::QuotaExceeded(format!(
                    "Quota exceeded for {:?}: requested {}, limit {:?}",
                    resource,
                    amount,
                    status.usage(resource).and_then(|l| l.limit)
                )));
            }
        }
        Ok(())
    }

    /// Record usage
    pub fn record_usage(&mut self, tenant_id: &str, resource: ResourceType, amount: u64) {
        if let Some(status) = self.quotas.get_mut(tenant_id) {
            status.record(resource, amount);
        }
    }

    /// Set usage (for absolute values like connections)
    pub fn set_usage(&mut self, tenant_id: &str, resource: ResourceType, value: u64) {
        if let Some(status) = self.quotas.get_mut(tenant_id) {
            if let Some(limit) = status.limits.get_mut(&resource) {
                limit.set(value);
                status.last_updated = chrono::Utc::now().timestamp_millis();
            }
        }
    }

    /// Get quota status for a tenant
    pub fn get_status(&self, tenant_id: &str) -> Option<&TenantQuotaStatus> {
        self.quotas.get(tenant_id)
    }

    /// Get all tenants with exceeded quotas
    pub fn exceeded_tenants(&self) -> Vec<&str> {
        self.quotas
            .iter()
            .filter(|(_, s)| s.any_exceeded())
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Get all tenants with warning quotas
    pub fn warning_tenants(&self) -> Vec<&str> {
        self.quotas
            .iter()
            .filter(|(_, s)| s.any_warning())
            .map(|(id, _)| id.as_str())
            .collect()
    }

    /// Set global policy
    pub fn set_global_policy(&mut self, policy: QuotaPolicy) {
        self.global_policy = policy;
    }

    /// Get tenant count
    pub fn tenant_count(&self) -> usize {
        self.quotas.len()
    }
}

impl Default for QuotaEnforcer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_quota() {
        let quota = ResourceQuota::new()
            .with_max_topics(100)
            .with_max_storage(1024 * 1024);

        assert_eq!(quota.get_limit(ResourceType::Topics), Some(100));
        assert_eq!(quota.get_limit(ResourceType::Storage), Some(1024 * 1024));
        assert!(quota.is_unlimited(ResourceType::Connections));
    }

    #[test]
    fn test_quota_limit() {
        let mut limit = QuotaLimit::new(Some(100));

        assert!(!limit.is_exceeded());
        assert!(!limit.would_exceed(50));
        assert!(limit.would_exceed(101));

        limit.add(80);
        assert!(limit.is_warning()); // 80% threshold

        limit.add(21);
        assert!(limit.is_exceeded());
    }

    #[test]
    fn test_quota_limit_unlimited() {
        let limit = QuotaLimit::new(None);

        assert!(!limit.is_exceeded());
        assert!(!limit.would_exceed(u64::MAX));
        assert!(limit.usage_percent().is_none());
    }

    #[test]
    fn test_quota_limit_operations() {
        let mut limit = QuotaLimit::new(Some(100));

        limit.add(50);
        assert_eq!(limit.current, 50);

        limit.subtract(20);
        assert_eq!(limit.current, 30);

        limit.set(75);
        assert_eq!(limit.current, 75);

        assert_eq!(limit.available(), Some(25));

        limit.reset();
        assert_eq!(limit.current, 0);
    }

    #[test]
    fn test_tenant_quota_status() {
        let quota = ResourceQuota::new()
            .with_max_topics(10)
            .with_max_connections(100);

        let mut status = TenantQuotaStatus::new("tenant1", quota);

        assert!(status.check(ResourceType::Topics, 5));
        assert!(!status.any_exceeded());

        status.record(ResourceType::Topics, 11);
        assert!(status.any_exceeded());
    }

    #[test]
    fn test_quota_enforcer() {
        let mut enforcer = QuotaEnforcer::new();

        let quota = ResourceQuota::new().with_max_topics(10);
        enforcer.set_quotas("tenant1", quota);

        // Should allow within limit
        assert!(enforcer
            .check_quota("tenant1", ResourceType::Topics, 5)
            .is_ok());

        // Record some usage
        enforcer.record_usage("tenant1", ResourceType::Topics, 8);

        // Now should fail for exceeding limit (soft enforcement still allows)
        let status = enforcer.get_status("tenant1").unwrap();
        assert_eq!(status.usage(ResourceType::Topics).unwrap().current, 8);
    }

    #[test]
    fn test_quota_enforcer_hard_enforcement() {
        let mut enforcer = QuotaEnforcer::new();

        let quota = ResourceQuota::new().with_max_topics(10);
        enforcer.set_quotas("tenant1", quota);

        // Set hard enforcement
        if let Some(status) = enforcer.quotas.get_mut("tenant1") {
            status.policy.enforcement = EnforcementMode::Hard;
            status.record(ResourceType::Topics, 10);
        }

        // Should fail with hard enforcement
        let result = enforcer.check_quota("tenant1", ResourceType::Topics, 1);
        assert!(matches!(result, Err(TenantError::QuotaExceeded(_))));
    }

    #[test]
    fn test_quota_enforcer_exceeded_tenants() {
        let mut enforcer = QuotaEnforcer::new();

        let quota1 = ResourceQuota::new().with_max_topics(10);
        let quota2 = ResourceQuota::new().with_max_topics(100);

        enforcer.set_quotas("tenant1", quota1);
        enforcer.set_quotas("tenant2", quota2);

        enforcer.record_usage("tenant1", ResourceType::Topics, 15);

        let exceeded = enforcer.exceeded_tenants();
        assert_eq!(exceeded.len(), 1);
        assert_eq!(exceeded[0], "tenant1");
    }

    #[test]
    fn test_quota_limit_usage_percent() {
        let mut limit = QuotaLimit::new(Some(100));

        limit.set(25);
        assert_eq!(limit.usage_percent(), Some(25.0));

        limit.set(100);
        assert_eq!(limit.usage_percent(), Some(100.0));
    }
}
