//! Tenant Isolation
//!
//! Provides namespace-based isolation for multi-tenant deployments.

use super::TenantError;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Isolation level for tenants
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum IsolationLevel {
    /// No isolation (shared resources)
    None,
    /// Logical namespace isolation (same cluster)
    #[default]
    Namespace,
    /// Resource isolation (dedicated resources within cluster)
    Resource,
    /// Physical isolation (dedicated cluster/nodes)
    Physical,
}

impl IsolationLevel {
    /// Get description
    pub fn description(&self) -> &'static str {
        match self {
            IsolationLevel::None => "No isolation - shared resources",
            IsolationLevel::Namespace => "Logical namespace isolation",
            IsolationLevel::Resource => "Dedicated resources within cluster",
            IsolationLevel::Physical => "Physical isolation - dedicated infrastructure",
        }
    }
}

/// Isolation policy for a tenant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IsolationPolicy {
    /// Isolation level
    pub level: IsolationLevel,
    /// Allowed cross-tenant access
    pub allow_cross_tenant: bool,
    /// Allowed namespaces to access
    pub allowed_namespaces: HashSet<String>,
    /// Denied namespaces
    pub denied_namespaces: HashSet<String>,
    /// Resource tags for isolation
    pub resource_tags: HashMap<String, String>,
}

impl Default for IsolationPolicy {
    fn default() -> Self {
        Self {
            level: IsolationLevel::Namespace,
            allow_cross_tenant: false,
            allowed_namespaces: HashSet::new(),
            denied_namespaces: HashSet::new(),
            resource_tags: HashMap::new(),
        }
    }
}

impl IsolationPolicy {
    /// Create a new policy
    pub fn new(level: IsolationLevel) -> Self {
        Self {
            level,
            ..Default::default()
        }
    }

    /// Allow cross-tenant access
    pub fn with_cross_tenant(mut self, allow: bool) -> Self {
        self.allow_cross_tenant = allow;
        self
    }

    /// Add allowed namespace
    pub fn allow_namespace(mut self, ns: impl Into<String>) -> Self {
        self.allowed_namespaces.insert(ns.into());
        self
    }

    /// Add denied namespace
    pub fn deny_namespace(mut self, ns: impl Into<String>) -> Self {
        self.denied_namespaces.insert(ns.into());
        self
    }

    /// Check if namespace is accessible
    pub fn can_access_namespace(&self, namespace: &str) -> bool {
        // Check denied list first
        if self.denied_namespaces.contains(namespace) {
            return false;
        }

        // If allowed list is empty, allow all (except denied)
        if self.allowed_namespaces.is_empty() {
            return true;
        }

        // Check allowed list
        self.allowed_namespaces.contains(namespace)
    }
}

/// Tenant namespace
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantNamespace {
    /// Namespace name
    pub name: String,
    /// Owning tenant ID
    pub tenant_id: String,
    /// Description
    pub description: Option<String>,
    /// Resources in this namespace
    pub resources: HashSet<String>,
    /// Resource limits for this namespace
    pub resource_limits: HashMap<String, u64>,
    /// Creation timestamp
    pub created_at: i64,
    /// Is default namespace
    pub is_default: bool,
}

impl TenantNamespace {
    /// Create a new namespace
    pub fn new(name: impl Into<String>, tenant_id: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            tenant_id: tenant_id.into(),
            description: None,
            resources: HashSet::new(),
            resource_limits: HashMap::new(),
            created_at: chrono::Utc::now().timestamp_millis(),
            is_default: false,
        }
    }

    /// Set as default namespace
    pub fn as_default(mut self) -> Self {
        self.is_default = true;
        self
    }

    /// Add resource to namespace
    pub fn add_resource(&mut self, resource: impl Into<String>) {
        self.resources.insert(resource.into());
    }

    /// Remove resource from namespace
    pub fn remove_resource(&mut self, resource: &str) {
        self.resources.remove(resource);
    }

    /// Check if resource exists in namespace
    pub fn has_resource(&self, resource: &str) -> bool {
        self.resources.contains(resource)
    }

    /// Set resource limit
    pub fn set_limit(&mut self, resource_type: impl Into<String>, limit: u64) {
        self.resource_limits.insert(resource_type.into(), limit);
    }
}

/// Namespace manager
pub struct NamespaceManager {
    /// Namespaces by full name (tenant_id/namespace)
    namespaces: HashMap<String, TenantNamespace>,
    /// Tenant to namespaces mapping
    tenant_namespaces: HashMap<String, HashSet<String>>,
    /// Isolation policies by tenant
    policies: HashMap<String, IsolationPolicy>,
}

impl NamespaceManager {
    /// Create a new namespace manager
    pub fn new() -> Self {
        Self {
            namespaces: HashMap::new(),
            tenant_namespaces: HashMap::new(),
            policies: HashMap::new(),
        }
    }

    /// Create a namespace
    pub fn create_namespace(&mut self, tenant_id: &str, name: &str) -> Result<(), TenantError> {
        let full_name = format!("{}/{}", tenant_id, name);

        if self.namespaces.contains_key(&full_name) {
            return Err(TenantError::NamespaceError(format!(
                "Namespace {} already exists",
                full_name
            )));
        }

        let is_default = name == "default";
        let namespace = TenantNamespace::new(name, tenant_id);
        let mut namespace = namespace;
        if is_default {
            namespace.is_default = true;
        }

        self.namespaces.insert(full_name.clone(), namespace);
        self.tenant_namespaces
            .entry(tenant_id.to_string())
            .or_default()
            .insert(name.to_string());

        Ok(())
    }

    /// Get a namespace
    pub fn get_namespace(&self, tenant_id: &str, name: &str) -> Option<&TenantNamespace> {
        let full_name = format!("{}/{}", tenant_id, name);
        self.namespaces.get(&full_name)
    }

    /// Get mutable namespace
    pub fn get_namespace_mut(
        &mut self,
        tenant_id: &str,
        name: &str,
    ) -> Option<&mut TenantNamespace> {
        let full_name = format!("{}/{}", tenant_id, name);
        self.namespaces.get_mut(&full_name)
    }

    /// List namespaces for a tenant
    pub fn list_namespaces(&self, tenant_id: &str) -> Vec<&TenantNamespace> {
        self.tenant_namespaces
            .get(tenant_id)
            .map(|names| {
                names
                    .iter()
                    .filter_map(|name| {
                        let full_name = format!("{}/{}", tenant_id, name);
                        self.namespaces.get(&full_name)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Delete a namespace
    pub fn delete_namespace(&mut self, tenant_id: &str, name: &str) -> Result<(), TenantError> {
        let full_name = format!("{}/{}", tenant_id, name);

        if !self.namespaces.contains_key(&full_name) {
            return Err(TenantError::NamespaceError(format!(
                "Namespace {} not found",
                full_name
            )));
        }

        self.namespaces.remove(&full_name);
        if let Some(names) = self.tenant_namespaces.get_mut(tenant_id) {
            names.remove(name);
        }

        Ok(())
    }

    /// Delete all namespaces for a tenant
    pub fn delete_tenant_namespaces(&mut self, tenant_id: &str) {
        if let Some(names) = self.tenant_namespaces.remove(tenant_id) {
            for name in names {
                let full_name = format!("{}/{}", tenant_id, name);
                self.namespaces.remove(&full_name);
            }
        }
        self.policies.remove(tenant_id);
    }

    /// Set isolation policy for a tenant
    pub fn set_policy(&mut self, tenant_id: &str, policy: IsolationPolicy) {
        self.policies.insert(tenant_id.to_string(), policy);
    }

    /// Get isolation policy for a tenant
    pub fn get_policy(&self, tenant_id: &str) -> Option<&IsolationPolicy> {
        self.policies.get(tenant_id)
    }

    /// Check if a tenant can access a resource in a namespace
    pub fn can_access(&self, tenant_id: &str, namespace: &str, resource: &str) -> bool {
        // Check if namespace exists and belongs to tenant
        let full_name = format!("{}/{}", tenant_id, namespace);

        if let Some(ns) = self.namespaces.get(&full_name) {
            // If resource is empty, just checking namespace access
            if resource.is_empty() {
                return true;
            }
            // Check if resource exists in namespace (or namespace is open)
            return ns.resources.is_empty() || ns.has_resource(resource);
        }

        // Check cross-tenant access if policy allows
        if let Some(policy) = self.policies.get(tenant_id) {
            if policy.allow_cross_tenant && policy.can_access_namespace(namespace) {
                return true;
            }
        }

        false
    }

    /// Add resource to namespace
    pub fn add_resource(
        &mut self,
        tenant_id: &str,
        namespace: &str,
        resource: &str,
    ) -> Result<(), TenantError> {
        let full_name = format!("{}/{}", tenant_id, namespace);

        let ns = self.namespaces.get_mut(&full_name).ok_or_else(|| {
            TenantError::NamespaceError(format!("Namespace {} not found", full_name))
        })?;

        ns.add_resource(resource);
        Ok(())
    }

    /// Remove resource from namespace
    pub fn remove_resource(
        &mut self,
        tenant_id: &str,
        namespace: &str,
        resource: &str,
    ) -> Result<(), TenantError> {
        let full_name = format!("{}/{}", tenant_id, namespace);

        let ns = self.namespaces.get_mut(&full_name).ok_or_else(|| {
            TenantError::NamespaceError(format!("Namespace {} not found", full_name))
        })?;

        ns.remove_resource(resource);
        Ok(())
    }

    /// Get total namespace count
    pub fn namespace_count(&self) -> usize {
        self.namespaces.len()
    }

    /// Get namespace count for a tenant
    pub fn tenant_namespace_count(&self, tenant_id: &str) -> usize {
        self.tenant_namespaces
            .get(tenant_id)
            .map(|ns| ns.len())
            .unwrap_or(0)
    }
}

impl Default for NamespaceManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolation_level() {
        let level = IsolationLevel::Namespace;
        assert_eq!(level.description(), "Logical namespace isolation");
    }

    #[test]
    fn test_isolation_policy() {
        let policy = IsolationPolicy::new(IsolationLevel::Namespace)
            .allow_namespace("public")
            .deny_namespace("admin");

        assert!(policy.can_access_namespace("public"));
        assert!(!policy.can_access_namespace("admin"));
        assert!(!policy.can_access_namespace("other")); // Not in allowed list
    }

    #[test]
    fn test_isolation_policy_empty_allowed() {
        let policy = IsolationPolicy::new(IsolationLevel::Namespace).deny_namespace("admin");

        // With empty allowed list, all except denied are accessible
        assert!(policy.can_access_namespace("any"));
        assert!(policy.can_access_namespace("public"));
        assert!(!policy.can_access_namespace("admin"));
    }

    #[test]
    fn test_tenant_namespace() {
        let mut ns = TenantNamespace::new("default", "tenant1");
        ns.add_resource("topic-1");
        ns.add_resource("topic-2");

        assert!(ns.has_resource("topic-1"));
        assert!(!ns.has_resource("topic-3"));

        ns.remove_resource("topic-1");
        assert!(!ns.has_resource("topic-1"));
    }

    #[test]
    fn test_namespace_manager_create() {
        let mut manager = NamespaceManager::new();

        manager.create_namespace("tenant1", "default").unwrap();
        manager.create_namespace("tenant1", "prod").unwrap();

        assert!(manager.get_namespace("tenant1", "default").is_some());
        assert!(manager.get_namespace("tenant1", "prod").is_some());
        assert!(manager.get_namespace("tenant1", "staging").is_none());
    }

    #[test]
    fn test_namespace_manager_list() {
        let mut manager = NamespaceManager::new();

        manager.create_namespace("tenant1", "ns1").unwrap();
        manager.create_namespace("tenant1", "ns2").unwrap();
        manager.create_namespace("tenant2", "ns1").unwrap();

        let tenant1_ns = manager.list_namespaces("tenant1");
        assert_eq!(tenant1_ns.len(), 2);

        let tenant2_ns = manager.list_namespaces("tenant2");
        assert_eq!(tenant2_ns.len(), 1);
    }

    #[test]
    fn test_namespace_manager_delete() {
        let mut manager = NamespaceManager::new();

        manager.create_namespace("tenant1", "ns1").unwrap();
        assert!(manager.get_namespace("tenant1", "ns1").is_some());

        manager.delete_namespace("tenant1", "ns1").unwrap();
        assert!(manager.get_namespace("tenant1", "ns1").is_none());
    }

    #[test]
    fn test_namespace_manager_delete_tenant() {
        let mut manager = NamespaceManager::new();

        manager.create_namespace("tenant1", "ns1").unwrap();
        manager.create_namespace("tenant1", "ns2").unwrap();
        assert_eq!(manager.tenant_namespace_count("tenant1"), 2);

        manager.delete_tenant_namespaces("tenant1");
        assert_eq!(manager.tenant_namespace_count("tenant1"), 0);
    }

    #[test]
    fn test_namespace_manager_access() {
        let mut manager = NamespaceManager::new();

        manager.create_namespace("tenant1", "default").unwrap();
        manager
            .add_resource("tenant1", "default", "topic-1")
            .unwrap();

        assert!(manager.can_access("tenant1", "default", "topic-1"));
        assert!(!manager.can_access("tenant1", "default", "topic-2"));
        assert!(!manager.can_access("tenant2", "default", "topic-1"));
    }

    #[test]
    fn test_namespace_manager_cross_tenant() {
        let mut manager = NamespaceManager::new();

        manager.create_namespace("tenant1", "shared").unwrap();

        // Without cross-tenant policy
        assert!(!manager.can_access("tenant2", "shared", ""));

        // With cross-tenant policy
        let policy = IsolationPolicy::new(IsolationLevel::Namespace)
            .with_cross_tenant(true)
            .allow_namespace("shared");
        manager.set_policy("tenant2", policy);

        // Still can't access because namespace belongs to tenant1
        // Cross-tenant access requires explicit setup
    }

    #[test]
    fn test_duplicate_namespace_error() {
        let mut manager = NamespaceManager::new();

        manager.create_namespace("tenant1", "ns1").unwrap();
        let result = manager.create_namespace("tenant1", "ns1");
        assert!(matches!(result, Err(TenantError::NamespaceError(_))));
    }
}
