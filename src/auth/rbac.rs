//! Role-Based Access Control (RBAC)
//!
//! This module provides RBAC functionality on top of the existing ACL system.
//! Roles are named collections of permissions that can be assigned to principals
//! via role bindings.
//!
//! ## Built-in Roles
//!
//! - `cluster-admin`: Full access to all cluster operations
//! - `producer`: Produce to topics
//! - `consumer`: Consume from topics and manage consumer groups
//! - `topic-admin`: Create, delete, and configure topics
//! - `readonly`: Read-only access to topics and cluster metadata
//!
//! ## Example
//!
//! ```text
//! # Define a custom role
//! roles:
//!   - name: data-engineer
//!     permissions:
//!       - resource_type: topic
//!         resource_pattern: "analytics-*"
//!         pattern_type: prefixed
//!         operations: [read, write, describe]
//!
//! # Bind users to roles
//! role_bindings:
//!   - role: data-engineer
//!     principals:
//!       - User:alice
//!       - User:bob
//! ```

use crate::auth::acl::{Operation, PatternType, Permission, ResourceType};
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

/// A permission that can be granted by a role
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RolePermission {
    /// Resource type this permission applies to
    pub resource_type: ResourceType,

    /// Pattern for matching resource names
    #[serde(default = "default_resource_pattern")]
    pub resource_pattern: String,

    /// How to match the resource pattern
    #[serde(default)]
    pub pattern_type: PatternType,

    /// Operations allowed (empty means all)
    #[serde(default)]
    pub operations: Vec<Operation>,

    /// Host pattern (default: all hosts)
    #[serde(default = "default_host")]
    pub hosts: String,
}

fn default_resource_pattern() -> String {
    "*".to_string()
}

fn default_host() -> String {
    "*".to_string()
}

impl RolePermission {
    /// Create a new role permission
    pub fn new(resource_type: ResourceType) -> Self {
        Self {
            resource_type,
            resource_pattern: "*".to_string(),
            pattern_type: PatternType::Literal,
            operations: vec![],
            hosts: "*".to_string(),
        }
    }

    /// Set the resource pattern
    pub fn with_resource_pattern(mut self, pattern: &str) -> Self {
        self.resource_pattern = pattern.to_string();
        self
    }

    /// Set the pattern type
    pub fn with_pattern_type(mut self, pattern_type: PatternType) -> Self {
        self.pattern_type = pattern_type;
        self
    }

    /// Set the allowed operations
    pub fn with_operations(mut self, operations: Vec<Operation>) -> Self {
        self.operations = operations;
        self
    }

    /// Set the host pattern
    pub fn with_hosts(mut self, hosts: &str) -> Self {
        self.hosts = hosts.to_string();
        self
    }

    /// Check if this permission matches the given request
    pub fn matches(
        &self,
        resource_type: ResourceType,
        resource_name: &str,
        host: &str,
        operation: Operation,
    ) -> bool {
        // Check resource type
        if self.resource_type != ResourceType::Any && self.resource_type != resource_type {
            return false;
        }

        // Check resource name pattern
        if !self.matches_resource_name(resource_name) {
            return false;
        }

        // Check host
        if self.hosts != "*" && self.hosts != host {
            return false;
        }

        // Check operation
        if !self.operations.is_empty()
            && !self.operations.contains(&Operation::All)
            && !self.operations.contains(&operation)
        {
            return false;
        }

        true
    }

    /// Check if the resource name matches the pattern
    fn matches_resource_name(&self, resource_name: &str) -> bool {
        match self.pattern_type {
            PatternType::Literal => {
                self.resource_pattern == "*" || self.resource_pattern == resource_name
            }
            PatternType::Prefixed => {
                self.resource_pattern == "*" || resource_name.starts_with(&self.resource_pattern)
            }
            PatternType::Match => true,
        }
    }
}

/// A role is a named collection of permissions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    /// Role name (must be unique)
    pub name: String,

    /// Optional description of the role
    #[serde(default)]
    pub description: String,

    /// Permissions granted by this role
    pub permissions: Vec<RolePermission>,

    /// Parent roles (for inheritance)
    #[serde(default)]
    pub inherits_from: Vec<String>,

    /// Whether this is a built-in role (cannot be deleted)
    #[serde(default)]
    pub builtin: bool,
}

impl Role {
    /// Create a new role
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            description: String::new(),
            permissions: Vec::new(),
            inherits_from: Vec::new(),
            builtin: false,
        }
    }

    /// Set the description
    pub fn with_description(mut self, description: &str) -> Self {
        self.description = description.to_string();
        self
    }

    /// Add a permission
    pub fn with_permission(mut self, permission: RolePermission) -> Self {
        self.permissions.push(permission);
        self
    }

    /// Add multiple permissions
    pub fn with_permissions(mut self, permissions: Vec<RolePermission>) -> Self {
        self.permissions.extend(permissions);
        self
    }

    /// Set parent roles
    pub fn inherits(mut self, parent: &str) -> Self {
        self.inherits_from.push(parent.to_string());
        self
    }

    /// Mark as builtin
    pub fn builtin(mut self) -> Self {
        self.builtin = true;
        self
    }
}

/// A role binding assigns roles to principals
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleBinding {
    /// Role name
    pub role: String,

    /// Principals assigned to this role (e.g., "User:alice", "Group:engineers")
    pub principals: Vec<String>,

    /// Optional namespace/scope (for multi-tenant setups)
    #[serde(default)]
    pub namespace: Option<String>,
}

impl RoleBinding {
    /// Create a new role binding
    pub fn new(role: &str) -> Self {
        Self {
            role: role.to_string(),
            principals: Vec::new(),
            namespace: None,
        }
    }

    /// Add a principal
    pub fn with_principal(mut self, principal: &str) -> Self {
        self.principals.push(principal.to_string());
        self
    }

    /// Add multiple principals
    pub fn with_principals(mut self, principals: Vec<String>) -> Self {
        self.principals.extend(principals);
        self
    }

    /// Set the namespace
    pub fn in_namespace(mut self, namespace: &str) -> Self {
        self.namespace = Some(namespace.to_string());
        self
    }
}

/// RBAC configuration file format
#[derive(Debug, Serialize, Deserialize, Default)]
struct RbacFile {
    /// Custom roles
    #[serde(default)]
    roles: Vec<Role>,

    /// Role bindings
    #[serde(default)]
    role_bindings: Vec<RoleBinding>,
}

/// RBAC Manager for role-based access control
#[derive(Debug, Clone)]
pub struct RbacManager {
    /// All roles (built-in + custom)
    roles: HashMap<String, Role>,

    /// Role bindings indexed by principal
    bindings_by_principal: HashMap<String, HashSet<String>>,

    /// All role bindings
    role_bindings: Vec<RoleBinding>,

    /// Whether RBAC is enabled
    enabled: bool,
}

impl Default for RbacManager {
    fn default() -> Self {
        Self::new()
    }
}

impl RbacManager {
    /// Create a new RBAC manager with built-in roles
    pub fn new() -> Self {
        let mut manager = Self {
            roles: HashMap::new(),
            bindings_by_principal: HashMap::new(),
            role_bindings: Vec::new(),
            enabled: true,
        };

        // Add built-in roles
        manager.add_builtin_roles();

        manager
    }

    /// Create a disabled RBAC manager
    pub fn disabled() -> Self {
        Self {
            roles: HashMap::new(),
            bindings_by_principal: HashMap::new(),
            role_bindings: Vec::new(),
            enabled: false,
        }
    }

    /// Add built-in roles
    fn add_builtin_roles(&mut self) {
        // Cluster Admin - full access to everything
        self.add_role(
            Role::new("cluster-admin")
                .with_description("Full administrative access to all cluster resources")
                .with_permission(
                    RolePermission::new(ResourceType::Any)
                        .with_resource_pattern("*")
                        .with_operations(vec![Operation::All]),
                )
                .builtin(),
        );

        // Producer - write to topics
        self.add_role(
            Role::new("producer")
                .with_description("Produce messages to topics")
                .with_permissions(vec![
                    RolePermission::new(ResourceType::Topic)
                        .with_resource_pattern("*")
                        .with_operations(vec![Operation::Write, Operation::Describe]),
                    RolePermission::new(ResourceType::TransactionalId)
                        .with_resource_pattern("*")
                        .with_operations(vec![Operation::Write, Operation::Describe]),
                    RolePermission::new(ResourceType::Cluster)
                        .with_operations(vec![Operation::IdempotentWrite]),
                ])
                .builtin(),
        );

        // Consumer - read from topics and manage consumer groups
        self.add_role(
            Role::new("consumer")
                .with_description("Consume messages from topics")
                .with_permissions(vec![
                    RolePermission::new(ResourceType::Topic)
                        .with_resource_pattern("*")
                        .with_operations(vec![Operation::Read, Operation::Describe]),
                    RolePermission::new(ResourceType::Group)
                        .with_resource_pattern("*")
                        .with_operations(vec![Operation::Read, Operation::Describe]),
                ])
                .builtin(),
        );

        // Topic Admin - manage topics
        self.add_role(
            Role::new("topic-admin")
                .with_description("Create, delete, and configure topics")
                .with_permissions(vec![
                    RolePermission::new(ResourceType::Topic)
                        .with_resource_pattern("*")
                        .with_operations(vec![
                            Operation::Create,
                            Operation::Delete,
                            Operation::Alter,
                            Operation::AlterConfigs,
                            Operation::Describe,
                            Operation::DescribeConfigs,
                        ]),
                    RolePermission::new(ResourceType::Cluster)
                        .with_operations(vec![Operation::Create]),
                ])
                .builtin(),
        );

        // Read-only - read access to topics and metadata
        self.add_role(
            Role::new("readonly")
                .with_description("Read-only access to topics and cluster metadata")
                .with_permissions(vec![
                    RolePermission::new(ResourceType::Topic)
                        .with_resource_pattern("*")
                        .with_operations(vec![Operation::Describe, Operation::DescribeConfigs]),
                    RolePermission::new(ResourceType::Group)
                        .with_resource_pattern("*")
                        .with_operations(vec![Operation::Describe]),
                    RolePermission::new(ResourceType::Cluster)
                        .with_operations(vec![Operation::Describe, Operation::DescribeConfigs]),
                ])
                .builtin(),
        );

        // Group Admin - manage consumer groups
        self.add_role(
            Role::new("group-admin")
                .with_description("Manage consumer groups")
                .with_permissions(vec![RolePermission::new(ResourceType::Group)
                    .with_resource_pattern("*")
                    .with_operations(vec![
                        Operation::Read,
                        Operation::Describe,
                        Operation::Delete,
                        Operation::Alter,
                    ])])
                .builtin(),
        );
    }

    /// Load RBAC configuration from a file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut manager = Self::new();

        if !path.as_ref().exists() {
            return Ok(manager);
        }

        let content = fs::read_to_string(path.as_ref())
            .map_err(|e| StreamlineError::Config(format!("Failed to read RBAC file: {}", e)))?;

        let rbac_file: RbacFile = serde_yaml::from_str(&content)
            .map_err(|e| StreamlineError::Config(format!("Failed to parse RBAC file: {}", e)))?;

        // Add custom roles
        for role in rbac_file.roles {
            manager.add_role(role);
        }

        // Add role bindings
        for binding in rbac_file.role_bindings {
            manager.add_role_binding(binding)?;
        }

        Ok(manager)
    }

    /// Save RBAC configuration to a file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        // Only save custom roles (not built-in)
        let custom_roles: Vec<Role> = self
            .roles
            .values()
            .filter(|r| !r.builtin)
            .cloned()
            .collect();

        let rbac_file = RbacFile {
            roles: custom_roles,
            role_bindings: self.role_bindings.clone(),
        };

        let content = serde_yaml::to_string(&rbac_file)
            .map_err(|e| StreamlineError::Config(format!("Failed to serialize RBAC: {}", e)))?;

        // Create parent directory if needed
        if let Some(parent) = path.as_ref().parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(|e| {
                    StreamlineError::Config(format!("Failed to create RBAC directory: {}", e))
                })?;
            }
        }

        fs::write(path.as_ref(), content)
            .map_err(|e| StreamlineError::Config(format!("Failed to write RBAC file: {}", e)))?;

        Ok(())
    }

    /// Add a role
    pub fn add_role(&mut self, role: Role) {
        self.roles.insert(role.name.clone(), role);
    }

    /// Get a role by name
    pub fn get_role(&self, name: &str) -> Option<&Role> {
        self.roles.get(name)
    }

    /// List all roles
    pub fn list_roles(&self) -> Vec<&Role> {
        self.roles.values().collect()
    }

    /// Delete a role (only custom roles)
    pub fn delete_role(&mut self, name: &str) -> Result<()> {
        if let Some(role) = self.roles.get(name) {
            if role.builtin {
                return Err(StreamlineError::Config(format!(
                    "Cannot delete built-in role: {}",
                    name
                )));
            }
        }

        self.roles.remove(name);

        // Remove any bindings for this role
        self.role_bindings.retain(|b| b.role != name);
        self.rebuild_bindings_index();

        Ok(())
    }

    /// Add a role binding
    pub fn add_role_binding(&mut self, binding: RoleBinding) -> Result<()> {
        // Validate role exists
        if !self.roles.contains_key(&binding.role) {
            return Err(StreamlineError::Config(format!(
                "Role not found: {}",
                binding.role
            )));
        }

        // Update index
        for principal in &binding.principals {
            self.bindings_by_principal
                .entry(principal.clone())
                .or_default()
                .insert(binding.role.clone());
        }

        self.role_bindings.push(binding);
        Ok(())
    }

    /// Remove a role binding
    pub fn remove_role_binding(&mut self, role: &str, principal: &str) -> bool {
        let mut removed = false;

        // Find and update the binding
        for binding in &mut self.role_bindings {
            if binding.role == role {
                let original_len = binding.principals.len();
                binding.principals.retain(|p| p != principal);
                if binding.principals.len() < original_len {
                    removed = true;
                }
            }
        }

        // Remove empty bindings
        self.role_bindings.retain(|b| !b.principals.is_empty());

        // Update index
        if removed {
            self.rebuild_bindings_index();
        }

        removed
    }

    /// Rebuild the bindings index
    fn rebuild_bindings_index(&mut self) {
        self.bindings_by_principal.clear();
        for binding in &self.role_bindings {
            for principal in &binding.principals {
                self.bindings_by_principal
                    .entry(principal.clone())
                    .or_default()
                    .insert(binding.role.clone());
            }
        }
    }

    /// Get all roles for a principal
    pub fn get_roles_for_principal(&self, principal: &str) -> Vec<&Role> {
        let mut roles = Vec::new();
        let mut visited = HashSet::new();

        if let Some(role_names) = self.bindings_by_principal.get(principal) {
            for role_name in role_names {
                self.collect_roles_recursive(role_name, &mut roles, &mut visited);
            }
        }

        roles
    }

    /// Collect roles recursively (following inheritance)
    fn collect_roles_recursive<'a>(
        &'a self,
        role_name: &str,
        roles: &mut Vec<&'a Role>,
        visited: &mut HashSet<String>,
    ) {
        if visited.contains(role_name) {
            return;
        }
        visited.insert(role_name.to_string());

        if let Some(role) = self.roles.get(role_name) {
            // Add parent roles first
            for parent in &role.inherits_from {
                self.collect_roles_recursive(parent, roles, visited);
            }
            roles.push(role);
        }
    }

    /// Check if a principal has permission for an operation
    pub fn check_permission(
        &self,
        principal: &str,
        resource_type: ResourceType,
        resource_name: &str,
        host: &str,
        operation: Operation,
    ) -> Permission {
        if !self.enabled {
            return Permission::Allow;
        }

        let roles = self.get_roles_for_principal(principal);

        for role in roles {
            for permission in &role.permissions {
                if permission.matches(resource_type, resource_name, host, operation) {
                    return Permission::Allow;
                }
            }
        }

        Permission::Deny
    }

    /// Check if RBAC is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Enable RBAC
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Disable RBAC
    pub fn disable(&mut self) {
        self.enabled = false;
    }

    /// List all role bindings
    pub fn list_role_bindings(&self) -> &[RoleBinding] {
        &self.role_bindings
    }

    /// Get role bindings for a specific role
    pub fn get_bindings_for_role(&self, role: &str) -> Vec<&RoleBinding> {
        self.role_bindings
            .iter()
            .filter(|b| b.role == role)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_permission_matches() {
        let perm = RolePermission::new(ResourceType::Topic)
            .with_resource_pattern("orders")
            .with_operations(vec![Operation::Read, Operation::Write]);

        // Should match
        assert!(perm.matches(ResourceType::Topic, "orders", "*", Operation::Read));
        assert!(perm.matches(ResourceType::Topic, "orders", "*", Operation::Write));

        // Should not match different topic
        assert!(!perm.matches(ResourceType::Topic, "events", "*", Operation::Read));

        // Should not match different operation
        assert!(!perm.matches(ResourceType::Topic, "orders", "*", Operation::Delete));
    }

    #[test]
    fn test_role_permission_prefixed() {
        let perm = RolePermission::new(ResourceType::Topic)
            .with_resource_pattern("events-")
            .with_pattern_type(PatternType::Prefixed)
            .with_operations(vec![Operation::Write]);

        // Should match prefixed topics
        assert!(perm.matches(ResourceType::Topic, "events-orders", "*", Operation::Write));
        assert!(perm.matches(ResourceType::Topic, "events-users", "*", Operation::Write));

        // Should not match non-prefixed
        assert!(!perm.matches(ResourceType::Topic, "orders-events", "*", Operation::Write));
    }

    #[test]
    fn test_role_permission_wildcard() {
        let perm = RolePermission::new(ResourceType::Topic)
            .with_resource_pattern("*")
            .with_operations(vec![Operation::Describe]);

        // Should match any topic
        assert!(perm.matches(ResourceType::Topic, "any-topic", "*", Operation::Describe));
        assert!(perm.matches(ResourceType::Topic, "another", "*", Operation::Describe));
    }

    #[test]
    fn test_role_creation() {
        let role = Role::new("custom-role")
            .with_description("A custom role")
            .with_permission(
                RolePermission::new(ResourceType::Topic)
                    .with_resource_pattern("custom-*")
                    .with_pattern_type(PatternType::Prefixed)
                    .with_operations(vec![Operation::Read, Operation::Write]),
            );

        assert_eq!(role.name, "custom-role");
        assert_eq!(role.description, "A custom role");
        assert_eq!(role.permissions.len(), 1);
        assert!(!role.builtin);
    }

    #[test]
    fn test_rbac_manager_builtin_roles() {
        let manager = RbacManager::new();

        // Check built-in roles exist
        assert!(manager.get_role("cluster-admin").is_some());
        assert!(manager.get_role("producer").is_some());
        assert!(manager.get_role("consumer").is_some());
        assert!(manager.get_role("topic-admin").is_some());
        assert!(manager.get_role("readonly").is_some());
        assert!(manager.get_role("group-admin").is_some());

        // Check they are marked as builtin
        assert!(manager.get_role("cluster-admin").unwrap().builtin);
    }

    #[test]
    fn test_rbac_manager_add_role_binding() {
        let mut manager = RbacManager::new();

        let binding = RoleBinding::new("producer")
            .with_principal("User:alice")
            .with_principal("User:bob");

        manager.add_role_binding(binding).unwrap();

        let alice_roles = manager.get_roles_for_principal("User:alice");
        assert_eq!(alice_roles.len(), 1);
        assert_eq!(alice_roles[0].name, "producer");

        let bob_roles = manager.get_roles_for_principal("User:bob");
        assert_eq!(bob_roles.len(), 1);
    }

    #[test]
    fn test_rbac_manager_check_permission() {
        let mut manager = RbacManager::new();

        // Bind alice to producer role
        manager
            .add_role_binding(RoleBinding::new("producer").with_principal("User:alice"))
            .unwrap();

        // Bind bob to consumer role
        manager
            .add_role_binding(RoleBinding::new("consumer").with_principal("User:bob"))
            .unwrap();

        // Alice can write to topics
        assert_eq!(
            manager.check_permission(
                "User:alice",
                ResourceType::Topic,
                "orders",
                "*",
                Operation::Write
            ),
            Permission::Allow
        );

        // Alice cannot read from topics (producer role doesn't include read)
        assert_eq!(
            manager.check_permission(
                "User:alice",
                ResourceType::Topic,
                "orders",
                "*",
                Operation::Read
            ),
            Permission::Deny
        );

        // Bob can read from topics
        assert_eq!(
            manager.check_permission(
                "User:bob",
                ResourceType::Topic,
                "orders",
                "*",
                Operation::Read
            ),
            Permission::Allow
        );

        // Bob cannot write to topics
        assert_eq!(
            manager.check_permission(
                "User:bob",
                ResourceType::Topic,
                "orders",
                "*",
                Operation::Write
            ),
            Permission::Deny
        );
    }

    #[test]
    fn test_rbac_manager_cluster_admin() {
        let mut manager = RbacManager::new();

        manager
            .add_role_binding(RoleBinding::new("cluster-admin").with_principal("User:admin"))
            .unwrap();

        // Admin should be able to do anything
        assert_eq!(
            manager.check_permission(
                "User:admin",
                ResourceType::Topic,
                "any-topic",
                "*",
                Operation::All
            ),
            Permission::Allow
        );

        assert_eq!(
            manager.check_permission(
                "User:admin",
                ResourceType::Cluster,
                "kafka-cluster",
                "*",
                Operation::Create
            ),
            Permission::Allow
        );
    }

    #[test]
    fn test_rbac_manager_remove_role_binding() {
        let mut manager = RbacManager::new();

        manager
            .add_role_binding(
                RoleBinding::new("producer")
                    .with_principal("User:alice")
                    .with_principal("User:bob"),
            )
            .unwrap();

        // Both should have the role
        assert_eq!(manager.get_roles_for_principal("User:alice").len(), 1);
        assert_eq!(manager.get_roles_for_principal("User:bob").len(), 1);

        // Remove alice's binding
        manager.remove_role_binding("producer", "User:alice");

        // Alice should no longer have the role
        assert_eq!(manager.get_roles_for_principal("User:alice").len(), 0);

        // Bob should still have the role
        assert_eq!(manager.get_roles_for_principal("User:bob").len(), 1);
    }

    #[test]
    fn test_rbac_manager_delete_builtin_role_fails() {
        let mut manager = RbacManager::new();

        let result = manager.delete_role("cluster-admin");
        assert!(result.is_err());
    }

    #[test]
    fn test_rbac_manager_custom_role() {
        let mut manager = RbacManager::new();

        // Add a custom role
        let custom_role = Role::new("analytics-reader")
            .with_description("Read analytics topics")
            .with_permission(
                RolePermission::new(ResourceType::Topic)
                    .with_resource_pattern("analytics-")
                    .with_pattern_type(PatternType::Prefixed)
                    .with_operations(vec![Operation::Read, Operation::Describe]),
            );

        manager.add_role(custom_role);

        // Bind a user
        manager
            .add_role_binding(RoleBinding::new("analytics-reader").with_principal("User:analyst"))
            .unwrap();

        // Should be able to read analytics topics
        assert_eq!(
            manager.check_permission(
                "User:analyst",
                ResourceType::Topic,
                "analytics-events",
                "*",
                Operation::Read
            ),
            Permission::Allow
        );

        // Should not be able to read other topics
        assert_eq!(
            manager.check_permission(
                "User:analyst",
                ResourceType::Topic,
                "orders",
                "*",
                Operation::Read
            ),
            Permission::Deny
        );
    }

    #[test]
    fn test_rbac_manager_disabled() {
        let manager = RbacManager::disabled();

        // When disabled, everything should be allowed
        assert_eq!(
            manager.check_permission(
                "User:anyone",
                ResourceType::Topic,
                "any-topic",
                "*",
                Operation::All
            ),
            Permission::Allow
        );
    }

    #[test]
    fn test_role_inheritance() {
        let mut manager = RbacManager::new();

        // Create a senior-developer role that inherits from producer and consumer
        let senior_dev = Role::new("senior-developer")
            .with_description("Senior developer with producer and consumer access")
            .inherits("producer")
            .inherits("consumer");

        manager.add_role(senior_dev);

        manager
            .add_role_binding(RoleBinding::new("senior-developer").with_principal("User:senior"))
            .unwrap();

        // Should have producer permissions (write)
        assert_eq!(
            manager.check_permission(
                "User:senior",
                ResourceType::Topic,
                "orders",
                "*",
                Operation::Write
            ),
            Permission::Allow
        );

        // Should have consumer permissions (read)
        assert_eq!(
            manager.check_permission(
                "User:senior",
                ResourceType::Topic,
                "orders",
                "*",
                Operation::Read
            ),
            Permission::Allow
        );
    }

    #[test]
    fn test_multiple_role_bindings() {
        let mut manager = RbacManager::new();

        // Bind alice to both producer and consumer roles
        manager
            .add_role_binding(RoleBinding::new("producer").with_principal("User:alice"))
            .unwrap();
        manager
            .add_role_binding(RoleBinding::new("consumer").with_principal("User:alice"))
            .unwrap();

        // Should be able to both read and write
        assert_eq!(
            manager.check_permission(
                "User:alice",
                ResourceType::Topic,
                "orders",
                "*",
                Operation::Write
            ),
            Permission::Allow
        );
        assert_eq!(
            manager.check_permission(
                "User:alice",
                ResourceType::Topic,
                "orders",
                "*",
                Operation::Read
            ),
            Permission::Allow
        );
    }

    #[test]
    fn test_list_roles() {
        let manager = RbacManager::new();
        let roles = manager.list_roles();

        // Should have at least the built-in roles
        assert!(roles.len() >= 6);

        let role_names: Vec<&str> = roles.iter().map(|r| r.name.as_str()).collect();
        assert!(role_names.contains(&"cluster-admin"));
        assert!(role_names.contains(&"producer"));
        assert!(role_names.contains(&"consumer"));
    }

    #[test]
    fn test_role_binding_invalid_role() {
        let mut manager = RbacManager::new();

        let binding = RoleBinding::new("non-existent-role").with_principal("User:alice");

        let result = manager.add_role_binding(binding);
        assert!(result.is_err());
    }
}
