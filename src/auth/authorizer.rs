//! Authorizer for checking permissions against ACLs and RBAC
//!
//! The Authorizer is responsible for making authorization decisions based on
//! the configured ACLs and RBAC roles. It supports super users who bypass all
//! ACL and RBAC checks.
//!
//! ## Authorization Order
//!
//! 1. If authorization is disabled, allow all
//! 2. Check super users - allow if matched
//! 3. Check RBAC roles - allow if any role grants permission
//! 4. Check ACLs - deny ACLs override allow ACLs
//! 5. Use default permission if no match

use crate::auth::acl::{
    Acl, AclFilter, AclStore, Operation, PatternType, Permission, ResourceType,
};
use crate::auth::rbac::RbacManager;
use crate::error::{Result, StreamlineError};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Authorization result
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationResult {
    /// Access is allowed
    Allowed,
    /// Access is denied
    Denied,
}

/// Authorizer configuration
#[derive(Debug, Clone)]
pub struct AuthorizerConfig {
    /// Whether authorization is enabled
    pub enabled: bool,

    /// Default permission when no ACL matches (usually Deny)
    pub default_permission: Permission,

    /// Super users who bypass all ACL checks (e.g., "User:admin")
    pub super_users: HashSet<String>,

    /// Allow all operations if no ACLs are configured
    pub allow_if_no_acls: bool,

    /// Enable RBAC (role-based access control) in addition to ACLs
    pub rbac_enabled: bool,
}

impl Default for AuthorizerConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            default_permission: Permission::Deny,
            super_users: HashSet::new(),
            allow_if_no_acls: true,
            rbac_enabled: false,
        }
    }
}

impl AuthorizerConfig {
    /// Create a new authorizer config with authorization enabled
    pub fn enabled() -> Self {
        Self {
            enabled: true,
            ..Default::default()
        }
    }

    /// Add a super user
    pub fn with_super_user(mut self, principal: &str) -> Self {
        self.super_users.insert(principal.to_string());
        self
    }

    /// Add multiple super users
    pub fn with_super_users(mut self, principals: &[String]) -> Self {
        for principal in principals {
            self.super_users.insert(principal.clone());
        }
        self
    }

    /// Set the default permission
    pub fn with_default_permission(mut self, permission: Permission) -> Self {
        self.default_permission = permission;
        self
    }

    /// Set whether to allow if no ACLs are configured
    pub fn with_allow_if_no_acls(mut self, allow: bool) -> Self {
        self.allow_if_no_acls = allow;
        self
    }

    /// Enable RBAC
    pub fn with_rbac(mut self, enabled: bool) -> Self {
        self.rbac_enabled = enabled;
        self
    }
}

/// The Authorizer checks permissions against ACLs and RBAC roles
#[derive(Debug)]
pub struct Authorizer {
    /// ACL store
    acl_store: Arc<RwLock<AclStore>>,

    /// RBAC manager
    rbac_manager: Arc<RwLock<RbacManager>>,

    /// Authorizer configuration
    config: AuthorizerConfig,
}

impl Authorizer {
    /// Create a new Authorizer with an empty ACL store and RBAC manager
    pub fn new(config: AuthorizerConfig) -> Self {
        let rbac_manager = if config.rbac_enabled {
            RbacManager::new()
        } else {
            RbacManager::disabled()
        };

        Self {
            acl_store: Arc::new(RwLock::new(AclStore::new())),
            rbac_manager: Arc::new(RwLock::new(rbac_manager)),
            config,
        }
    }

    /// Create a new Authorizer loading ACLs from a file
    pub fn from_file<P: AsRef<Path>>(path: P, config: AuthorizerConfig) -> Result<Self> {
        let acl_store = if path.as_ref().exists() {
            AclStore::from_file(path)?
        } else {
            AclStore::new()
        };

        let rbac_manager = if config.rbac_enabled {
            RbacManager::new()
        } else {
            RbacManager::disabled()
        };

        Ok(Self {
            acl_store: Arc::new(RwLock::new(acl_store)),
            rbac_manager: Arc::new(RwLock::new(rbac_manager)),
            config,
        })
    }

    /// Create a new Authorizer with RBAC loaded from a file
    pub fn with_rbac_file<P: AsRef<Path>, Q: AsRef<Path>>(
        acl_path: P,
        rbac_path: Q,
        config: AuthorizerConfig,
    ) -> Result<Self> {
        let acl_store = if acl_path.as_ref().exists() {
            AclStore::from_file(acl_path)?
        } else {
            AclStore::new()
        };

        let rbac_manager = if config.rbac_enabled {
            RbacManager::from_file(rbac_path)?
        } else {
            RbacManager::disabled()
        };

        Ok(Self {
            acl_store: Arc::new(RwLock::new(acl_store)),
            rbac_manager: Arc::new(RwLock::new(rbac_manager)),
            config,
        })
    }

    /// Get access to the underlying ACL store
    /// This is primarily used by the Kafka ACL APIs
    pub fn acl_store(&self) -> Arc<RwLock<AclStore>> {
        self.acl_store.clone()
    }

    /// Get access to the RBAC manager
    pub fn rbac_manager(&self) -> Arc<RwLock<RbacManager>> {
        self.rbac_manager.clone()
    }

    /// Check if a principal is authorized to perform an operation on a resource
    pub async fn authorize(
        &self,
        principal: &str,
        host: &str,
        operation: Operation,
        resource_type: ResourceType,
        resource_name: &str,
    ) -> AuthorizationResult {
        // If authorization is disabled, allow everything
        if !self.config.enabled {
            return AuthorizationResult::Allowed;
        }

        // Check if principal is a super user
        if self.config.super_users.contains(principal) {
            debug!(
                principal = %principal,
                operation = %operation,
                resource_type = %resource_type,
                resource_name = %resource_name,
                "Super user access granted"
            );
            return AuthorizationResult::Allowed;
        }

        // Check RBAC permissions if enabled
        if self.config.rbac_enabled {
            let rbac_manager = self.rbac_manager.read().await;
            if rbac_manager.is_enabled() {
                // Check if principal has any roles assigned
                let roles = rbac_manager.get_roles_for_principal(principal);
                if !roles.is_empty() {
                    // Principal has roles, check if any grants the permission
                    let result = rbac_manager.check_permission(
                        principal,
                        resource_type,
                        resource_name,
                        host,
                        operation,
                    );
                    match result {
                        Permission::Allow => {
                            debug!(
                                principal = %principal,
                                operation = %operation,
                                resource_type = %resource_type,
                                resource_name = %resource_name,
                                "Access allowed by RBAC role"
                            );
                            return AuthorizationResult::Allowed;
                        }
                        Permission::Deny => {
                            debug!(
                                principal = %principal,
                                operation = %operation,
                                resource_type = %resource_type,
                                resource_name = %resource_name,
                                "Access denied by RBAC (no role grants permission)"
                            );
                            return AuthorizationResult::Denied;
                        }
                    }
                }
                // If no roles are assigned, fall through to ACL check
            }
        }

        let acl_store = self.acl_store.read().await;

        // If no ACLs are configured, use the allow_if_no_acls setting
        if acl_store.is_empty() {
            if self.config.allow_if_no_acls {
                return AuthorizationResult::Allowed;
            } else {
                return AuthorizationResult::Denied;
            }
        }

        // Find all matching ACLs
        let matching_acls: Vec<&Acl> = acl_store
            .list_acls()
            .iter()
            .filter(|acl| acl.matches(resource_type, resource_name, principal, host, operation))
            .collect();

        // Apply Kafka ACL evaluation logic:
        // 1. If any Deny ACL matches, deny access
        // 2. If any Allow ACL matches, allow access
        // 3. Otherwise, use default permission

        for acl in &matching_acls {
            if acl.permission == Permission::Deny {
                debug!(
                    principal = %principal,
                    operation = %operation,
                    resource_type = %resource_type,
                    resource_name = %resource_name,
                    acl = %acl,
                    "Access denied by ACL"
                );
                return AuthorizationResult::Denied;
            }
        }

        for acl in &matching_acls {
            if acl.permission == Permission::Allow {
                debug!(
                    principal = %principal,
                    operation = %operation,
                    resource_type = %resource_type,
                    resource_name = %resource_name,
                    acl = %acl,
                    "Access allowed by ACL"
                );
                return AuthorizationResult::Allowed;
            }
        }

        // No matching ACL, use default permission
        match self.config.default_permission {
            Permission::Allow => {
                debug!(
                    principal = %principal,
                    operation = %operation,
                    resource_type = %resource_type,
                    resource_name = %resource_name,
                    "Access allowed by default permission"
                );
                AuthorizationResult::Allowed
            }
            Permission::Deny => {
                warn!(
                    principal = %principal,
                    operation = %operation,
                    resource_type = %resource_type,
                    resource_name = %resource_name,
                    "Access denied (no matching ACL)"
                );
                AuthorizationResult::Denied
            }
        }
    }

    /// Check authorization and return an error if denied
    pub async fn check_authorization(
        &self,
        principal: &str,
        host: &str,
        operation: Operation,
        resource_type: ResourceType,
        resource_name: &str,
    ) -> Result<()> {
        match self
            .authorize(principal, host, operation, resource_type, resource_name)
            .await
        {
            AuthorizationResult::Allowed => Ok(()),
            AuthorizationResult::Denied => Err(StreamlineError::AuthorizationFailed(format!(
                "Principal {} is not authorized to {} on {}:{}",
                principal, operation, resource_type, resource_name
            ))),
        }
    }

    /// Add an ACL
    pub async fn add_acl(&self, acl: Acl) -> Result<()> {
        let mut store = self.acl_store.write().await;
        store.add_acl(acl);
        Ok(())
    }

    /// Remove ACLs matching the filter
    pub async fn remove_acls(&self, filter: &AclFilter) -> Result<usize> {
        let mut store = self.acl_store.write().await;
        Ok(store.remove_acls(filter))
    }

    /// Find ACLs matching the filter
    pub async fn find_acls(&self, filter: &AclFilter) -> Vec<Acl> {
        let store = self.acl_store.read().await;
        store.find_acls(filter).into_iter().cloned().collect()
    }

    /// Get all ACLs
    pub async fn list_acls(&self) -> Vec<Acl> {
        let store = self.acl_store.read().await;
        store.list_acls().to_vec()
    }

    /// Save ACLs to a file
    pub async fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let store = self.acl_store.read().await;
        store.save_to_file(path)
    }

    /// Check if authorization is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the authorizer configuration
    pub fn config(&self) -> &AuthorizerConfig {
        &self.config
    }

    /// Create an authorizer that allows everything (for testing or disabled auth)
    pub fn allow_all() -> Self {
        Self::new(AuthorizerConfig {
            enabled: false,
            ..Default::default()
        })
    }
}

/// Helper functions for common authorization checks
impl Authorizer {
    /// Check if principal can produce to a topic
    pub async fn can_produce(&self, principal: &str, host: &str, topic: &str) -> Result<()> {
        self.check_authorization(
            principal,
            host,
            Operation::Write,
            ResourceType::Topic,
            topic,
        )
        .await
    }

    /// Check if principal can fetch from a topic
    pub async fn can_fetch(&self, principal: &str, host: &str, topic: &str) -> Result<()> {
        self.check_authorization(principal, host, Operation::Read, ResourceType::Topic, topic)
            .await
    }

    /// Check if principal can create topics
    pub async fn can_create_topic(&self, principal: &str, host: &str) -> Result<()> {
        self.check_authorization(
            principal,
            host,
            Operation::Create,
            ResourceType::Cluster,
            "kafka-cluster",
        )
        .await
    }

    /// Check if principal can delete a topic
    pub async fn can_delete_topic(&self, principal: &str, host: &str, topic: &str) -> Result<()> {
        self.check_authorization(
            principal,
            host,
            Operation::Delete,
            ResourceType::Topic,
            topic,
        )
        .await
    }

    /// Check if principal can describe a topic
    pub async fn can_describe_topic(&self, principal: &str, host: &str, topic: &str) -> Result<()> {
        self.check_authorization(
            principal,
            host,
            Operation::Describe,
            ResourceType::Topic,
            topic,
        )
        .await
    }

    /// Check if principal can read from a consumer group
    pub async fn can_read_group(&self, principal: &str, host: &str, group_id: &str) -> Result<()> {
        self.check_authorization(
            principal,
            host,
            Operation::Read,
            ResourceType::Group,
            group_id,
        )
        .await
    }

    /// Check if principal can describe a consumer group
    pub async fn can_describe_group(
        &self,
        principal: &str,
        host: &str,
        group_id: &str,
    ) -> Result<()> {
        self.check_authorization(
            principal,
            host,
            Operation::Describe,
            ResourceType::Group,
            group_id,
        )
        .await
    }

    /// Check if principal can delete a consumer group
    pub async fn can_delete_group(
        &self,
        principal: &str,
        host: &str,
        group_id: &str,
    ) -> Result<()> {
        self.check_authorization(
            principal,
            host,
            Operation::Delete,
            ResourceType::Group,
            group_id,
        )
        .await
    }

    /// Check if principal has cluster-level describe permission
    pub async fn can_describe_cluster(&self, principal: &str, host: &str) -> Result<()> {
        self.check_authorization(
            principal,
            host,
            Operation::Describe,
            ResourceType::Cluster,
            "kafka-cluster",
        )
        .await
    }
}

/// Builder for creating ACLs with a fluent interface
pub struct AclBuilder {
    resource_type: ResourceType,
    resource_name: String,
    pattern_type: PatternType,
    principal: String,
    host: String,
    operation: Operation,
    permission: Permission,
}

impl AclBuilder {
    /// Start building an ACL for a topic
    pub fn topic(name: &str) -> Self {
        Self {
            resource_type: ResourceType::Topic,
            resource_name: name.to_string(),
            pattern_type: PatternType::Literal,
            principal: String::new(),
            host: "*".to_string(),
            operation: Operation::All,
            permission: Permission::Allow,
        }
    }

    /// Start building an ACL for a consumer group
    pub fn group(name: &str) -> Self {
        Self {
            resource_type: ResourceType::Group,
            resource_name: name.to_string(),
            pattern_type: PatternType::Literal,
            principal: String::new(),
            host: "*".to_string(),
            operation: Operation::All,
            permission: Permission::Allow,
        }
    }

    /// Start building an ACL for the cluster
    pub fn cluster() -> Self {
        Self {
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster".to_string(),
            pattern_type: PatternType::Literal,
            principal: String::new(),
            host: "*".to_string(),
            operation: Operation::All,
            permission: Permission::Allow,
        }
    }

    /// Set the pattern type to prefixed
    pub fn prefixed(mut self) -> Self {
        self.pattern_type = PatternType::Prefixed;
        self
    }

    /// Set the principal
    pub fn principal(mut self, principal: &str) -> Self {
        self.principal = principal.to_string();
        self
    }

    /// Set the principal to a user
    pub fn user(mut self, username: &str) -> Self {
        self.principal = format!("User:{}", username);
        self
    }

    /// Set the principal to match all users
    pub fn all_users(mut self) -> Self {
        self.principal = "User:*".to_string();
        self
    }

    /// Set the host
    pub fn host(mut self, host: &str) -> Self {
        self.host = host.to_string();
        self
    }

    /// Set the operation
    pub fn operation(mut self, operation: Operation) -> Self {
        self.operation = operation;
        self
    }

    /// Set operation to Read
    pub fn read(mut self) -> Self {
        self.operation = Operation::Read;
        self
    }

    /// Set operation to Write
    pub fn write(mut self) -> Self {
        self.operation = Operation::Write;
        self
    }

    /// Set operation to All
    pub fn all_operations(mut self) -> Self {
        self.operation = Operation::All;
        self
    }

    /// Set permission to Allow
    pub fn allow(mut self) -> Self {
        self.permission = Permission::Allow;
        self
    }

    /// Set permission to Deny
    pub fn deny(mut self) -> Self {
        self.permission = Permission::Deny;
        self
    }

    /// Build the ACL
    pub fn build(self) -> Acl {
        Acl::new(
            self.resource_type,
            &self.resource_name,
            self.pattern_type,
            &self.principal,
            &self.host,
            self.operation,
            self.permission,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_authorizer_disabled() {
        let authorizer = Authorizer::new(AuthorizerConfig::default());

        // Authorization should pass when disabled
        let result = authorizer
            .authorize(
                "User:alice",
                "127.0.0.1",
                Operation::Write,
                ResourceType::Topic,
                "orders",
            )
            .await;

        assert_eq!(result, AuthorizationResult::Allowed);
    }

    #[tokio::test]
    async fn test_authorizer_super_user() {
        let config = AuthorizerConfig::enabled().with_super_user("User:admin");
        let authorizer = Authorizer::new(config);

        // Super user should be allowed even without ACLs
        let result = authorizer
            .authorize(
                "User:admin",
                "127.0.0.1",
                Operation::Write,
                ResourceType::Topic,
                "orders",
            )
            .await;

        assert_eq!(result, AuthorizationResult::Allowed);
    }

    #[tokio::test]
    async fn test_authorizer_no_acls_allow() {
        let config = AuthorizerConfig::enabled().with_allow_if_no_acls(true);
        let authorizer = Authorizer::new(config);

        // Should be allowed when no ACLs and allow_if_no_acls is true
        let result = authorizer
            .authorize(
                "User:alice",
                "127.0.0.1",
                Operation::Read,
                ResourceType::Topic,
                "orders",
            )
            .await;

        assert_eq!(result, AuthorizationResult::Allowed);
    }

    #[tokio::test]
    async fn test_authorizer_no_acls_deny() {
        let config = AuthorizerConfig::enabled().with_allow_if_no_acls(false);
        let authorizer = Authorizer::new(config);

        // Should be denied when no ACLs and allow_if_no_acls is false
        let result = authorizer
            .authorize(
                "User:alice",
                "127.0.0.1",
                Operation::Read,
                ResourceType::Topic,
                "orders",
            )
            .await;

        assert_eq!(result, AuthorizationResult::Denied);
    }

    #[tokio::test]
    async fn test_authorizer_allow_acl() {
        let config = AuthorizerConfig::enabled().with_allow_if_no_acls(false);
        let authorizer = Authorizer::new(config);

        // Add an allow ACL
        authorizer
            .add_acl(Acl::new(
                ResourceType::Topic,
                "orders",
                PatternType::Literal,
                "User:alice",
                "*",
                Operation::Read,
                Permission::Allow,
            ))
            .await
            .unwrap();

        // Should be allowed by ACL
        let result = authorizer
            .authorize(
                "User:alice",
                "127.0.0.1",
                Operation::Read,
                ResourceType::Topic,
                "orders",
            )
            .await;

        assert_eq!(result, AuthorizationResult::Allowed);

        // Different user should be denied
        let result = authorizer
            .authorize(
                "User:bob",
                "127.0.0.1",
                Operation::Read,
                ResourceType::Topic,
                "orders",
            )
            .await;

        assert_eq!(result, AuthorizationResult::Denied);
    }

    #[tokio::test]
    async fn test_authorizer_deny_overrides_allow() {
        let config = AuthorizerConfig::enabled()
            .with_allow_if_no_acls(false)
            .with_default_permission(Permission::Allow);
        let authorizer = Authorizer::new(config);

        // Add an allow ACL for all users
        authorizer
            .add_acl(Acl::new(
                ResourceType::Topic,
                "orders",
                PatternType::Literal,
                "User:*",
                "*",
                Operation::Read,
                Permission::Allow,
            ))
            .await
            .unwrap();

        // Add a deny ACL for specific user
        authorizer
            .add_acl(Acl::new(
                ResourceType::Topic,
                "orders",
                PatternType::Literal,
                "User:blocked",
                "*",
                Operation::Read,
                Permission::Deny,
            ))
            .await
            .unwrap();

        // Alice should be allowed
        let result = authorizer
            .authorize(
                "User:alice",
                "127.0.0.1",
                Operation::Read,
                ResourceType::Topic,
                "orders",
            )
            .await;

        assert_eq!(result, AuthorizationResult::Allowed);

        // Blocked user should be denied despite wildcard allow
        let result = authorizer
            .authorize(
                "User:blocked",
                "127.0.0.1",
                Operation::Read,
                ResourceType::Topic,
                "orders",
            )
            .await;

        assert_eq!(result, AuthorizationResult::Denied);
    }

    #[tokio::test]
    async fn test_authorizer_prefixed_acl() {
        let config = AuthorizerConfig::enabled().with_allow_if_no_acls(false);
        let authorizer = Authorizer::new(config);

        // Add a prefixed ACL
        authorizer
            .add_acl(Acl::new(
                ResourceType::Topic,
                "events-",
                PatternType::Prefixed,
                "User:producer",
                "*",
                Operation::Write,
                Permission::Allow,
            ))
            .await
            .unwrap();

        // Should be allowed for topics with prefix
        let result = authorizer
            .authorize(
                "User:producer",
                "127.0.0.1",
                Operation::Write,
                ResourceType::Topic,
                "events-orders",
            )
            .await;

        assert_eq!(result, AuthorizationResult::Allowed);

        // Should be denied for topics without prefix
        let result = authorizer
            .authorize(
                "User:producer",
                "127.0.0.1",
                Operation::Write,
                ResourceType::Topic,
                "orders",
            )
            .await;

        assert_eq!(result, AuthorizationResult::Denied);
    }

    #[tokio::test]
    async fn test_authorizer_check_authorization() {
        let config = AuthorizerConfig::enabled().with_allow_if_no_acls(false);
        let authorizer = Authorizer::new(config);

        // Add an allow ACL
        authorizer
            .add_acl(Acl::new(
                ResourceType::Topic,
                "orders",
                PatternType::Literal,
                "User:alice",
                "*",
                Operation::Read,
                Permission::Allow,
            ))
            .await
            .unwrap();

        // Should succeed
        let result = authorizer
            .check_authorization(
                "User:alice",
                "127.0.0.1",
                Operation::Read,
                ResourceType::Topic,
                "orders",
            )
            .await;

        assert!(result.is_ok());

        // Should fail with error
        let result = authorizer
            .check_authorization(
                "User:bob",
                "127.0.0.1",
                Operation::Read,
                ResourceType::Topic,
                "orders",
            )
            .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, StreamlineError::AuthorizationFailed(_)));
    }

    #[tokio::test]
    async fn test_authorizer_remove_acls() {
        let config = AuthorizerConfig::enabled().with_allow_if_no_acls(false);
        let authorizer = Authorizer::new(config);

        // Add ACLs
        authorizer
            .add_acl(Acl::new(
                ResourceType::Topic,
                "orders",
                PatternType::Literal,
                "User:alice",
                "*",
                Operation::Read,
                Permission::Allow,
            ))
            .await
            .unwrap();

        authorizer
            .add_acl(Acl::new(
                ResourceType::Topic,
                "orders",
                PatternType::Literal,
                "User:bob",
                "*",
                Operation::Read,
                Permission::Allow,
            ))
            .await
            .unwrap();

        // Verify both can access
        assert_eq!(
            authorizer
                .authorize(
                    "User:alice",
                    "127.0.0.1",
                    Operation::Read,
                    ResourceType::Topic,
                    "orders"
                )
                .await,
            AuthorizationResult::Allowed
        );

        // Remove alice's ACL
        let removed = authorizer
            .remove_acls(&AclFilter::new().with_principal("User:alice"))
            .await
            .unwrap();

        assert_eq!(removed, 1);

        // Alice should now be denied
        assert_eq!(
            authorizer
                .authorize(
                    "User:alice",
                    "127.0.0.1",
                    Operation::Read,
                    ResourceType::Topic,
                    "orders"
                )
                .await,
            AuthorizationResult::Denied
        );

        // Bob should still be allowed
        assert_eq!(
            authorizer
                .authorize(
                    "User:bob",
                    "127.0.0.1",
                    Operation::Read,
                    ResourceType::Topic,
                    "orders"
                )
                .await,
            AuthorizationResult::Allowed
        );
    }

    #[tokio::test]
    async fn test_acl_builder() {
        let acl = AclBuilder::topic("orders")
            .user("alice")
            .read()
            .allow()
            .build();

        assert_eq!(acl.resource_type, ResourceType::Topic);
        assert_eq!(acl.resource_name, "orders");
        assert_eq!(acl.principal, "User:alice");
        assert_eq!(acl.operation, Operation::Read);
        assert_eq!(acl.permission, Permission::Allow);
    }

    #[tokio::test]
    async fn test_acl_builder_prefixed() {
        let acl = AclBuilder::topic("events-")
            .prefixed()
            .user("producer")
            .write()
            .build();

        assert_eq!(acl.pattern_type, PatternType::Prefixed);
        assert_eq!(acl.resource_name, "events-");
    }

    #[tokio::test]
    async fn test_helper_methods() {
        let config = AuthorizerConfig::enabled().with_super_user("User:admin");
        let authorizer = Authorizer::new(config);

        // Super user should pass all checks
        assert!(authorizer
            .can_produce("User:admin", "*", "orders")
            .await
            .is_ok());
        assert!(authorizer
            .can_fetch("User:admin", "*", "orders")
            .await
            .is_ok());
        assert!(authorizer.can_create_topic("User:admin", "*").await.is_ok());
        assert!(authorizer
            .can_delete_topic("User:admin", "*", "orders")
            .await
            .is_ok());
        assert!(authorizer
            .can_read_group("User:admin", "*", "my-group")
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_rbac_producer_role() {
        use crate::auth::rbac::RoleBinding;

        let config = AuthorizerConfig::enabled()
            .with_rbac(true)
            .with_allow_if_no_acls(false);
        let authorizer = Authorizer::new(config);

        // Bind user to producer role
        {
            let rbac_manager = authorizer.rbac_manager();
            let mut rbac = rbac_manager.write().await;
            rbac.add_role_binding(RoleBinding::new("producer").with_principal("User:producer1"))
                .unwrap();
        }

        // Producer should be able to write to any topic
        let result = authorizer
            .authorize(
                "User:producer1",
                "127.0.0.1",
                Operation::Write,
                ResourceType::Topic,
                "orders",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Allowed);

        // Producer should be able to describe topics
        let result = authorizer
            .authorize(
                "User:producer1",
                "127.0.0.1",
                Operation::Describe,
                ResourceType::Topic,
                "orders",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Allowed);

        // Producer should NOT be able to read from topics
        let result = authorizer
            .authorize(
                "User:producer1",
                "127.0.0.1",
                Operation::Read,
                ResourceType::Topic,
                "orders",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Denied);
    }

    #[tokio::test]
    async fn test_rbac_consumer_role() {
        use crate::auth::rbac::RoleBinding;

        let config = AuthorizerConfig::enabled()
            .with_rbac(true)
            .with_allow_if_no_acls(false);
        let authorizer = Authorizer::new(config);

        // Bind user to consumer role
        {
            let rbac_manager = authorizer.rbac_manager();
            let mut rbac = rbac_manager.write().await;
            rbac.add_role_binding(RoleBinding::new("consumer").with_principal("User:consumer1"))
                .unwrap();
        }

        // Consumer should be able to read from topics
        let result = authorizer
            .authorize(
                "User:consumer1",
                "127.0.0.1",
                Operation::Read,
                ResourceType::Topic,
                "orders",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Allowed);

        // Consumer should be able to read from groups
        let result = authorizer
            .authorize(
                "User:consumer1",
                "127.0.0.1",
                Operation::Read,
                ResourceType::Group,
                "my-consumer-group",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Allowed);

        // Consumer should NOT be able to write to topics
        let result = authorizer
            .authorize(
                "User:consumer1",
                "127.0.0.1",
                Operation::Write,
                ResourceType::Topic,
                "orders",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Denied);
    }

    #[tokio::test]
    async fn test_rbac_cluster_admin_role() {
        use crate::auth::rbac::RoleBinding;

        let config = AuthorizerConfig::enabled()
            .with_rbac(true)
            .with_allow_if_no_acls(false);
        let authorizer = Authorizer::new(config);

        // Bind user to cluster-admin role
        {
            let rbac_manager = authorizer.rbac_manager();
            let mut rbac = rbac_manager.write().await;
            rbac.add_role_binding(RoleBinding::new("cluster-admin").with_principal("User:admin1"))
                .unwrap();
        }

        // Cluster admin should have full access to everything
        let result = authorizer
            .authorize(
                "User:admin1",
                "127.0.0.1",
                Operation::All,
                ResourceType::Cluster,
                "kafka-cluster",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Allowed);

        // Cluster admin should have full topic access
        let result = authorizer
            .authorize(
                "User:admin1",
                "127.0.0.1",
                Operation::Delete,
                ResourceType::Topic,
                "any-topic",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Allowed);
    }

    #[tokio::test]
    async fn test_rbac_disabled() {
        use crate::auth::rbac::RoleBinding;

        let config = AuthorizerConfig::enabled()
            .with_rbac(false)
            .with_allow_if_no_acls(false);
        let authorizer = Authorizer::new(config);

        // Even with RBAC disabled, try to bind a role (shouldn't affect auth)
        {
            let rbac_manager = authorizer.rbac_manager();
            let mut rbac = rbac_manager.write().await;
            // RBAC manager should be disabled, so this won't have effect
            let _ = rbac
                .add_role_binding(RoleBinding::new("producer").with_principal("User:producer1"));
        }

        // Should be denied since RBAC is disabled and no ACLs match
        let result = authorizer
            .authorize(
                "User:producer1",
                "127.0.0.1",
                Operation::Write,
                ResourceType::Topic,
                "orders",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Denied);
    }

    #[tokio::test]
    async fn test_rbac_fallback_to_acl() {
        let config = AuthorizerConfig::enabled()
            .with_rbac(true)
            .with_allow_if_no_acls(false);
        let authorizer = Authorizer::new(config);

        // Don't bind any RBAC roles, but add an ACL
        authorizer
            .add_acl(Acl::new(
                ResourceType::Topic,
                "special-topic",
                PatternType::Literal,
                "User:special",
                "*",
                Operation::Read,
                Permission::Allow,
            ))
            .await
            .unwrap();

        // User with ACL should be allowed
        let result = authorizer
            .authorize(
                "User:special",
                "127.0.0.1",
                Operation::Read,
                ResourceType::Topic,
                "special-topic",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Allowed);

        // User without role or ACL should be denied
        let result = authorizer
            .authorize(
                "User:random",
                "127.0.0.1",
                Operation::Read,
                ResourceType::Topic,
                "special-topic",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Denied);
    }

    #[tokio::test]
    async fn test_rbac_topic_admin_role() {
        use crate::auth::rbac::RoleBinding;

        let config = AuthorizerConfig::enabled()
            .with_rbac(true)
            .with_allow_if_no_acls(false);
        let authorizer = Authorizer::new(config);

        // Bind user to topic-admin role
        {
            let rbac_manager = authorizer.rbac_manager();
            let mut rbac = rbac_manager.write().await;
            rbac.add_role_binding(
                RoleBinding::new("topic-admin").with_principal("User:topic-manager"),
            )
            .unwrap();
        }

        // Topic admin should be able to create topics
        let result = authorizer
            .authorize(
                "User:topic-manager",
                "127.0.0.1",
                Operation::Create,
                ResourceType::Topic,
                "new-topic",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Allowed);

        // Topic admin should be able to delete topics
        let result = authorizer
            .authorize(
                "User:topic-manager",
                "127.0.0.1",
                Operation::Delete,
                ResourceType::Topic,
                "old-topic",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Allowed);

        // Topic admin should be able to alter topics
        let result = authorizer
            .authorize(
                "User:topic-manager",
                "127.0.0.1",
                Operation::Alter,
                ResourceType::Topic,
                "some-topic",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Allowed);
    }

    #[tokio::test]
    async fn test_rbac_readonly_role() {
        use crate::auth::rbac::RoleBinding;

        let config = AuthorizerConfig::enabled()
            .with_rbac(true)
            .with_allow_if_no_acls(false);
        let authorizer = Authorizer::new(config);

        // Bind user to readonly role
        {
            let rbac_manager = authorizer.rbac_manager();
            let mut rbac = rbac_manager.write().await;
            rbac.add_role_binding(RoleBinding::new("readonly").with_principal("User:viewer"))
                .unwrap();
        }

        // Readonly should be able to describe cluster
        let result = authorizer
            .authorize(
                "User:viewer",
                "127.0.0.1",
                Operation::Describe,
                ResourceType::Cluster,
                "kafka-cluster",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Allowed);

        // Readonly should be able to describe topics
        let result = authorizer
            .authorize(
                "User:viewer",
                "127.0.0.1",
                Operation::Describe,
                ResourceType::Topic,
                "any-topic",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Allowed);

        // Readonly should NOT be able to write
        let result = authorizer
            .authorize(
                "User:viewer",
                "127.0.0.1",
                Operation::Write,
                ResourceType::Topic,
                "any-topic",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Denied);
    }

    #[tokio::test]
    async fn test_rbac_multiple_roles() {
        use crate::auth::rbac::RoleBinding;

        let config = AuthorizerConfig::enabled()
            .with_rbac(true)
            .with_allow_if_no_acls(false);
        let authorizer = Authorizer::new(config);

        // Bind user to both producer and consumer roles
        {
            let rbac_manager = authorizer.rbac_manager();
            let mut rbac = rbac_manager.write().await;
            rbac.add_role_binding(RoleBinding::new("producer").with_principal("User:fullaccess"))
                .unwrap();
            rbac.add_role_binding(RoleBinding::new("consumer").with_principal("User:fullaccess"))
                .unwrap();
        }

        // Should be able to write (from producer role)
        let result = authorizer
            .authorize(
                "User:fullaccess",
                "127.0.0.1",
                Operation::Write,
                ResourceType::Topic,
                "orders",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Allowed);

        // Should be able to read (from consumer role)
        let result = authorizer
            .authorize(
                "User:fullaccess",
                "127.0.0.1",
                Operation::Read,
                ResourceType::Topic,
                "orders",
            )
            .await;
        assert_eq!(result, AuthorizationResult::Allowed);
    }
}
