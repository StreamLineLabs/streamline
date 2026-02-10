//! Access Control Lists (ACL) for authorization
//!
//! This module provides Kafka-compatible ACL functionality for controlling
//! access to resources (topics, groups, cluster).

use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Resource types that can be protected by ACLs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResourceType {
    /// Topic resource
    Topic,
    /// Consumer group resource
    Group,
    /// Cluster-level resource
    Cluster,
    /// Transactional ID resource
    TransactionalId,
    /// Any resource (matches all types)
    Any,
}

impl ResourceType {
    /// Convert from Kafka protocol resource type code
    pub fn from_code(code: i8) -> Option<Self> {
        match code {
            0 => Some(ResourceType::Any),
            1 => Some(ResourceType::Any), // UNKNOWN maps to Any
            2 => Some(ResourceType::Topic),
            3 => Some(ResourceType::Group),
            4 => Some(ResourceType::Cluster),
            5 => Some(ResourceType::TransactionalId),
            _ => None,
        }
    }

    /// Convert to Kafka protocol resource type code
    pub fn to_code(self) -> i8 {
        match self {
            ResourceType::Any => 0,
            ResourceType::Topic => 2,
            ResourceType::Group => 3,
            ResourceType::Cluster => 4,
            ResourceType::TransactionalId => 5,
        }
    }

    /// Parse from string representation
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "topic" => Some(ResourceType::Topic),
            "group" => Some(ResourceType::Group),
            "cluster" => Some(ResourceType::Cluster),
            "transactionalid" | "transactional_id" => Some(ResourceType::TransactionalId),
            "any" | "*" => Some(ResourceType::Any),
            _ => None,
        }
    }
}

impl std::fmt::Display for ResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceType::Topic => write!(f, "Topic"),
            ResourceType::Group => write!(f, "Group"),
            ResourceType::Cluster => write!(f, "Cluster"),
            ResourceType::TransactionalId => write!(f, "TransactionalId"),
            ResourceType::Any => write!(f, "Any"),
        }
    }
}

/// Pattern type for resource name matching
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PatternType {
    /// Exact literal match
    #[default]
    Literal,
    /// Prefix match (resource name starts with pattern)
    Prefixed,
    /// Match any resource (wildcard)
    Match,
}

impl PatternType {
    /// Convert from Kafka protocol pattern type code
    pub fn from_code(code: i8) -> Option<Self> {
        match code {
            0 => Some(PatternType::Match),    // ANY
            1 => Some(PatternType::Match),    // MATCH
            2 => Some(PatternType::Literal),  // LITERAL
            3 => Some(PatternType::Prefixed), // PREFIXED
            _ => None,
        }
    }

    /// Convert to Kafka protocol pattern type code
    pub fn to_code(self) -> i8 {
        match self {
            PatternType::Match => 1,
            PatternType::Literal => 3,
            PatternType::Prefixed => 4,
        }
    }

    /// Parse from string representation
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "literal" => Some(PatternType::Literal),
            "prefixed" | "prefix" => Some(PatternType::Prefixed),
            "match" | "any" | "*" => Some(PatternType::Match),
            _ => None,
        }
    }
}

impl std::fmt::Display for PatternType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PatternType::Literal => write!(f, "Literal"),
            PatternType::Prefixed => write!(f, "Prefixed"),
            PatternType::Match => write!(f, "Match"),
        }
    }
}

/// Operations that can be performed on resources
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    /// Any operation (matches all)
    All,
    /// Read operation (Fetch, ListOffsets)
    Read,
    /// Write operation (Produce)
    Write,
    /// Create operation (CreateTopics)
    Create,
    /// Delete operation (DeleteTopics, DeleteGroups)
    Delete,
    /// Alter operation (AlterConfigs)
    Alter,
    /// Describe operation (Metadata, DescribeGroups)
    Describe,
    /// Cluster action operation (internal cluster operations)
    ClusterAction,
    /// Describe configs operation
    DescribeConfigs,
    /// Alter configs operation
    AlterConfigs,
    /// Idempotent write operation
    IdempotentWrite,
}

impl Operation {
    /// Convert from Kafka protocol operation code
    pub fn from_code(code: i8) -> Option<Self> {
        match code {
            0 => Some(Operation::All), // UNKNOWN maps to All
            1 => Some(Operation::All), // ANY
            2 => Some(Operation::All), // ALL
            3 => Some(Operation::Read),
            4 => Some(Operation::Write),
            5 => Some(Operation::Create),
            6 => Some(Operation::Delete),
            7 => Some(Operation::Alter),
            8 => Some(Operation::Describe),
            9 => Some(Operation::ClusterAction),
            10 => Some(Operation::DescribeConfigs),
            11 => Some(Operation::AlterConfigs),
            12 => Some(Operation::IdempotentWrite),
            _ => None,
        }
    }

    /// Convert to Kafka protocol operation code
    pub fn to_code(self) -> i8 {
        match self {
            Operation::All => 2,
            Operation::Read => 3,
            Operation::Write => 4,
            Operation::Create => 5,
            Operation::Delete => 6,
            Operation::Alter => 7,
            Operation::Describe => 8,
            Operation::ClusterAction => 9,
            Operation::DescribeConfigs => 10,
            Operation::AlterConfigs => 11,
            Operation::IdempotentWrite => 12,
        }
    }

    /// Parse from string representation
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "all" | "*" => Some(Operation::All),
            "read" => Some(Operation::Read),
            "write" => Some(Operation::Write),
            "create" => Some(Operation::Create),
            "delete" => Some(Operation::Delete),
            "alter" => Some(Operation::Alter),
            "describe" => Some(Operation::Describe),
            "clusteraction" | "cluster_action" => Some(Operation::ClusterAction),
            "describeconfigs" | "describe_configs" => Some(Operation::DescribeConfigs),
            "alterconfigs" | "alter_configs" => Some(Operation::AlterConfigs),
            "idempotentwrite" | "idempotent_write" => Some(Operation::IdempotentWrite),
            _ => None,
        }
    }
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::All => write!(f, "All"),
            Operation::Read => write!(f, "Read"),
            Operation::Write => write!(f, "Write"),
            Operation::Create => write!(f, "Create"),
            Operation::Delete => write!(f, "Delete"),
            Operation::Alter => write!(f, "Alter"),
            Operation::Describe => write!(f, "Describe"),
            Operation::ClusterAction => write!(f, "ClusterAction"),
            Operation::DescribeConfigs => write!(f, "DescribeConfigs"),
            Operation::AlterConfigs => write!(f, "AlterConfigs"),
            Operation::IdempotentWrite => write!(f, "IdempotentWrite"),
        }
    }
}

/// Permission type (Allow or Deny)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Permission {
    /// Deny the operation
    Deny,
    /// Allow the operation
    #[default]
    Allow,
}

impl Permission {
    /// Convert from Kafka protocol permission type code
    pub fn from_code(code: i8) -> Option<Self> {
        match code {
            0 => Some(Permission::Allow), // UNKNOWN maps to Allow
            1 => Some(Permission::Allow), // ANY
            2 => Some(Permission::Deny),
            3 => Some(Permission::Allow),
            _ => None,
        }
    }

    /// Convert to Kafka protocol permission type code
    pub fn to_code(self) -> i8 {
        match self {
            Permission::Deny => 2,
            Permission::Allow => 3,
        }
    }

    /// Parse from string representation
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "allow" => Some(Permission::Allow),
            "deny" => Some(Permission::Deny),
            _ => None,
        }
    }
}

impl std::fmt::Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Permission::Allow => write!(f, "Allow"),
            Permission::Deny => write!(f, "Deny"),
        }
    }
}

/// An Access Control List entry
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Acl {
    /// Type of resource this ACL applies to
    pub resource_type: ResourceType,

    /// Name of the resource (topic name, group id, or "*" for all)
    pub resource_name: String,

    /// Pattern type for matching resource names
    #[serde(default)]
    pub pattern_type: PatternType,

    /// Principal (e.g., "User:alice" or "User:*")
    pub principal: String,

    /// Host pattern ("*" for any, or specific IP/hostname)
    #[serde(default = "default_host")]
    pub host: String,

    /// Operation this ACL controls
    pub operation: Operation,

    /// Permission type (Allow or Deny)
    #[serde(default)]
    pub permission: Permission,
}

fn default_host() -> String {
    "*".to_string()
}

impl Acl {
    /// Create a new ACL entry
    pub fn new(
        resource_type: ResourceType,
        resource_name: &str,
        pattern_type: PatternType,
        principal: &str,
        host: &str,
        operation: Operation,
        permission: Permission,
    ) -> Self {
        Self {
            resource_type,
            resource_name: resource_name.to_string(),
            pattern_type,
            principal: principal.to_string(),
            host: host.to_string(),
            operation,
            permission,
        }
    }

    /// Check if this ACL matches the given criteria
    pub fn matches(
        &self,
        resource_type: ResourceType,
        resource_name: &str,
        principal: &str,
        host: &str,
        operation: Operation,
    ) -> bool {
        // Check resource type
        if self.resource_type != ResourceType::Any && self.resource_type != resource_type {
            return false;
        }

        // Check resource name based on pattern type
        if !self.matches_resource_name(resource_name) {
            return false;
        }

        // Check principal
        if !self.matches_principal(principal) {
            return false;
        }

        // Check host
        if !self.matches_host(host) {
            return false;
        }

        // Check operation
        if self.operation != Operation::All && self.operation != operation {
            return false;
        }

        true
    }

    /// Check if the resource name matches based on pattern type
    fn matches_resource_name(&self, resource_name: &str) -> bool {
        match self.pattern_type {
            PatternType::Literal => {
                self.resource_name == "*" || self.resource_name == resource_name
            }
            PatternType::Prefixed => {
                self.resource_name == "*" || resource_name.starts_with(&self.resource_name)
            }
            PatternType::Match => true,
        }
    }

    /// Check if the principal matches
    fn matches_principal(&self, principal: &str) -> bool {
        if self.principal == "User:*" || self.principal == "*" {
            return true;
        }
        self.principal == principal
    }

    /// Check if the host matches
    fn matches_host(&self, host: &str) -> bool {
        if self.host == "*" {
            return true;
        }
        self.host == host
    }
}

impl std::fmt::Display for Acl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "(principal={}, host={}, operation={}, permission={}) on {}:{}:{}",
            self.principal,
            self.host,
            self.operation,
            self.permission,
            self.resource_type,
            self.pattern_type,
            self.resource_name
        )
    }
}

/// ACL file format for YAML persistence
#[derive(Debug, Serialize, Deserialize)]
struct AclFile {
    acls: Vec<Acl>,
}

/// ACL store for managing and persisting ACLs
#[derive(Debug, Clone)]
pub struct AclStore {
    /// All ACLs indexed by resource type for efficient lookup
    acls: Vec<Acl>,

    /// Index by resource type for faster matching
    by_resource_type: HashMap<ResourceType, Vec<usize>>,
}

impl AclStore {
    /// Create a new empty ACL store
    pub fn new() -> Self {
        Self {
            acls: Vec::new(),
            by_resource_type: HashMap::new(),
        }
    }

    /// Load ACLs from a YAML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref())
            .map_err(|e| StreamlineError::Config(format!("Failed to read ACL file: {}", e)))?;

        let acl_file: AclFile = serde_yaml::from_str(&content)
            .map_err(|e| StreamlineError::Config(format!("Failed to parse ACL file: {}", e)))?;

        let mut store = Self::new();
        for acl in acl_file.acls {
            store.add_acl(acl);
        }

        Ok(store)
    }

    /// Save ACLs to a YAML file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let acl_file = AclFile {
            acls: self.acls.clone(),
        };

        let content = serde_yaml::to_string(&acl_file)
            .map_err(|e| StreamlineError::Config(format!("Failed to serialize ACLs: {}", e)))?;

        // Create parent directory if it doesn't exist
        if let Some(parent) = path.as_ref().parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).map_err(|e| {
                    StreamlineError::Config(format!("Failed to create ACL directory: {}", e))
                })?;
            }
        }

        fs::write(path.as_ref(), content)
            .map_err(|e| StreamlineError::Config(format!("Failed to write ACL file: {}", e)))?;

        Ok(())
    }

    /// Add an ACL to the store
    pub fn add_acl(&mut self, acl: Acl) {
        let index = self.acls.len();
        let resource_type = acl.resource_type;
        self.acls.push(acl);

        // Update index
        self.by_resource_type
            .entry(resource_type)
            .or_default()
            .push(index);

        // Also add to Any type index if not already Any
        if resource_type != ResourceType::Any {
            self.by_resource_type
                .entry(ResourceType::Any)
                .or_default()
                .push(index);
        }
    }

    /// Remove ACLs matching the given filter
    /// Returns the number of ACLs removed
    pub fn remove_acls(&mut self, filter: &AclFilter) -> usize {
        let original_len = self.acls.len();

        // Filter out matching ACLs
        self.acls.retain(|acl| !filter.matches(acl));

        let removed = original_len - self.acls.len();

        // Rebuild index if any were removed
        if removed > 0 {
            self.rebuild_index();
        }

        removed
    }

    /// Rebuild the resource type index
    fn rebuild_index(&mut self) {
        self.by_resource_type.clear();
        for (index, acl) in self.acls.iter().enumerate() {
            self.by_resource_type
                .entry(acl.resource_type)
                .or_default()
                .push(index);

            if acl.resource_type != ResourceType::Any {
                self.by_resource_type
                    .entry(ResourceType::Any)
                    .or_default()
                    .push(index);
            }
        }
    }

    /// Find all ACLs matching the given filter
    pub fn find_acls(&self, filter: &AclFilter) -> Vec<&Acl> {
        self.acls.iter().filter(|acl| filter.matches(acl)).collect()
    }

    /// Get all ACLs
    pub fn list_acls(&self) -> &[Acl] {
        &self.acls
    }

    /// Get ACLs for a specific resource type
    pub fn get_acls_for_resource_type(&self, resource_type: ResourceType) -> Vec<&Acl> {
        self.by_resource_type
            .get(&resource_type)
            .map(|indices| indices.iter().map(|&i| &self.acls[i]).collect())
            .unwrap_or_default()
    }

    /// Check if there are any ACLs defined
    pub fn is_empty(&self) -> bool {
        self.acls.is_empty()
    }

    /// Get the number of ACLs
    pub fn len(&self) -> usize {
        self.acls.len()
    }
}

impl Default for AclStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Filter for querying or deleting ACLs
#[derive(Debug, Clone, Default)]
pub struct AclFilter {
    /// Resource type filter (None matches any)
    pub resource_type: Option<ResourceType>,

    /// Resource name filter (None matches any)
    pub resource_name: Option<String>,

    /// Pattern type filter (None matches any)
    pub pattern_type: Option<PatternType>,

    /// Principal filter (None matches any)
    pub principal: Option<String>,

    /// Host filter (None matches any)
    pub host: Option<String>,

    /// Operation filter (None matches any)
    pub operation: Option<Operation>,

    /// Permission filter (None matches any)
    pub permission: Option<Permission>,
}

impl AclFilter {
    /// Create a new filter with default values (matches all)
    pub fn new() -> Self {
        Self::default()
    }

    /// Set resource type filter
    pub fn with_resource_type(mut self, resource_type: ResourceType) -> Self {
        self.resource_type = Some(resource_type);
        self
    }

    /// Set resource name filter
    pub fn with_resource_name(mut self, name: &str) -> Self {
        self.resource_name = Some(name.to_string());
        self
    }

    /// Set pattern type filter
    pub fn with_pattern_type(mut self, pattern_type: PatternType) -> Self {
        self.pattern_type = Some(pattern_type);
        self
    }

    /// Set principal filter
    pub fn with_principal(mut self, principal: &str) -> Self {
        self.principal = Some(principal.to_string());
        self
    }

    /// Set host filter
    pub fn with_host(mut self, host: &str) -> Self {
        self.host = Some(host.to_string());
        self
    }

    /// Set operation filter
    pub fn with_operation(mut self, operation: Operation) -> Self {
        self.operation = Some(operation);
        self
    }

    /// Set permission filter
    pub fn with_permission(mut self, permission: Permission) -> Self {
        self.permission = Some(permission);
        self
    }

    /// Check if this filter matches the given ACL
    pub fn matches(&self, acl: &Acl) -> bool {
        // Check resource type
        if let Some(rt) = self.resource_type {
            if rt != ResourceType::Any && acl.resource_type != rt {
                return false;
            }
        }

        // Check resource name
        if let Some(ref name) = self.resource_name {
            if name != "*" && acl.resource_name != *name {
                return false;
            }
        }

        // Check pattern type
        if let Some(pt) = self.pattern_type {
            if acl.pattern_type != pt {
                return false;
            }
        }

        // Check principal
        if let Some(ref principal) = self.principal {
            if principal != "*" && principal != "User:*" && acl.principal != *principal {
                return false;
            }
        }

        // Check host
        if let Some(ref host) = self.host {
            if host != "*" && acl.host != *host {
                return false;
            }
        }

        // Check operation
        if let Some(op) = self.operation {
            if op != Operation::All && acl.operation != op {
                return false;
            }
        }

        // Check permission
        if let Some(perm) = self.permission {
            if acl.permission != perm {
                return false;
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_resource_type_conversions() {
        assert_eq!(ResourceType::from_code(2), Some(ResourceType::Topic));
        assert_eq!(ResourceType::from_code(3), Some(ResourceType::Group));
        assert_eq!(ResourceType::Topic.to_code(), 2);
        assert_eq!(ResourceType::Group.to_code(), 3);

        assert_eq!(ResourceType::parse("topic"), Some(ResourceType::Topic));
        assert_eq!(ResourceType::parse("GROUP"), Some(ResourceType::Group));
    }

    #[test]
    fn test_operation_conversions() {
        assert_eq!(Operation::from_code(3), Some(Operation::Read));
        assert_eq!(Operation::from_code(4), Some(Operation::Write));
        assert_eq!(Operation::Read.to_code(), 3);
        assert_eq!(Operation::Write.to_code(), 4);

        assert_eq!(Operation::parse("read"), Some(Operation::Read));
        assert_eq!(Operation::parse("WRITE"), Some(Operation::Write));
    }

    #[test]
    fn test_acl_literal_matching() {
        let acl = Acl::new(
            ResourceType::Topic,
            "orders",
            PatternType::Literal,
            "User:alice",
            "*",
            Operation::Read,
            Permission::Allow,
        );

        // Should match exact topic name
        assert!(acl.matches(
            ResourceType::Topic,
            "orders",
            "User:alice",
            "127.0.0.1",
            Operation::Read
        ));

        // Should not match different topic
        assert!(!acl.matches(
            ResourceType::Topic,
            "events",
            "User:alice",
            "127.0.0.1",
            Operation::Read
        ));

        // Should not match different user
        assert!(!acl.matches(
            ResourceType::Topic,
            "orders",
            "User:bob",
            "127.0.0.1",
            Operation::Read
        ));

        // Should not match different operation
        assert!(!acl.matches(
            ResourceType::Topic,
            "orders",
            "User:alice",
            "127.0.0.1",
            Operation::Write
        ));
    }

    #[test]
    fn test_acl_prefixed_matching() {
        let acl = Acl::new(
            ResourceType::Topic,
            "events-",
            PatternType::Prefixed,
            "User:producer",
            "*",
            Operation::Write,
            Permission::Allow,
        );

        // Should match topics with prefix
        assert!(acl.matches(
            ResourceType::Topic,
            "events-orders",
            "User:producer",
            "127.0.0.1",
            Operation::Write
        ));
        assert!(acl.matches(
            ResourceType::Topic,
            "events-users",
            "User:producer",
            "127.0.0.1",
            Operation::Write
        ));

        // Should not match topics without prefix
        assert!(!acl.matches(
            ResourceType::Topic,
            "orders-events",
            "User:producer",
            "127.0.0.1",
            Operation::Write
        ));
    }

    #[test]
    fn test_acl_wildcard_principal() {
        let acl = Acl::new(
            ResourceType::Topic,
            "public",
            PatternType::Literal,
            "User:*",
            "*",
            Operation::Read,
            Permission::Allow,
        );

        // Should match any user
        assert!(acl.matches(
            ResourceType::Topic,
            "public",
            "User:alice",
            "127.0.0.1",
            Operation::Read
        ));
        assert!(acl.matches(
            ResourceType::Topic,
            "public",
            "User:bob",
            "127.0.0.1",
            Operation::Read
        ));
    }

    #[test]
    fn test_acl_all_operation() {
        let acl = Acl::new(
            ResourceType::Topic,
            "admin-topic",
            PatternType::Literal,
            "User:admin",
            "*",
            Operation::All,
            Permission::Allow,
        );

        // Should match any operation
        assert!(acl.matches(
            ResourceType::Topic,
            "admin-topic",
            "User:admin",
            "127.0.0.1",
            Operation::Read
        ));
        assert!(acl.matches(
            ResourceType::Topic,
            "admin-topic",
            "User:admin",
            "127.0.0.1",
            Operation::Write
        ));
        assert!(acl.matches(
            ResourceType::Topic,
            "admin-topic",
            "User:admin",
            "127.0.0.1",
            Operation::Delete
        ));
    }

    #[test]
    fn test_acl_store_add_and_find() {
        let mut store = AclStore::new();

        store.add_acl(Acl::new(
            ResourceType::Topic,
            "orders",
            PatternType::Literal,
            "User:alice",
            "*",
            Operation::Read,
            Permission::Allow,
        ));

        store.add_acl(Acl::new(
            ResourceType::Topic,
            "orders",
            PatternType::Literal,
            "User:alice",
            "*",
            Operation::Write,
            Permission::Allow,
        ));

        store.add_acl(Acl::new(
            ResourceType::Group,
            "my-group",
            PatternType::Literal,
            "User:alice",
            "*",
            Operation::Read,
            Permission::Allow,
        ));

        assert_eq!(store.len(), 3);

        // Find by principal
        let filter = AclFilter::new().with_principal("User:alice");
        let acls = store.find_acls(&filter);
        assert_eq!(acls.len(), 3);

        // Find by resource type
        let filter = AclFilter::new().with_resource_type(ResourceType::Topic);
        let acls = store.find_acls(&filter);
        assert_eq!(acls.len(), 2);

        // Find by operation
        let filter = AclFilter::new().with_operation(Operation::Read);
        let acls = store.find_acls(&filter);
        assert_eq!(acls.len(), 2);
    }

    #[test]
    fn test_acl_store_remove() {
        let mut store = AclStore::new();

        store.add_acl(Acl::new(
            ResourceType::Topic,
            "orders",
            PatternType::Literal,
            "User:alice",
            "*",
            Operation::Read,
            Permission::Allow,
        ));

        store.add_acl(Acl::new(
            ResourceType::Topic,
            "orders",
            PatternType::Literal,
            "User:bob",
            "*",
            Operation::Read,
            Permission::Allow,
        ));

        assert_eq!(store.len(), 2);

        // Remove ACLs for alice
        let filter = AclFilter::new().with_principal("User:alice");
        let removed = store.remove_acls(&filter);
        assert_eq!(removed, 1);
        assert_eq!(store.len(), 1);

        // Verify remaining ACL is for bob
        let remaining = store.list_acls();
        assert_eq!(remaining[0].principal, "User:bob");
    }

    #[test]
    fn test_acl_store_file_io() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut store = AclStore::new();

        store.add_acl(Acl::new(
            ResourceType::Topic,
            "*",
            PatternType::Literal,
            "User:admin",
            "*",
            Operation::All,
            Permission::Allow,
        ));

        store.add_acl(Acl::new(
            ResourceType::Topic,
            "events-",
            PatternType::Prefixed,
            "User:producer",
            "*",
            Operation::Write,
            Permission::Allow,
        ));

        // Save and reload
        store.save_to_file(temp_file.path()).unwrap();

        let loaded_store = AclStore::from_file(temp_file.path()).unwrap();
        assert_eq!(loaded_store.len(), 2);

        let acls = loaded_store.list_acls();
        assert!(acls.iter().any(|a| a.principal == "User:admin"));
        assert!(acls.iter().any(|a| a.principal == "User:producer"));
    }

    #[test]
    fn test_acl_filter() {
        let acl = Acl::new(
            ResourceType::Topic,
            "orders",
            PatternType::Literal,
            "User:alice",
            "*",
            Operation::Read,
            Permission::Allow,
        );

        // Empty filter matches everything
        let filter = AclFilter::new();
        assert!(filter.matches(&acl));

        // Specific filters
        let filter = AclFilter::new()
            .with_resource_type(ResourceType::Topic)
            .with_principal("User:alice");
        assert!(filter.matches(&acl));

        // Non-matching filter
        let filter = AclFilter::new().with_principal("User:bob");
        assert!(!filter.matches(&acl));

        // Operation filter
        let filter = AclFilter::new().with_operation(Operation::Write);
        assert!(!filter.matches(&acl));
    }

    #[test]
    fn test_acl_display() {
        let acl = Acl::new(
            ResourceType::Topic,
            "orders",
            PatternType::Literal,
            "User:alice",
            "*",
            Operation::Read,
            Permission::Allow,
        );

        let display = format!("{}", acl);
        assert!(display.contains("User:alice"));
        assert!(display.contains("Read"));
        assert!(display.contains("Topic"));
        assert!(display.contains("orders"));
    }
}
