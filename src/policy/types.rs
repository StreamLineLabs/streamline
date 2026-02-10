//! Policy type definitions for Streamline's Policy-as-Code framework

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Version of the policy schema
pub const POLICY_SCHEMA_VERSION: &str = "1.0";

/// A complete policy document containing all governance rules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyDocument {
    /// Schema version for compatibility checking
    #[serde(default = "default_version")]
    pub version: String,

    /// Document metadata
    #[serde(default)]
    pub metadata: PolicyMetadata,

    /// Topic policies
    #[serde(default)]
    pub topics: Vec<TopicPolicy>,

    /// ACL policies
    #[serde(default)]
    pub acls: Vec<AclPolicy>,

    /// Quota policies
    #[serde(default)]
    pub quotas: Vec<QuotaPolicy>,

    /// Schema policies
    #[serde(default)]
    pub schemas: Vec<SchemaPolicy>,

    /// Retention policies (can be referenced by topics)
    #[serde(default)]
    pub retention_policies: HashMap<String, RetentionPolicy>,

    /// Labels for filtering and organization
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

fn default_version() -> String {
    POLICY_SCHEMA_VERSION.to_string()
}

impl Default for PolicyDocument {
    fn default() -> Self {
        Self {
            version: default_version(),
            metadata: PolicyMetadata::default(),
            topics: Vec::new(),
            acls: Vec::new(),
            quotas: Vec::new(),
            schemas: Vec::new(),
            retention_policies: HashMap::new(),
            labels: HashMap::new(),
        }
    }
}

impl PolicyDocument {
    /// Create a new empty policy document
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a policy document with metadata
    pub fn with_metadata(name: impl Into<String>, environment: impl Into<String>) -> Self {
        Self {
            metadata: PolicyMetadata {
                name: name.into(),
                environment: Some(environment.into()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Add a topic policy
    pub fn add_topic(&mut self, policy: TopicPolicy) {
        self.topics.push(policy);
    }

    /// Add an ACL policy
    pub fn add_acl(&mut self, policy: AclPolicy) {
        self.acls.push(policy);
    }

    /// Add a quota policy
    pub fn add_quota(&mut self, policy: QuotaPolicy) {
        self.quotas.push(policy);
    }

    /// Add a schema policy
    pub fn add_schema(&mut self, policy: SchemaPolicy) {
        self.schemas.push(policy);
    }

    /// Merge another policy document into this one
    pub fn merge(&mut self, other: PolicyDocument) {
        self.topics.extend(other.topics);
        self.acls.extend(other.acls);
        self.quotas.extend(other.quotas);
        self.schemas.extend(other.schemas);
        self.retention_policies.extend(other.retention_policies);
        self.labels.extend(other.labels);
    }

    /// Get all topic names defined in this document
    pub fn topic_names(&self) -> Vec<&str> {
        self.topics.iter().map(|t| t.name.as_str()).collect()
    }

    /// Find a topic policy by name
    pub fn find_topic(&self, name: &str) -> Option<&TopicPolicy> {
        self.topics.iter().find(|t| t.name == name)
    }

    /// Find ACL policies for a resource
    pub fn find_acls_for_resource(
        &self,
        resource_type: &str,
        resource_name: &str,
    ) -> Vec<&AclPolicy> {
        self.acls
            .iter()
            .filter(|a| {
                a.resource_type.eq_ignore_ascii_case(resource_type)
                    && (a.resource_name == resource_name || a.resource_name == "*")
            })
            .collect()
    }
}

/// Metadata about a policy document
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PolicyMetadata {
    /// Policy name/identifier
    #[serde(default)]
    pub name: String,

    /// Description of the policy
    #[serde(default)]
    pub description: Option<String>,

    /// Environment (e.g., "production", "staging", "development")
    #[serde(default)]
    pub environment: Option<String>,

    /// Owner/team responsible for this policy
    #[serde(default)]
    pub owner: Option<String>,

    /// Source control reference (e.g., git commit hash)
    #[serde(default)]
    pub source_ref: Option<String>,

    /// When this policy was last updated
    #[serde(default)]
    pub last_updated: Option<String>,

    /// Annotations for tooling
    #[serde(default)]
    pub annotations: HashMap<String, String>,
}

/// Policy for a topic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicPolicy {
    /// Topic name (supports patterns with * wildcard)
    pub name: String,

    /// Number of partitions
    #[serde(default = "default_partitions")]
    pub partitions: i32,

    /// Replication factor
    #[serde(default = "default_replication_factor")]
    pub replication_factor: i16,

    /// Retention settings
    #[serde(default)]
    pub retention: RetentionPolicy,

    /// Additional topic configuration
    #[serde(default)]
    pub config: HashMap<String, String>,

    /// Whether this topic should be auto-created if it doesn't exist
    #[serde(default)]
    pub auto_create: bool,

    /// Labels for organization
    #[serde(default)]
    pub labels: HashMap<String, String>,

    /// Reference to a named retention policy
    #[serde(default)]
    pub retention_policy_ref: Option<String>,

    /// Storage mode (local, hybrid, diskless)
    #[serde(default)]
    pub storage_mode: Option<String>,

    /// Compression type
    #[serde(default)]
    pub compression: Option<String>,
}

fn default_partitions() -> i32 {
    1
}

fn default_replication_factor() -> i16 {
    1
}

impl Default for TopicPolicy {
    fn default() -> Self {
        Self {
            name: String::new(),
            partitions: default_partitions(),
            replication_factor: default_replication_factor(),
            retention: RetentionPolicy::default(),
            config: HashMap::new(),
            auto_create: false,
            labels: HashMap::new(),
            retention_policy_ref: None,
            storage_mode: None,
            compression: None,
        }
    }
}

impl TopicPolicy {
    /// Create a new topic policy
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Set partitions
    pub fn with_partitions(mut self, partitions: i32) -> Self {
        self.partitions = partitions;
        self
    }

    /// Set replication factor
    pub fn with_replication(mut self, factor: i16) -> Self {
        self.replication_factor = factor;
        self
    }

    /// Set retention
    pub fn with_retention(mut self, retention: RetentionPolicy) -> Self {
        self.retention = retention;
        self
    }

    /// Add a config entry
    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.insert(key.into(), value.into());
        self
    }

    /// Check if this policy matches a topic name (supports wildcards)
    pub fn matches(&self, topic_name: &str) -> bool {
        if self.name.contains('*') {
            let pattern = self.name.replace('*', ".*");
            regex::Regex::new(&format!("^{}$", pattern))
                .map(|r| r.is_match(topic_name))
                .unwrap_or(false)
        } else {
            self.name == topic_name
        }
    }
}

/// Retention policy settings
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetentionPolicy {
    /// Retention time in milliseconds (-1 for unlimited)
    #[serde(default = "default_retention_ms")]
    pub ms: i64,

    /// Retention size in bytes (-1 for unlimited)
    #[serde(default = "default_retention_bytes")]
    pub bytes: i64,

    /// Segment size in bytes
    #[serde(default)]
    pub segment_bytes: Option<i64>,

    /// Segment time in milliseconds
    #[serde(default)]
    pub segment_ms: Option<i64>,

    /// Cleanup policy (delete, compact, delete,compact)
    #[serde(default = "default_cleanup_policy")]
    pub cleanup_policy: String,

    /// Min compaction lag in milliseconds
    #[serde(default)]
    pub min_compaction_lag_ms: Option<i64>,

    /// Delete retention in milliseconds (for compacted topics)
    #[serde(default)]
    pub delete_retention_ms: Option<i64>,
}

fn default_retention_ms() -> i64 {
    604_800_000 // 7 days
}

fn default_retention_bytes() -> i64 {
    -1 // unlimited
}

fn default_cleanup_policy() -> String {
    "delete".to_string()
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            ms: default_retention_ms(),
            bytes: default_retention_bytes(),
            segment_bytes: None,
            segment_ms: None,
            cleanup_policy: default_cleanup_policy(),
            min_compaction_lag_ms: None,
            delete_retention_ms: None,
        }
    }
}

impl RetentionPolicy {
    /// Create a retention policy with time-based retention
    pub fn with_time(ms: i64) -> Self {
        Self {
            ms,
            ..Default::default()
        }
    }

    /// Create a retention policy with size-based retention
    pub fn with_size(bytes: i64) -> Self {
        Self {
            bytes,
            ..Default::default()
        }
    }

    /// Create a retention policy for compacted topics
    pub fn compacted() -> Self {
        Self {
            cleanup_policy: "compact".to_string(),
            ..Default::default()
        }
    }
}

/// ACL policy definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclPolicy {
    /// Principal (e.g., "User:alice", "Group:admins")
    pub principal: String,

    /// Resource type (topic, group, cluster, transactional_id)
    pub resource_type: String,

    /// Resource name (supports patterns with * wildcard)
    pub resource_name: String,

    /// Pattern type (literal, prefixed)
    #[serde(default = "default_pattern_type")]
    pub pattern_type: String,

    /// Operations allowed/denied
    pub operations: Vec<String>,

    /// Permission (allow, deny)
    #[serde(default = "default_permission")]
    pub permission: String,

    /// Host restriction (optional, * for any)
    #[serde(default = "default_host")]
    pub host: String,

    /// Description of why this ACL exists
    #[serde(default)]
    pub description: Option<String>,

    /// Labels for organization
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

fn default_pattern_type() -> String {
    "literal".to_string()
}

fn default_permission() -> String {
    "allow".to_string()
}

fn default_host() -> String {
    "*".to_string()
}

impl AclPolicy {
    /// Create a new ACL policy
    pub fn new(
        principal: impl Into<String>,
        resource_type: impl Into<String>,
        resource_name: impl Into<String>,
    ) -> Self {
        Self {
            principal: principal.into(),
            resource_type: resource_type.into(),
            resource_name: resource_name.into(),
            pattern_type: default_pattern_type(),
            operations: Vec::new(),
            permission: default_permission(),
            host: default_host(),
            description: None,
            labels: HashMap::new(),
        }
    }

    /// Add allowed operations
    pub fn with_operations(mut self, ops: Vec<&str>) -> Self {
        self.operations = ops.into_iter().map(|s| s.to_string()).collect();
        self
    }

    /// Set permission to deny
    pub fn deny(mut self) -> Self {
        self.permission = "deny".to_string();
        self
    }

    /// Set pattern type to prefixed
    pub fn prefixed(mut self) -> Self {
        self.pattern_type = "prefixed".to_string();
        self
    }
}

/// Quota policy definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaPolicy {
    /// Entity type (user, client-id, ip)
    #[serde(default = "default_entity_type")]
    pub entity_type: String,

    /// Entity name (supports * for default)
    pub entity: String,

    /// Producer byte rate limit per second
    #[serde(default)]
    pub producer_byte_rate: Option<i64>,

    /// Consumer byte rate limit per second
    #[serde(default)]
    pub consumer_byte_rate: Option<i64>,

    /// Request percentage limit (0-100)
    #[serde(default)]
    pub request_percentage: Option<f64>,

    /// Controller mutation rate limit
    #[serde(default)]
    pub controller_mutation_rate: Option<f64>,

    /// Description
    #[serde(default)]
    pub description: Option<String>,

    /// Labels for organization
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

fn default_entity_type() -> String {
    "user".to_string()
}

impl QuotaPolicy {
    /// Create a new quota policy for a user
    pub fn for_user(user: impl Into<String>) -> Self {
        Self {
            entity_type: "user".to_string(),
            entity: user.into(),
            producer_byte_rate: None,
            consumer_byte_rate: None,
            request_percentage: None,
            controller_mutation_rate: None,
            description: None,
            labels: HashMap::new(),
        }
    }

    /// Create a new quota policy for a client ID
    pub fn for_client_id(client_id: impl Into<String>) -> Self {
        Self {
            entity_type: "client-id".to_string(),
            entity: client_id.into(),
            producer_byte_rate: None,
            consumer_byte_rate: None,
            request_percentage: None,
            controller_mutation_rate: None,
            description: None,
            labels: HashMap::new(),
        }
    }

    /// Set producer byte rate limit
    pub fn with_producer_rate(mut self, rate: i64) -> Self {
        self.producer_byte_rate = Some(rate);
        self
    }

    /// Set consumer byte rate limit
    pub fn with_consumer_rate(mut self, rate: i64) -> Self {
        self.consumer_byte_rate = Some(rate);
        self
    }
}

/// Schema policy definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaPolicy {
    /// Subject name (typically topic name + "-key" or "-value")
    pub subject: String,

    /// Schema type (avro, protobuf, json)
    pub schema_type: String,

    /// Compatibility level (backward, forward, full, none)
    #[serde(default = "default_compatibility")]
    pub compatibility: String,

    /// Schema definition (inline)
    #[serde(default)]
    pub schema: Option<String>,

    /// Path to schema file (alternative to inline)
    #[serde(default)]
    pub schema_file: Option<String>,

    /// Whether this schema is required for the topic
    #[serde(default)]
    pub required: bool,

    /// Description
    #[serde(default)]
    pub description: Option<String>,

    /// Labels for organization
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

fn default_compatibility() -> String {
    "backward".to_string()
}

impl SchemaPolicy {
    /// Create a new schema policy for a subject
    pub fn new(subject: impl Into<String>, schema_type: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            schema_type: schema_type.into(),
            compatibility: default_compatibility(),
            schema: None,
            schema_file: None,
            required: false,
            description: None,
            labels: HashMap::new(),
        }
    }

    /// Set inline schema
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set schema file path
    pub fn with_file(mut self, path: impl Into<String>) -> Self {
        self.schema_file = Some(path.into());
        self
    }

    /// Set compatibility level
    pub fn with_compatibility(mut self, level: impl Into<String>) -> Self {
        self.compatibility = level.into();
        self
    }

    /// Mark as required
    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_document_default() {
        let doc = PolicyDocument::default();
        assert_eq!(doc.version, POLICY_SCHEMA_VERSION);
        assert!(doc.topics.is_empty());
        assert!(doc.acls.is_empty());
        assert!(doc.quotas.is_empty());
    }

    #[test]
    fn test_policy_document_with_metadata() {
        let doc = PolicyDocument::with_metadata("test-policy", "production");
        assert_eq!(doc.metadata.name, "test-policy");
        assert_eq!(doc.metadata.environment, Some("production".to_string()));
    }

    #[test]
    fn test_topic_policy_builder() {
        let policy = TopicPolicy::new("events")
            .with_partitions(12)
            .with_replication(3)
            .with_config("compression.type", "lz4");

        assert_eq!(policy.name, "events");
        assert_eq!(policy.partitions, 12);
        assert_eq!(policy.replication_factor, 3);
        assert_eq!(
            policy.config.get("compression.type"),
            Some(&"lz4".to_string())
        );
    }

    #[test]
    fn test_topic_policy_wildcard_matching() {
        let policy = TopicPolicy::new("events.*");
        assert!(policy.matches("events.clicks"));
        assert!(policy.matches("events.views"));
        assert!(!policy.matches("logs.errors"));

        let exact_policy = TopicPolicy::new("events");
        assert!(exact_policy.matches("events"));
        assert!(!exact_policy.matches("events.clicks"));
    }

    #[test]
    fn test_retention_policy() {
        let time_based = RetentionPolicy::with_time(86_400_000);
        assert_eq!(time_based.ms, 86_400_000);

        let size_based = RetentionPolicy::with_size(1_073_741_824);
        assert_eq!(size_based.bytes, 1_073_741_824);

        let compacted = RetentionPolicy::compacted();
        assert_eq!(compacted.cleanup_policy, "compact");
    }

    #[test]
    fn test_acl_policy_builder() {
        let policy = AclPolicy::new("User:alice", "topic", "events")
            .with_operations(vec!["read", "write"])
            .prefixed();

        assert_eq!(policy.principal, "User:alice");
        assert_eq!(policy.resource_type, "topic");
        assert_eq!(policy.resource_name, "events");
        assert_eq!(policy.pattern_type, "prefixed");
        assert_eq!(policy.operations, vec!["read", "write"]);
    }

    #[test]
    fn test_quota_policy_builder() {
        let policy = QuotaPolicy::for_user("alice")
            .with_producer_rate(10_485_760)
            .with_consumer_rate(20_971_520);

        assert_eq!(policy.entity_type, "user");
        assert_eq!(policy.entity, "alice");
        assert_eq!(policy.producer_byte_rate, Some(10_485_760));
        assert_eq!(policy.consumer_byte_rate, Some(20_971_520));
    }

    #[test]
    fn test_schema_policy_builder() {
        let policy = SchemaPolicy::new("events-value", "avro")
            .with_compatibility("full")
            .with_file("schemas/events.avsc")
            .required();

        assert_eq!(policy.subject, "events-value");
        assert_eq!(policy.schema_type, "avro");
        assert_eq!(policy.compatibility, "full");
        assert_eq!(policy.schema_file, Some("schemas/events.avsc".to_string()));
        assert!(policy.required);
    }

    #[test]
    fn test_policy_document_serialization() {
        let mut doc = PolicyDocument::with_metadata("test", "dev");
        doc.add_topic(TopicPolicy::new("events").with_partitions(3));
        doc.add_acl(AclPolicy::new("User:alice", "topic", "events").with_operations(vec!["read"]));

        let yaml = serde_yaml::to_string(&doc).unwrap();
        let deserialized: PolicyDocument = serde_yaml::from_str(&yaml).unwrap();

        assert_eq!(deserialized.topics.len(), 1);
        assert_eq!(deserialized.topics[0].name, "events");
        assert_eq!(deserialized.acls.len(), 1);
    }

    #[test]
    fn test_policy_document_merge() {
        let mut doc1 = PolicyDocument::default();
        doc1.add_topic(TopicPolicy::new("events"));

        let mut doc2 = PolicyDocument::default();
        doc2.add_topic(TopicPolicy::new("logs"));
        doc2.add_acl(AclPolicy::new("User:bob", "topic", "*").with_operations(vec!["read"]));

        doc1.merge(doc2);

        assert_eq!(doc1.topics.len(), 2);
        assert_eq!(doc1.acls.len(), 1);
    }

    #[test]
    fn test_find_acls_for_resource() {
        let mut doc = PolicyDocument::default();
        doc.add_acl(AclPolicy::new("User:alice", "topic", "events").with_operations(vec!["read"]));
        doc.add_acl(AclPolicy::new("User:bob", "topic", "*").with_operations(vec!["read"]));
        doc.add_acl(
            AclPolicy::new("User:charlie", "group", "consumers").with_operations(vec!["read"]),
        );

        let topic_acls = doc.find_acls_for_resource("topic", "events");
        assert_eq!(topic_acls.len(), 2); // alice + bob (wildcard)

        let group_acls = doc.find_acls_for_resource("group", "consumers");
        assert_eq!(group_acls.len(), 1);
    }
}
