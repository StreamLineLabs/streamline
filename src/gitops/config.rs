//! GitOps Configuration Types
//!
//! Defines the schema for Streamline declarative configuration manifests.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// GitOps configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitOpsConfig {
    /// Enable GitOps mode
    pub enabled: bool,
    /// Path to watch for configuration files
    pub config_path: Option<String>,
    /// Poll interval for file changes (seconds)
    pub poll_interval_seconds: u64,
    /// Reconciliation settings
    pub reconcile: ReconcileConfig,
    /// Kubernetes integration
    pub kubernetes: KubernetesConfig,
}

impl Default for GitOpsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            config_path: None,
            poll_interval_seconds: 30,
            reconcile: ReconcileConfig::default(),
            kubernetes: KubernetesConfig::default(),
        }
    }
}

/// Reconciliation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcileConfig {
    /// Enable automatic reconciliation
    pub enabled: bool,
    /// Dry run mode (don't apply changes)
    pub dry_run: bool,
    /// Maximum concurrent reconciliations
    pub max_concurrent: usize,
    /// Retry failed reconciliations
    pub retry_on_failure: bool,
    /// Max retries
    pub max_retries: u32,
    /// Retry delay (seconds)
    pub retry_delay_seconds: u64,
}

impl Default for ReconcileConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            dry_run: false,
            max_concurrent: 5,
            retry_on_failure: true,
            max_retries: 3,
            retry_delay_seconds: 5,
        }
    }
}

/// Kubernetes integration configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KubernetesConfig {
    /// Enable Kubernetes operator mode
    pub enabled: bool,
    /// Namespace to watch (empty = all namespaces)
    pub namespace: Option<String>,
    /// CRD API version
    pub api_version: String,
}

impl Default for KubernetesConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            namespace: None,
            api_version: "streamline.io/v1".to_string(),
        }
    }
}

/// Manifest kind
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ManifestKind {
    #[default]
    StreamlineConfig,
    Topic,
    Acl,
    Schema,
    Sink,
    Connector,
    Cluster,
}

/// Configuration manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConfigManifest {
    /// API version
    #[serde(default = "default_api_version")]
    pub api_version: String,
    /// Manifest kind
    #[serde(default)]
    pub kind: ManifestKind,
    /// Metadata
    pub metadata: ManifestMetadata,
    /// Specification
    pub spec: Option<StreamlineConfigSpec>,
}

fn default_api_version() -> String {
    "streamline.io/v1".to_string()
}

/// Manifest metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ManifestMetadata {
    /// Resource name
    pub name: String,
    /// Namespace
    #[serde(default = "default_namespace")]
    pub namespace: String,
    /// Labels
    #[serde(default)]
    pub labels: HashMap<String, String>,
    /// Annotations
    #[serde(default)]
    pub annotations: HashMap<String, String>,
    /// Generation number
    #[serde(default)]
    pub generation: u64,
    /// Resource version
    #[serde(default)]
    pub resource_version: Option<String>,
}

fn default_namespace() -> String {
    "default".to_string()
}

/// Streamline configuration specification
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamlineConfigSpec {
    /// Topics to create/manage
    #[serde(default)]
    pub topics: Vec<TopicSpec>,
    /// ACLs to create/manage
    #[serde(default)]
    pub acls: Vec<AclSpec>,
    /// Schemas to register
    #[serde(default)]
    pub schemas: Vec<SchemaSpec>,
    /// Sinks to configure
    #[serde(default)]
    pub sinks: Vec<SinkSpec>,
    /// Global configuration overrides
    #[serde(default)]
    pub config: HashMap<String, String>,
}

/// Topic specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicSpec {
    /// Topic name
    pub name: String,
    /// Number of partitions
    #[serde(default = "default_partitions")]
    pub partitions: u32,
    /// Replication factor
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u16,
    /// Topic configuration
    #[serde(default)]
    pub config: HashMap<String, String>,
    /// Storage mode
    #[serde(default)]
    pub storage_mode: Option<String>,
    /// Labels
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

fn default_partitions() -> u32 {
    1
}

fn default_replication_factor() -> u16 {
    1
}

impl Default for TopicSpec {
    fn default() -> Self {
        Self {
            name: String::new(),
            partitions: default_partitions(),
            replication_factor: default_replication_factor(),
            config: HashMap::new(),
            storage_mode: None,
            labels: HashMap::new(),
        }
    }
}

/// ACL specification
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AclSpec {
    /// Principal (e.g., "User:alice", "Group:admins")
    pub principal: String,
    /// Resource type (topic, group, cluster, transactional_id)
    pub resource_type: String,
    /// Resource name
    pub resource_name: String,
    /// Resource pattern (literal, prefixed, *)
    #[serde(default)]
    pub resource_pattern: Option<String>,
    /// Operation (read, write, create, delete, alter, describe, all)
    pub operation: String,
    /// Permission (allow, deny)
    pub permission: String,
    /// Host restriction
    #[serde(default)]
    pub host: Option<String>,
}

/// Schema specification
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaSpec {
    /// Subject name
    pub subject: String,
    /// Schema type (avro, protobuf, json)
    pub schema_type: String,
    /// Schema definition
    pub schema: String,
    /// Compatibility level
    #[serde(default)]
    pub compatibility: Option<String>,
    /// Schema references
    #[serde(default)]
    pub references: Vec<SchemaReferenceSpec>,
}

/// Schema reference
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaReferenceSpec {
    /// Reference name
    pub name: String,
    /// Subject
    pub subject: String,
    /// Version
    pub version: i32,
}

/// Sink specification
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SinkSpec {
    /// Sink name
    pub name: String,
    /// Sink type (iceberg, delta, s3, kafka)
    pub sink_type: String,
    /// Topics to sink
    pub topics: Vec<String>,
    /// Sink configuration
    #[serde(default)]
    pub config: HashMap<String, String>,
    /// Enabled
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_deserialization() {
        let yaml = r#"
apiVersion: streamline.io/v1
kind: StreamlineConfig
metadata:
  name: test
  namespace: default
spec:
  topics:
    - name: events
      partitions: 3
"#;

        let manifest: ConfigManifest = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(manifest.metadata.name, "test");
        assert_eq!(manifest.spec.as_ref().unwrap().topics[0].name, "events");
        assert_eq!(manifest.spec.as_ref().unwrap().topics[0].partitions, 3);
    }

    #[test]
    fn test_topic_spec_defaults() {
        let yaml = r#"
name: test-topic
"#;

        let topic: TopicSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(topic.partitions, 1);
        assert_eq!(topic.replication_factor, 1);
    }

    #[test]
    fn test_acl_spec() {
        let yaml = r#"
principal: User:alice
resource_type: topic
resource_name: events
operation: write
permission: allow
"#;

        let acl: AclSpec = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(acl.principal, "User:alice");
        assert_eq!(acl.resource_type, "topic");
        assert_eq!(acl.operation, "write");
    }
}
