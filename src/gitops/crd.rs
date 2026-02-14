//! Kubernetes Custom Resource Definitions
//!
//! Generates CRDs for Streamline resources:
//! - StreamlineTopic
//! - StreamlineSink
//! - StreamlineCluster
//! - StreamlineConfig

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Custom Resource Definition metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomResourceDefinition {
    /// API version
    pub api_version: String,
    /// Kind
    pub kind: String,
    /// Metadata
    pub metadata: CrdMetadata,
    /// Spec
    pub spec: CrdSpec,
}

/// CRD metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdMetadata {
    /// CRD name
    pub name: String,
    /// Annotations
    #[serde(default)]
    pub annotations: HashMap<String, String>,
}

/// CRD specification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdSpec {
    /// Group
    pub group: String,
    /// Names
    pub names: CrdNames,
    /// Scope
    pub scope: String,
    /// Versions
    pub versions: Vec<CrdVersion>,
}

/// CRD names
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdNames {
    /// Plural name
    pub plural: String,
    /// Singular name
    pub singular: String,
    /// Kind
    pub kind: String,
    /// Short names
    #[serde(default)]
    pub short_names: Vec<String>,
}

/// CRD version
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdVersion {
    /// Version name
    pub name: String,
    /// Is this the storage version?
    pub served: bool,
    /// Is this the storage version?
    pub storage: bool,
    /// Schema
    pub schema: CrdSchema,
    /// Subresources
    #[serde(default)]
    pub subresources: Option<CrdSubresources>,
}

/// CRD schema
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CrdSchema {
    /// OpenAPI v3 schema
    pub open_apiv3_schema: OpenApiSchema,
}

/// OpenAPI v3 schema
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OpenApiSchema {
    /// Type
    #[serde(rename = "type")]
    pub schema_type: String,
    /// Properties
    #[serde(default)]
    pub properties: HashMap<String, SchemaProperty>,
    /// Required fields
    #[serde(default)]
    pub required: Vec<String>,
}

/// Schema property
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaProperty {
    /// Type
    #[serde(rename = "type")]
    pub prop_type: String,
    /// Description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Default value
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
    /// Nested properties
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub properties: HashMap<String, SchemaProperty>,
    /// Items (for arrays)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub items: Option<Box<SchemaProperty>>,
    /// Additional properties
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_properties: Option<Box<SchemaProperty>>,
}

/// CRD subresources
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrdSubresources {
    /// Status subresource
    #[serde(default)]
    pub status: Option<CrdStatusSubresource>,
}

/// Status subresource (empty struct enables it)
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CrdStatusSubresource {}

/// StreamlineTopic custom resource
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamlineTopic {
    /// API version
    pub api_version: String,
    /// Kind
    pub kind: String,
    /// Metadata
    pub metadata: K8sMetadata,
    /// Spec
    pub spec: StreamlineTopicSpec,
    /// Status
    #[serde(default)]
    pub status: Option<StreamlineTopicStatus>,
}

/// Kubernetes metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct K8sMetadata {
    /// Name
    pub name: String,
    /// Namespace
    #[serde(default)]
    pub namespace: Option<String>,
    /// Labels
    #[serde(default)]
    pub labels: HashMap<String, String>,
    /// Annotations
    #[serde(default)]
    pub annotations: HashMap<String, String>,
    /// Generation
    #[serde(default)]
    pub generation: Option<i64>,
    /// Resource version
    #[serde(default)]
    pub resource_version: Option<String>,
}

/// StreamlineTopic spec
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamlineTopicSpec {
    /// Number of partitions
    #[serde(default = "default_partitions")]
    pub partitions: i32,
    /// Replication factor
    #[serde(default = "default_replication_factor")]
    pub replication_factor: i16,
    /// Topic configuration
    #[serde(default)]
    pub config: HashMap<String, String>,
    /// Retention policy
    #[serde(default)]
    pub retention: Option<RetentionSpec>,
}

fn default_partitions() -> i32 {
    1
}

fn default_replication_factor() -> i16 {
    1
}

/// Retention specification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetentionSpec {
    /// Time-based retention (e.g., "7d", "24h")
    #[serde(default)]
    pub time: Option<String>,
    /// Size-based retention (e.g., "10Gi", "1Ti")
    #[serde(default)]
    pub size: Option<String>,
}

/// StreamlineTopic status
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamlineTopicStatus {
    /// Observed generation
    pub observed_generation: i64,
    /// Ready condition
    pub ready: bool,
    /// Conditions
    #[serde(default)]
    pub conditions: Vec<Condition>,
    /// Current partitions
    #[serde(default)]
    pub current_partitions: i32,
    /// ISR (in-sync replicas) count
    #[serde(default)]
    pub isr_count: i32,
}

/// Kubernetes condition
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Condition {
    /// Type
    #[serde(rename = "type")]
    pub condition_type: String,
    /// Status (True, False, Unknown)
    pub status: String,
    /// Last transition time
    pub last_transition_time: Option<String>,
    /// Reason
    #[serde(default)]
    pub reason: Option<String>,
    /// Message
    #[serde(default)]
    pub message: Option<String>,
}

/// StreamlineSink custom resource
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamlineSink {
    /// API version
    pub api_version: String,
    /// Kind
    pub kind: String,
    /// Metadata
    pub metadata: K8sMetadata,
    /// Spec
    pub spec: StreamlineSinkSpec,
    /// Status
    #[serde(default)]
    pub status: Option<StreamlineSinkStatus>,
}

/// StreamlineSink spec
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamlineSinkSpec {
    /// Sink type (iceberg, delta, s3)
    pub sink_type: String,
    /// Source topics
    pub topics: Vec<String>,
    /// Sink configuration
    #[serde(default)]
    pub config: HashMap<String, String>,
    /// Destination configuration
    pub destination: SinkDestination,
}

/// Sink destination
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SinkDestination {
    /// Catalog URI (for Iceberg)
    #[serde(default)]
    pub catalog_uri: Option<String>,
    /// Table namespace
    #[serde(default)]
    pub namespace: Option<String>,
    /// Table name
    #[serde(default)]
    pub table: Option<String>,
    /// S3 bucket
    #[serde(default)]
    pub bucket: Option<String>,
    /// S3 prefix
    #[serde(default)]
    pub prefix: Option<String>,
}

/// StreamlineSink status
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamlineSinkStatus {
    /// Observed generation
    pub observed_generation: i64,
    /// Running state
    pub running: bool,
    /// Records written
    pub records_written: i64,
    /// Last write time
    pub last_write_time: Option<String>,
    /// Conditions
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

/// StreamlineCluster custom resource
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamlineCluster {
    /// API version
    pub api_version: String,
    /// Kind
    pub kind: String,
    /// Metadata
    pub metadata: K8sMetadata,
    /// Spec
    pub spec: StreamlineClusterSpec,
    /// Status
    #[serde(default)]
    pub status: Option<StreamlineClusterStatus>,
}

/// StreamlineCluster spec
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamlineClusterSpec {
    /// Number of replicas
    pub replicas: i32,
    /// Version
    #[serde(default)]
    pub version: Option<String>,
    /// Resources
    #[serde(default)]
    pub resources: Option<ResourceSpec>,
    /// Storage
    #[serde(default)]
    pub storage: Option<StorageSpec>,
    /// TLS configuration
    #[serde(default)]
    pub tls: Option<TlsSpec>,
}

/// Resource specification
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceSpec {
    /// CPU request
    #[serde(default)]
    pub cpu: Option<String>,
    /// Memory request
    #[serde(default)]
    pub memory: Option<String>,
}

/// Storage specification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StorageSpec {
    /// Storage size
    pub size: String,
    /// Storage class
    #[serde(default)]
    pub storage_class: Option<String>,
}

/// TLS specification
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TlsSpec {
    /// Enabled
    pub enabled: bool,
    /// Secret name
    #[serde(default)]
    pub secret_name: Option<String>,
}

/// StreamlineCluster status
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamlineClusterStatus {
    /// Observed generation
    pub observed_generation: i64,
    /// Ready replicas
    pub ready_replicas: i32,
    /// Current version
    pub current_version: Option<String>,
    /// Conditions
    #[serde(default)]
    pub conditions: Vec<Condition>,
}

/// CRD generator
pub struct CrdGenerator;

impl CrdGenerator {
    /// Generate StreamlineTopic CRD
    pub fn streamline_topic_crd() -> CustomResourceDefinition {
        CustomResourceDefinition {
            api_version: "apiextensions.k8s.io/v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
            metadata: CrdMetadata {
                name: "streamlinetopics.streamline.io".to_string(),
                annotations: HashMap::new(),
            },
            spec: CrdSpec {
                group: "streamline.io".to_string(),
                names: CrdNames {
                    plural: "streamlinetopics".to_string(),
                    singular: "streamlinetopic".to_string(),
                    kind: "StreamlineTopic".to_string(),
                    short_names: vec!["st".to_string(), "topic".to_string()],
                },
                scope: "Namespaced".to_string(),
                versions: vec![CrdVersion {
                    name: "v1".to_string(),
                    served: true,
                    storage: true,
                    schema: Self::topic_schema(),
                    subresources: Some(CrdSubresources {
                        status: Some(CrdStatusSubresource {}),
                    }),
                }],
            },
        }
    }

    /// Generate StreamlineSink CRD
    pub fn streamline_sink_crd() -> CustomResourceDefinition {
        CustomResourceDefinition {
            api_version: "apiextensions.k8s.io/v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
            metadata: CrdMetadata {
                name: "streamlinesinks.streamline.io".to_string(),
                annotations: HashMap::new(),
            },
            spec: CrdSpec {
                group: "streamline.io".to_string(),
                names: CrdNames {
                    plural: "streamlinesinks".to_string(),
                    singular: "streamlinesink".to_string(),
                    kind: "StreamlineSink".to_string(),
                    short_names: vec!["ss".to_string(), "sink".to_string()],
                },
                scope: "Namespaced".to_string(),
                versions: vec![CrdVersion {
                    name: "v1".to_string(),
                    served: true,
                    storage: true,
                    schema: Self::sink_schema(),
                    subresources: Some(CrdSubresources {
                        status: Some(CrdStatusSubresource {}),
                    }),
                }],
            },
        }
    }

    /// Generate StreamlineCluster CRD
    pub fn streamline_cluster_crd() -> CustomResourceDefinition {
        CustomResourceDefinition {
            api_version: "apiextensions.k8s.io/v1".to_string(),
            kind: "CustomResourceDefinition".to_string(),
            metadata: CrdMetadata {
                name: "streamlineclusters.streamline.io".to_string(),
                annotations: HashMap::new(),
            },
            spec: CrdSpec {
                group: "streamline.io".to_string(),
                names: CrdNames {
                    plural: "streamlineclusters".to_string(),
                    singular: "streamlinecluster".to_string(),
                    kind: "StreamlineCluster".to_string(),
                    short_names: vec!["sc".to_string(), "cluster".to_string()],
                },
                scope: "Namespaced".to_string(),
                versions: vec![CrdVersion {
                    name: "v1".to_string(),
                    served: true,
                    storage: true,
                    schema: Self::cluster_schema(),
                    subresources: Some(CrdSubresources {
                        status: Some(CrdStatusSubresource {}),
                    }),
                }],
            },
        }
    }

    /// Generate all CRDs as YAML
    pub fn generate_all_yaml() -> String {
        let topic_crd = serde_yaml::to_string(&Self::streamline_topic_crd()).unwrap_or_default();
        let sink_crd = serde_yaml::to_string(&Self::streamline_sink_crd()).unwrap_or_default();
        let cluster_crd =
            serde_yaml::to_string(&Self::streamline_cluster_crd()).unwrap_or_default();

        format!(
            "---\n{}\n---\n{}\n---\n{}",
            topic_crd, sink_crd, cluster_crd
        )
    }

    fn topic_schema() -> CrdSchema {
        let mut properties = HashMap::new();

        properties.insert(
            "partitions".to_string(),
            SchemaProperty {
                prop_type: "integer".to_string(),
                description: Some("Number of partitions".to_string()),
                default: Some(serde_json::json!(1)),
                ..Default::default()
            },
        );

        properties.insert(
            "replicationFactor".to_string(),
            SchemaProperty {
                prop_type: "integer".to_string(),
                description: Some("Replication factor".to_string()),
                default: Some(serde_json::json!(1)),
                ..Default::default()
            },
        );

        properties.insert(
            "config".to_string(),
            SchemaProperty {
                prop_type: "object".to_string(),
                description: Some("Topic configuration".to_string()),
                additional_properties: Some(Box::new(SchemaProperty {
                    prop_type: "string".to_string(),
                    ..Default::default()
                })),
                ..Default::default()
            },
        );

        CrdSchema {
            open_apiv3_schema: OpenApiSchema {
                schema_type: "object".to_string(),
                properties,
                required: vec![],
            },
        }
    }

    fn sink_schema() -> CrdSchema {
        let mut properties = HashMap::new();

        properties.insert(
            "sinkType".to_string(),
            SchemaProperty {
                prop_type: "string".to_string(),
                description: Some("Sink type (iceberg, delta, s3)".to_string()),
                ..Default::default()
            },
        );

        properties.insert(
            "topics".to_string(),
            SchemaProperty {
                prop_type: "array".to_string(),
                description: Some("Source topics".to_string()),
                items: Some(Box::new(SchemaProperty {
                    prop_type: "string".to_string(),
                    ..Default::default()
                })),
                ..Default::default()
            },
        );

        CrdSchema {
            open_apiv3_schema: OpenApiSchema {
                schema_type: "object".to_string(),
                properties,
                required: vec!["sinkType".to_string(), "topics".to_string()],
            },
        }
    }

    fn cluster_schema() -> CrdSchema {
        let mut properties = HashMap::new();

        properties.insert(
            "replicas".to_string(),
            SchemaProperty {
                prop_type: "integer".to_string(),
                description: Some("Number of replicas".to_string()),
                default: Some(serde_json::json!(3)),
                ..Default::default()
            },
        );

        properties.insert(
            "version".to_string(),
            SchemaProperty {
                prop_type: "string".to_string(),
                description: Some("Streamline version".to_string()),
                ..Default::default()
            },
        );

        CrdSchema {
            open_apiv3_schema: OpenApiSchema {
                schema_type: "object".to_string(),
                properties,
                required: vec!["replicas".to_string()],
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crd_generation() {
        let crd = CrdGenerator::streamline_topic_crd();
        assert_eq!(crd.spec.group, "streamline.io");
        assert_eq!(crd.spec.names.kind, "StreamlineTopic");
    }

    #[test]
    fn test_generate_all_yaml() {
        let yaml = CrdGenerator::generate_all_yaml();
        assert!(yaml.contains("StreamlineTopic"));
        assert!(yaml.contains("StreamlineSink"));
        assert!(yaml.contains("StreamlineCluster"));
    }

    #[test]
    fn test_streamline_topic_serialization() {
        let topic = StreamlineTopic {
            api_version: "streamline.io/v1".to_string(),
            kind: "StreamlineTopic".to_string(),
            metadata: K8sMetadata {
                name: "events".to_string(),
                namespace: Some("default".to_string()),
                ..Default::default()
            },
            spec: StreamlineTopicSpec {
                partitions: 6,
                replication_factor: 3,
                config: HashMap::from([("retention.ms".to_string(), "604800000".to_string())]),
                retention: None,
            },
            status: None,
        };

        let yaml = serde_yaml::to_string(&topic).unwrap();
        assert!(yaml.contains("partitions: 6"));
    }
}
