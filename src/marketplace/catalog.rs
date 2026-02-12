//! Connector catalog — browsable list of available connectors.

use serde::{Deserialize, Serialize};

/// Category of connector.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorCategory {
    Source,
    Sink,
    Transform,
}

impl std::fmt::Display for ConnectorCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Source => write!(f, "source"),
            Self::Sink => write!(f, "sink"),
            Self::Transform => write!(f, "transform"),
        }
    }
}

/// Metadata about a connector in the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorMetadata {
    /// Unique connector name (e.g., "postgres-cdc")
    pub name: String,
    /// Display name
    pub display_name: String,
    /// Description
    pub description: String,
    /// Connector category
    pub category: ConnectorCategory,
    /// Version
    pub version: String,
    /// Author
    pub author: String,
    /// License
    pub license: String,
    /// Tags for discovery
    pub tags: Vec<String>,
    /// Whether this is an official connector
    pub official: bool,
    /// Download count
    pub downloads: u64,
}

/// A catalog entry with additional install info.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorCatalogEntry {
    /// Connector metadata
    pub metadata: ConnectorMetadata,
    /// Required configuration keys
    pub required_config: Vec<ConfigKey>,
    /// Optional configuration keys
    pub optional_config: Vec<ConfigKey>,
    /// Required feature flags
    pub required_features: Vec<String>,
    /// Compatible Streamline versions
    pub compatible_versions: String,
}

/// A configuration key for a connector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigKey {
    pub name: String,
    pub description: String,
    pub default_value: Option<String>,
    pub required: bool,
}

/// The marketplace catalog — a searchable collection of connectors.
pub struct MarketplaceCatalog {
    entries: Vec<ConnectorCatalogEntry>,
}

impl MarketplaceCatalog {
    /// Create a catalog with built-in connectors.
    pub fn with_builtins() -> Self {
        Self {
            entries: Self::builtin_catalog(),
        }
    }

    /// Search for connectors matching a query.
    pub fn search(&self, query: &str) -> Vec<&ConnectorCatalogEntry> {
        let q = query.to_lowercase();
        self.entries
            .iter()
            .filter(|e| {
                e.metadata.name.to_lowercase().contains(&q)
                    || e.metadata.display_name.to_lowercase().contains(&q)
                    || e.metadata.description.to_lowercase().contains(&q)
                    || e.metadata
                        .tags
                        .iter()
                        .any(|t| t.to_lowercase().contains(&q))
            })
            .collect()
    }

    /// List all connectors in a category.
    pub fn by_category(&self, category: &ConnectorCategory) -> Vec<&ConnectorCatalogEntry> {
        self.entries
            .iter()
            .filter(|e| &e.metadata.category == category)
            .collect()
    }

    /// Get a specific connector by name.
    pub fn get(&self, name: &str) -> Option<&ConnectorCatalogEntry> {
        self.entries.iter().find(|e| e.metadata.name == name)
    }

    /// List all available connectors.
    pub fn list(&self) -> &[ConnectorCatalogEntry] {
        &self.entries
    }

    fn builtin_catalog() -> Vec<ConnectorCatalogEntry> {
        vec![
            Self::make_entry(
                "postgres-cdc",
                "PostgreSQL CDC",
                ConnectorCategory::Source,
                "Capture changes from PostgreSQL via logical replication",
                &["database", "cdc", "postgresql"],
                &["postgres-cdc"],
                &[
                    ("connection_url", "PostgreSQL connection string", true),
                    ("slot_name", "Replication slot name", true),
                    ("publication", "Publication name", false),
                ],
            ),
            Self::make_entry(
                "mysql-cdc",
                "MySQL CDC",
                ConnectorCategory::Source,
                "Capture changes from MySQL via binlog replication",
                &["database", "cdc", "mysql"],
                &["mysql-cdc"],
                &[
                    ("host", "MySQL host", true),
                    ("port", "MySQL port", false),
                    ("database", "Database name", true),
                ],
            ),
            Self::make_entry(
                "s3-sink",
                "Amazon S3 Sink",
                ConnectorCategory::Sink,
                "Write stream data to S3 in Parquet, JSON, or CSV format",
                &["cloud", "storage", "aws", "s3"],
                &["cloud-storage"],
                &[
                    ("bucket", "S3 bucket name", true),
                    ("region", "AWS region", true),
                    ("format", "Output format (parquet/json/csv)", false),
                ],
            ),
            Self::make_entry(
                "elasticsearch-sink",
                "Elasticsearch Sink",
                ConnectorCategory::Sink,
                "Index stream data into Elasticsearch",
                &["search", "elasticsearch", "indexing"],
                &[],
                &[
                    ("hosts", "Elasticsearch hosts (comma-separated)", true),
                    ("index", "Target index name", true),
                ],
            ),
            Self::make_entry(
                "http-webhook",
                "HTTP Webhook",
                ConnectorCategory::Sink,
                "POST stream events to HTTP endpoints",
                &["http", "webhook", "api"],
                &[],
                &[
                    ("url", "Target URL", true),
                    ("method", "HTTP method (POST/PUT)", false),
                    ("headers", "Custom headers (JSON)", false),
                ],
            ),
            Self::make_entry(
                "redis-sink",
                "Redis Sink",
                ConnectorCategory::Sink,
                "Write stream data to Redis (strings, lists, streams)",
                &["cache", "redis"],
                &[],
                &[
                    ("url", "Redis connection URL", true),
                    ("data_type", "Redis data type (string/list/stream)", false),
                ],
            ),
            Self::make_entry(
                "json-transform",
                "JSON Transform",
                ConnectorCategory::Transform,
                "Transform JSON messages with jq-like expressions",
                &["transform", "json", "jq"],
                &[],
                &[("expression", "JQ expression", true)],
            ),
            Self::make_entry(
                "filter",
                "Message Filter",
                ConnectorCategory::Transform,
                "Filter messages based on conditions",
                &["transform", "filter"],
                &[],
                &[("condition", "Filter condition expression", true)],
            ),
        ]
    }

    fn make_entry(
        name: &str,
        display: &str,
        category: ConnectorCategory,
        desc: &str,
        tags: &[&str],
        features: &[&str],
        config: &[(&str, &str, bool)],
    ) -> ConnectorCatalogEntry {
        ConnectorCatalogEntry {
            metadata: ConnectorMetadata {
                name: name.to_string(),
                display_name: display.to_string(),
                description: desc.to_string(),
                category,
                version: env!("CARGO_PKG_VERSION").to_string(),
                author: "Streamline".to_string(),
                license: "Apache-2.0".to_string(),
                tags: tags.iter().map(|s| s.to_string()).collect(),
                official: true,
                downloads: 0,
            },
            required_config: config
                .iter()
                .filter(|(_, _, req)| *req)
                .map(|(n, d, _)| ConfigKey {
                    name: n.to_string(),
                    description: d.to_string(),
                    default_value: None,
                    required: true,
                })
                .collect(),
            optional_config: config
                .iter()
                .filter(|(_, _, req)| !req)
                .map(|(n, d, _)| ConfigKey {
                    name: n.to_string(),
                    description: d.to_string(),
                    default_value: None,
                    required: false,
                })
                .collect(),
            required_features: features.iter().map(|s| s.to_string()).collect(),
            compatible_versions: format!(">={}", env!("CARGO_PKG_VERSION")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_catalog_search() {
        let catalog = MarketplaceCatalog::with_builtins();
        let results = catalog.search("postgres");
        assert!(!results.is_empty());
        assert_eq!(results[0].metadata.name, "postgres-cdc");
    }

    #[test]
    fn test_catalog_by_category() {
        let catalog = MarketplaceCatalog::with_builtins();
        let sinks = catalog.by_category(&ConnectorCategory::Sink);
        assert!(sinks.len() >= 3);
        assert!(sinks
            .iter()
            .all(|e| e.metadata.category == ConnectorCategory::Sink));
    }

    #[test]
    fn test_catalog_get() {
        let catalog = MarketplaceCatalog::with_builtins();
        let entry = catalog.get("http-webhook");
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().metadata.display_name, "HTTP Webhook");
    }
}
