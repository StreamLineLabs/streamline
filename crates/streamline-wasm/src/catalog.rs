//! WASM Transform Catalog - Versioned transform management with A/B deployment
//!
//! Provides:
//! - Transform versioning with semantic versions
//! - A/B deployment (canary) for gradual rollout
//! - Transform chain definitions for multi-step pipelines
//! - Hot-reload without message loss

use crate::error::{Result, WasmError};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// Transform version
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransformVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl TransformVersion {
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 3 {
            return Err(WasmError::Validation(format!(
                "Invalid version format: {}",
                s
            )));
        }
        Ok(Self {
            major: parts[0]
                .parse()
                .map_err(|_| WasmError::Validation("invalid major version".into()))?,
            minor: parts[1]
                .parse()
                .map_err(|_| WasmError::Validation("invalid minor version".into()))?,
            patch: parts[2]
                .parse()
                .map_err(|_| WasmError::Validation("invalid patch version".into()))?,
        })
    }
}

impl std::fmt::Display for TransformVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl PartialOrd for TransformVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TransformVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.major
            .cmp(&other.major)
            .then(self.minor.cmp(&other.minor))
            .then(self.patch.cmp(&other.patch))
    }
}

/// Catalog entry for a transform
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub id: String,
    pub name: String,
    pub description: String,
    pub version: TransformVersion,
    pub source_topics: Vec<String>,
    pub target_topics: Vec<String>,
    pub wasm_module_hash: String,
    pub config: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: TransformStatus,
    pub deployment: DeploymentConfig,
    pub metrics: TransformMetrics,
}

/// Transform deployment status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransformStatus {
    Draft,
    Active,
    Canary,
    Deprecated,
    Disabled,
}

impl std::fmt::Display for TransformStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Draft => write!(f, "draft"),
            Self::Active => write!(f, "active"),
            Self::Canary => write!(f, "canary"),
            Self::Deprecated => write!(f, "deprecated"),
            Self::Disabled => write!(f, "disabled"),
        }
    }
}

/// A/B deployment configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentConfig {
    /// Traffic percentage for this version (0-100)
    pub traffic_percentage: u8,
    /// Canary version (if in A/B mode)
    pub canary_version: Option<TransformVersion>,
    /// Canary traffic percentage
    pub canary_traffic_percentage: u8,
    /// Auto-promote canary after N successful messages
    pub auto_promote_after: Option<u64>,
    /// Rollback on error rate exceeding threshold
    pub rollback_error_threshold: f64,
}

impl Default for DeploymentConfig {
    fn default() -> Self {
        Self {
            traffic_percentage: 100,
            canary_version: None,
            canary_traffic_percentage: 0,
            auto_promote_after: None,
            rollback_error_threshold: 5.0,
        }
    }
}

/// Transform metrics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransformMetrics {
    pub messages_processed: u64,
    pub messages_failed: u64,
    pub avg_latency_us: f64,
    pub p99_latency_us: f64,
    pub bytes_in: u64,
    pub bytes_out: u64,
}

/// Transform chain definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformChain {
    pub id: String,
    pub name: String,
    pub description: String,
    pub steps: Vec<ChainStep>,
    pub source_topic: String,
    pub sink_topic: String,
    pub error_handling: ChainErrorHandling,
    pub created_at: DateTime<Utc>,
}

/// A single step in a transform chain
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainStep {
    pub transform_id: String,
    pub version: TransformVersion,
    pub config_overrides: HashMap<String, String>,
    pub order: u32,
}

/// Error handling for transform chains
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum ChainErrorHandling {
    /// Stop the chain on error
    #[default]
    StopOnError,
    /// Skip the failed step and continue
    SkipOnError,
    /// Route failed messages to a DLQ
    DeadLetterQueue { topic: String },
}

impl std::fmt::Display for ChainErrorHandling {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StopOnError => write!(f, "stop-on-error"),
            Self::SkipOnError => write!(f, "skip-on-error"),
            Self::DeadLetterQueue { topic } => write!(f, "dlq({})", topic),
        }
    }
}

/// Transform Catalog
pub struct TransformCatalog {
    entries: Arc<RwLock<HashMap<String, Vec<CatalogEntry>>>>,
    chains: Arc<RwLock<HashMap<String, TransformChain>>>,
}

impl Default for TransformCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl TransformCatalog {
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            chains: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Publish a new version of a transform
    pub async fn publish(
        &self,
        name: &str,
        version: TransformVersion,
        description: &str,
        wasm_hash: &str,
        source_topics: Vec<String>,
        target_topics: Vec<String>,
    ) -> Result<CatalogEntry> {
        let entry = CatalogEntry {
            id: format!("{}:{}", name, version),
            name: name.to_string(),
            description: description.to_string(),
            version: version.clone(),
            source_topics,
            target_topics,
            wasm_module_hash: wasm_hash.to_string(),
            config: HashMap::new(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            status: TransformStatus::Draft,
            deployment: DeploymentConfig::default(),
            metrics: TransformMetrics::default(),
        };

        let mut entries = self.entries.write().await;
        entries
            .entry(name.to_string())
            .or_default()
            .push(entry.clone());

        info!("Published transform {}:{}", name, version);
        Ok(entry)
    }

    /// Activate a transform version
    pub async fn activate(&self, name: &str, version: &TransformVersion) -> Result<()> {
        let mut entries = self.entries.write().await;
        let versions = entries
            .get_mut(name)
            .ok_or_else(|| WasmError::Internal(format!("Transform not found: {}", name)))?;

        // Deprecate currently active version
        for entry in versions.iter_mut() {
            if entry.status == TransformStatus::Active {
                entry.status = TransformStatus::Deprecated;
            }
        }

        // Activate requested version
        let entry = versions
            .iter_mut()
            .find(|e| &e.version == version)
            .ok_or_else(|| {
                WasmError::Internal(format!("Version {} not found for {}", version, name))
            })?;

        entry.status = TransformStatus::Active;
        entry.updated_at = Utc::now();
        info!("Activated transform {}:{}", name, version);
        Ok(())
    }

    /// Start a canary deployment
    pub async fn start_canary(
        &self,
        name: &str,
        canary_version: &TransformVersion,
        traffic_pct: u8,
    ) -> Result<()> {
        let mut entries = self.entries.write().await;
        let versions = entries
            .get_mut(name)
            .ok_or_else(|| WasmError::Internal(format!("Transform not found: {}", name)))?;

        // Set canary on the active version
        for entry in versions.iter_mut() {
            if entry.status == TransformStatus::Active {
                entry.deployment.canary_version = Some(canary_version.clone());
                entry.deployment.canary_traffic_percentage = traffic_pct;
                entry.deployment.traffic_percentage = 100 - traffic_pct;
            }
        }

        // Mark canary version
        let canary_entry = versions
            .iter_mut()
            .find(|e| &e.version == canary_version)
            .ok_or_else(|| {
                WasmError::Internal(format!(
                    "Canary version {} not found for {}",
                    canary_version, name
                ))
            })?;
        canary_entry.status = TransformStatus::Canary;

        info!(
            "Started canary for {}: {}% traffic to {}",
            name, traffic_pct, canary_version
        );
        Ok(())
    }

    /// Promote canary to active
    pub async fn promote_canary(&self, name: &str) -> Result<()> {
        let mut entries = self.entries.write().await;
        let versions = entries
            .get_mut(name)
            .ok_or_else(|| WasmError::Internal(format!("Transform not found: {}", name)))?;

        for entry in versions.iter_mut() {
            match entry.status {
                TransformStatus::Active => {
                    entry.status = TransformStatus::Deprecated;
                    entry.deployment = DeploymentConfig::default();
                }
                TransformStatus::Canary => {
                    entry.status = TransformStatus::Active;
                    entry.deployment = DeploymentConfig::default();
                }
                _ => {}
            }
        }

        info!("Promoted canary to active for {}", name);
        Ok(())
    }

    /// List all versions of a transform
    pub async fn list_versions(&self, name: &str) -> Vec<CatalogEntry> {
        self.entries
            .read()
            .await
            .get(name)
            .cloned()
            .unwrap_or_default()
    }

    /// List all transforms
    pub async fn list_all(&self) -> Vec<CatalogEntry> {
        self.entries
            .read()
            .await
            .values()
            .flatten()
            .cloned()
            .collect()
    }

    /// Get the active version of a transform
    pub async fn get_active(&self, name: &str) -> Option<CatalogEntry> {
        self.entries
            .read()
            .await
            .get(name)?
            .iter()
            .find(|e| e.status == TransformStatus::Active)
            .cloned()
    }

    /// Create a transform chain
    pub async fn create_chain(&self, chain: TransformChain) -> Result<()> {
        self.chains.write().await.insert(chain.id.clone(), chain);
        Ok(())
    }

    /// Get a transform chain
    pub async fn get_chain(&self, chain_id: &str) -> Option<TransformChain> {
        self.chains.read().await.get(chain_id).cloned()
    }

    /// List all chains
    pub async fn list_chains(&self) -> Vec<TransformChain> {
        self.chains.read().await.values().cloned().collect()
    }

    /// Delete a chain
    pub async fn delete_chain(&self, chain_id: &str) -> Result<()> {
        self.chains
            .write()
            .await
            .remove(chain_id)
            .ok_or_else(|| WasmError::Internal(format!("Chain not found: {}", chain_id)))
            .map(|_| ())
    }

    /// Determine which version should process a message (A/B routing)
    pub async fn route_message(&self, transform_name: &str, message_hash: u64) -> Option<String> {
        let entries = self.entries.read().await;
        let versions = entries.get(transform_name)?;

        let active = versions
            .iter()
            .find(|e| e.status == TransformStatus::Active)?;

        // Check if canary is active
        if let Some(ref canary_version) = active.deployment.canary_version {
            let canary_pct = active.deployment.canary_traffic_percentage as u64;
            if message_hash % 100 < canary_pct {
                return Some(format!("{}:{}", transform_name, canary_version));
            }
        }

        Some(active.id.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_publish_and_activate() {
        let catalog = TransformCatalog::new();
        catalog
            .publish(
                "filter",
                TransformVersion::new(1, 0, 0),
                "Basic filter",
                "abc123",
                vec!["events".into()],
                vec!["filtered-events".into()],
            )
            .await
            .unwrap();

        catalog
            .activate("filter", &TransformVersion::new(1, 0, 0))
            .await
            .unwrap();

        let active = catalog.get_active("filter").await.unwrap();
        assert_eq!(active.status, TransformStatus::Active);
    }

    #[tokio::test]
    async fn test_canary_deployment() {
        let catalog = TransformCatalog::new();

        // Publish v1 and v2
        catalog
            .publish(
                "enrich",
                TransformVersion::new(1, 0, 0),
                "v1",
                "h1",
                vec![],
                vec![],
            )
            .await
            .unwrap();
        catalog
            .publish(
                "enrich",
                TransformVersion::new(2, 0, 0),
                "v2",
                "h2",
                vec![],
                vec![],
            )
            .await
            .unwrap();

        catalog
            .activate("enrich", &TransformVersion::new(1, 0, 0))
            .await
            .unwrap();

        // Start canary with 10% traffic
        catalog
            .start_canary("enrich", &TransformVersion::new(2, 0, 0), 10)
            .await
            .unwrap();

        let versions = catalog.list_versions("enrich").await;
        let canary = versions
            .iter()
            .find(|v| v.status == TransformStatus::Canary)
            .unwrap();
        assert_eq!(canary.version, TransformVersion::new(2, 0, 0));

        // Promote canary
        catalog.promote_canary("enrich").await.unwrap();
        let active = catalog.get_active("enrich").await.unwrap();
        assert_eq!(active.version, TransformVersion::new(2, 0, 0));
    }

    #[tokio::test]
    async fn test_transform_chain() {
        let catalog = TransformCatalog::new();

        let chain = TransformChain {
            id: "pipeline-1".to_string(),
            name: "ETL Pipeline".to_string(),
            description: "Extract → Transform → Load".to_string(),
            steps: vec![
                ChainStep {
                    transform_id: "extract".to_string(),
                    version: TransformVersion::new(1, 0, 0),
                    config_overrides: HashMap::new(),
                    order: 1,
                },
                ChainStep {
                    transform_id: "transform".to_string(),
                    version: TransformVersion::new(1, 0, 0),
                    config_overrides: HashMap::new(),
                    order: 2,
                },
            ],
            source_topic: "raw-events".to_string(),
            sink_topic: "processed-events".to_string(),
            error_handling: ChainErrorHandling::DeadLetterQueue {
                topic: "dlq".to_string(),
            },
            created_at: Utc::now(),
        };

        catalog.create_chain(chain).await.unwrap();
        let chains = catalog.list_chains().await;
        assert_eq!(chains.len(), 1);
        assert_eq!(chains[0].steps.len(), 2);

        let chain = catalog.get_chain("pipeline-1").await.unwrap();
        assert_eq!(chain.source_topic, "raw-events");

        catalog.delete_chain("pipeline-1").await.unwrap();
        assert!(catalog.list_chains().await.is_empty());
    }

    #[tokio::test]
    async fn test_ab_routing() {
        let catalog = TransformCatalog::new();

        catalog
            .publish(
                "route-test",
                TransformVersion::new(1, 0, 0),
                "v1",
                "h1",
                vec![],
                vec![],
            )
            .await
            .unwrap();
        catalog
            .publish(
                "route-test",
                TransformVersion::new(2, 0, 0),
                "v2",
                "h2",
                vec![],
                vec![],
            )
            .await
            .unwrap();

        catalog
            .activate("route-test", &TransformVersion::new(1, 0, 0))
            .await
            .unwrap();
        catalog
            .start_canary("route-test", &TransformVersion::new(2, 0, 0), 50)
            .await
            .unwrap();

        // Route messages - should split roughly 50/50
        let mut v1_count = 0;
        let mut v2_count = 0;
        for i in 0..100u64 {
            let routed = catalog.route_message("route-test", i).await.unwrap();
            if routed.contains("2.0.0") {
                v2_count += 1;
            } else {
                v1_count += 1;
            }
        }
        assert!(v1_count > 0);
        assert!(v2_count > 0);
    }

    #[test]
    fn test_version_parsing() {
        let v = TransformVersion::parse("1.2.3").unwrap();
        assert_eq!(v, TransformVersion::new(1, 2, 3));
        assert_eq!(v.to_string(), "1.2.3");
        assert!(TransformVersion::parse("invalid").is_err());
    }

    #[test]
    fn test_version_ordering() {
        let v1 = TransformVersion::new(1, 0, 0);
        let v2 = TransformVersion::new(2, 0, 0);
        let v1_1 = TransformVersion::new(1, 1, 0);
        assert!(v1 < v1_1);
        assert!(v1_1 < v2);
    }

    #[test]
    fn test_status_display() {
        assert_eq!(TransformStatus::Active.to_string(), "active");
        assert_eq!(TransformStatus::Canary.to_string(), "canary");
        assert_eq!(TransformStatus::Draft.to_string(), "draft");
    }

    #[test]
    fn test_chain_error_handling_display() {
        assert_eq!(ChainErrorHandling::StopOnError.to_string(), "stop-on-error");
        assert_eq!(
            ChainErrorHandling::DeadLetterQueue {
                topic: "dlq".into()
            }
            .to_string(),
            "dlq(dlq)"
        );
    }
}
