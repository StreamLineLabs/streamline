//! Edge node zero-config bootstrapping (M3 P2).
//!
//! Implements `streamline join --token=... --cluster=...` to allow edge
//! nodes to bootstrap with minimal configuration. The bootstrap process:
//!
//! 1. Validates the join token against the cluster
//! 2. Downloads sync configuration (topics, retention, compression)
//! 3. Registers the edge node with the cluster
//! 4. Returns a `BootstrapResult` with node identity and sync state

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// Configuration for bootstrapping an edge node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapConfig {
    /// Join token issued by the cluster (short-lived, single-use).
    pub token: String,
    /// Cluster URL to join (e.g., `https://cloud.example.com:9094`).
    pub cluster_url: String,
    /// Topics to sync after joining (empty = all topics per cluster policy).
    #[serde(default)]
    pub sync_topics: Vec<String>,
    /// Optional edge node identifier (auto-generated if empty).
    #[serde(default)]
    pub node_id: String,
    /// Maximum time to wait for bootstrap (milliseconds).
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_timeout_ms() -> u64 {
    30_000
}

impl BootstrapConfig {
    /// Creates a new bootstrap config with the minimum required fields.
    pub fn new(token: impl Into<String>, cluster_url: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            cluster_url: cluster_url.into(),
            sync_topics: Vec::new(),
            node_id: String::new(),
            timeout_ms: default_timeout_ms(),
        }
    }

    /// Sets topics to sync.
    pub fn with_sync_topics(mut self, topics: Vec<String>) -> Self {
        self.sync_topics = topics;
        self
    }

    /// Sets the edge node identifier.
    pub fn with_node_id(mut self, id: impl Into<String>) -> Self {
        self.node_id = id.into();
        self
    }
}

/// Result of a successful bootstrap.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapResult {
    /// Assigned node identifier.
    pub node_id: String,
    /// When the node connected to the cluster (epoch milliseconds).
    pub connected_at: u64,
    /// Topics that will be synced.
    pub topics_syncing: Vec<String>,
    /// Cluster identifier.
    pub cluster_id: String,
}

/// Validates the join token format.
///
/// Tokens must be non-empty and at least 16 characters (in production,
/// tokens are signed JWTs or HMAC-based one-time codes).
fn validate_token(token: &str) -> Result<(), BootstrapError> {
    if token.is_empty() {
        return Err(BootstrapError::InvalidToken(
            "token must not be empty".into(),
        ));
    }
    if token.len() < 16 {
        return Err(BootstrapError::InvalidToken(
            "token must be at least 16 characters".into(),
        ));
    }
    Ok(())
}

/// Validates the cluster URL format.
fn validate_cluster_url(url: &str) -> Result<(), BootstrapError> {
    if url.is_empty() {
        return Err(BootstrapError::InvalidConfig(
            "cluster_url must not be empty".into(),
        ));
    }
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(BootstrapError::InvalidConfig(
            "cluster_url must start with http:// or https://".into(),
        ));
    }
    Ok(())
}

/// Bootstraps an edge node with the given configuration.
///
/// In production this makes an HTTP call to the cluster's bootstrap
/// endpoint. This implementation validates configuration and simulates
/// the handshake for local / test usage.
pub fn bootstrap(config: &BootstrapConfig) -> Result<BootstrapResult, BootstrapError> {
    validate_token(&config.token)?;
    validate_cluster_url(&config.cluster_url)?;

    let node_id = if config.node_id.is_empty() {
        format!("edge-{:016x}", {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            now as u64
        })
    } else {
        config.node_id.clone()
    };

    let connected_at = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

    Ok(BootstrapResult {
        node_id,
        connected_at,
        topics_syncing: config.sync_topics.clone(),
        cluster_id: format!(
            "cluster-{}",
            &config.cluster_url[config.cluster_url.len().saturating_sub(8)..]
        ),
    })
}

/// Errors that can occur during bootstrap.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    /// The join token is invalid or expired.
    #[error("invalid token: {0}")]
    InvalidToken(String),
    /// The bootstrap configuration is invalid.
    #[error("invalid config: {0}")]
    InvalidConfig(String),
    /// The cluster rejected the join request.
    #[error("cluster rejected join: {0}")]
    Rejected(String),
    /// Network or transport error.
    #[error("transport error: {0}")]
    Transport(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_succeeds_with_valid_config() {
        let config = BootstrapConfig::new(
            "abcdefghijklmnopqrstuvwxyz",
            "https://cloud.example.com:9094",
        )
        .with_sync_topics(vec!["orders".into(), "events".into()])
        .with_node_id("edge-001");

        let result = bootstrap(&config).unwrap();
        assert_eq!(result.node_id, "edge-001");
        assert_eq!(result.topics_syncing, vec!["orders", "events"]);
        assert!(result.connected_at > 0);
    }

    #[test]
    fn bootstrap_auto_generates_node_id() {
        let config = BootstrapConfig::new(
            "abcdefghijklmnopqrstuvwxyz",
            "https://cloud.example.com:9094",
        );
        let result = bootstrap(&config).unwrap();
        assert!(result.node_id.starts_with("edge-"));
    }

    #[test]
    fn bootstrap_rejects_empty_token() {
        let config = BootstrapConfig::new("", "https://cloud.example.com:9094");
        let err = bootstrap(&config).unwrap_err();
        assert!(matches!(err, BootstrapError::InvalidToken(_)));
    }

    #[test]
    fn bootstrap_rejects_short_token() {
        let config = BootstrapConfig::new("short", "https://cloud.example.com:9094");
        let err = bootstrap(&config).unwrap_err();
        assert!(matches!(err, BootstrapError::InvalidToken(_)));
    }

    #[test]
    fn bootstrap_rejects_invalid_url() {
        let config = BootstrapConfig::new("abcdefghijklmnopqrstuvwxyz", "not-a-url");
        let err = bootstrap(&config).unwrap_err();
        assert!(matches!(err, BootstrapError::InvalidConfig(_)));
    }

    #[test]
    fn bootstrap_rejects_empty_url() {
        let config = BootstrapConfig::new("abcdefghijklmnopqrstuvwxyz", "");
        let err = bootstrap(&config).unwrap_err();
        assert!(matches!(err, BootstrapError::InvalidConfig(_)));
    }
}
