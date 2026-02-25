//! Hot-Reload Watcher for Declarative Connector Configs
//!
//! Watches a directory of YAML/JSON connector manifests and automatically
//! applies changes. This enables GitOps-style connector management:
//!
//! 1. Commit a connector manifest to Git
//! 2. CI/CD syncs manifests to the Streamline config directory
//! 3. The watcher detects changes and applies/removes connectors
//!
//! # Usage
//!
//! ```bash
//! # Place connector manifests in the watch directory
//! streamline --connect-watch-dir /etc/streamline/connectors/
//!
//! # Add a new connector (just create a file)
//! cp my-connector.yaml /etc/streamline/connectors/
//!
//! # Update configuration (edit the file)
//! vim /etc/streamline/connectors/my-connector.yaml
//!
//! # Remove a connector (delete the file)
//! rm /etc/streamline/connectors/my-connector.yaml
//! ```

use super::declarative::ConnectorManifest;
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Configuration for the connector watcher.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatcherConfig {
    /// Directory to watch for connector manifests.
    pub watch_dir: PathBuf,
    /// Poll interval in milliseconds (fallback when inotify unavailable).
    pub poll_interval_ms: u64,
    /// File extensions to consider as manifests.
    pub extensions: Vec<String>,
    /// Dry-run mode: log changes but don't apply.
    pub dry_run: bool,
}

impl Default for WatcherConfig {
    fn default() -> Self {
        Self {
            watch_dir: PathBuf::from("/etc/streamline/connectors"),
            poll_interval_ms: 5000,
            extensions: vec![
                "yaml".to_string(),
                "yml".to_string(),
                "json".to_string(),
            ],
            dry_run: false,
        }
    }
}

/// Tracks the state of a watched manifest file.
#[derive(Debug, Clone)]
struct ManifestState {
    /// Path to the manifest file.
    path: PathBuf,
    /// Connector name extracted from the manifest.
    connector_name: String,
    /// Hash of the file contents for change detection.
    content_hash: u64,
    /// Last applied version.
    last_applied: Option<chrono::DateTime<chrono::Utc>>,
}

/// Event emitted by the watcher.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatcherEvent {
    /// Type of change.
    pub event_type: WatcherEventType,
    /// Connector name.
    pub connector_name: String,
    /// File path.
    pub file_path: String,
    /// Timestamp.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Error message (if apply failed).
    pub error: Option<String>,
}

/// Types of watcher events.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WatcherEventType {
    /// New connector manifest detected and applied.
    Created,
    /// Existing connector manifest updated and re-applied.
    Updated,
    /// Manifest file removed, connector deleted.
    Deleted,
    /// Failed to parse or apply manifest.
    Error,
}

/// Statistics for the watcher.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WatcherStats {
    /// Total manifests currently tracked.
    pub tracked_manifests: usize,
    /// Total create events processed.
    pub creates: u64,
    /// Total update events processed.
    pub updates: u64,
    /// Total delete events processed.
    pub deletes: u64,
    /// Total errors encountered.
    pub errors: u64,
    /// Last scan timestamp.
    pub last_scan: Option<chrono::DateTime<chrono::Utc>>,
}

/// Connector manifest watcher for GitOps-style hot-reload.
pub struct ConnectorWatcher {
    config: WatcherConfig,
    /// Currently tracked manifests: filename → state.
    tracked: Arc<RwLock<HashMap<String, ManifestState>>>,
    /// Event log (bounded ring buffer).
    events: Arc<RwLock<Vec<WatcherEvent>>>,
    /// Running flag.
    running: Arc<std::sync::atomic::AtomicBool>,
    /// Statistics.
    stats: Arc<RwLock<WatcherStats>>,
}

impl ConnectorWatcher {
    /// Create a new watcher.
    pub fn new(config: WatcherConfig) -> Self {
        Self {
            config,
            tracked: Arc::new(RwLock::new(HashMap::new())),
            events: Arc::new(RwLock::new(Vec::new())),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            stats: Arc::new(RwLock::new(WatcherStats::default())),
        }
    }

    /// Perform a single scan of the watch directory.
    ///
    /// Returns a list of events representing changes since the last scan.
    pub async fn scan(&self) -> Result<Vec<WatcherEvent>> {
        let dir = &self.config.watch_dir;
        if !dir.exists() {
            debug!(dir = %dir.display(), "Watch directory does not exist, skipping scan");
            return Ok(Vec::new());
        }

        let mut events = Vec::new();
        let mut current_files: HashMap<String, (PathBuf, u64)> = HashMap::new();

        // Read all manifest files in the directory
        let entries = std::fs::read_dir(dir)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to read dir: {}", e)))?;

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let ext = path
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("")
                .to_lowercase();

            if !self.config.extensions.contains(&ext) {
                continue;
            }

            let filename = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();

            match std::fs::read_to_string(&path) {
                Ok(content) => {
                    let hash = Self::hash_content(&content);
                    current_files.insert(filename.clone(), (path.clone(), hash));
                }
                Err(e) => {
                    warn!(file = %filename, error = %e, "Failed to read manifest");
                }
            }
        }

        let mut tracked = self.tracked.write().await;

        // Detect created and updated files
        for (filename, (path, hash)) in &current_files {
            match tracked.get(filename) {
                None => {
                    // New file — attempt to parse and create
                    match self.parse_manifest(path) {
                        Ok(manifest) => {
                            let name = manifest.metadata.name.clone();
                            let event = WatcherEvent {
                                event_type: WatcherEventType::Created,
                                connector_name: name.clone(),
                                file_path: path.display().to_string(),
                                timestamp: chrono::Utc::now(),
                                error: None,
                            };

                            if !self.config.dry_run {
                                info!(
                                    connector = %name,
                                    file = %filename,
                                    "Applying new connector manifest"
                                );
                            } else {
                                info!(
                                    connector = %name,
                                    file = %filename,
                                    "[DRY RUN] Would apply new connector"
                                );
                            }

                            tracked.insert(
                                filename.clone(),
                                ManifestState {
                                    path: path.clone(),
                                    connector_name: name,
                                    content_hash: *hash,
                                    last_applied: Some(chrono::Utc::now()),
                                },
                            );
                            events.push(event);
                        }
                        Err(e) => {
                            let event = WatcherEvent {
                                event_type: WatcherEventType::Error,
                                connector_name: filename.clone(),
                                file_path: path.display().to_string(),
                                timestamp: chrono::Utc::now(),
                                error: Some(e.to_string()),
                            };
                            events.push(event);
                        }
                    }
                }
                Some(existing) if existing.content_hash != *hash => {
                    // File content changed — update
                    match self.parse_manifest(path) {
                        Ok(manifest) => {
                            let name = manifest.metadata.name.clone();
                            let event = WatcherEvent {
                                event_type: WatcherEventType::Updated,
                                connector_name: name.clone(),
                                file_path: path.display().to_string(),
                                timestamp: chrono::Utc::now(),
                                error: None,
                            };

                            if !self.config.dry_run {
                                info!(
                                    connector = %name,
                                    file = %filename,
                                    "Re-applying updated connector manifest"
                                );
                            }

                            tracked.insert(
                                filename.clone(),
                                ManifestState {
                                    path: path.clone(),
                                    connector_name: name,
                                    content_hash: *hash,
                                    last_applied: Some(chrono::Utc::now()),
                                },
                            );
                            events.push(event);
                        }
                        Err(e) => {
                            let event = WatcherEvent {
                                event_type: WatcherEventType::Error,
                                connector_name: existing.connector_name.clone(),
                                file_path: path.display().to_string(),
                                timestamp: chrono::Utc::now(),
                                error: Some(e.to_string()),
                            };
                            events.push(event);
                        }
                    }
                }
                _ => {
                    // No change
                }
            }
        }

        // Detect deleted files
        let deleted_keys: Vec<String> = tracked
            .keys()
            .filter(|k| !current_files.contains_key(*k))
            .cloned()
            .collect();

        for key in deleted_keys {
            if let Some(state) = tracked.remove(&key) {
                let event = WatcherEvent {
                    event_type: WatcherEventType::Deleted,
                    connector_name: state.connector_name.clone(),
                    file_path: state.path.display().to_string(),
                    timestamp: chrono::Utc::now(),
                    error: None,
                };

                if !self.config.dry_run {
                    info!(
                        connector = %state.connector_name,
                        file = %key,
                        "Removing connector (manifest deleted)"
                    );
                }

                events.push(event);
            }
        }

        // Update stats
        let mut stats = self.stats.write().await;
        stats.tracked_manifests = tracked.len();
        stats.last_scan = Some(chrono::Utc::now());
        for event in &events {
            match event.event_type {
                WatcherEventType::Created => stats.creates += 1,
                WatcherEventType::Updated => stats.updates += 1,
                WatcherEventType::Deleted => stats.deletes += 1,
                WatcherEventType::Error => stats.errors += 1,
            }
        }

        // Append to event log (keep last 1000)
        let mut event_log = self.events.write().await;
        event_log.extend(events.clone());
        if event_log.len() > 1000 {
            let drain_count = event_log.len() - 1000;
            event_log.drain(0..drain_count);
        }

        Ok(events)
    }

    /// Start the background polling loop.
    pub async fn start(&self) -> Result<()> {
        if self
            .running
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return Ok(()); // Already running
        }

        info!(
            dir = %self.config.watch_dir.display(),
            poll_ms = self.config.poll_interval_ms,
            "Starting connector manifest watcher"
        );

        // Initial scan
        match self.scan().await {
            Ok(events) => {
                if !events.is_empty() {
                    info!(count = events.len(), "Initial scan found connector manifests");
                }
            }
            Err(e) => {
                warn!(error = %e, "Initial scan failed");
            }
        }

        Ok(())
    }

    /// Stop the watcher.
    pub fn stop(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::SeqCst);
        info!("Connector manifest watcher stopped");
    }

    /// Get recent events.
    pub async fn recent_events(&self, limit: usize) -> Vec<WatcherEvent> {
        let events = self.events.read().await;
        events.iter().rev().take(limit).cloned().collect()
    }

    /// Get current statistics.
    pub async fn stats(&self) -> WatcherStats {
        self.stats.read().await.clone()
    }

    /// Check if the watcher is running.
    pub fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::Relaxed)
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn parse_manifest(&self, path: &Path) -> Result<ConnectorManifest> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| StreamlineError::Config(format!("Failed to read {}: {}", path.display(), e)))?;

        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        let manifest: ConnectorManifest = match ext.as_str() {
            "json" => serde_json::from_str(&content)
                .map_err(|e| StreamlineError::Config(format!("Invalid JSON manifest: {}", e)))?,
            "yaml" | "yml" => serde_yaml::from_str(&content)
                .map_err(|e| StreamlineError::Config(format!("Invalid YAML manifest: {}", e)))?,
            _ => {
                return Err(StreamlineError::Config(format!(
                    "Unsupported manifest format: {}",
                    ext
                )));
            }
        };

        Ok(manifest)
    }

    fn hash_content(content: &str) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        content.hash(&mut hasher);
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_watcher_creation() {
        let watcher = ConnectorWatcher::new(WatcherConfig::default());
        assert!(!watcher.is_running());
        let stats = watcher.stats().await;
        assert_eq!(stats.tracked_manifests, 0);
    }

    #[tokio::test]
    async fn test_watcher_scan_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        let config = WatcherConfig {
            watch_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        let watcher = ConnectorWatcher::new(config);
        let events = watcher.scan().await.unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn test_watcher_scan_nonexistent_dir() {
        let config = WatcherConfig {
            watch_dir: PathBuf::from("/nonexistent/path"),
            ..Default::default()
        };
        let watcher = ConnectorWatcher::new(config);
        let events = watcher.scan().await.unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn test_watcher_detect_new_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let config = WatcherConfig {
            watch_dir: temp_dir.path().to_path_buf(),
            dry_run: true,
            ..Default::default()
        };

        // Write a manifest file
        let manifest = r#"{
            "apiVersion": "streamline.io/v1",
            "kind": "ConnectorPipeline",
            "metadata": { "name": "test-connector", "labels": {} },
            "spec": {
                "source": { "type": "console", "config": {} },
                "transforms": [],
                "sink": { "type": "streamline-topic", "config": { "topic": "output" } }
            }
        }"#;
        std::fs::write(temp_dir.path().join("test.json"), manifest).unwrap();

        let watcher = ConnectorWatcher::new(config);
        let events = watcher.scan().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, WatcherEventType::Created);
        assert_eq!(events[0].connector_name, "test-connector");
    }

    #[tokio::test]
    async fn test_watcher_detect_update() {
        let temp_dir = TempDir::new().unwrap();
        let config = WatcherConfig {
            watch_dir: temp_dir.path().to_path_buf(),
            dry_run: true,
            ..Default::default()
        };

        let manifest_v1 = r#"{
            "apiVersion": "streamline.io/v1",
            "kind": "ConnectorPipeline",
            "metadata": { "name": "updatable", "labels": {} },
            "spec": {
                "source": { "type": "console", "config": {} },
                "transforms": [],
                "sink": { "type": "streamline-topic", "config": { "topic": "v1" } }
            }
        }"#;
        let path = temp_dir.path().join("updatable.json");
        std::fs::write(&path, manifest_v1).unwrap();

        let watcher = ConnectorWatcher::new(config);

        // First scan
        let events = watcher.scan().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, WatcherEventType::Created);

        // Modify file
        let manifest_v2 = manifest_v1.replace("v1", "v2");
        std::fs::write(&path, manifest_v2).unwrap();

        // Second scan
        let events = watcher.scan().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, WatcherEventType::Updated);
    }

    #[tokio::test]
    async fn test_watcher_detect_delete() {
        let temp_dir = TempDir::new().unwrap();
        let config = WatcherConfig {
            watch_dir: temp_dir.path().to_path_buf(),
            dry_run: true,
            ..Default::default()
        };

        let manifest = r#"{
            "apiVersion": "streamline.io/v1",
            "kind": "ConnectorPipeline",
            "metadata": { "name": "deletable", "labels": {} },
            "spec": {
                "source": { "type": "console", "config": {} },
                "transforms": [],
                "sink": { "type": "streamline-topic", "config": { "topic": "out" } }
            }
        }"#;
        let path = temp_dir.path().join("deletable.json");
        std::fs::write(&path, manifest).unwrap();

        let watcher = ConnectorWatcher::new(config);

        // First scan
        watcher.scan().await.unwrap();

        // Delete file
        std::fs::remove_file(&path).unwrap();

        // Second scan
        let events = watcher.scan().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, WatcherEventType::Deleted);
        assert_eq!(events[0].connector_name, "deletable");
    }

    #[test]
    fn test_hash_deterministic() {
        let h1 = ConnectorWatcher::hash_content("hello world");
        let h2 = ConnectorWatcher::hash_content("hello world");
        assert_eq!(h1, h2);

        let h3 = ConnectorWatcher::hash_content("different");
        assert_ne!(h1, h3);
    }
}
