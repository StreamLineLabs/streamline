//! File watcher for hot-reload of streamline.yaml manifest
//!
//! Monitors the manifest file for changes and triggers re-application
//! of topics and mock data generators.

use crate::devserver::manifest::DevManifest;
use crate::error::{Result, StreamlineError};
use crate::storage::TopicManager;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;

/// File watcher that polls for manifest changes
pub struct ManifestWatcher {
    path: PathBuf,
    topic_manager: Arc<TopicManager>,
    poll_interval: Duration,
    last_modified: Option<SystemTime>,
    shutdown_rx: watch::Receiver<bool>,
}

impl ManifestWatcher {
    pub fn new(
        path: PathBuf,
        topic_manager: Arc<TopicManager>,
        shutdown_rx: watch::Receiver<bool>,
    ) -> Self {
        Self {
            path,
            topic_manager,
            poll_interval: Duration::from_secs(2),
            last_modified: None,
            shutdown_rx,
        }
    }

    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Run the watcher loop
    pub async fn run(&mut self) -> Result<()> {
        // Get initial modification time
        self.last_modified = self.get_modified_time();

        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.poll_interval) => {
                    if let Err(e) = self.check_for_changes().await {
                        tracing::warn!("Error checking manifest changes: {}", e);
                    }
                }
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        tracing::info!("Manifest watcher shutting down");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn check_for_changes(&mut self) -> Result<()> {
        let current_modified = self.get_modified_time();

        let changed = match (self.last_modified, current_modified) {
            (Some(last), Some(current)) => current > last,
            (None, Some(_)) => true, // File appeared
            _ => false,
        };

        if changed {
            tracing::info!("Manifest file changed, reloading...");
            self.last_modified = current_modified;

            match self.reload_manifest().await {
                Ok(()) => tracing::info!("Manifest reloaded successfully"),
                Err(e) => tracing::error!("Failed to reload manifest: {}", e),
            }
        }

        Ok(())
    }

    async fn reload_manifest(&self) -> Result<()> {
        let content = tokio::fs::read_to_string(&self.path)
            .await
            .map_err(|e| StreamlineError::Config(format!("Failed to read manifest: {}", e)))?;

        let manifest = DevManifest::from_yaml(&content)?;

        // Apply new topics
        for topic in &manifest.topics {
            let partitions = topic.partitions.unwrap_or(1);
            match self.topic_manager.create_topic(&topic.name, partitions) {
                Ok(_) => {
                    tracing::info!("Created topic '{}' ({} partitions)", topic.name, partitions)
                }
                Err(StreamlineError::TopicAlreadyExists(_)) => {
                    // Topic already exists, skip
                }
                Err(e) => tracing::warn!("Failed to create topic '{}': {}", topic.name, e),
            }
        }

        Ok(())
    }

    fn get_modified_time(&self) -> Option<SystemTime> {
        std::fs::metadata(&self.path)
            .and_then(|m| m.modified())
            .ok()
    }
}

/// Check if a manifest file exists at the given path
pub fn find_manifest(dir: &Path) -> Option<PathBuf> {
    let candidates = [
        dir.join("streamline.yaml"),
        dir.join("streamline.yml"),
        dir.join(".streamline.yaml"),
        dir.join(".streamline.yml"),
    ];

    candidates.into_iter().find(|p| p.exists())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_manifest_none() {
        let result = find_manifest(Path::new("/nonexistent/path"));
        assert!(result.is_none());
    }
}
