//! Transactional Outbox Pattern Module
//!
//! Implements the transactional outbox pattern for reliable event publishing
//! from databases to Streamline topics with exactly-once semantics.
//!
//! # Features
//!
//! - **Outbox Table Management**: Automatic creation and schema for outbox tables
//! - **Polling Publisher**: Efficient polling-based event relay
//! - **Exactly-Once Semantics**: Deduplication and ordering guarantees
//! - **Multiple Databases**: Support for PostgreSQL, MySQL, and more
//! - **Dead Letter Queue**: Handle failed messages gracefully
//!
//! # Pattern Overview
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │  Application Transaction                                        │
//! │  ┌──────────────────────┐  ┌──────────────────────┐            │
//! │  │  Business Table(s)   │  │   Outbox Table       │            │
//! │  │  ┌───────────────┐   │  │  ┌────────────────┐  │            │
//! │  │  │ INSERT/UPDATE │   │  │  │ INSERT event   │  │            │
//! │  │  └───────────────┘   │  │  └────────────────┘  │            │
//! │  └──────────────────────┘  └──────────────────────┘            │
//! │            │                           │                        │
//! │            └────────── COMMIT ─────────┘                        │
//! └─────────────────────────────────────────────────────────────────┘
//!                                          │
//!                                          ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │  Outbox Publisher (Polling)                                     │
//! │  ┌────────────────────┐   ┌────────────────────┐               │
//! │  │ Poll outbox table  │──▶│ Publish to topic   │               │
//! │  └────────────────────┘   └────────────────────┘               │
//! │                                    │                            │
//! │                                    ▼                            │
//! │                           ┌────────────────────┐               │
//! │                           │ Mark as published  │               │
//! │                           └────────────────────┘               │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

pub mod config;
pub mod deduplication;
pub mod publisher;
pub mod schema;

pub use config::{OutboxConfig, PublisherConfig};
pub use deduplication::DeduplicationStore;
pub use publisher::OutboxPublisher;

use crate::error::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Outbox manager - coordinates outbox operations
pub struct OutboxManager {
    /// Configuration
    config: OutboxConfig,
    /// Publishers by database connection ID
    publishers: Arc<RwLock<HashMap<String, Arc<OutboxPublisher>>>>,
    /// Deduplication store
    dedup_store: Arc<DeduplicationStore>,
    /// Running flag
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl OutboxManager {
    /// Create a new outbox manager
    pub fn new(config: OutboxConfig) -> Self {
        Self {
            dedup_store: Arc::new(DeduplicationStore::new(config.deduplication.clone())),
            config,
            publishers: Arc::new(RwLock::new(HashMap::new())),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Register a new outbox publisher
    pub async fn register_publisher(
        &self,
        connection_id: impl Into<String>,
        publisher_config: PublisherConfig,
    ) -> Result<()> {
        let id = connection_id.into();
        let publisher =
            OutboxPublisher::new(id.clone(), publisher_config, self.dedup_store.clone());

        let mut publishers = self.publishers.write().await;
        publishers.insert(id, Arc::new(publisher));

        Ok(())
    }

    /// Get a publisher by connection ID
    pub async fn get_publisher(&self, connection_id: &str) -> Option<Arc<OutboxPublisher>> {
        let publishers = self.publishers.read().await;
        publishers.get(connection_id).cloned()
    }

    /// List all publishers
    pub async fn list_publishers(&self) -> Vec<String> {
        let publishers = self.publishers.read().await;
        publishers.keys().cloned().collect()
    }

    /// Start all publishers
    pub async fn start(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        self.running
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let publishers = self.publishers.read().await;
        for (id, publisher) in publishers.iter() {
            tracing::info!(publisher_id = %id, "Starting outbox publisher");
            publisher.start().await?;
        }

        Ok(())
    }

    /// Stop all publishers
    pub async fn stop(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::SeqCst);

        let publishers = self.publishers.read().await;
        for (id, publisher) in publishers.iter() {
            tracing::info!(publisher_id = %id, "Stopping outbox publisher");
            publisher.stop().await;
        }
    }

    /// Get aggregate statistics
    pub async fn stats(&self) -> OutboxManagerStats {
        let publishers = self.publishers.read().await;
        let mut total_published = 0u64;
        let mut total_failed = 0u64;
        let mut total_retried = 0u64;

        for publisher in publishers.values() {
            let stats = publisher.stats();
            total_published += stats.messages_published;
            total_failed += stats.messages_failed;
            total_retried += stats.messages_retried;
        }

        OutboxManagerStats {
            publisher_count: publishers.len(),
            total_published,
            total_failed,
            total_retried,
            dedup_cache_size: self.dedup_store.cache_size().await,
        }
    }

    /// Get configuration
    pub fn config(&self) -> &OutboxConfig {
        &self.config
    }
}

/// Outbox manager statistics
#[derive(Debug, Clone, Default)]
pub struct OutboxManagerStats {
    /// Number of registered publishers
    pub publisher_count: usize,
    /// Total messages published across all publishers
    pub total_published: u64,
    /// Total messages failed
    pub total_failed: u64,
    /// Total messages retried
    pub total_retried: u64,
    /// Deduplication cache size
    pub dedup_cache_size: usize,
}

impl Default for OutboxManager {
    fn default() -> Self {
        Self::new(OutboxConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_outbox_manager_creation() {
        let config = OutboxConfig::default();
        let manager = OutboxManager::new(config);

        assert!(manager.list_publishers().await.is_empty());
    }

    #[tokio::test]
    async fn test_register_publisher() {
        let manager = OutboxManager::default();

        let publisher_config = PublisherConfig::default();
        manager
            .register_publisher("test-db", publisher_config)
            .await
            .unwrap();

        let publishers = manager.list_publishers().await;
        assert_eq!(publishers.len(), 1);
        assert!(publishers.contains(&"test-db".to_string()));
    }
}
