//! Partition Lease Manager for Stateless Agent Architecture
//!
//! This module provides lease-based partition ownership for multi-agent
//! coordination in S3-native deployments. Unlike the traditional Raft-based
//! leadership, leases use an external state store for coordination.
//!
//! ## Key Features
//!
//! - **Lease-based ownership**: Partitions are owned via time-limited leases
//! - **Background heartbeats**: Automatic lease renewal via background tasks
//! - **Graceful handoff**: Leases can be released early for fast failover
//! - **Multi-agent coordination**: Multiple agents can coordinate partition access
//!
//! ## Usage
//!
//! ```rust,ignore
//! use streamline::cluster::partition_lease::{PartitionLeaseManager, LeaseConfig};
//! use streamline::storage::state_store::InMemoryStateStore;
//!
//! let state_store = Arc::new(InMemoryStateStore::new());
//! let config = LeaseConfig::default();
//! let manager = PartitionLeaseManager::new("agent-1", state_store, config);
//!
//! // Acquire lease for a partition
//! let lease = manager.acquire("events", 0).await?;
//!
//! // Check if we own the partition
//! if manager.is_owner("events", 0) {
//!     // Safe to write
//! }
//!
//! // Release on shutdown
//! manager.release_all().await;
//! ```

use crate::error::{Result, StreamlineError};
use crate::storage::state_store::{LeaseHandle, StateStore};
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

/// Configuration for the partition lease manager
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    /// Duration of each lease in milliseconds
    pub lease_duration_ms: u64,

    /// Heartbeat interval for lease renewal
    /// Should be < lease_duration / 3 to ensure renewals complete before expiry
    pub heartbeat_interval_ms: u64,

    /// Maximum number of partitions this agent can own
    pub max_partitions_per_agent: usize,

    /// Grace period before attempting to steal an expired lease (ms)
    /// This prevents multiple agents from racing to acquire the same partition
    pub steal_grace_period_ms: u64,

    /// Whether to block on lease acquisition or return immediately if unavailable
    pub block_on_acquire: bool,

    /// Maximum wait time when blocking on acquisition (ms)
    pub acquire_timeout_ms: u64,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            lease_duration_ms: 30_000,      // 30 seconds
            heartbeat_interval_ms: 5_000,   // 5 seconds (1/6 of lease duration)
            max_partitions_per_agent: 1000, // Max partitions per agent
            steal_grace_period_ms: 5_000,   // 5 seconds grace before stealing
            block_on_acquire: false,        // Don't block by default
            acquire_timeout_ms: 60_000,     // 1 minute max wait
        }
    }
}

impl LeaseConfig {
    /// Create a configuration with shorter timeouts for testing
    pub fn for_testing() -> Self {
        Self {
            lease_duration_ms: 3_000,   // 3 seconds
            heartbeat_interval_ms: 500, // 500ms
            max_partitions_per_agent: 100,
            steal_grace_period_ms: 500,
            block_on_acquire: false,
            acquire_timeout_ms: 5_000,
        }
    }

    /// Create a configuration optimized for low latency
    pub fn low_latency() -> Self {
        Self {
            lease_duration_ms: 10_000,    // 10 seconds
            heartbeat_interval_ms: 1_000, // 1 second
            max_partitions_per_agent: 500,
            steal_grace_period_ms: 2_000,
            block_on_acquire: false,
            acquire_timeout_ms: 30_000,
        }
    }

    /// Create a configuration optimized for stability
    pub fn high_availability() -> Self {
        Self {
            lease_duration_ms: 60_000,     // 60 seconds
            heartbeat_interval_ms: 10_000, // 10 seconds
            max_partitions_per_agent: 2000,
            steal_grace_period_ms: 10_000,
            block_on_acquire: true,
            acquire_timeout_ms: 120_000, // 2 minutes
        }
    }
}

/// Event emitted when lease status changes
#[derive(Debug, Clone)]
pub enum LeaseEvent {
    /// Lease was acquired for a partition
    Acquired {
        topic: String,
        partition: i32,
        expiry_ms: i64,
    },
    /// Lease was renewed
    Renewed {
        topic: String,
        partition: i32,
        expiry_ms: i64,
    },
    /// Lease was lost (expired or stolen)
    Lost {
        topic: String,
        partition: i32,
        reason: String,
    },
    /// Lease was voluntarily released
    Released { topic: String, partition: i32 },
}

/// Partition lease manager for multi-agent coordination
pub struct PartitionLeaseManager {
    /// Unique identifier for this agent
    agent_id: String,

    /// External state store for lease persistence
    state_store: Arc<dyn StateStore>,

    /// Configuration
    config: LeaseConfig,

    /// Active leases held by this agent
    /// Key: (topic, partition)
    active_leases: DashMap<(String, i32), Arc<LeaseHandle>>,

    /// Shutdown flag
    shutdown: Arc<AtomicBool>,

    /// Background heartbeat task handle
    heartbeat_task: RwLock<Option<JoinHandle<()>>>,

    /// Event broadcaster for lease changes
    event_tx: broadcast::Sender<LeaseEvent>,

    /// Statistics
    stats: LeaseManagerStats,
}

/// Statistics for the lease manager
#[derive(Debug, Default)]
pub struct LeaseManagerStats {
    /// Total lease acquisitions attempted
    pub acquisitions_attempted: AtomicU64,
    /// Successful lease acquisitions
    pub acquisitions_succeeded: AtomicU64,
    /// Failed lease acquisitions (partition already owned)
    pub acquisitions_failed: AtomicU64,
    /// Total lease renewals
    pub renewals: AtomicU64,
    /// Failed renewals (lease lost)
    pub renewals_failed: AtomicU64,
    /// Total leases released
    pub releases: AtomicU64,
    /// Leases lost due to timeout
    pub leases_lost: AtomicU64,
}

impl LeaseManagerStats {
    /// Get statistics as a snapshot
    pub fn snapshot(&self) -> LeaseManagerStatsSnapshot {
        LeaseManagerStatsSnapshot {
            acquisitions_attempted: self.acquisitions_attempted.load(Ordering::Relaxed),
            acquisitions_succeeded: self.acquisitions_succeeded.load(Ordering::Relaxed),
            acquisitions_failed: self.acquisitions_failed.load(Ordering::Relaxed),
            renewals: self.renewals.load(Ordering::Relaxed),
            renewals_failed: self.renewals_failed.load(Ordering::Relaxed),
            releases: self.releases.load(Ordering::Relaxed),
            leases_lost: self.leases_lost.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of lease manager statistics
#[derive(Debug, Clone)]
pub struct LeaseManagerStatsSnapshot {
    pub acquisitions_attempted: u64,
    pub acquisitions_succeeded: u64,
    pub acquisitions_failed: u64,
    pub renewals: u64,
    pub renewals_failed: u64,
    pub releases: u64,
    pub leases_lost: u64,
}

impl PartitionLeaseManager {
    /// Create a new partition lease manager
    pub fn new(
        agent_id: impl Into<String>,
        state_store: Arc<dyn StateStore>,
        config: LeaseConfig,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(1024);

        Self {
            agent_id: agent_id.into(),
            state_store,
            config,
            active_leases: DashMap::new(),
            shutdown: Arc::new(AtomicBool::new(false)),
            heartbeat_task: RwLock::new(None),
            event_tx,
            stats: LeaseManagerStats::default(),
        }
    }

    /// Get the agent ID
    pub fn agent_id(&self) -> &str {
        &self.agent_id
    }

    /// Get the configuration
    pub fn config(&self) -> &LeaseConfig {
        &self.config
    }

    /// Start the background heartbeat task for lease renewal
    pub async fn start(&self) {
        let mut task_guard = self.heartbeat_task.write().await;
        if task_guard.is_some() {
            warn!(agent_id = %self.agent_id, "Heartbeat task already running");
            return;
        }

        info!(
            agent_id = %self.agent_id,
            heartbeat_interval_ms = self.config.heartbeat_interval_ms,
            "Starting partition lease heartbeat task"
        );

        let agent_id = self.agent_id.clone();
        let state_store = self.state_store.clone();
        let active_leases = self.active_leases.clone();
        let shutdown = self.shutdown.clone();
        let event_tx = self.event_tx.clone();
        let heartbeat_interval = Duration::from_millis(self.config.heartbeat_interval_ms);
        let stats = LeaseManagerStats::default();

        let task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            while !shutdown.load(Ordering::Acquire) {
                interval.tick().await;

                // Renew all active leases
                let lease_entries: Vec<_> = active_leases
                    .iter()
                    .map(|entry| {
                        let (key, handle) = entry.pair();
                        (key.clone(), Arc::clone(handle))
                    })
                    .collect();

                for ((topic, partition), lease) in lease_entries {
                    // Skip if lease is already invalid
                    if !lease.is_valid() {
                        debug!(
                            agent_id = %agent_id,
                            topic = %topic,
                            partition,
                            "Removing invalid lease"
                        );
                        active_leases.remove(&(topic.clone(), partition));
                        let _ = event_tx.send(LeaseEvent::Lost {
                            topic: topic.clone(),
                            partition,
                            reason: "Lease expired".to_string(),
                        });
                        stats.leases_lost.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }

                    // Attempt to renew
                    match state_store.renew_lease(&lease).await {
                        Ok(new_handle) => {
                            debug!(
                                agent_id = %agent_id,
                                topic = %topic,
                                partition,
                                expiry_ms = new_handle.expiry_ms,
                                "Lease renewed"
                            );

                            // Update the stored handle
                            active_leases.insert((topic.clone(), partition), Arc::new(new_handle));
                            stats.renewals.fetch_add(1, Ordering::Relaxed);

                            let new_lease = active_leases.get(&(topic.clone(), partition));
                            if let Some(l) = new_lease {
                                let _ = event_tx.send(LeaseEvent::Renewed {
                                    topic: topic.clone(),
                                    partition,
                                    expiry_ms: l.expiry_ms,
                                });
                            }
                        }
                        Err(e) => {
                            warn!(
                                agent_id = %agent_id,
                                topic = %topic,
                                partition,
                                error = %e,
                                "Failed to renew lease"
                            );
                            active_leases.remove(&(topic.clone(), partition));
                            stats.renewals_failed.fetch_add(1, Ordering::Relaxed);
                            stats.leases_lost.fetch_add(1, Ordering::Relaxed);

                            let _ = event_tx.send(LeaseEvent::Lost {
                                topic: topic.clone(),
                                partition,
                                reason: format!("Renewal failed: {}", e),
                            });
                        }
                    }
                }
            }

            debug!(agent_id = %agent_id, "Heartbeat task shutting down");
        });

        *task_guard = Some(task);
    }

    /// Acquire a lease for a partition
    pub async fn acquire(&self, topic: &str, partition: i32) -> Result<Arc<LeaseHandle>> {
        // Check if we already own this partition
        if let Some(lease) = self.active_leases.get(&(topic.to_string(), partition)) {
            if lease.is_valid() {
                return Ok(Arc::clone(&lease));
            }
            // Remove invalid lease
            drop(lease);
            self.active_leases.remove(&(topic.to_string(), partition));
        }

        // Check capacity
        if self.active_leases.len() >= self.config.max_partitions_per_agent {
            return Err(StreamlineError::Cluster(format!(
                "Agent {} has reached max partitions ({})",
                self.agent_id, self.config.max_partitions_per_agent
            )));
        }

        self.stats
            .acquisitions_attempted
            .fetch_add(1, Ordering::Relaxed);

        // Try to acquire lease
        let result = self
            .state_store
            .acquire_lease(
                topic,
                partition,
                &self.agent_id,
                self.config.lease_duration_ms,
            )
            .await;

        match result {
            Ok(handle) => {
                let handle = Arc::new(handle);
                info!(
                    agent_id = %self.agent_id,
                    topic = %topic,
                    partition,
                    expiry_ms = handle.expiry_ms,
                    "Acquired partition lease"
                );

                self.active_leases
                    .insert((topic.to_string(), partition), Arc::clone(&handle));
                self.stats
                    .acquisitions_succeeded
                    .fetch_add(1, Ordering::Relaxed);

                let _ = self.event_tx.send(LeaseEvent::Acquired {
                    topic: topic.to_string(),
                    partition,
                    expiry_ms: handle.expiry_ms,
                });

                Ok(handle)
            }
            Err(e) => {
                self.stats
                    .acquisitions_failed
                    .fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Try to acquire a lease, returning None if unavailable (non-blocking)
    pub async fn try_acquire(&self, topic: &str, partition: i32) -> Option<Arc<LeaseHandle>> {
        match self.acquire(topic, partition).await {
            Ok(lease) => Some(lease),
            Err(e) => {
                debug!(
                    agent_id = %self.agent_id,
                    topic = %topic,
                    partition,
                    error = %e,
                    "Failed to acquire lease"
                );
                None
            }
        }
    }

    /// Release a lease for a partition
    pub async fn release(&self, topic: &str, partition: i32) -> Result<()> {
        let lease = self
            .active_leases
            .remove(&(topic.to_string(), partition))
            .map(|(_, v)| v);

        if let Some(lease) = lease {
            match self.state_store.release_lease(&lease).await {
                Ok(()) => {
                    info!(
                        agent_id = %self.agent_id,
                        topic = %topic,
                        partition,
                        "Released partition lease"
                    );
                    self.stats.releases.fetch_add(1, Ordering::Relaxed);

                    let _ = self.event_tx.send(LeaseEvent::Released {
                        topic: topic.to_string(),
                        partition,
                    });

                    Ok(())
                }
                Err(e) => {
                    warn!(
                        agent_id = %self.agent_id,
                        topic = %topic,
                        partition,
                        error = %e,
                        "Error releasing lease (may have already expired)"
                    );
                    // Still count as released from our perspective
                    self.stats.releases.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            }
        } else {
            // We don't hold this lease
            Ok(())
        }
    }

    /// Release all leases (for graceful shutdown)
    pub async fn release_all(&self) {
        info!(
            agent_id = %self.agent_id,
            leases = self.active_leases.len(),
            "Releasing all partition leases"
        );

        let keys: Vec<_> = self
            .active_leases
            .iter()
            .map(|entry| entry.key().clone())
            .collect();

        for (topic, partition) in keys {
            if let Err(e) = self.release(&topic, partition).await {
                warn!(
                    agent_id = %self.agent_id,
                    topic = %topic,
                    partition,
                    error = %e,
                    "Error releasing lease during shutdown"
                );
            }
        }
    }

    /// Check if this agent owns a partition
    ///
    /// Also proactively removes expired leases from the active set and
    /// emits a Lost event so callers are notified promptly.
    pub fn is_owner(&self, topic: &str, partition: i32) -> bool {
        let key = (topic.to_string(), partition);
        if let Some(lease) = self.active_leases.get(&key) {
            if lease.is_valid() {
                return true;
            }
            // Proactively clean up expired lease
            drop(lease);
            if let Some((_, _removed)) = self.active_leases.remove(&key) {
                let _ = self.event_tx.send(LeaseEvent::Lost {
                    topic: topic.to_string(),
                    partition,
                    reason: "Lease expired (detected during ownership check)".to_string(),
                });
                self.stats.leases_lost.fetch_add(1, Ordering::Relaxed);
            }
        }
        false
    }

    /// Check remaining time on a lease in milliseconds.
    /// Returns None if the lease is not held or already expired.
    pub fn remaining_lease_ms(&self, topic: &str, partition: i32) -> Option<i64> {
        let key = (topic.to_string(), partition);
        self.active_leases.get(&key).and_then(|lease| {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let remaining = lease.expiry_ms - now_ms;
            if remaining > 0 {
                Some(remaining)
            } else {
                None
            }
        })
    }

    /// Get the lease for a partition if owned
    pub fn get_lease(&self, topic: &str, partition: i32) -> Option<Arc<LeaseHandle>> {
        self.active_leases
            .get(&(topic.to_string(), partition))
            .filter(|lease| lease.is_valid())
            .map(|lease| Arc::clone(&lease))
    }

    /// Get all partitions owned by this agent
    pub fn owned_partitions(&self) -> Vec<(String, i32)> {
        self.active_leases
            .iter()
            .filter(|entry| entry.value().is_valid())
            .map(|entry| entry.key().clone())
            .collect()
    }

    /// Get the number of partitions owned
    pub fn owned_count(&self) -> usize {
        self.active_leases
            .iter()
            .filter(|entry| entry.value().is_valid())
            .count()
    }

    /// Subscribe to lease events
    pub fn subscribe(&self) -> broadcast::Receiver<LeaseEvent> {
        self.event_tx.subscribe()
    }

    /// Get statistics
    pub fn stats(&self) -> LeaseManagerStatsSnapshot {
        self.stats.snapshot()
    }

    /// Graceful shutdown
    pub async fn shutdown(&self) {
        info!(agent_id = %self.agent_id, "Shutting down partition lease manager");

        // Signal shutdown
        self.shutdown.store(true, Ordering::Release);

        // Stop heartbeat task
        let mut task_guard = self.heartbeat_task.write().await;
        if let Some(task) = task_guard.take() {
            task.abort();
            let _ = task.await;
        }

        // Release all leases
        self.release_all().await;

        info!(agent_id = %self.agent_id, "Partition lease manager shutdown complete");
    }
}

impl Drop for PartitionLeaseManager {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::state_store::{InMemoryStateStore, PartitionState};

    fn create_test_store() -> Arc<InMemoryStateStore> {
        Arc::new(InMemoryStateStore::new())
    }

    async fn create_partition(store: &InMemoryStateStore, topic: &str, partition: i32) {
        store
            .create_partition_state(PartitionState::new(topic, partition))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_lease_acquisition() {
        let store = create_test_store();
        let config = LeaseConfig::for_testing();

        // Create partition state first
        create_partition(&store, "events", 0).await;
        create_partition(&store, "events", 1).await;

        let manager = PartitionLeaseManager::new("agent-1", store as Arc<dyn StateStore>, config);

        // Acquire a lease
        let lease = manager.acquire("events", 0).await.unwrap();
        assert!(lease.is_valid());
        assert_eq!(lease.agent_id, "agent-1");

        // Verify ownership
        assert!(manager.is_owner("events", 0));
        assert!(!manager.is_owner("events", 1));

        // Acquiring again should return the same lease
        let lease2 = manager.acquire("events", 0).await.unwrap();
        assert_eq!(lease.expiry_ms, lease2.expiry_ms);
    }

    #[tokio::test]
    async fn test_lease_conflict() {
        let store = create_test_store();
        let config = LeaseConfig::for_testing();

        // Create partition state first
        create_partition(&store, "events", 0).await;

        let manager1 = PartitionLeaseManager::new(
            "agent-1",
            Arc::clone(&store) as Arc<dyn StateStore>,
            config.clone(),
        );
        let manager2 = PartitionLeaseManager::new("agent-2", store as Arc<dyn StateStore>, config);

        // Agent 1 acquires the lease
        let _lease1 = manager1.acquire("events", 0).await.unwrap();
        assert!(manager1.is_owner("events", 0));

        // Agent 2 should fail to acquire
        let result = manager2.acquire("events", 0).await;
        assert!(result.is_err());
        assert!(!manager2.is_owner("events", 0));
    }

    #[tokio::test]
    async fn test_lease_release() {
        let store = create_test_store();
        let config = LeaseConfig::for_testing();

        // Create partition state first
        create_partition(&store, "events", 0).await;

        let manager1 = PartitionLeaseManager::new(
            "agent-1",
            Arc::clone(&store) as Arc<dyn StateStore>,
            config.clone(),
        );
        let manager2 = PartitionLeaseManager::new("agent-2", store as Arc<dyn StateStore>, config);

        // Agent 1 acquires and releases
        let _lease = manager1.acquire("events", 0).await.unwrap();
        manager1.release("events", 0).await.unwrap();
        assert!(!manager1.is_owner("events", 0));

        // Agent 2 can now acquire
        let lease2 = manager2.acquire("events", 0).await.unwrap();
        assert!(lease2.is_valid());
        assert!(manager2.is_owner("events", 0));
    }

    #[tokio::test]
    async fn test_owned_partitions() {
        let store = create_test_store();
        let config = LeaseConfig::for_testing();

        // Create partition states first
        create_partition(&store, "events", 0).await;
        create_partition(&store, "events", 1).await;
        create_partition(&store, "logs", 0).await;

        let manager = PartitionLeaseManager::new("agent-1", store as Arc<dyn StateStore>, config);

        // Acquire multiple partitions
        manager.acquire("events", 0).await.unwrap();
        manager.acquire("events", 1).await.unwrap();
        manager.acquire("logs", 0).await.unwrap();

        let owned = manager.owned_partitions();
        assert_eq!(owned.len(), 3);
        assert!(owned.contains(&("events".to_string(), 0)));
        assert!(owned.contains(&("events".to_string(), 1)));
        assert!(owned.contains(&("logs".to_string(), 0)));

        assert_eq!(manager.owned_count(), 3);
    }

    #[tokio::test]
    async fn test_release_all() {
        let store = create_test_store();
        let config = LeaseConfig::for_testing();

        // Create partition states first
        create_partition(&store, "events", 0).await;
        create_partition(&store, "events", 1).await;
        create_partition(&store, "logs", 0).await;

        let manager = PartitionLeaseManager::new("agent-1", store as Arc<dyn StateStore>, config);

        // Acquire multiple partitions
        manager.acquire("events", 0).await.unwrap();
        manager.acquire("events", 1).await.unwrap();
        manager.acquire("logs", 0).await.unwrap();
        assert_eq!(manager.owned_count(), 3);

        // Release all
        manager.release_all().await;
        assert_eq!(manager.owned_count(), 0);
    }

    #[tokio::test]
    async fn test_stats() {
        let store = create_test_store();
        let config = LeaseConfig::for_testing();

        // Create partition state first
        create_partition(&store, "events", 0).await;

        let manager = PartitionLeaseManager::new("agent-1", store as Arc<dyn StateStore>, config);

        manager.acquire("events", 0).await.unwrap();
        manager.release("events", 0).await.unwrap();

        let stats = manager.stats();
        assert_eq!(stats.acquisitions_attempted, 1);
        assert_eq!(stats.acquisitions_succeeded, 1);
        assert_eq!(stats.releases, 1);
    }

    #[tokio::test]
    async fn test_max_partitions_limit() {
        let store = create_test_store();
        let mut config = LeaseConfig::for_testing();
        config.max_partitions_per_agent = 2;

        // Create partition states first
        create_partition(&store, "topic", 0).await;
        create_partition(&store, "topic", 1).await;
        create_partition(&store, "topic", 2).await;

        let manager = PartitionLeaseManager::new("agent-1", store as Arc<dyn StateStore>, config);

        manager.acquire("topic", 0).await.unwrap();
        manager.acquire("topic", 1).await.unwrap();

        // Should fail due to limit
        let result = manager.acquire("topic", 2).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("max partitions"));
    }

    #[tokio::test]
    async fn test_event_subscription() {
        let store = create_test_store();
        let config = LeaseConfig::for_testing();

        // Create partition state first
        create_partition(&store, "events", 0).await;

        let manager = PartitionLeaseManager::new("agent-1", store as Arc<dyn StateStore>, config);

        let mut rx = manager.subscribe();

        // Acquire a lease
        manager.acquire("events", 0).await.unwrap();

        // Should receive acquired event
        let event = rx.try_recv().unwrap();
        match event {
            LeaseEvent::Acquired {
                topic, partition, ..
            } => {
                assert_eq!(topic, "events");
                assert_eq!(partition, 0);
            }
            _ => panic!("Expected Acquired event"),
        }

        // Release the lease
        manager.release("events", 0).await.unwrap();

        // Should receive released event
        let event = rx.try_recv().unwrap();
        match event {
            LeaseEvent::Released { topic, partition } => {
                assert_eq!(topic, "events");
                assert_eq!(partition, 0);
            }
            _ => panic!("Expected Released event"),
        }
    }

    #[tokio::test]
    async fn test_shutdown() {
        let store = create_test_store();
        let config = LeaseConfig::for_testing();

        // Create partition states first
        create_partition(&store, "events", 0).await;
        create_partition(&store, "events", 1).await;

        let manager = PartitionLeaseManager::new(
            "agent-1",
            Arc::clone(&store) as Arc<dyn StateStore>,
            config,
        );

        // Acquire some partitions
        manager.acquire("events", 0).await.unwrap();
        manager.acquire("events", 1).await.unwrap();

        // Shutdown
        manager.shutdown().await;

        // Should no longer own any partitions
        assert_eq!(manager.owned_count(), 0);
    }
}
