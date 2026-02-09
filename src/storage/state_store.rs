//! External State Store for Stateless Agent Architecture
//!
//! This module provides the infrastructure for externalizing partition state
//! to enable truly stateless Streamline agents. By storing partition metadata
//! (LEO, HWM, lease information) in an external consensus store, agents can:
//!
//! - Start instantly without scanning S3 manifests
//! - Fail over seamlessly via lease-based coordination
//! - Scale horizontally with multi-agent support
//!
//! ## Backends
//!
//! - **DynamoDB**: Recommended for production. Single-digit ms latency with
//!   conditional writes for safe concurrent updates.
//! - **InMemory**: For testing and development.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use streamline::storage::state_store::{StateStore, DynamoDbStateStore, StateStoreConfig};
//!
//! // Create DynamoDB state store
//! let config = StateStoreConfig::dynamodb("streamline-state", "us-east-1");
//! let store = DynamoDbStateStore::new(config).await?;
//!
//! // Get partition state (fast - single DynamoDB read)
//! let state = store.get_partition_state("events", 0).await?;
//!
//! // Update with optimistic locking
//! store.update_partition_state("events", 0, new_state, state.version).await?;
//!
//! // Acquire exclusive lease for writes
//! let lease = store.acquire_lease("events", 0, "agent-1", 30_000).await?;
//! ```

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Result, StreamlineError};

/// Partition state stored in the external state store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionState {
    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition: i32,

    /// Log end offset (next offset to be written)
    pub log_end_offset: i64,

    /// High watermark (last committed/replicated offset)
    pub high_watermark: i64,

    /// Path to the active segment in object storage
    pub active_segment_path: String,

    /// Size of the active segment in bytes
    pub active_segment_bytes: u64,

    /// Version for optimistic locking (CAS operations)
    pub version: u64,

    /// Agent ID that currently owns this partition (if leased)
    pub owner_agent_id: Option<String>,

    /// Lease expiry timestamp in milliseconds since epoch
    pub lease_expiry_ms: Option<i64>,

    /// Last modified timestamp in milliseconds since epoch
    pub last_modified_ms: i64,
}

impl PartitionState {
    /// Create a new partition state
    pub fn new(topic: &str, partition: i32) -> Self {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        Self {
            topic: topic.to_string(),
            partition,
            log_end_offset: 0,
            high_watermark: 0,
            active_segment_path: String::new(),
            active_segment_bytes: 0,
            version: 0,
            owner_agent_id: None,
            lease_expiry_ms: None,
            last_modified_ms: now_ms,
        }
    }

    /// Check if the lease is currently valid
    pub fn is_lease_valid(&self) -> bool {
        if let Some(expiry) = self.lease_expiry_ms {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            expiry > now_ms
        } else {
            false
        }
    }

    /// Check if a specific agent owns the lease
    pub fn is_owner(&self, agent_id: &str) -> bool {
        self.owner_agent_id.as_deref() == Some(agent_id) && self.is_lease_valid()
    }

    /// Generate the partition key for storage
    pub fn partition_key(topic: &str, partition: i32) -> String {
        format!("{}#{}", topic, partition)
    }
}

/// Handle for an acquired partition lease
#[derive(Debug)]
pub struct LeaseHandle {
    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition: i32,

    /// Agent ID that holds the lease
    pub agent_id: String,

    /// Lease expiry timestamp in milliseconds
    pub expiry_ms: i64,

    /// Duration of the lease in milliseconds
    pub duration_ms: u64,

    /// Whether the lease is still valid (may be revoked)
    valid: Arc<std::sync::atomic::AtomicBool>,
}

impl LeaseHandle {
    /// Check if the lease is still valid
    pub fn is_valid(&self) -> bool {
        if !self.valid.load(Ordering::Acquire) {
            return false;
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        now_ms < self.expiry_ms
    }

    /// Revoke the lease (called when releasing)
    pub fn revoke(&self) {
        self.valid.store(false, Ordering::Release);
    }

    /// Get remaining time in milliseconds
    pub fn remaining_ms(&self) -> i64 {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;
        (self.expiry_ms - now_ms).max(0)
    }
}

/// Configuration for the state store
#[derive(Debug, Clone)]
pub struct StateStoreConfig {
    /// Backend type
    pub backend: StateStoreBackend,

    /// Default lease duration in milliseconds
    pub default_lease_duration_ms: u64,

    /// Heartbeat interval for lease renewal (should be < lease_duration / 3)
    pub heartbeat_interval_ms: u64,

    /// Maximum retries for transient failures
    pub max_retries: u32,

    /// Base delay for exponential backoff in milliseconds
    pub retry_base_delay_ms: u64,
}

/// Backend configuration
#[derive(Debug, Clone)]
pub enum StateStoreBackend {
    /// In-memory backend for testing
    InMemory,

    /// DynamoDB backend
    DynamoDb {
        /// DynamoDB table name
        table_name: String,
        /// AWS region
        region: String,
        /// Optional endpoint URL (for local DynamoDB)
        endpoint_url: Option<String>,
    },
}

impl Default for StateStoreConfig {
    fn default() -> Self {
        Self {
            backend: StateStoreBackend::InMemory,
            default_lease_duration_ms: 30_000, // 30 seconds
            heartbeat_interval_ms: 5_000,      // 5 seconds
            max_retries: 3,
            retry_base_delay_ms: 100,
        }
    }
}

impl StateStoreConfig {
    /// Create a DynamoDB configuration
    pub fn dynamodb(table_name: &str, region: &str) -> Self {
        Self {
            backend: StateStoreBackend::DynamoDb {
                table_name: table_name.to_string(),
                region: region.to_string(),
                endpoint_url: None,
            },
            ..Default::default()
        }
    }

    /// Create a DynamoDB configuration with custom endpoint (for local testing)
    pub fn dynamodb_local(table_name: &str, endpoint_url: &str) -> Self {
        Self {
            backend: StateStoreBackend::DynamoDb {
                table_name: table_name.to_string(),
                region: "us-east-1".to_string(),
                endpoint_url: Some(endpoint_url.to_string()),
            },
            ..Default::default()
        }
    }

    /// Create an in-memory configuration for testing
    pub fn in_memory() -> Self {
        Self {
            backend: StateStoreBackend::InMemory,
            ..Default::default()
        }
    }

    /// Set the lease duration
    pub fn with_lease_duration(mut self, duration_ms: u64) -> Self {
        self.default_lease_duration_ms = duration_ms;
        // Heartbeat should be at most 1/3 of lease duration
        self.heartbeat_interval_ms = duration_ms / 6;
        self
    }
}

/// Error type for version conflicts during conditional updates
#[derive(Debug)]
pub struct VersionConflictError {
    pub expected_version: u64,
    pub actual_version: u64,
}

impl std::fmt::Display for VersionConflictError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Version conflict: expected {}, found {}",
            self.expected_version, self.actual_version
        )
    }
}

/// External state store trait for partition metadata
///
/// Implementations provide atomic operations on partition state with
/// optimistic locking for safe concurrent access.
#[async_trait]
pub trait StateStore: Send + Sync + Debug {
    /// Get the current state for a partition
    ///
    /// Returns `None` if the partition has never been initialized.
    async fn get_partition_state(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<PartitionState>>;

    /// Initialize state for a new partition
    ///
    /// Fails if the partition already exists.
    async fn create_partition_state(&self, state: PartitionState) -> Result<()>;

    /// Update partition state with optimistic locking
    ///
    /// The update only succeeds if the current version matches `expected_version`.
    /// On success, the version is incremented automatically.
    ///
    /// Returns `Err(StreamlineError::VersionConflict)` if versions don't match.
    async fn update_partition_state(
        &self,
        topic: &str,
        partition: i32,
        state: PartitionState,
        expected_version: u64,
    ) -> Result<()>;

    /// Update only the LEO (Log End Offset) with optimistic locking
    ///
    /// This is an optimized path for the common case of appending records.
    async fn update_leo(
        &self,
        topic: &str,
        partition: i32,
        new_leo: i64,
        expected_version: u64,
    ) -> Result<u64>;

    /// Update the high watermark
    async fn update_high_watermark(
        &self,
        topic: &str,
        partition: i32,
        new_hwm: i64,
        expected_version: u64,
    ) -> Result<u64>;

    /// Acquire an exclusive lease on a partition
    ///
    /// The lease grants exclusive write access to the partition for the
    /// specified duration. Other agents can still read.
    ///
    /// Returns `Err(StreamlineError::LeaseConflict)` if another agent holds the lease.
    async fn acquire_lease(
        &self,
        topic: &str,
        partition: i32,
        agent_id: &str,
        ttl_ms: u64,
    ) -> Result<LeaseHandle>;

    /// Renew an existing lease
    ///
    /// Extends the lease expiry. Only the current lease holder can renew.
    async fn renew_lease(&self, handle: &LeaseHandle) -> Result<LeaseHandle>;

    /// Release a lease early
    ///
    /// Allows another agent to acquire the partition immediately.
    async fn release_lease(&self, handle: &LeaseHandle) -> Result<()>;

    /// List all partitions for a topic
    async fn list_partitions(&self, topic: &str) -> Result<Vec<PartitionState>>;

    /// Delete partition state (use with caution)
    async fn delete_partition_state(&self, topic: &str, partition: i32) -> Result<()>;

    /// Get statistics about the state store
    fn stats(&self) -> StateStoreStats;
}

/// Statistics for the state store
#[derive(Debug, Clone, Default)]
pub struct StateStoreStats {
    /// Total read operations
    pub reads: u64,
    /// Total write operations
    pub writes: u64,
    /// Conditional write conflicts
    pub conflicts: u64,
    /// Lease acquisitions
    pub lease_acquisitions: u64,
    /// Lease renewals
    pub lease_renewals: u64,
    /// Average read latency in microseconds
    pub avg_read_latency_us: u64,
    /// Average write latency in microseconds
    pub avg_write_latency_us: u64,
}

/// In-memory state store for testing and development
#[derive(Debug)]
pub struct InMemoryStateStore {
    states: parking_lot::RwLock<HashMap<String, PartitionState>>,
    stats: InMemoryStateStoreStats,
}

#[derive(Debug, Default)]
struct InMemoryStateStoreStats {
    reads: AtomicU64,
    writes: AtomicU64,
    conflicts: AtomicU64,
    lease_acquisitions: AtomicU64,
    lease_renewals: AtomicU64,
}

impl InMemoryStateStore {
    /// Create a new in-memory state store
    pub fn new() -> Self {
        Self {
            states: parking_lot::RwLock::new(HashMap::new()),
            stats: InMemoryStateStoreStats::default(),
        }
    }

    fn key(topic: &str, partition: i32) -> String {
        PartitionState::partition_key(topic, partition)
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }
}

impl Default for InMemoryStateStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StateStore for InMemoryStateStore {
    async fn get_partition_state(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Option<PartitionState>> {
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        let states = self.states.read();
        Ok(states.get(&Self::key(topic, partition)).cloned())
    }

    async fn create_partition_state(&self, state: PartitionState) -> Result<()> {
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        let mut states = self.states.write();
        let key = Self::key(&state.topic, state.partition);

        if states.contains_key(&key) {
            return Err(StreamlineError::storage_msg(format!(
                "Partition {}:{} already exists",
                state.topic, state.partition
            )));
        }

        states.insert(key, state);
        Ok(())
    }

    async fn update_partition_state(
        &self,
        topic: &str,
        partition: i32,
        mut state: PartitionState,
        expected_version: u64,
    ) -> Result<()> {
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        let mut states = self.states.write();
        let key = Self::key(topic, partition);

        let current = states.get(&key).ok_or_else(|| {
            StreamlineError::storage_msg(format!("Partition {}:{} not found", topic, partition))
        })?;

        if current.version != expected_version {
            self.stats.conflicts.fetch_add(1, Ordering::Relaxed);
            return Err(StreamlineError::storage_msg(format!(
                "Version conflict: expected {}, found {}",
                expected_version, current.version
            )));
        }

        state.version = expected_version + 1;
        state.last_modified_ms = Self::now_ms();
        states.insert(key, state);
        Ok(())
    }

    async fn update_leo(
        &self,
        topic: &str,
        partition: i32,
        new_leo: i64,
        expected_version: u64,
    ) -> Result<u64> {
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        let mut states = self.states.write();
        let key = Self::key(topic, partition);

        let state = states.get_mut(&key).ok_or_else(|| {
            StreamlineError::storage_msg(format!("Partition {}:{} not found", topic, partition))
        })?;

        if state.version != expected_version {
            self.stats.conflicts.fetch_add(1, Ordering::Relaxed);
            return Err(StreamlineError::storage_msg(format!(
                "Version conflict: expected {}, found {}",
                expected_version, state.version
            )));
        }

        state.log_end_offset = new_leo;
        state.version += 1;
        state.last_modified_ms = Self::now_ms();
        Ok(state.version)
    }

    async fn update_high_watermark(
        &self,
        topic: &str,
        partition: i32,
        new_hwm: i64,
        expected_version: u64,
    ) -> Result<u64> {
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        let mut states = self.states.write();
        let key = Self::key(topic, partition);

        let state = states.get_mut(&key).ok_or_else(|| {
            StreamlineError::storage_msg(format!("Partition {}:{} not found", topic, partition))
        })?;

        if state.version != expected_version {
            self.stats.conflicts.fetch_add(1, Ordering::Relaxed);
            return Err(StreamlineError::storage_msg(format!(
                "Version conflict: expected {}, found {}",
                expected_version, state.version
            )));
        }

        state.high_watermark = new_hwm;
        state.version += 1;
        state.last_modified_ms = Self::now_ms();
        Ok(state.version)
    }

    async fn acquire_lease(
        &self,
        topic: &str,
        partition: i32,
        agent_id: &str,
        ttl_ms: u64,
    ) -> Result<LeaseHandle> {
        self.stats
            .lease_acquisitions
            .fetch_add(1, Ordering::Relaxed);
        let mut states = self.states.write();
        let key = Self::key(topic, partition);

        let state = states.get_mut(&key).ok_or_else(|| {
            StreamlineError::storage_msg(format!("Partition {}:{} not found", topic, partition))
        })?;

        let now_ms = Self::now_ms();

        // Check if there's an existing valid lease by another agent
        if let (Some(owner), Some(expiry)) = (&state.owner_agent_id, state.lease_expiry_ms) {
            if expiry > now_ms && owner != agent_id {
                return Err(StreamlineError::storage_msg(format!(
                    "Lease held by agent {} until {}",
                    owner, expiry
                )));
            }
        }

        // Grant the lease
        let expiry_ms = now_ms + ttl_ms as i64;
        state.owner_agent_id = Some(agent_id.to_string());
        state.lease_expiry_ms = Some(expiry_ms);
        state.version += 1;
        state.last_modified_ms = now_ms;

        Ok(LeaseHandle {
            topic: topic.to_string(),
            partition,
            agent_id: agent_id.to_string(),
            expiry_ms,
            duration_ms: ttl_ms,
            valid: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        })
    }

    async fn renew_lease(&self, handle: &LeaseHandle) -> Result<LeaseHandle> {
        self.stats.lease_renewals.fetch_add(1, Ordering::Relaxed);
        let mut states = self.states.write();
        let key = Self::key(&handle.topic, handle.partition);

        let state = states.get_mut(&key).ok_or_else(|| {
            StreamlineError::storage_msg(format!(
                "Partition {}:{} not found",
                handle.topic, handle.partition
            ))
        })?;

        let now_ms = Self::now_ms();

        // Verify the caller owns the lease
        if state.owner_agent_id.as_deref() != Some(&handle.agent_id) {
            return Err(StreamlineError::storage_msg(format!(
                "Lease not owned by agent {}",
                handle.agent_id
            )));
        }

        // Renew the lease
        let expiry_ms = now_ms + handle.duration_ms as i64;
        state.lease_expiry_ms = Some(expiry_ms);
        state.version += 1;
        state.last_modified_ms = now_ms;

        Ok(LeaseHandle {
            topic: handle.topic.clone(),
            partition: handle.partition,
            agent_id: handle.agent_id.clone(),
            expiry_ms,
            duration_ms: handle.duration_ms,
            valid: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        })
    }

    async fn release_lease(&self, handle: &LeaseHandle) -> Result<()> {
        let mut states = self.states.write();
        let key = Self::key(&handle.topic, handle.partition);

        let state = states.get_mut(&key).ok_or_else(|| {
            StreamlineError::storage_msg(format!(
                "Partition {}:{} not found",
                handle.topic, handle.partition
            ))
        })?;

        // Verify the caller owns the lease
        if state.owner_agent_id.as_deref() != Some(&handle.agent_id) {
            return Err(StreamlineError::storage_msg(format!(
                "Lease not owned by agent {}",
                handle.agent_id
            )));
        }

        // Release the lease
        state.owner_agent_id = None;
        state.lease_expiry_ms = None;
        state.version += 1;
        state.last_modified_ms = Self::now_ms();
        handle.revoke();

        Ok(())
    }

    async fn list_partitions(&self, topic: &str) -> Result<Vec<PartitionState>> {
        self.stats.reads.fetch_add(1, Ordering::Relaxed);
        let states = self.states.read();
        let prefix = format!("{}#", topic);

        Ok(states
            .iter()
            .filter(|(k, _)| k.starts_with(&prefix))
            .map(|(_, v)| v.clone())
            .collect())
    }

    async fn delete_partition_state(&self, topic: &str, partition: i32) -> Result<()> {
        self.stats.writes.fetch_add(1, Ordering::Relaxed);
        let mut states = self.states.write();
        let key = Self::key(topic, partition);
        states.remove(&key);
        Ok(())
    }

    fn stats(&self) -> StateStoreStats {
        StateStoreStats {
            reads: self.stats.reads.load(Ordering::Relaxed),
            writes: self.stats.writes.load(Ordering::Relaxed),
            conflicts: self.stats.conflicts.load(Ordering::Relaxed),
            lease_acquisitions: self.stats.lease_acquisitions.load(Ordering::Relaxed),
            lease_renewals: self.stats.lease_renewals.load(Ordering::Relaxed),
            avg_read_latency_us: 1,  // Negligible for in-memory
            avg_write_latency_us: 1, // Negligible for in-memory
        }
    }
}

/// Factory function to create a state store from configuration
pub async fn create_state_store(config: &StateStoreConfig) -> Result<Arc<dyn StateStore>> {
    match &config.backend {
        StateStoreBackend::InMemory => Ok(Arc::new(InMemoryStateStore::new())),
        StateStoreBackend::DynamoDb { .. } => {
            // DynamoDB implementation would go here
            // For now, fall back to in-memory with a warning
            tracing::warn!("DynamoDB state store not yet implemented, using in-memory fallback");
            Ok(Arc::new(InMemoryStateStore::new()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_partition_state_lifecycle() {
        let store = InMemoryStateStore::new();

        // Create partition state
        let state = PartitionState::new("test-topic", 0);
        store.create_partition_state(state.clone()).await.unwrap();

        // Read it back
        let read_state = store
            .get_partition_state("test-topic", 0)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read_state.topic, "test-topic");
        assert_eq!(read_state.partition, 0);
        assert_eq!(read_state.log_end_offset, 0);
        assert_eq!(read_state.version, 0);
    }

    #[tokio::test]
    async fn test_update_with_version_check() {
        let store = InMemoryStateStore::new();

        // Create partition
        let state = PartitionState::new("test-topic", 0);
        store.create_partition_state(state).await.unwrap();

        // Update LEO with correct version
        let new_version = store.update_leo("test-topic", 0, 100, 0).await.unwrap();
        assert_eq!(new_version, 1);

        // Try to update with stale version - should fail
        let result = store.update_leo("test-topic", 0, 200, 0).await;
        assert!(result.is_err());

        // Update with correct version should work
        let new_version = store.update_leo("test-topic", 0, 200, 1).await.unwrap();
        assert_eq!(new_version, 2);
    }

    #[tokio::test]
    async fn test_lease_acquisition() {
        let store = InMemoryStateStore::new();

        // Create partition
        let state = PartitionState::new("test-topic", 0);
        store.create_partition_state(state).await.unwrap();

        // Agent 1 acquires lease
        let lease1 = store
            .acquire_lease("test-topic", 0, "agent-1", 30_000)
            .await
            .unwrap();
        assert!(lease1.is_valid());
        assert_eq!(lease1.agent_id, "agent-1");

        // Agent 2 tries to acquire - should fail
        let result = store
            .acquire_lease("test-topic", 0, "agent-2", 30_000)
            .await;
        assert!(result.is_err());

        // Agent 1 can renew
        let lease1_renewed = store.renew_lease(&lease1).await.unwrap();
        assert!(lease1_renewed.is_valid());

        // Agent 1 releases
        store.release_lease(&lease1_renewed).await.unwrap();

        // Now agent 2 can acquire
        let lease2 = store
            .acquire_lease("test-topic", 0, "agent-2", 30_000)
            .await
            .unwrap();
        assert!(lease2.is_valid());
        assert_eq!(lease2.agent_id, "agent-2");
    }

    #[tokio::test]
    async fn test_list_partitions() {
        let store = InMemoryStateStore::new();

        // Create multiple partitions
        for i in 0..5 {
            let state = PartitionState::new("test-topic", i);
            store.create_partition_state(state).await.unwrap();
        }

        // Also create a partition for another topic
        let other_state = PartitionState::new("other-topic", 0);
        store.create_partition_state(other_state).await.unwrap();

        // List partitions for test-topic
        let partitions = store.list_partitions("test-topic").await.unwrap();
        assert_eq!(partitions.len(), 5);

        // List partitions for other-topic
        let other_partitions = store.list_partitions("other-topic").await.unwrap();
        assert_eq!(other_partitions.len(), 1);
    }

    #[tokio::test]
    async fn test_stats() {
        let store = InMemoryStateStore::new();

        // Create partition
        let state = PartitionState::new("test-topic", 0);
        store.create_partition_state(state).await.unwrap();

        // Perform some operations
        store.get_partition_state("test-topic", 0).await.unwrap();
        store.update_leo("test-topic", 0, 100, 0).await.unwrap();
        store
            .acquire_lease("test-topic", 0, "agent-1", 30_000)
            .await
            .unwrap();

        let stats = store.stats();
        assert_eq!(stats.reads, 1);
        assert_eq!(stats.writes, 2); // create + update_leo
        assert_eq!(stats.lease_acquisitions, 1);
    }
}
