// Multi-DC replication is scaffolding for future geo-distributed deployment feature
#![allow(dead_code)]

//! Multi-datacenter replication for disaster recovery and geo-distribution
//!
//! This module implements asynchronous replication between datacenters,
//! enabling disaster recovery, read replicas in multiple regions, and
//! active-active configurations.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    MultiDcReplicationManager                     │
//! │  Coordinates cross-datacenter replication for all topics        │
//! ├─────────────────────────────────────────────────────────────────┤
//! │                                                                 │
//! │  ┌──────────────────┐      ┌──────────────────────────────┐    │
//! │  │ DcConnectionMgr  │      │ ReplicationCoordinator       │    │
//! │  │ (DC connections) │      │ (async replication logic)    │    │
//! │  └────────┬─────────┘      └────────────┬─────────────────┘    │
//! │           │                              │                      │
//! │  ┌────────┴──────────────────────────────┴────────┐            │
//! │  │                                                │            │
//! │  │  TopicReplicationState per (topic, dc)        │            │
//! │  │  - tracks offsets, lag, and health            │            │
//! │  │                                                │            │
//! │  └────────────────────────────────────────────────┘            │
//! │                                                                 │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Replication Modes
//!
//! - **Async**: Records are replicated asynchronously with configurable lag tolerance
//! - **Semi-sync**: Wait for at least one remote DC ack before acknowledging producer
//! - **Active-Active**: Bidirectional replication with conflict resolution

use crate::cluster::node::NodeId;
use crate::error::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, RwLock};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Unique identifier for a datacenter
pub type DatacenterId = String;

/// Default port for inter-DC replication
pub const DEFAULT_INTER_DC_PORT: u16 = 9094;

/// Default replication batch size
pub const DEFAULT_BATCH_SIZE: usize = 1000;

/// Default replication interval in milliseconds
pub const DEFAULT_REPLICATION_INTERVAL_MS: u64 = 100;

/// Default lag threshold before alerting (in milliseconds)
pub const DEFAULT_LAG_THRESHOLD_MS: u64 = 30000;

/// Replication mode between datacenters
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ReplicationMode {
    /// Asynchronous replication - fire and forget with best-effort delivery
    #[default]
    Async,

    /// Semi-synchronous - wait for at least one remote DC ack
    SemiSync,

    /// Active-Active with bidirectional replication and conflict resolution
    ActiveActive,
}

impl std::fmt::Display for ReplicationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplicationMode::Async => write!(f, "async"),
            ReplicationMode::SemiSync => write!(f, "semi-sync"),
            ReplicationMode::ActiveActive => write!(f, "active-active"),
        }
    }
}

/// Conflict resolution strategy for active-active replication
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ConflictResolution {
    /// Last write wins based on timestamp
    #[default]
    LastWriteWins,

    /// Primary DC always wins
    PrimaryWins,

    /// Higher offset wins
    HigherOffsetWins,

    /// Custom resolution (requires handler)
    Custom,
}

impl std::fmt::Display for ConflictResolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConflictResolution::LastWriteWins => write!(f, "last-write-wins"),
            ConflictResolution::PrimaryWins => write!(f, "primary-wins"),
            ConflictResolution::HigherOffsetWins => write!(f, "higher-offset-wins"),
            ConflictResolution::Custom => write!(f, "custom"),
        }
    }
}

/// Information about a remote datacenter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatacenterInfo {
    /// Unique datacenter identifier
    pub dc_id: DatacenterId,

    /// Human-readable name
    pub name: String,

    /// Geographic region (e.g., "us-east-1", "eu-west-1")
    pub region: String,

    /// Replication endpoint addresses (multiple for HA)
    pub endpoints: Vec<SocketAddr>,

    /// Whether this is the primary DC for writes
    pub is_primary: bool,

    /// Whether this DC is currently active
    pub active: bool,

    /// Priority for failover (lower = higher priority)
    pub failover_priority: u32,

    /// Estimated network latency to this DC in milliseconds
    pub estimated_latency_ms: u64,
}

impl DatacenterInfo {
    /// Create a new datacenter info
    pub fn new(dc_id: DatacenterId, name: String, region: String) -> Self {
        Self {
            dc_id,
            name,
            region,
            endpoints: Vec::new(),
            is_primary: false,
            active: true,
            failover_priority: 100,
            estimated_latency_ms: 0,
        }
    }

    /// Add an endpoint
    pub fn with_endpoint(mut self, addr: SocketAddr) -> Self {
        self.endpoints.push(addr);
        self
    }

    /// Set as primary
    #[allow(clippy::wrong_self_convention)]
    pub fn as_primary(mut self) -> Self {
        self.is_primary = true;
        self.failover_priority = 0;
        self
    }

    /// Set failover priority
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.failover_priority = priority;
        self
    }
}

/// Configuration for multi-DC replication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiDcConfig {
    /// Local datacenter ID
    pub local_dc_id: DatacenterId,

    /// Replication mode
    pub mode: ReplicationMode,

    /// Conflict resolution strategy (for active-active)
    pub conflict_resolution: ConflictResolution,

    /// Remote datacenters
    pub remote_dcs: Vec<DatacenterInfo>,

    /// Topics to replicate (empty = all)
    pub topics: Vec<String>,

    /// Maximum batch size for replication
    pub batch_size: usize,

    /// Replication check interval in milliseconds
    pub replication_interval_ms: u64,

    /// Lag threshold before alerting (milliseconds)
    pub lag_threshold_ms: u64,

    /// Connection timeout in milliseconds
    pub connection_timeout_ms: u64,

    /// Whether to enable compression for cross-DC traffic
    pub compression_enabled: bool,

    /// TLS configuration for inter-DC communication
    pub tls_enabled: bool,

    /// Retry configuration
    pub max_retries: u32,

    /// Retry backoff base in milliseconds
    pub retry_backoff_ms: u64,
}

impl Default for MultiDcConfig {
    fn default() -> Self {
        Self {
            local_dc_id: "dc1".to_string(),
            mode: ReplicationMode::Async,
            conflict_resolution: ConflictResolution::LastWriteWins,
            remote_dcs: Vec::new(),
            topics: Vec::new(),
            batch_size: DEFAULT_BATCH_SIZE,
            replication_interval_ms: DEFAULT_REPLICATION_INTERVAL_MS,
            lag_threshold_ms: DEFAULT_LAG_THRESHOLD_MS,
            connection_timeout_ms: 10000,
            compression_enabled: true,
            tls_enabled: false,
            max_retries: 3,
            retry_backoff_ms: 1000,
        }
    }
}

impl MultiDcConfig {
    /// Create a new config with local DC ID
    pub fn new(local_dc_id: DatacenterId) -> Self {
        Self {
            local_dc_id,
            ..Default::default()
        }
    }

    /// Add a remote datacenter
    pub fn with_remote_dc(mut self, dc: DatacenterInfo) -> Self {
        self.remote_dcs.push(dc);
        self
    }

    /// Set replication mode
    pub fn with_mode(mut self, mode: ReplicationMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set conflict resolution
    pub fn with_conflict_resolution(mut self, strategy: ConflictResolution) -> Self {
        self.conflict_resolution = strategy;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.local_dc_id.is_empty() {
            return Err(StreamlineError::Config(
                "Local DC ID cannot be empty".to_string(),
            ));
        }

        if self.batch_size == 0 {
            return Err(StreamlineError::Config(
                "Batch size must be greater than 0".to_string(),
            ));
        }

        if self.mode == ReplicationMode::ActiveActive
            && self.conflict_resolution == ConflictResolution::Custom
        {
            // Custom conflict resolution requires a handler
            warn!("Active-Active mode with Custom conflict resolution requires a handler");
        }

        // Check for duplicate DC IDs
        let mut seen_ids = std::collections::HashSet::new();
        for dc in &self.remote_dcs {
            if dc.dc_id == self.local_dc_id {
                return Err(StreamlineError::Config(format!(
                    "Remote DC '{}' has same ID as local DC",
                    dc.dc_id
                )));
            }
            if !seen_ids.insert(&dc.dc_id) {
                return Err(StreamlineError::Config(format!(
                    "Duplicate DC ID: {}",
                    dc.dc_id
                )));
            }
        }

        Ok(())
    }
}

/// State of replication for a topic-partition to a remote DC
#[derive(Debug)]
pub(crate) struct TopicPartitionReplicationState {
    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition_id: i32,

    /// Remote DC ID
    pub remote_dc_id: DatacenterId,

    /// Last replicated offset
    pub last_replicated_offset: AtomicI64,

    /// Local high watermark (what we need to replicate to)
    pub local_hwm: AtomicI64,

    /// Timestamp of last successful replication
    pub last_success_time: RwLock<Instant>,

    /// Current replication lag in milliseconds
    pub lag_ms: AtomicU64,

    /// Number of consecutive failures
    pub consecutive_failures: AtomicU64,

    /// Whether replication is paused
    pub paused: RwLock<bool>,
}

impl TopicPartitionReplicationState {
    /// Create a new replication state
    pub fn new(topic: String, partition_id: i32, remote_dc_id: DatacenterId) -> Self {
        Self {
            topic,
            partition_id,
            remote_dc_id,
            last_replicated_offset: AtomicI64::new(-1),
            local_hwm: AtomicI64::new(0),
            last_success_time: RwLock::new(Instant::now()),
            lag_ms: AtomicU64::new(0),
            consecutive_failures: AtomicU64::new(0),
            paused: RwLock::new(false),
        }
    }

    /// Get the replication lag in offsets
    pub fn offset_lag(&self) -> i64 {
        let hwm = self.local_hwm.load(Ordering::Relaxed);
        let replicated = self.last_replicated_offset.load(Ordering::Relaxed);
        if replicated < 0 {
            hwm
        } else {
            hwm - replicated
        }
    }

    /// Update after successful replication
    pub async fn record_success(&self, offset: i64) {
        self.last_replicated_offset.store(offset, Ordering::Relaxed);
        *self.last_success_time.write().await = Instant::now();
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.lag_ms.store(0, Ordering::Relaxed);
    }

    /// Update after failed replication
    pub fn record_failure(&self) {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Check if replication is healthy
    pub async fn is_healthy(&self, lag_threshold_ms: u64) -> bool {
        if *self.paused.read().await {
            return true; // Paused is not unhealthy
        }

        let lag = self.lag_ms.load(Ordering::Relaxed);
        let failures = self.consecutive_failures.load(Ordering::Relaxed);

        lag < lag_threshold_ms && failures < 3
    }
}

/// Connection state to a remote datacenter
#[derive(Debug)]
pub(crate) struct DcConnection {
    /// Remote DC info
    pub dc_info: DatacenterInfo,

    /// Whether currently connected
    pub connected: RwLock<bool>,

    /// Current endpoint index (for failover)
    pub current_endpoint_idx: RwLock<usize>,

    /// Last connection attempt time
    pub last_attempt: RwLock<Instant>,

    /// Number of consecutive connection failures
    pub failures: AtomicU64,
}

impl DcConnection {
    /// Create a new DC connection
    pub fn new(dc_info: DatacenterInfo) -> Self {
        Self {
            dc_info,
            connected: RwLock::new(false),
            current_endpoint_idx: RwLock::new(0),
            last_attempt: RwLock::new(Instant::now()),
            failures: AtomicU64::new(0),
        }
    }

    /// Get the current endpoint to connect to
    pub async fn current_endpoint(&self) -> Option<SocketAddr> {
        let idx = *self.current_endpoint_idx.read().await;
        self.dc_info.endpoints.get(idx).copied()
    }

    /// Try next endpoint on connection failure
    pub async fn try_next_endpoint(&self) -> Option<SocketAddr> {
        let mut idx = self.current_endpoint_idx.write().await;
        *idx = (*idx + 1) % self.dc_info.endpoints.len().max(1);
        self.dc_info.endpoints.get(*idx).copied()
    }
}

/// Commands for the replication coordinator
#[derive(Debug)]
pub(crate) enum ReplicationCommand {
    /// Start replicating a topic-partition
    StartReplication {
        topic: String,
        partition_id: i32,
        remote_dc_id: DatacenterId,
    },

    /// Stop replicating a topic-partition
    StopReplication {
        topic: String,
        partition_id: i32,
        remote_dc_id: DatacenterId,
    },

    /// Pause all replication to a DC
    PauseDc { dc_id: DatacenterId },

    /// Resume replication to a DC
    ResumeDc { dc_id: DatacenterId },

    /// Force sync from a specific offset
    ForceSync {
        topic: String,
        partition_id: i32,
        remote_dc_id: DatacenterId,
        from_offset: i64,
    },

    /// Shutdown the coordinator
    Shutdown,
}

/// Statistics for multi-DC replication
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MultiDcStats {
    /// Total records replicated
    pub records_replicated: u64,

    /// Total bytes replicated
    pub bytes_replicated: u64,

    /// Number of replication batches sent
    pub batches_sent: u64,

    /// Number of failed batches
    pub batches_failed: u64,

    /// Average replication latency in milliseconds
    pub avg_latency_ms: u64,

    /// Per-DC statistics
    pub dc_stats: HashMap<DatacenterId, DcReplicationStats>,
}

/// Statistics for replication to a specific DC
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DcReplicationStats {
    /// DC identifier
    pub dc_id: DatacenterId,

    /// Records replicated to this DC
    pub records_replicated: u64,

    /// Bytes replicated to this DC
    pub bytes_replicated: u64,

    /// Current lag in milliseconds
    pub current_lag_ms: u64,

    /// Current lag in offsets (max across all partitions)
    pub max_offset_lag: i64,

    /// Whether connection is healthy
    pub healthy: bool,

    /// Last successful replication timestamp (unix millis)
    pub last_success_ms: u64,
}

/// Record to be replicated across datacenters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReplicationRecord {
    /// Source datacenter ID
    pub source_dc: DatacenterId,

    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition_id: i32,

    /// Offset in source partition
    pub offset: i64,

    /// Timestamp of original write (milliseconds since epoch)
    pub timestamp_ms: u64,

    /// Record key (if any)
    pub key: Option<Vec<u8>>,

    /// Record value
    pub value: Vec<u8>,

    /// Record headers
    pub headers: Vec<(String, Vec<u8>)>,
}

impl ReplicationRecord {
    /// Create a new replication record
    pub fn new(
        source_dc: DatacenterId,
        topic: String,
        partition_id: i32,
        offset: i64,
        key: Option<Vec<u8>>,
        value: Vec<u8>,
    ) -> Self {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            source_dc,
            topic,
            partition_id,
            offset,
            timestamp_ms,
            key,
            value,
            headers: Vec::new(),
        }
    }

    /// Add a header
    pub fn with_header(mut self, key: String, value: Vec<u8>) -> Self {
        self.headers.push((key, value));
        self
    }
}

/// Batch of records to replicate
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReplicationBatch {
    /// Batch ID for deduplication
    pub batch_id: u64,

    /// Source datacenter
    pub source_dc: DatacenterId,

    /// Target datacenter
    pub target_dc: DatacenterId,

    /// Records in this batch
    pub records: Vec<ReplicationRecord>,

    /// Whether this batch requires acknowledgment
    pub requires_ack: bool,

    /// Compression codec used (if any)
    pub compression: Option<String>,
}

impl ReplicationBatch {
    /// Create a new replication batch
    pub fn new(source_dc: DatacenterId, target_dc: DatacenterId) -> Self {
        Self {
            batch_id: 0,
            source_dc,
            target_dc,
            records: Vec::new(),
            requires_ack: false,
            compression: None,
        }
    }

    /// Add a record to the batch
    pub fn add_record(&mut self, record: ReplicationRecord) {
        self.records.push(record);
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Get number of records
    pub fn len(&self) -> usize {
        self.records.len()
    }
}

/// Response to a replication batch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReplicationResponse {
    /// Batch ID this responds to
    pub batch_id: u64,

    /// Whether the batch was successfully applied
    pub success: bool,

    /// Error message if not successful
    pub error: Option<String>,

    /// Offsets written in target DC
    pub written_offsets: HashMap<(String, i32), i64>,
}

/// Manages connections to remote datacenters
#[derive(Debug)]
pub(crate) struct DcConnectionManager {
    /// Local DC ID
    local_dc_id: DatacenterId,

    /// Connections by DC ID
    connections: RwLock<HashMap<DatacenterId, Arc<DcConnection>>>,

    /// Connection timeout
    connection_timeout: Duration,
}

impl DcConnectionManager {
    /// Create a new connection manager
    pub fn new(local_dc_id: DatacenterId, connection_timeout_ms: u64) -> Self {
        Self {
            local_dc_id,
            connections: RwLock::new(HashMap::new()),
            connection_timeout: Duration::from_millis(connection_timeout_ms),
        }
    }

    /// Register a remote DC
    pub async fn register_dc(&self, dc_info: DatacenterInfo) {
        let dc_id = dc_info.dc_id.clone();
        let connection = Arc::new(DcConnection::new(dc_info));
        self.connections.write().await.insert(dc_id, connection);
    }

    /// Get connection for a DC
    pub async fn get_connection(&self, dc_id: &DatacenterId) -> Option<Arc<DcConnection>> {
        self.connections.read().await.get(dc_id).cloned()
    }

    /// Try to connect to a DC
    pub async fn connect(&self, dc_id: &DatacenterId) -> Result<()> {
        let conn = self
            .get_connection(dc_id)
            .await
            .ok_or_else(|| StreamlineError::Cluster(format!("Unknown DC: {}", dc_id)))?;

        let endpoint = conn.current_endpoint().await.ok_or_else(|| {
            StreamlineError::Cluster(format!("No endpoints configured for DC: {}", dc_id))
        })?;

        info!(
            local_dc = %self.local_dc_id,
            remote_dc = %dc_id,
            endpoint = %endpoint,
            "Connecting to remote datacenter"
        );

        *conn.last_attempt.write().await = Instant::now();

        match tokio::time::timeout(self.connection_timeout, TcpStream::connect(endpoint)).await {
            Ok(Ok(_stream)) => {
                *conn.connected.write().await = true;
                conn.failures.store(0, Ordering::Relaxed);
                info!(
                    local_dc = %self.local_dc_id,
                    remote_dc = %dc_id,
                    "Connected to remote datacenter"
                );
                Ok(())
            }
            Ok(Err(e)) => {
                conn.failures.fetch_add(1, Ordering::Relaxed);
                // Try next endpoint
                conn.try_next_endpoint().await;
                Err(StreamlineError::Io(e))
            }
            Err(_) => {
                conn.failures.fetch_add(1, Ordering::Relaxed);
                conn.try_next_endpoint().await;
                Err(StreamlineError::Cluster("Connection timeout".to_string()))
            }
        }
    }

    /// Check if connected to a DC
    pub async fn is_connected(&self, dc_id: &DatacenterId) -> bool {
        if let Some(conn) = self.get_connection(dc_id).await {
            *conn.connected.read().await
        } else {
            false
        }
    }

    /// Disconnect from a DC
    pub async fn disconnect(&self, dc_id: &DatacenterId) {
        if let Some(conn) = self.get_connection(dc_id).await {
            *conn.connected.write().await = false;
            debug!(dc_id = %dc_id, "Disconnected from remote datacenter");
        }
    }

    /// Get all registered DCs
    pub async fn list_dcs(&self) -> Vec<DatacenterId> {
        self.connections.read().await.keys().cloned().collect()
    }
}

/// Coordinates replication across datacenters
#[derive(Debug)]
pub(crate) struct ReplicationCoordinator {
    /// Local DC ID
    local_dc_id: DatacenterId,

    /// Configuration
    config: MultiDcConfig,

    /// Local node ID
    node_id: NodeId,

    /// Connection manager
    connection_manager: Arc<DcConnectionManager>,

    /// Replication state by (topic, partition, dc_id)
    replication_states:
        RwLock<HashMap<(String, i32, DatacenterId), Arc<TopicPartitionReplicationState>>>,

    /// Statistics
    stats: RwLock<MultiDcStats>,

    /// Next batch ID
    next_batch_id: AtomicU64,

    /// Command receiver
    command_rx: RwLock<Option<mpsc::Receiver<ReplicationCommand>>>,

    /// Command sender (for external use)
    command_tx: mpsc::Sender<ReplicationCommand>,
}

impl ReplicationCoordinator {
    /// Create a new replication coordinator
    pub fn new(node_id: NodeId, config: MultiDcConfig) -> Self {
        let (command_tx, command_rx) = mpsc::channel(1024);

        let connection_manager = Arc::new(DcConnectionManager::new(
            config.local_dc_id.clone(),
            config.connection_timeout_ms,
        ));

        Self {
            local_dc_id: config.local_dc_id.clone(),
            config,
            node_id,
            connection_manager,
            replication_states: RwLock::new(HashMap::new()),
            stats: RwLock::new(MultiDcStats::default()),
            next_batch_id: AtomicU64::new(1),
            command_rx: RwLock::new(Some(command_rx)),
            command_tx,
        }
    }

    /// Get the local node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the command sender
    pub fn command_tx(&self) -> mpsc::Sender<ReplicationCommand> {
        self.command_tx.clone()
    }

    /// Get connection manager
    pub fn connection_manager(&self) -> Arc<DcConnectionManager> {
        self.connection_manager.clone()
    }

    /// Initialize connections to remote DCs
    pub async fn initialize(&self) -> Result<()> {
        info!(
            local_dc = %self.local_dc_id,
            mode = %self.config.mode,
            remote_dcs = self.config.remote_dcs.len(),
            "Initializing multi-DC replication coordinator"
        );

        // Register all remote DCs
        for dc in &self.config.remote_dcs {
            self.connection_manager.register_dc(dc.clone()).await;
        }

        // Try to establish initial connections
        for dc in &self.config.remote_dcs {
            if dc.active {
                if let Err(e) = self.connection_manager.connect(&dc.dc_id).await {
                    warn!(
                        dc_id = %dc.dc_id,
                        error = %e,
                        "Failed to connect to remote DC during initialization"
                    );
                }
            }
        }

        Ok(())
    }

    /// Start replication for a topic-partition to a remote DC
    pub async fn start_replication(
        &self,
        topic: &str,
        partition_id: i32,
        remote_dc_id: &DatacenterId,
    ) {
        let key = (topic.to_string(), partition_id, remote_dc_id.clone());

        let state = Arc::new(TopicPartitionReplicationState::new(
            topic.to_string(),
            partition_id,
            remote_dc_id.clone(),
        ));

        self.replication_states.write().await.insert(key, state);

        info!(
            topic,
            partition_id,
            remote_dc = %remote_dc_id,
            "Started multi-DC replication for partition"
        );
    }

    /// Stop replication for a topic-partition to a remote DC
    pub async fn stop_replication(
        &self,
        topic: &str,
        partition_id: i32,
        remote_dc_id: &DatacenterId,
    ) {
        let key = (topic.to_string(), partition_id, remote_dc_id.clone());
        self.replication_states.write().await.remove(&key);

        debug!(
            topic,
            partition_id,
            remote_dc = %remote_dc_id,
            "Stopped multi-DC replication for partition"
        );
    }

    /// Get replication state for a partition
    pub async fn get_state(
        &self,
        topic: &str,
        partition_id: i32,
        remote_dc_id: &DatacenterId,
    ) -> Option<Arc<TopicPartitionReplicationState>> {
        let key = (topic.to_string(), partition_id, remote_dc_id.clone());
        self.replication_states.read().await.get(&key).cloned()
    }

    /// Update local high watermark for a partition
    pub async fn update_hwm(&self, topic: &str, partition_id: i32, hwm: i64) {
        let states = self.replication_states.read().await;

        for ((t, p, _dc), state) in states.iter() {
            if t == topic && *p == partition_id {
                state.local_hwm.store(hwm, Ordering::Relaxed);
            }
        }
    }

    /// Replicate a batch of records to a remote DC
    pub async fn replicate_batch(&self, batch: ReplicationBatch) -> Result<ReplicationResponse> {
        let dc_id = &batch.target_dc;

        // Check if connected
        if !self.connection_manager.is_connected(dc_id).await {
            // Try to reconnect
            self.connection_manager.connect(dc_id).await?;
        }

        // In a real implementation, this would serialize and send the batch
        // For now, we simulate the replication
        debug!(
            batch_id = batch.batch_id,
            target_dc = %dc_id,
            records = batch.len(),
            "Replicating batch to remote DC"
        );

        // Simulate network latency based on DC info
        if let Some(conn) = self.connection_manager.get_connection(dc_id).await {
            let latency = conn.dc_info.estimated_latency_ms;
            if latency > 0 {
                tokio::time::sleep(Duration::from_millis(latency)).await;
            }
        }

        // Update statistics
        {
            let mut stats = self.stats.write().await;
            stats.records_replicated += batch.len() as u64;
            stats.batches_sent += 1;

            let dc_stats =
                stats
                    .dc_stats
                    .entry(dc_id.clone())
                    .or_insert_with(|| DcReplicationStats {
                        dc_id: dc_id.clone(),
                        ..Default::default()
                    });
            dc_stats.records_replicated += batch.len() as u64;
            dc_stats.healthy = true;
            dc_stats.last_success_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
        }

        // Update replication states
        let mut written_offsets = HashMap::new();
        for record in &batch.records {
            let key = (record.topic.clone(), record.partition_id, dc_id.clone());

            if let Some(state) = self.replication_states.read().await.get(&key) {
                state.record_success(record.offset).await;
            }

            written_offsets.insert((record.topic.clone(), record.partition_id), record.offset);
        }

        Ok(ReplicationResponse {
            batch_id: batch.batch_id,
            success: true,
            error: None,
            written_offsets,
        })
    }

    /// Get the next batch ID
    pub fn next_batch_id(&self) -> u64 {
        self.next_batch_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Check health of all DC connections
    pub async fn check_health(&self) -> HashMap<DatacenterId, bool> {
        let mut health = HashMap::new();

        for dc in &self.config.remote_dcs {
            let connected = self.connection_manager.is_connected(&dc.dc_id).await;
            health.insert(dc.dc_id.clone(), connected);
        }

        health
    }

    /// Get current statistics
    pub async fn stats(&self) -> MultiDcStats {
        self.stats.read().await.clone()
    }

    /// Get replication lag for a specific DC
    pub async fn get_dc_lag(&self, dc_id: &DatacenterId) -> i64 {
        let states = self.replication_states.read().await;
        let mut max_lag: i64 = 0;

        for ((_, _, dc), state) in states.iter() {
            if dc == dc_id {
                max_lag = max_lag.max(state.offset_lag());
            }
        }

        max_lag
    }

    /// Run the replication loop
    pub async fn run(&self) {
        info!(
            local_dc = %self.local_dc_id,
            "Starting multi-DC replication loop"
        );

        let cmd_rx = self.command_rx.write().await.take();
        let Some(mut cmd_rx) = cmd_rx else {
            error!("Replication command receiver already taken");
            return;
        };

        let mut interval = interval(Duration::from_millis(self.config.replication_interval_ms));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Periodic replication check
                    self.check_and_replicate().await;
                }

                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        ReplicationCommand::StartReplication { topic, partition_id, remote_dc_id } => {
                            self.start_replication(&topic, partition_id, &remote_dc_id).await;
                        }
                        ReplicationCommand::StopReplication { topic, partition_id, remote_dc_id } => {
                            self.stop_replication(&topic, partition_id, &remote_dc_id).await;
                        }
                        ReplicationCommand::PauseDc { dc_id } => {
                            self.pause_dc(&dc_id).await;
                        }
                        ReplicationCommand::ResumeDc { dc_id } => {
                            self.resume_dc(&dc_id).await;
                        }
                        ReplicationCommand::ForceSync { topic, partition_id, remote_dc_id, from_offset } => {
                            self.force_sync(&topic, partition_id, &remote_dc_id, from_offset).await;
                        }
                        ReplicationCommand::Shutdown => {
                            info!(local_dc = %self.local_dc_id, "Shutting down replication coordinator");
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Check for data to replicate and send batches
    async fn check_and_replicate(&self) {
        // In a real implementation, this would:
        // 1. Check for new records since last replicated offset
        // 2. Build batches per target DC
        // 3. Send batches and handle responses
        //
        // For now, this is a placeholder that demonstrates the structure
        debug!(
            local_dc = %self.local_dc_id,
            "Checking for records to replicate"
        );
    }

    /// Pause replication to a DC
    async fn pause_dc(&self, dc_id: &DatacenterId) {
        let states = self.replication_states.read().await;

        for ((_, _, dc), state) in states.iter() {
            if dc == dc_id {
                *state.paused.write().await = true;
            }
        }

        info!(dc_id = %dc_id, "Paused replication to DC");
    }

    /// Resume replication to a DC
    async fn resume_dc(&self, dc_id: &DatacenterId) {
        let states = self.replication_states.read().await;

        for ((_, _, dc), state) in states.iter() {
            if dc == dc_id {
                *state.paused.write().await = false;
            }
        }

        info!(dc_id = %dc_id, "Resumed replication to DC");
    }

    /// Force sync from a specific offset
    async fn force_sync(
        &self,
        topic: &str,
        partition_id: i32,
        remote_dc_id: &DatacenterId,
        from_offset: i64,
    ) {
        let key = (topic.to_string(), partition_id, remote_dc_id.clone());

        if let Some(state) = self.replication_states.read().await.get(&key) {
            state
                .last_replicated_offset
                .store(from_offset - 1, Ordering::Relaxed);
            info!(
                topic,
                partition_id,
                remote_dc = %remote_dc_id,
                from_offset,
                "Forced sync from offset"
            );
        }
    }

    /// Shutdown the coordinator
    pub async fn shutdown(&self) {
        let _ = self.command_tx.send(ReplicationCommand::Shutdown).await;
    }
}

/// DC failover event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DcFailoverEvent {
    /// Timestamp of the event
    pub timestamp_ms: u64,

    /// DC that failed
    pub failed_dc: DatacenterId,

    /// DC that took over
    pub new_primary_dc: Option<DatacenterId>,

    /// Reason for failover
    pub reason: String,

    /// Whether automatic failover was triggered
    pub automatic: bool,
}

/// Handles datacenter failover
#[derive(Debug)]
pub(crate) struct DcFailoverHandler {
    /// Local DC ID
    local_dc_id: DatacenterId,

    /// Configuration
    config: MultiDcConfig,

    /// Current primary DC
    primary_dc: RwLock<DatacenterId>,

    /// Failover history
    history: RwLock<Vec<DcFailoverEvent>>,

    /// Whether failover is in progress
    failover_in_progress: RwLock<bool>,
}

impl DcFailoverHandler {
    /// Create a new failover handler
    pub fn new(config: MultiDcConfig) -> Self {
        let primary = config
            .remote_dcs
            .iter()
            .find(|dc| dc.is_primary)
            .map(|dc| dc.dc_id.clone())
            .unwrap_or_else(|| config.local_dc_id.clone());

        Self {
            local_dc_id: config.local_dc_id.clone(),
            config,
            primary_dc: RwLock::new(primary),
            history: RwLock::new(Vec::new()),
            failover_in_progress: RwLock::new(false),
        }
    }

    /// Get current primary DC
    pub async fn primary_dc(&self) -> DatacenterId {
        self.primary_dc.read().await.clone()
    }

    /// Check if local DC is primary
    pub async fn is_local_primary(&self) -> bool {
        *self.primary_dc.read().await == self.local_dc_id
    }

    /// Trigger failover to next available DC
    pub async fn trigger_failover(&self, failed_dc: &DatacenterId, reason: &str) -> Result<()> {
        // Prevent concurrent failovers
        {
            let mut in_progress = self.failover_in_progress.write().await;
            if *in_progress {
                return Err(StreamlineError::Cluster(
                    "Failover already in progress".to_string(),
                ));
            }
            *in_progress = true;
        }

        info!(
            failed_dc = %failed_dc,
            reason,
            "Initiating DC failover"
        );

        // Find next DC by priority
        let mut candidates: Vec<_> = self
            .config
            .remote_dcs
            .iter()
            .filter(|dc| dc.active && &dc.dc_id != failed_dc)
            .collect();

        // Include local DC as candidate if not failed
        let local_candidate = if &self.local_dc_id != failed_dc {
            Some(DatacenterInfo {
                dc_id: self.local_dc_id.clone(),
                name: "local".to_string(),
                region: "local".to_string(),
                endpoints: Vec::new(),
                is_primary: false,
                active: true,
                failover_priority: 50, // Default priority for local
                estimated_latency_ms: 0,
            })
        } else {
            None
        };

        if let Some(ref local) = local_candidate {
            candidates.push(local);
        }

        // Sort by priority
        candidates.sort_by_key(|dc| dc.failover_priority);

        let new_primary = candidates.first().map(|dc| dc.dc_id.clone());

        // Record failover event
        let event = DcFailoverEvent {
            timestamp_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            failed_dc: failed_dc.clone(),
            new_primary_dc: new_primary.clone(),
            reason: reason.to_string(),
            automatic: true,
        };

        self.history.write().await.push(event);

        // Update primary
        if let Some(new_primary_dc) = new_primary {
            *self.primary_dc.write().await = new_primary_dc.clone();
            info!(
                new_primary = %new_primary_dc,
                "Failover complete, new primary DC elected"
            );
        } else {
            warn!("No available DC for failover, system degraded");
        }

        *self.failover_in_progress.write().await = false;
        Ok(())
    }

    /// Get failover history
    pub async fn history(&self) -> Vec<DcFailoverEvent> {
        self.history.read().await.clone()
    }
}

/// Main manager for multi-DC replication
pub struct MultiDcReplicationManager {
    /// Configuration
    config: MultiDcConfig,

    /// Local node ID
    node_id: NodeId,

    /// Replication coordinator
    coordinator: Arc<ReplicationCoordinator>,

    /// Failover handler
    failover_handler: Arc<DcFailoverHandler>,
}

impl MultiDcReplicationManager {
    /// Create a new multi-DC replication manager
    pub fn new(node_id: NodeId, config: MultiDcConfig) -> Self {
        let coordinator = Arc::new(ReplicationCoordinator::new(node_id, config.clone()));
        let failover_handler = Arc::new(DcFailoverHandler::new(config.clone()));

        Self {
            config,
            node_id,
            coordinator,
            failover_handler,
        }
    }

    /// Get the coordinator
    pub(crate) fn coordinator(&self) -> Arc<ReplicationCoordinator> {
        self.coordinator.clone()
    }

    /// Get the failover handler
    pub(crate) fn failover_handler(&self) -> Arc<DcFailoverHandler> {
        self.failover_handler.clone()
    }

    /// Initialize and start replication
    pub async fn start(&self) -> Result<()> {
        self.config.validate()?;
        self.coordinator.initialize().await?;

        // Start replication loop in background
        let coordinator = self.coordinator.clone();
        tokio::spawn(async move {
            coordinator.run().await;
        });

        info!(
            node_id = self.node_id,
            local_dc = %self.config.local_dc_id,
            "Multi-DC replication manager started"
        );

        Ok(())
    }

    /// Shutdown the manager
    pub async fn shutdown(&self) {
        self.coordinator.shutdown().await;
        info!("Multi-DC replication manager shut down");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datacenter_info_creation() {
        let dc = DatacenterInfo::new(
            "dc1".to_string(),
            "Primary DC".to_string(),
            "us-east-1".to_string(),
        )
        .with_endpoint("127.0.0.1:9094".parse().unwrap())
        .as_primary();

        assert_eq!(dc.dc_id, "dc1");
        assert!(dc.is_primary);
        assert_eq!(dc.failover_priority, 0);
        assert_eq!(dc.endpoints.len(), 1);
    }

    #[test]
    fn test_multi_dc_config_validation() {
        let config = MultiDcConfig::new("dc1".to_string());
        assert!(config.validate().is_ok());

        let config = MultiDcConfig {
            local_dc_id: "".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_duplicate_dc_detection() {
        let config = MultiDcConfig::new("dc1".to_string()).with_remote_dc(DatacenterInfo::new(
            "dc2".to_string(),
            "DC2".to_string(),
            "eu-west-1".to_string(),
        ));
        assert!(config.validate().is_ok());

        // Add same DC ID again
        let config = config.with_remote_dc(DatacenterInfo::new(
            "dc2".to_string(),
            "DC2 Copy".to_string(),
            "ap-south-1".to_string(),
        ));
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_replication_record_creation() {
        let record = ReplicationRecord::new(
            "dc1".to_string(),
            "test-topic".to_string(),
            0,
            100,
            Some(b"key".to_vec()),
            b"value".to_vec(),
        )
        .with_header("source".to_string(), b"producer1".to_vec());

        assert_eq!(record.source_dc, "dc1");
        assert_eq!(record.offset, 100);
        assert_eq!(record.headers.len(), 1);
    }

    #[test]
    fn test_replication_batch() {
        let mut batch = ReplicationBatch::new("dc1".to_string(), "dc2".to_string());
        assert!(batch.is_empty());

        batch.add_record(ReplicationRecord::new(
            "dc1".to_string(),
            "topic1".to_string(),
            0,
            1,
            None,
            b"data".to_vec(),
        ));

        assert_eq!(batch.len(), 1);
        assert!(!batch.is_empty());
    }

    #[tokio::test]
    async fn test_topic_partition_replication_state() {
        let state = TopicPartitionReplicationState::new("test".to_string(), 0, "dc2".to_string());

        assert_eq!(state.offset_lag(), 0);

        state.local_hwm.store(100, Ordering::Relaxed);
        assert_eq!(state.offset_lag(), 100);

        state.record_success(50).await;
        assert_eq!(state.offset_lag(), 50);

        assert!(state.is_healthy(30000).await);
    }

    #[tokio::test]
    async fn test_dc_connection_manager() {
        let mgr = DcConnectionManager::new("dc1".to_string(), 10000);

        let dc = DatacenterInfo::new(
            "dc2".to_string(),
            "Remote DC".to_string(),
            "eu-west-1".to_string(),
        )
        .with_endpoint("127.0.0.1:9094".parse().unwrap());

        mgr.register_dc(dc).await;

        assert!(!mgr.is_connected(&"dc2".to_string()).await);

        let dcs = mgr.list_dcs().await;
        assert_eq!(dcs.len(), 1);
        assert!(dcs.contains(&"dc2".to_string()));
    }

    #[tokio::test]
    async fn test_replication_coordinator() {
        let config = MultiDcConfig::new("dc1".to_string()).with_remote_dc(
            DatacenterInfo::new(
                "dc2".to_string(),
                "Remote".to_string(),
                "eu-west-1".to_string(),
            )
            .with_endpoint("127.0.0.1:9094".parse().unwrap()),
        );

        let coordinator = ReplicationCoordinator::new(1, config);

        coordinator
            .start_replication("test-topic", 0, &"dc2".to_string())
            .await;

        let state = coordinator
            .get_state("test-topic", 0, &"dc2".to_string())
            .await;
        assert!(state.is_some());

        coordinator
            .stop_replication("test-topic", 0, &"dc2".to_string())
            .await;

        let state = coordinator
            .get_state("test-topic", 0, &"dc2".to_string())
            .await;
        assert!(state.is_none());
    }

    #[tokio::test]
    async fn test_dc_failover_handler() {
        let config = MultiDcConfig::new("dc1".to_string())
            .with_remote_dc(
                DatacenterInfo::new(
                    "dc2".to_string(),
                    "Secondary".to_string(),
                    "eu-west-1".to_string(),
                )
                .with_priority(10),
            )
            .with_remote_dc(
                DatacenterInfo::new(
                    "dc3".to_string(),
                    "Tertiary".to_string(),
                    "ap-south-1".to_string(),
                )
                .with_priority(20),
            );

        let handler = DcFailoverHandler::new(config);

        // Trigger failover from dc2
        handler
            .trigger_failover(&"dc2".to_string(), "Connection lost")
            .await
            .unwrap();

        // dc3 should become primary (dc2 failed, dc3 next by priority)
        let primary = handler.primary_dc().await;
        assert_ne!(primary, "dc2");

        let history = handler.history().await;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].failed_dc, "dc2");
    }

    #[test]
    fn test_replication_mode_display() {
        assert_eq!(ReplicationMode::Async.to_string(), "async");
        assert_eq!(ReplicationMode::SemiSync.to_string(), "semi-sync");
        assert_eq!(ReplicationMode::ActiveActive.to_string(), "active-active");
    }

    #[test]
    fn test_conflict_resolution_display() {
        assert_eq!(
            ConflictResolution::LastWriteWins.to_string(),
            "last-write-wins"
        );
        assert_eq!(ConflictResolution::PrimaryWins.to_string(), "primary-wins");
    }

    #[tokio::test]
    async fn test_multi_dc_replication_manager_creation() {
        let config = MultiDcConfig::new("dc1".to_string());
        let manager = MultiDcReplicationManager::new(1, config);

        assert!(manager.failover_handler().is_local_primary().await);
    }
}
