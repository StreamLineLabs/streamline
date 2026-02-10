// Fetcher scaffolding for replication feature (not yet fully integrated)
#![allow(dead_code)]

//! Replica fetcher for follower replicas
//!
//! The ReplicaFetcher runs on follower brokers to pull data from the leader.
//! It implements the Kafka-like replication protocol:
//!
//! 1. Follower sends fetch request with current LEO
//! 2. Leader responds with records and current HWM
//! 3. Follower appends records to local log
//! 4. Follower updates local HWM based on leader's HWM
//!
//! On leader change:
//! - Follower truncates log to last committed offset (HWM)
//! - Resumes fetching from new leader

use crate::cluster::membership::MembershipManager;
use crate::cluster::node::NodeId;
use crate::cluster::tls::InterBrokerTls;
use crate::metrics;
use crate::protocol::internal::{
    FetchPartition, FetchTopic, InternalMessage, ReplicaFetchRequest, ReplicaFetchResponse,
};
use crate::storage::TopicManager;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Type alias for partitions grouped by leader
type PartitionsByLeader = HashMap<NodeId, HashMap<(String, i32), FetchPartitionState>>;

/// Configuration for the replica fetcher
#[derive(Debug, Clone)]
pub struct FetcherConfig {
    /// Interval between fetch requests
    pub fetch_interval: Duration,

    /// Maximum bytes to fetch per request
    pub fetch_max_bytes: i32,

    /// Minimum bytes to wait for before responding (0 = don't wait)
    pub fetch_min_bytes: i32,

    /// Maximum wait time for fetch_min_bytes
    pub fetch_max_wait: Duration,

    /// Timeout for network operations
    pub request_timeout: Duration,

    /// Number of retries on failure
    pub max_retries: u32,

    /// Backoff between retries
    pub retry_backoff: Duration,
}

impl Default for FetcherConfig {
    fn default() -> Self {
        Self {
            fetch_interval: Duration::from_millis(500),
            fetch_max_bytes: 1024 * 1024, // 1MB
            fetch_min_bytes: 1,
            fetch_max_wait: Duration::from_millis(500),
            request_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_backoff: Duration::from_millis(100),
        }
    }
}

/// State of a partition being fetched
#[derive(Debug, Clone)]
pub(crate) struct FetchPartitionState {
    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition_id: i32,

    /// Current leader node
    pub leader_id: Option<NodeId>,

    /// Leader epoch
    pub leader_epoch: i32,

    /// Local log end offset (where to fetch from)
    pub fetch_offset: i64,

    /// Local high watermark
    pub high_watermark: i64,

    /// Whether currently fetching
    pub is_fetching: bool,

    /// Last successful fetch timestamp
    pub last_fetch_time: Option<std::time::Instant>,

    /// Consecutive fetch errors
    pub error_count: u32,
}

impl FetchPartitionState {
    /// Create new fetch state for a partition
    pub fn new(topic: String, partition_id: i32, leader_id: NodeId, leader_epoch: i32) -> Self {
        Self {
            topic,
            partition_id,
            leader_id: Some(leader_id),
            leader_epoch,
            fetch_offset: 0,
            high_watermark: 0,
            is_fetching: false,
            last_fetch_time: None,
            error_count: 0,
        }
    }

    /// Update leader info
    pub fn set_leader(&mut self, leader_id: NodeId, leader_epoch: i32) {
        if leader_epoch > self.leader_epoch {
            self.leader_id = Some(leader_id);
            self.leader_epoch = leader_epoch;
            self.error_count = 0;
        }
    }

    /// Clear leader (when leader is unknown)
    pub fn clear_leader(&mut self) {
        self.leader_id = None;
    }

    /// Record successful fetch
    pub fn record_fetch_success(&mut self, new_leo: i64, new_hwm: i64, replica_id: NodeId) {
        self.fetch_offset = new_leo;
        self.high_watermark = new_hwm;
        self.last_fetch_time = Some(std::time::Instant::now());
        self.error_count = 0;

        // Record metrics: successful replication fetch resets lag time to 0
        metrics::record_last_replication_time(&self.topic, self.partition_id, replica_id, 0.0);
    }

    /// Record fetch error
    pub fn record_fetch_error(&mut self) {
        self.error_count += 1;
    }
}

/// Command to control the fetcher
#[derive(Debug)]
pub enum FetcherCommand {
    /// Add a partition to fetch
    AddPartition {
        topic: String,
        partition_id: i32,
        leader_id: NodeId,
        leader_epoch: i32,
        fetch_offset: i64,
    },

    /// Remove a partition from fetching (e.g., became leader)
    RemovePartition { topic: String, partition_id: i32 },

    /// Update leader for a partition
    UpdateLeader {
        topic: String,
        partition_id: i32,
        leader_id: NodeId,
        leader_epoch: i32,
    },

    /// Truncate partition to offset (on leader change)
    TruncateTo {
        topic: String,
        partition_id: i32,
        offset: i64,
    },

    /// Shutdown the fetcher
    Shutdown,
}

/// Result of a fetch operation
#[derive(Debug)]
pub(crate) struct FetchResult {
    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition_id: i32,

    /// Fetched records (raw bytes)
    pub records: Vec<u8>,

    /// Number of records fetched
    pub record_count: usize,

    /// Leader's high watermark
    pub leader_hwm: i64,

    /// Last offset in the fetched records
    pub last_offset: i64,
}

/// Manages replica fetching for all follower partitions on this node
#[derive(Debug)]
pub struct ReplicaFetcher {
    /// Local node ID
    node_id: NodeId,

    /// Fetcher configuration
    config: FetcherConfig,

    /// Partitions being fetched, grouped by leader
    /// Key: leader node ID, Value: map of (topic, partition) -> state
    partitions_by_leader: Arc<RwLock<PartitionsByLeader>>,

    /// Command channel sender
    command_tx: Option<mpsc::Sender<FetcherCommand>>,
}

impl ReplicaFetcher {
    /// Create a new replica fetcher
    pub fn new(node_id: NodeId, config: FetcherConfig) -> Self {
        Self {
            node_id,
            config,
            partitions_by_leader: Arc::new(RwLock::new(HashMap::new())),
            command_tx: None,
        }
    }

    /// Get the command channel sender for controlling the fetcher
    pub fn command_sender(&self) -> Option<mpsc::Sender<FetcherCommand>> {
        self.command_tx.clone()
    }

    /// Add a partition to be fetched
    pub async fn add_partition(
        &self,
        topic: &str,
        partition_id: i32,
        leader_id: NodeId,
        leader_epoch: i32,
        fetch_offset: i64,
    ) {
        let mut state =
            FetchPartitionState::new(topic.to_string(), partition_id, leader_id, leader_epoch);
        state.fetch_offset = fetch_offset;

        let mut by_leader = self.partitions_by_leader.write().await;
        let leader_partitions = by_leader.entry(leader_id).or_default();
        leader_partitions.insert((topic.to_string(), partition_id), state);

        info!(
            topic,
            partition_id, leader_id, fetch_offset, "Added partition to replica fetcher"
        );
    }

    /// Remove a partition from fetching
    pub async fn remove_partition(&self, topic: &str, partition_id: i32) {
        let mut by_leader = self.partitions_by_leader.write().await;

        for leader_partitions in by_leader.values_mut() {
            leader_partitions.remove(&(topic.to_string(), partition_id));
        }

        // Clean up empty leader entries
        by_leader.retain(|_, partitions| !partitions.is_empty());

        debug!(
            topic,
            partition_id, "Removed partition from replica fetcher"
        );
    }

    /// Update leader for a partition
    pub async fn update_leader(
        &self,
        topic: &str,
        partition_id: i32,
        new_leader_id: NodeId,
        leader_epoch: i32,
    ) {
        let key = (topic.to_string(), partition_id);
        let mut by_leader = self.partitions_by_leader.write().await;

        // Find and remove from old leader
        let mut state = None;
        for leader_partitions in by_leader.values_mut() {
            if let Some(s) = leader_partitions.remove(&key) {
                state = Some(s);
                break;
            }
        }

        // Update and add to new leader
        if let Some(mut s) = state {
            s.set_leader(new_leader_id, leader_epoch);
            let leader_partitions = by_leader.entry(new_leader_id).or_default();
            leader_partitions.insert(key, s);
        }

        // Clean up empty leader entries
        by_leader.retain(|_, partitions| !partitions.is_empty());

        info!(
            topic,
            partition_id,
            new_leader_id,
            leader_epoch,
            "Updated partition leader in replica fetcher"
        );
    }

    /// Get all partitions being fetched
    pub async fn list_partitions(&self) -> Vec<(String, i32, Option<NodeId>)> {
        let by_leader = self.partitions_by_leader.read().await;
        let mut result = Vec::new();

        for (leader_id, partitions) in by_leader.iter() {
            for (topic, partition_id) in partitions.keys() {
                result.push((topic.clone(), *partition_id, Some(*leader_id)));
            }
        }

        result
    }

    /// Get fetch state for a partition
    pub(crate) async fn get_partition_state(
        &self,
        topic: &str,
        partition_id: i32,
    ) -> Option<FetchPartitionState> {
        let key = (topic.to_string(), partition_id);
        let by_leader = self.partitions_by_leader.read().await;

        for partitions in by_leader.values() {
            if let Some(state) = partitions.get(&key) {
                return Some(state.clone());
            }
        }

        None
    }

    /// Get partitions grouped by leader for batched fetching
    pub(crate) async fn get_partitions_by_leader(
        &self,
    ) -> HashMap<NodeId, Vec<FetchPartitionState>> {
        let by_leader = self.partitions_by_leader.read().await;
        let mut result = HashMap::new();

        for (&leader_id, partitions) in by_leader.iter() {
            let states: Vec<_> = partitions.values().cloned().collect();
            if !states.is_empty() {
                result.insert(leader_id, states);
            }
        }

        result
    }

    /// Record successful fetch for a partition
    pub async fn record_fetch_success(
        &self,
        topic: &str,
        partition_id: i32,
        new_leo: i64,
        new_hwm: i64,
    ) {
        let key = (topic.to_string(), partition_id);
        let mut by_leader = self.partitions_by_leader.write().await;

        for partitions in by_leader.values_mut() {
            if let Some(state) = partitions.get_mut(&key) {
                state.record_fetch_success(new_leo, new_hwm, self.node_id);
                return;
            }
        }
    }

    /// Record fetch error for a partition
    pub async fn record_fetch_error(&self, topic: &str, partition_id: i32) {
        let key = (topic.to_string(), partition_id);
        let mut by_leader = self.partitions_by_leader.write().await;

        for partitions in by_leader.values_mut() {
            if let Some(state) = partitions.get_mut(&key) {
                state.record_fetch_error();
                return;
            }
        }
    }
}

/// Runs the fetch loop for the replica fetcher
/// This would be started as a background task
pub async fn run_fetch_loop(
    fetcher: Arc<ReplicaFetcher>,
    mut command_rx: mpsc::Receiver<FetcherCommand>,
    // Storage handle would be passed here to append fetched records
    // network handle would be passed here to make fetch requests
) {
    let mut fetch_ticker = interval(fetcher.config.fetch_interval);

    info!(node_id = fetcher.node_id, "Starting replica fetch loop");

    loop {
        tokio::select! {
            // Handle commands
            Some(cmd) = command_rx.recv() => {
                match cmd {
                    FetcherCommand::AddPartition { topic, partition_id, leader_id, leader_epoch, fetch_offset } => {
                        fetcher.add_partition(&topic, partition_id, leader_id, leader_epoch, fetch_offset).await;
                    }
                    FetcherCommand::RemovePartition { topic, partition_id } => {
                        fetcher.remove_partition(&topic, partition_id).await;
                    }
                    FetcherCommand::UpdateLeader { topic, partition_id, leader_id, leader_epoch } => {
                        fetcher.update_leader(&topic, partition_id, leader_id, leader_epoch).await;
                    }
                    FetcherCommand::TruncateTo { topic, partition_id, offset } => {
                        // Basic loop doesn't have storage - truncation handled in run_fetch_loop_with_storage
                        debug!(topic, partition_id, offset, "Truncation command received (no-op in basic loop)");
                    }
                    FetcherCommand::Shutdown => {
                        info!("Shutting down replica fetch loop");
                        break;
                    }
                }
            }

            // Periodic fetch tick
            _ = fetch_ticker.tick() => {
                let partitions_by_leader = fetcher.get_partitions_by_leader().await;

                if partitions_by_leader.is_empty() {
                    continue;
                }

                // Basic loop logs fetch intent but doesn't perform network I/O
                // Use run_fetch_loop_with_storage for actual replication
                for (leader_id, partitions) in &partitions_by_leader {
                    debug!(
                        leader_id,
                        partition_count = partitions.len(),
                        "Would fetch from leader (basic loop - no network)"
                    );
                }
            }
        }
    }
}

/// Runs the fetch loop with network capabilities
///
/// This is the production version that actually performs network fetches
/// from leader nodes for follower partitions.
///
/// # Arguments
/// * `fetcher` - The replica fetcher instance
/// * `command_rx` - Channel for receiving fetcher commands
/// * `membership` - Membership manager for resolving leader addresses
/// * `topic_manager` - Optional topic manager for appending fetched records to storage
pub async fn run_fetch_loop_with_network(
    fetcher: Arc<ReplicaFetcher>,
    command_rx: mpsc::Receiver<FetcherCommand>,
    membership: Arc<MembershipManager>,
) {
    run_fetch_loop_with_storage(fetcher, command_rx, membership, None, None).await
}

/// Runs the fetch loop with network and storage capabilities
///
/// This version can append fetched records to local storage.
///
/// # Arguments
/// * `fetcher` - The replica fetcher instance
/// * `command_rx` - Channel for receiving fetcher commands
/// * `membership` - Membership manager for resolving leader addresses
/// * `topic_manager` - Optional topic manager for appending fetched records to storage
/// * `tls` - Optional inter-broker TLS context for secure connections
pub async fn run_fetch_loop_with_storage(
    fetcher: Arc<ReplicaFetcher>,
    mut command_rx: mpsc::Receiver<FetcherCommand>,
    membership: Arc<MembershipManager>,
    topic_manager: Option<Arc<TopicManager>>,
    tls: Option<InterBrokerTls>,
) {
    let mut fetch_ticker = interval(fetcher.config.fetch_interval);
    let node_id = fetcher.node_id;

    info!(
        node_id,
        storage_enabled = topic_manager.is_some(),
        tls_enabled = tls.is_some(),
        "Starting replica fetch loop"
    );

    loop {
        tokio::select! {
            // Handle commands
            Some(cmd) = command_rx.recv() => {
                match cmd {
                    FetcherCommand::AddPartition { topic, partition_id, leader_id, leader_epoch, fetch_offset } => {
                        fetcher.add_partition(&topic, partition_id, leader_id, leader_epoch, fetch_offset).await;
                    }
                    FetcherCommand::RemovePartition { topic, partition_id } => {
                        fetcher.remove_partition(&topic, partition_id).await;
                    }
                    FetcherCommand::UpdateLeader { topic, partition_id, leader_id, leader_epoch } => {
                        fetcher.update_leader(&topic, partition_id, leader_id, leader_epoch).await;
                    }
                    FetcherCommand::TruncateTo { topic, partition_id, offset } => {
                        // Truncation is handled at storage level
                        if let Some(ref tm) = topic_manager {
                            if let Err(e) = tm.truncate_partition(&topic, partition_id, offset) {
                                error!(topic, partition_id, offset, error = %e, "Failed to truncate partition");
                            } else {
                                info!(topic, partition_id, offset, "Partition truncated for resync");
                            }
                        }
                    }
                    FetcherCommand::Shutdown => {
                        info!("Shutting down replica fetch loop");
                        break;
                    }
                }
            }

            // Periodic fetch tick
            _ = fetch_ticker.tick() => {
                let partitions_by_leader = fetcher.get_partitions_by_leader().await;

                if partitions_by_leader.is_empty() {
                    continue;
                }

                // Fetch from each leader
                for (leader_id, partitions) in partitions_by_leader {
                    // Get leader's address from membership
                    let leader_addr = match membership.get_peer(leader_id).await {
                        Some(broker) => broker.inter_broker_addr,
                        None => {
                            debug!(leader_id, "Leader not found in membership, skipping fetch");
                            continue;
                        }
                    };

                    // Build fetch request
                    let request = build_fetch_request(node_id, &partitions);

                    // Perform fetch (with optional TLS)
                    match fetch_from_leader(leader_addr, request, fetcher.config.request_timeout, tls.as_ref()).await {
                        Ok(response) => {
                            // Process response, update fetcher state, and append to storage
                            process_fetch_response_with_storage(
                                &fetcher,
                                &partitions,
                                response,
                                topic_manager.as_ref(),
                            ).await;
                        }
                        Err(e) => {
                            warn!(
                                leader_id,
                                leader_addr = %leader_addr,
                                error = %e,
                                "Failed to fetch from leader"
                            );
                            // Record error for each partition
                            for state in &partitions {
                                fetcher.record_fetch_error(&state.topic, state.partition_id).await;
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Build a fetch request for the given partitions
fn build_fetch_request(
    replica_id: NodeId,
    partitions: &[FetchPartitionState],
) -> ReplicaFetchRequest {
    let mut request = ReplicaFetchRequest::new(replica_id);

    // Group partitions by topic
    let mut by_topic: HashMap<&str, Vec<&FetchPartitionState>> = HashMap::new();
    for p in partitions {
        by_topic.entry(&p.topic).or_default().push(p);
    }

    // Build request structure
    for (topic, parts) in by_topic {
        request.topics.push(FetchTopic {
            name: topic.to_string(),
            partitions: parts
                .into_iter()
                .map(|p| FetchPartition {
                    partition: p.partition_id,
                    current_leader_epoch: p.leader_epoch,
                    fetch_offset: p.fetch_offset,
                    log_start_offset: 0,
                    partition_max_bytes: 1024 * 1024, // 1MB
                })
                .collect(),
        });
    }

    request
}

/// Fetch data from a leader node
async fn fetch_from_leader(
    leader_addr: SocketAddr,
    request: ReplicaFetchRequest,
    timeout: Duration,
    tls: Option<&InterBrokerTls>,
) -> Result<ReplicaFetchResponse, std::io::Error> {
    // Connect to leader
    let stream = tokio::time::timeout(timeout, TcpStream::connect(leader_addr))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "Connection timeout"))??;

    // Optionally upgrade to TLS
    if let Some(tls) = tls {
        let tls_stream = tls.connect(stream, &leader_addr).await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e.to_string())
        })?;
        let (reader, writer) = tokio::io::split(tls_stream);
        return fetch_from_leader_on_stream(request, reader, writer, timeout).await;
    }

    let (reader, writer) = stream.into_split();
    fetch_from_leader_on_stream(request, reader, writer, timeout).await
}

/// Helper to perform fetch on any stream implementing AsyncRead + AsyncWrite
async fn fetch_from_leader_on_stream<R, W>(
    request: ReplicaFetchRequest,
    mut reader: R,
    mut writer: W,
    timeout: Duration,
) -> Result<ReplicaFetchResponse, std::io::Error>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // Serialize request
    let msg = InternalMessage::ReplicaFetchRequest(request);
    let data = serde_json::to_vec(&msg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    // Send length-prefixed message
    let mut buf = BytesMut::with_capacity(4 + data.len());
    buf.put_u32(data.len() as u32);
    buf.put_slice(&data);

    tokio::time::timeout(timeout, writer.write_all(&buf))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "Write timeout"))??;

    tokio::time::timeout(timeout, writer.flush())
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "Flush timeout"))??;

    // Read response length
    let mut len_buf = [0u8; 4];
    tokio::time::timeout(timeout, reader.read_exact(&mut len_buf))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "Read timeout"))??;

    let response_len = (&len_buf[..]).get_u32() as usize;
    if response_len > 100 * 1024 * 1024 {
        // 100MB max
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Response too large",
        ));
    }

    // Read response body
    let mut response_buf = vec![0u8; response_len];
    tokio::time::timeout(timeout, reader.read_exact(&mut response_buf))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "Read timeout"))??;

    // Deserialize response
    let msg: InternalMessage = serde_json::from_slice(&response_buf)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    match msg {
        InternalMessage::ReplicaFetchResponse(response) => Ok(response),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unexpected response type",
        )),
    }
}

/// Process a fetch response, update fetcher state, and optionally append to storage
async fn process_fetch_response_with_storage(
    fetcher: &ReplicaFetcher,
    partitions: &[FetchPartitionState],
    response: ReplicaFetchResponse,
    topic_manager: Option<&Arc<TopicManager>>,
) {
    for topic_response in response.topics {
        for partition_response in topic_response.partitions {
            // Find matching partition state
            let state = partitions.iter().find(|p| {
                p.topic == topic_response.name && p.partition_id == partition_response.partition
            });

            if let Some(state) = state {
                if partition_response.error_code == 0 {
                    let records = &partition_response.records;

                    // Calculate new LEO based on fetched records
                    let new_leo = if records.is_empty() {
                        state.fetch_offset
                    } else {
                        // Last record offset + 1
                        records
                            .last()
                            .map(|r| r.offset + 1)
                            .unwrap_or(state.fetch_offset)
                    };

                    // Append records to local storage if topic_manager is provided
                    if !records.is_empty() {
                        if let Some(tm) = topic_manager {
                            // Convert internal records to storage records
                            let storage_records: Vec<crate::storage::record::Record> = records
                                .iter()
                                .map(|r| {
                                    crate::storage::record::Record::new(
                                        r.offset,
                                        r.timestamp,
                                        r.key.as_ref().map(|k| Bytes::copy_from_slice(k)),
                                        Bytes::copy_from_slice(&r.value),
                                    )
                                })
                                .collect();

                            match tm.append_replicated_records(
                                &state.topic,
                                state.partition_id,
                                storage_records,
                            ) {
                                Ok(actual_leo) => {
                                    debug!(
                                        topic = %state.topic,
                                        partition = state.partition_id,
                                        records = records.len(),
                                        leo = actual_leo,
                                        hwm = partition_response.high_watermark,
                                        "Appended replicated records to storage"
                                    );
                                }
                                Err(e) => {
                                    error!(
                                        topic = %state.topic,
                                        partition = state.partition_id,
                                        error = %e,
                                        "Failed to append replicated records"
                                    );
                                    fetcher
                                        .record_fetch_error(&state.topic, state.partition_id)
                                        .await;
                                    continue;
                                }
                            }
                        } else {
                            debug!(
                                topic = %state.topic,
                                partition = state.partition_id,
                                records = records.len(),
                                new_leo,
                                hwm = partition_response.high_watermark,
                                "Fetched records from leader (storage not enabled)"
                            );
                        }
                    }

                    // Update fetcher state
                    fetcher
                        .record_fetch_success(
                            &state.topic,
                            state.partition_id,
                            new_leo,
                            partition_response.high_watermark,
                        )
                        .await;
                } else {
                    warn!(
                        topic = %state.topic,
                        partition = state.partition_id,
                        error_code = partition_response.error_code,
                        "Fetch error from leader"
                    );
                    fetcher
                        .record_fetch_error(&state.topic, state.partition_id)
                        .await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fetcher_config_default() {
        let config = FetcherConfig::default();
        assert_eq!(config.fetch_interval, Duration::from_millis(500));
        assert_eq!(config.fetch_max_bytes, 1024 * 1024);
    }

    #[test]
    fn test_fetch_partition_state() {
        let mut state = FetchPartitionState::new("test".to_string(), 0, 2, 1);
        assert_eq!(state.leader_id, Some(2));
        assert_eq!(state.leader_epoch, 1);
        assert_eq!(state.fetch_offset, 0);
        assert_eq!(state.error_count, 0);

        // Update leader
        state.set_leader(3, 2);
        assert_eq!(state.leader_id, Some(3));
        assert_eq!(state.leader_epoch, 2);

        // Stale epoch should be ignored
        state.set_leader(4, 1);
        assert_eq!(state.leader_id, Some(3)); // Still 3

        // Record success
        state.record_fetch_success(100, 90, 1);
        assert_eq!(state.fetch_offset, 100);
        assert_eq!(state.high_watermark, 90);
        assert!(state.last_fetch_time.is_some());

        // Record error
        state.record_fetch_error();
        assert_eq!(state.error_count, 1);
        state.record_fetch_error();
        assert_eq!(state.error_count, 2);

        // Success resets error count
        state.record_fetch_success(110, 100, 1);
        assert_eq!(state.error_count, 0);
    }

    #[tokio::test]
    async fn test_replica_fetcher_add_remove() {
        let fetcher = ReplicaFetcher::new(1, FetcherConfig::default());

        // Add partitions
        fetcher.add_partition("topic1", 0, 2, 1, 0).await;
        fetcher.add_partition("topic1", 1, 2, 1, 100).await;
        fetcher.add_partition("topic2", 0, 3, 1, 50).await;

        let partitions = fetcher.list_partitions().await;
        assert_eq!(partitions.len(), 3);

        // Check state
        let state = fetcher.get_partition_state("topic1", 1).await.unwrap();
        assert_eq!(state.fetch_offset, 100);
        assert_eq!(state.leader_id, Some(2));

        // Remove partition
        fetcher.remove_partition("topic1", 0).await;
        let partitions = fetcher.list_partitions().await;
        assert_eq!(partitions.len(), 2);

        // Get partitions by leader
        let by_leader = fetcher.get_partitions_by_leader().await;
        assert_eq!(by_leader.get(&2).map(|v| v.len()), Some(1)); // topic1-1
        assert_eq!(by_leader.get(&3).map(|v| v.len()), Some(1)); // topic2-0
    }

    #[tokio::test]
    async fn test_replica_fetcher_update_leader() {
        let fetcher = ReplicaFetcher::new(1, FetcherConfig::default());

        fetcher.add_partition("topic1", 0, 2, 1, 100).await;

        // Update leader
        fetcher.update_leader("topic1", 0, 3, 2).await;

        let state = fetcher.get_partition_state("topic1", 0).await.unwrap();
        assert_eq!(state.leader_id, Some(3));
        assert_eq!(state.leader_epoch, 2);

        // Partition should be under new leader
        let by_leader = fetcher.get_partitions_by_leader().await;
        assert!(!by_leader.contains_key(&2));
        assert!(by_leader.contains_key(&3));
    }

    #[tokio::test]
    async fn test_record_fetch_success_error() {
        let fetcher = ReplicaFetcher::new(1, FetcherConfig::default());

        fetcher.add_partition("topic1", 0, 2, 1, 0).await;

        // Record success
        fetcher.record_fetch_success("topic1", 0, 100, 80).await;
        let state = fetcher.get_partition_state("topic1", 0).await.unwrap();
        assert_eq!(state.fetch_offset, 100);
        assert_eq!(state.high_watermark, 80);

        // Record error
        fetcher.record_fetch_error("topic1", 0).await;
        let state = fetcher.get_partition_state("topic1", 0).await.unwrap();
        assert_eq!(state.error_count, 1);
    }
}
