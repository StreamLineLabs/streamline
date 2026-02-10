//! Raft network implementation for inter-broker communication
//!
//! This module provides network transport for Raft RPC messages between
//! cluster nodes using TCP connections with a custom binary protocol.
//!
//! The handler supports two message types:
//! - `RaftMessage`: Raft consensus messages (vote, append entries, install snapshot, join)
//! - `InternalMessage`: Inter-broker protocol messages (replica fetch, leader/ISR updates)

use super::types::{ClusterCommand, StreamlineTypeConfig};
use crate::cluster::node::{BrokerInfo, NodeId, NodeState};
use crate::cluster::tls::InterBrokerTls;
use crate::protocol::internal::{
    error_codes, FetchPartitionResponse, FetchTopicResponse, InternalMessage, RecordData,
    ReplicaFetchRequest, ReplicaFetchResponse,
};
use crate::storage::TopicManager;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Snapshot, Vote};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tracing::{debug, error, warn};

/// Raft RPC message types
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftMessage {
    /// Vote request
    VoteRequest(VoteRequest<NodeId>),
    /// Vote response
    VoteResponse(VoteResponse<NodeId>),
    /// Append entries request
    AppendEntriesRequest(AppendEntriesRequest<StreamlineTypeConfig>),
    /// Append entries response
    AppendEntriesResponse(AppendEntriesResponse<NodeId>),
    /// Install snapshot request
    InstallSnapshotRequest(InstallSnapshotRequest<StreamlineTypeConfig>),
    /// Install snapshot response
    InstallSnapshotResponse(InstallSnapshotResponse<NodeId>),
    /// Join cluster request (node wants to join)
    JoinRequest(JoinRequest),
    /// Join cluster response
    JoinResponse(JoinResponse),
}

/// Request to join an existing cluster
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    /// Node ID of the joining node
    pub node_id: NodeId,
    /// Address for Raft RPC communication
    pub raft_addr: String,
    /// Advertised address for clients
    pub advertised_addr: String,
    /// Version of the joining node (for compatibility checking)
    #[serde(default)]
    pub version: Option<String>,
}

/// Response to a join request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinResponse {
    /// Whether the join was successful
    pub success: bool,
    /// Error message if join failed
    pub error: Option<String>,
    /// Current leader node ID (if not this node)
    pub leader_id: Option<NodeId>,
    /// Leader's Raft address (for redirect)
    pub leader_addr: Option<String>,
    /// Version of the responding node
    #[serde(default)]
    pub version: Option<String>,
}

/// Network factory for creating Raft network connections
pub struct NetworkFactory {
    /// Local node ID
    node_id: NodeId,

    /// Connection timeout
    connect_timeout: Duration,

    /// Request timeout
    request_timeout: Duration,

    /// Optional TLS context for inter-broker communication
    tls: Option<InterBrokerTls>,
}

impl NetworkFactory {
    /// Create a new network factory
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            tls: None,
        }
    }

    /// Set connection timeout
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set request timeout
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set TLS context for secure inter-broker communication
    pub fn with_tls(mut self, tls: InterBrokerTls) -> Self {
        self.tls = Some(tls);
        self
    }
}

impl RaftNetworkFactory<StreamlineTypeConfig> for NetworkFactory {
    type Network = NetworkClient;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        debug!(
            from = self.node_id,
            to = target,
            addr = %node.addr,
            tls_enabled = self.tls.is_some(),
            "Creating network client"
        );

        NetworkClient {
            target,
            addr: node.addr.clone(),
            connect_timeout: self.connect_timeout,
            request_timeout: self.request_timeout,
            tls: self.tls.clone(),
        }
    }
}

/// Network client for communicating with a specific Raft peer
pub struct NetworkClient {
    /// Target node ID
    target: NodeId,

    /// Target address
    addr: String,

    /// Connection timeout
    connect_timeout: Duration,

    /// Request timeout
    request_timeout: Duration,

    /// Optional TLS context for secure communication
    tls: Option<InterBrokerTls>,
}

impl NetworkClient {
    /// Send a message and receive a RaftMessage response
    async fn send_message(
        &self,
        msg: RaftMessage,
    ) -> Result<RaftMessage, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        // Connect with timeout
        let tcp_stream = tokio::time::timeout(self.connect_timeout, TcpStream::connect(&self.addr))
            .await
            .map_err(|_| {
                RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "connection timeout",
                )))
            })?
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        // If TLS is enabled, upgrade the connection
        if let Some(ref tls) = self.tls {
            let addr: SocketAddr = self.addr.parse().map_err(|_| {
                RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "invalid address",
                )))
            })?;

            let tls_stream = tls.connect(tcp_stream, &addr).await.map_err(|e| {
                RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    format!("TLS handshake failed: {}", e),
                )))
            })?;

            let (reader, writer) = tokio::io::split(tls_stream);
            return self.send_message_on_stream(msg, reader, writer).await;
        }

        // Plain TCP connection
        let (reader, writer) = tcp_stream.into_split();
        self.send_message_on_stream(msg, reader, writer).await
    }

    /// Send a message on a given stream and receive a RaftMessage response
    async fn send_message_on_stream<R, W>(
        &self,
        msg: RaftMessage,
        mut reader: R,
        mut writer: W,
    ) -> Result<RaftMessage, RPCError<NodeId, BasicNode, RaftError<NodeId>>>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        // Serialize message
        let data = serde_json::to_vec(&msg).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e,
            )))
        })?;

        // Write length prefix and data
        let len = data.len() as u32;
        writer
            .write_all(&len.to_be_bytes())
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        writer
            .write_all(&data)
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;
        writer
            .flush()
            .await
            .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        // Read response with timeout
        let response = tokio::time::timeout(self.request_timeout, async {
            // Read length prefix
            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf).await?;
            let len = u32::from_be_bytes(len_buf) as usize;

            // Read response data
            let mut buf = vec![0u8; len];
            reader.read_exact(&mut buf).await?;

            Ok::<_, std::io::Error>(buf)
        })
        .await
        .map_err(|_| {
            RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "request timeout",
            )))
        })?
        .map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        // Deserialize response as RaftMessage
        let result: RaftMessage = serde_json::from_slice(&response).map_err(|e| {
            RPCError::Unreachable(Unreachable::new(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e,
            )))
        })?;

        Ok(result)
    }
}

impl RaftNetwork<StreamlineTypeConfig> for NetworkClient {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<StreamlineTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        debug!(target = self.target, "Sending AppendEntries");

        let msg = RaftMessage::AppendEntriesRequest(req);
        let response = self.send_message(msg).await?;

        // Extract AppendEntriesResponse from RaftMessage wrapper
        match response {
            RaftMessage::AppendEntriesResponse(resp) => Ok(resp),
            other => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Expected AppendEntriesResponse, got {:?}",
                        std::mem::discriminant(&other)
                    ),
                ),
            ))),
        }
    }

    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        debug!(target = self.target, "Sending Vote request");

        let msg = RaftMessage::VoteRequest(req);
        let response = self.send_message(msg).await?;

        // Extract VoteResponse from RaftMessage wrapper
        match response {
            RaftMessage::VoteResponse(resp) => Ok(resp),
            other => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Expected VoteResponse, got {:?}",
                        std::mem::discriminant(&other)
                    ),
                ),
            ))),
        }
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<NodeId>,
        snapshot: Snapshot<StreamlineTypeConfig>,
        _cancel: impl Future<Output = openraft::error::ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::SnapshotResponse<NodeId>,
        StreamingError<StreamlineTypeConfig, openraft::error::Fatal<NodeId>>,
    > {
        debug!(
            target = self.target,
            snapshot_id = %snapshot.meta.snapshot_id,
            "Sending full snapshot"
        );

        // Read snapshot data
        let mut snapshot_box = snapshot.snapshot;
        let mut data = Vec::new();
        let cursor: &mut Cursor<Vec<u8>> = snapshot_box.as_mut();
        std::io::Read::read_to_end(cursor, &mut data).map_err(|e| {
            StreamingError::StorageError(openraft::StorageError::IO {
                source: openraft::StorageIOError::new(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Read,
                    openraft::AnyError::new(&e),
                ),
            })
        })?;

        let req = InstallSnapshotRequest {
            vote,
            meta: snapshot.meta.clone(),
            offset: 0,
            data,
            done: true,
        };

        let msg = RaftMessage::InstallSnapshotRequest(req);
        let raft_response = self.send_message(msg).await.map_err(|e| match e {
            RPCError::Unreachable(u) => StreamingError::Unreachable(u),
            RPCError::PayloadTooLarge(_) => StreamingError::Unreachable(Unreachable::new(
                &std::io::Error::other("payload too large"),
            )),
            RPCError::Timeout(_) => StreamingError::Unreachable(Unreachable::new(
                &std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"),
            )),
            RPCError::Network(_) => StreamingError::Unreachable(Unreachable::new(
                &std::io::Error::other("network error"),
            )),
            RPCError::RemoteError(_) => {
                // Convert to Unreachable since we can't create Fatal from RaftError
                StreamingError::Unreachable(Unreachable::new(&std::io::Error::other(
                    "remote error",
                )))
            }
        })?;

        // Extract InstallSnapshotResponse from RaftMessage wrapper
        let response = match raft_response {
            RaftMessage::InstallSnapshotResponse(resp) => resp,
            _ => {
                return Err(StreamingError::Unreachable(Unreachable::new(
                    &std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Expected InstallSnapshotResponse",
                    ),
                )));
            }
        };

        Ok(openraft::raft::SnapshotResponse {
            vote: response.vote,
        })
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<StreamlineTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        debug!(
            target = self.target,
            snapshot_id = %req.meta.snapshot_id,
            "Sending install snapshot request"
        );

        let msg = RaftMessage::InstallSnapshotRequest(req);
        let raft_response = self.send_message(msg).await.map_err(|e| match e {
            RPCError::Unreachable(u) => RPCError::Unreachable(u),
            RPCError::PayloadTooLarge(p) => RPCError::PayloadTooLarge(p),
            RPCError::Timeout(t) => RPCError::Timeout(t),
            RPCError::Network(n) => RPCError::Network(n),
            RPCError::RemoteError(_) => {
                // Convert generic remote error to Unreachable since we can't convert RaftError types
                RPCError::Unreachable(Unreachable::new(&std::io::Error::other(
                    "remote error during install_snapshot",
                )))
            }
        })?;

        // Extract InstallSnapshotResponse from RaftMessage wrapper
        match raft_response {
            RaftMessage::InstallSnapshotResponse(resp) => Ok(resp),
            other => Err(RPCError::Unreachable(Unreachable::new(
                &std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Expected InstallSnapshotResponse, got {:?}",
                        std::mem::discriminant(&other)
                    ),
                ),
            ))),
        }
    }
}

/// Unified message wrapper for inter-broker communication
///
/// This allows the handler to distinguish between Raft consensus messages
/// and internal protocol messages using the same connection.
#[derive(Debug, Serialize, Deserialize)]
pub enum InterBrokerMessage {
    /// Raft consensus message
    Raft(RaftMessage),
    /// Internal protocol message (replica fetch, etc.)
    Internal(InternalMessage),
}

/// Server-side handler for Raft RPC messages and internal protocol
pub struct RaftRpcHandler {
    /// Raft node reference
    raft: Arc<RwLock<Option<openraft::Raft<StreamlineTypeConfig>>>>,
    /// Topic manager for handling replica fetch requests
    topic_manager: Arc<RwLock<Option<Arc<TopicManager>>>>,
    /// This node's ID (for future use in leadership checks)
    #[allow(dead_code)]
    node_id: NodeId,
}

impl RaftRpcHandler {
    /// Create a new RPC handler
    pub fn new(node_id: NodeId) -> Self {
        Self {
            raft: Arc::new(RwLock::new(None)),
            topic_manager: Arc::new(RwLock::new(None)),
            node_id,
        }
    }

    /// Set the Raft node reference
    pub async fn set_raft(&self, raft: openraft::Raft<StreamlineTypeConfig>) {
        let mut guard = self.raft.write().await;
        *guard = Some(raft);
    }

    /// Set the topic manager for handling replica fetch requests
    pub async fn set_topic_manager(&self, topic_manager: Arc<TopicManager>) {
        let mut guard = self.topic_manager.write().await;
        *guard = Some(topic_manager);
    }

    /// Handle an incoming Raft RPC message
    pub async fn handle_message(&self, msg: RaftMessage) -> Result<RaftMessage, String> {
        let raft_guard = self.raft.read().await;
        let raft = raft_guard
            .as_ref()
            .ok_or_else(|| "Raft not initialized".to_string())?;

        match msg {
            RaftMessage::VoteRequest(req) => {
                debug!(?req, "Handling Vote request");
                let response = raft.vote(req).await.map_err(|e| format!("{:?}", e))?;
                Ok(RaftMessage::VoteResponse(response))
            }
            RaftMessage::AppendEntriesRequest(req) => {
                debug!(
                    prev_log_id = ?req.prev_log_id,
                    entries = req.entries.len(),
                    "Handling AppendEntries"
                );
                let response = raft
                    .append_entries(req)
                    .await
                    .map_err(|e| format!("{:?}", e))?;
                Ok(RaftMessage::AppendEntriesResponse(response))
            }
            RaftMessage::InstallSnapshotRequest(req) => {
                debug!(
                    snapshot_id = %req.meta.snapshot_id,
                    "Handling InstallSnapshot"
                );
                let response = raft
                    .install_snapshot(req)
                    .await
                    .map_err(|e| format!("{:?}", e))?;
                Ok(RaftMessage::InstallSnapshotResponse(response))
            }
            RaftMessage::JoinRequest(req) => {
                debug!(
                    node_id = req.node_id,
                    raft_addr = %req.raft_addr,
                    "Handling JoinRequest"
                );
                let response = self.handle_join_request(raft, req).await;
                Ok(RaftMessage::JoinResponse(response))
            }
            _ => Err("Unexpected message type".to_string()),
        }
    }

    /// Handle a join request from a node wanting to join the cluster
    async fn handle_join_request(
        &self,
        raft: &openraft::Raft<StreamlineTypeConfig>,
        req: JoinRequest,
    ) -> JoinResponse {
        let local_version = crate::cluster::version::StreamlineVersion::current();
        let local_version_str = local_version.semver();

        // Check version compatibility if the joining node reports its version
        if let Some(ref remote_version_str) = req.version {
            if let Some(remote_version) =
                crate::cluster::version::StreamlineVersion::parse(remote_version_str)
            {
                let compat =
                    crate::cluster::version::check_compatibility(&local_version, &remote_version);
                if !compat.compatible {
                    warn!(
                        node_id = req.node_id,
                        local_version = %local_version,
                        remote_version = %remote_version,
                        reason = ?compat.reason,
                        "Rejecting join: incompatible version"
                    );
                    return JoinResponse {
                        success: false,
                        error: Some(format!(
                            "Version incompatible: local={}, remote={}: {}",
                            local_version,
                            remote_version,
                            compat.reason.unwrap_or_default()
                        )),
                        leader_id: None,
                        leader_addr: None,
                        version: Some(local_version_str),
                    };
                }
            }
        }

        // Check if we're the leader
        let metrics = raft.metrics().borrow().clone();

        if metrics.current_leader != Some(metrics.id) {
            // Not the leader, redirect to leader
            if let Some(leader_id) = metrics.current_leader {
                // Try to find leader's address from membership config
                let membership = metrics.membership_config.membership();
                let leader_addr = membership.get_node(&leader_id).map(|n| n.addr.clone());

                return JoinResponse {
                    success: false,
                    error: Some(format!(
                        "Not the leader (leader is node {})",
                        leader_id
                    )),
                    leader_id: Some(leader_id),
                    leader_addr,
                    version: Some(local_version_str),
                };
            } else {
                return JoinResponse {
                    success: false,
                    error: Some("No leader elected yet, cluster may be initializing".to_string()),
                    leader_id: None,
                    leader_addr: None,
                    version: Some(local_version_str),
                };
            }
        }

        // We're the leader, add the node as a learner first
        let node = BasicNode {
            addr: req.raft_addr.clone(),
        };

        // Add as learner with blocking=true to wait for replication
        match raft.add_learner(req.node_id, node.clone(), true).await {
            Ok(_) => {
                debug!(node_id = req.node_id, "Added node as learner");

                // Wait for the learner to catch up before promoting to voter
                // This prevents split-vote scenarios during rapid node joins
                if let Err(e) = self.wait_for_learner_caught_up(raft, req.node_id).await {
                    warn!(
                        node_id = req.node_id,
                        error = %e,
                        "Learner did not catch up in time, promoting anyway"
                    );
                }

                // Now promote to voter
                // Get current voter IDs and add the new node
                let membership = raft.metrics().borrow().membership_config.clone();
                let mut voters: std::collections::BTreeSet<NodeId> =
                    membership.membership().voter_ids().collect();
                voters.insert(req.node_id);

                match raft.change_membership(voters, false).await {
                    Ok(_) => {
                        debug!(node_id = req.node_id, "Promoted node to voter");

                        // Register the joining broker in cluster metadata
                        // Parse the advertised_addr from the join request
                        let advertised_addr: SocketAddr = match req.advertised_addr.parse() {
                            Ok(addr) => addr,
                            Err(e) => {
                                warn!(
                                    node_id = req.node_id,
                                    error = %e,
                                    addr = %req.advertised_addr,
                                    "Failed to parse advertised addr, skipping broker registration"
                                );
                                return JoinResponse {
                                    success: true,
                                    error: None,
                                    leader_id: Some(metrics.id),
                                    leader_addr: None,
                                    version: Some(local_version_str),
                                };
                            }
                        };

                        let inter_broker_addr: SocketAddr = match req.raft_addr.parse() {
                            Ok(addr) => addr,
                            Err(e) => {
                                warn!(
                                    node_id = req.node_id,
                                    error = %e,
                                    addr = %req.raft_addr,
                                    "Failed to parse raft addr, skipping broker registration"
                                );
                                return JoinResponse {
                                    success: true,
                                    error: None,
                                    leader_id: Some(metrics.id),
                                    leader_addr: None,
                                    version: Some(local_version_str),
                                };
                            }
                        };

                        let broker = BrokerInfo {
                            node_id: req.node_id,
                            advertised_addr,
                            inter_broker_addr,
                            rack: None,
                            datacenter: None,
                            state: NodeState::Running,
                        };

                        // Write the broker registration through Raft
                        match raft
                            .client_write(ClusterCommand::RegisterBroker(broker))
                            .await
                        {
                            Ok(_) => {
                                debug!(node_id = req.node_id, "Registered broker via Raft");
                            }
                            Err(e) => {
                                warn!(
                                    node_id = req.node_id,
                                    error = ?e,
                                    "Failed to register broker via Raft (will retry on follower)"
                                );
                            }
                        }

                        JoinResponse {
                            success: true,
                            error: None,
                            leader_id: Some(metrics.id),
                            leader_addr: None,
                            version: Some(local_version_str),
                        }
                    }
                    Err(e) => {
                        error!(node_id = req.node_id, error = ?e, "Failed to promote to voter");
                        JoinResponse {
                            success: false,
                            error: Some(format!("Failed to promote node {} to voter: {:?}", req.node_id, e)),
                            leader_id: Some(metrics.id),
                            leader_addr: None,
                            version: Some(local_version_str),
                        }
                    }
                }
            }
            Err(e) => {
                error!(node_id = req.node_id, error = ?e, "Failed to add learner");
                JoinResponse {
                    success: false,
                    error: Some(format!("Failed to add node {} as learner: {:?}", req.node_id, e)),
                    leader_id: Some(metrics.id),
                    leader_addr: None,
                    version: Some(local_version_str),
                }
            }
        }
    }

    /// Wait for a learner to catch up with the leader's log
    ///
    /// This is important to prevent split-vote elections when adding multiple
    /// nodes rapidly. A learner that hasn't caught up may vote for a different
    /// candidate if it becomes a voter too soon.
    async fn wait_for_learner_caught_up(
        &self,
        raft: &openraft::Raft<StreamlineTypeConfig>,
        learner_id: NodeId,
    ) -> Result<(), String> {
        let timeout = Duration::from_secs(5);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > timeout {
                return Err("Timeout waiting for learner to catch up".to_string());
            }

            let metrics = raft.metrics().borrow().clone();

            // Get the leader's last log index
            let leader_last_log = metrics.last_log_index.unwrap_or(0);

            // Check if the learner has caught up by looking at replication status
            // ReplicationMetrics is BTreeMap<NID, Option<LogId<NID>>>
            if let Some(replication) = &metrics.replication {
                if let Some(Some(matched_log_id)) = replication.get(&learner_id) {
                    // Check if learner's matched index is close to leader's last log
                    // Allow a small gap (2 entries) for in-flight replication
                    if matched_log_id.index + 2 >= leader_last_log {
                        debug!(
                            learner_id = learner_id,
                            matched_index = matched_log_id.index,
                            leader_last_log = leader_last_log,
                            "Learner caught up"
                        );
                        return Ok(());
                    }
                }
            }

            // Brief sleep before checking again
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Handle an incoming internal protocol message
    pub async fn handle_internal_message(
        &self,
        msg: InternalMessage,
    ) -> Result<InternalMessage, String> {
        match msg {
            InternalMessage::ReplicaFetchRequest(req) => {
                debug!(
                    replica_id = req.replica_id,
                    topics = req.topics.len(),
                    "Handling ReplicaFetchRequest"
                );
                let response = self.handle_replica_fetch(req).await;
                Ok(InternalMessage::ReplicaFetchResponse(response))
            }
            InternalMessage::LeaderAndIsrRequest(req) => {
                debug!(
                    controller_id = req.controller_id,
                    "Handling LeaderAndIsrRequest"
                );
                // For now, just acknowledge - full implementation would update local state
                let response = crate::protocol::internal::LeaderAndIsrResponse::success();
                Ok(InternalMessage::LeaderAndIsrResponse(response))
            }
            InternalMessage::UpdateMetadataRequest(req) => {
                debug!(
                    controller_id = req.controller_id,
                    "Handling UpdateMetadataRequest"
                );
                // For now, just acknowledge
                let response = crate::protocol::internal::UpdateMetadataResponse::success();
                Ok(InternalMessage::UpdateMetadataResponse(response))
            }
            InternalMessage::StopReplicaRequest(req) => {
                debug!(
                    controller_id = req.controller_id,
                    "Handling StopReplicaRequest"
                );
                // For now, just acknowledge
                let response = crate::protocol::internal::StopReplicaResponse::success();
                Ok(InternalMessage::StopReplicaResponse(response))
            }
            _ => Err(
                "Unexpected internal message type (received response instead of request)"
                    .to_string(),
            ),
        }
    }

    /// Handle a replica fetch request from a follower
    ///
    /// This is the leader-side handler that reads data from local storage
    /// and returns it to the follower for replication.
    async fn handle_replica_fetch(&self, req: ReplicaFetchRequest) -> ReplicaFetchResponse {
        let topic_manager_guard = self.topic_manager.read().await;
        let topic_manager = match topic_manager_guard.as_ref() {
            Some(tm) => tm,
            None => {
                warn!("TopicManager not set, cannot handle replica fetch");
                return ReplicaFetchResponse::empty();
            }
        };

        let mut response_topics = Vec::new();

        for fetch_topic in &req.topics {
            let mut partition_responses = Vec::new();

            for fetch_partition in &fetch_topic.partitions {
                let partition_response = self
                    .fetch_partition_data(
                        topic_manager,
                        &fetch_topic.name,
                        fetch_partition.partition,
                        fetch_partition.fetch_offset,
                        fetch_partition.partition_max_bytes,
                    )
                    .await;

                partition_responses.push(partition_response);
            }

            response_topics.push(FetchTopicResponse {
                name: fetch_topic.name.clone(),
                partitions: partition_responses,
            });
        }

        ReplicaFetchResponse {
            throttle_time_ms: 0,
            topics: response_topics,
        }
    }

    /// Fetch data from a single partition using TopicManager API
    async fn fetch_partition_data(
        &self,
        topic_manager: &TopicManager,
        topic_name: &str,
        partition_id: i32,
        fetch_offset: i64,
        max_bytes: i32,
    ) -> FetchPartitionResponse {
        // Get partition offsets using TopicManager API
        let log_start_offset = match topic_manager.earliest_offset(topic_name, partition_id) {
            Ok(offset) => offset,
            Err(_) => {
                return FetchPartitionResponse {
                    partition: partition_id,
                    error_code: error_codes::UNKNOWN_TOPIC_OR_PARTITION,
                    high_watermark: -1,
                    last_stable_offset: -1,
                    log_start_offset: 0,
                    preferred_read_replica: None,
                    records: Vec::new(),
                };
            }
        };

        let log_end_offset = match topic_manager.latest_offset(topic_name, partition_id) {
            Ok(offset) => offset,
            Err(_) => {
                return FetchPartitionResponse {
                    partition: partition_id,
                    error_code: error_codes::UNKNOWN_TOPIC_OR_PARTITION,
                    high_watermark: -1,
                    last_stable_offset: -1,
                    log_start_offset: 0,
                    preferred_read_replica: None,
                    records: Vec::new(),
                };
            }
        };

        let high_watermark = match topic_manager.high_watermark(topic_name, partition_id) {
            Ok(hwm) => hwm,
            Err(_) => log_end_offset, // Fall back to LEO if HWM unavailable
        };

        // Check if fetch offset is valid
        if fetch_offset < log_start_offset {
            return FetchPartitionResponse {
                partition: partition_id,
                error_code: error_codes::OFFSET_OUT_OF_RANGE,
                high_watermark,
                last_stable_offset: high_watermark,
                log_start_offset,
                preferred_read_replica: None,
                records: Vec::new(),
            };
        }

        // Replica fetch can read up to LEO (not just HWM like consumers)
        if fetch_offset >= log_end_offset {
            // No new data available
            return FetchPartitionResponse {
                partition: partition_id,
                error_code: error_codes::NONE,
                high_watermark,
                last_stable_offset: high_watermark,
                log_start_offset,
                preferred_read_replica: None,
                records: Vec::new(),
            };
        }

        // Calculate how many records to fetch (rough estimate based on max_bytes)
        let max_records = (max_bytes / 1024).max(1) as usize;

        // Read records from storage using TopicManager API
        let storage_records =
            match topic_manager.read(topic_name, partition_id, fetch_offset, max_records) {
                Ok(recs) => recs,
                Err(e) => {
                    warn!(
                        topic = topic_name,
                        partition = partition_id,
                        fetch_offset = fetch_offset,
                        error = ?e,
                        "Error reading records for replica fetch"
                    );
                    return FetchPartitionResponse {
                        partition: partition_id,
                        error_code: error_codes::UNKNOWN_TOPIC_OR_PARTITION,
                        high_watermark,
                        last_stable_offset: high_watermark,
                        log_start_offset,
                        preferred_read_replica: None,
                        records: Vec::new(),
                    };
                }
            };

        // Convert to wire format, respecting max_bytes
        let mut records = Vec::new();
        let mut current_bytes = 0i32;

        for record in &storage_records {
            let record_size = record.value.len() as i32;
            if current_bytes + record_size > max_bytes && !records.is_empty() {
                break;
            }
            records.push(RecordData::from(record));
            current_bytes += record_size;
        }

        debug!(
            topic = topic_name,
            partition = partition_id,
            fetch_offset = fetch_offset,
            records_returned = records.len(),
            high_watermark = high_watermark,
            "Served replica fetch request"
        );

        FetchPartitionResponse {
            partition: partition_id,
            error_code: error_codes::NONE,
            high_watermark,
            last_stable_offset: high_watermark,
            log_start_offset,
            preferred_read_replica: None,
            records,
        }
    }

    /// Handle a raw TCP connection (non-TLS)
    ///
    /// This method first tries to parse the message as an InterBrokerMessage wrapper,
    /// and falls back to parsing as a RaftMessage for backward compatibility.
    pub async fn handle_connection(
        &self,
        stream: TcpStream,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (reader, writer) = stream.into_split();
        self.handle_connection_on_stream(reader, writer).await
    }

    /// Handle a connection on any stream implementing AsyncRead + AsyncWrite
    ///
    /// This method first tries to parse the message as an InterBrokerMessage wrapper,
    /// and falls back to parsing as a RaftMessage for backward compatibility.
    pub async fn handle_connection_on_stream<R, W>(
        &self,
        mut reader: R,
        mut writer: W,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        // Read length prefix
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        // Guard against oversized messages to prevent OOM (256 MB limit)
        const MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;
        if len > MAX_MESSAGE_SIZE {
            return Err(format!(
                "Message size {} exceeds maximum allowed {} bytes",
                len, MAX_MESSAGE_SIZE
            )
            .into());
        }

        // Read message data
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await?;

        // Try to parse as InterBrokerMessage first (new format)
        // Fall back to RaftMessage for backward compatibility
        let response_data =
            if let Ok(inter_broker_msg) = serde_json::from_slice::<InterBrokerMessage>(&buf) {
                match inter_broker_msg {
                    InterBrokerMessage::Raft(raft_msg) => {
                        let response = match self.handle_message(raft_msg).await {
                            Ok(resp) => resp,
                            Err(e) => {
                                error!(error = %e, "Failed to handle Raft message");
                                return Err(e.into());
                            }
                        };
                        serde_json::to_vec(&InterBrokerMessage::Raft(response))?
                    }
                    InterBrokerMessage::Internal(internal_msg) => {
                        let response = match self.handle_internal_message(internal_msg).await {
                            Ok(resp) => resp,
                            Err(e) => {
                                error!(error = %e, "Failed to handle internal message");
                                return Err(e.into());
                            }
                        };
                        serde_json::to_vec(&InterBrokerMessage::Internal(response))?
                    }
                }
            } else if let Ok(raft_msg) = serde_json::from_slice::<RaftMessage>(&buf) {
                // Backward compatibility: handle raw RaftMessage
                let response = match self.handle_message(raft_msg).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        error!(error = %e, "Failed to handle Raft message");
                        return Err(e.into());
                    }
                };
                serde_json::to_vec(&response)?
            } else if let Ok(internal_msg) = serde_json::from_slice::<InternalMessage>(&buf) {
                // Also support raw InternalMessage for direct internal protocol calls
                let response = match self.handle_internal_message(internal_msg).await {
                    Ok(resp) => resp,
                    Err(e) => {
                        error!(error = %e, "Failed to handle internal message");
                        return Err(e.into());
                    }
                };
                serde_json::to_vec(&response)?
            } else {
                error!("Failed to parse incoming message");
                return Err("Failed to parse message".into());
            };

        // Write length prefix and response
        let len = response_data.len() as u32;
        writer.write_all(&len.to_be_bytes()).await?;
        writer.write_all(&response_data).await?;
        writer.flush().await?;

        Ok(())
    }

    /// Handle a TLS connection
    ///
    /// Accepts a TCP stream and performs TLS handshake before processing the message.
    pub async fn handle_tls_connection(
        &self,
        stream: TcpStream,
        tls: &InterBrokerTls,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Perform TLS handshake
        let tls_stream = tls.accept(stream).await.map_err(|e| {
            error!(error = %e, "TLS handshake failed");
            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
        })?;

        // Split the TLS stream and handle the connection
        let (reader, writer) = tokio::io::split(tls_stream);
        self.handle_connection_on_stream(reader, writer).await
    }
}

/// Send a join request to a seed node
///
/// Returns the JoinResponse or an error if the request failed
pub async fn send_join_request(
    seed_addr: &str,
    request: JoinRequest,
    timeout: Duration,
) -> Result<JoinResponse, std::io::Error> {
    // Connect with timeout
    let stream = tokio::time::timeout(timeout, TcpStream::connect(seed_addr))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connection timeout"))??;

    let (mut reader, mut writer) = stream.into_split();

    // Serialize message
    let msg = RaftMessage::JoinRequest(request);
    let data = serde_json::to_vec(&msg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    // Write length prefix and data
    let len = data.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&data).await?;
    writer.flush().await?;

    // Read response with timeout
    let response = tokio::time::timeout(timeout, async {
        // Read length prefix
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        // Read response data
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await?;

        Ok::<_, std::io::Error>(buf)
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "request timeout"))??;

    // Deserialize response
    let msg: RaftMessage = serde_json::from_slice(&response)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    match msg {
        RaftMessage::JoinResponse(resp) => Ok(resp),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unexpected response type",
        )),
    }
}

/// Send a join request to a seed node with optional TLS
///
/// Returns the JoinResponse or an error if the request failed
pub async fn send_join_request_with_tls(
    seed_addr: &str,
    request: JoinRequest,
    timeout: Duration,
    tls: Option<&InterBrokerTls>,
) -> Result<JoinResponse, std::io::Error> {
    // Connect with timeout
    let stream = tokio::time::timeout(timeout, TcpStream::connect(seed_addr))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connection timeout"))??;

    // Optionally upgrade to TLS
    if let Some(tls) = tls {
        let addr: SocketAddr = seed_addr
            .parse()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        let tls_stream = tls.connect(stream, &addr).await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e.to_string())
        })?;
        let (reader, writer) = tokio::io::split(tls_stream);
        return send_join_request_on_stream(request, reader, writer, timeout).await;
    }

    let (reader, writer) = stream.into_split();
    send_join_request_on_stream(request, reader, writer, timeout).await
}

/// Helper function to send join request on any stream
async fn send_join_request_on_stream<R, W>(
    request: JoinRequest,
    mut reader: R,
    mut writer: W,
    timeout: Duration,
) -> Result<JoinResponse, std::io::Error>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // Serialize message
    let msg = RaftMessage::JoinRequest(request);
    let data = serde_json::to_vec(&msg)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    // Write length prefix and data
    let len = data.len() as u32;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&data).await?;
    writer.flush().await?;

    // Read response with timeout
    let response = tokio::time::timeout(timeout, async {
        // Read length prefix
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;

        // Read response data
        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf).await?;

        Ok::<_, std::io::Error>(buf)
    })
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "request timeout"))??;

    // Deserialize response
    let msg: RaftMessage = serde_json::from_slice(&response)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    match msg {
        RaftMessage::JoinResponse(resp) => Ok(resp),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unexpected response type",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_factory_creation() {
        let factory = NetworkFactory::new(1)
            .with_connect_timeout(Duration::from_secs(10))
            .with_request_timeout(Duration::from_secs(60));

        assert_eq!(factory.node_id, 1);
        assert_eq!(factory.connect_timeout, Duration::from_secs(10));
        assert_eq!(factory.request_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_raft_message_serialization() {
        let req = VoteRequest {
            vote: Vote::new(1, 2),
            last_log_id: None,
        };
        let msg = RaftMessage::VoteRequest(req);

        let serialized = serde_json::to_vec(&msg).unwrap();
        let deserialized: RaftMessage = serde_json::from_slice(&serialized).unwrap();

        match deserialized {
            RaftMessage::VoteRequest(r) => {
                assert_eq!(r.vote.leader_id().voted_for(), Some(2));
            }
            _ => panic!("Expected VoteRequest"),
        }
    }
}
