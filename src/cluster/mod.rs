//! Cluster management for Streamline
//!
//! This module provides multi-node clustering support including:
//! - Raft-based consensus for cluster metadata
//! - Node discovery and membership management
//! - Partition leadership and replication coordination
//! - Rolling upgrades with version tracking
//! - Auto-scaling based on metrics and policies

pub mod autoscaler;
pub mod config;
pub mod failover;
pub mod health;
pub mod leadership;
pub mod membership;
pub mod node;
pub mod partition_lease;
pub mod partition_split;
pub mod rack;
pub mod raft;
pub mod rebalancer;
pub mod tls;
pub mod upgrade;
pub mod version;

pub use autoscaler::{
    AutoScaler, AutoScalerConfig, AutoScalerStats, DrainInfo, DrainState, HpaMetric,
    HpaMetricsAdapter, MetricType, MetricsSnapshot, ScaleDirection, ScalingDecision, ScalingPolicy,
};
pub use config::{ClusterConfig, InterBrokerTlsConfig};
pub use failover::{FailoverConfig, FailoverEvent, FailoverHandler};
pub use health::{ClusterHealthMonitor, ClusterHealthReport, ClusterHealthStatus, ClusterStats};
pub use membership::MembershipManager;
pub use node::{BrokerInfo, NodeId, NodeState};
pub use rack::RackAwareAssigner;
pub use raft::{
    ClusterCommand, ClusterMetadata, ClusterResponse, PartitionAssignment, StreamlineRaft,
    TopicAssignment,
};
pub use rebalancer::{
    PartitionMovement, PartitionRebalancer, RebalanceStrategy, RebalancePlan, RebalancerConfig,
};
pub use tls::InterBrokerTls;

use crate::error::{Result, StreamlineError};
use crate::replication::{FetcherCommand, ReplicationConfig, ReplicationManager};
use openraft::BasicNode;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Main cluster manager that coordinates all cluster operations
pub struct ClusterManager {
    /// Local node ID
    node_id: NodeId,

    /// Cluster configuration
    config: ClusterConfig,

    /// Raft consensus engine
    raft: StreamlineRaft,

    /// Store (for reading cluster metadata)
    store: Arc<raft::StreamlineStore>,

    /// Raft RPC handler
    rpc_handler: Arc<raft::RaftRpcHandler>,

    /// Membership manager
    membership: Arc<MembershipManager>,

    /// Replication manager for data replication
    replication: Arc<ReplicationManager>,

    /// Command sender for the replica fetcher loop
    fetcher_cmd_tx: mpsc::Sender<FetcherCommand>,

    /// Receiver for the fetcher command channel (moved to fetch loop when started)
    fetcher_cmd_rx: Arc<RwLock<Option<mpsc::Receiver<FetcherCommand>>>>,

    /// Whether the cluster is initialized
    initialized: Arc<RwLock<bool>>,

    /// Inter-broker TLS context (if enabled)
    inter_broker_tls: Option<InterBrokerTls>,

    /// Partition rebalancer for automatic load distribution
    rebalancer: PartitionRebalancer,

    /// Cluster health monitor
    health_monitor: Arc<RwLock<ClusterHealthMonitor>>,
}

impl ClusterManager {
    /// Create a new cluster manager
    pub async fn new(config: ClusterConfig, data_dir: PathBuf) -> Result<Self> {
        let node_id = config.node_id;
        info!(node_id, "Creating cluster manager");

        // Validate configuration
        config.validate().map_err(StreamlineError::Config)?;

        // Create cluster ID
        let cluster_id = format!("streamline-{}", uuid::Uuid::new_v4());

        // Create Raft configuration
        let raft_config = raft::create_raft_config_full(
            config.heartbeat_interval_ms,
            config.election_timeout_min_ms,
            config.election_timeout_max_ms,
            config.max_payload_entries,
            config.max_logs_in_snapshot,
            config.purge_batch_size,
            config.snapshot_interval_logs,
        )?;

        // Initialize Raft
        let (raft, store, rpc_handler) =
            raft::initialize_raft(node_id, data_dir, cluster_id, raft_config)
                .await
                .map_err(|e| {
                    StreamlineError::Cluster(format!("Failed to initialize Raft: {}", e))
                })?;

        // Create membership manager
        let membership = Arc::new(MembershipManager::new(node_id, config.clone()));

        // Create replication manager
        let replication_config = ReplicationConfig::default();
        let replication = Arc::new(ReplicationManager::new(node_id, replication_config));

        // Create command channel for fetcher
        // The fetch loop will be started when set_topic_manager is called
        let (fetcher_cmd_tx, fetcher_cmd_rx) = mpsc::channel(1024);

        // Initialize inter-broker TLS if enabled
        let inter_broker_tls = if config.inter_broker_tls.enabled {
            info!(node_id, "Initializing inter-broker TLS");
            Some(InterBrokerTls::new(&config.inter_broker_tls).map_err(|e| {
                StreamlineError::Cluster(format!("Failed to initialize inter-broker TLS: {}", e))
            })?)
        } else {
            None
        };

        Ok(Self {
            node_id,
            config: config.clone(),
            raft,
            store,
            rpc_handler,
            membership,
            replication,
            fetcher_cmd_tx,
            fetcher_cmd_rx: Arc::new(RwLock::new(Some(fetcher_cmd_rx))),
            initialized: Arc::new(RwLock::new(false)),
            inter_broker_tls,
            rebalancer: PartitionRebalancer::new(RebalancerConfig {
                rack_aware: config.rack_aware_assignment,
                ..Default::default()
            }),
            health_monitor: Arc::new(RwLock::new(ClusterHealthMonitor::new(
                config.min_insync_replicas as usize,
            ))),
        })
    }

    /// Get local node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get cluster configuration
    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    /// Get a reference to the underlying Raft instance for membership operations
    pub fn raft_ref(&self) -> raft::StreamlineRaft {
        self.raft.clone()
    }

    /// Get the Raft RPC handler
    pub fn rpc_handler(&self) -> Arc<raft::RaftRpcHandler> {
        self.rpc_handler.clone()
    }

    /// Get the membership manager
    pub fn membership(&self) -> Arc<MembershipManager> {
        self.membership.clone()
    }

    /// Get the replication manager
    pub fn replication(&self) -> Arc<ReplicationManager> {
        self.replication.clone()
    }

    /// Get the fetcher command sender for sending commands to the fetch loop
    pub fn fetcher_cmd_tx(&self) -> mpsc::Sender<FetcherCommand> {
        self.fetcher_cmd_tx.clone()
    }

    /// Get the inter-broker TLS context (if enabled)
    pub fn inter_broker_tls(&self) -> Option<&InterBrokerTls> {
        self.inter_broker_tls.as_ref()
    }

    /// Set the topic manager for inter-broker data replication
    ///
    /// This is called after server initialization to wire up the RPC handler
    /// with storage access for handling ReplicaFetch requests, and to start
    /// the replica fetcher loop for pulling data from leaders.
    pub async fn set_topic_manager(&self, topic_manager: Arc<crate::storage::TopicManager>) {
        // Set on RPC handler for serving replica fetch requests
        self.rpc_handler
            .set_topic_manager(topic_manager.clone())
            .await;

        // Start the replica fetch loop with storage integration
        // Take the receiver from the option (can only be done once)
        let mut rx_guard = self.fetcher_cmd_rx.write().await;
        if let Some(fetcher_cmd_rx) = rx_guard.take() {
            let fetcher = self.replication.replica_fetcher();
            let membership = self.membership.clone();
            let topic_manager_clone = topic_manager.clone();
            let tls = self.inter_broker_tls.clone();

            info!(
                node_id = self.node_id,
                tls_enabled = tls.is_some(),
                "Starting replica fetcher loop with storage integration"
            );

            tokio::spawn(async move {
                crate::replication::fetcher::run_fetch_loop_with_storage(
                    fetcher,
                    fetcher_cmd_rx,
                    membership,
                    Some(topic_manager_clone),
                    tls,
                )
                .await;
            });
        } else {
            warn!(
                node_id = self.node_id,
                "Fetcher loop already started, skipping"
            );
        }
    }

    /// Check if this node is the Raft leader
    pub async fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.node_id)
    }

    /// Check if this node is the cluster controller (same as Raft leader)
    /// The controller handles cluster-wide operations like leader election
    pub async fn is_controller(&self) -> bool {
        self.is_leader().await
    }

    /// Get the current leader node ID
    pub async fn leader_id(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// Get the current controller node ID (same as Raft leader)
    pub async fn controller_id(&self) -> Option<NodeId> {
        self.leader_id().await
    }

    /// Get current cluster metadata
    pub async fn metadata(&self) -> ClusterMetadata {
        self.store.get_metadata().await
    }

    /// Bootstrap a new cluster (only call on first node)
    pub async fn bootstrap(&self) -> Result<()> {
        let mut initialized = self.initialized.write().await;
        if *initialized {
            return Err(StreamlineError::Cluster(
                "Cluster already initialized".into(),
            ));
        }

        info!(node_id = self.node_id, "Bootstrapping new cluster");

        // Create initial membership with just this node
        raft::bootstrap_cluster(
            &self.raft,
            self.node_id,
            self.config.inter_broker_addr.to_string(),
        )
        .await
        .map_err(|e| StreamlineError::Cluster(format!("Bootstrap failed: {}", e)))?;

        // Register self as broker (with rack info if configured)
        let broker = BrokerInfo {
            node_id: self.node_id,
            advertised_addr: self.config.advertised_addr,
            inter_broker_addr: self.config.inter_broker_addr,
            rack: self.config.rack_id.clone(),
            datacenter: None,
            state: NodeState::Running,
        };

        info!(
            node_id = self.node_id,
            rack = ?self.config.rack_id,
            "Registering broker with rack info"
        );

        self.propose_command(ClusterCommand::RegisterBroker(broker))
            .await?;

        *initialized = true;
        info!(node_id = self.node_id, "Cluster bootstrap complete");

        Ok(())
    }

    /// Join an existing cluster
    pub async fn join_cluster(&self, seed_nodes: &[SocketAddr]) -> Result<()> {
        let mut initialized = self.initialized.write().await;
        if *initialized {
            return Err(StreamlineError::Cluster("Already joined a cluster".into()));
        }

        info!(node_id = self.node_id, "Joining existing cluster");

        let join_request = raft::JoinRequest {
            node_id: self.node_id,
            raft_addr: self.config.inter_broker_addr.to_string(),
            advertised_addr: self.config.advertised_addr.to_string(),
            version: Some(
                crate::cluster::version::StreamlineVersion::current().semver(),
            ),
        };

        let timeout = std::time::Duration::from_secs(10);
        let mut last_error = None;
        let tls = self.inter_broker_tls.as_ref();

        // Try each seed node
        for seed_addr in seed_nodes {
            info!(%seed_addr, tls_enabled = tls.is_some(), "Attempting to join via seed node");

            match raft::send_join_request_with_tls(
                &seed_addr.to_string(),
                join_request.clone(),
                timeout,
                tls,
            )
            .await
            {
                Ok(response) => {
                    if response.success {
                        info!(
                            node_id = self.node_id,
                            leader_id = ?response.leader_id,
                            "Successfully joined cluster"
                        );

                        // Register self as broker
                        let broker = BrokerInfo {
                            node_id: self.node_id,
                            advertised_addr: self.config.advertised_addr,
                            inter_broker_addr: self.config.inter_broker_addr,
                            rack: None,
                            datacenter: None,
                            state: NodeState::Running,
                        };

                        // Wait a bit for Raft to sync, then register
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                        if let Err(e) = self
                            .propose_command(ClusterCommand::RegisterBroker(broker))
                            .await
                        {
                            warn!(error = %e, "Failed to register broker after join (may be registered by leader)");
                        }

                        *initialized = true;
                        return Ok(());
                    } else if let Some(leader_addr) = response.leader_addr {
                        // Redirect to leader
                        info!(
                            leader_id = ?response.leader_id,
                            leader_addr = %leader_addr,
                            "Redirecting to leader"
                        );

                        match raft::send_join_request_with_tls(
                            &leader_addr,
                            join_request.clone(),
                            timeout,
                            tls,
                        )
                        .await
                        {
                            Ok(leader_response) if leader_response.success => {
                                info!(
                                    node_id = self.node_id,
                                    "Successfully joined cluster via leader redirect"
                                );

                                let broker = BrokerInfo {
                                    node_id: self.node_id,
                                    advertised_addr: self.config.advertised_addr,
                                    inter_broker_addr: self.config.inter_broker_addr,
                                    rack: None,
                                    datacenter: None,
                                    state: NodeState::Running,
                                };

                                tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                                if let Err(e) = self
                                    .propose_command(ClusterCommand::RegisterBroker(broker))
                                    .await
                                {
                                    warn!(error = %e, "Failed to register broker after join");
                                }

                                *initialized = true;
                                return Ok(());
                            }
                            Ok(leader_response) => {
                                last_error = Some(format!(
                                    "Leader rejected join: {:?}",
                                    leader_response.error
                                ));
                            }
                            Err(e) => {
                                last_error = Some(format!("Failed to connect to leader: {}", e));
                            }
                        }
                    } else {
                        last_error = response.error;
                    }
                }
                Err(e) => {
                    warn!(%seed_addr, error = %e, "Failed to connect to seed node");
                    last_error = Some(format!("Connection failed: {}", e));
                }
            }
        }

        Err(StreamlineError::Cluster(format!(
            "Failed to join cluster after trying {} seed node(s): {}",
            seed_nodes.len(),
            last_error.unwrap_or_else(|| "no seed nodes provided".to_string())
        )))
    }

    /// Propose a command to the Raft cluster
    ///
    /// Applies a 10-second timeout to prevent indefinite blocking if the
    /// cluster is partitioned or a leader election is in progress.
    pub async fn propose_command(&self, cmd: ClusterCommand) -> Result<ClusterResponse> {
        debug!(?cmd, "Proposing command to Raft");

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.raft.client_write(cmd),
        )
        .await
        .map_err(|_| {
            StreamlineError::Timeout(
                "Raft proposal timed out after 10s (cluster may be partitioned or leaderless)"
                    .to_string(),
            )
        })?
        .map_err(|e| StreamlineError::Cluster(format!("Raft write failed: {:?}", e)))?;

        Ok(response.data)
    }

    /// Create a topic with the given configuration
    pub async fn create_topic(
        &self,
        name: String,
        num_partitions: i32,
        replication_factor: i16,
    ) -> Result<()> {
        info!(
            topic = %name,
            partitions = num_partitions,
            replication_factor,
            "Creating topic"
        );

        // Get alive brokers for replica assignment
        let metadata = self.metadata().await;
        let alive_brokers: Vec<BrokerInfo> =
            metadata.alive_brokers().into_iter().cloned().collect();

        if alive_brokers.is_empty() {
            return Err(StreamlineError::Cluster("No alive brokers".into()));
        }

        if alive_brokers.len() < replication_factor as usize {
            return Err(StreamlineError::Cluster(format!(
                "Not enough brokers ({}) for replication factor ({})",
                alive_brokers.len(),
                replication_factor
            )));
        }

        // Create rack-aware assigner based on config
        let assigner = RackAwareAssigner::new(self.config.rack_aware_assignment);

        // Log rack distribution for debugging
        let rack_count = alive_brokers
            .iter()
            .filter_map(|b| b.rack.as_ref())
            .collect::<std::collections::HashSet<_>>()
            .len();
        debug!(
            rack_aware = self.config.rack_aware_assignment,
            rack_count,
            broker_count = alive_brokers.len(),
            "Creating topic with rack-aware assignment"
        );

        // Create partition assignments using rack-aware or round-robin strategy
        let mut assignment = TopicAssignment::new(name.clone(), num_partitions, replication_factor);

        for partition_id in 0..num_partitions {
            let replicas =
                assigner.assign_replicas(partition_id, replication_factor, &alive_brokers);

            if replicas.is_empty() {
                return Err(StreamlineError::Cluster(
                    "Failed to assign replicas for partition".into(),
                ));
            }

            let partition =
                PartitionAssignment::new(partition_id, replicas.clone()).with_leader(replicas[0]);

            assignment = assignment.with_partition(partition);
        }

        // Propose topic creation
        let cmd = ClusterCommand::CreateTopic {
            name: name.clone(),
            assignment,
        };

        match self.propose_command(cmd).await? {
            ClusterResponse::TopicCreated { name } => {
                info!(topic = %name, "Topic created successfully");
                Ok(())
            }
            ClusterResponse::Error(e) => Err(StreamlineError::Cluster(format!(
                "Failed to create topic: {}",
                e
            ))),
            _ => Ok(()),
        }
    }

    /// Delete a topic
    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        info!(topic = %name, "Deleting topic");

        let cmd = ClusterCommand::DeleteTopic(name.to_string());
        self.propose_command(cmd).await?;

        Ok(())
    }

    /// Update the leader for a partition
    pub async fn update_partition_leader(
        &self,
        topic: &str,
        partition: i32,
        leader: NodeId,
        leader_epoch: i32,
    ) -> Result<()> {
        debug!(
            topic,
            partition, leader, leader_epoch, "Updating partition leader"
        );

        let cmd = ClusterCommand::UpdateLeader {
            topic: topic.to_string(),
            partition,
            leader,
            leader_epoch,
        };

        self.propose_command(cmd).await?;
        Ok(())
    }

    /// Update the ISR for a partition
    pub async fn update_partition_isr(
        &self,
        topic: &str,
        partition: i32,
        isr: Vec<NodeId>,
    ) -> Result<()> {
        debug!(topic, partition, ?isr, "Updating partition ISR");

        let cmd = ClusterCommand::UpdateIsr {
            topic: topic.to_string(),
            partition,
            isr,
        };

        self.propose_command(cmd).await?;
        Ok(())
    }

    /// Run a cluster health check and return a health report
    pub async fn health_check(&self) -> ClusterHealthReport {
        let metadata = self.metadata().await;
        let mut monitor = self.health_monitor.write().await;
        monitor.evaluate(&metadata)
    }

    /// Plan a partition rebalance based on current cluster state
    pub async fn plan_rebalance(
        &self,
        strategy: RebalanceStrategy,
    ) -> RebalancePlan {
        let metadata = self.metadata().await;
        self.rebalancer.plan_rebalance(&metadata, strategy)
    }

    /// Execute a rebalance plan by applying partition movements via Raft
    pub async fn execute_rebalance(&self, plan: &RebalancePlan) -> Result<usize> {
        if plan.movements.is_empty() {
            return Ok(0);
        }

        info!(
            movements = plan.movements.len(),
            "Executing partition rebalance"
        );

        let mut applied = 0;
        for movement in &plan.movements {
            // Update replicas via ISR update
            let cmd = ClusterCommand::UpdateIsr {
                topic: movement.topic.clone(),
                partition: movement.partition_id,
                isr: movement.new_replicas.clone(),
            };

            match self.propose_command(cmd).await {
                Ok(_) => {
                    // Update leader
                    let epoch = self.metadata().await
                        .get_partition(&movement.topic, movement.partition_id)
                        .map(|p| p.leader_epoch + 1)
                        .unwrap_or(1);

                    if let Err(e) = self
                        .update_partition_leader(
                            &movement.topic,
                            movement.partition_id,
                            movement.new_leader,
                            epoch,
                        )
                        .await
                    {
                        warn!(
                            topic = movement.topic,
                            partition = movement.partition_id,
                            error = %e,
                            "Failed to update leader during rebalance"
                        );
                    }
                    applied += 1;
                }
                Err(e) => {
                    warn!(
                        topic = movement.topic,
                        partition = movement.partition_id,
                        error = %e,
                        "Failed to apply rebalance movement"
                    );
                }
            }
        }

        info!(
            applied,
            total = plan.movements.len(),
            "Rebalance execution complete"
        );
        Ok(applied)
    }

    /// Handle a node failure: trigger rebalance for affected partitions
    pub async fn handle_node_failure(&self, failed_node: NodeId) -> Result<usize> {
        info!(
            node_id = self.node_id,
            failed_node,
            "Handling node failure, planning rebalance"
        );

        // Mark the node as dead
        self.propose_command(ClusterCommand::UpdateBrokerState {
            node_id: failed_node,
            state: NodeState::Dead,
        })
        .await?;

        // Plan and execute rebalance to repair affected partitions
        let plan = self
            .plan_rebalance(rebalancer::RebalanceStrategy::RepairOnly)
            .await;

        if plan.movements.is_empty() {
            info!(failed_node, "No partition movements needed after node failure");
            return Ok(0);
        }

        self.execute_rebalance(&plan).await
    }

    /// Handle a new node joining: optionally rebalance for even distribution
    pub async fn handle_node_join(&self, new_node: NodeId) -> Result<usize> {
        info!(
            node_id = self.node_id,
            new_node,
            "New node joined, checking if rebalance is needed"
        );

        let plan = self
            .plan_rebalance(rebalancer::RebalanceStrategy::MinimalMovement)
            .await;

        if plan.movements.is_empty() {
            info!(new_node, "Cluster is balanced, no movements needed");
            return Ok(0);
        }

        self.execute_rebalance(&plan).await
    }

    /// Get cluster statistics from the health monitor
    pub async fn cluster_stats(&self) -> ClusterStats {
        let report = self.health_check().await;
        report.stats
    }

    /// Start the inter-broker listener for Raft RPC
    pub async fn start_rpc_listener(&self) -> Result<()> {
        let addr = self.config.inter_broker_addr;
        let rpc_handler = self.rpc_handler.clone();
        let tls = self.inter_broker_tls.clone();

        if tls.is_some() {
            info!(%addr, "Starting inter-broker RPC listener with TLS");
        } else {
            info!(%addr, "Starting inter-broker RPC listener");
        }

        let listener = TcpListener::bind(addr).await.map_err(StreamlineError::Io)?;

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, peer)) => {
                        debug!(%peer, "Accepted inter-broker connection");
                        let handler = rpc_handler.clone();
                        let tls = tls.clone();

                        tokio::spawn(async move {
                            let result = if let Some(ref tls) = tls {
                                handler.handle_tls_connection(stream, tls).await
                            } else {
                                handler.handle_connection(stream).await
                            };

                            if let Err(e) = result {
                                error!(%peer, error = %e, "RPC handler error");
                            }
                        });
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to accept inter-broker connection");
                    }
                }
            }
        });

        Ok(())
    }

    /// Get Raft metrics for monitoring
    pub fn metrics(&self) -> openraft::RaftMetrics<NodeId, BasicNode> {
        self.raft.metrics().borrow().clone()
    }

    /// Graceful shutdown with optional leadership transfer
    ///
    /// This method performs a graceful shutdown:
    /// 1. If this node is the controller (Raft leader), attempt to transfer leadership
    /// 2. Deregister from the cluster
    /// 3. Shutdown Raft and membership manager
    pub async fn shutdown(&self) -> Result<()> {
        info!(node_id = self.node_id, "Starting graceful shutdown...");

        // Step 1: If we're the controller, try to transfer leadership
        if self.is_controller().await {
            info!(
                node_id = self.node_id,
                "This node is the controller, attempting leadership transfer"
            );
            if let Err(e) = self.transfer_leadership().await {
                warn!(
                    error = %e,
                    "Failed to transfer leadership during shutdown (cluster may elect new leader)"
                );
            }
        }

        // Step 2: Notify cluster that this broker is going offline
        // This is best-effort with a timeout - if we can't reach the cluster, continue with shutdown
        match tokio::time::timeout(std::time::Duration::from_secs(2), self.deregister_broker())
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                warn!(
                    error = %e,
                    "Failed to deregister broker during shutdown"
                );
            }
            Err(_) => {
                warn!("Timeout deregistering broker during shutdown, continuing...");
            }
        }

        // Step 3: Shutdown replication manager (stops fetcher loop)
        info!(node_id = self.node_id, "Shutting down replication manager");
        self.replication.shutdown().await;

        // Step 4: Shutdown membership manager
        info!(node_id = self.node_id, "Shutting down membership manager");
        self.membership.shutdown().await;

        // Step 5: Shutdown Raft with timeout
        info!(node_id = self.node_id, "Shutting down Raft consensus");
        match tokio::time::timeout(std::time::Duration::from_secs(5), self.raft.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                error!(error = ?e, "Error during Raft shutdown");
            }
            Err(_) => {
                warn!("Timeout during Raft shutdown, forcing completion");
            }
        }

        info!(node_id = self.node_id, "Cluster manager shutdown complete");
        Ok(())
    }

    /// Prepare for leadership transfer before shutdown
    ///
    /// This is called during graceful shutdown to minimize disruption.
    /// OpenRaft 0.9 doesn't have explicit leadership transfer, so we:
    /// 1. Send a heartbeat to ensure followers are up-to-date
    /// 2. Wait briefly to allow replication to complete
    ///
    /// The remaining cluster members will elect a new leader after we shutdown.
    async fn transfer_leadership(&self) -> Result<()> {
        // Get all other members from the cluster
        let metrics = self.metrics();
        let membership = metrics.membership_config.membership();
        let voter_ids = membership.voter_ids();

        // Check if there are other voters to take over
        let other_voters: Vec<_> = voter_ids
            .into_iter()
            .filter(|&id| id != self.node_id)
            .collect();

        if other_voters.is_empty() {
            info!(
                node_id = self.node_id,
                "No other voters in cluster, leadership will be lost"
            );
            return Ok(());
        }

        info!(
            node_id = self.node_id,
            other_voters = ?other_voters,
            "Preparing leadership handoff to remaining voters"
        );

        // Send a heartbeat to ensure followers are caught up
        if let Err(e) = self.raft.trigger().heartbeat().await {
            warn!(error = ?e, "Failed to send final heartbeat before shutdown");
        }

        // Wait briefly for replication to complete
        // This gives followers time to receive any pending entries
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        info!(
            node_id = self.node_id,
            "Leadership handoff preparation complete, cluster will elect new leader"
        );

        Ok(())
    }

    /// Deregister this broker from the cluster
    ///
    /// Called during graceful shutdown to notify the cluster
    async fn deregister_broker(&self) -> Result<()> {
        debug!(node_id = self.node_id, "Deregistering broker from cluster");

        // Update our broker state to offline
        let metadata = self.metadata().await;
        if let Some(broker) = metadata.brokers.get(&self.node_id) {
            let mut updated_broker = broker.clone();
            updated_broker.state = NodeState::ShuttingDown;

            self.propose_command(ClusterCommand::RegisterBroker(updated_broker))
                .await?;

            info!(node_id = self.node_id, "Broker marked as shutting down");
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn test_config() -> ClusterConfig {
        ClusterConfig {
            node_id: 1,
            advertised_addr: "127.0.0.1:9092".parse().unwrap(),
            inter_broker_addr: "127.0.0.1:9093".parse().unwrap(),
            seed_nodes: vec![],
            default_replication_factor: 1,
            min_insync_replicas: 1,
            heartbeat_interval_ms: 500,
            session_timeout_ms: 10000,
            election_timeout_min_ms: 1000,
            election_timeout_max_ms: 2000,
            unclean_leader_election: false,
            inter_broker_tls: config::InterBrokerTlsConfig::default(),
            rack_id: None,
            rack_aware_assignment: true,
            snapshot_interval_logs: config::DEFAULT_SNAPSHOT_INTERVAL_LOGS,
            max_logs_in_snapshot: config::DEFAULT_MAX_LOGS_IN_SNAPSHOT,
            purge_batch_size: config::DEFAULT_PURGE_BATCH_SIZE,
            max_payload_entries: config::DEFAULT_MAX_PAYLOAD_ENTRIES,
        }
    }

    #[tokio::test]
    async fn test_cluster_manager_creation() {
        let dir = tempdir().unwrap();
        let config = test_config();

        let manager = ClusterManager::new(config.clone(), dir.path().to_path_buf())
            .await
            .unwrap();

        assert_eq!(manager.node_id(), 1);
        assert!(!manager.is_leader().await);
    }

    #[tokio::test]
    async fn test_cluster_bootstrap() {
        let dir = tempdir().unwrap();
        let config = test_config();

        let manager = ClusterManager::new(config.clone(), dir.path().to_path_buf())
            .await
            .unwrap();

        // Bootstrap should succeed
        manager.bootstrap().await.unwrap();

        // Give Raft time to elect leader
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Should be leader since we're the only node
        assert!(manager.is_leader().await);

        // Metadata should have our broker registered
        let metadata = manager.metadata().await;
        assert!(metadata.brokers.contains_key(&1));
    }
}
