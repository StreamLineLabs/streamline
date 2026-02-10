//! Failure detection and leader failover handling
//!
//! This module monitors node failures and triggers leader elections
//! when partition leaders become unavailable.

use crate::cluster::leadership::LeadershipManager;
use crate::cluster::membership::MembershipManager;
use crate::cluster::node::{NodeId, NodeState};
use crate::cluster::raft::types::ClusterCommand;
use crate::cluster::ClusterManager;
use parking_lot::RwLock;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

/// Configuration for failure detection and failover
#[derive(Debug, Clone)]
pub struct FailoverConfig {
    /// Interval at which to check for failed nodes
    pub check_interval_ms: u64,

    /// Number of consecutive failures before marking a node as dead
    pub failure_threshold: u32,

    /// Whether to allow unclean leader election (from non-ISR replicas)
    pub unclean_leader_election: bool,

    /// Minimum time to wait before re-electing a leader after failure
    pub leader_election_delay_ms: u64,
}

impl Default for FailoverConfig {
    fn default() -> Self {
        Self {
            check_interval_ms: 5000, // 5 seconds
            failure_threshold: 3,    // 3 consecutive failures
            unclean_leader_election: false,
            leader_election_delay_ms: 1000, // 1 second delay
        }
    }
}

/// Events emitted by the failover handler
#[derive(Debug, Clone)]
pub enum FailoverEvent {
    /// A node has been detected as failed
    NodeFailed { node_id: NodeId },

    /// A node has recovered
    NodeRecovered { node_id: NodeId },

    /// Leader election has been triggered for a partition
    LeaderElectionTriggered { topic: String, partition: i32 },

    /// A new leader has been elected
    NewLeaderElected {
        topic: String,
        partition: i32,
        new_leader: NodeId,
        leader_epoch: i32,
    },

    /// ISR has been shrunk for a partition
    IsrShrunk {
        topic: String,
        partition: i32,
        removed: Vec<NodeId>,
    },

    /// ISR has been expanded for a partition
    IsrExpanded {
        topic: String,
        partition: i32,
        added: Vec<NodeId>,
    },
}

/// Handles failure detection and leader failover
pub struct FailoverHandler {
    /// Local node ID
    node_id: NodeId,

    /// Configuration
    config: FailoverConfig,

    /// Reference to cluster manager
    cluster_manager: Arc<ClusterManager>,

    /// Reference to membership manager
    membership_manager: Arc<MembershipManager>,

    /// Reference to leadership manager
    leadership_manager: Arc<LeadershipManager>,

    /// Set of nodes currently known to be failed
    failed_nodes: Arc<RwLock<HashSet<NodeId>>>,

    /// Event broadcaster
    event_tx: broadcast::Sender<FailoverEvent>,

    /// Shutdown flag
    shutdown: Arc<RwLock<bool>>,
}

impl FailoverHandler {
    /// Create a new failover handler
    pub fn new(
        node_id: NodeId,
        config: FailoverConfig,
        cluster_manager: Arc<ClusterManager>,
        membership_manager: Arc<MembershipManager>,
        leadership_manager: Arc<LeadershipManager>,
    ) -> Self {
        let (event_tx, _) = broadcast::channel(100);

        Self {
            node_id,
            config,
            cluster_manager,
            membership_manager,
            leadership_manager,
            failed_nodes: Arc::new(RwLock::new(HashSet::new())),
            event_tx,
            shutdown: Arc::new(RwLock::new(false)),
        }
    }

    /// Get the local node ID
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Get the configuration
    pub fn config(&self) -> &FailoverConfig {
        &self.config
    }

    /// Subscribe to failover events
    pub fn subscribe(&self) -> broadcast::Receiver<FailoverEvent> {
        self.event_tx.subscribe()
    }

    /// Check if a node is currently marked as failed
    pub async fn is_node_failed(&self, node_id: NodeId) -> bool {
        self.failed_nodes.read().contains(&node_id)
    }

    /// Get all currently failed nodes
    pub async fn failed_nodes(&self) -> HashSet<NodeId> {
        self.failed_nodes.read().clone()
    }

    /// Start the failure detection and failover loop
    pub async fn start(&self) {
        let cluster_manager = self.cluster_manager.clone();
        let membership_manager = self.membership_manager.clone();
        let leadership_manager = self.leadership_manager.clone();
        let failed_nodes = self.failed_nodes.clone();
        let event_tx = self.event_tx.clone();
        let shutdown = self.shutdown.clone();
        let config = self.config.clone();
        let local_node_id = self.node_id;

        info!(
            node_id = local_node_id,
            check_interval_ms = config.check_interval_ms,
            "Starting failover handler"
        );

        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(config.check_interval_ms));

            loop {
                ticker.tick().await;

                // Check shutdown
                if *shutdown.read() {
                    info!("Failover handler shutting down");
                    break;
                }

                // Only the controller should handle failover
                if !cluster_manager.is_controller().await {
                    continue;
                }

                // Detect failed nodes
                let newly_failed =
                    detect_failed_nodes(&membership_manager, &failed_nodes, &event_tx).await;

                // Handle leader failover for partitions with failed leaders
                if !newly_failed.is_empty() {
                    handle_leader_failover(
                        &cluster_manager,
                        &leadership_manager,
                        &newly_failed,
                        &config,
                        &event_tx,
                    )
                    .await;

                    // Shrink ISR for affected partitions
                    shrink_isr_for_failed_nodes(&cluster_manager, &newly_failed, &event_tx).await;
                }

                // Check for recovered nodes
                detect_recovered_nodes(&membership_manager, &failed_nodes, &event_tx).await;
            }
        });
    }

    /// Signal shutdown
    pub async fn shutdown(&self) {
        *self.shutdown.write() = true;
    }

    /// Manually trigger leader election for a partition
    pub async fn trigger_leader_election(
        &self,
        topic: &str,
        partition: i32,
        preferred_leader: Option<NodeId>,
    ) -> Result<NodeId, String> {
        let _ = self.event_tx.send(FailoverEvent::LeaderElectionTriggered {
            topic: topic.to_string(),
            partition,
        });

        // Elect new leader
        let new_leader = self
            .leadership_manager
            .elect_leader(topic, partition, preferred_leader)
            .await?;

        // Get current leader epoch and increment
        let current_state = self
            .leadership_manager
            .get_leadership_state(topic, partition)
            .await
            .ok_or_else(|| "Partition not found".to_string())?;

        let new_epoch = current_state.leader_epoch + 1;

        // Update cluster metadata via Raft
        self.cluster_manager
            .propose_command(ClusterCommand::UpdateLeader {
                topic: topic.to_string(),
                partition,
                leader: new_leader,
                leader_epoch: new_epoch,
            })
            .await
            .map_err(|e| format!("Failed to update leader via Raft: {}", e))?;

        // Update local leadership cache
        self.leadership_manager
            .update_leadership(topic, partition, new_leader, new_epoch)
            .await;

        let _ = self.event_tx.send(FailoverEvent::NewLeaderElected {
            topic: topic.to_string(),
            partition,
            new_leader,
            leader_epoch: new_epoch,
        });

        info!(
            topic,
            partition,
            new_leader,
            leader_epoch = new_epoch,
            "Leader election completed"
        );

        Ok(new_leader)
    }
}

/// Detect newly failed nodes
async fn detect_failed_nodes(
    membership_manager: &Arc<MembershipManager>,
    failed_nodes: &Arc<RwLock<HashSet<NodeId>>>,
    event_tx: &broadcast::Sender<FailoverEvent>,
) -> Vec<NodeId> {
    let peers = membership_manager.peers().await;
    let mut newly_failed = Vec::new();
    let mut failed_set = failed_nodes.write();

    for (node_id, peer_info) in peers {
        if peer_info.broker.state == NodeState::Dead && !failed_set.contains(&node_id) {
            warn!(node_id, "Node detected as failed");
            newly_failed.push(node_id);
            failed_set.insert(node_id);

            let _ = event_tx.send(FailoverEvent::NodeFailed { node_id });
        }
    }

    newly_failed
}

/// Detect nodes that have recovered
async fn detect_recovered_nodes(
    membership_manager: &Arc<MembershipManager>,
    failed_nodes: &Arc<RwLock<HashSet<NodeId>>>,
    event_tx: &broadcast::Sender<FailoverEvent>,
) {
    let peers = membership_manager.peers().await;
    let mut failed_set = failed_nodes.write();

    let recovered: Vec<NodeId> = failed_set
        .iter()
        .filter(|&node_id| {
            peers
                .get(node_id)
                .map(|p| p.broker.state == NodeState::Running)
                .unwrap_or(false)
        })
        .cloned()
        .collect();

    for node_id in recovered {
        info!(node_id, "Node recovered");
        failed_set.remove(&node_id);
        let _ = event_tx.send(FailoverEvent::NodeRecovered { node_id });
    }
}

/// Handle leader failover for partitions whose leaders have failed
async fn handle_leader_failover(
    cluster_manager: &Arc<ClusterManager>,
    leadership_manager: &Arc<LeadershipManager>,
    failed_nodes: &[NodeId],
    config: &FailoverConfig,
    event_tx: &broadcast::Sender<FailoverEvent>,
) {
    let metadata = cluster_manager.metadata().await;

    for (topic_name, topic_assignment) in &metadata.topics {
        for (&partition_id, partition_assignment) in &topic_assignment.partitions {
            // Check if the leader is in the failed nodes list
            if let Some(leader) = partition_assignment.leader {
                if failed_nodes.contains(&leader) {
                    warn!(
                        topic = %topic_name,
                        partition = partition_id,
                        failed_leader = leader,
                        "Leader has failed, triggering election"
                    );

                    let _ = event_tx.send(FailoverEvent::LeaderElectionTriggered {
                        topic: topic_name.clone(),
                        partition: partition_id,
                    });

                    // Small delay before election to allow cluster state to stabilize
                    tokio::time::sleep(Duration::from_millis(config.leader_election_delay_ms))
                        .await;

                    // Elect new leader
                    match leadership_manager
                        .elect_leader(topic_name, partition_id, None)
                        .await
                    {
                        Ok(new_leader) => {
                            let new_epoch = partition_assignment.leader_epoch + 1;

                            // Update via Raft
                            if let Err(e) = cluster_manager
                                .propose_command(ClusterCommand::UpdateLeader {
                                    topic: topic_name.clone(),
                                    partition: partition_id,
                                    leader: new_leader,
                                    leader_epoch: new_epoch,
                                })
                                .await
                            {
                                error!(
                                    topic = %topic_name,
                                    partition = partition_id,
                                    error = %e,
                                    "Failed to update leader via Raft"
                                );
                                continue;
                            }

                            // Update local cache
                            leadership_manager
                                .update_leadership(topic_name, partition_id, new_leader, new_epoch)
                                .await;

                            let _ = event_tx.send(FailoverEvent::NewLeaderElected {
                                topic: topic_name.clone(),
                                partition: partition_id,
                                new_leader,
                                leader_epoch: new_epoch,
                            });

                            info!(
                                topic = %topic_name,
                                partition = partition_id,
                                new_leader,
                                leader_epoch = new_epoch,
                                "New leader elected after failover"
                            );
                        }
                        Err(e) => {
                            error!(
                                topic = %topic_name,
                                partition = partition_id,
                                error = %e,
                                "Failed to elect new leader"
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Shrink ISR for partitions that have replicas in failed nodes
async fn shrink_isr_for_failed_nodes(
    cluster_manager: &Arc<ClusterManager>,
    failed_nodes: &[NodeId],
    event_tx: &broadcast::Sender<FailoverEvent>,
) {
    let metadata = cluster_manager.metadata().await;

    for (topic_name, topic_assignment) in &metadata.topics {
        for (&partition_id, partition_assignment) in &topic_assignment.partitions {
            // Find ISR members that are in failed nodes
            let failed_isr: Vec<NodeId> = partition_assignment
                .isr
                .iter()
                .filter(|node_id| failed_nodes.contains(node_id))
                .cloned()
                .collect();

            if failed_isr.is_empty() {
                continue;
            }

            // Calculate new ISR
            let new_isr: Vec<NodeId> = partition_assignment
                .isr
                .iter()
                .filter(|node_id| !failed_nodes.contains(node_id))
                .cloned()
                .collect();

            debug!(
                topic = %topic_name,
                partition = partition_id,
                removed = ?failed_isr,
                new_isr = ?new_isr,
                "Shrinking ISR due to failed nodes"
            );

            // Update via Raft
            if let Err(e) = cluster_manager
                .propose_command(ClusterCommand::UpdateIsr {
                    topic: topic_name.clone(),
                    partition: partition_id,
                    isr: new_isr,
                })
                .await
            {
                error!(
                    topic = %topic_name,
                    partition = partition_id,
                    error = %e,
                    "Failed to update ISR via Raft"
                );
                continue;
            }

            let _ = event_tx.send(FailoverEvent::IsrShrunk {
                topic: topic_name.clone(),
                partition: partition_id,
                removed: failed_isr,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterConfig;
    use tempfile::tempdir;

    fn test_config() -> FailoverConfig {
        FailoverConfig {
            check_interval_ms: 100,
            failure_threshold: 3,
            unclean_leader_election: false,
            leader_election_delay_ms: 10,
        }
    }

    #[test]
    fn test_failover_config_default() {
        let config = FailoverConfig::default();
        assert_eq!(config.check_interval_ms, 5000);
        assert_eq!(config.failure_threshold, 3);
        assert!(!config.unclean_leader_election);
    }

    #[tokio::test]
    async fn test_failover_handler_creation() {
        let dir = tempdir().unwrap();
        let cluster_config = ClusterConfig {
            node_id: 1,
            advertised_addr: "127.0.0.1:9092".parse().unwrap(),
            inter_broker_addr: "127.0.0.1:9093".parse().unwrap(),
            ..Default::default()
        };

        let cluster_manager = Arc::new(
            ClusterManager::new(cluster_config.clone(), dir.path().to_path_buf())
                .await
                .unwrap(),
        );

        let membership_manager = Arc::new(MembershipManager::new(1, cluster_config.clone()));
        let leadership_manager = Arc::new(LeadershipManager::new(1, cluster_manager.clone()));

        let handler = FailoverHandler::new(
            1,
            test_config(),
            cluster_manager,
            membership_manager,
            leadership_manager,
        );

        assert_eq!(handler.node_id(), 1);
        assert!(handler.failed_nodes().await.is_empty());
    }

    #[tokio::test]
    async fn test_is_node_failed() {
        let dir = tempdir().unwrap();
        let cluster_config = ClusterConfig {
            node_id: 1,
            advertised_addr: "127.0.0.1:9092".parse().unwrap(),
            inter_broker_addr: "127.0.0.1:9093".parse().unwrap(),
            ..Default::default()
        };

        let cluster_manager = Arc::new(
            ClusterManager::new(cluster_config.clone(), dir.path().to_path_buf())
                .await
                .unwrap(),
        );

        let membership_manager = Arc::new(MembershipManager::new(1, cluster_config.clone()));
        let leadership_manager = Arc::new(LeadershipManager::new(1, cluster_manager.clone()));

        let handler = FailoverHandler::new(
            1,
            test_config(),
            cluster_manager,
            membership_manager,
            leadership_manager,
        );

        assert!(!handler.is_node_failed(2).await);

        // Manually add a failed node
        handler.failed_nodes.write().insert(2);
        assert!(handler.is_node_failed(2).await);
    }

    #[tokio::test]
    async fn test_subscribe_to_events() {
        let dir = tempdir().unwrap();
        let cluster_config = ClusterConfig {
            node_id: 1,
            advertised_addr: "127.0.0.1:9092".parse().unwrap(),
            inter_broker_addr: "127.0.0.1:9093".parse().unwrap(),
            ..Default::default()
        };

        let cluster_manager = Arc::new(
            ClusterManager::new(cluster_config.clone(), dir.path().to_path_buf())
                .await
                .unwrap(),
        );

        let membership_manager = Arc::new(MembershipManager::new(1, cluster_config.clone()));
        let leadership_manager = Arc::new(LeadershipManager::new(1, cluster_manager.clone()));

        let handler = FailoverHandler::new(
            1,
            test_config(),
            cluster_manager,
            membership_manager,
            leadership_manager,
        );

        let mut rx = handler.subscribe();

        // Send an event
        let _ = handler
            .event_tx
            .send(FailoverEvent::NodeFailed { node_id: 2 });

        // Receive the event
        let event = rx.recv().await.unwrap();
        match event {
            FailoverEvent::NodeFailed { node_id } => {
                assert_eq!(node_id, 2);
            }
            _ => panic!("Unexpected event type"),
        }
    }
}
