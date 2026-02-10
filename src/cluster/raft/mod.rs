//! Raft consensus integration for Streamline cluster
//!
//! This module provides Raft-based consensus for cluster metadata using openraft.
//! It handles leader election, log replication, and state machine management.

pub mod network;
pub mod state_machine;
pub mod storage;
pub mod types;

pub use network::{send_join_request_with_tls, JoinRequest, NetworkFactory, RaftRpcHandler};
pub use state_machine::ClusterMetadata;
pub use storage::StreamlineStore;
pub use types::{
    ClusterCommand, ClusterResponse, PartitionAssignment, StreamlineTypeConfig, TopicAssignment,
};

use crate::cluster::node::NodeId;
use crate::error::StreamlineError;
#[allow(deprecated)]
use openraft::storage::Adaptor;
use openraft::{BasicNode, Config, Raft};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, info};

/// Type alias for our Raft instance
pub type StreamlineRaft = Raft<StreamlineTypeConfig>;

/// Create a new Raft configuration from cluster config
pub fn create_raft_config(
    heartbeat_interval_ms: u64,
    election_timeout_min_ms: u64,
    election_timeout_max_ms: u64,
) -> Result<Arc<Config>, StreamlineError> {
    create_raft_config_full(
        heartbeat_interval_ms,
        election_timeout_min_ms,
        election_timeout_max_ms,
        crate::cluster::config::DEFAULT_MAX_PAYLOAD_ENTRIES,
        crate::cluster::config::DEFAULT_MAX_LOGS_IN_SNAPSHOT,
        crate::cluster::config::DEFAULT_PURGE_BATCH_SIZE,
        crate::cluster::config::DEFAULT_SNAPSHOT_INTERVAL_LOGS,
    )
}

/// Create a new Raft configuration with full snapshot/compaction control
pub fn create_raft_config_full(
    heartbeat_interval_ms: u64,
    election_timeout_min_ms: u64,
    election_timeout_max_ms: u64,
    max_payload_entries: u64,
    max_in_snapshot_log_to_keep: u64,
    purge_batch_size: u64,
    snapshot_interval_logs: u64,
) -> Result<Arc<Config>, StreamlineError> {
    let config = Config {
        heartbeat_interval: heartbeat_interval_ms,
        election_timeout_min: election_timeout_min_ms,
        election_timeout_max: election_timeout_max_ms,
        max_payload_entries,
        max_in_snapshot_log_to_keep,
        purge_batch_size,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(snapshot_interval_logs),
        ..Default::default()
    };

    Ok(Arc::new(config.validate().map_err(|e| {
        StreamlineError::Config(format!("Invalid Raft configuration: {}", e))
    })?))
}

/// Initialize a new Raft node
pub async fn initialize_raft(
    node_id: NodeId,
    data_dir: PathBuf,
    cluster_id: String,
    config: Arc<Config>,
) -> Result<
    (StreamlineRaft, Arc<StreamlineStore>, Arc<RaftRpcHandler>),
    Box<dyn std::error::Error + Send + Sync>,
> {
    info!(node_id, %cluster_id, "Initializing Raft node");

    // Create storage directory
    let raft_dir = data_dir.join("raft");
    std::fs::create_dir_all(&raft_dir)?;

    // Initialize combined storage (logs + state machine)
    let store = Arc::new(StreamlineStore::new(raft_dir, cluster_id)?);

    // Set self reference for snapshot builder to work
    store.set_self_ref(store.clone()).await;

    // Create network factory
    let network = NetworkFactory::new(node_id);

    // Create RPC handler
    let rpc_handler = Arc::new(RaftRpcHandler::new(node_id));

    // Use Adaptor to convert RaftStorage to RaftLogStorage + RaftStateMachine
    #[allow(deprecated)]
    let (log_store, state_machine) = Adaptor::new(store.clone());

    // Create Raft instance
    let raft = Raft::new(node_id, config, network, log_store, state_machine).await?;

    // Set Raft reference in RPC handler
    rpc_handler.set_raft(raft.clone()).await;

    debug!(node_id, "Raft node initialized");

    Ok((raft, store, rpc_handler))
}

/// Bootstrap a new cluster with a single node
pub async fn bootstrap_cluster(
    raft: &StreamlineRaft,
    node_id: NodeId,
    node_addr: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(node_id, %node_addr, "Bootstrapping new cluster");

    let mut members = BTreeMap::new();
    members.insert(node_id, BasicNode::new(node_addr));

    raft.initialize(members).await?;

    info!(node_id, "Cluster bootstrap complete");
    Ok(())
}

/// Add a learner node to the cluster
pub async fn add_learner(
    raft: &StreamlineRaft,
    node_id: NodeId,
    node_addr: String,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(node_id, %node_addr, "Adding learner node");

    raft.add_learner(node_id, BasicNode::new(node_addr), true)
        .await?;

    info!(node_id, "Learner node added");
    Ok(())
}

/// Promote learner to voter
pub async fn change_membership(
    raft: &StreamlineRaft,
    members: BTreeMap<NodeId, BasicNode>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!(?members, "Changing cluster membership");

    raft.change_membership(
        members
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>(),
        true,
    )
    .await?;

    info!("Membership change complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_raft_config() {
        let config = create_raft_config(500, 1000, 2000).unwrap();
        assert_eq!(config.heartbeat_interval, 500);
        assert_eq!(config.election_timeout_min, 1000);
        assert_eq!(config.election_timeout_max, 2000);
    }
}
