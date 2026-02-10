//! HTTP REST API for Raft cluster health and membership management
//!
//! This module provides REST endpoints for cluster health monitoring and
//! dynamic membership management when clustering is enabled.
//!
//! ## Endpoints
//!
//! - `GET /api/v1/cluster/status` - Cluster health status (Raft state, leader, log indices)
//! - `GET /api/v1/cluster/members` - List cluster members with roles
//! - `POST /api/v1/cluster/members` - Add a new member (learner then voter)
//! - `DELETE /api/v1/cluster/members/{node_id}` - Remove a member from the cluster

use crate::cluster::ClusterManager;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{error, info, warn};

/// Shared state for the Raft cluster API
#[derive(Clone)]
pub(crate) struct RaftClusterApiState {
    pub cluster_manager: Arc<ClusterManager>,
}

/// Cluster health status response
#[derive(Debug, Serialize)]
pub struct ClusterStatus {
    /// Current Raft state of this node (Leader, Follower, Candidate, Learner)
    pub node_state: String,
    /// This node's ID
    pub node_id: u64,
    /// Current leader node ID (None if no leader elected)
    pub leader_id: Option<u64>,
    /// Last committed log index
    pub committed_log_index: Option<u64>,
    /// Last applied log index
    pub applied_log_index: Option<u64>,
    /// Current Raft term
    pub current_term: u64,
    /// List of voter node IDs in the cluster
    pub voters: Vec<u64>,
    /// List of learner node IDs in the cluster
    pub learners: Vec<u64>,
    /// Whether this node is healthy (can accept reads/writes)
    pub healthy: bool,
    /// Human-readable health description
    pub health_reason: String,
}

/// Cluster member information
#[derive(Debug, Serialize)]
pub struct ClusterMember {
    /// Node ID
    pub node_id: u64,
    /// Raft address for inter-node communication
    pub raft_addr: String,
    /// Role in the cluster (voter or learner)
    pub role: String,
    /// Whether this node is the current leader
    pub is_leader: bool,
    /// Broker state from cluster metadata (if registered)
    pub broker_state: Option<String>,
    /// Advertised address for client connections (if registered)
    pub advertised_addr: Option<String>,
}

/// Members list response
#[derive(Debug, Serialize)]
pub struct MembersResponse {
    pub members: Vec<ClusterMember>,
    pub total: usize,
    pub leader_id: Option<u64>,
}

/// Request to add a new member
#[derive(Debug, Deserialize)]
pub struct AddMemberRequest {
    /// Node ID for the new member
    pub node_id: u64,
    /// Raft address (inter-broker) for the new member
    pub raft_addr: String,
    /// Whether to promote to voter immediately (default: true)
    #[serde(default = "default_promote")]
    pub promote_to_voter: bool,
}

fn default_promote() -> bool {
    true
}

/// Error response
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

/// Create the Raft cluster API router
pub(crate) fn create_raft_cluster_api_router(state: RaftClusterApiState) -> Router {
    Router::new()
        .route("/api/v1/cluster/status", get(cluster_status_handler))
        .route(
            "/api/v1/cluster/members",
            get(list_members_handler).post(add_member_handler),
        )
        .route(
            "/api/v1/cluster/members/:node_id",
            delete(remove_member_handler),
        )
        .with_state(state)
}

/// GET /api/v1/cluster/status - Get cluster health status
async fn cluster_status_handler(State(state): State<RaftClusterApiState>) -> Response {
    let metrics = state.cluster_manager.metrics();
    let node_id = state.cluster_manager.node_id();

    let node_state = format!("{:?}", metrics.state);
    let leader_id = metrics.current_leader;
    let current_term = metrics.current_term;

    let committed_log_index = metrics.last_log_index;
    let applied_log_index = metrics.last_applied.map(|log_id| log_id.index);

    let membership = metrics.membership_config.membership();
    let voters: Vec<u64> = membership.voter_ids().collect();
    let learner_ids = membership.learner_ids();
    let learners: Vec<u64> = learner_ids.collect();

    // Determine health: node is healthy if there's a known leader and this node is a voter
    let is_voter = voters.contains(&node_id);
    let has_leader = leader_id.is_some();
    let healthy = has_leader && is_voter;

    let health_reason = if !has_leader {
        "No leader elected — cluster may be starting or partitioned".to_string()
    } else if !is_voter {
        "Node is a learner, not yet a voting member".to_string()
    } else {
        "Node is a healthy voting member with an elected leader".to_string()
    };

    let status = ClusterStatus {
        node_state,
        node_id,
        leader_id,
        committed_log_index,
        applied_log_index,
        current_term,
        voters,
        learners,
        healthy,
        health_reason,
    };

    (StatusCode::OK, Json(status)).into_response()
}

/// GET /api/v1/cluster/members - List cluster members
async fn list_members_handler(State(state): State<RaftClusterApiState>) -> Response {
    let metrics = state.cluster_manager.metrics();
    let leader_id = metrics.current_leader;

    let membership = metrics.membership_config.membership();
    let voter_ids: Vec<u64> = membership.voter_ids().collect();
    let learner_ids: Vec<u64> = membership.learner_ids().collect();

    // Get node addresses from membership config
    let nodes = membership.nodes();
    let node_map: HashMap<u64, String> = nodes
        .map(|(id, node)| (*id, node.addr.clone()))
        .collect();

    // Get broker metadata for additional info
    let metadata = state.cluster_manager.metadata().await;

    let mut members = Vec::new();
    for node_id in voter_ids.iter().chain(learner_ids.iter()) {
        let role = if voter_ids.contains(node_id) {
            "voter"
        } else {
            "learner"
        };
        let raft_addr = node_map
            .get(node_id)
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());
        let is_leader = leader_id == Some(*node_id);

        let (broker_state, advertised_addr) = if let Some(broker) = metadata.brokers.get(node_id) {
            (
                Some(broker.state.to_string()),
                Some(broker.advertised_addr.to_string()),
            )
        } else {
            (None, None)
        };

        members.push(ClusterMember {
            node_id: *node_id,
            raft_addr,
            role: role.to_string(),
            is_leader,
            broker_state,
            advertised_addr,
        });
    }

    let total = members.len();
    let response = MembersResponse {
        members,
        total,
        leader_id,
    };

    (StatusCode::OK, Json(response)).into_response()
}

/// POST /api/v1/cluster/members - Add a new member
async fn add_member_handler(
    State(state): State<RaftClusterApiState>,
    Json(request): Json<AddMemberRequest>,
) -> Response {
    info!(
        node_id = request.node_id,
        raft_addr = %request.raft_addr,
        promote = request.promote_to_voter,
        "Adding new cluster member via API"
    );

    // Only the leader can modify membership
    if !state.cluster_manager.is_leader().await {
        let leader_id = state.cluster_manager.leader_id().await;
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "This node is not the leader",
                "leader_id": leader_id,
            })),
        )
            .into_response();
    }

    // Add as learner first
    if let Err(e) = crate::cluster::raft::add_learner(
        &state.cluster_manager.raft_ref(),
        request.node_id,
        request.raft_addr.clone(),
    )
    .await
    {
        error!(error = %e, "Failed to add learner");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to add learner: {}", e),
            }),
        )
            .into_response();
    }

    // Optionally promote to voter
    if request.promote_to_voter {
        // Get current membership and add the new node
        let metrics = state.cluster_manager.metrics();
        let membership = metrics.membership_config.membership();
        let nodes = membership.nodes();
        let mut new_members = std::collections::BTreeMap::new();
        for (id, node) in nodes {
            new_members.insert(*id, node.clone());
        }
        new_members.insert(
            request.node_id,
            openraft::BasicNode::new(request.raft_addr.clone()),
        );

        if let Err(e) =
            crate::cluster::raft::change_membership(&state.cluster_manager.raft_ref(), new_members)
                .await
        {
            warn!(error = %e, "Learner added but promotion to voter failed");
            return (
                StatusCode::OK,
                Json(serde_json::json!({
                    "node_id": request.node_id,
                    "status": "learner",
                    "warning": format!("Added as learner but promotion failed: {}", e),
                })),
            )
                .into_response();
        }
    }

    let status = if request.promote_to_voter {
        "voter"
    } else {
        "learner"
    };

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "node_id": request.node_id,
            "raft_addr": request.raft_addr,
            "status": status,
        })),
    )
        .into_response()
}

/// DELETE /api/v1/cluster/members/{node_id} - Remove a member
async fn remove_member_handler(
    State(state): State<RaftClusterApiState>,
    Path(node_id): Path<u64>,
) -> Response {
    info!(node_id, "Removing cluster member via API");

    // Only the leader can modify membership
    if !state.cluster_manager.is_leader().await {
        let leader_id = state.cluster_manager.leader_id().await;
        return (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "error": "This node is not the leader",
                "leader_id": leader_id,
            })),
        )
            .into_response();
    }

    // Cannot remove self if we're the leader
    if node_id == state.cluster_manager.node_id() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Cannot remove the current leader. Transfer leadership first.".to_string(),
            }),
        )
            .into_response();
    }

    // Remove from Raft membership by creating new membership without this node
    let metrics = state.cluster_manager.metrics();
    let membership = metrics.membership_config.membership();
    let nodes = membership.nodes();
    let new_members: std::collections::BTreeMap<u64, openraft::BasicNode> = nodes
        .filter(|(id, _)| **id != node_id)
        .map(|(id, node)| (*id, node.clone()))
        .collect();

    if new_members.len() == membership.nodes().count() {
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Node {} is not a member of the cluster", node_id),
            }),
        )
            .into_response();
    }

    if let Err(e) =
        crate::cluster::raft::change_membership(&state.cluster_manager.raft_ref(), new_members)
            .await
    {
        error!(error = %e, node_id, "Failed to remove member");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: format!("Failed to remove member: {}", e),
            }),
        )
            .into_response();
    }

    // Also deregister broker from cluster metadata
    if let Err(e) = state
        .cluster_manager
        .propose_command(crate::cluster::ClusterCommand::RemoveBroker(node_id))
        .await
    {
        warn!(error = %e, node_id, "Member removed from Raft but failed to deregister broker");
    }

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "node_id": node_id,
            "status": "removed",
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_cluster_api_state_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<RaftClusterApiState>();
    }

    #[test]
    fn test_add_member_request_deserialization() {
        let json = r#"{"node_id": 2, "raft_addr": "192.168.1.2:9093"}"#;
        let request: AddMemberRequest =
            serde_json::from_str(json).expect("should deserialize");
        assert_eq!(request.node_id, 2);
        assert_eq!(request.raft_addr, "192.168.1.2:9093");
        assert!(request.promote_to_voter); // default true
    }

    #[test]
    fn test_add_member_request_no_promote() {
        let json = r#"{"node_id": 3, "raft_addr": "192.168.1.3:9093", "promote_to_voter": false}"#;
        let request: AddMemberRequest =
            serde_json::from_str(json).expect("should deserialize");
        assert!(!request.promote_to_voter);
    }
}
