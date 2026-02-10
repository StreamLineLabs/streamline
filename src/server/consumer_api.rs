//! HTTP REST API for consumer group management and lag monitoring
//!
//! This module provides REST endpoints for monitoring consumer groups:
//!
//! ## Endpoints
//!
//! - `GET /api/v1/consumer-groups` - List all consumer groups
//! - `GET /api/v1/consumer-groups/{group}` - Get consumer group details
//! - `GET /api/v1/consumer-groups/{group}/lag` - Get consumer group lag
//! - `GET /api/v1/consumer-groups/{group}/lag/{topic}` - Get lag for specific topic
//! - `DELETE /api/v1/consumer-groups/{group}` - Delete a consumer group

use crate::consumer::{ConsumerGroupLag, GroupCoordinator, LagCalculator, LagStats, TopicLag};
use crate::storage::TopicManager;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

/// Shared state for the consumer groups API
#[derive(Clone)]
pub(crate) struct ConsumerApiState {
    /// Group coordinator for consumer group operations
    pub coordinator: Arc<GroupCoordinator>,
    /// Topic manager for offset lookups
    pub topic_manager: Arc<TopicManager>,
}

/// Consumer group information for list response (matches UI client ConsumerGroupInfo)
#[derive(Debug, Serialize)]
pub struct ConsumerGroupInfo {
    pub group_id: String,
    pub state: String,
    pub member_count: usize,
    pub coordinator: Option<u64>,
    pub protocol_type: String,
}

/// Detailed consumer group information (matches UI client ConsumerGroupDetails)
#[derive(Debug, Serialize)]
pub struct ConsumerGroupDetails {
    pub group_id: String,
    pub state: String,
    pub protocol_type: String,
    pub protocol: String,
    pub coordinator: Option<u64>,
    pub members: Vec<ConsumerGroupMember>,
    pub offsets: Vec<ConsumerGroupOffset>,
}

/// Consumer group member (matches UI client ConsumerGroupMember)
#[derive(Debug, Serialize)]
pub struct ConsumerGroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub assignments: Vec<MemberAssignment>,
}

/// Member assignment (matches UI client MemberAssignment)
#[derive(Debug, Serialize)]
pub struct MemberAssignment {
    pub topic: String,
    pub partitions: Vec<i32>,
}

/// Consumer group offset (matches UI client ConsumerGroupOffset)
#[derive(Debug, Serialize)]
pub struct ConsumerGroupOffset {
    pub topic: String,
    pub partition: i32,
    pub committed_offset: i64,
    pub metadata: Option<String>,
}

/// Lag response with statistics
#[derive(Debug, Serialize)]
pub struct LagResponse {
    /// Consumer group lag information
    #[serde(flatten)]
    pub lag: ConsumerGroupLag,
    /// Lag statistics
    pub stats: LagStats,
}

/// Topic lag response
#[derive(Debug, Serialize)]
pub struct TopicLagResponse {
    /// Group ID
    pub group_id: String,
    /// Topic lag information
    #[serde(flatten)]
    pub topic_lag: TopicLag,
    /// Lag statistics for this topic
    pub stats: LagStats,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// Error code
    pub error: String,
    /// Error message
    pub message: String,
}

impl ErrorResponse {
    fn new(error: &str, message: impl Into<String>) -> Self {
        Self {
            error: error.to_string(),
            message: message.into(),
        }
    }
}

/// Strategy for resetting consumer group offsets
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ResetStrategy {
    /// Reset to earliest available offset
    Earliest,
    /// Reset to latest (end) offset
    Latest,
    /// Reset to specific timestamp (requires timestamp_ms field)
    Timestamp,
    /// Reset to specific offset (requires offset field)
    Offset,
}

/// Request to reset consumer group offsets
#[derive(Debug, Clone, Deserialize)]
pub struct ResetOffsetsRequest {
    /// Topic to reset offsets for
    pub topic: String,
    /// Reset strategy
    pub strategy: ResetStrategy,
    /// Specific partitions to reset (None = all partitions)
    pub partitions: Option<Vec<i32>>,
    /// Timestamp for Timestamp strategy (Unix timestamp in ms)
    pub timestamp_ms: Option<i64>,
    /// Specific offset for Offset strategy
    pub offset: Option<i64>,
}

/// Single partition offset result
#[derive(Debug, Clone, Serialize)]
pub struct PartitionOffsetResult {
    /// Partition ID
    pub partition: i32,
    /// Previous committed offset
    pub previous_offset: i64,
    /// New offset after reset
    pub new_offset: i64,
    /// Error message if reset failed for this partition
    pub error: Option<String>,
}

/// Response from offset reset operation
#[derive(Debug, Clone, Serialize)]
pub struct ResetOffsetsResponse {
    /// Consumer group ID
    pub group_id: String,
    /// Topic name
    pub topic: String,
    /// Strategy used
    pub strategy: String,
    /// Whether this was a dry run (preview only)
    pub dry_run: bool,
    /// Results for each partition
    pub partitions: Vec<PartitionOffsetResult>,
    /// Number of successful resets
    pub success_count: usize,
    /// Number of failed resets
    pub error_count: usize,
}

/// Lag severity level for heatmap coloring
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LagSeverity {
    Low,
    Medium,
    High,
    Critical,
}

impl LagSeverity {
    fn from_lag(lag: i64) -> Self {
        match lag {
            0..=100 => Self::Low,
            101..=1000 => Self::Medium,
            1001..=10000 => Self::High,
            _ => Self::Critical,
        }
    }
}

/// Heatmap cell data for a group/topic combination
#[derive(Debug, Clone, Serialize)]
pub struct HeatmapCell {
    /// Total lag for this group/topic combination
    pub total_lag: i64,
    /// Severity level for coloring
    pub severity: LagSeverity,
    /// Number of partitions
    pub partition_count: usize,
}

/// Heatmap data response
#[derive(Debug, Clone, Serialize)]
pub struct HeatmapData {
    /// List of consumer group IDs (rows)
    pub groups: Vec<String>,
    /// List of topic names (columns)
    pub topics: Vec<String>,
    /// Cells indexed by [group_idx][topic_idx], None if no subscription
    pub cells: Vec<Vec<Option<HeatmapCell>>>,
}

/// Create the consumer groups API router
pub(crate) fn create_consumer_api_router(state: ConsumerApiState) -> Router {
    Router::new()
        .route("/api/v1/consumer-groups", get(list_consumer_groups))
        .route("/api/v1/consumer-groups/heatmap", get(get_lag_heatmap))
        .route("/api/v1/consumer-groups/:group", get(get_consumer_group))
        .route(
            "/api/v1/consumer-groups/:group",
            delete(delete_consumer_group),
        )
        .route("/api/v1/consumer-groups/:group/lag", get(get_group_lag))
        .route(
            "/api/v1/consumer-groups/:group/lag/:topic",
            get(get_topic_lag),
        )
        .route(
            "/api/v1/consumer-groups/:group/reset-offsets",
            post(reset_offsets),
        )
        .route(
            "/api/v1/consumer-groups/:group/reset-offsets/dry-run",
            post(reset_offsets_dry_run),
        )
        .with_state(state)
}

/// List all consumer groups
async fn list_consumer_groups(State(state): State<ConsumerApiState>) -> Response {
    match state.coordinator.list_groups() {
        Ok(group_ids) => {
            let groups: Vec<ConsumerGroupInfo> = group_ids
                .into_iter()
                .map(|group_id| {
                    // Try to get group details for each group
                    match state.coordinator.get_group(&group_id) {
                        Ok(Some(group)) => ConsumerGroupInfo {
                            group_id: group.group_id.clone(),
                            state: format!("{:?}", group.state),
                            member_count: group.members.len(),
                            coordinator: Some(1), // Single-node, node 1 is coordinator
                            protocol_type: group.protocol_type.clone(),
                        },
                        _ => ConsumerGroupInfo {
                            group_id,
                            state: "Unknown".to_string(),
                            member_count: 0,
                            coordinator: Some(1),
                            protocol_type: "consumer".to_string(),
                        },
                    }
                })
                .collect();
            (StatusCode::OK, Json(groups)).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to list consumer groups");
            let error = ErrorResponse::new("INTERNAL_ERROR", e.to_string());
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
        }
    }
}

/// Get consumer group details
async fn get_consumer_group(
    State(state): State<ConsumerApiState>,
    Path(group_id): Path<String>,
) -> Response {
    match state.coordinator.get_group(&group_id) {
        Ok(Some(group)) => {
            // Convert members - group assignments by topic for each member
            let members: Vec<ConsumerGroupMember> = group
                .members
                .values()
                .map(|m| {
                    // Group assignments by topic
                    let mut topic_partitions: HashMap<String, Vec<i32>> = HashMap::new();
                    for (topic, partition) in &m.assignment {
                        topic_partitions
                            .entry(topic.clone())
                            .or_default()
                            .push(*partition);
                    }

                    // Sort partitions within each topic
                    for partitions in topic_partitions.values_mut() {
                        partitions.sort();
                    }

                    let assignments: Vec<MemberAssignment> = topic_partitions
                        .into_iter()
                        .map(|(topic, partitions)| MemberAssignment { topic, partitions })
                        .collect();

                    ConsumerGroupMember {
                        member_id: m.member_id.clone(),
                        client_id: m.client_id.clone(),
                        client_host: m.client_host.clone(),
                        assignments,
                    }
                })
                .collect();

            // Convert offsets to flat list
            let mut offsets: Vec<ConsumerGroupOffset> = group
                .offsets
                .iter()
                .map(|((topic, partition), committed)| ConsumerGroupOffset {
                    topic: topic.clone(),
                    partition: *partition,
                    committed_offset: committed.offset,
                    metadata: if committed.metadata.is_empty() {
                        None
                    } else {
                        Some(committed.metadata.clone())
                    },
                })
                .collect();

            // Sort offsets by topic then partition
            offsets.sort_by(|a, b| {
                a.topic
                    .cmp(&b.topic)
                    .then_with(|| a.partition.cmp(&b.partition))
            });

            let response = ConsumerGroupDetails {
                group_id: group.group_id.clone(),
                state: format!("{:?}", group.state),
                protocol_type: group.protocol_type.clone(),
                protocol: group.protocol_name.clone(),
                coordinator: Some(1), // Single-node, node 1 is coordinator
                members,
                offsets,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Ok(None) => {
            let error = ErrorResponse::new("GROUP_NOT_FOUND", "Consumer group does not exist");
            (StatusCode::NOT_FOUND, Json(error)).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to get consumer group");
            let error = ErrorResponse::new("INTERNAL_ERROR", e.to_string());
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
        }
    }
}

/// Delete a consumer group
async fn delete_consumer_group(
    State(state): State<ConsumerApiState>,
    Path(group_id): Path<String>,
) -> Response {
    match state.coordinator.delete_group(&group_id) {
        Ok(_) => {
            info!(group_id = %group_id, "Consumer group deleted via REST API");
            (StatusCode::NO_CONTENT, "").into_response()
        }
        Err(e) => {
            if e.to_string().contains("non-empty") {
                let error =
                    ErrorResponse::new("GROUP_NOT_EMPTY", "Cannot delete non-empty consumer group");
                (StatusCode::CONFLICT, Json(error)).into_response()
            } else {
                error!(error = %e, "Failed to delete consumer group");
                let error = ErrorResponse::new("INTERNAL_ERROR", e.to_string());
                (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
            }
        }
    }
}

/// Get consumer group lag
async fn get_group_lag(
    State(state): State<ConsumerApiState>,
    Path(group_id): Path<String>,
) -> Response {
    let calculator = LagCalculator::new(state.topic_manager.clone());

    match calculator.calculate_group_lag(&state.coordinator, &group_id) {
        Ok(lag) => {
            debug!(group_id = %group_id, total_lag = lag.total_lag, "Calculated consumer group lag");

            // Calculate statistics
            let stats = LagStats::from_partitions(&lag.partitions);

            // Update metrics
            let partition_data: Vec<_> = lag
                .partitions
                .iter()
                .map(|p| (p.topic.clone(), p.partition, p.lag))
                .collect();
            crate::metrics::update_consumer_group_lag_metrics(
                &group_id,
                &partition_data,
                lag.total_lag,
                lag.members,
            );

            let response = LagResponse { lag, stats };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to calculate consumer group lag");
            let error = ErrorResponse::new("INTERNAL_ERROR", e.to_string());
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
        }
    }
}

/// Get lag for a specific topic in a consumer group
async fn get_topic_lag(
    State(state): State<ConsumerApiState>,
    Path((group_id, topic)): Path<(String, String)>,
) -> Response {
    let calculator = LagCalculator::new(state.topic_manager.clone());

    match calculator.calculate_topic_lag(&state.coordinator, &group_id, &topic) {
        Ok(topic_lag) => {
            debug!(
                group_id = %group_id,
                topic = %topic,
                total_lag = topic_lag.total_lag,
                "Calculated topic lag"
            );

            let stats = LagStats::from_partitions(&topic_lag.partitions);

            let response = TopicLagResponse {
                group_id: group_id.clone(),
                topic_lag,
                stats,
            };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!(error = %e, "Failed to calculate topic lag");
            let error = ErrorResponse::new("INTERNAL_ERROR", e.to_string());
            (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response()
        }
    }
}

/// Get consumer lag heatmap for all groups and topics
async fn get_lag_heatmap(State(state): State<ConsumerApiState>) -> Response {
    let calculator = LagCalculator::new(state.topic_manager.clone());

    // Get all consumer groups
    let group_ids = match state.coordinator.list_groups() {
        Ok(ids) => ids,
        Err(e) => {
            error!(error = %e, "Failed to list groups for heatmap");
            let error = ErrorResponse::new("INTERNAL_ERROR", e.to_string());
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response();
        }
    };

    // Collect all unique topics subscribed by any group
    let mut all_topics: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Calculate lag for each group and collect topic subscriptions
    let mut group_lags: Vec<(String, Vec<crate::consumer::PartitionLag>)> = Vec::new();

    for group_id in &group_ids {
        match calculator.calculate_group_lag(&state.coordinator, group_id) {
            Ok(lag) => {
                // Collect topics from this group's lag info
                for p in &lag.partitions {
                    all_topics.insert(p.topic.clone());
                }
                group_lags.push((group_id.clone(), lag.partitions));
            }
            Err(e) => {
                debug!(group_id = %group_id, error = %e, "Failed to calculate lag for heatmap");
                // Continue with other groups
                group_lags.push((group_id.clone(), vec![]));
            }
        }
    }

    // Convert to sorted vectors for stable ordering
    let mut topics: Vec<String> = all_topics.into_iter().collect();
    topics.sort();

    let groups: Vec<String> = group_ids;

    // Build topic index map for quick lookups
    let topic_index: HashMap<&str, usize> = topics
        .iter()
        .enumerate()
        .map(|(i, t)| (t.as_str(), i))
        .collect();

    // Build cells matrix
    let mut cells: Vec<Vec<Option<HeatmapCell>>> = Vec::with_capacity(groups.len());

    for (_group_id, partitions) in &group_lags {
        // Initialize row with None for all topics
        let mut row: Vec<Option<HeatmapCell>> = vec![None; topics.len()];

        // Group partitions by topic and calculate totals
        let mut topic_totals: HashMap<&str, (i64, usize)> = HashMap::new();
        for p in partitions {
            let entry = topic_totals.entry(p.topic.as_str()).or_insert((0, 0));
            entry.0 += p.lag;
            entry.1 += 1;
        }

        // Fill in cells for topics this group subscribes to
        for (topic, (total_lag, partition_count)) in topic_totals {
            if let Some(&idx) = topic_index.get(topic) {
                row[idx] = Some(HeatmapCell {
                    total_lag,
                    severity: LagSeverity::from_lag(total_lag),
                    partition_count,
                });
            }
        }

        cells.push(row);
    }

    let heatmap = HeatmapData {
        groups,
        topics,
        cells,
    };

    debug!(
        groups = heatmap.groups.len(),
        topics = heatmap.topics.len(),
        "Generated lag heatmap"
    );

    (StatusCode::OK, Json(heatmap)).into_response()
}

/// POST /api/v1/consumer-groups/{group}/reset-offsets - Reset consumer group offsets
async fn reset_offsets(
    State(state): State<ConsumerApiState>,
    Path(group_id): Path<String>,
    Json(request): Json<ResetOffsetsRequest>,
) -> Response {
    execute_offset_reset(state, group_id, request, false).await
}

/// POST /api/v1/consumer-groups/{group}/reset-offsets/dry-run - Preview offset reset
async fn reset_offsets_dry_run(
    State(state): State<ConsumerApiState>,
    Path(group_id): Path<String>,
    Json(request): Json<ResetOffsetsRequest>,
) -> Response {
    execute_offset_reset(state, group_id, request, true).await
}

/// Execute offset reset (or dry-run preview)
async fn execute_offset_reset(
    state: ConsumerApiState,
    group_id: String,
    request: ResetOffsetsRequest,
    dry_run: bool,
) -> Response {
    // Get topic info to know partition count
    let topic_stats = match state.topic_manager.get_topic_stats(&request.topic) {
        Ok(stats) => stats,
        Err(e) => {
            if e.to_string().contains("not found") {
                let error = ErrorResponse::new("TOPIC_NOT_FOUND", "Topic does not exist");
                return (StatusCode::NOT_FOUND, Json(error)).into_response();
            }
            error!(error = %e, "Failed to get topic info");
            let error = ErrorResponse::new("INTERNAL_ERROR", e.to_string());
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response();
        }
    };

    let partition_count = topic_stats.num_partitions;

    // Determine which partitions to reset
    let partitions_to_reset: Vec<i32> = match &request.partitions {
        Some(p) => p.clone(),
        None => (0..partition_count as i32).collect(),
    };

    // Get current committed offsets for the group
    let current_offsets: HashMap<i32, i64> = match state.coordinator.get_group(&group_id) {
        Ok(Some(group)) => group
            .offsets
            .iter()
            .filter(|((topic, _), _)| topic == &request.topic)
            .map(|((_, partition), offset)| (*partition, offset.offset))
            .collect(),
        Ok(None) => HashMap::new(),
        Err(e) => {
            error!(error = %e, "Failed to get group");
            let error = ErrorResponse::new("INTERNAL_ERROR", e.to_string());
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(error)).into_response();
        }
    };

    let mut results: Vec<PartitionOffsetResult> = Vec::new();
    let mut success_count = 0usize;
    let mut error_count = 0usize;

    for partition in partitions_to_reset {
        let previous_offset = current_offsets.get(&partition).copied().unwrap_or(-1);

        // Calculate new offset based on strategy
        let new_offset_result = calculate_new_offset(
            &state.topic_manager,
            &request.topic,
            partition,
            &request.strategy,
            request.timestamp_ms,
            request.offset,
        );

        match new_offset_result {
            Ok(new_offset) => {
                // If not dry run, actually commit the new offset
                if !dry_run {
                    if let Err(e) = state.coordinator.commit_offset(
                        &group_id,
                        &request.topic,
                        partition,
                        new_offset,
                        format!("reset via API: {:?}", request.strategy),
                    ) {
                        error!(error = %e, "Failed to commit offset");
                        results.push(PartitionOffsetResult {
                            partition,
                            previous_offset,
                            new_offset,
                            error: Some(e.to_string()),
                        });
                        error_count += 1;
                        continue;
                    }
                }

                results.push(PartitionOffsetResult {
                    partition,
                    previous_offset,
                    new_offset,
                    error: None,
                });
                success_count += 1;
            }
            Err(e) => {
                results.push(PartitionOffsetResult {
                    partition,
                    previous_offset,
                    new_offset: -1,
                    error: Some(e),
                });
                error_count += 1;
            }
        }
    }

    let strategy_str = match request.strategy {
        ResetStrategy::Earliest => "earliest",
        ResetStrategy::Latest => "latest",
        ResetStrategy::Timestamp => "timestamp",
        ResetStrategy::Offset => "offset",
    };

    if !dry_run {
        info!(
            group_id = %group_id,
            topic = %request.topic,
            strategy = strategy_str,
            success = success_count,
            errors = error_count,
            "Reset consumer group offsets"
        );
    }

    let response = ResetOffsetsResponse {
        group_id,
        topic: request.topic,
        strategy: strategy_str.to_string(),
        dry_run,
        partitions: results,
        success_count,
        error_count,
    };

    (StatusCode::OK, Json(response)).into_response()
}

/// Calculate new offset based on reset strategy
fn calculate_new_offset(
    topic_manager: &Arc<TopicManager>,
    topic: &str,
    partition: i32,
    strategy: &ResetStrategy,
    timestamp_ms: Option<i64>,
    specific_offset: Option<i64>,
) -> Result<i64, String> {
    match strategy {
        ResetStrategy::Earliest => {
            // Get earliest offset for partition
            topic_manager
                .earliest_offset(topic, partition)
                .map_err(|e| format!("Failed to get earliest offset: {}", e))
        }
        ResetStrategy::Latest => {
            // Get latest offset for partition
            topic_manager
                .latest_offset(topic, partition)
                .map_err(|e| format!("Failed to get latest offset: {}", e))
        }
        ResetStrategy::Timestamp => {
            // Timestamp-based reset: find first offset >= timestamp
            let ts = timestamp_ms.ok_or("timestamp_ms is required for timestamp strategy")?;
            match topic_manager.find_offset_by_timestamp(topic, partition, ts) {
                Ok(Some(offset)) => Ok(offset),
                Ok(None) => {
                    // No offset found for timestamp - fall back to latest
                    topic_manager
                        .latest_offset(topic, partition)
                        .map_err(|e| format!("Failed to get latest offset: {}", e))
                }
                Err(e) => Err(format!("Failed to find offset by timestamp: {}", e)),
            }
        }
        ResetStrategy::Offset => {
            // Use specific offset provided
            let offset = specific_offset.ok_or("offset is required for offset strategy")?;
            // Validate offset is within valid range
            let start = topic_manager
                .earliest_offset(topic, partition)
                .map_err(|e| format!("Failed to get earliest offset: {}", e))?;
            let end = topic_manager
                .latest_offset(topic, partition)
                .map_err(|e| format!("Failed to get latest offset: {}", e))?;

            if offset < start {
                Err(format!(
                    "Offset {} is before earliest available offset {}",
                    offset, start
                ))
            } else if offset > end {
                Err(format!(
                    "Offset {} is after latest available offset {}",
                    offset, end
                ))
            } else {
                Ok(offset)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tempfile::TempDir;
    use tower::util::ServiceExt;

    fn create_test_state() -> (ConsumerApiState, TempDir, TempDir) {
        let data_dir = TempDir::new().unwrap();
        let offset_dir = TempDir::new().unwrap();
        let topic_manager = Arc::new(TopicManager::new(data_dir.path()).unwrap());
        let coordinator =
            Arc::new(GroupCoordinator::new(offset_dir.path(), topic_manager.clone()).unwrap());

        let state = ConsumerApiState {
            coordinator,
            topic_manager,
        };
        (state, data_dir, offset_dir)
    }

    #[tokio::test]
    async fn test_list_consumer_groups_empty() {
        let (state, _data_dir, _offset_dir) = create_test_state();
        let app = create_consumer_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/consumer-groups")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_nonexistent_consumer_group() {
        let (state, _data_dir, _offset_dir) = create_test_state();
        let app = create_consumer_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/consumer-groups/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_group_lag_nonexistent() {
        let (state, _data_dir, _offset_dir) = create_test_state();
        let app = create_consumer_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/consumer-groups/nonexistent/lag")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should return 200 with empty lag info for unknown group
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_topic_lag() {
        let (state, _data_dir, _offset_dir) = create_test_state();
        let app = create_consumer_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/consumer-groups/test-group/lag/test-topic")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_list_groups_with_group() {
        let (state, _data_dir, _offset_dir) = create_test_state();

        // Create a group by joining
        let _ = state.coordinator.join_group(
            "test-group",
            None,
            "client1",
            "localhost",
            45000,
            300000,
            "consumer",
            "range",
            vec!["topic1".to_string()],
            vec![],
        );

        let app = create_consumer_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/consumer-groups")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_existing_consumer_group() {
        let (state, _data_dir, _offset_dir) = create_test_state();

        // Create a group by joining
        let _ = state.coordinator.join_group(
            "test-group",
            None,
            "client1",
            "localhost",
            45000,
            300000,
            "consumer",
            "range",
            vec!["topic1".to_string()],
            vec![],
        );

        let app = create_consumer_api_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/consumer-groups/test-group")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
