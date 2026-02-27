//! Agentic AI API - REST endpoints for autonomous streaming agents
//!
//! ## Endpoints
//!
//! - `POST   /api/v1/agents`                        - Create an agent
//! - `GET    /api/v1/agents`                        - List agents
//! - `GET    /api/v1/agents/:id`                    - Get agent details
//! - `DELETE /api/v1/agents/:id`                    - Delete agent
//! - `POST   /api/v1/agents/:id/start`             - Start agent
//! - `POST   /api/v1/agents/:id/stop`              - Stop agent
//! - `GET    /api/v1/agents/:id/actions`            - Get agent's action history
//! - `POST   /api/v1/agents/:id/approve/:action_id` - Approve pending action
//! - `POST   /api/v1/agents/:id/reject/:action_id`  - Reject pending action
//! - `GET    /api/v1/agents/stats`                  - Aggregate stats

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Shared state for the Agent API.
#[derive(Clone)]
pub struct AgentApiState {
    agents: Arc<RwLock<HashMap<String, StreamlineAgent>>>,
}

impl Default for AgentApiState {
    fn default() -> Self {
        Self::new()
    }
}

impl AgentApiState {
    pub fn new() -> Self {
        Self {
            agents: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

/// An autonomous streaming agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamlineAgent {
    pub id: String,
    pub name: String,
    pub agent_type: AgentType,
    pub goal: String,
    pub config: AgentConfig,
    pub status: AgentStatus,
    pub actions: Vec<AgentAction>,
    pub observations: Vec<Observation>,
    pub created_at: String,
    pub last_active_at: Option<String>,
}

/// The kind of agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum AgentType {
    ScalingAgent,
    AnomalyAgent,
    CostOptimizer,
    ComplianceAgent,
    PerformanceAgent,
    Custom(String),
}

/// Agent lifecycle status.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", content = "detail")]
pub enum AgentStatus {
    Idle,
    Observing,
    Deciding,
    Acting,
    WaitingApproval,
    Stopped,
    Error(String),
}

/// Configuration for an agent's behaviour.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfig {
    pub autonomy_level: AutonomyLevel,
    pub observation_interval_secs: u64,
    pub max_actions_per_hour: u32,
    pub require_approval: bool,
    pub notification_channels: Vec<String>,
}

impl Default for AgentConfig {
    fn default() -> Self {
        Self {
            autonomy_level: AutonomyLevel::RecommendOnly,
            observation_interval_secs: 60,
            max_actions_per_hour: 10,
            require_approval: true,
            notification_channels: vec![],
        }
    }
}

/// How autonomous the agent is allowed to be.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AutonomyLevel {
    ObserveOnly,
    RecommendOnly,
    ApproveAndAct,
    FullAutonomy,
}

/// An action taken (or proposed) by an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAction {
    pub id: String,
    pub action_type: String,
    pub description: String,
    pub reason: String,
    pub confidence: f64,
    pub status: ActionApproval,
    pub impact: String,
    pub created_at: String,
    pub resolved_at: Option<String>,
}

/// Approval state for an agent action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActionApproval {
    Pending,
    Approved,
    Rejected,
    AutoApproved,
    Expired,
}

/// A single observation recorded by an agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observation {
    pub metric: String,
    pub value: f64,
    pub threshold: Option<f64>,
    pub anomaly: bool,
    pub timestamp: String,
}

// ---------------------------------------------------------------------------
// Request / Response DTOs
// ---------------------------------------------------------------------------

/// Request body for creating an agent.
#[derive(Debug, Deserialize)]
pub struct CreateAgentRequest {
    pub name: String,
    pub agent_type: AgentType,
    pub goal: String,
    pub config: Option<AgentConfig>,
}

/// Lightweight agent info for list responses.
#[derive(Debug, Serialize)]
pub struct AgentSummary {
    pub id: String,
    pub name: String,
    pub agent_type: AgentType,
    pub status: AgentStatus,
    pub actions_count: usize,
    pub created_at: String,
}

/// Aggregate stats across all agents.
#[derive(Debug, Serialize, Deserialize)]
pub struct AgentStats {
    pub total_agents: usize,
    pub active_agents: usize,
    pub stopped_agents: usize,
    pub total_actions: usize,
    pub pending_approvals: usize,
    pub agents_by_type: HashMap<String, usize>,
}

/// Error payload.
#[derive(Debug, Serialize)]
pub struct AgentErrorResponse {
    pub error: String,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Create the agent API router.
pub fn create_agent_api_router(state: AgentApiState) -> Router {
    Router::new()
        .route("/api/v1/agents", post(create_agent))
        .route("/api/v1/agents", get(list_agents))
        .route("/api/v1/agents/stats", get(get_stats))
        .route("/api/v1/agents/:id", get(get_agent))
        .route("/api/v1/agents/:id", delete(delete_agent))
        .route("/api/v1/agents/:id/start", post(start_agent))
        .route("/api/v1/agents/:id/stop", post(stop_agent))
        .route("/api/v1/agents/:id/actions", get(get_actions))
        .route(
            "/api/v1/agents/:id/approve/:action_id",
            post(approve_action),
        )
        .route(
            "/api/v1/agents/:id/reject/:action_id",
            post(reject_action),
        )
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

async fn create_agent(
    State(state): State<AgentApiState>,
    Json(req): Json<CreateAgentRequest>,
) -> (StatusCode, Json<serde_json::Value>) {
    let id = uuid::Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

    let agent = StreamlineAgent {
        id: id.clone(),
        name: req.name.clone(),
        agent_type: req.agent_type,
        goal: req.goal,
        config: req.config.unwrap_or_default(),
        status: AgentStatus::Idle,
        actions: vec![],
        observations: vec![],
        created_at: now,
        last_active_at: None,
    };

    info!(agent_id = %id, name = %req.name, "Creating agent");
    let mut agents = state.agents.write().await;
    agents.insert(id.clone(), agent.clone());

    (
        StatusCode::CREATED,
        Json(serde_json::to_value(&agent).unwrap()),
    )
}

async fn list_agents(
    State(state): State<AgentApiState>,
) -> Json<Vec<AgentSummary>> {
    let agents = state.agents.read().await;
    let summaries: Vec<AgentSummary> = agents
        .values()
        .map(|a| AgentSummary {
            id: a.id.clone(),
            name: a.name.clone(),
            agent_type: a.agent_type.clone(),
            status: a.status.clone(),
            actions_count: a.actions.len(),
            created_at: a.created_at.clone(),
        })
        .collect();
    Json(summaries)
}

async fn get_agent(
    State(state): State<AgentApiState>,
    Path(id): Path<String>,
) -> Result<Json<StreamlineAgent>, (StatusCode, Json<AgentErrorResponse>)> {
    let agents = state.agents.read().await;
    agents.get(&id).cloned().map(Json).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(AgentErrorResponse {
                error: format!("Agent {id} not found"),
            }),
        )
    })
}

async fn delete_agent(
    State(state): State<AgentApiState>,
    Path(id): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<AgentErrorResponse>)> {
    let mut agents = state.agents.write().await;
    if agents.remove(&id).is_some() {
        info!(agent_id = %id, "Deleted agent");
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err((
            StatusCode::NOT_FOUND,
            Json(AgentErrorResponse {
                error: format!("Agent {id} not found"),
            }),
        ))
    }
}

async fn start_agent(
    State(state): State<AgentApiState>,
    Path(id): Path<String>,
) -> Result<Json<StreamlineAgent>, (StatusCode, Json<AgentErrorResponse>)> {
    let mut agents = state.agents.write().await;
    let agent = agents.get_mut(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(AgentErrorResponse {
                error: format!("Agent {id} not found"),
            }),
        )
    })?;

    match &agent.status {
        AgentStatus::Observing | AgentStatus::Acting | AgentStatus::Deciding => {
            return Err((
                StatusCode::CONFLICT,
                Json(AgentErrorResponse {
                    error: "Agent is already running".to_string(),
                }),
            ));
        }
        _ => {}
    }

    agent.status = AgentStatus::Observing;
    agent.last_active_at = Some(chrono::Utc::now().to_rfc3339());
    info!(agent_id = %id, "Started agent");
    Ok(Json(agent.clone()))
}

async fn stop_agent(
    State(state): State<AgentApiState>,
    Path(id): Path<String>,
) -> Result<Json<StreamlineAgent>, (StatusCode, Json<AgentErrorResponse>)> {
    let mut agents = state.agents.write().await;
    let agent = agents.get_mut(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(AgentErrorResponse {
                error: format!("Agent {id} not found"),
            }),
        )
    })?;

    agent.status = AgentStatus::Stopped;
    agent.last_active_at = Some(chrono::Utc::now().to_rfc3339());
    info!(agent_id = %id, "Stopped agent");
    Ok(Json(agent.clone()))
}

async fn get_actions(
    State(state): State<AgentApiState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<AgentAction>>, (StatusCode, Json<AgentErrorResponse>)> {
    let agents = state.agents.read().await;
    let agent = agents.get(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(AgentErrorResponse {
                error: format!("Agent {id} not found"),
            }),
        )
    })?;
    Ok(Json(agent.actions.clone()))
}

async fn approve_action(
    State(state): State<AgentApiState>,
    Path((id, action_id)): Path<(String, String)>,
) -> Result<Json<AgentAction>, (StatusCode, Json<AgentErrorResponse>)> {
    let mut agents = state.agents.write().await;
    let agent = agents.get_mut(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(AgentErrorResponse {
                error: format!("Agent {id} not found"),
            }),
        )
    })?;

    let action = agent
        .actions
        .iter_mut()
        .find(|a| a.id == action_id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(AgentErrorResponse {
                    error: format!("Action {action_id} not found"),
                }),
            )
        })?;

    if action.status != ActionApproval::Pending {
        return Err((
            StatusCode::CONFLICT,
            Json(AgentErrorResponse {
                error: format!("Action is not pending (current: {:?})", action.status),
            }),
        ));
    }

    action.status = ActionApproval::Approved;
    action.resolved_at = Some(chrono::Utc::now().to_rfc3339());
    info!(agent_id = %id, action_id = %action_id, "Approved action");
    Ok(Json(action.clone()))
}

async fn reject_action(
    State(state): State<AgentApiState>,
    Path((id, action_id)): Path<(String, String)>,
) -> Result<Json<AgentAction>, (StatusCode, Json<AgentErrorResponse>)> {
    let mut agents = state.agents.write().await;
    let agent = agents.get_mut(&id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(AgentErrorResponse {
                error: format!("Agent {id} not found"),
            }),
        )
    })?;

    let action = agent
        .actions
        .iter_mut()
        .find(|a| a.id == action_id)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(AgentErrorResponse {
                    error: format!("Action {action_id} not found"),
                }),
            )
        })?;

    if action.status != ActionApproval::Pending {
        return Err((
            StatusCode::CONFLICT,
            Json(AgentErrorResponse {
                error: format!("Action is not pending (current: {:?})", action.status),
            }),
        ));
    }

    action.status = ActionApproval::Rejected;
    action.resolved_at = Some(chrono::Utc::now().to_rfc3339());
    warn!(agent_id = %id, action_id = %action_id, "Rejected action");
    Ok(Json(action.clone()))
}

async fn get_stats(
    State(state): State<AgentApiState>,
) -> Json<AgentStats> {
    let agents = state.agents.read().await;

    let mut agents_by_type: HashMap<String, usize> = HashMap::new();
    let mut active = 0usize;
    let mut stopped = 0usize;
    let mut total_actions = 0usize;
    let mut pending_approvals = 0usize;

    for agent in agents.values() {
        let type_key = match &agent.agent_type {
            AgentType::ScalingAgent => "ScalingAgent".to_string(),
            AgentType::AnomalyAgent => "AnomalyAgent".to_string(),
            AgentType::CostOptimizer => "CostOptimizer".to_string(),
            AgentType::ComplianceAgent => "ComplianceAgent".to_string(),
            AgentType::PerformanceAgent => "PerformanceAgent".to_string(),
            AgentType::Custom(name) => format!("Custom({name})"),
        };
        *agents_by_type.entry(type_key).or_default() += 1;

        match &agent.status {
            AgentStatus::Observing | AgentStatus::Acting | AgentStatus::Deciding => {
                active += 1;
            }
            AgentStatus::Stopped => stopped += 1,
            _ => {}
        }

        total_actions += agent.actions.len();
        pending_approvals += agent
            .actions
            .iter()
            .filter(|a| a.status == ActionApproval::Pending)
            .count();
    }

    Json(AgentStats {
        total_agents: agents.len(),
        active_agents: active,
        stopped_agents: stopped,
        total_actions,
        pending_approvals,
        agents_by_type,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request};
    use tower::ServiceExt;

    fn test_state() -> AgentApiState {
        AgentApiState::new()
    }

    fn test_router() -> (AgentApiState, Router) {
        let state = test_state();
        let router = create_agent_api_router(state.clone());
        (state, router)
    }

    fn create_request_body() -> String {
        serde_json::json!({
            "name": "test-agent",
            "agent_type": { "type": "ScalingAgent" },
            "goal": "Scale partitions based on lag"
        })
        .to_string()
    }

    async fn do_request(
        router: &Router,
        method: Method,
        uri: &str,
        body: Option<String>,
    ) -> (StatusCode, serde_json::Value) {
        let mut builder = Request::builder().method(method).uri(uri);
        let b = match body {
            Some(s) => {
                builder = builder.header("content-type", "application/json");
                Body::from(s)
            }
            None => Body::empty(),
        };
        let req = builder.body(b).unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let json = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
        };
        (status, json)
    }

    async fn create_agent_via_api(router: &Router) -> serde_json::Value {
        let (_, json) = do_request(
            router,
            Method::POST,
            "/api/v1/agents",
            Some(create_request_body()),
        )
        .await;
        json
    }

    #[tokio::test]
    async fn test_create_agent() {
        let (_, router) = test_router();
        let (status, _) = do_request(
            &router,
            Method::POST,
            "/api/v1/agents",
            Some(create_request_body()),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_create_agent_response_fields() {
        let (_, router) = test_router();
        let agent = create_agent_via_api(&router).await;
        assert_eq!(agent["name"], "test-agent");
        assert!(agent["id"].as_str().is_some());
        assert_eq!(agent["status"]["state"], "Idle");
    }

    #[tokio::test]
    async fn test_list_agents_empty() {
        let (_, router) = test_router();
        let (status, json) = do_request(&router, Method::GET, "/api/v1/agents", None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_list_agents_after_create() {
        let (_, router) = test_router();
        create_agent_via_api(&router).await;
        let (_, json) = do_request(&router, Method::GET, "/api/v1/agents", None).await;
        assert_eq!(json.as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn test_get_agent_not_found() {
        let (_, router) = test_router();
        let (status, _) =
            do_request(&router, Method::GET, "/api/v1/agents/nonexistent", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_agent_found() {
        let (_, router) = test_router();
        let agent = create_agent_via_api(&router).await;
        let id = agent["id"].as_str().unwrap();
        let (status, _) =
            do_request(&router, Method::GET, &format!("/api/v1/agents/{id}"), None).await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_agent() {
        let (_, router) = test_router();
        let agent = create_agent_via_api(&router).await;
        let id = agent["id"].as_str().unwrap();

        let (status, _) =
            do_request(&router, Method::DELETE, &format!("/api/v1/agents/{id}"), None).await;
        assert_eq!(status, StatusCode::NO_CONTENT);

        let (status, _) =
            do_request(&router, Method::GET, &format!("/api/v1/agents/{id}"), None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_delete_agent_not_found() {
        let (_, router) = test_router();
        let (status, _) =
            do_request(&router, Method::DELETE, "/api/v1/agents/nonexistent", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_start_agent() {
        let (_, router) = test_router();
        let agent = create_agent_via_api(&router).await;
        let id = agent["id"].as_str().unwrap();

        let (status, body) = do_request(
            &router,
            Method::POST,
            &format!("/api/v1/agents/{id}/start"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"]["state"], "Observing");
    }

    #[tokio::test]
    async fn test_start_already_running_agent() {
        let (_, router) = test_router();
        let agent = create_agent_via_api(&router).await;
        let id = agent["id"].as_str().unwrap();

        do_request(
            &router,
            Method::POST,
            &format!("/api/v1/agents/{id}/start"),
            None,
        )
        .await;

        let (status, _) = do_request(
            &router,
            Method::POST,
            &format!("/api/v1/agents/{id}/start"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_stop_agent() {
        let (_, router) = test_router();
        let agent = create_agent_via_api(&router).await;
        let id = agent["id"].as_str().unwrap();

        do_request(
            &router,
            Method::POST,
            &format!("/api/v1/agents/{id}/start"),
            None,
        )
        .await;

        let (status, body) = do_request(
            &router,
            Method::POST,
            &format!("/api/v1/agents/{id}/stop"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"]["state"], "Stopped");
    }

    #[tokio::test]
    async fn test_get_actions_empty() {
        let (_, router) = test_router();
        let agent = create_agent_via_api(&router).await;
        let id = agent["id"].as_str().unwrap();

        let (status, json) = do_request(
            &router,
            Method::GET,
            &format!("/api/v1/agents/{id}/actions"),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(json.as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_approve_action() {
        let (state, router) = test_router();

        let action = AgentAction {
            id: "act-1".to_string(),
            action_type: "scale_up".to_string(),
            description: "Add 2 partitions".to_string(),
            reason: "Consumer lag > 10k".to_string(),
            confidence: 0.92,
            status: ActionApproval::Pending,
            impact: "Increased throughput".to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            resolved_at: None,
        };
        let agent = StreamlineAgent {
            id: "agent-1".to_string(),
            name: "scaler".to_string(),
            agent_type: AgentType::ScalingAgent,
            goal: "Scale partitions".to_string(),
            config: AgentConfig::default(),
            status: AgentStatus::WaitingApproval,
            actions: vec![action],
            observations: vec![],
            created_at: chrono::Utc::now().to_rfc3339(),
            last_active_at: None,
        };
        state
            .agents
            .write()
            .await
            .insert("agent-1".to_string(), agent);

        let (status, body) = do_request(
            &router,
            Method::POST,
            "/api/v1/agents/agent-1/approve/act-1",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "Approved");
    }

    #[tokio::test]
    async fn test_reject_action() {
        let (state, router) = test_router();

        let action = AgentAction {
            id: "act-2".to_string(),
            action_type: "delete_topic".to_string(),
            description: "Delete stale topic".to_string(),
            reason: "No consumers for 30 days".to_string(),
            confidence: 0.7,
            status: ActionApproval::Pending,
            impact: "Data loss".to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            resolved_at: None,
        };
        let agent = StreamlineAgent {
            id: "agent-2".to_string(),
            name: "cleaner".to_string(),
            agent_type: AgentType::CostOptimizer,
            goal: "Reduce costs".to_string(),
            config: AgentConfig::default(),
            status: AgentStatus::WaitingApproval,
            actions: vec![action],
            observations: vec![],
            created_at: chrono::Utc::now().to_rfc3339(),
            last_active_at: None,
        };
        state
            .agents
            .write()
            .await
            .insert("agent-2".to_string(), agent);

        let (status, body) = do_request(
            &router,
            Method::POST,
            "/api/v1/agents/agent-2/reject/act-2",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "Rejected");
    }

    #[tokio::test]
    async fn test_approve_non_pending_action_conflict() {
        let (state, router) = test_router();

        let action = AgentAction {
            id: "act-3".to_string(),
            action_type: "scale_up".to_string(),
            description: "Already approved".to_string(),
            reason: "test".to_string(),
            confidence: 0.9,
            status: ActionApproval::Approved,
            impact: "none".to_string(),
            created_at: chrono::Utc::now().to_rfc3339(),
            resolved_at: Some(chrono::Utc::now().to_rfc3339()),
        };
        let agent = StreamlineAgent {
            id: "agent-3".to_string(),
            name: "test".to_string(),
            agent_type: AgentType::AnomalyAgent,
            goal: "test".to_string(),
            config: AgentConfig::default(),
            status: AgentStatus::Observing,
            actions: vec![action],
            observations: vec![],
            created_at: chrono::Utc::now().to_rfc3339(),
            last_active_at: None,
        };
        state
            .agents
            .write()
            .await
            .insert("agent-3".to_string(), agent);

        let (status, _) = do_request(
            &router,
            Method::POST,
            "/api/v1/agents/agent-3/approve/act-3",
            None,
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn test_get_stats_empty() {
        let (_, router) = test_router();
        let (status, json) =
            do_request(&router, Method::GET, "/api/v1/agents/stats", None).await;
        assert_eq!(status, StatusCode::OK);
        let stats: AgentStats = serde_json::from_value(json).unwrap();
        assert_eq!(stats.total_agents, 0);
    }

    #[tokio::test]
    async fn test_get_stats_with_agents() {
        let (_, router) = test_router();
        create_agent_via_api(&router).await;
        create_agent_via_api(&router).await;

        let (_, json) =
            do_request(&router, Method::GET, "/api/v1/agents/stats", None).await;
        let stats: AgentStats = serde_json::from_value(json).unwrap();
        assert_eq!(stats.total_agents, 2);
    }

    #[tokio::test]
    async fn test_custom_agent_type() {
        let (_, router) = test_router();
        let body = serde_json::json!({
            "name": "custom-agent",
            "agent_type": { "type": "Custom", "value": "MyCustomAgent" },
            "goal": "Custom goal"
        })
        .to_string();

        let (status, _) = do_request(
            &router,
            Method::POST,
            "/api/v1/agents",
            Some(body),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    #[test]
    fn test_agent_config_default() {
        let config = AgentConfig::default();
        assert_eq!(config.autonomy_level, AutonomyLevel::RecommendOnly);
        assert_eq!(config.observation_interval_secs, 60);
        assert_eq!(config.max_actions_per_hour, 10);
        assert!(config.require_approval);
        assert!(config.notification_channels.is_empty());
    }

    #[test]
    fn test_agent_status_serde_roundtrip() {
        let statuses = vec![
            AgentStatus::Idle,
            AgentStatus::Observing,
            AgentStatus::Error("test error".to_string()),
        ];
        for status in statuses {
            let json = serde_json::to_string(&status).unwrap();
            let parsed: AgentStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, status);
        }
    }

    #[test]
    fn test_action_approval_serde_roundtrip() {
        let variants = vec![
            ActionApproval::Pending,
            ActionApproval::Approved,
            ActionApproval::Rejected,
            ActionApproval::AutoApproved,
            ActionApproval::Expired,
        ];
        for v in variants {
            let json = serde_json::to_string(&v).unwrap();
            let parsed: ActionApproval = serde_json::from_str(&json).unwrap();
            assert_eq!(parsed, v);
        }
    }
}
