//! GitOps API - REST endpoints for declarative configuration management
//!
//! ## Endpoints
//!
//! - `POST /api/v1/gitops/apply` - Apply a configuration manifest
//! - `POST /api/v1/gitops/validate` - Validate a manifest without applying
//! - `POST /api/v1/gitops/diff` - Show diff between current state and manifest
//! - `GET /api/v1/gitops/export` - Export current configuration as manifest
//! - `GET /api/v1/gitops/manifests` - List loaded manifests
//! - `GET /api/v1/gitops/status` - Get reconciliation status

use crate::gitops::{ConfigManifest, GitOpsConfig, GitOpsManager};
use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Shared state for GitOps API
#[derive(Clone)]
pub struct GitOpsApiState {
    pub manager: Arc<GitOpsManager>,
    pub last_reconcile: Arc<RwLock<Option<ReconcileStatusSnapshot>>>,
}

impl GitOpsApiState {
    pub fn new() -> Self {
        Self {
            manager: Arc::new(GitOpsManager::new(GitOpsConfig::default())),
            last_reconcile: Arc::new(RwLock::new(None)),
        }
    }
}

impl Default for GitOpsApiState {
    fn default() -> Self {
        Self::new()
    }
}

/// Apply request - accepts YAML or JSON manifest
#[derive(Debug, Deserialize)]
pub struct ApplyRequest {
    pub manifest: String,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub force: bool,
}

/// Apply response
#[derive(Debug, Serialize)]
pub struct ApplyResponse {
    pub status: String,
    pub resources_applied: usize,
    pub changes: Vec<ChangeDetail>,
    pub dry_run: bool,
}

/// Change detail in apply result
#[derive(Debug, Serialize)]
pub struct ChangeDetail {
    pub kind: String,
    pub name: String,
    pub action: String,
}

/// Validate response
#[derive(Debug, Serialize)]
pub struct ValidateResponse {
    pub valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub resource_count: usize,
}

/// Diff response
#[derive(Debug, Serialize)]
pub struct DiffResponse {
    pub has_changes: bool,
    pub drifts: Vec<DriftDetail>,
}

/// Drift detail
#[derive(Debug, Serialize)]
pub struct DriftDetail {
    pub resource_kind: String,
    pub resource_name: String,
    pub drift_type: String,
    pub current_value: Option<String>,
    pub desired_value: Option<String>,
}

/// Export response
#[derive(Debug, Serialize)]
pub struct ExportResponse {
    pub manifest: String,
    pub format: String,
    pub resource_count: usize,
}

/// Reconcile status snapshot
#[derive(Debug, Clone, Serialize)]
pub struct ReconcileStatusSnapshot {
    pub last_run: String,
    pub status: String,
    pub resources_synced: usize,
    pub resources_drifted: usize,
    pub errors: Vec<String>,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct GitOpsErrorResponse {
    pub error: String,
}

/// Create the GitOps API router
pub fn create_gitops_api_router(state: GitOpsApiState) -> Router {
    Router::new()
        .route("/api/v1/gitops/apply", post(apply_manifest))
        .route("/api/v1/gitops/validate", post(validate_manifest))
        .route("/api/v1/gitops/diff", post(diff_manifest))
        .route("/api/v1/gitops/export", get(export_config))
        .route("/api/v1/gitops/manifests", get(list_manifests))
        .route("/api/v1/gitops/status", get(reconcile_status))
        .with_state(state)
}

async fn apply_manifest(
    State(state): State<GitOpsApiState>,
    Json(req): Json<ApplyRequest>,
) -> Result<Json<ApplyResponse>, (StatusCode, Json<GitOpsErrorResponse>)> {
    // Parse the manifest
    let manifest: ConfigManifest = serde_yaml::from_str(&req.manifest)
        .or_else(|_| serde_json::from_str(&req.manifest))
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(GitOpsErrorResponse {
                    error: format!("Invalid manifest: {}", e),
                }),
            )
        })?;

    let mut changes = Vec::new();

    // Count resources
    if let Some(ref spec) = manifest.spec {
        for topic in &spec.topics {
            changes.push(ChangeDetail {
                kind: "Topic".to_string(),
                name: topic.name.clone(),
                action: if req.dry_run {
                    "would-apply".to_string()
                } else {
                    "applied".to_string()
                },
            });
        }
        for acl in &spec.acls {
            changes.push(ChangeDetail {
                kind: "ACL".to_string(),
                name: format!("{}:{}", acl.resource_type, acl.resource_name),
                action: if req.dry_run {
                    "would-apply".to_string()
                } else {
                    "applied".to_string()
                },
            });
        }
    }

    let count = changes.len();

    if !req.dry_run {
        state.manager.load_yaml(&req.manifest).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(GitOpsErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;
    }

    Ok(Json(ApplyResponse {
        status: if req.dry_run {
            "dry-run".to_string()
        } else {
            "applied".to_string()
        },
        resources_applied: count,
        changes,
        dry_run: req.dry_run,
    }))
}

async fn validate_manifest(
    State(_state): State<GitOpsApiState>,
    Json(req): Json<ApplyRequest>,
) -> Json<ValidateResponse> {
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut resource_count = 0;

    match serde_yaml::from_str::<ConfigManifest>(&req.manifest)
        .or_else(|_| serde_json::from_str::<ConfigManifest>(&req.manifest))
    {
        Ok(manifest) => {
            if manifest.api_version.is_empty() {
                warnings.push("Missing apiVersion field".to_string());
            }
            if let Some(ref spec) = manifest.spec {
                resource_count = spec.topics.len() + spec.acls.len();
                for topic in &spec.topics {
                    if topic.name.is_empty() {
                        errors.push("Topic name cannot be empty".to_string());
                    }
                    if topic.partitions == 0 {
                        errors.push(format!("Topic '{}': partitions must be > 0", topic.name));
                    }
                }
            } else {
                warnings.push("Manifest has no spec section".to_string());
            }
        }
        Err(e) => {
            errors.push(format!("Parse error: {}", e));
        }
    }

    Json(ValidateResponse {
        valid: errors.is_empty(),
        errors,
        warnings,
        resource_count,
    })
}

async fn diff_manifest(
    State(state): State<GitOpsApiState>,
    Json(req): Json<ApplyRequest>,
) -> Result<Json<DiffResponse>, (StatusCode, Json<GitOpsErrorResponse>)> {
    let manifest: ConfigManifest = serde_yaml::from_str(&req.manifest)
        .or_else(|_| serde_json::from_str(&req.manifest))
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(GitOpsErrorResponse {
                    error: format!("Invalid manifest: {}", e),
                }),
            )
        })?;

    // Detect drift by comparing with loaded manifests
    let drifts = state.manager.detect_drift().await.unwrap_or_default();

    let drift_details: Vec<DriftDetail> = drifts
        .iter()
        .map(|d| DriftDetail {
            resource_kind: d.resource_type.clone(),
            resource_name: d.resource_name.clone(),
            drift_type: format!("{:?}", d.drift_type),
            current_value: d.actual.clone(),
            desired_value: d.expected.clone(),
        })
        .collect();

    let has_changes = !drift_details.is_empty()
        || manifest
            .spec
            .as_ref()
            .is_some_and(|s| !s.topics.is_empty() || !s.acls.is_empty());

    Ok(Json(DiffResponse {
        has_changes,
        drifts: drift_details,
    }))
}

async fn export_config(
    State(state): State<GitOpsApiState>,
) -> Result<Json<ExportResponse>, (StatusCode, Json<GitOpsErrorResponse>)> {
    match state.manager.export_yaml().await {
        Ok(manifest_yaml) => {
            let count = serde_yaml::from_str::<ConfigManifest>(&manifest_yaml)
                .map(|m| m.spec.map(|s| s.topics.len() + s.acls.len()).unwrap_or(0))
                .unwrap_or(0);

            Ok(Json(ExportResponse {
                manifest: manifest_yaml,
                format: "yaml".to_string(),
                resource_count: count,
            }))
        }
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(GitOpsErrorResponse {
                error: e.to_string(),
            }),
        )),
    }
}

async fn list_manifests(State(state): State<GitOpsApiState>) -> Json<serde_json::Value> {
    let manifests = state.manager.list_manifests().await;
    Json(serde_json::json!({
        "manifests": manifests,
        "total": manifests.len(),
    }))
}

async fn reconcile_status(State(state): State<GitOpsApiState>) -> Json<serde_json::Value> {
    let last = state.last_reconcile.read().await;
    match last.as_ref() {
        Some(snapshot) => Json(serde_json::json!(snapshot)),
        None => Json(serde_json::json!({
            "status": "idle",
            "message": "No reconciliation has been performed yet",
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gitops_api_state_new() {
        let state = GitOpsApiState::new();
        assert!(Arc::strong_count(&state.manager) >= 1);
    }

    #[test]
    fn test_validate_response_serialization() {
        let resp = ValidateResponse {
            valid: true,
            errors: vec![],
            warnings: vec!["test warning".to_string()],
            resource_count: 5,
        };
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"valid\":true"));
    }
}
