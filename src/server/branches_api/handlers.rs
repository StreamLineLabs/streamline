//! Handler functions for the branches API.

use std::sync::{Arc, OnceLock};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::branches::{BranchId, BranchMeta, BranchStore, CowReader};
use crate::branches::diff::{DiffConfig, DiffError};
use crate::branches::merge::{MergeConfig, MergeError};
use crate::branches::runner::{RunConfig, RunError, TransformKind};

use super::types::*;

/// Shared state for the branches API, wrapping an `Arc<BranchStore>`.
#[derive(Clone)]
pub struct BranchesApiState {
    pub store: Arc<BranchStore>,
}

impl BranchesApiState {
    /// Returns a process-global shared instance backed by a single `BranchStore`.
    pub fn shared() -> Self {
        static STORE: OnceLock<Arc<BranchStore>> = OnceLock::new();
        Self {
            store: STORE
                .get_or_init(|| Arc::new(BranchStore::new()))
                .clone(),
        }
    }
}

pub(crate) async fn create_branch(
    State(state): State<BranchesApiState>,
    Json(req): Json<CreateBranchRequest>,
) -> Response {
    if req.base_topic.is_empty() || req.name.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiError::new("invalid_argument", "base_topic and name are required")),
        )
            .into_response();
    }
    let meta = BranchMeta::new(&req.base_topic, &req.name, req.base_offsets, &req.created_by);
    match state.store.create(meta.clone()) {
        Ok(()) => (StatusCode::CREATED, Json(BranchView::from(meta))).into_response(),
        Err(e) => map_store_err(e),
    }
}

pub(crate) async fn list_branches(State(state): State<BranchesApiState>) -> Response {
    let metas = state.store.list();
    let views: Vec<BranchView> = metas.into_iter().map(BranchView::from).collect();
    (StatusCode::OK, Json(views)).into_response()
}

pub(crate) async fn get_branch(
    State(state): State<BranchesApiState>,
    Path(id): Path<String>,
) -> Response {
    match state.store.get(&BranchId(id.clone())) {
        Some(m) => (StatusCode::OK, Json(BranchView::from(m))).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(ApiError::new("not_found", format!("branch {} not found", id))),
        )
            .into_response(),
    }
}

pub(crate) async fn discard_branch(
    State(state): State<BranchesApiState>,
    Path(id): Path<String>,
) -> Response {
    match state.store.discard(&BranchId(id)) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => map_store_err(e),
    }
}

pub(crate) async fn append_branch(
    State(state): State<BranchesApiState>,
    Path(id): Path<String>,
    Json(req): Json<AppendBranchRequest>,
) -> Response {
    let bid = BranchId(id);
    match state.store.append(&bid, req.partition, req.value.into_bytes()) {
        Ok(offset) => (
            StatusCode::CREATED,
            Json(AppendBranchResponse {
                partition: req.partition,
                offset,
            }),
        )
            .into_response(),
        Err(e) => map_store_err(e),
    }
}

pub(crate) async fn read_branch(
    State(state): State<BranchesApiState>,
    Path(id): Path<String>,
    Query(q): Query<ReadBranchQuery>,
) -> Response {
    let bid = BranchId(id);
    let meta = match state.store.get(&bid) {
        Some(m) => m,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ApiError::new("not_found", format!("branch {} not found", bid.0))),
            )
                .into_response();
        }
    };
    let reader = CowReader::with_store(&meta, &state.store);
    match reader.next_after(q.partition, q.after) {
        Some(r) => (
            StatusCode::OK,
            Json(ReadBranchResponse {
                from_base: r.from_base,
                partition: r.partition,
                offset: r.offset,
                value: r.value.map(|v| String::from_utf8_lossy(&v).into_owned()),
            }),
        )
            .into_response(),
        None => StatusCode::NO_CONTENT.into_response(),
    }
}

pub(crate) async fn run_handler(
    State(state): State<BranchesApiState>,
    Path(id): Path<String>,
    Json(req): Json<RunBranchRequest>,
) -> Response {
    let transform = match req.transform.as_str() {
        "identity" => TransformKind::Identity,
        "wasm" => {
            let bytes = req.data.unwrap_or_default().into_bytes();
            TransformKind::Wasm(bytes)
        }
        "sql" => {
            let expr = req.data.unwrap_or_default();
            TransformKind::Sql(expr)
        }
        other => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiError::new(
                    "invalid_transform",
                    format!("unknown transform: {}. Must be identity, wasm, or sql", other),
                )),
            )
                .into_response();
        }
    };

    let config = RunConfig::new(id, transform);
    match crate::branches::runner::run_transform(&config, &state.store) {
        Ok(progress) => (
            StatusCode::OK,
            Json(RunBranchResponse {
                records_read: progress.records_read,
                records_written: progress.records_written,
                records_skipped: progress.records_skipped,
                errors: progress.errors,
                completed: progress.completed,
            }),
        )
            .into_response(),
        Err(RunError::Store(e)) => map_store_err(e),
        Err(RunError::TransformNotImplemented(t)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(ApiError::new(
                "transform_not_implemented",
                format!("transform '{}' is not yet implemented", t),
            )),
        )
            .into_response(),
        Err(RunError::NotActive) => (
            StatusCode::CONFLICT,
            Json(ApiError::new("not_active", "branch is not active")),
        )
            .into_response(),
    }
}

pub(crate) async fn diff_handler(
    State(state): State<BranchesApiState>,
    Path(id): Path<String>,
) -> Response {
    let bid = BranchId(id.clone());
    let meta = match state.store.get(&bid) {
        Some(m) => m,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ApiError::new("not_found", format!("branch {} not found", id))),
            )
                .into_response();
        }
    };

    let config = DiffConfig {
        base_topic: meta.base_topic.clone(),
        branch_name: bid
            .parts()
            .map(|(_, name)| name.to_string())
            .unwrap_or_else(|| id.clone()),
        metric: None,
    };

    match crate::branches::diff::diff_branches(&config, &state.store) {
        Ok(result) => (
            StatusCode::OK,
            Json(DiffBranchResponse {
                base_count: result.base_count,
                branch_count: result.branch_count,
                records_added: result.records_added,
                records_removed: result.records_removed,
                records_modified: result.records_modified,
            }),
        )
            .into_response(),
        Err(DiffError::Store(e)) => map_store_err(e),
        Err(DiffError::NotActive) => (
            StatusCode::CONFLICT,
            Json(ApiError::new("not_active", "branch is not active")),
        )
            .into_response(),
    }
}

pub(crate) async fn merge_handler(
    State(state): State<BranchesApiState>,
    Path(id): Path<String>,
    Json(req): Json<MergeBranchRequest>,
) -> Response {
    if req.target_topic.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiError::new("invalid_argument", "target_topic is required")),
        )
            .into_response();
    }

    let config = MergeConfig {
        branch_name: id,
        target_topic: req.target_topic,
        confirm: req.confirm,
    };

    match crate::branches::merge::merge_branch(&config, &state.store) {
        Ok(result) => (
            StatusCode::OK,
            Json(MergeBranchResponse {
                records_merged: result.records_merged,
                branch_discarded: result.branch_discarded,
            }),
        )
            .into_response(),
        Err(MergeError::Store(e)) => map_store_err(e),
        Err(MergeError::NotConfirmed) => (
            StatusCode::BAD_REQUEST,
            Json(ApiError::new(
                "not_confirmed",
                "merge not confirmed — set confirm=true to execute",
            )),
        )
            .into_response(),
        Err(MergeError::NotActive) => (
            StatusCode::CONFLICT,
            Json(ApiError::new("not_active", "branch is not active")),
        )
            .into_response(),
    }
}
