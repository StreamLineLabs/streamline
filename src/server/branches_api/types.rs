//! Request/response types and error helpers for the branches API.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use crate::branches::{BranchMeta, BranchStoreError};

/// `POST /api/v1/branches` request body.
#[derive(Debug, Deserialize)]
pub struct CreateBranchRequest {
    pub base_topic: String,
    pub name: String,
    pub base_offsets: Vec<i64>,
    pub created_by: String,
}

/// Wire representation of a branch returned by list/get endpoints.
#[derive(Debug, Serialize, Deserialize)]
pub struct BranchView {
    pub id: String,
    pub base_topic: String,
    pub base_offsets: Vec<i64>,
    pub created_by: String,
    pub created_at_ms: u64,
    pub state: String,
    pub write_topic: String,
}

impl From<BranchMeta> for BranchView {
    fn from(m: BranchMeta) -> Self {
        let write_topic = m.write_topic();
        Self {
            id: m.id.0.clone(),
            base_topic: m.base_topic,
            base_offsets: m.base_offsets,
            created_by: m.created_by,
            created_at_ms: m.created_at_ms,
            state: format!("{:?}", m.state),
            write_topic,
        }
    }
}

/// Structured error response for the branches API.
#[derive(Debug, Serialize)]
pub struct ApiError {
    pub error: String,
    pub message: String,
}

impl ApiError {
    pub(crate) fn new(error: &str, message: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            message: message.into(),
        }
    }
}

pub(crate) fn map_store_err(e: BranchStoreError) -> Response {
    let (status, code) = match &e {
        BranchStoreError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
        BranchStoreError::AlreadyExists(_) => (StatusCode::CONFLICT, "already_exists"),
        BranchStoreError::NotActive(_) => (StatusCode::CONFLICT, "not_active"),
    };
    (status, Json(ApiError::new(code, e.to_string()))).into_response()
}

/// `POST /api/v1/branches/:id/messages` request body.
#[derive(Debug, Deserialize)]
pub struct AppendBranchRequest {
    pub partition: i32,
    /// Raw bytes encoded as UTF-8 string. Use a base64 wrapper if you need
    /// arbitrary bytes; this is intentionally simple for the wiring layer.
    pub value: String,
}

/// `POST /api/v1/branches/:id/messages` response body.
#[derive(Debug, Serialize)]
pub struct AppendBranchResponse {
    pub partition: i32,
    pub offset: i64,
}

/// `GET /api/v1/branches/:id/messages` query parameters.
#[derive(Debug, Deserialize)]
pub struct ReadBranchQuery {
    pub partition: i32,
    /// Last logical offset already consumed. Pass `-1` to read the first
    /// available record (base or branch).
    #[serde(default = "default_after")]
    pub after: i64,
}

fn default_after() -> i64 {
    -1
}

/// `GET /api/v1/branches/:id/messages` response body.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReadBranchResponse {
    pub from_base: bool,
    pub partition: i32,
    pub offset: i64,
    /// Present only for branch reads (base reads resolve via the log layer).
    pub value: Option<String>,
}

/// `POST /api/v1/branches/:id/run` request body.
#[derive(Debug, Deserialize)]
pub struct RunBranchRequest {
    pub transform: String,
    #[serde(default)]
    pub data: Option<String>,
}

/// `POST /api/v1/branches/:id/run` response body.
#[derive(Debug, Serialize)]
pub struct RunBranchResponse {
    pub records_read: u64,
    pub records_written: u64,
    pub records_skipped: u64,
    pub errors: u64,
    pub completed: bool,
}

/// `POST /api/v1/branches/:id/diff` response body.
#[derive(Debug, Serialize)]
pub struct DiffBranchResponse {
    pub base_count: u64,
    pub branch_count: u64,
    pub records_added: u64,
    pub records_removed: u64,
    pub records_modified: u64,
}

/// `POST /api/v1/branches/:id/merge` request body.
#[derive(Debug, Deserialize)]
pub struct MergeBranchRequest {
    #[serde(default)]
    pub confirm: bool,
    pub target_topic: String,
}

/// `POST /api/v1/branches/:id/merge` response body.
#[derive(Debug, Serialize)]
pub struct MergeBranchResponse {
    pub records_merged: u64,
    pub branch_discarded: bool,
}
