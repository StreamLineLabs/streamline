//! Notebooks API - Browser-based SQL notebooks for interactive stream analysis
//!
//! ## Endpoints
//!
//! - `POST /api/v1/notebooks` - Create notebook
//! - `GET /api/v1/notebooks` - List notebooks
//! - `GET /api/v1/notebooks/:id` - Get notebook
//! - `PUT /api/v1/notebooks/:id` - Update notebook
//! - `DELETE /api/v1/notebooks/:id` - Delete notebook
//! - `POST /api/v1/notebooks/:id/cells/:idx/execute` - Execute a cell
//! - `POST /api/v1/notebooks/:id/run-all` - Execute all cells
//! - `POST /api/v1/notebooks/:id/export` - Export as markdown/JSON
//! - `POST /api/v1/notebooks/:id/share` - Generate share link
//! - `GET /api/v1/notebooks/shared/:token` - View shared notebook

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post, put},
    Json, Router,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{info, warn};
use uuid::Uuid;

// ============================================================================
// State
// ============================================================================

/// Shared state for the Notebooks API
#[derive(Clone)]
pub struct NotebooksApiState {
    pub notebooks: Arc<RwLock<HashMap<String, Notebook>>>,
    /// share_token -> notebook_id
    pub shared: Arc<RwLock<HashMap<String, String>>>,
}

impl NotebooksApiState {
    pub fn new() -> Self {
        Self {
            notebooks: Arc::new(RwLock::new(HashMap::new())),
            shared: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

// ============================================================================
// Types
// ============================================================================

/// A notebook containing cells for interactive stream analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notebook {
    pub id: String,
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub cells: Vec<Cell>,
    pub variables: HashMap<String, serde_json::Value>,
    pub created_at: String,
    pub updated_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    pub tags: Vec<String>,
}

/// A single cell within a notebook
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cell {
    pub cell_type: CellType,
    pub source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<CellOutput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_count: Option<u32>,
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

/// The type of a notebook cell
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum CellType {
    Sql,
    Markdown,
    Code,
}

/// Output produced by executing a cell
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellOutput {
    pub output_type: OutputType,
    pub data: serde_json::Value,
    pub execution_time_ms: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// The type of output a cell produces
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum OutputType {
    Table,
    Text,
    Error,
    Chart,
}

// ============================================================================
// Request / Response Types
// ============================================================================

/// Request to create a new notebook
#[derive(Debug, Deserialize)]
pub struct CreateNotebookRequest {
    pub title: String,
    pub description: Option<String>,
    pub cells: Option<Vec<Cell>>,
    pub tags: Option<Vec<String>>,
}

/// Request to update an existing notebook
#[derive(Debug, Deserialize)]
pub struct UpdateNotebookRequest {
    pub title: Option<String>,
    pub description: Option<String>,
    pub cells: Option<Vec<Cell>>,
    pub tags: Option<Vec<String>>,
}

/// Response after executing a cell
#[derive(Debug, Serialize)]
pub struct ExecuteCellResponse {
    pub output: CellOutput,
    pub variables: HashMap<String, serde_json::Value>,
}

/// Request to export a notebook
#[derive(Debug, Deserialize)]
pub struct ExportRequest {
    /// "markdown" or "json"
    pub format: String,
}

/// Response after generating a share link
#[derive(Debug, Serialize)]
pub struct ShareResponse {
    pub token: String,
    pub url: String,
}

// ============================================================================
// Router
// ============================================================================

/// Create the Notebooks API router
pub fn create_notebooks_api_router(state: NotebooksApiState) -> Router {
    Router::new()
        .route("/api/v1/notebooks", post(create_notebook))
        .route("/api/v1/notebooks", get(list_notebooks))
        .route("/api/v1/notebooks/shared/:token", get(get_shared_notebook))
        .route("/api/v1/notebooks/:id", get(get_notebook))
        .route("/api/v1/notebooks/:id", put(update_notebook))
        .route("/api/v1/notebooks/:id", delete(delete_notebook))
        .route(
            "/api/v1/notebooks/:id/cells/:idx/execute",
            post(execute_cell),
        )
        .route("/api/v1/notebooks/:id/run-all", post(run_all_cells))
        .route("/api/v1/notebooks/:id/export", post(export_notebook))
        .route("/api/v1/notebooks/:id/share", post(share_notebook))
        .with_state(state)
}

// ============================================================================
// Handlers
// ============================================================================

fn now_iso() -> String {
    chrono::Utc::now().to_rfc3339()
}

async fn create_notebook(
    State(state): State<NotebooksApiState>,
    Json(req): Json<CreateNotebookRequest>,
) -> (StatusCode, Json<Notebook>) {
    let now = now_iso();
    let notebook = Notebook {
        id: Uuid::new_v4().to_string(),
        title: req.title,
        description: req.description,
        cells: req.cells.unwrap_or_default(),
        variables: HashMap::new(),
        created_at: now.clone(),
        updated_at: now,
        author: None,
        tags: req.tags.unwrap_or_default(),
    };
    info!(notebook_id = %notebook.id, "Created notebook");
    state
        .notebooks
        .write()
        .insert(notebook.id.clone(), notebook.clone());
    (StatusCode::CREATED, Json(notebook))
}

async fn list_notebooks(
    State(state): State<NotebooksApiState>,
) -> Json<Vec<Notebook>> {
    let notebooks = state.notebooks.read();
    Json(notebooks.values().cloned().collect())
}

async fn get_notebook(
    State(state): State<NotebooksApiState>,
    Path(id): Path<String>,
) -> Result<Json<Notebook>, StatusCode> {
    state
        .notebooks
        .read()
        .get(&id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn update_notebook(
    State(state): State<NotebooksApiState>,
    Path(id): Path<String>,
    Json(req): Json<UpdateNotebookRequest>,
) -> Result<Json<Notebook>, StatusCode> {
    let mut notebooks = state.notebooks.write();
    let notebook = notebooks.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;

    if let Some(title) = req.title {
        notebook.title = title;
    }
    if let Some(desc) = req.description {
        notebook.description = Some(desc);
    }
    if let Some(cells) = req.cells {
        notebook.cells = cells;
    }
    if let Some(tags) = req.tags {
        notebook.tags = tags;
    }
    notebook.updated_at = now_iso();
    info!(notebook_id = %id, "Updated notebook");
    Ok(Json(notebook.clone()))
}

async fn delete_notebook(
    State(state): State<NotebooksApiState>,
    Path(id): Path<String>,
) -> StatusCode {
    let removed = state.notebooks.write().remove(&id).is_some();
    if removed {
        // Remove any share tokens pointing to this notebook
        state.shared.write().retain(|_, nid| nid != &id);
        info!(notebook_id = %id, "Deleted notebook");
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn execute_cell(
    State(state): State<NotebooksApiState>,
    Path((id, idx)): Path<(String, usize)>,
) -> Result<Json<ExecuteCellResponse>, StatusCode> {
    let mut notebooks = state.notebooks.write();
    let notebook = notebooks.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;
    let cell = notebook.cells.get_mut(idx).ok_or(StatusCode::BAD_REQUEST)?;

    let start = Instant::now();

    let output = match cell.cell_type {
        CellType::Sql => {
            let rows = serde_json::json!([
                {"info": "SQL execution simulated", "query": cell.source}
            ]);
            CellOutput {
                output_type: OutputType::Table,
                data: rows,
                execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
                row_count: Some(1),
                error: None,
            }
        }
        CellType::Markdown => CellOutput {
            output_type: OutputType::Text,
            data: serde_json::Value::String(cell.source.clone()),
            execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
            row_count: None,
            error: None,
        },
        CellType::Code => CellOutput {
            output_type: OutputType::Text,
            data: serde_json::json!({"status": "executed", "source": cell.source}),
            execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
            row_count: None,
            error: None,
        },
    };

    cell.output = Some(output.clone());
    cell.execution_count = Some(cell.execution_count.unwrap_or(0) + 1);
    notebook.updated_at = now_iso();

    Ok(Json(ExecuteCellResponse {
        output,
        variables: notebook.variables.clone(),
    }))
}

async fn run_all_cells(
    State(state): State<NotebooksApiState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<CellOutput>>, StatusCode> {
    let mut notebooks = state.notebooks.write();
    let notebook = notebooks.get_mut(&id).ok_or(StatusCode::NOT_FOUND)?;

    let mut outputs = Vec::new();
    for cell in notebook.cells.iter_mut() {
        let start = Instant::now();
        let output = match cell.cell_type {
            CellType::Sql => CellOutput {
                output_type: OutputType::Table,
                data: serde_json::json!([{"info": "SQL execution simulated", "query": cell.source}]),
                execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
                row_count: Some(1),
                error: None,
            },
            CellType::Markdown => CellOutput {
                output_type: OutputType::Text,
                data: serde_json::Value::String(cell.source.clone()),
                execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
                row_count: None,
                error: None,
            },
            CellType::Code => CellOutput {
                output_type: OutputType::Text,
                data: serde_json::json!({"status": "executed", "source": cell.source}),
                execution_time_ms: start.elapsed().as_secs_f64() * 1000.0,
                row_count: None,
                error: None,
            },
        };
        cell.output = Some(output.clone());
        cell.execution_count = Some(cell.execution_count.unwrap_or(0) + 1);
        outputs.push(output);
    }
    notebook.updated_at = now_iso();
    info!(notebook_id = %id, cells = outputs.len(), "Executed all cells");
    Ok(Json(outputs))
}

async fn export_notebook(
    State(state): State<NotebooksApiState>,
    Path(id): Path<String>,
    Json(req): Json<ExportRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let notebooks = state.notebooks.read();
    let notebook = notebooks.get(&id).ok_or(StatusCode::NOT_FOUND)?;

    match req.format.as_str() {
        "markdown" => {
            let mut md = format!("# {}\n\n", notebook.title);
            if let Some(desc) = &notebook.description {
                md.push_str(&format!("{}\n\n", desc));
            }
            for (i, cell) in notebook.cells.iter().enumerate() {
                match cell.cell_type {
                    CellType::Sql => {
                        md.push_str(&format!("## Cell {} (SQL)\n\n```sql\n{}\n```\n\n", i + 1, cell.source));
                    }
                    CellType::Markdown => {
                        md.push_str(&format!("{}\n\n", cell.source));
                    }
                    CellType::Code => {
                        md.push_str(&format!("## Cell {} (Code)\n\n```\n{}\n```\n\n", i + 1, cell.source));
                    }
                }
            }
            Ok(Json(serde_json::json!({
                "format": "markdown",
                "content": md,
            })))
        }
        "json" => Ok(Json(serde_json::json!({
            "format": "json",
            "content": notebook,
        }))),
        _ => {
            warn!(format = %req.format, "Unsupported export format");
            Err(StatusCode::BAD_REQUEST)
        }
    }
}

async fn share_notebook(
    State(state): State<NotebooksApiState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<ShareResponse>), StatusCode> {
    if !state.notebooks.read().contains_key(&id) {
        return Err(StatusCode::NOT_FOUND);
    }
    let token = Uuid::new_v4().to_string();
    state.shared.write().insert(token.clone(), id.clone());
    info!(notebook_id = %id, token = %token, "Shared notebook");
    Ok((
        StatusCode::CREATED,
        Json(ShareResponse {
            url: format!("/api/v1/notebooks/shared/{}", token),
            token,
        }),
    ))
}

async fn get_shared_notebook(
    State(state): State<NotebooksApiState>,
    Path(token): Path<String>,
) -> Result<Json<Notebook>, StatusCode> {
    let shared = state.shared.read();
    let notebook_id = shared.get(&token).ok_or(StatusCode::NOT_FOUND)?;
    state
        .notebooks
        .read()
        .get(notebook_id)
        .cloned()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn test_state() -> NotebooksApiState {
        NotebooksApiState::new()
    }

    fn test_router() -> Router {
        create_notebooks_api_router(test_state())
    }

    fn sql_cell(src: &str) -> Cell {
        Cell {
            cell_type: CellType::Sql,
            source: src.to_string(),
            output: None,
            execution_count: None,
            metadata: HashMap::new(),
        }
    }

    fn md_cell(src: &str) -> Cell {
        Cell {
            cell_type: CellType::Markdown,
            source: src.to_string(),
            output: None,
            execution_count: None,
            metadata: HashMap::new(),
        }
    }

    async fn create_test_notebook(state: &NotebooksApiState) -> Notebook {
        let notebook = Notebook {
            id: Uuid::new_v4().to_string(),
            title: "Test Notebook".to_string(),
            description: Some("A test notebook".to_string()),
            cells: vec![sql_cell("SELECT 1"), md_cell("# Hello")],
            variables: HashMap::new(),
            created_at: now_iso(),
            updated_at: now_iso(),
            author: None,
            tags: vec!["test".to_string()],
        };
        state
            .notebooks
            .write()
            .insert(notebook.id.clone(), notebook.clone());
        notebook
    }

    #[test]
    fn test_state_new() {
        let state = NotebooksApiState::new();
        assert!(state.notebooks.read().is_empty());
        assert!(state.shared.read().is_empty());
    }

    #[test]
    fn test_cell_type_serde() {
        let json = serde_json::to_string(&CellType::Sql).unwrap();
        assert_eq!(json, "\"sql\"");
        let ct: CellType = serde_json::from_str("\"markdown\"").unwrap();
        assert_eq!(ct, CellType::Markdown);
    }

    #[test]
    fn test_output_type_serde() {
        let json = serde_json::to_string(&OutputType::Table).unwrap();
        assert_eq!(json, "\"table\"");
        let ot: OutputType = serde_json::from_str("\"error\"").unwrap();
        assert_eq!(ot, OutputType::Error);
    }

    #[test]
    fn test_notebook_serde_roundtrip() {
        let nb = Notebook {
            id: "nb-1".to_string(),
            title: "Test".to_string(),
            description: None,
            cells: vec![sql_cell("SELECT 1")],
            variables: HashMap::new(),
            created_at: now_iso(),
            updated_at: now_iso(),
            author: None,
            tags: vec![],
        };
        let json = serde_json::to_string(&nb).unwrap();
        let parsed: Notebook = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.id, "nb-1");
        assert_eq!(parsed.cells.len(), 1);
    }

    #[test]
    fn test_cell_output_with_error() {
        let output = CellOutput {
            output_type: OutputType::Error,
            data: serde_json::json!(null),
            execution_time_ms: 5.0,
            row_count: None,
            error: Some("syntax error".to_string()),
        };
        let json = serde_json::to_string(&output).unwrap();
        assert!(json.contains("syntax error"));
    }

    #[tokio::test]
    async fn test_create_notebook_endpoint() {
        let app = test_router();
        let body = serde_json::json!({
            "title": "My Notebook",
            "description": "Analysis notebook",
            "tags": ["analytics"]
        });
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/notebooks")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    #[tokio::test]
    async fn test_list_notebooks_empty() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/notebooks")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_get_notebook_not_found() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/notebooks/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_get_notebook_found() {
        let state = test_state();
        let nb = create_test_notebook(&state).await;
        let app = create_notebooks_api_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(&format!("/api/v1/notebooks/{}", nb.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_update_notebook() {
        let state = test_state();
        let nb = create_test_notebook(&state).await;
        let app = create_notebooks_api_router(state);
        let body = serde_json::json!({"title": "Updated Title"});
        let resp = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri(&format!("/api/v1/notebooks/{}", nb.id))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_notebook() {
        let state = test_state();
        let nb = create_test_notebook(&state).await;
        let app = create_notebooks_api_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(&format!("/api/v1/notebooks/{}", nb.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_delete_notebook_not_found() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/notebooks/missing")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_execute_cell() {
        let state = test_state();
        let nb = create_test_notebook(&state).await;
        let app = create_notebooks_api_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/notebooks/{}/cells/0/execute", nb.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_execute_cell_invalid_index() {
        let state = test_state();
        let nb = create_test_notebook(&state).await;
        let app = create_notebooks_api_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/notebooks/{}/cells/99/execute", nb.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_run_all_cells() {
        let state = test_state();
        let nb = create_test_notebook(&state).await;
        let app = create_notebooks_api_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/notebooks/{}/run-all", nb.id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_export_markdown() {
        let state = test_state();
        let nb = create_test_notebook(&state).await;
        let app = create_notebooks_api_router(state);
        let body = serde_json::json!({"format": "markdown"});
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/notebooks/{}/export", nb.id))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_export_json() {
        let state = test_state();
        let nb = create_test_notebook(&state).await;
        let app = create_notebooks_api_router(state);
        let body = serde_json::json!({"format": "json"});
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/notebooks/{}/export", nb.id))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_export_invalid_format() {
        let state = test_state();
        let nb = create_test_notebook(&state).await;
        let app = create_notebooks_api_router(state);
        let body = serde_json::json!({"format": "pdf"});
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(&format!("/api/v1/notebooks/{}/export", nb.id))
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_share_and_get_shared() {
        let state = test_state();
        let nb = create_test_notebook(&state).await;
        // Share the notebook
        let token = Uuid::new_v4().to_string();
        state.shared.write().insert(token.clone(), nb.id.clone());
        // Retrieve via share token
        let app = create_notebooks_api_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri(&format!("/api/v1/notebooks/shared/{}", token))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_shared_notebook_invalid_token() {
        let app = test_router();
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/notebooks/shared/bad-token")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_create_notebook_request_deser() {
        let json = serde_json::json!({
            "title": "T",
            "cells": [{"cell_type": "sql", "source": "SELECT 1", "metadata": {}}]
        });
        let req: CreateNotebookRequest = serde_json::from_value(json).unwrap();
        assert_eq!(req.title, "T");
        assert_eq!(req.cells.unwrap().len(), 1);
    }
}
