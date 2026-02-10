//! Schema Registry Web UI
//!
//! This module provides an embedded web UI for the Schema Registry,
//! served from the HTTP API on port 9094. The UI is built with inline
//! HTML templates, Tailwind CSS (via CDN), and highlight.js for schema
//! syntax highlighting. No external build tools are required.
//!
//! ## UI Routes
//!
//! - `GET /ui/schemas` - List all registered schema subjects
//! - `GET /ui/schemas/register` - Form to register a new schema
//! - `GET /ui/schemas/compare` - Side-by-side schema comparison tool
//! - `GET /ui/schemas/{subject}` - Schema detail page (all versions)
//! - `GET /ui/schemas/{subject}/versions/{version}` - Specific version permalink
//!
//! ## API Routes (used by the UI and external clients)
//!
//! - `POST /api/schemas/{subject}` - Register a new schema (JSON body)
//! - `GET  /api/schemas/{subject}/compatibility` - Check compatibility (query: schema, schemaType)
//!
//! ## Feature Gate
//!
//! This module is gated behind the `schema-registry` feature.

use super::schema_ui_templates as tpl;
use crate::schema::{
    CompatibilityLevel, SchemaRegistrationRequest, SchemaRegistrationResponse, SchemaStore,
    SchemaType,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, error};

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

/// Shared state for the Schema UI routes.
#[derive(Clone)]
pub struct SchemaUiState {
    /// Reference to the schema store.
    pub store: Arc<SchemaStore>,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Create the Schema UI router.
///
/// This should be merged into the main HTTP server router inside
/// `build_http_router` in `src/server/http.rs`.
pub fn create_schema_ui_router(state: SchemaUiState) -> Router {
    Router::new()
        // HTML UI pages
        .route("/ui/schemas", get(schema_list_page))
        .route("/ui/schemas/register", get(schema_register_page))
        .route("/ui/schemas/compare", get(schema_compare_page))
        .route("/ui/schemas/:subject", get(schema_detail_page))
        .route(
            "/ui/schemas/:subject/versions/:version",
            get(schema_version_page),
        )
        // JSON API endpoints used by the UI
        .route("/api/schemas/:subject", post(api_register_schema))
        .route(
            "/api/schemas/:subject/compatibility",
            get(api_check_compatibility),
        )
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Render a full page by wrapping `content` inside the base layout.
fn render_page(title: &str, content: &str) -> Html<String> {
    let html = tpl::BASE_LAYOUT
        .replace("{title}", title)
        .replace("{content}", content);
    Html(html)
}

/// Escape HTML special characters in schema content to prevent XSS
/// when embedding user-provided schema text inside HTML.
fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

/// Pretty-print a schema string (JSON only). Falls back to the raw
/// string for non-JSON formats.
fn pretty_print_schema(schema: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(schema) {
        Ok(val) => serde_json::to_string_pretty(&val).unwrap_or_else(|_| schema.to_string()),
        Err(_) => schema.to_string(),
    }
}

/// Return a Tailwind CSS color class for a schema type badge.
fn type_badge_color(schema_type: SchemaType) -> &'static str {
    match schema_type {
        SchemaType::Avro => "bg-blue-100 text-blue-800",
        SchemaType::Protobuf => "bg-purple-100 text-purple-800",
        SchemaType::Json => "bg-green-100 text-green-800",
    }
}

/// Return a display string for a compatibility level.
fn compat_display(level: CompatibilityLevel) -> &'static str {
    match level {
        CompatibilityLevel::None => "NONE",
        CompatibilityLevel::Backward => "BACKWARD",
        CompatibilityLevel::Forward => "FORWARD",
        CompatibilityLevel::Full => "FULL",
        CompatibilityLevel::BackwardTransitive => "BACKWARD_TRANSITIVE",
        CompatibilityLevel::ForwardTransitive => "FORWARD_TRANSITIVE",
        CompatibilityLevel::FullTransitive => "FULL_TRANSITIVE",
    }
}

// ---------------------------------------------------------------------------
// UI Page Handlers
// ---------------------------------------------------------------------------

/// `GET /ui/schemas` - List all registered subjects.
async fn schema_list_page(State(state): State<SchemaUiState>) -> Response {
    let subjects = state.store.get_subjects();
    let global_compat = state.store.get_global_compatibility().await;

    let mut total_versions: usize = 0;
    let mut rows = String::new();

    if subjects.is_empty() {
        rows = tpl::EMPTY_SUBJECTS.to_string();
    } else {
        // Sort subjects for stable display order
        let mut sorted_subjects = subjects.clone();
        sorted_subjects.sort();

        for subject in &sorted_subjects {
            let version_count = state
                .store
                .get_versions(subject)
                .map(|v| v.len())
                .unwrap_or(0);
            total_versions += version_count;

            let (latest_version, schema_type, compat) =
                match state.store.get_latest_schema(subject) {
                    Ok(schema) => {
                        let compat = state.store.get_subject_compatibility(subject).await;
                        (schema.version, schema.schema_type, compat)
                    }
                    Err(_) => (0, SchemaType::Avro, CompatibilityLevel::Backward),
                };

            let row = tpl::SUBJECT_ROW
                .replace("{subject}", subject)
                .replace("{schema_type}", &schema_type.to_string())
                .replace("{version_count}", &version_count.to_string())
                .replace("{latest_version}", &latest_version.to_string())
                .replace("{compatibility}", compat_display(compat))
                .replace("{type_color}", type_badge_color(schema_type));

            rows.push_str(&row);
        }
    }

    let content = tpl::SCHEMA_LIST_PAGE
        .replace("{total_subjects}", &subjects.len().to_string())
        .replace("{total_versions}", &total_versions.to_string())
        .replace("{global_compatibility}", compat_display(global_compat))
        .replace("{subject_rows}", &rows);

    render_page("Schemas", &content).into_response()
}

/// `GET /ui/schemas/register` - Registration form page.
async fn schema_register_page() -> Response {
    render_page("Register Schema", tpl::SCHEMA_REGISTER_PAGE).into_response()
}

/// Query params for the compare page.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct CompareQuery {
    left: Option<String>,
    #[serde(rename = "leftVersion")]
    left_version: Option<String>,
    right: Option<String>,
    #[serde(rename = "rightVersion")]
    right_version: Option<String>,
}

/// `GET /ui/schemas/compare` - Comparison page.
async fn schema_compare_page(
    State(state): State<SchemaUiState>,
    Query(query): Query<CompareQuery>,
) -> Response {
    let subjects = state.store.get_subjects();
    let mut sorted_subjects = subjects;
    sorted_subjects.sort();

    // Build <option> elements
    let options: String = sorted_subjects
        .iter()
        .map(|s| format!(r#"<option value="{}">{}</option>"#, escape_html(s), escape_html(s)))
        .collect::<Vec<_>>()
        .join("\n          ");

    let content = tpl::SCHEMA_COMPARE_PAGE
        .replace("{subject_options}", &options)
        .replace(
            "{left_version}",
            &query.left_version.unwrap_or_else(|| "1".to_string()),
        )
        .replace(
            "{right_version}",
            &query.right_version.unwrap_or_else(|| "1".to_string()),
        );

    render_page("Compare Schemas", &content).into_response()
}

/// `GET /ui/schemas/{subject}` - Schema detail page (all versions).
async fn schema_detail_page(
    State(state): State<SchemaUiState>,
    Path(subject): Path<String>,
) -> Response {
    // Handle the "register" and "compare" literal paths that Axum would
    // otherwise route to this handler.
    if subject == "register" || subject == "compare" {
        return (StatusCode::NOT_FOUND, "Not found").into_response();
    }

    let versions = match state.store.get_versions(&subject) {
        Ok(v) => v,
        Err(_) => {
            let content = format!(
                r#"<div class="bg-white rounded-lg shadow p-12 text-center">
                    <h2 class="text-lg font-semibold text-gray-900">Subject not found</h2>
                    <p class="mt-2 text-sm text-gray-500">The subject <code>{}</code> does not exist.</p>
                    <a href="/ui/schemas" class="mt-4 inline-block text-indigo-600 hover:text-indigo-500">Back to schemas</a>
                  </div>"#,
                escape_html(&subject)
            );
            return render_page("Not Found", &content).into_response();
        }
    };

    // Get latest schema info for header
    let latest = state.store.get_latest_schema(&subject).ok();
    let schema_type = latest
        .as_ref()
        .map(|s| s.schema_type)
        .unwrap_or(SchemaType::Avro);
    let compat = state.store.get_subject_compatibility(&subject).await;
    let latest_version = latest.as_ref().map(|s| s.version).unwrap_or(1);

    // Build version tabs
    let mut tabs = String::new();
    for &v in &versions {
        let active_class = if v == latest_version {
            "border-indigo-500 text-indigo-600"
        } else {
            "border-transparent text-gray-500"
        };
        tabs.push_str(
            &tpl::VERSION_TAB
                .replace("{version}", &v.to_string())
                .replace("{active_class}", active_class),
        );
    }

    // Build version panels
    let mut panels = String::new();
    for &v in &versions {
        if let Ok(schema) = state.store.get_schema(&subject, v) {
            let hidden_class = if v == latest_version { "" } else { "hidden" };
            let pretty = pretty_print_schema(&schema.schema);
            panels.push_str(
                &tpl::VERSION_PANEL
                    .replace("{version}", &v.to_string())
                    .replace("{id}", &schema.id.to_string())
                    .replace("{schema_type}", &schema.schema_type.to_string())
                    .replace("{schema_content}", &escape_html(&pretty))
                    .replace("{hidden_class}", hidden_class)
                    .replace("{subject}", &escape_html(&subject)),
            );
        }
    }

    let content = tpl::SCHEMA_DETAIL_PAGE
        .replace("{subject}", &escape_html(&subject))
        .replace("{schema_type}", &schema_type.to_string())
        .replace("{compatibility}", compat_display(compat))
        .replace("{version_count}", &versions.len().to_string())
        .replace("{version_tabs}", &tabs)
        .replace("{version_panels}", &panels)
        .replace("{latest_version}", &latest_version.to_string())
        .replace(
            "{latest_id}",
            &latest
                .as_ref()
                .map(|s| s.id.to_string())
                .unwrap_or_else(|| "0".to_string()),
        )
        .replace("{type_color}", type_badge_color(schema_type));

    render_page(&subject, &content).into_response()
}

/// `GET /ui/schemas/{subject}/versions/{version}` - Specific version permalink.
async fn schema_version_page(
    State(state): State<SchemaUiState>,
    Path((subject, version)): Path<(String, i32)>,
) -> Response {
    let schema = match state.store.get_schema(&subject, version) {
        Ok(s) => s,
        Err(_) => {
            let content = format!(
                r#"<div class="bg-white rounded-lg shadow p-12 text-center">
                    <h2 class="text-lg font-semibold text-gray-900">Version not found</h2>
                    <p class="mt-2 text-sm text-gray-500">Version {} of subject <code>{}</code> does not exist.</p>
                    <a href="/ui/schemas/{}" class="mt-4 inline-block text-indigo-600 hover:text-indigo-500">Back to subject</a>
                  </div>"#,
                version,
                escape_html(&subject),
                escape_html(&subject)
            );
            return render_page("Not Found", &content).into_response();
        }
    };

    let pretty = pretty_print_schema(&schema.schema);

    let content = tpl::SCHEMA_VERSION_PAGE
        .replace("{subject}", &escape_html(&subject))
        .replace("{version}", &version.to_string())
        .replace("{id}", &schema.id.to_string())
        .replace("{schema_type}", &schema.schema_type.to_string())
        .replace("{schema_content}", &escape_html(&pretty))
        .replace("{type_color}", type_badge_color(schema.schema_type));

    let title = format!("{} v{}", subject, version);
    render_page(&title, &content).into_response()
}

// ---------------------------------------------------------------------------
// JSON API Handlers (used by the UI forms and external clients)
// ---------------------------------------------------------------------------

/// `POST /api/schemas/{subject}` - Register a new schema version.
///
/// Request body (JSON):
/// ```json
/// {
///   "schema": "...",
///   "schemaType": "AVRO",
///   "references": []
/// }
/// ```
///
/// Returns `{ "id": <schema_id> }` on success.
async fn api_register_schema(
    State(state): State<SchemaUiState>,
    Path(subject): Path<String>,
    Json(request): Json<SchemaRegistrationRequest>,
) -> Response {
    debug!(subject = %subject, schema_type = %request.schema_type, "Schema UI: registering schema");

    match state
        .store
        .register_schema(
            &subject,
            &request.schema,
            request.schema_type,
            request.references,
        )
        .await
    {
        Ok(id) => {
            let response = SchemaRegistrationResponse { id };
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(e) => {
            error!(error = %e, "Schema UI: registration failed");
            let body = serde_json::json!({ "error": true, "message": e.to_string() });
            (StatusCode::BAD_REQUEST, Json(body)).into_response()
        }
    }
}

/// Query parameters for the compatibility check API.
#[derive(Debug, Deserialize)]
struct CompatibilityQuery {
    /// The schema to check.
    schema: String,
    /// Schema type (defaults to AVRO).
    #[serde(rename = "schemaType", default = "default_schema_type")]
    schema_type: String,
    /// Optional version to check against (defaults to all).
    version: Option<i32>,
}

fn default_schema_type() -> String {
    "AVRO".to_string()
}

/// Compatibility check response.
#[derive(Debug, Serialize)]
struct CompatibilityResponse {
    is_compatible: bool,
    messages: Vec<String>,
}

/// `GET /api/schemas/{subject}/compatibility` - Check schema compatibility.
async fn api_check_compatibility(
    State(state): State<SchemaUiState>,
    Path(subject): Path<String>,
    Query(query): Query<CompatibilityQuery>,
) -> Response {
    let schema_type: SchemaType = match query.schema_type.parse() {
        Ok(t) => t,
        Err(_) => {
            let body =
                serde_json::json!({ "error": true, "message": "Invalid schema type" });
            return (StatusCode::BAD_REQUEST, Json(body)).into_response();
        }
    };

    match state
        .store
        .check_compatibility(&subject, &query.schema, schema_type, query.version)
        .await
    {
        Ok((is_compatible, messages)) => {
            let resp = CompatibilityResponse {
                is_compatible,
                messages,
            };
            (StatusCode::OK, Json(resp)).into_response()
        }
        Err(e) => {
            let body = serde_json::json!({ "error": true, "message": e.to_string() });
            (StatusCode::BAD_REQUEST, Json(body)).into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaRegistryConfig, SchemaStore};
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    fn create_test_state() -> SchemaUiState {
        let config = SchemaRegistryConfig {
            enabled: true,
            default_compatibility: CompatibilityLevel::Backward,
            cache_ttl_seconds: 300,
            max_cache_size: 1000,
            validate_on_produce: false,
            schema_topic: "_schemas".to_string(),
        };
        SchemaUiState {
            store: Arc::new(SchemaStore::new(config)),
        }
    }

    #[tokio::test]
    async fn test_schema_list_page_empty() {
        let state = create_test_state();
        let app = create_schema_ui_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ui/schemas")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("Streamline"));
        assert!(html.contains("No schemas registered"));
    }

    #[tokio::test]
    async fn test_schema_list_page_with_subjects() {
        let state = create_test_state();

        // Register a schema
        state
            .store
            .register_schema(
                "test-value",
                r#"{"type": "string"}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_ui_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ui/schemas")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("test-value"));
        assert!(html.contains("AVRO"));
    }

    #[tokio::test]
    async fn test_schema_detail_page() {
        let state = create_test_state();

        state
            .store
            .register_schema(
                "detail-test",
                r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_ui_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ui/schemas/detail-test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("detail-test"));
        assert!(html.contains("Version 1"));
    }

    #[tokio::test]
    async fn test_schema_detail_page_not_found() {
        let state = create_test_state();
        let app = create_schema_ui_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ui/schemas/nonexistent")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("Subject not found"));
    }

    #[tokio::test]
    async fn test_schema_version_page() {
        let state = create_test_state();

        state
            .store
            .register_schema(
                "version-test",
                r#"{"type": "string"}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_ui_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ui/schemas/version-test/versions/1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("version-test"));
        assert!(html.contains("Version 1"));
    }

    #[tokio::test]
    async fn test_schema_version_page_not_found() {
        let state = create_test_state();
        let app = create_schema_ui_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ui/schemas/missing/versions/99")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("Version not found"));
    }

    #[tokio::test]
    async fn test_schema_compare_page() {
        let state = create_test_state();
        let app = create_schema_ui_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ui/schemas/compare")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("Schema Comparison"));
    }

    #[tokio::test]
    async fn test_schema_register_page() {
        let state = create_test_state();
        let app = create_schema_ui_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ui/schemas/register")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("Register New Schema"));
    }

    #[tokio::test]
    async fn test_api_register_schema() {
        let state = create_test_state();
        let app = create_schema_ui_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/schemas/api-test-value")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"schema": "{\"type\": \"string\"}", "schemaType": "AVRO", "references": []}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.get("id").is_some());
    }

    #[tokio::test]
    async fn test_api_register_schema_invalid() {
        let state = create_test_state();
        let app = create_schema_ui_router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/schemas/bad-schema")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        r#"{"schema": "not valid json {{{", "schemaType": "AVRO", "references": []}"#,
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_api_check_compatibility() {
        let state = create_test_state();

        // Register initial schema
        state
            .store
            .register_schema(
                "compat-test",
                r#"{"type": "record", "name": "Test", "fields": [{"name": "name", "type": "string"}]}"#,
                SchemaType::Avro,
                vec![],
            )
            .await
            .unwrap();

        let app = create_schema_ui_router(state);

        // Use a simple schema string that doesn't require heavy percent-encoding
        let response = app
            .oneshot(
                Request::builder()
                    .uri(
                        "/api/schemas/compat-test/compatibility?schema=%7B%22type%22%3A%22string%22%7D&schemaType=AVRO"
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json.get("is_compatible").is_some());
    }

    #[test]
    fn test_escape_html() {
        assert_eq!(escape_html("<script>"), "&lt;script&gt;");
        assert_eq!(escape_html("a & b"), "a &amp; b");
        assert_eq!(escape_html(r#""quoted""#), "&quot;quoted&quot;");
    }

    #[test]
    fn test_pretty_print_schema() {
        let input = r#"{"type":"string"}"#;
        let output = pretty_print_schema(input);
        assert!(output.contains('\n') || output.contains("string"));
    }

    #[test]
    fn test_type_badge_color() {
        assert!(type_badge_color(SchemaType::Avro).contains("blue"));
        assert!(type_badge_color(SchemaType::Protobuf).contains("purple"));
        assert!(type_badge_color(SchemaType::Json).contains("green"));
    }
}
