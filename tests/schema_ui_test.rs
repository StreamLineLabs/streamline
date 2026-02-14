//! Integration tests for the Schema Registry Web UI
//!
//! These tests verify that the schema UI routes return proper HTML pages
//! and that the JSON API endpoints work correctly from an end-to-end
//! perspective.
//!
//! Requires the `schema-registry` feature to be enabled.

#![cfg(feature = "schema-registry")]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

use streamline::schema::{
    CompatibilityLevel, SchemaRegistryConfig, SchemaStore, SchemaType,
};
use streamline::server::schema_ui::{create_schema_ui_router, SchemaUiState};

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

// ---------------------------------------------------------------------------
// HTML page tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_schema_list_returns_html() {
    let state = create_test_state();
    let app = create_schema_ui_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ui/schemas")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8(body.to_vec()).unwrap();

    // The page should contain the base layout
    assert!(html.contains("<!DOCTYPE html>"));
    assert!(html.contains("Streamline"));
    assert!(html.contains("Schema Registry"));
}

#[tokio::test]
async fn test_schema_list_shows_registered_subjects() {
    let state = create_test_state();

    // Register two schemas under different subjects
    state
        .store
        .register_schema(
            "orders-value",
            r#"{"type": "string"}"#,
            SchemaType::Avro,
            vec![],
        )
        .await
        .unwrap();
    state
        .store
        .register_schema(
            "users-value",
            r#"{"type": "object", "properties": {"name": {"type": "string"}}}"#,
            SchemaType::Json,
            vec![],
        )
        .await
        .unwrap();

    let app = create_schema_ui_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ui/schemas")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8(body.to_vec()).unwrap();

    assert!(html.contains("orders-value"));
    assert!(html.contains("users-value"));
    // Stats should show 2 subjects
    assert!(html.contains(">2<"));
}

#[tokio::test]
async fn test_schema_detail_page_shows_versions() {
    let state = create_test_state();

    // Register two versions
    state
        .store
        .register_schema(
            "events-value",
            r#"{"type": "record", "name": "Event", "fields": [{"name": "id", "type": "string"}]}"#,
            SchemaType::Avro,
            vec![],
        )
        .await
        .unwrap();
    state
        .store
        .register_schema(
            "events-value",
            r#"{"type": "record", "name": "Event", "fields": [{"name": "id", "type": "string"}, {"name": "ts", "type": "long", "default": 0}]}"#,
            SchemaType::Avro,
            vec![],
        )
        .await
        .unwrap();

    let app = create_schema_ui_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ui/schemas/events-value")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8(body.to_vec()).unwrap();

    assert!(html.contains("events-value"));
    assert!(html.contains("2 version(s)"));
    // Both version tabs should be present
    assert!(html.contains("tab-v1"));
    assert!(html.contains("tab-v2"));
}

#[tokio::test]
async fn test_schema_version_permalink() {
    let state = create_test_state();

    state
        .store
        .register_schema(
            "permalink-test",
            r#"{"type": "string"}"#,
            SchemaType::Avro,
            vec![],
        )
        .await
        .unwrap();

    let app = create_schema_ui_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ui/schemas/permalink-test/versions/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8(body.to_vec()).unwrap();

    assert!(html.contains("permalink-test"));
    assert!(html.contains("Version 1"));
    assert!(html.contains("Schema ID:"));
}

#[tokio::test]
async fn test_compare_page_loads() {
    let state = create_test_state();
    let app = create_schema_ui_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ui/schemas/compare")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8(body.to_vec()).unwrap();

    assert!(html.contains("Schema Comparison"));
    assert!(html.contains("compare-form"));
}

#[tokio::test]
async fn test_register_page_loads() {
    let state = create_test_state();
    let app = create_schema_ui_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ui/schemas/register")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8(body.to_vec()).unwrap();

    assert!(html.contains("Register New Schema"));
    assert!(html.contains("register-form"));
}

// ---------------------------------------------------------------------------
// JSON API tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_api_register_avro_schema() {
    let state = create_test_state();
    let app = create_schema_ui_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/schemas/my-topic-value")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"schema": "{\"type\": \"string\"}", "schemaType": "AVRO", "references": []}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["id"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn test_api_register_json_schema() {
    let state = create_test_state();
    let app = create_schema_ui_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/schemas/json-topic-value")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"schema": "{\"type\": \"object\", \"properties\": {\"name\": {\"type\": \"string\"}}}", "schemaType": "JSON", "references": []}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_api_register_invalid_schema_returns_error() {
    let state = create_test_state();
    let app = create_schema_ui_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/schemas/bad-subject")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"schema": "this is not valid avro {{{", "schemaType": "AVRO", "references": []}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["error"], true);
    assert!(json["message"].as_str().is_some());
}

#[tokio::test]
async fn test_nonexistent_subject_shows_not_found_message() {
    let state = create_test_state();
    let app = create_schema_ui_router(state);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/ui/schemas/does-not-exist")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // The page returns 200 with an HTML "not found" message, not a 404
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let html = String::from_utf8(body.to_vec()).unwrap();
    assert!(html.contains("Subject not found"));
}
