//! HTTP API for branched streams (M5 wiring).
//!
//! Routes:
//!   * `POST   /api/v1/branches`              — create a branch
//!   * `GET    /api/v1/branches`              — list branches
//!   * `GET    /api/v1/branches/:id`          — get a branch
//!   * `DELETE /api/v1/branches/:id`          — discard a branch
//!   * `POST   /api/v1/branches/:id/messages` — append to branch write topic
//!   * `GET    /api/v1/branches/:id/messages?partition=&after=`
//!                                            — read next message after offset
//!   * `POST   /api/v1/branches/:id/run`      — run a transform on branch
//!   * `POST   /api/v1/branches/:id/diff`     — diff branch vs base
//!   * `POST   /api/v1/branches/:id/merge`    — merge branch into target topic
//!
//! Branch IDs are URL-encoded `<base_topic>:<name>`. Backed by a process-
//! global in-memory [`BranchStore`]; production swaps to the log layer.

mod types;
mod handlers;

pub use types::*;
pub use handlers::BranchesApiState;

use axum::{routing::{get, post}, Router};

use handlers::{
    create_branch, list_branches, get_branch, discard_branch,
    append_branch, read_branch, run_handler, diff_handler, merge_handler,
};

/// Build the branches API router wired to the given [`BranchesApiState`].
pub fn create_branches_api_router(state: BranchesApiState) -> Router {
    Router::new()
        .route("/api/v1/branches", post(create_branch).get(list_branches))
        .route("/api/v1/branches/:id", get(get_branch).delete(discard_branch))
        .route("/api/v1/branches/:id/messages", post(append_branch).get(read_branch))
        .route("/api/v1/branches/:id/run", post(run_handler))
        .route("/api/v1/branches/:id/diff", post(diff_handler))
        .route("/api/v1/branches/:id/merge", post(merge_handler))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use axum::body::{to_bytes, Body};
    use axum::http::Request;
    use axum::http::StatusCode;
    use crate::branches::BranchStore;
    use tower::ServiceExt;

    fn fresh_state() -> BranchesApiState {
        BranchesApiState {
            store: Arc::new(BranchStore::new()),
        }
    }

    #[tokio::test]
    async fn create_then_list_then_get_then_discard() {
        let state = fresh_state();
        let app = create_branches_api_router(state.clone());

        // Create
        let body = serde_json::to_string(&serde_json::json!({
            "base_topic": "orders",
            "name": "exp-a",
            "base_offsets": [10],
            "created_by": "alice"
        }))
        .unwrap();
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/branches")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // List
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/branches")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), 65_536).await.unwrap();
        let views: Vec<BranchView> = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].id, "orders:exp-a");

        // Get
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/branches/orders:exp-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Discard
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/branches/orders:exp-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn duplicate_create_returns_409() {
        let state = fresh_state();
        let app = create_branches_api_router(state);
        let body = serde_json::to_string(&serde_json::json!({
            "base_topic": "t", "name": "b", "base_offsets": [0], "created_by": "a"
        }))
        .unwrap();
        let make_req = || {
            Request::builder()
                .method("POST")
                .uri("/api/v1/branches")
                .header("content-type", "application/json")
                .body(Body::from(body.clone()))
                .unwrap()
        };
        assert_eq!(
            app.clone().oneshot(make_req()).await.unwrap().status(),
            StatusCode::CREATED
        );
        assert_eq!(
            app.clone().oneshot(make_req()).await.unwrap().status(),
            StatusCode::CONFLICT
        );
    }

    #[tokio::test]
    async fn append_then_read_returns_branch_value() {
        let state = fresh_state();
        let app = create_branches_api_router(state);

        let body = serde_json::to_string(&serde_json::json!({
            "base_topic": "t", "name": "b", "base_offsets": [10], "created_by": "a"
        }))
        .unwrap();
        let _ = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/branches")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Append
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/branches/t:b/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::to_string(&serde_json::json!({
                            "partition": 0, "value": "hello"
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // Read past base boundary (after=9 → next is branch offset 0 at logical 10)
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/branches/t:b/messages?partition=0&after=9")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), 65_536).await.unwrap();
        let v: ReadBranchResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(!v.from_base);
        assert_eq!(v.offset, 10);
        assert_eq!(v.value.as_deref(), Some("hello"));
    }

    #[tokio::test]
    async fn read_unknown_branch_404() {
        let state = fresh_state();
        let app = create_branches_api_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/branches/ghost:x/messages?partition=0&after=-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn create_with_empty_topic_400() {
        let state = fresh_state();
        let app = create_branches_api_router(state);
        let body = serde_json::to_string(&serde_json::json!({
            "base_topic": "", "name": "b", "base_offsets": [], "created_by": ""
        }))
        .unwrap();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/branches")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// Regression: when the broker is constructed with an injected
    /// `BranchStore`, the API must operate on *that* store, not the
    /// process-wide `shared()` global. This isolates multi-tenant
    /// deployments.
    #[tokio::test]
    async fn injected_store_isolates_from_global() {
        // Two distinct stores, two distinct apps.
        let store_a = Arc::new(BranchStore::new());
        let store_b = Arc::new(BranchStore::new());
        let app_a = create_branches_api_router(BranchesApiState {
            store: store_a.clone(),
        });
        let app_b = create_branches_api_router(BranchesApiState {
            store: store_b.clone(),
        });

        // Create a branch through app_a only.
        let body = serde_json::to_string(&serde_json::json!({
            "base_topic": "orders",
            "name": "isolated",
            "base_offsets": [0],
            "created_by": "tenant-a"
        }))
        .unwrap();
        let resp = app_a
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/branches")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);

        // app_b must not see it.
        let resp = app_b
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/api/v1/branches")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), 65_536).await.unwrap();
        let views: Vec<BranchView> = serde_json::from_slice(&bytes).unwrap();
        assert!(
            views.is_empty(),
            "tenant-b store leaked tenant-a's branch: {:?}",
            views
        );

        // Direct store inspection confirms.
        assert_eq!(store_a.list().len(), 1);
        assert_eq!(store_b.list().len(), 0);
    }

    // -----------------------------------------------------------------------
    // Serde unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn create_branch_request_deserializes() {
        let json = r#"{
            "base_topic": "orders",
            "name": "exp-a",
            "base_offsets": [0, 1, 2],
            "created_by": "alice"
        }"#;
        let req: CreateBranchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.base_topic, "orders");
        assert_eq!(req.name, "exp-a");
        assert_eq!(req.base_offsets, vec![0, 1, 2]);
        assert_eq!(req.created_by, "alice");
    }

    #[test]
    fn create_branch_request_empty_offsets() {
        let json = r#"{
            "base_topic": "t",
            "name": "b",
            "base_offsets": [],
            "created_by": "a"
        }"#;
        let req: CreateBranchRequest = serde_json::from_str(json).unwrap();
        assert!(req.base_offsets.is_empty());
    }

    #[test]
    fn branch_view_serializes_all_fields() {
        let view = BranchView {
            id: "orders:exp-a".into(),
            base_topic: "orders".into(),
            base_offsets: vec![10, 20],
            created_by: "alice".into(),
            created_at_ms: 1700000000000,
            state: "Active".into(),
            write_topic: "__branch.orders.exp-a".into(),
        };
        let json = serde_json::to_value(&view).unwrap();
        assert_eq!(json["id"], "orders:exp-a");
        assert_eq!(json["base_topic"], "orders");
        assert_eq!(json["base_offsets"], serde_json::json!([10, 20]));
        assert_eq!(json["created_by"], "alice");
        assert_eq!(json["created_at_ms"], 1700000000000u64);
        assert_eq!(json["state"], "Active");
        assert_eq!(json["write_topic"], "__branch.orders.exp-a");
    }

    #[test]
    fn branch_view_roundtrip() {
        let view = BranchView {
            id: "t:b".into(),
            base_topic: "t".into(),
            base_offsets: vec![0],
            created_by: "bob".into(),
            created_at_ms: 123456,
            state: "Active".into(),
            write_topic: "__branch.t.b".into(),
        };
        let json = serde_json::to_string(&view).unwrap();
        let deser: BranchView = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.id, view.id);
        assert_eq!(deser.base_topic, view.base_topic);
        assert_eq!(deser.created_at_ms, view.created_at_ms);
    }

    #[test]
    fn run_branch_request_deserializes_identity() {
        let json = r#"{"transform": "identity"}"#;
        let req: RunBranchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.transform, "identity");
        assert!(req.data.is_none());
    }

    #[test]
    fn run_branch_request_deserializes_sql_with_data() {
        let json = r#"{"transform": "sql", "data": "SELECT * FROM t WHERE amount > 100"}"#;
        let req: RunBranchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.transform, "sql");
        assert_eq!(
            req.data.as_deref(),
            Some("SELECT * FROM t WHERE amount > 100")
        );
    }

    #[test]
    fn run_branch_request_deserializes_wasm_with_data() {
        let json = r#"{"transform": "wasm", "data": "base64bytes"}"#;
        let req: RunBranchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.transform, "wasm");
        assert_eq!(req.data.as_deref(), Some("base64bytes"));
    }

    #[test]
    fn diff_branch_response_serializes() {
        let resp = DiffBranchResponse {
            base_count: 100,
            branch_count: 110,
            records_added: 15,
            records_removed: 5,
            records_modified: 3,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["base_count"], 100);
        assert_eq!(json["branch_count"], 110);
        assert_eq!(json["records_added"], 15);
        assert_eq!(json["records_removed"], 5);
        assert_eq!(json["records_modified"], 3);
    }

    #[test]
    fn merge_branch_request_defaults_confirm_false() {
        let json = r#"{"target_topic": "orders"}"#;
        let req: MergeBranchRequest = serde_json::from_str(json).unwrap();
        assert!(!req.confirm);
        assert_eq!(req.target_topic, "orders");
    }

    #[test]
    fn merge_branch_request_with_confirm_true() {
        let json = r#"{"target_topic": "orders", "confirm": true}"#;
        let req: MergeBranchRequest = serde_json::from_str(json).unwrap();
        assert!(req.confirm);
    }

    #[test]
    fn append_branch_request_deserializes() {
        let json = r#"{"partition": 3, "value": "hello world"}"#;
        let req: AppendBranchRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.partition, 3);
        assert_eq!(req.value, "hello world");
    }

    #[test]
    fn read_branch_query_defaults_after() {
        let q: ReadBranchQuery =
            serde_json::from_str(r#"{"partition": 0}"#).unwrap();
        assert_eq!(q.partition, 0);
        assert_eq!(q.after, -1);
    }

    #[test]
    fn read_branch_query_explicit_after() {
        let q: ReadBranchQuery =
            serde_json::from_str(r#"{"partition": 2, "after": 99}"#).unwrap();
        assert_eq!(q.partition, 2);
        assert_eq!(q.after, 99);
    }

    #[test]
    fn read_branch_response_roundtrip() {
        let resp = ReadBranchResponse {
            from_base: true,
            partition: 0,
            offset: 42,
            value: Some("payload".into()),
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deser: ReadBranchResponse = serde_json::from_str(&json).unwrap();
        assert!(deser.from_base);
        assert_eq!(deser.offset, 42);
        assert_eq!(deser.value.as_deref(), Some("payload"));
    }

    #[test]
    fn read_branch_response_null_value() {
        let resp = ReadBranchResponse {
            from_base: true,
            partition: 0,
            offset: 5,
            value: None,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert!(json["value"].is_null());
    }

    #[test]
    fn append_branch_response_serializes() {
        let resp = AppendBranchResponse {
            partition: 1,
            offset: 42,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["partition"], 1);
        assert_eq!(json["offset"], 42);
    }

    #[test]
    fn merge_branch_response_serializes() {
        let resp = MergeBranchResponse {
            records_merged: 10,
            branch_discarded: true,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["records_merged"], 10);
        assert_eq!(json["branch_discarded"], true);
    }

    #[test]
    fn run_branch_response_serializes() {
        let resp = RunBranchResponse {
            records_read: 100,
            records_written: 90,
            records_skipped: 5,
            errors: 5,
            completed: true,
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["records_read"], 100);
        assert_eq!(json["records_written"], 90);
        assert_eq!(json["records_skipped"], 5);
        assert_eq!(json["errors"], 5);
        assert_eq!(json["completed"], true);
    }

    #[test]
    fn api_error_serializes() {
        let err = ApiError::new("not_found", "branch x not found");
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(json["error"], "not_found");
        assert_eq!(json["message"], "branch x not found");
    }

    // -----------------------------------------------------------------------
    // Handler validation tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn create_with_empty_name_returns_400() {
        let state = fresh_state();
        let app = create_branches_api_router(state);
        let body = serde_json::to_string(&serde_json::json!({
            "base_topic": "orders", "name": "", "base_offsets": [], "created_by": "a"
        }))
        .unwrap();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/branches")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn merge_rejects_empty_target_topic() {
        let state = fresh_state();
        let app = create_branches_api_router(state);

        // Create a branch first.
        let body = serde_json::to_string(&serde_json::json!({
            "base_topic": "t", "name": "m", "base_offsets": [0], "created_by": "a"
        }))
        .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/branches")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = serde_json::to_string(&serde_json::json!({
            "target_topic": " ", "confirm": true
        }))
        .unwrap();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/branches/t:m/merge")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn run_rejects_unknown_transform() {
        let state = fresh_state();
        let app = create_branches_api_router(state);

        let body = serde_json::to_string(&serde_json::json!({
            "base_topic": "t", "name": "r", "base_offsets": [0], "created_by": "a"
        }))
        .unwrap();
        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/branches")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = serde_json::to_string(&serde_json::json!({
            "transform": "exotic_transform"
        }))
        .unwrap();
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/branches/t:r/run")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn diff_unknown_branch_returns_404() {
        let state = fresh_state();
        let app = create_branches_api_router(state);
        let resp = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/branches/ghost:x/diff")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
