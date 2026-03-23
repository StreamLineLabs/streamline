//! HTTP API for the Agent Memory Fabric (M1).
//!
//! Endpoints (all under `/api/v1/memory`):
//!   * `POST /remember` — write a memory for an agent.
//!   * `POST /recall`   — recall top-k memories for a query.
//!   * `GET  /agents/:agent_id/stats`  — per-agent telemetry stats.
//!   * `POST /agents/:agent_id/export` — export agent memories (JSONL).
//!   * `DELETE /agents/:agent_id`      — GDPR delete all agent data.
//!
//! Both endpoints proxy to [`crate::memory::tier_router`] which currently
//! uses an in-process store. Replace the backend in P2 with a real
//! per-tenant log without changing the wire contract.
//!
//! Stability tier: **Experimental** (gated behind `agent-memory`).

mod types;
mod handlers;

pub use types::*;

use axum::{
    routing::{delete, get, post},
    Router,
};

use handlers::{
    remember_handler, recall_handler, stats_handler, export_handler, delete_handler,
};

/// Build the agent-memory API router. Stateless — uses the global tier
/// store inside [`tier_router`].
pub fn create_memory_api_router() -> Router {
    Router::new()
        .route("/api/v1/memory/remember", post(remember_handler))
        .route("/api/v1/memory/recall", post(recall_handler))
        .route(
            "/api/v1/memory/agents/:agent_id/stats",
            get(stats_handler),
        )
        .route(
            "/api/v1/memory/agents/:agent_id/export",
            post(export_handler),
        )
        .route(
            "/api/v1/memory/agents/:agent_id",
            delete(delete_handler),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::StatusCode;
    use tower::ServiceExt;

    #[tokio::test]
    async fn remember_then_recall_roundtrip() {
        let app = create_memory_api_router();
        let body = serde_json::json!({
            "agent_id": "http-test-agent",
            "kind": "fact",
            "content": "the deploy key lives in vault under prod/api",
            "importance": 0.9,
        });
        let res = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/memory/remember")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);

        let recall_body = serde_json::json!({
            "agent_id": "http-test-agent",
            "query": "deploy key vault",
            "k": 5,
        });
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/memory/recall")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(recall_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = to_bytes(res.into_body(), 8192).await.unwrap();
        let resp: RecallResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(!resp.hits.is_empty(), "expected at least one recall hit");
    }

    #[tokio::test]
    async fn remember_rejects_empty_content() {
        let app = create_memory_api_router();
        let body = serde_json::json!({
            "agent_id": "a",
            "kind": "fact",
            "content": "",
        });
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/memory/remember")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn recall_rejects_zero_k() {
        let app = create_memory_api_router();
        let body = serde_json::json!({
            "agent_id": "a",
            "query": "q",
            "k": 0,
        });
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/memory/recall")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn stats_returns_ok_for_known_agent() {
        let app = create_memory_api_router();
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .method("GET")
                    .uri("/api/v1/memory/agents/test-stats-agent/stats")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = to_bytes(res.into_body(), 8192).await.unwrap();
        let resp: StatsResponse = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(resp.agent_id, "test-stats-agent");
    }

    #[tokio::test]
    async fn export_returns_ok() {
        let app = create_memory_api_router();
        let body = serde_json::json!({});
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/memory/agents/export-agent/export")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = to_bytes(res.into_body(), 65_536).await.unwrap();
        let resp: ExportResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(resp.lines.len() as u64 >= resp.records_exported);
    }

    #[tokio::test]
    async fn export_rejects_invalid_format() {
        let app = create_memory_api_router();
        let body = serde_json::json!({"format": "xml"});
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/memory/agents/a/export")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn delete_returns_ok() {
        let app = create_memory_api_router();
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .method("DELETE")
                    .uri("/api/v1/memory/agents/gdpr-agent")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let bytes = to_bytes(res.into_body(), 8192).await.unwrap();
        let resp: DeleteResponse = serde_json::from_slice(&bytes).unwrap();
        assert!(resp.deleted);
        assert_eq!(resp.topics_purged.len(), 3);
    }

    // -----------------------------------------------------------------------
    // Serde unit tests
    // -----------------------------------------------------------------------

    #[test]
    fn remember_request_deserializes_fact() {
        let json = r#"{
            "agent_id": "agent-1",
            "kind": "fact",
            "content": "some content",
            "importance": 0.8,
            "tags": ["tag1", "tag2"]
        }"#;
        let req: RememberRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.agent_id, "agent-1");
        assert_eq!(req.content, "some content");
        assert_eq!(req.importance, 0.8);
        assert_eq!(req.tags, vec!["tag1", "tag2"]);
        assert!(matches!(req.kind, WriteKindWire::Fact));
    }

    #[test]
    fn remember_request_deserializes_procedure_with_skill() {
        let json = r#"{
            "agent_id": "a",
            "kind": "procedure",
            "skill": "deploy",
            "content": "c"
        }"#;
        let req: RememberRequest = serde_json::from_str(json).unwrap();
        match req.kind {
            WriteKindWire::Procedure { ref skill } => assert_eq!(skill, "deploy"),
            other => panic!("expected Procedure, got {:?}", other),
        }
    }

    #[test]
    fn remember_request_defaults_importance_and_tags() {
        let json = r#"{
            "agent_id": "a",
            "kind": "observation",
            "content": "c"
        }"#;
        let req: RememberRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.importance, 0.5);
        assert!(req.tags.is_empty());
        assert!(matches!(req.kind, WriteKindWire::Observation));
    }

    #[test]
    fn validate_remember_rejects_empty_agent_id() {
        let json = r#"{"agent_id":"","kind":"fact","content":"c"}"#;
        let req: RememberRequest = serde_json::from_str(json).unwrap();
        assert_eq!(
            validate_remember(&req),
            Err("agent_id must not be empty")
        );
    }

    #[test]
    fn validate_remember_rejects_whitespace_content() {
        let json = r#"{"agent_id":"a","kind":"fact","content":"   "}"#;
        let req: RememberRequest = serde_json::from_str(json).unwrap();
        assert_eq!(
            validate_remember(&req),
            Err("content must not be empty")
        );
    }

    #[test]
    fn validate_remember_rejects_out_of_range_importance() {
        let json = r#"{"agent_id":"a","kind":"fact","content":"c","importance":1.5}"#;
        let req: RememberRequest = serde_json::from_str(json).unwrap();
        assert!(validate_remember(&req).is_err());

        let json = r#"{"agent_id":"a","kind":"fact","content":"c","importance":-0.1}"#;
        let req: RememberRequest = serde_json::from_str(json).unwrap();
        assert!(validate_remember(&req).is_err());
    }

    #[test]
    fn validate_remember_accepts_boundary_importance() {
        let json = r#"{"agent_id":"a","kind":"fact","content":"c","importance":0.0}"#;
        let req: RememberRequest = serde_json::from_str(json).unwrap();
        assert!(validate_remember(&req).is_ok());

        let json = r#"{"agent_id":"a","kind":"fact","content":"c","importance":1.0}"#;
        let req: RememberRequest = serde_json::from_str(json).unwrap();
        assert!(validate_remember(&req).is_ok());
    }

    #[test]
    fn recall_request_defaults_k_to_10() {
        let req: RecallRequest =
            serde_json::from_str(r#"{"agent_id":"a","query":"q"}"#).unwrap();
        assert_eq!(req.k, 10);
        assert_eq!(req.min_hits, 0);
    }

    #[test]
    fn recall_request_explicit_k() {
        let req: RecallRequest =
            serde_json::from_str(r#"{"agent_id":"a","query":"q","k":50,"min_hits":3}"#)
                .unwrap();
        assert_eq!(req.k, 50);
        assert_eq!(req.min_hits, 3);
    }

    #[tokio::test]
    async fn recall_rejects_k_over_1000() {
        let app = create_memory_api_router();
        let body = serde_json::json!({
            "agent_id": "a", "query": "q", "k": 1001
        });
        let res = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/api/v1/memory/recall")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn export_request_deserializes_with_defaults() {
        let req: ExportRequest = serde_json::from_str(r#"{}"#).unwrap();
        assert!(req.format.is_none());
        assert!(req.since.is_none());
        assert!(req.until.is_none());
        assert!(req.tier.is_none());
    }

    #[test]
    fn export_request_deserializes_with_all_fields() {
        let json = r#"{"format":"csv","since":100,"until":200,"tier":"episodic"}"#;
        let req: ExportRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.format.as_deref(), Some("csv"));
        assert_eq!(req.since, Some(100));
        assert_eq!(req.until, Some(200));
        assert_eq!(req.tier.as_deref(), Some("episodic"));
    }

    #[test]
    fn stats_response_serializes_correctly() {
        let resp = StatsResponse {
            agent_id: "agent-1".into(),
            recall_total: 42,
            remember_total: 100,
            tiers: TierCounts {
                episodic: 50,
                semantic: 30,
                procedural: 20,
            },
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["agent_id"], "agent-1");
        assert_eq!(json["recall_total"], 42);
        assert_eq!(json["remember_total"], 100);
        assert_eq!(json["tiers"]["episodic"], 50);
        assert_eq!(json["tiers"]["semantic"], 30);
        assert_eq!(json["tiers"]["procedural"], 20);
    }

    #[test]
    fn stats_response_roundtrip() {
        let resp = StatsResponse {
            agent_id: "a".into(),
            recall_total: 1,
            remember_total: 2,
            tiers: TierCounts { episodic: 3, semantic: 4, procedural: 5 },
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deser: StatsResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.agent_id, "a");
        assert_eq!(deser.tiers.episodic, 3);
    }

    #[test]
    fn recall_response_roundtrip() {
        let resp = RecallResponse {
            hits: vec![RecalledHit {
                tier: "episodic".into(),
                topic: "t".into(),
                offset: 42,
                content: "hello".into(),
                score: 0.95,
            }],
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deser: RecallResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.hits.len(), 1);
        assert_eq!(deser.hits[0].offset, 42);
        assert_eq!(deser.hits[0].score, 0.95);
    }

    #[test]
    fn delete_response_roundtrip() {
        let resp = DeleteResponse {
            deleted: true,
            topics_purged: vec!["t1".into(), "t2".into()],
            records_purged: 10,
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deser: DeleteResponse = serde_json::from_str(&json).unwrap();
        assert!(deser.deleted);
        assert_eq!(deser.topics_purged.len(), 2);
        assert_eq!(deser.records_purged, 10);
    }

    #[test]
    fn export_response_roundtrip() {
        let resp = ExportResponse {
            records_exported: 5,
            bytes_written: 1024,
            lines: vec!["line1".into(), "line2".into()],
        };
        let json = serde_json::to_string(&resp).unwrap();
        let deser: ExportResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.records_exported, 5);
        assert_eq!(deser.bytes_written, 1024);
        assert_eq!(deser.lines.len(), 2);
    }

    #[test]
    fn parse_tier_valid_variants() {
        assert!(matches!(parse_tier("episodic"), Ok(crate::memory::Tier::Episodic)));
        assert!(matches!(parse_tier("semantic"), Ok(crate::memory::Tier::Semantic)));
        assert!(matches!(parse_tier("procedural"), Ok(crate::memory::Tier::Procedural)));
        assert!(matches!(parse_tier("EPISODIC"), Ok(crate::memory::Tier::Episodic)));
    }

    #[test]
    fn parse_tier_invalid() {
        assert!(parse_tier("unknown").is_err());
        assert!(parse_tier("").is_err());
    }

    #[test]
    fn write_kind_wire_converts_to_domain() {
        use crate::memory::WriteKind;
        assert!(matches!(WriteKind::from(WriteKindWire::Observation), WriteKind::Observation));
        assert!(matches!(WriteKind::from(WriteKindWire::Fact), WriteKind::Fact));
        match WriteKind::from(WriteKindWire::Procedure { skill: "deploy".into() }) {
            WriteKind::Procedure { skill } => assert_eq!(skill, "deploy"),
            other => panic!("expected Procedure, got {:?}", other),
        }
    }
}
