//! HTTP search handler scaffold for semantic topics (M2 P1).
//!
//! Endpoint: `POST /topics/{topic}/search`
//!
//! Body:
//! ```json
//! { "query": "string or vector", "k": 10, "filter": {"key": "value"} }
//! ```
//!
//! Stability tier: **Experimental**. Not yet registered with the router; wire
//! up under `feature = "semantic-topics"` when M2 P1 lands.

use std::time::Instant;

use axum::{
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Json, Router,
};
use serde::{Deserialize, Serialize};

use crate::ai::semantic_topics::worker::Embedder;
use crate::ai::semantic_topics::{registry, HashEmbedder, SemanticIndex};

#[derive(Debug, Deserialize)]
pub struct SearchRequest {
    pub query: String,
    #[serde(default = "default_k")]
    pub k: usize,
    #[serde(default)]
    pub filter: Option<serde_json::Value>,
}

#[derive(Debug, Serialize)]
pub struct SearchResponse {
    pub hits: Vec<SearchResultItem>,
    pub took_ms: u32,
}

#[derive(Debug, Serialize)]
pub struct SearchResultItem {
    pub partition: i32,
    pub offset: i64,
    pub score: f32,
    /// Optional record value (only included when `?include=value`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
}

fn default_k() -> usize {
    10
}

/// Real handler: embed `req.query` with the default `HashEmbedder` and
/// look up nearest neighbors in the per-topic registry. Returns an empty
/// hit list (with an honest `took_ms`) for unknown topics.
pub fn handle_search(topic: &str, req: SearchRequest) -> SearchResponse {
    let started = Instant::now();
    let embedder = HashEmbedder::default();
    let query_vec = match embedder.embed(&req.query) {
        Ok(v) => v,
        Err(_) => {
            return SearchResponse {
                hits: Vec::new(),
                took_ms: started.elapsed().as_millis() as u32,
            }
        }
    };
    let hits = match registry::get(topic) {
        Some(idx) => idx
            .search(&query_vec, req.k)
            .into_iter()
            .map(|h| SearchResultItem {
                partition: h.partition,
                offset: h.offset,
                score: h.score,
                value: None,
            })
            .collect(),
        None => Vec::new(),
    };
    SearchResponse {
        hits,
        took_ms: started.elapsed().as_millis() as u32,
    }
}

/// Axum handler: `POST /api/v1/topics/:topic/search`.
async fn search_handler(
    Path(topic): Path<String>,
    Json(req): Json<SearchRequest>,
) -> Response {
    if req.query.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "query must not be empty"})),
        )
            .into_response();
    }
    if req.k == 0 || req.k > 1000 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "k must be in [1, 1000]"})),
        )
            .into_response();
    }
    let resp = handle_search(&topic, req);
    (StatusCode::OK, Json(resp)).into_response()
}

/// Build the search API router. Stateless — uses the global per-topic registry.
pub fn create_search_api_router() -> Router {
    Router::new().route("/api/v1/topics/:topic/search", post(search_handler))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_request_default_k_is_10() {
        let req: SearchRequest = serde_json::from_str(r#"{"query":"hello"}"#).unwrap();
        assert_eq!(req.k, 10);
    }

    #[test]
    fn empty_topic_returns_empty_hits() {
        let resp = handle_search(
            "topic-that-was-never-indexed",
            SearchRequest {
                query: "x".into(),
                k: 5,
                filter: None,
            },
        );
        assert!(resp.hits.is_empty());
    }

    #[test]
    fn search_after_indexing_returns_top_match() {
        use crate::ai::semantic_topics::{registry, HashEmbedder, SemanticIndex};
        use crate::ai::semantic_topics::worker::Embedder;

        let embedder = HashEmbedder::default();
        let topic = "search-after-indexing-topic";
        let idx = registry::get_or_create(topic);
        for (offset, text) in [
            (1i64, "payment failed for user alice"),
            (2, "user alice logged in"),
            (3, "checkout completed for bob"),
        ] {
            let v = embedder.embed(text).expect("embed");
            idx.insert(0, offset, &v);
        }

        let resp = handle_search(
            topic,
            SearchRequest {
                query: "payment failure".into(),
                k: 3,
                filter: None,
            },
        );
        assert_eq!(resp.hits.len(), 3);
        assert_eq!(resp.hits[0].offset, 1, "payment record should rank first");
    }
}
