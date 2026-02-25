//! Message Inspector API — real-time message browsing and search
//!
//! Provides HTTP endpoints for inspecting topic messages with JSON formatting,
//! key/value decoding, and filtering. Designed for the dev mode and web console.
//!
//! ## Endpoints
//!
//! - `GET /api/v1/inspect/:topic` — Browse messages with pagination
//! - `GET /api/v1/inspect/:topic/latest` — Get the N most recent messages
//! - `GET /api/v1/inspect/:topic/search` — Search messages by content

use crate::storage::TopicManager;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// State for the message inspector API
#[derive(Clone)]
pub struct InspectorState {
    pub topic_manager: Arc<TopicManager>,
}

/// Query parameters for message browsing
#[derive(Debug, Deserialize)]
pub struct BrowseParams {
    /// Partition to read from (default: 0)
    pub partition: Option<i32>,
    /// Starting offset (default: 0)
    pub offset: Option<i64>,
    /// Maximum number of messages to return (default: 20, max: 100)
    pub limit: Option<usize>,
    /// Output format: "json" (default) or "raw"
    pub format: Option<String>,
}

/// Query parameters for latest messages
#[derive(Debug, Deserialize)]
pub struct LatestParams {
    /// Number of recent messages (default: 10, max: 100)
    pub count: Option<usize>,
    /// Partition (default: all partitions)
    pub partition: Option<i32>,
}

/// Query parameters for message search
#[derive(Debug, Deserialize)]
pub struct SearchParams {
    /// Search query (substring match on value)
    pub q: String,
    /// Maximum results (default: 20)
    pub limit: Option<usize>,
    /// Partition to search (default: all)
    pub partition: Option<i32>,
}

/// Inspected message with decoded content
#[derive(Debug, Serialize)]
pub struct InspectedMessage {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<String>,
    pub value: serde_json::Value,
    pub headers: Vec<InspectedHeader>,
    pub size_bytes: usize,
}

#[derive(Debug, Serialize)]
pub struct InspectedHeader {
    pub key: String,
    pub value: String,
}

/// Response for browse/latest endpoints
#[derive(Debug, Serialize)]
pub struct InspectResponse {
    pub topic: String,
    pub messages: Vec<InspectedMessage>,
    pub count: usize,
    pub has_more: bool,
    pub next_offset: Option<i64>,
}

/// Create the message inspector router
pub fn create_inspector_router(state: InspectorState) -> Router {
    Router::new()
        .route("/api/v1/inspect/:topic", get(browse_handler))
        .route("/api/v1/inspect/:topic/latest", get(latest_handler))
        .route("/api/v1/inspect/:topic/search", get(search_handler))
        .with_state(state)
}

async fn browse_handler(
    State(state): State<InspectorState>,
    Path(topic): Path<String>,
    Query(params): Query<BrowseParams>,
) -> Response {
    let partition = params.partition.unwrap_or(0);
    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(20).min(100);

    let records = match state.topic_manager.read(&topic, partition, offset, limit) {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    let messages: Vec<InspectedMessage> = records
        .iter()
        .map(|r| record_to_inspected(r, partition))
        .collect();

    let next_offset = messages.last().map(|m| m.offset + 1);
    let has_more = messages.len() == limit;
    let count = messages.len();

    Json(InspectResponse {
        topic,
        messages,
        count,
        has_more,
        next_offset,
    })
    .into_response()
}

async fn latest_handler(
    State(state): State<InspectorState>,
    Path(topic): Path<String>,
    Query(params): Query<LatestParams>,
) -> Response {
    let count = params.count.unwrap_or(10).min(100);
    let partition = params.partition.unwrap_or(0);

    let latest_offset = match state.topic_manager.latest_offset(&topic, partition) {
        Ok(o) => o,
        Err(e) => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    let start_offset = (latest_offset - count as i64).max(0);
    let records = match state
        .topic_manager
        .read(&topic, partition, start_offset, count)
    {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    let messages: Vec<InspectedMessage> = records
        .iter()
        .map(|r| record_to_inspected(r, partition))
        .collect();

    let msg_count = messages.len();
    Json(InspectResponse {
        topic,
        messages,
        count: msg_count,
        has_more: false,
        next_offset: None,
    })
    .into_response()
}

async fn search_handler(
    State(state): State<InspectorState>,
    Path(topic): Path<String>,
    Query(params): Query<SearchParams>,
) -> Response {
    let limit = params.limit.unwrap_or(20).min(100);
    let partition = params.partition.unwrap_or(0);
    let query_lower = params.q.to_lowercase();

    let latest = state
        .topic_manager
        .latest_offset(&topic, partition)
        .unwrap_or(0);

    let mut results = Vec::new();
    let mut offset = 0i64;
    let batch_size = 100;

    while offset < latest && results.len() < limit {
        let records = match state.topic_manager.read(&topic, partition, offset, batch_size) {
            Ok(r) => r,
            Err(_) => break,
        };

        if records.is_empty() {
            break;
        }

        for record in &records {
            let value_str = String::from_utf8_lossy(&record.value).to_lowercase();
            if value_str.contains(&query_lower) {
                results.push(record_to_inspected(record, partition));
                if results.len() >= limit {
                    break;
                }
            }
        }

        offset += batch_size as i64;
    }

    let count = results.len();
    Json(InspectResponse {
        topic,
        messages: results,
        count,
        has_more: offset < latest,
        next_offset: Some(offset),
    })
    .into_response()
}

fn record_to_inspected(record: &crate::storage::Record, partition: i32) -> InspectedMessage {
    let key = record
        .key
        .as_ref()
        .map(|k| String::from_utf8_lossy(k).to_string());

    let value = match serde_json::from_slice::<serde_json::Value>(&record.value) {
        Ok(v) => v,
        Err(_) => serde_json::Value::String(String::from_utf8_lossy(&record.value).to_string()),
    };

    let headers = record
        .headers
        .iter()
        .map(|h| InspectedHeader {
            key: h.key.clone(),
            value: String::from_utf8_lossy(&h.value).to_string(),
        })
        .collect();

    InspectedMessage {
        partition,
        offset: record.offset,
        timestamp: record.timestamp,
        key,
        value,
        headers,
        size_bytes: record.value.len(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_browse_params_defaults() {
        let params: BrowseParams = serde_json::from_str("{}").unwrap();
        assert!(params.partition.is_none());
        assert!(params.offset.is_none());
        assert!(params.limit.is_none());
    }

    #[test]
    fn test_inspect_response_serialization() {
        let response = InspectResponse {
            topic: "test".to_string(),
            messages: vec![],
            count: 0,
            has_more: false,
            next_offset: None,
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"topic\":\"test\""));
    }
}
