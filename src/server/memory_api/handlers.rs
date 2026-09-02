//! Handler functions for the agent memory API.

use axum::{
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::memory::export::{ExportConfig, ExportFormat};
use crate::memory::gdpr::{GdprDeleteError, GdprDeleteRequest};
use crate::memory::tier_router;
use crate::memory::{MemoryWrite, Tier};

use super::types::*;

pub(crate) async fn remember_handler(Json(req): Json<RememberRequest>) -> Response {
    if let Err(msg) = validate_remember(&req) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": msg})),
        )
            .into_response();
    }
    let write = MemoryWrite {
        agent_id: req.agent_id,
        kind: req.kind.into(),
        content: req.content,
        importance: req.importance,
        tags: req.tags,
    };
    match tier_router::remember(&write) {
        Ok(written) => {
            crate::memory::telemetry::record_remember(
                &write.agent_id,
                crate::memory::Tier::Episodic,
            );
            let entries: Vec<_> = written
                .into_iter()
                .map(|(topic, offset)| WrittenEntry { topic, offset })
                .collect();
            (StatusCode::OK, Json(RememberResponse { written: entries })).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub(crate) async fn recall_handler(Json(req): Json<RecallRequest>) -> Response {
    if req.agent_id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "agent_id must not be empty"})),
        )
            .into_response();
    }
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
    let hits = tier_router::recall(&req.agent_id, &req.query, req.k, req.min_hits);
    crate::memory::telemetry::record_recall(&req.agent_id, hits.len());
    let wire: Vec<_> = hits
        .into_iter()
        .map(|h| RecalledHit {
            tier: format!("{:?}", h.tier).to_lowercase(),
            topic: h.topic,
            offset: h.offset,
            content: h.content,
            score: h.score,
        })
        .collect();
    (StatusCode::OK, Json(RecallResponse { hits: wire })).into_response()
}

pub(crate) async fn stats_handler(Path(agent_id): Path<String>) -> Response {
    if agent_id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "agent_id must not be empty"})),
        )
            .into_response();
    }

    let metrics = crate::memory::telemetry::get();
    let (recalls, remembers) = metrics.agent_stats(&agent_id);

    let episodic = {
        let topic = format!("__mem.{}.{}", agent_id, Tier::Episodic.topic_suffix());
        tier_router::content_store_entries(&topic).len() as u64
    };
    let semantic = {
        let topic = format!("__mem.{}.{}", agent_id, Tier::Semantic.topic_suffix());
        tier_router::content_store_entries(&topic).len() as u64
    };
    let procedural = {
        let topic = format!("__mem.{}.{}", agent_id, Tier::Procedural.topic_suffix());
        tier_router::content_store_entries(&topic).len() as u64
    };

    let resp = StatsResponse {
        agent_id,
        recall_total: recalls,
        remember_total: remembers,
        tiers: TierCounts {
            episodic,
            semantic,
            procedural,
        },
    };
    (StatusCode::OK, Json(resp)).into_response()
}

pub(crate) async fn export_handler(
    Path(agent_id): Path<String>,
    Json(req): Json<ExportRequest>,
) -> Response {
    if agent_id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "agent_id must not be empty"})),
        )
            .into_response();
    }

    let format = match req.format.as_deref().unwrap_or("jsonl") {
        "jsonl" => ExportFormat::Jsonl,
        "csv" => ExportFormat::Csv,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({"error": "format must be 'jsonl' or 'csv'"})),
            )
                .into_response();
        }
    };

    let tier = match req.tier {
        Some(ref t) => match parse_tier(t) {
            Ok(tier) => Some(tier),
            Err(msg) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": msg})),
                )
                    .into_response();
            }
        },
        None => None,
    };

    let config = ExportConfig {
        agent_id,
        since: req.since,
        until: req.until,
        tier,
        format,
    };

    match crate::memory::export::export_memories(&config) {
        Ok((lines, result)) => (
            StatusCode::OK,
            Json(ExportResponse {
                records_exported: result.records_exported,
                bytes_written: result.bytes_written,
                lines,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

pub(crate) async fn delete_handler(Path(agent_id): Path<String>) -> Response {
    if agent_id.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "agent_id must not be empty"})),
        )
            .into_response();
    }

    let request = GdprDeleteRequest {
        agent_id,
        tenant_id: "default".to_string(),
        requested_by: "api".to_string(),
    };

    match crate::memory::gdpr::delete_agent_data(&request) {
        Ok(result) => (
            StatusCode::OK,
            Json(DeleteResponse {
                deleted: true,
                topics_purged: result.topics_deleted,
                records_purged: result.records_purged,
            }),
        )
            .into_response(),
        Err(GdprDeleteError::InvalidRequest(msg)) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": msg})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}
