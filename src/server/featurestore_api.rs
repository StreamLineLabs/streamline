//! Feature Store API - REST endpoints for ML feature serving
//!
//! ## Endpoints
//!
//! - `POST /api/v1/feature-store/views` - Register a feature view
//! - `GET /api/v1/feature-store/views` - List all feature views
//! - `GET /api/v1/feature-store/views/:name` - Get feature view details
//! - `DELETE /api/v1/feature-store/views/:name` - Delete a feature view
//! - `POST /api/v1/feature-store/online` - Get online features
//! - `POST /api/v1/feature-store/historical` - Get historical features
//! - `POST /api/v1/feature-store/materialize/:view` - Trigger materialization
//! - `POST /api/v1/feature-store/ingest` - Ingest feature records
//! - `GET /api/v1/feature-store/stats` - Get feature store statistics

use crate::featurestore::engine::FeatureStoreEngine;
use crate::featurestore::feature::FeatureValue;
use crate::featurestore::feature_view::{
    AggregationWindow, AggregationType, FeatureDataType, FeatureDefinitionView,
    FeatureViewDefinition, FeatureViewEntity, WindowType,
};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Shared state for Feature Store API
#[derive(Clone)]
pub struct FeatureStoreApiState {
    pub engine: Arc<FeatureStoreEngine>,
}

/// Error response
#[derive(Debug, Serialize)]
pub struct FeatureStoreErrorResponse {
    pub error: String,
}

/// Create the Feature Store API router
pub fn create_featurestore_api_router(state: FeatureStoreApiState) -> Router {
    Router::new()
        .route(
            "/api/v1/feature-store/views",
            get(list_views).post(register_view),
        )
        .route(
            "/api/v1/feature-store/views/:name",
            get(get_view).delete(delete_view),
        )
        .route("/api/v1/feature-store/online", post(get_online_features))
        .route(
            "/api/v1/feature-store/historical",
            post(get_historical_features),
        )
        .route(
            "/api/v1/feature-store/materialize/:view",
            post(materialize),
        )
        .route("/api/v1/feature-store/ingest", post(ingest_features))
        .route("/api/v1/feature-store/stats", get(get_stats))
        .with_state(state)
}

// ── Request/Response Types ──

/// Request to register a feature view
#[derive(Debug, Deserialize)]
pub struct RegisterViewRequest {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub entities: Vec<EntityRequest>,
    #[serde(default)]
    pub features: Vec<FeatureRequest>,
    #[serde(default)]
    pub source_topic: Option<String>,
    #[serde(default)]
    pub transformation: Option<String>,
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
    #[serde(default)]
    pub aggregation_windows: Vec<AggregationWindowRequest>,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    #[serde(default)]
    pub owner: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct EntityRequest {
    pub name: String,
    #[serde(default)]
    pub join_key: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FeatureRequest {
    pub name: String,
    #[serde(default = "default_dtype")]
    pub dtype: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub aggregation: Option<String>,
}

fn default_dtype() -> String {
    "float64".to_string()
}

#[derive(Debug, Deserialize)]
pub struct AggregationWindowRequest {
    pub name: String,
    pub duration_seconds: u64,
    #[serde(default = "default_window_type")]
    pub window_type: String,
    #[serde(default)]
    pub slide_seconds: Option<u64>,
}

fn default_window_type() -> String {
    "tumbling".to_string()
}

/// Request to get online features
#[derive(Debug, Deserialize)]
pub struct OnlineFeaturesRequest {
    pub entity_key: String,
    pub features: Vec<String>,
}

/// Request to get historical features
#[derive(Debug, Deserialize)]
pub struct HistoricalFeaturesRequest {
    pub entity_keys: Vec<String>,
    pub timestamps: Vec<i64>,
}

/// Request to ingest features
#[derive(Debug, Deserialize)]
pub struct IngestRequest {
    pub feature_view: String,
    pub entity_key: String,
    pub features: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub event_timestamp: Option<i64>,
}

/// View summary for list response
#[derive(Debug, Serialize)]
pub struct ViewSummary {
    pub name: String,
    pub description: Option<String>,
    pub entities: Vec<String>,
    pub features: Vec<String>,
    pub source_topic: Option<String>,
    pub ttl_seconds: Option<u64>,
    pub created_at: i64,
}

// ── Handler Functions ──

/// POST /api/v1/feature-store/views - Register a feature view
async fn register_view(
    State(state): State<FeatureStoreApiState>,
    Json(req): Json<RegisterViewRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<FeatureStoreErrorResponse>)> {
    let mut view = FeatureViewDefinition::new(&req.name);

    if let Some(desc) = &req.description {
        view = view.with_description(desc);
    }

    for entity in &req.entities {
        let mut e = FeatureViewEntity::new(&entity.name);
        if let Some(jk) = &entity.join_key {
            e = e.with_join_key(jk);
        }
        if let Some(desc) = &entity.description {
            e = e.with_description(desc);
        }
        view = view.with_entity(e);
    }

    for feature in &req.features {
        let dtype = parse_dtype(&feature.dtype);
        let mut f = FeatureDefinitionView::new(&feature.name, dtype);
        if let Some(desc) = &feature.description {
            f = f.with_description(desc);
        }
        if let Some(agg) = &feature.aggregation {
            if let Some(agg_type) = parse_aggregation(agg) {
                f = f.with_aggregation(agg_type);
            }
        }
        view = view.with_feature(f);
    }

    if let Some(topic) = &req.source_topic {
        view = view.with_source_topic(topic);
    }

    if let Some(transformation) = &req.transformation {
        view = view.with_transformation(transformation);
    }

    if let Some(ttl) = req.ttl_seconds {
        view = view.with_ttl_seconds(ttl);
    }

    for window in &req.aggregation_windows {
        let wt = parse_window_type(&window.window_type);
        let aw = AggregationWindow {
            name: window.name.clone(),
            duration_seconds: window.duration_seconds,
            window_type: wt,
            slide_seconds: window.slide_seconds,
        };
        view = view.with_aggregation_window(aw);
    }

    for (k, v) in &req.tags {
        view = view.with_tag(k, v);
    }

    if let Some(owner) = &req.owner {
        view = view.with_owner(owner);
    }

    state.engine.register_feature_view(view).await.map_err(|e| {
        (
            StatusCode::CONFLICT,
            Json(FeatureStoreErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "status": "created",
            "name": req.name,
        })),
    ))
}

/// GET /api/v1/feature-store/views - List all feature views
async fn list_views(State(state): State<FeatureStoreApiState>) -> Json<serde_json::Value> {
    let views = state.engine.list_feature_views().await;
    let summaries: Vec<ViewSummary> = views
        .iter()
        .map(|v| ViewSummary {
            name: v.name.clone(),
            description: v.description.clone(),
            entities: v.entities.iter().map(|e| e.name.clone()).collect(),
            features: v.features.iter().map(|f| f.name.clone()).collect(),
            source_topic: v.source_topic.clone(),
            ttl_seconds: v.ttl_seconds,
            created_at: v.created_at,
        })
        .collect();

    Json(serde_json::json!({
        "views": summaries,
        "total": summaries.len(),
    }))
}

/// GET /api/v1/feature-store/views/:name - Get feature view details
async fn get_view(
    State(state): State<FeatureStoreApiState>,
    Path(name): Path<String>,
) -> Result<Json<FeatureViewDefinition>, (StatusCode, Json<FeatureStoreErrorResponse>)> {
    state
        .engine
        .get_feature_view(&name)
        .await
        .map(Json)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(FeatureStoreErrorResponse {
                    error: format!("Feature view '{}' not found", name),
                }),
            )
        })
}

/// DELETE /api/v1/feature-store/views/:name - Delete a feature view
async fn delete_view(
    State(state): State<FeatureStoreApiState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<FeatureStoreErrorResponse>)> {
    state.engine.delete_feature_view(&name).await.map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(FeatureStoreErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

/// POST /api/v1/feature-store/online - Get online features
async fn get_online_features(
    State(state): State<FeatureStoreApiState>,
    Json(req): Json<OnlineFeaturesRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<FeatureStoreErrorResponse>)> {
    let result = state
        .engine
        .get_online_features(&req.entity_key, &req.features)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(FeatureStoreErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;

    Ok(Json(serde_json::json!({
        "entity_key": result.entity_key,
        "features": result.features,
        "found": result.found_features,
        "missing": result.missing_features,
        "latency_us": result.latency_us,
    })))
}

/// POST /api/v1/feature-store/historical - Get historical features
async fn get_historical_features(
    State(state): State<FeatureStoreApiState>,
    Json(req): Json<HistoricalFeaturesRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<FeatureStoreErrorResponse>)> {
    let results = state
        .engine
        .get_historical_features(&req.entity_keys, &req.timestamps)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(FeatureStoreErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;

    Ok(Json(serde_json::json!({
        "results": results,
        "total": results.len(),
    })))
}

/// POST /api/v1/feature-store/materialize/:view - Trigger materialization
async fn materialize(
    State(state): State<FeatureStoreApiState>,
    Path(view): Path<String>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<FeatureStoreErrorResponse>)> {
    let result = state.engine.materialize(&view).await.map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(FeatureStoreErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;

    Ok(Json(serde_json::json!({
        "feature_view": result.feature_view,
        "records_written": result.records_written,
        "duration_ms": result.duration_ms,
    })))
}

/// POST /api/v1/feature-store/ingest - Ingest feature records
async fn ingest_features(
    State(state): State<FeatureStoreApiState>,
    Json(req): Json<IngestRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<FeatureStoreErrorResponse>)> {
    let event_timestamp = req
        .event_timestamp
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());

    // Convert JSON values to FeatureValues
    let features: HashMap<String, FeatureValue> = req
        .features
        .into_iter()
        .map(|(k, v)| {
            let fv = json_to_feature_value(&v);
            (k, fv)
        })
        .collect();

    state
        .engine
        .ingest(&req.feature_view, &req.entity_key, features, event_timestamp)
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(FeatureStoreErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "status": "ingested",
            "feature_view": req.feature_view,
            "entity_key": req.entity_key,
            "event_timestamp": event_timestamp,
        })),
    ))
}

/// GET /api/v1/feature-store/stats - Get feature store statistics
async fn get_stats(State(state): State<FeatureStoreApiState>) -> Json<serde_json::Value> {
    let stats = state.engine.stats().await;
    Json(serde_json::json!({
        "feature_views": stats.feature_views,
        "online_store_entries": stats.online_store_entries,
        "offline_store_entries": stats.offline_store_entries,
        "online_store_evictions": stats.online_store_evictions,
    }))
}

// ── Helper Functions ──

fn parse_dtype(s: &str) -> FeatureDataType {
    match s.to_lowercase().as_str() {
        "bool" | "boolean" => FeatureDataType::Bool,
        "int64" | "int" | "integer" | "long" => FeatureDataType::Int64,
        "float64" | "float" | "double" => FeatureDataType::Float64,
        "string" | "str" | "text" => FeatureDataType::String,
        "timestamp" | "datetime" => FeatureDataType::Timestamp,
        _ => FeatureDataType::Float64,
    }
}

fn parse_aggregation(s: &str) -> Option<AggregationType> {
    match s.to_lowercase().as_str() {
        "sum" => Some(AggregationType::Sum),
        "avg" | "average" | "mean" => Some(AggregationType::Avg),
        "count" => Some(AggregationType::Count),
        "min" | "minimum" => Some(AggregationType::Min),
        "max" | "maximum" => Some(AggregationType::Max),
        "last" => Some(AggregationType::Last),
        "first" => Some(AggregationType::First),
        _ => None,
    }
}

fn parse_window_type(s: &str) -> WindowType {
    match s.to_lowercase().as_str() {
        "sliding" => WindowType::Sliding,
        "session" => WindowType::Session,
        _ => WindowType::Tumbling,
    }
}

fn json_to_feature_value(v: &serde_json::Value) -> FeatureValue {
    match v {
        serde_json::Value::Null => FeatureValue::Null,
        serde_json::Value::Bool(b) => FeatureValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                FeatureValue::Int64(i)
            } else if let Some(f) = n.as_f64() {
                FeatureValue::Float64(f)
            } else {
                FeatureValue::Null
            }
        }
        serde_json::Value::String(s) => FeatureValue::String(s.clone()),
        _ => FeatureValue::Json(v.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dtype() {
        assert_eq!(parse_dtype("float64"), FeatureDataType::Float64);
        assert_eq!(parse_dtype("int64"), FeatureDataType::Int64);
        assert_eq!(parse_dtype("string"), FeatureDataType::String);
        assert_eq!(parse_dtype("bool"), FeatureDataType::Bool);
        assert_eq!(parse_dtype("timestamp"), FeatureDataType::Timestamp);
        assert_eq!(parse_dtype("unknown"), FeatureDataType::Float64); // default
    }

    #[test]
    fn test_parse_aggregation() {
        assert_eq!(parse_aggregation("sum"), Some(AggregationType::Sum));
        assert_eq!(parse_aggregation("avg"), Some(AggregationType::Avg));
        assert_eq!(parse_aggregation("count"), Some(AggregationType::Count));
        assert_eq!(parse_aggregation("min"), Some(AggregationType::Min));
        assert_eq!(parse_aggregation("max"), Some(AggregationType::Max));
        assert_eq!(parse_aggregation("last"), Some(AggregationType::Last));
        assert_eq!(parse_aggregation("first"), Some(AggregationType::First));
        assert_eq!(parse_aggregation("unknown"), None);
    }

    #[test]
    fn test_parse_window_type() {
        assert_eq!(parse_window_type("tumbling"), WindowType::Tumbling);
        assert_eq!(parse_window_type("sliding"), WindowType::Sliding);
        assert_eq!(parse_window_type("session"), WindowType::Session);
        assert_eq!(parse_window_type("unknown"), WindowType::Tumbling); // default
    }

    #[test]
    fn test_json_to_feature_value() {
        assert_eq!(
            json_to_feature_value(&serde_json::json!(null)),
            FeatureValue::Null
        );
        assert_eq!(
            json_to_feature_value(&serde_json::json!(true)),
            FeatureValue::Bool(true)
        );
        assert_eq!(
            json_to_feature_value(&serde_json::json!(42)),
            FeatureValue::Int64(42)
        );
        assert_eq!(
            json_to_feature_value(&serde_json::json!(3.14)),
            FeatureValue::Float64(3.14)
        );
        assert_eq!(
            json_to_feature_value(&serde_json::json!("hello")),
            FeatureValue::String("hello".to_string())
        );
    }
}
