//! HTTP routes for the web UI.

use super::client::{
    AlertConfig, BenchmarkConfig, ClientError, CreateTopicRequest, ProduceMessageRequest,
    ProduceRecord, ResetOffsetsRequest, SearchResponse, SearchResult, SearchResultType,
};
use super::sse::SseEvent;
use super::WebUiState;
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, StatusCode},
    response::{Html, IntoResponse, Response, Sse},
    routing::{delete, get, post},
    Json, Router,
};
use futures_util::stream::Stream;
use serde::Deserialize;
use std::convert::Infallible;
use std::time::Duration;

/// Query parameters for message browsing.
#[derive(Debug, Deserialize)]
pub struct BrowseQuery {
    #[serde(default)]
    pub partition: i32,
    #[serde(default)]
    pub offset: i64,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

fn default_limit() -> usize {
    20
}

/// Create the web UI router.
pub fn create_router(state: WebUiState) -> Router {
    Router::new()
        // Page routes
        .route("/", get(dashboard_page))
        .route("/topics", get(topics_page))
        .route("/topics/:name", get(topic_details_page))
        .route("/topics/:name/browse", get(topic_browse_page))
        .route("/topics/:name/produce", get(topic_produce_page))
        .route("/topics/:name/config", get(topic_config_page))
        .route("/consumer-groups", get(consumer_groups_page))
        .route("/consumer-groups/:id", get(consumer_group_details_page))
        .route(
            "/consumer-groups/:id/offsets",
            get(consumer_group_offsets_page),
        )
        .route("/brokers", get(brokers_page))
        .route("/metrics", get(metrics_page))
        .route("/lag-heatmap", get(lag_heatmap_page))
        .route("/logs", get(logs_page))
        .route("/connections", get(connections_page))
        .route("/alerts", get(alerts_page))
        .route("/topics/compare", get(topic_compare_page))
        .route("/benchmark", get(benchmark_page))
        // API routes (proxy to Streamline)
        .route("/api/topics", get(api_list_topics))
        .route("/api/topics", post(api_create_topic))
        .route("/api/topics/:name", delete(api_delete_topic))
        .route("/api/topics/:name/messages", post(api_produce_message))
        .route(
            "/api/topics/:name/partitions/:partition/messages",
            get(api_browse_messages),
        )
        .route("/api/topics/:name/timeline", get(api_get_topic_timeline))
        .route("/api/consumer-groups", get(api_list_consumer_groups))
        .route(
            "/api/consumer-groups/:id/reset-offsets",
            post(api_reset_offsets),
        )
        .route(
            "/api/consumer-groups/:id/reset-offsets/dry-run",
            post(api_reset_offsets_dry_run),
        )
        .route("/api/metrics", get(api_get_metrics))
        .route("/api/events", get(api_events_stream))
        // Benchmark API proxy routes
        .route("/api/v1/benchmark", get(api_list_benchmarks))
        .route(
            "/api/v1/benchmark/produce",
            post(api_start_produce_benchmark),
        )
        .route(
            "/api/v1/benchmark/consume",
            post(api_start_consume_benchmark),
        )
        .route(
            "/api/v1/benchmark/:id/status",
            get(api_get_benchmark_status),
        )
        // Connections API proxy routes
        .route("/api/v1/connections", get(api_get_connections))
        .route("/api/v1/connections/stats", get(api_get_connection_stats))
        // Logs API proxy routes
        .route("/api/v1/logs", get(api_get_logs).delete(api_clear_logs))
        .route("/api/v1/logs/search", get(api_search_logs))
        .route("/api/v1/logs/stream", get(api_logs_stream))
        // Alerts API proxy routes
        .route(
            "/api/v1/alerts",
            get(api_list_alerts).post(api_create_alert),
        )
        .route("/api/v1/alerts/stats", get(api_get_alert_stats))
        .route("/api/v1/alerts/history", get(api_get_alert_history))
        .route(
            "/api/v1/alerts/:id",
            get(api_get_alert)
                .put(api_update_alert)
                .delete(api_delete_alert),
        )
        .route("/api/v1/alerts/:id/toggle", post(api_toggle_alert))
        // Audit log routes
        .route("/audit", get(audit_log_page))
        .route("/api/v1/audit", get(api_get_audit_log))
        // DLQ routes
        .route("/dlq", get(dlq_dashboard_page))
        .route("/dlq/:topic", get(dlq_topic_page))
        .route("/api/v1/dlq", get(api_get_dlq_topics))
        .route("/api/v1/dlq/:topic/messages", get(api_get_dlq_messages))
        .route("/api/v1/dlq/:topic/retry", post(api_dlq_retry))
        .route("/api/v1/dlq/:topic/purge", post(api_dlq_purge))
        // Consumer Lag History routes
        .route(
            "/api/consumer-groups/:id/lag/history",
            get(api_get_lag_history),
        )
        .route(
            "/api/consumer-groups/:id/lag/history/export",
            get(api_export_lag_history),
        )
        // Schema Evolution routes
        .route("/schemas", get(schema_list_page))
        .route("/schemas/:subject", get(schema_details_page))
        .route("/schemas/:subject/diff/:v1/:v2", get(schema_diff_page))
        .route("/api/schemas", get(api_list_schemas))
        .route("/api/schemas/:subject", get(api_get_schema_subject))
        .route(
            "/api/schemas/:subject/diff/:v1/:v2",
            get(api_get_schema_diff),
        )
        // Multi-Cluster Management routes
        .route("/clusters", get(clusters_page))
        .route(
            "/api/clusters",
            get(api_list_clusters).post(api_add_cluster),
        )
        .route(
            "/api/clusters/:id",
            get(api_get_cluster)
                .put(api_update_cluster)
                .delete(api_delete_cluster),
        )
        .route("/api/clusters/:id/test", post(api_test_cluster))
        .route("/api/clusters/:id/health", get(api_get_cluster_health))
        .route("/api/clusters/compare", get(api_compare_clusters))
        // Partition Rebalancing routes
        .route("/rebalances", get(rebalances_page))
        .route("/rebalances/:group_id", get(rebalance_group_page))
        .route("/api/rebalances", get(api_list_rebalances))
        .route("/api/rebalances/:group_id", get(api_get_group_rebalances))
        .route(
            "/api/rebalances/:group_id/current",
            get(api_get_current_rebalance),
        )
        .route(
            "/api/rebalances/:group_id/assignments",
            get(api_get_group_assignments),
        )
        .route(
            "/api/rebalances/:group_id/diff/:gen1/:gen2",
            get(api_get_assignment_diff),
        )
        // Command palette search API
        .route("/api/search", get(api_search))
        // CDC (Change Data Capture) routes
        .route("/cdc", get(cdc_dashboard_page))
        .route("/cdc/sources", get(cdc_sources_page))
        .route("/cdc/sources/:name", get(cdc_source_details_page))
        .route("/api/v1/cdc/sources", get(api_list_cdc_sources))
        .route("/api/v1/cdc/sources/:name", get(api_get_cdc_source))
        .route(
            "/api/v1/cdc/sources/:name/metrics",
            get(api_get_cdc_metrics),
        )
        // AI-Native Streaming routes
        .route("/ai", get(ai_dashboard_page))
        .route("/ai/pipelines", get(ai_pipelines_page))
        .route("/ai/search", get(ai_search_page))
        .route("/ai/anomalies", get(ai_anomalies_page))
        .route("/api/v1/ai/pipelines", get(api_list_ai_pipelines))
        .route("/api/v1/ai/search", post(api_ai_semantic_search))
        .route("/api/v1/ai/anomalies/:topic", get(api_get_anomalies))
        .route(
            "/api/v1/ai/embeddings/:topic",
            get(api_get_embeddings_status),
        )
        // Edge-First Architecture routes
        .route("/edge", get(edge_dashboard_page))
        .route("/edge/nodes", get(edge_nodes_page))
        .route("/edge/sync", get(edge_sync_page))
        .route("/edge/conflicts", get(edge_conflicts_page))
        .route("/api/v1/edge/nodes", get(api_list_edge_nodes))
        .route("/api/v1/edge/sync/status", get(api_get_edge_sync_status))
        .route("/api/v1/edge/conflicts", get(api_list_edge_conflicts))
        // Lakehouse-Native Streaming routes
        .route("/lakehouse", get(lakehouse_dashboard_page))
        .route("/lakehouse/views", get(lakehouse_views_page))
        .route("/lakehouse/exports", get(lakehouse_exports_page))
        .route("/lakehouse/query", get(lakehouse_query_page))
        .route("/api/v1/lakehouse/views", get(api_list_materialized_views))
        .route("/api/v1/lakehouse/exports", get(api_list_exports))
        .route("/api/v1/lakehouse/statistics", get(api_get_lakehouse_stats))
        // Integration Pipeline routes
        .route("/pipelines", get(pipelines_dashboard_page))
        .route("/pipelines/:name", get(pipeline_details_page))
        .route("/api/v1/pipelines", get(api_list_pipelines))
        .route("/api/v1/pipelines/:name", get(api_get_pipeline))
        .route(
            "/api/v1/pipelines/:name/metrics",
            get(api_get_pipeline_metrics),
        )
        // Health check
        .route("/health", get(health_check))
        // System settings and plugins
        .route("/settings", get(settings_page))
        .route("/settings/plugins", get(plugins_page))
        // StreamQL Query Editor
        .route("/streamql", get(streamql_editor_page))
        .route("/streamql/history", get(streamql_history_page))
        .route("/api/v1/streamql/execute", post(api_execute_streamql))
        .route("/api/v1/streamql/validate", post(api_validate_streamql))
        .route("/api/v1/streamql/history", get(api_get_streamql_history))
        .route("/api/v1/streamql/views", get(api_list_materialized_views))
        // Message Inspector (deep-dive into individual messages)
        .route("/messages/inspect", get(message_inspector_page))
        .route("/api/v1/messages/decode", post(api_decode_message))
        .route("/api/v1/messages/search", get(api_search_messages))
        // Connector Management UI
        .route("/connectors", get(connectors_page))
        .route("/connectors/:name", get(connector_details_page))
        .route("/api/v1/connectors", get(api_list_connectors_ui).post(api_create_connector_ui))
        .route("/api/v1/connectors/:name", get(api_get_connector_ui).delete(api_delete_connector_ui))
        .route("/api/v1/connectors/:name/restart", post(api_restart_connector_ui))
        // SPA shell for client-side routing (returns the same HTML for all /app/* paths)
        .route("/app", get(spa_shell_page))
        .route("/app/*path", get(spa_shell_page))
        // Canvas-based lag heatmap data API
        .route("/api/v1/lag-heatmap/data", get(api_lag_heatmap_data))
        .route("/api/v1/topology", get(api_cluster_topology))
        // Static assets
        .route("/static/css/main.css", get(static_css))
        .route("/static/js/main.js", get(static_js))
        // Favicon
        .route("/favicon.ico", get(favicon))
        .with_state(state)
}

// Page handlers

async fn dashboard_page(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.get_cluster_overview().await {
        Ok(overview) => Html(state.templates.dashboard(&overview)),
        Err(e) => {
            let html = state.templates.error_page(
                "Connection Error",
                &format!("Failed to connect to Streamline: {}", e),
            );
            Html(html)
        }
    }
}

async fn topics_page(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.list_topics().await {
        Ok(topics) => Html(state.templates.topics_list(&topics)),
        Err(e) => {
            let html = state
                .templates
                .error_page("Error", &format!("Failed to load topics: {}", e));
            Html(html)
        }
    }
}

async fn topic_details_page(
    State(state): State<WebUiState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.client.get_topic(&name).await {
        Ok(topic) => Html(state.templates.topic_details(&topic)),
        Err(ClientError::ApiError(msg)) if msg.contains("404") => {
            let html = state
                .templates
                .error_page("Not Found", &format!("Topic '{}' not found", name));
            Html(html)
        }
        Err(e) => {
            let html = state
                .templates
                .error_page("Error", &format!("Failed to load topic: {}", e));
            Html(html)
        }
    }
}

async fn topic_browse_page(
    State(state): State<WebUiState>,
    Path(name): Path<String>,
    Query(query): Query<BrowseQuery>,
) -> impl IntoResponse {
    // Get topic details first to show partition info
    match state.client.get_topic(&name).await {
        Ok(topic) => {
            // Fetch messages from the selected partition
            let messages = state
                .client
                .browse_messages(&name, query.partition, query.offset, query.limit)
                .await
                .ok();
            Html(state.templates.topic_browse(
                &topic,
                messages.as_ref(),
                query.partition,
                query.offset,
            ))
        }
        Err(ClientError::ApiError(msg)) if msg.contains("404") => {
            let html = state
                .templates
                .error_page("Not Found", &format!("Topic '{}' not found", name));
            Html(html)
        }
        Err(e) => {
            let html = state
                .templates
                .error_page("Error", &format!("Failed to load topic: {}", e));
            Html(html)
        }
    }
}

async fn topic_produce_page(
    State(state): State<WebUiState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.client.get_topic(&name).await {
        Ok(topic) => Html(state.templates.topic_produce(&topic)),
        Err(ClientError::ApiError(msg)) if msg.contains("404") => {
            let html = state
                .templates
                .error_page("Not Found", &format!("Topic '{}' not found", name));
            Html(html)
        }
        Err(e) => {
            let html = state
                .templates
                .error_page("Error", &format!("Failed to load topic: {}", e));
            Html(html)
        }
    }
}

async fn topic_config_page(
    State(state): State<WebUiState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.client.get_topic(&name).await {
        Ok(topic) => Html(state.templates.topic_config(&topic)),
        Err(ClientError::ApiError(msg)) if msg.contains("404") => {
            let html = state
                .templates
                .error_page("Not Found", &format!("Topic '{}' not found", name));
            Html(html)
        }
        Err(e) => {
            let html = state
                .templates
                .error_page("Error", &format!("Failed to load topic: {}", e));
            Html(html)
        }
    }
}

async fn consumer_groups_page(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.list_consumer_groups().await {
        Ok(groups) => Html(state.templates.consumer_groups_list(&groups)),
        Err(e) => {
            let html = state
                .templates
                .error_page("Error", &format!("Failed to load consumer groups: {}", e));
            Html(html)
        }
    }
}

async fn consumer_group_details_page(
    State(state): State<WebUiState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.client.get_consumer_group(&id).await {
        Ok(group) => {
            // Also try to get lag data
            let lag = state.client.get_consumer_group_lag(&id).await.ok();
            Html(state.templates.consumer_group_details(&group, lag.as_ref()))
        }
        Err(ClientError::ApiError(msg)) if msg.contains("404") => {
            let html = state
                .templates
                .error_page("Not Found", &format!("Consumer group '{}' not found", id));
            Html(html)
        }
        Err(e) => {
            let html = state
                .templates
                .error_page("Error", &format!("Failed to load consumer group: {}", e));
            Html(html)
        }
    }
}

async fn consumer_group_offsets_page(
    State(state): State<WebUiState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.client.get_consumer_group(&id).await {
        Ok(group) => {
            let lag = state.client.get_consumer_group_lag(&id).await.ok();
            let topics = state.client.list_topics().await.ok().unwrap_or_default();
            Html(
                state
                    .templates
                    .consumer_group_offsets(&group, lag.as_ref(), &topics),
            )
        }
        Err(ClientError::ApiError(msg)) if msg.contains("404") => {
            let html = state
                .templates
                .error_page("Not Found", &format!("Consumer group '{}' not found", id));
            Html(html)
        }
        Err(e) => {
            let html = state
                .templates
                .error_page("Error", &format!("Failed to load consumer group: {}", e));
            Html(html)
        }
    }
}

async fn brokers_page(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.list_brokers().await {
        Ok(brokers) => Html(state.templates.brokers_list(&brokers)),
        Err(e) => {
            let html = state
                .templates
                .error_page("Error", &format!("Failed to load brokers: {}", e));
            Html(html)
        }
    }
}

async fn metrics_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.metrics_page())
}

async fn lag_heatmap_page(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.get_heatmap().await {
        Ok(heatmap) => Html(state.templates.lag_heatmap(&heatmap)),
        Err(e) => {
            let html = state
                .templates
                .error_page("Error", &format!("Failed to load heatmap data: {}", e));
            Html(html)
        }
    }
}

async fn logs_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.logs_viewer())
}

async fn connections_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.connections())
}

async fn alerts_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.alerts())
}

async fn audit_log_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.audit_log())
}

/// Query parameters for topic comparison page.
#[derive(Debug, Deserialize)]
pub struct CompareQuery {
    #[serde(default)]
    pub topic1: Option<String>,
    #[serde(default)]
    pub topic2: Option<String>,
}

async fn topic_compare_page(
    State(state): State<WebUiState>,
    Query(query): Query<CompareQuery>,
) -> impl IntoResponse {
    // Get list of topics for the selector dropdowns
    let topics = state.client.list_topics().await.ok().unwrap_or_default();

    // If both topics are selected, load comparison data
    let comparison = match (&query.topic1, &query.topic2) {
        (Some(t1), Some(t2)) if !t1.is_empty() && !t2.is_empty() => {
            state.client.compare_topics(t1, t2).await.ok()
        }
        _ => None,
    };

    Html(state.templates.topic_compare(
        &topics,
        comparison.as_ref(),
        query.topic1.as_deref(),
        query.topic2.as_deref(),
    ))
}

async fn benchmark_page(State(state): State<WebUiState>) -> impl IntoResponse {
    // Get list of topics for the topic selector
    let topics = state.client.list_topics().await.ok().unwrap_or_default();
    // Get list of benchmarks
    let benchmarks = state.client.list_benchmarks().await.ok();
    Html(state.templates.benchmark(&topics, benchmarks.as_ref()))
}

// API handlers (proxy to Streamline)

async fn api_list_topics(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.list_topics().await {
        Ok(topics) => Json(topics).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

async fn api_create_topic(
    State(state): State<WebUiState>,
    Json(request): Json<CreateTopicRequest>,
) -> impl IntoResponse {
    match state.client.create_topic(request).await {
        Ok(()) => StatusCode::CREATED.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn api_delete_topic(
    State(state): State<WebUiState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    match state.client.delete_topic(&name).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

/// Request body for producing a message from the UI.
#[derive(Debug, Deserialize)]
pub struct UiProduceRequest {
    pub key: Option<String>,
    pub value: String,
    pub partition: Option<i32>,
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
}

async fn api_produce_message(
    State(state): State<WebUiState>,
    Path(name): Path<String>,
    Json(request): Json<UiProduceRequest>,
) -> impl IntoResponse {
    // Parse the value as JSON or use as string
    let value = match serde_json::from_str::<serde_json::Value>(&request.value) {
        Ok(v) => v,
        Err(_) => serde_json::Value::String(request.value),
    };

    let produce_request = ProduceMessageRequest {
        records: vec![ProduceRecord {
            key: request.key,
            value,
            partition: request.partition,
            headers: request.headers,
        }],
    };

    match state.client.produce_message(&name, produce_request).await {
        Ok(response) => Json(response).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn api_browse_messages(
    State(state): State<WebUiState>,
    Path((name, partition)): Path<(String, i32)>,
    Query(query): Query<BrowseQuery>,
) -> impl IntoResponse {
    match state
        .client
        .browse_messages(&name, partition, query.offset, query.limit)
        .await
    {
        Ok(response) => Json(response).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

async fn api_list_consumer_groups(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.list_consumer_groups().await {
        Ok(groups) => Json(groups).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

async fn api_reset_offsets(
    State(state): State<WebUiState>,
    Path(id): Path<String>,
    Json(request): Json<ResetOffsetsRequest>,
) -> impl IntoResponse {
    match state.client.reset_offsets(&id, request).await {
        Ok(response) => Json(response).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn api_reset_offsets_dry_run(
    State(state): State<WebUiState>,
    Path(id): Path<String>,
    Json(request): Json<ResetOffsetsRequest>,
) -> impl IntoResponse {
    match state.client.reset_offsets_dry_run(&id, request).await {
        Ok(response) => Json(response).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn api_get_metrics(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.get_metrics().await {
        Ok(metrics) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            metrics,
        )
            .into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

/// SSE event stream handler.
async fn api_events_stream(
    State(state): State<WebUiState>,
) -> Sse<impl Stream<Item = Result<axum::response::sse::Event, Infallible>>> {
    let (id, mut rx) = state.sse_manager.register().unwrap_or_else(|| {
        // If max connections reached, create a dummy channel
        let (tx, rx) = tokio::sync::broadcast::channel(1);
        drop(tx);
        (0, rx)
    });

    // Send cached events first
    let cached = state.sse_manager.get_cached_events();
    let sse_manager = state.sse_manager.clone();

    let stream = async_stream::stream! {
        // Send connection event
        let event = SseEvent::Connected { connection_id: id };
        yield Ok(axum::response::sse::Event::default()
            .event(event.event_name())
            .data(serde_json::to_string(&event).unwrap_or_default()));

        // Send cached events
        for event in cached {
            yield Ok(axum::response::sse::Event::default()
                .event(event.event_name())
                .data(serde_json::to_string(&event).unwrap_or_default()));
        }

        // Stream new events
        loop {
            match rx.recv().await {
                Ok(event) => {
                    yield Ok(axum::response::sse::Event::default()
                        .event(event.event_name())
                        .data(serde_json::to_string(&event).unwrap_or_default()));
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // Skip lagged events
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }

        // Cleanup on disconnect
        sse_manager.unregister(id);
    };

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    )
}

// Benchmark API proxy handlers

async fn api_list_benchmarks(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.list_benchmarks().await {
        Ok(benchmarks) => Json(benchmarks).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

async fn api_start_produce_benchmark(
    State(state): State<WebUiState>,
    Json(config): Json<BenchmarkConfig>,
) -> impl IntoResponse {
    match state.client.start_produce_benchmark(config).await {
        Ok(benchmark) => Json(benchmark).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn api_start_consume_benchmark(
    State(state): State<WebUiState>,
    Json(config): Json<BenchmarkConfig>,
) -> impl IntoResponse {
    match state.client.start_consume_benchmark(config).await {
        Ok(benchmark) => Json(benchmark).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn api_get_benchmark_status(
    State(state): State<WebUiState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.client.get_benchmark_status(&id).await {
        Ok(benchmark) => Json(benchmark).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

// Connections API proxy handlers

async fn api_get_connections(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.get_connections().await {
        Ok(connections) => Json(connections).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

async fn api_get_connection_stats(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.get_connection_stats().await {
        Ok(stats) => Json(stats).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

// Logs API proxy handlers

/// Query parameters for log retrieval
#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default = "default_log_limit")]
    pub limit: usize,
    #[serde(default)]
    pub after: Option<u64>,
}

fn default_log_limit() -> usize {
    100
}

/// Query parameters for log search
#[derive(Debug, Deserialize)]
pub struct LogSearchQuery {
    pub q: String,
    #[serde(default)]
    pub level: Option<String>,
    #[serde(default = "default_log_limit")]
    pub limit: usize,
}

async fn api_get_logs(
    State(state): State<WebUiState>,
    Query(query): Query<LogsQuery>,
) -> impl IntoResponse {
    match state
        .client
        .get_logs(query.level.as_deref(), Some(query.limit), query.after)
        .await
    {
        Ok(logs) => Json(logs).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

async fn api_search_logs(
    State(state): State<WebUiState>,
    Query(query): Query<LogSearchQuery>,
) -> impl IntoResponse {
    match state
        .client
        .search_logs(&query.q, query.level.as_deref(), Some(query.limit))
        .await
    {
        Ok(logs) => Json(logs).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

async fn api_clear_logs(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.clear_logs().await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

/// Query parameters for log streaming
#[derive(Debug, Deserialize)]
pub struct LogStreamQuery {
    #[serde(default)]
    pub level: Option<String>,
}

async fn api_logs_stream(
    State(state): State<WebUiState>,
    Query(query): Query<LogStreamQuery>,
) -> Response {
    use futures_util::StreamExt;

    match state.client.get_logs_stream(query.level.as_deref()).await {
        Ok(response) => {
            // Stream the SSE response from Streamline server
            let stream = response
                .bytes_stream()
                .map(|result| result.map_err(std::io::Error::other));

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .header(header::CACHE_CONTROL, "no-cache")
                .header(header::CONNECTION, "keep-alive")
                .body(Body::from_stream(stream))
                .unwrap_or_else(|_| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Failed to create stream response",
                    )
                        .into_response()
                })
        }
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

// Alerts API proxy handlers

async fn api_list_alerts(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.list_alerts().await {
        Ok(alerts) => Json(alerts).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

async fn api_get_alert_stats(State(state): State<WebUiState>) -> impl IntoResponse {
    match state.client.get_alert_stats().await {
        Ok(stats) => Json(stats).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

/// Query parameters for alert history
#[derive(Debug, Deserialize)]
pub struct AlertHistoryQuery {
    #[serde(default = "default_alert_history_limit")]
    pub limit: usize,
}

fn default_alert_history_limit() -> usize {
    20
}

async fn api_get_alert_history(
    State(state): State<WebUiState>,
    Query(query): Query<AlertHistoryQuery>,
) -> impl IntoResponse {
    match state.client.get_alert_history(Some(query.limit)).await {
        Ok(history) => Json(history).into_response(),
        Err(e) => (StatusCode::BAD_GATEWAY, e.to_string()).into_response(),
    }
}

async fn api_get_alert(
    State(state): State<WebUiState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.client.get_alert(&id).await {
        Ok(alert) => Json(alert).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

async fn api_create_alert(
    State(state): State<WebUiState>,
    Json(config): Json<AlertConfig>,
) -> impl IntoResponse {
    match state.client.create_alert(config).await {
        Ok(alert) => Json(alert).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn api_update_alert(
    State(state): State<WebUiState>,
    Path(id): Path<String>,
    Json(config): Json<AlertConfig>,
) -> impl IntoResponse {
    match state.client.update_alert(&id, config).await {
        Ok(alert) => Json(alert).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn api_toggle_alert(
    State(state): State<WebUiState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.client.toggle_alert(&id).await {
        Ok(result) => Json(result).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

async fn api_delete_alert(
    State(state): State<WebUiState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match state.client.delete_alert(&id).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

// Command palette search API

/// Query parameters for search.
#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    /// Search query string.
    pub q: String,
    /// Optional entity type filter.
    #[serde(rename = "type")]
    pub entity_type: Option<String>,
}

/// Fuzzy search across topics, consumer groups, and brokers.
async fn api_search(
    State(state): State<WebUiState>,
    Query(query): Query<SearchQuery>,
) -> impl IntoResponse {
    let query_lower = query.q.to_lowercase();
    let mut results = Vec::new();

    // Define built-in actions (always available)
    let actions = vec![
        (
            "Create Topic",
            "/topics?action=create",
            "Create a new topic",
            "plus",
        ),
        ("View Metrics", "/metrics", "View cluster metrics", "chart"),
        (
            "Lag Heatmap",
            "/lag-heatmap",
            "Consumer lag heatmap",
            "grid",
        ),
        ("View Logs", "/logs", "View server logs", "file-text"),
        ("Manage Alerts", "/alerts", "Configure alerts", "bell"),
        (
            "Benchmark",
            "/benchmark",
            "Run performance benchmark",
            "zap",
        ),
        (
            "Connections",
            "/connections",
            "View active connections",
            "link",
        ),
    ];

    // Add matching actions
    if query.entity_type.is_none() || query.entity_type.as_deref() == Some("action") {
        for (name, url, desc, icon) in &actions {
            if name.to_lowercase().contains(&query_lower)
                || desc.to_lowercase().contains(&query_lower)
            {
                results.push(SearchResult {
                    result_type: SearchResultType::Action,
                    name: name.to_string(),
                    url: url.to_string(),
                    description: desc.to_string(),
                    icon: icon.to_string(),
                });
            }
        }
    }

    // Search topics
    if query.entity_type.is_none() || query.entity_type.as_deref() == Some("topic") {
        if let Ok(topics) = state.client.list_topics().await {
            for topic in topics {
                if topic.name.to_lowercase().contains(&query_lower) {
                    results.push(SearchResult {
                        result_type: SearchResultType::Topic,
                        name: topic.name.clone(),
                        url: format!("/topics/{}", topic.name),
                        description: format!(
                            "{} partitions, {} messages",
                            topic.partition_count, topic.total_messages
                        ),
                        icon: "database".to_string(),
                    });
                }
            }
        }
    }

    // Search consumer groups
    if query.entity_type.is_none() || query.entity_type.as_deref() == Some("group") {
        if let Ok(groups) = state.client.list_consumer_groups().await {
            for group in groups {
                if group.group_id.to_lowercase().contains(&query_lower)
                    || group.state.to_lowercase().contains(&query_lower)
                {
                    results.push(SearchResult {
                        result_type: SearchResultType::ConsumerGroup,
                        name: group.group_id.clone(),
                        url: format!("/consumer-groups/{}", group.group_id),
                        description: format!("{} ({} members)", group.state, group.member_count),
                        icon: "users".to_string(),
                    });
                }
            }
        }
    }

    // Search brokers
    if query.entity_type.is_none() || query.entity_type.as_deref() == Some("broker") {
        if let Ok(brokers) = state.client.list_brokers().await {
            for broker in brokers {
                let id_str = broker.node_id.to_string();
                if id_str.contains(&query_lower)
                    || broker.host.to_lowercase().contains(&query_lower)
                {
                    results.push(SearchResult {
                        result_type: SearchResultType::Broker,
                        name: format!("Broker {}", broker.node_id),
                        url: "/brokers".to_string(),
                        description: format!(
                            "{}:{} ({})",
                            broker.host,
                            broker.port,
                            if broker.is_controller {
                                "controller"
                            } else {
                                &broker.state
                            }
                        ),
                        icon: "server".to_string(),
                    });
                }
            }
        }
    }

    // Limit results
    results.truncate(20);

    Json(SearchResponse {
        results,
        query: query.q,
    })
}

// Audit log API

/// Query parameters for audit log.
#[derive(Debug, Deserialize)]
pub struct AuditLogQuery {
    #[serde(default)]
    pub page: Option<usize>,
    #[serde(default)]
    pub page_size: Option<usize>,
    #[serde(default)]
    pub event_type: Option<String>,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub search: Option<String>,
}

async fn api_get_audit_log(
    State(_state): State<WebUiState>,
    Query(query): Query<AuditLogQuery>,
) -> impl IntoResponse {
    use crate::ui::client::{AuditEventView, AuditLogResponse};

    let page = query.page.unwrap_or(1);
    let page_size = query.page_size.unwrap_or(50);

    // Generate sample audit events for demonstration
    // In a real implementation, this would read from the audit log file or database
    let mut events = vec![
        AuditEventView {
            timestamp: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
            event_type: "TOPIC_CREATE".to_string(),
            user: Some("admin".to_string()),
            resource: Some("test-topic".to_string()),
            client_ip: Some("127.0.0.1".to_string()),
            details: "Created topic with 3 partitions".to_string(),
        },
        AuditEventView {
            timestamp: (chrono::Utc::now() - chrono::Duration::minutes(5))
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            event_type: "AUTH_SUCCESS".to_string(),
            user: Some("admin".to_string()),
            resource: None,
            client_ip: Some("127.0.0.1".to_string()),
            details: "Authentication via SCRAM-SHA-256".to_string(),
        },
        AuditEventView {
            timestamp: (chrono::Utc::now() - chrono::Duration::minutes(10))
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            event_type: "CONFIG_CHANGE".to_string(),
            user: Some("admin".to_string()),
            resource: Some("server".to_string()),
            client_ip: Some("127.0.0.1".to_string()),
            details: "Changed retention.ms from 604800000 to 86400000".to_string(),
        },
        AuditEventView {
            timestamp: (chrono::Utc::now() - chrono::Duration::minutes(15))
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            event_type: "ACL_ALLOW".to_string(),
            user: Some("producer-user".to_string()),
            resource: Some("Topic:orders".to_string()),
            client_ip: Some("192.168.1.100".to_string()),
            details: "WRITE operation allowed".to_string(),
        },
        AuditEventView {
            timestamp: (chrono::Utc::now() - chrono::Duration::minutes(20))
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            event_type: "CONNECTION".to_string(),
            user: None,
            resource: None,
            client_ip: Some("192.168.1.50".to_string()),
            details: "New connection (TLS enabled)".to_string(),
        },
    ];

    // Filter by event type
    if let Some(ref event_type) = query.event_type {
        events.retain(|e| e.event_type == *event_type);
    }

    // Filter by user
    if let Some(ref user) = query.user {
        events.retain(|e| e.user.as_ref().map(|u| u.contains(user)).unwrap_or(false));
    }

    // Filter by search term
    if let Some(ref search) = query.search {
        let search_lower = search.to_lowercase();
        events.retain(|e| {
            e.details.to_lowercase().contains(&search_lower)
                || e.event_type.to_lowercase().contains(&search_lower)
                || e.user
                    .as_ref()
                    .map(|u| u.to_lowercase().contains(&search_lower))
                    .unwrap_or(false)
                || e.resource
                    .as_ref()
                    .map(|r| r.to_lowercase().contains(&search_lower))
                    .unwrap_or(false)
        });
    }

    let total = events.len();

    Json(AuditLogResponse {
        events,
        total,
        page,
        page_size,
    })
}

/// Query parameters for timeline API.
#[derive(Debug, Deserialize)]
pub struct TimelineQuery {
    /// Partition to get timeline for (default: all partitions).
    #[serde(default)]
    pub partition: Option<i32>,
    /// Number of buckets (default: 100).
    #[serde(default)]
    pub buckets: Option<usize>,
}

/// Get timeline data for a topic (Time Machine feature).
async fn api_get_topic_timeline(
    State(state): State<WebUiState>,
    Path(name): Path<String>,
    Query(query): Query<TimelineQuery>,
) -> impl IntoResponse {
    use crate::ui::client::{TimelineBucket, TimelineData};

    let num_buckets = query.buckets.unwrap_or(100).clamp(10, 500);

    // Get topic info from the client
    let topic_info = match state.client.get_topic(&name).await {
        Ok(info) => info,
        Err(e) => {
            // Check if it's a 404 error
            let error_str = e.to_string();
            if error_str.contains("404") || error_str.contains("not found") {
                return (
                    StatusCode::NOT_FOUND,
                    Json(serde_json::json!({ "error": "Topic not found" })),
                )
                    .into_response();
            }
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({ "error": error_str })),
            )
                .into_response();
        }
    };

    // Calculate timeline based on partition info
    // In a real implementation, this would query actual message timestamps
    let now = chrono::Utc::now();
    let one_day_ago = now - chrono::Duration::days(1);

    // Calculate bucket duration
    let total_duration_ms = (now - one_day_ago).num_milliseconds();
    let bucket_duration_ms = total_duration_ms / num_buckets as i64;

    // Generate buckets with realistic-looking data
    // In production, this would aggregate actual message timestamps
    let partitions: Vec<_> = topic_info.partitions.iter().collect();
    let total_messages: usize = partitions
        .iter()
        .filter(|p| query.partition.map_or(true, |pnum| p.partition_id == pnum))
        .map(|p| (p.end_offset - p.start_offset).max(0) as usize)
        .sum();

    let mut buckets = Vec::with_capacity(num_buckets);
    let messages_per_bucket = if num_buckets > 0 {
        total_messages / num_buckets
    } else {
        0
    };

    // Create time buckets
    for i in 0..num_buckets {
        let bucket_start =
            one_day_ago + chrono::Duration::milliseconds(bucket_duration_ms * i as i64);
        let bucket_start_ms = bucket_start.timestamp_millis();

        // Add some variance to make the visualization interesting
        let variance = ((i as f64 * 0.1).sin() * 0.5 + 0.5) * 2.0;
        let count = (messages_per_bucket as f64 * variance).round() as usize;

        // Calculate approximate offsets for this bucket
        let start_offset = (i * messages_per_bucket) as i64;
        let end_offset = start_offset + count as i64;

        buckets.push(TimelineBucket {
            timestamp: bucket_start.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
            timestamp_ms: bucket_start_ms,
            count,
            start_offset,
            end_offset,
        });
    }

    let response = TimelineData {
        topic: name,
        partition: query.partition,
        start_timestamp: one_day_ago.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        end_timestamp: now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        start_timestamp_ms: one_day_ago.timestamp_millis(),
        end_timestamp_ms: now.timestamp_millis(),
        total_messages,
        buckets,
        bucket_duration_ms,
    };

    Json(response).into_response()
}

// DLQ Dashboard handlers

/// DLQ dashboard page.
async fn dlq_dashboard_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.dlq_dashboard())
}

/// DLQ topic details page.
async fn dlq_topic_page(
    State(state): State<WebUiState>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    Html(state.templates.dlq_topic_details(&topic))
}

/// Get DLQ topics list.
async fn api_get_dlq_topics(State(_state): State<WebUiState>) -> impl IntoResponse {
    use crate::ui::client::{DlqListResponse, DlqStats, DlqTopic};
    use std::collections::HashMap;

    // Generate sample DLQ data for demonstration
    // In production, this would query the actual DLQ backend
    let mut error_counts1 = HashMap::new();
    error_counts1.insert("DeserializationFailure".to_string(), 45);
    error_counts1.insert("ProcessingError".to_string(), 23);

    let mut error_counts2 = HashMap::new();
    error_counts2.insert("SchemaMismatch".to_string(), 12);
    error_counts2.insert("TimeoutError".to_string(), 8);

    let mut error_counts3 = HashMap::new();
    error_counts3.insert("ProcessingError".to_string(), 156);
    error_counts3.insert("Unknown".to_string(), 34);

    let topics = vec![
        DlqTopic {
            name: "orders-dlq".to_string(),
            source_topic: Some("orders".to_string()),
            message_count: 68,
            error_counts: error_counts1,
            has_spike: true,
            last_message_time: Some(chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string()),
        },
        DlqTopic {
            name: "payments-dlq".to_string(),
            source_topic: Some("payments".to_string()),
            message_count: 20,
            error_counts: error_counts2,
            has_spike: false,
            last_message_time: Some(
                (chrono::Utc::now() - chrono::Duration::hours(2))
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string(),
            ),
        },
        DlqTopic {
            name: "events-dlq".to_string(),
            source_topic: Some("events".to_string()),
            message_count: 190,
            error_counts: error_counts3,
            has_spike: true,
            last_message_time: Some(
                (chrono::Utc::now() - chrono::Duration::minutes(15))
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string(),
            ),
        },
    ];

    let mut by_error_type = HashMap::new();
    by_error_type.insert("DeserializationFailure".to_string(), 45);
    by_error_type.insert("ProcessingError".to_string(), 179);
    by_error_type.insert("SchemaMismatch".to_string(), 12);
    by_error_type.insert("TimeoutError".to_string(), 8);
    by_error_type.insert("Unknown".to_string(), 34);

    let mut by_source = HashMap::new();
    by_source.insert("orders".to_string(), 68);
    by_source.insert("payments".to_string(), 20);
    by_source.insert("events".to_string(), 190);

    let stats = DlqStats {
        total_topics: 3,
        total_messages: 278,
        by_error_type,
        by_source,
        recent_count: 45,
    };

    Json(DlqListResponse { topics, stats })
}

/// Query parameters for DLQ messages.
#[derive(Debug, Deserialize)]
pub struct DlqMessagesQuery {
    #[serde(default)]
    pub offset: Option<i64>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub error_type: Option<String>,
}

/// Get messages from a DLQ topic.
async fn api_get_dlq_messages(
    State(_state): State<WebUiState>,
    Path(topic): Path<String>,
    Query(query): Query<DlqMessagesQuery>,
) -> impl IntoResponse {
    use crate::ui::client::DlqMessage;

    let _offset = query.offset.unwrap_or(0);
    let limit = query.limit.unwrap_or(20).clamp(1, 100);

    // Generate sample DLQ messages for demonstration
    let error_types = [
        "DeserializationFailure",
        "ProcessingError",
        "SchemaMismatch",
        "TimeoutError",
    ];
    let mut messages = Vec::new();

    for i in 0..limit {
        let error_type = error_types[i % error_types.len()];

        // Skip if filtering by error type and doesn't match
        if let Some(ref filter) = query.error_type {
            if error_type != filter {
                continue;
            }
        }

        messages.push(DlqMessage {
            offset: i as i64,
            error_type: error_type.to_string(),
            source_topic: topic.strip_suffix("-dlq").map(|s| s.to_string()),
            source_partition: Some((i % 3) as i32),
            source_offset: Some((i * 100) as i64),
            timestamp: (chrono::Utc::now() - chrono::Duration::minutes(i as i64 * 5))
                .format("%Y-%m-%d %H:%M:%S")
                .to_string(),
            error_details: match error_type {
                "DeserializationFailure" => {
                    "Failed to deserialize message: invalid JSON at position 42".to_string()
                }
                "ProcessingError" => "Consumer processing failed: NullPointerException".to_string(),
                "SchemaMismatch" => {
                    "Schema incompatible: expected field 'user_id' not found".to_string()
                }
                "TimeoutError" => "Processing timeout after 30000ms".to_string(),
                _ => "Unknown error occurred".to_string(),
            },
            original_key: Some(format!("key-{}", i)),
            original_value_preview: Some(format!(r#"{{"id": {}, "type": "test"}}"#, i)),
        });
    }

    Json(serde_json::json!({
        "topic": topic,
        "messages": messages,
        "total": messages.len(),
    }))
}

/// Retry messages in DLQ.
async fn api_dlq_retry(
    State(_state): State<WebUiState>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    // In production, this would retry messages from the DLQ
    Json(serde_json::json!({
        "success": true,
        "topic": topic,
        "retried_count": 10,
        "message": "Messages queued for retry"
    }))
}

/// Purge messages from DLQ.
async fn api_dlq_purge(
    State(_state): State<WebUiState>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    // In production, this would purge messages from the DLQ
    Json(serde_json::json!({
        "success": true,
        "topic": topic,
        "purged_count": 25,
        "message": "Messages purged from DLQ"
    }))
}

// Consumer Lag History handlers

/// Query parameters for lag history.
#[derive(Debug, Deserialize)]
pub struct LagHistoryQuery {
    /// Time range: 1h, 6h, 24h, 7d.
    #[serde(default = "default_lag_range")]
    pub range: String,
}

fn default_lag_range() -> String {
    "1h".to_string()
}

/// Get lag history for a consumer group.
async fn api_get_lag_history(
    State(_state): State<WebUiState>,
    Path(group_id): Path<String>,
    Query(query): Query<LagHistoryQuery>,
) -> impl IntoResponse {
    use crate::ui::client::{LagHistoryPoint, LagHistoryResponse, LagVelocity};

    // Parse time range
    let (duration_hours, num_points) = match query.range.as_str() {
        "1h" => (1, 60),
        "6h" => (6, 72),
        "24h" => (24, 96),
        "7d" => (168, 168),
        _ => (1, 60),
    };

    let now = chrono::Utc::now();
    let start = now - chrono::Duration::hours(duration_hours);

    // Generate sample lag history data for demonstration
    // In production, this would read from actual metrics storage
    let mut history = Vec::with_capacity(num_points);
    let interval_ms = (duration_hours * 3600 * 1000) / num_points as i64;

    // Generate realistic-looking lag pattern
    let base_lag = 500i64;

    for i in 0..num_points {
        let timestamp = start + chrono::Duration::milliseconds(interval_ms * i as i64);

        // Simulate some variation in lag
        let variation = ((i as f64 * 0.1).sin() * 200.0
            + (i as f64 * 0.05).cos() * 100.0
            + (rand_simple(i) * 50.0)) as i64;
        let current_lag = (base_lag + variation).max(0);

        // Determine velocity based on trend
        let velocity = if i > 0 {
            let prev_lag = history
                .last()
                .map(|p: &LagHistoryPoint| p.lag)
                .unwrap_or(current_lag);
            if current_lag > prev_lag + 50 {
                LagVelocity::Growing
            } else if current_lag < prev_lag - 50 {
                LagVelocity::Shrinking
            } else {
                LagVelocity::Stable
            }
        } else {
            LagVelocity::Stable
        };

        history.push(LagHistoryPoint {
            timestamp: timestamp.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
            timestamp_ms: timestamp.timestamp_millis(),
            lag: current_lag,
            velocity,
            consume_rate: 100.0 + (rand_simple(i + 100) * 50.0),
            produce_rate: 100.0 + (rand_simple(i + 200) * 50.0),
        });
    }

    // Generate per-partition sparklines
    let partitions = vec![
        generate_partition_history("orders", 0, num_points),
        generate_partition_history("orders", 1, num_points),
        generate_partition_history("orders", 2, num_points),
        generate_partition_history("events", 0, num_points),
    ];

    // Calculate overall stats
    let current_total_lag: i64 = partitions.iter().map(|p| p.current_lag).sum();
    let overall_velocity = if history.len() >= 2 {
        let recent = &history[history.len() - 5..];
        let avg_recent: i64 = recent.iter().map(|p| p.lag).sum::<i64>() / recent.len() as i64;
        let earlier = &history[..5];
        let avg_earlier: i64 = earlier.iter().map(|p| p.lag).sum::<i64>() / earlier.len() as i64;

        if avg_recent > avg_earlier + 100 {
            LagVelocity::Growing
        } else if avg_recent < avg_earlier - 100 {
            LagVelocity::Shrinking
        } else {
            LagVelocity::Stable
        }
    } else {
        LagVelocity::Stable
    };

    let response = LagHistoryResponse {
        group_id,
        range: query.range,
        start_timestamp: start.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        end_timestamp: now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        start_timestamp_ms: start.timestamp_millis(),
        end_timestamp_ms: now.timestamp_millis(),
        history,
        partitions,
        current_total_lag,
        overall_velocity,
        alert_threshold: Some(10000),
        threshold_exceeded: current_total_lag > 10000,
    };

    Json(response)
}

/// Simple deterministic pseudo-random for demo data.
fn rand_simple(seed: usize) -> f64 {
    let x = (seed as f64 * 12.9898).sin() * 43758.5453;
    x - x.floor()
}

/// Generate partition lag history for sparklines.
fn generate_partition_history(
    topic: &str,
    partition: i32,
    _num_points: usize,
) -> crate::ui::client::PartitionLagHistory {
    use crate::ui::client::{LagVelocity, PartitionLagHistory};

    let seed_offset = topic.len() + partition as usize;
    let base_lag = 100 + (partition as i64 * 50);

    let sparkline: Vec<i64> = (0..20)
        .map(|i| {
            let variation =
                ((i as f64 * 0.3).sin() * 30.0 + rand_simple(seed_offset + i) * 20.0) as i64;
            (base_lag + variation).max(0)
        })
        .collect();

    let current_lag = *sparkline.last().unwrap_or(&base_lag);
    let prev_lag = sparkline
        .get(sparkline.len().saturating_sub(2))
        .copied()
        .unwrap_or(current_lag);

    let velocity = if current_lag > prev_lag + 10 {
        LagVelocity::Growing
    } else if current_lag < prev_lag - 10 {
        LagVelocity::Shrinking
    } else {
        LagVelocity::Stable
    };

    let min_lag = sparkline.iter().min().copied().unwrap_or(0);
    let max_lag = sparkline.iter().max().copied().unwrap_or(0);
    let avg_lag = sparkline.iter().sum::<i64>() as f64 / sparkline.len() as f64;

    PartitionLagHistory {
        topic: topic.to_string(),
        partition,
        current_lag,
        velocity,
        sparkline,
        min_lag,
        max_lag,
        avg_lag,
    }
}

/// Export lag history as CSV.
async fn api_export_lag_history(
    State(_state): State<WebUiState>,
    Path(group_id): Path<String>,
    Query(query): Query<LagHistoryQuery>,
) -> impl IntoResponse {
    use crate::ui::client::{LagHistoryPoint, LagVelocity};

    // Generate the same data as the history endpoint
    let (duration_hours, num_points) = match query.range.as_str() {
        "1h" => (1, 60),
        "6h" => (6, 72),
        "24h" => (24, 96),
        "7d" => (168, 168),
        _ => (1, 60),
    };

    let now = chrono::Utc::now();
    let start = now - chrono::Duration::hours(duration_hours);
    let interval_ms = (duration_hours * 3600 * 1000) / num_points as i64;

    let base_lag = 500i64;
    let mut history: Vec<LagHistoryPoint> = Vec::with_capacity(num_points);

    for i in 0..num_points {
        let timestamp = start + chrono::Duration::milliseconds(interval_ms * i as i64);

        let variation = ((i as f64 * 0.1).sin() * 200.0
            + (i as f64 * 0.05).cos() * 100.0
            + (rand_simple(i) * 50.0)) as i64;
        let current_lag = (base_lag + variation).max(0);

        let velocity = if i > 0 {
            let prev_lag = history.last().map(|p| p.lag).unwrap_or(current_lag);
            if current_lag > prev_lag + 50 {
                LagVelocity::Growing
            } else if current_lag < prev_lag - 50 {
                LagVelocity::Shrinking
            } else {
                LagVelocity::Stable
            }
        } else {
            LagVelocity::Stable
        };

        history.push(LagHistoryPoint {
            timestamp: timestamp.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
            timestamp_ms: timestamp.timestamp_millis(),
            lag: current_lag,
            velocity,
            consume_rate: 100.0 + (rand_simple(i + 100) * 50.0),
            produce_rate: 100.0 + (rand_simple(i + 200) * 50.0),
        });
    }

    // Build CSV
    let mut csv = String::from("timestamp,lag,velocity,consume_rate,produce_rate\n");
    for point in &history {
        csv.push_str(&format!(
            "{},{},{},{:.2},{:.2}\n",
            point.timestamp, point.lag, point.velocity, point.consume_rate, point.produce_rate
        ));
    }

    let filename = format!("{}-lag-history-{}.csv", group_id, query.range);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/csv; charset=utf-8")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", filename),
        )
        .body(Body::from(csv))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

// Schema Evolution handlers

/// Schema registry browser page.
async fn schema_list_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.schema_list())
}

/// Schema evolution timeline page.
async fn schema_details_page(
    State(state): State<WebUiState>,
    Path(subject): Path<String>,
) -> impl IntoResponse {
    Html(state.templates.schema_details(&subject))
}

/// Schema diff page.
async fn schema_diff_page(
    State(state): State<WebUiState>,
    Path((subject, v1, v2)): Path<(String, i32, i32)>,
) -> impl IntoResponse {
    Html(state.templates.schema_diff(&subject, v1, v2))
}

/// List all schema subjects.
async fn api_list_schemas(State(_state): State<WebUiState>) -> impl IntoResponse {
    use crate::ui::client::{CompatibilityLevel, SchemaListResponse, SchemaSubject, SchemaType};

    // Generate sample schema data for demonstration
    // In production, this would query the schema registry
    let subjects = vec![
        SchemaSubject {
            name: "orders-value".to_string(),
            schema_type: SchemaType::Avro,
            latest_version: 3,
            compatibility: CompatibilityLevel::Backward,
            linked_topics: vec!["orders".to_string(), "orders-dlq".to_string()],
            versions_count: 3,
        },
        SchemaSubject {
            name: "orders-key".to_string(),
            schema_type: SchemaType::Avro,
            latest_version: 1,
            compatibility: CompatibilityLevel::Full,
            linked_topics: vec!["orders".to_string()],
            versions_count: 1,
        },
        SchemaSubject {
            name: "users-value".to_string(),
            schema_type: SchemaType::JsonSchema,
            latest_version: 5,
            compatibility: CompatibilityLevel::BackwardTransitive,
            linked_topics: vec!["users".to_string(), "user-events".to_string()],
            versions_count: 5,
        },
        SchemaSubject {
            name: "events-value".to_string(),
            schema_type: SchemaType::Protobuf,
            latest_version: 2,
            compatibility: CompatibilityLevel::Forward,
            linked_topics: vec!["events".to_string()],
            versions_count: 2,
        },
        SchemaSubject {
            name: "payments-value".to_string(),
            schema_type: SchemaType::Avro,
            latest_version: 4,
            compatibility: CompatibilityLevel::None,
            linked_topics: vec!["payments".to_string()],
            versions_count: 4,
        },
    ];

    Json(SchemaListResponse { subjects, total: 5 })
}

/// Get schema subject details with version history.
async fn api_get_schema_subject(
    State(_state): State<WebUiState>,
    Path(subject): Path<String>,
) -> impl IntoResponse {
    use crate::ui::client::{
        CompatibilityLevel, SchemaChange, SchemaChangeType, SchemaSubjectDetails, SchemaType,
        SchemaVersion,
    };

    // Determine schema type from subject name
    let schema_type = if subject.contains("events") {
        SchemaType::Protobuf
    } else if subject.contains("users") {
        SchemaType::JsonSchema
    } else {
        SchemaType::Avro
    };

    // Generate sample versions based on subject name
    let base_time = chrono::Utc::now();
    let versions = match subject.as_str() {
        "orders-value" => vec![
            SchemaVersion {
                version: 3,
                schema_id: 103,
                schema: generate_avro_schema(
                    "Order",
                    &["id", "customer_id", "total", "status", "created_at"],
                ),
                registered_at: base_time.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                compatibility_status: "COMPATIBLE".to_string(),
                changes_summary: vec![SchemaChange {
                    change_type: SchemaChangeType::Added,
                    field_name: "created_at".to_string(),
                    description: "Added timestamp field for order creation time".to_string(),
                    old_value: None,
                    new_value: Some("long (timestamp-millis)".to_string()),
                }],
            },
            SchemaVersion {
                version: 2,
                schema_id: 102,
                schema: generate_avro_schema("Order", &["id", "customer_id", "total", "status"]),
                registered_at: (base_time - chrono::Duration::days(7))
                    .format("%Y-%m-%dT%H:%M:%SZ")
                    .to_string(),
                compatibility_status: "COMPATIBLE".to_string(),
                changes_summary: vec![SchemaChange {
                    change_type: SchemaChangeType::Added,
                    field_name: "status".to_string(),
                    description: "Added order status field".to_string(),
                    old_value: None,
                    new_value: Some(
                        "string (enum: pending, processing, shipped, delivered)".to_string(),
                    ),
                }],
            },
            SchemaVersion {
                version: 1,
                schema_id: 101,
                schema: generate_avro_schema("Order", &["id", "customer_id", "total"]),
                registered_at: (base_time - chrono::Duration::days(30))
                    .format("%Y-%m-%dT%H:%M:%SZ")
                    .to_string(),
                compatibility_status: "INITIAL".to_string(),
                changes_summary: vec![],
            },
        ],
        _ => vec![SchemaVersion {
            version: 1,
            schema_id: 1,
            schema: generate_avro_schema(
                &subject.replace("-value", "").replace("-key", ""),
                &["id", "name", "data"],
            ),
            registered_at: (base_time - chrono::Duration::days(30))
                .format("%Y-%m-%dT%H:%M:%SZ")
                .to_string(),
            compatibility_status: "INITIAL".to_string(),
            changes_summary: vec![],
        }],
    };

    let compatibility = if subject.contains("orders") {
        CompatibilityLevel::Backward
    } else if subject.contains("users") {
        CompatibilityLevel::BackwardTransitive
    } else {
        CompatibilityLevel::Full
    };

    let linked_topics = if subject.ends_with("-value") {
        vec![subject.replace("-value", "")]
    } else if subject.ends_with("-key") {
        vec![subject.replace("-key", "")]
    } else {
        vec![subject.clone()]
    };

    let response = SchemaSubjectDetails {
        name: subject,
        schema_type,
        compatibility,
        versions,
        linked_topics,
    };

    Json(response)
}

/// Get schema diff between two versions.
async fn api_get_schema_diff(
    State(_state): State<WebUiState>,
    Path((subject, v1, v2)): Path<(String, i32, i32)>,
) -> impl IntoResponse {
    use crate::ui::client::{SchemaChange, SchemaChangeType, SchemaDiff};

    // Generate sample diff data
    let schema1 = generate_avro_schema("Order", &["id", "customer_id", "total"]);
    let schema2 = generate_avro_schema("Order", &["id", "customer_id", "total", "status"]);

    let changes = vec![SchemaChange {
        change_type: SchemaChangeType::Added,
        field_name: "status".to_string(),
        description: "Added order status field".to_string(),
        old_value: None,
        new_value: Some("string".to_string()),
    }];

    let response = SchemaDiff {
        subject,
        version1: v1,
        version2: v2,
        schema1,
        schema2,
        changes,
        is_compatible: true,
    };

    Json(response)
}

/// Generate a sample Avro schema for demonstration.
fn generate_avro_schema(name: &str, fields: &[&str]) -> String {
    let field_defs: Vec<String> = fields
        .iter()
        .map(|f| {
            let field_type = match *f {
                "id" => r#"{"name": "id", "type": "string"}"#,
                "customer_id" => r#"{"name": "customer_id", "type": "string"}"#,
                "total" => r#"{"name": "total", "type": "double"}"#,
                "status" => r#"{"name": "status", "type": ["null", "string"], "default": null}"#,
                "created_at" => r#"{"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}"#,
                "name" => r#"{"name": "name", "type": "string"}"#,
                "data" => r#"{"name": "data", "type": ["null", "bytes"], "default": null}"#,
                _ => r#"{"name": "unknown", "type": "string"}"#,
            };
            field_type.to_string()
        })
        .collect();

    format!(
        r#"{{
  "type": "record",
  "name": "{}",
  "namespace": "com.streamline.schemas",
  "fields": [
    {}
  ]
}}"#,
        name,
        field_defs.join(",\n    ")
    )
}

// Multi-Cluster Management handlers

/// Clusters management page.
async fn clusters_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.clusters_page())
}

/// List all clusters.
async fn api_list_clusters(State(_state): State<WebUiState>) -> impl IntoResponse {
    use crate::ui::client::{ClusterHealth, ClusterInfo, ClusterListResponse};

    // Generate sample cluster data for demonstration
    // In production, this would read from cluster registry
    let clusters = vec![
        ClusterInfo {
            id: "local".to_string(),
            name: "Local Development".to_string(),
            url: "localhost:9092".to_string(),
            health: ClusterHealth::Healthy,
            version: Some(env!("CARGO_PKG_VERSION").to_string()),
            broker_count: 1,
            topic_count: 5,
            message_count: 12500,
            consumer_group_count: 3,
            color: Some("#3b82f6".to_string()),
            added_at: "2024-01-15T10:00:00Z".to_string(),
            last_seen: Some(chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()),
            is_current: true,
        },
        ClusterInfo {
            id: "staging".to_string(),
            name: "Staging".to_string(),
            url: "staging.streamline.internal:9092".to_string(),
            health: ClusterHealth::Healthy,
            version: Some(env!("CARGO_PKG_VERSION").to_string()),
            broker_count: 3,
            topic_count: 15,
            message_count: 250000,
            consumer_group_count: 8,
            color: Some("#f59e0b".to_string()),
            added_at: "2024-02-01T14:30:00Z".to_string(),
            last_seen: Some(
                (chrono::Utc::now() - chrono::Duration::minutes(5))
                    .format("%Y-%m-%dT%H:%M:%SZ")
                    .to_string(),
            ),
            is_current: false,
        },
        ClusterInfo {
            id: "production".to_string(),
            name: "Production US-East".to_string(),
            url: "prod-us-east.streamline.io:9092".to_string(),
            health: ClusterHealth::Degraded,
            version: Some(env!("CARGO_PKG_VERSION").to_string()),
            broker_count: 5,
            topic_count: 42,
            message_count: 15_000_000,
            consumer_group_count: 25,
            color: Some("#ef4444".to_string()),
            added_at: "2024-03-10T09:00:00Z".to_string(),
            last_seen: Some(
                (chrono::Utc::now() - chrono::Duration::minutes(2))
                    .format("%Y-%m-%dT%H:%M:%SZ")
                    .to_string(),
            ),
            is_current: false,
        },
        ClusterInfo {
            id: "prod-eu".to_string(),
            name: "Production EU-West".to_string(),
            url: "prod-eu-west.streamline.io:9092".to_string(),
            health: ClusterHealth::Unhealthy,
            version: Some(env!("CARGO_PKG_VERSION").to_string()),
            broker_count: 5,
            topic_count: 38,
            message_count: 8_500_000,
            consumer_group_count: 18,
            color: Some("#8b5cf6".to_string()),
            added_at: "2024-03-15T11:00:00Z".to_string(),
            last_seen: Some(
                (chrono::Utc::now() - chrono::Duration::hours(1))
                    .format("%Y-%m-%dT%H:%M:%SZ")
                    .to_string(),
            ),
            is_current: false,
        },
    ];

    Json(ClusterListResponse {
        clusters,
        current_cluster_id: Some("local".to_string()),
    })
}

/// Add a new cluster.
async fn api_add_cluster(
    State(_state): State<WebUiState>,
    Json(config): Json<crate::ui::client::ClusterConfig>,
) -> impl IntoResponse {
    use crate::ui::client::{ClusterHealth, ClusterInfo};

    // In production, this would save to cluster registry and test connection
    let cluster = ClusterInfo {
        id: format!("cluster-{}", chrono::Utc::now().timestamp_millis()),
        name: config.name,
        url: config.url,
        health: ClusterHealth::Unknown,
        version: None,
        broker_count: 0,
        topic_count: 0,
        message_count: 0,
        consumer_group_count: 0,
        color: config.color,
        added_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        last_seen: None,
        is_current: false,
    };

    (StatusCode::CREATED, Json(cluster))
}

/// Get cluster by ID.
async fn api_get_cluster(
    State(_state): State<WebUiState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    use crate::ui::client::{ClusterHealth, ClusterInfo};

    // In production, this would read from cluster registry
    let cluster = ClusterInfo {
        id: id.clone(),
        name: format!("Cluster {}", id),
        url: format!("{}.streamline.io:9092", id),
        health: ClusterHealth::Healthy,
        version: Some(env!("CARGO_PKG_VERSION").to_string()),
        broker_count: 3,
        topic_count: 10,
        message_count: 50000,
        consumer_group_count: 5,
        color: Some("#3b82f6".to_string()),
        added_at: "2024-01-01T00:00:00Z".to_string(),
        last_seen: Some(chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string()),
        is_current: false,
    };

    Json(cluster)
}

/// Update cluster configuration.
async fn api_update_cluster(
    State(_state): State<WebUiState>,
    Path(id): Path<String>,
    Json(config): Json<crate::ui::client::ClusterConfig>,
) -> impl IntoResponse {
    use crate::ui::client::{ClusterHealth, ClusterInfo};

    // In production, this would update cluster registry
    let cluster = ClusterInfo {
        id,
        name: config.name,
        url: config.url,
        health: ClusterHealth::Unknown,
        version: Some(env!("CARGO_PKG_VERSION").to_string()),
        broker_count: 0,
        topic_count: 0,
        message_count: 0,
        consumer_group_count: 0,
        color: config.color,
        added_at: "2024-01-01T00:00:00Z".to_string(),
        last_seen: None,
        is_current: false,
    };

    Json(cluster)
}

/// Delete a cluster.
async fn api_delete_cluster(
    State(_state): State<WebUiState>,
    Path(_id): Path<String>,
) -> impl IntoResponse {
    // In production, this would remove from cluster registry
    StatusCode::NO_CONTENT
}

/// Test cluster connection.
async fn api_test_cluster(
    State(_state): State<WebUiState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    // In production, this would actually test the connection
    Json(serde_json::json!({
        "cluster_id": id,
        "success": true,
        "latency_ms": 45,
        "version": env!("CARGO_PKG_VERSION"),
        "broker_count": 3,
        "message": "Connection successful"
    }))
}

/// Get cluster health.
async fn api_get_cluster_health(
    State(_state): State<WebUiState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    use crate::ui::client::{ClusterHealth, ClusterHealthDetails};

    let health = ClusterHealthDetails {
        cluster_id: id,
        health: ClusterHealth::Healthy,
        brokers_healthy: 3,
        brokers_total: 3,
        under_replicated_partitions: 0,
        active_alerts: 1,
        error_message: None,
        checked_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    };

    Json(health)
}

/// Compare health across clusters.
async fn api_compare_clusters(State(_state): State<WebUiState>) -> impl IntoResponse {
    use crate::ui::client::{ClusterComparison, ClusterHealth, ClusterHealthDetails};

    let comparison = ClusterComparison {
        clusters: vec![
            ClusterHealthDetails {
                cluster_id: "local".to_string(),
                health: ClusterHealth::Healthy,
                brokers_healthy: 1,
                brokers_total: 1,
                under_replicated_partitions: 0,
                active_alerts: 0,
                error_message: None,
                checked_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            },
            ClusterHealthDetails {
                cluster_id: "staging".to_string(),
                health: ClusterHealth::Healthy,
                brokers_healthy: 3,
                brokers_total: 3,
                under_replicated_partitions: 0,
                active_alerts: 2,
                error_message: None,
                checked_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            },
            ClusterHealthDetails {
                cluster_id: "production".to_string(),
                health: ClusterHealth::Degraded,
                brokers_healthy: 4,
                brokers_total: 5,
                under_replicated_partitions: 3,
                active_alerts: 5,
                error_message: Some("Broker 3 unresponsive".to_string()),
                checked_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            },
        ],
        compared_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    };

    Json(comparison)
}

// Partition Rebalancing page handlers

async fn rebalances_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.rebalances_page())
}

async fn rebalance_group_page(
    State(state): State<WebUiState>,
    Path(group_id): Path<String>,
) -> impl IntoResponse {
    Html(state.templates.rebalance_group_page(&group_id))
}

// Partition Rebalancing API handlers

async fn api_list_rebalances(State(_state): State<WebUiState>) -> impl IntoResponse {
    use crate::ui::client::{
        RebalanceEvent, RebalanceListResponse, RebalanceStatus, RebalanceTrigger,
    };

    // Generate mock rebalance events for demonstration
    let events = vec![
        RebalanceEvent {
            id: "rb-001".to_string(),
            group_id: "order-processing-group".to_string(),
            trigger: RebalanceTrigger::MemberJoin,
            started_at: "2024-12-13T19:45:00Z".to_string(),
            completed_at: Some("2024-12-13T19:45:03Z".to_string()),
            duration_ms: Some(3200),
            generation_id: 42,
            previous_generation: Some(41),
            members_before: 3,
            members_after: 4,
            partitions_moved: 6,
            status: RebalanceStatus::Completed,
        },
        RebalanceEvent {
            id: "rb-002".to_string(),
            group_id: "analytics-consumer".to_string(),
            trigger: RebalanceTrigger::HeartbeatTimeout,
            started_at: "2024-12-13T19:30:00Z".to_string(),
            completed_at: Some("2024-12-13T19:30:15Z".to_string()),
            duration_ms: Some(15400),
            generation_id: 18,
            previous_generation: Some(17),
            members_before: 5,
            members_after: 4,
            partitions_moved: 12,
            status: RebalanceStatus::Completed,
        },
        RebalanceEvent {
            id: "rb-003".to_string(),
            group_id: "payment-processor".to_string(),
            trigger: RebalanceTrigger::MemberLeave,
            started_at: "2024-12-13T19:20:00Z".to_string(),
            completed_at: None,
            duration_ms: None,
            generation_id: 7,
            previous_generation: Some(6),
            members_before: 2,
            members_after: 1,
            partitions_moved: 0,
            status: RebalanceStatus::Stuck,
        },
        RebalanceEvent {
            id: "rb-004".to_string(),
            group_id: "notification-service".to_string(),
            trigger: RebalanceTrigger::TopicMetadataChange,
            started_at: "2024-12-13T18:00:00Z".to_string(),
            completed_at: Some("2024-12-13T18:00:02Z".to_string()),
            duration_ms: Some(1850),
            generation_id: 25,
            previous_generation: Some(24),
            members_before: 2,
            members_after: 2,
            partitions_moved: 4,
            status: RebalanceStatus::Completed,
        },
    ];

    Json(RebalanceListResponse { events, total: 4 })
}

async fn api_get_group_rebalances(
    State(_state): State<WebUiState>,
    Path(group_id): Path<String>,
) -> impl IntoResponse {
    use crate::ui::client::{
        RebalanceEvent, RebalanceListResponse, RebalanceStatus, RebalanceTrigger,
    };

    // Generate mock rebalance history for a specific group
    let events = vec![
        RebalanceEvent {
            id: format!("{}-rb-001", group_id),
            group_id: group_id.clone(),
            trigger: RebalanceTrigger::MemberJoin,
            started_at: "2024-12-13T19:45:00Z".to_string(),
            completed_at: Some("2024-12-13T19:45:03Z".to_string()),
            duration_ms: Some(3200),
            generation_id: 42,
            previous_generation: Some(41),
            members_before: 3,
            members_after: 4,
            partitions_moved: 6,
            status: RebalanceStatus::Completed,
        },
        RebalanceEvent {
            id: format!("{}-rb-002", group_id),
            group_id: group_id.clone(),
            trigger: RebalanceTrigger::MemberLeave,
            started_at: "2024-12-13T18:30:00Z".to_string(),
            completed_at: Some("2024-12-13T18:30:05Z".to_string()),
            duration_ms: Some(4800),
            generation_id: 41,
            previous_generation: Some(40),
            members_before: 4,
            members_after: 3,
            partitions_moved: 8,
            status: RebalanceStatus::Completed,
        },
        RebalanceEvent {
            id: format!("{}-rb-003", group_id),
            group_id: group_id.clone(),
            trigger: RebalanceTrigger::Manual,
            started_at: "2024-12-13T15:00:00Z".to_string(),
            completed_at: Some("2024-12-13T15:00:02Z".to_string()),
            duration_ms: Some(1500),
            generation_id: 40,
            previous_generation: Some(39),
            members_before: 4,
            members_after: 4,
            partitions_moved: 0,
            status: RebalanceStatus::Completed,
        },
    ];

    Json(RebalanceListResponse { events, total: 3 })
}

async fn api_get_current_rebalance(
    State(_state): State<WebUiState>,
    Path(group_id): Path<String>,
) -> impl IntoResponse {
    use crate::ui::client::{RebalanceEvent, RebalanceStatus, RebalanceTrigger};

    // Return current rebalance if in progress
    let current = if group_id == "payment-processor" {
        Some(RebalanceEvent {
            id: "rb-current".to_string(),
            group_id: group_id.clone(),
            trigger: RebalanceTrigger::MemberLeave,
            started_at: "2024-12-13T19:20:00Z".to_string(),
            completed_at: None,
            duration_ms: None,
            generation_id: 7,
            previous_generation: Some(6),
            members_before: 2,
            members_after: 1,
            partitions_moved: 0,
            status: RebalanceStatus::Stuck,
        })
    } else {
        None
    };

    Json(current)
}

async fn api_get_group_assignments(
    State(_state): State<WebUiState>,
    Path(group_id): Path<String>,
) -> impl IntoResponse {
    use crate::ui::client::{ConsumerMember, GroupAssignmentState, MemberState, TopicPartition};

    let state = GroupAssignmentState {
        group_id: group_id.clone(),
        generation_id: 42,
        members: vec![
            ConsumerMember {
                member_id: "consumer-1-abc123".to_string(),
                client_id: "order-service-1".to_string(),
                host: "10.0.1.15".to_string(),
                assigned_partitions: vec![
                    TopicPartition {
                        topic: "orders".to_string(),
                        partition: 0,
                    },
                    TopicPartition {
                        topic: "orders".to_string(),
                        partition: 1,
                    },
                    TopicPartition {
                        topic: "orders".to_string(),
                        partition: 2,
                    },
                ],
                session_timeout_ms: 30000,
                heartbeat_interval_ms: 3000,
                last_heartbeat: Some("2024-12-13T19:59:58Z".to_string()),
                state: MemberState::Stable,
            },
            ConsumerMember {
                member_id: "consumer-2-def456".to_string(),
                client_id: "order-service-2".to_string(),
                host: "10.0.1.16".to_string(),
                assigned_partitions: vec![
                    TopicPartition {
                        topic: "orders".to_string(),
                        partition: 3,
                    },
                    TopicPartition {
                        topic: "orders".to_string(),
                        partition: 4,
                    },
                    TopicPartition {
                        topic: "orders".to_string(),
                        partition: 5,
                    },
                ],
                session_timeout_ms: 30000,
                heartbeat_interval_ms: 3000,
                last_heartbeat: Some("2024-12-13T19:59:57Z".to_string()),
                state: MemberState::Stable,
            },
            ConsumerMember {
                member_id: "consumer-3-ghi789".to_string(),
                client_id: "order-service-3".to_string(),
                host: "10.0.1.17".to_string(),
                assigned_partitions: vec![
                    TopicPartition {
                        topic: "orders".to_string(),
                        partition: 6,
                    },
                    TopicPartition {
                        topic: "orders".to_string(),
                        partition: 7,
                    },
                ],
                session_timeout_ms: 30000,
                heartbeat_interval_ms: 3000,
                last_heartbeat: Some("2024-12-13T19:59:59Z".to_string()),
                state: MemberState::Stable,
            },
            ConsumerMember {
                member_id: "consumer-4-jkl012".to_string(),
                client_id: "order-service-4".to_string(),
                host: "10.0.1.18".to_string(),
                assigned_partitions: vec![
                    TopicPartition {
                        topic: "orders".to_string(),
                        partition: 8,
                    },
                    TopicPartition {
                        topic: "orders".to_string(),
                        partition: 9,
                    },
                ],
                session_timeout_ms: 30000,
                heartbeat_interval_ms: 3000,
                last_heartbeat: Some("2024-12-13T19:59:56Z".to_string()),
                state: MemberState::Stable,
            },
        ],
        rebalance_status: None,
        subscribed_topics: vec!["orders".to_string(), "order-events".to_string()],
        total_partitions: 10,
        last_updated: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    };

    Json(state)
}

async fn api_get_assignment_diff(
    State(_state): State<WebUiState>,
    Path((group_id, gen1, gen2)): Path<(String, i32, i32)>,
) -> impl IntoResponse {
    use crate::ui::client::{AssignmentDiff, PartitionAssignment, PartitionMove};

    let diff = AssignmentDiff {
        group_id,
        before_generation: gen1,
        after_generation: gen2,
        added: vec![
            PartitionAssignment {
                topic: "orders".to_string(),
                partition: 8,
                consumer_id: "consumer-4-jkl012".to_string(),
                client_id: Some("order-service-4".to_string()),
                host: Some("10.0.1.18".to_string()),
            },
            PartitionAssignment {
                topic: "orders".to_string(),
                partition: 9,
                consumer_id: "consumer-4-jkl012".to_string(),
                client_id: Some("order-service-4".to_string()),
                host: Some("10.0.1.18".to_string()),
            },
        ],
        removed: vec![],
        moved: vec![
            PartitionMove {
                topic: "orders".to_string(),
                partition: 2,
                from_consumer: "consumer-1-abc123".to_string(),
                to_consumer: "consumer-3-ghi789".to_string(),
            },
            PartitionMove {
                topic: "orders".to_string(),
                partition: 5,
                from_consumer: "consumer-2-def456".to_string(),
                to_consumer: "consumer-4-jkl012".to_string(),
            },
        ],
    };

    Json(diff)
}

// Health check

async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "service": "streamline-ui"
    }))
}

async fn settings_page(State(state): State<WebUiState>) -> impl IntoResponse {
    let overview = state.client.get_cluster_overview().await;
    let content = match overview {
        Ok(info) => {
            format!(
                r#"<div class="page-header"><h1>⚙️ System Settings</h1></div>
                <div class="grid grid-cols-2 gap-4">
                    <div class="card"><h3>Server Configuration</h3>
                    <table class="table"><tbody>
                        <tr><td>Cluster ID</td><td>{}</td></tr>
                        <tr><td>Brokers</td><td>{}</td></tr>
                        <tr><td>Topics</td><td>{}</td></tr>
                    </tbody></table></div>
                    <div class="card"><h3>Feature Flags</h3>
                    <p>Manage feature flags and runtime configuration from this panel.</p>
                    <ul>
                        <li>Authentication: <code>auth</code></li>
                        <li>Clustering: <code>clustering</code></li>
                        <li>Telemetry: <code>telemetry</code></li>
                        <li>Analytics: <code>analytics</code></li>
                        <li>AI Pipeline: <code>ai</code></li>
                        <li>CDC: <code>cdc</code></li>
                    </ul></div>
                    <div class="card"><h3>Storage</h3>
                    <p>Storage mode, data directory, and retention policies.</p></div>
                    <div class="card"><h3>Security</h3>
                    <p>TLS/mTLS, SASL mechanisms, ACL configuration.</p></div>
                </div>"#,
                info.cluster_id, info.broker_count, info.topic_count,
            )
        }
        Err(_) => "<div class=\"alert alert-error\">Failed to load server info</div>".to_string(),
    };
    Html(state.templates.layout("Settings", &content, "settings"))
}

async fn plugins_page(State(state): State<WebUiState>) -> impl IntoResponse {
    let content = r#"<div class="page-header"><h1>🧩 Plugin Manager</h1>
        <p>Install, configure, and manage Streamline extensions.</p></div>
        <div class="grid grid-cols-1 gap-4">
            <div class="card"><h3>Installed Plugins</h3>
            <p id="plugins-list">Loading plugins...</p>
            <script>
                fetch('/api/v1/plugins').then(r => r.json()).then(data => {
                    const el = document.getElementById('plugins-list');
                    if (data.plugins && data.plugins.length > 0) {
                        el.innerHTML = '<table class="table"><thead><tr><th>Name</th><th>Type</th><th>Version</th><th>Status</th></tr></thead><tbody>' +
                            data.plugins.map(p => `<tr><td>${p.manifest.name}</td><td>${p.manifest.plugin_type}</td><td>${p.manifest.version}</td><td>${p.enabled ? '✅ Enabled' : '⏸ Disabled'}</td></tr>`).join('') +
                            '</tbody></table>';
                    } else {
                        el.innerHTML = '<p class="text-muted">No plugins installed. Browse the registry to get started.</p>';
                    }
                });
            </script></div>
            <div class="card"><h3>Plugin Registry</h3>
            <p id="registry-results">Loading registry...</p>
            <script>
                fetch('/api/v1/plugins/registry/search').then(r => r.json()).then(data => {
                    const el = document.getElementById('registry-results');
                    el.innerHTML = '<table class="table"><thead><tr><th>Name</th><th>Type</th><th>Description</th><th>Action</th></tr></thead><tbody>' +
                        data.plugins.map(p => `<tr><td>${p.name}</td><td>${p.plugin_type}</td><td>${p.description}</td><td><button class="btn btn-sm btn-primary">Install</button></td></tr>`).join('') +
                        '</tbody></table>';
                });
            </script></div>
        </div>"#;
    Html(state.templates.layout("Plugins", content, "plugins"))
}

// Static assets (embedded)

async fn static_css() -> impl IntoResponse {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/css; charset=utf-8")
        .header(header::CACHE_CONTROL, "public, max-age=3600")
        .body(Body::from(include_str!("static/main.css")))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

async fn static_js() -> impl IntoResponse {
    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )
        .header(header::CACHE_CONTROL, "public, max-age=3600")
        .body(Body::from(include_str!("static/main.js")))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

async fn favicon() -> impl IntoResponse {
    // Return a minimal SVG favicon
    let svg = r##"<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">
        <rect width="100" height="100" rx="20" fill="#3b82f6"/>
        <text x="50" y="70" font-family="Arial" font-size="60" fill="white" text-anchor="middle">S</text>
    </svg>"##;

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "image/svg+xml")
        .header(header::CACHE_CONTROL, "public, max-age=86400")
        .body(Body::from(svg))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}

// ================================
// CDC (Change Data Capture) Pages
// ================================

async fn cdc_dashboard_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.feature_dashboard(
        "CDC Dashboard",
        "Change Data Capture",
        "Monitor database change streams, capture events, and track schema evolution.",
        &[
            ("Sources", "4 active", "database"),
            ("Events/sec", "12,345", "activity"),
            ("Lag", "2.3ms", "clock"),
            ("Tables", "28 tracked", "table"),
        ],
        &["/cdc/sources", "/cdc/schema", "/cdc/metrics"],
    ))
}

async fn cdc_sources_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.list_page(
        "CDC Sources",
        "Database change capture sources",
        &[
            ("postgres-main", "PostgreSQL", "running", "1.2M events"),
            ("mysql-replica", "MySQL", "running", "845K events"),
            ("mongo-orders", "MongoDB", "running", "523K events"),
            ("sqlserver-legacy", "SQL Server", "paused", "156K events"),
        ],
    ))
}

async fn cdc_source_details_page(
    State(state): State<WebUiState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    Html(state.templates.details_page(
        &format!("CDC Source: {}", name),
        &[
            ("Status", "Running"),
            ("Type", "PostgreSQL"),
            ("Database", "inventory"),
            ("Tables", "users, orders, products"),
            ("Position", "0/1A2B3C4D"),
            ("Events Processed", "1,234,567"),
            ("Lag", "2.3ms"),
        ],
    ))
}

async fn api_list_cdc_sources(State(_state): State<WebUiState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "sources": [
            {"name": "postgres-main", "type": "postgres", "status": "running", "events": 1234567},
            {"name": "mysql-replica", "type": "mysql", "status": "running", "events": 845123},
        ]
    }))
}

async fn api_get_cdc_source(
    State(_state): State<WebUiState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    Json(serde_json::json!({
        "name": name,
        "type": "postgres",
        "status": "running",
        "database": "inventory",
        "tables": ["users", "orders", "products"],
        "position": "0/1A2B3C4D",
        "events_processed": 1234567,
        "lag_ms": 2.3
    }))
}

async fn api_get_cdc_metrics(
    State(_state): State<WebUiState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    Json(serde_json::json!({
        "source": name,
        "events_per_second": 12345,
        "bytes_per_second": 5242880,
        "lag_ms": 2.3,
        "error_rate": 0.001
    }))
}

// ================================
// AI-Native Streaming Pages
// ================================

async fn ai_dashboard_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.feature_dashboard(
        "AI Dashboard",
        "AI-Native Streaming",
        "Semantic search, LLM enrichment, classification, and anomaly detection.",
        &[
            ("Pipelines", "8 active", "cpu"),
            ("Embeddings", "2.3M vectors", "box"),
            ("Anomalies", "15 today", "alert-triangle"),
            ("Searches/min", "456", "search"),
        ],
        &["/ai/pipelines", "/ai/search", "/ai/anomalies"],
    ))
}

async fn ai_pipelines_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.list_page(
        "AI Pipelines",
        "AI processing pipelines",
        &[
            (
                "support-enrichment",
                "Enrichment",
                "running",
                "15K processed",
            ),
            (
                "log-classifier",
                "Classification",
                "running",
                "892K processed",
            ),
            (
                "anomaly-detector",
                "Anomaly Detection",
                "running",
                "5M processed",
            ),
            (
                "sentiment-analyzer",
                "Enrichment",
                "paused",
                "45K processed",
            ),
        ],
    ))
}

async fn ai_search_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.search_page(
        "Semantic Search",
        "Search across streams by meaning",
        &["support-tickets", "system-logs", "customer-feedback"],
    ))
}

async fn ai_anomalies_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.anomalies_page(
        "Anomaly Detection",
        &[
            ("HIGH", "Response time spike: 5.2s", "10:15:00", "metrics"),
            (
                "MEDIUM",
                "Unusual request pattern: 3x volume",
                "10:20:00",
                "api-logs",
            ),
            ("LOW", "Message content deviation", "10:25:00", "support"),
        ],
    ))
}

async fn api_list_ai_pipelines(State(_state): State<WebUiState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "pipelines": [
            {"name": "support-enrichment", "type": "enrichment", "status": "running", "processed": 15234},
            {"name": "log-classifier", "type": "classification", "status": "running", "processed": 892341},
        ]
    }))
}

async fn api_ai_semantic_search(
    State(_state): State<WebUiState>,
    Json(_payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    Json(serde_json::json!({
        "results": [
            {"topic": "support-tickets", "offset": 12345, "similarity": 0.95, "content": "Customer reported login issues"},
            {"topic": "system-logs", "offset": 98765, "similarity": 0.82, "content": "Authentication failure for user"},
        ]
    }))
}

async fn api_get_anomalies(
    State(_state): State<WebUiState>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    Json(serde_json::json!({
        "topic": topic,
        "anomalies": [
            {"id": 1, "type": "statistical_outlier", "severity": "high", "description": "Response time spike"},
            {"id": 2, "type": "pattern_deviation", "severity": "medium", "description": "Unusual volume"},
        ]
    }))
}

async fn api_get_embeddings_status(
    State(_state): State<WebUiState>,
    Path(topic): Path<String>,
) -> impl IntoResponse {
    Json(serde_json::json!({
        "topic": topic,
        "model": "text-embedding-ada-002",
        "vectors_count": 1523456,
        "index_size_mb": 2048,
        "indexing_lag": 150
    }))
}

// ================================
// Edge-First Architecture Pages
// ================================

async fn edge_dashboard_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.feature_dashboard(
        "Edge Dashboard",
        "Edge-First Architecture",
        "Manage edge nodes, offline sync, and conflict resolution.",
        &[
            ("Nodes", "12 online", "server"),
            ("Pending Sync", "1,234 msgs", "refresh-cw"),
            ("Conflicts", "3 unresolved", "git-merge"),
            ("Bandwidth", "45 MB/s", "wifi"),
        ],
        &["/edge/nodes", "/edge/sync", "/edge/conflicts"],
    ))
}

async fn edge_nodes_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.list_page(
        "Edge Nodes",
        "Connected edge devices and gateways",
        &[
            ("edge-us-east-001", "Gateway", "online", "2.3ms latency"),
            ("edge-eu-west-003", "Gateway", "online", "45ms latency"),
            ("sensor-floor-2", "Sensor", "online", "12ms latency"),
            (
                "actuator-hvac-1",
                "Actuator",
                "offline",
                "Last seen: 5m ago",
            ),
        ],
    ))
}

async fn edge_sync_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.sync_status_page(
        "Edge Sync Status",
        &[
            ("edge-us-east-001", "synced", 0, "2s ago"),
            ("edge-eu-west-003", "syncing", 234, "in progress"),
            ("sensor-floor-2", "pending", 1000, "queued"),
        ],
    ))
}

async fn edge_conflicts_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.conflicts_page(
        "Edge Conflicts",
        &[
            ("config-update", "edge-001 vs cloud", "pending", "10:15:00"),
            (
                "sensor-reading",
                "edge-003 vs edge-005",
                "auto-resolved",
                "10:12:00",
            ),
            ("user-preference", "edge-002 vs cloud", "manual", "10:08:00"),
        ],
    ))
}

async fn api_list_edge_nodes(State(_state): State<WebUiState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "nodes": [
            {"id": "edge-us-east-001", "type": "gateway", "status": "online", "latency_ms": 2.3},
            {"id": "edge-eu-west-003", "type": "gateway", "status": "online", "latency_ms": 45},
        ]
    }))
}

async fn api_get_edge_sync_status(State(_state): State<WebUiState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "total_nodes": 12,
        "synced": 10,
        "syncing": 1,
        "pending": 1,
        "pending_messages": 1234
    }))
}

async fn api_list_edge_conflicts(State(_state): State<WebUiState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "conflicts": [
            {"id": "conflict-1", "type": "config-update", "parties": ["edge-001", "cloud"], "status": "pending"},
        ]
    }))
}

// ================================
// Lakehouse-Native Streaming Pages
// ================================

async fn lakehouse_dashboard_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.feature_dashboard(
        "Lakehouse Dashboard",
        "Lakehouse-Native Streaming",
        "Parquet storage, materialized views, and SQL analytics.",
        &[
            ("Views", "15 active", "eye"),
            ("Exports", "8 running", "upload-cloud"),
            ("Data Size", "2.3 TB", "hard-drive"),
            ("Queries/hr", "1,234", "terminal"),
        ],
        &["/lakehouse/views", "/lakehouse/exports", "/lakehouse/query"],
    ))
}

async fn lakehouse_views_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.list_page(
        "Materialized Views",
        "Precomputed views with incremental refresh",
        &[
            ("hourly_sales", "orders", "refreshing", "Last: 5m ago"),
            ("customer_summary", "customers", "ready", "Last: 1h ago"),
            ("product_metrics", "products", "ready", "Last: 30m ago"),
        ],
    ))
}

async fn lakehouse_exports_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.list_page(
        "Export Jobs",
        "Data export to external systems",
        &[
            ("events-to-iceberg", "Iceberg", "running", "1.2M rows"),
            ("logs-to-s3", "Parquet/S3", "running", "500K rows"),
            ("metrics-daily", "Delta Lake", "scheduled", "Next: 2h"),
        ],
    ))
}

async fn lakehouse_query_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.query_page(
        "SQL Query",
        "Execute SQL queries on streaming data",
        "SELECT * FROM streamline_topic('events') LIMIT 10",
    ))
}

async fn api_list_materialized_views(State(_state): State<WebUiState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "views": [
            {"name": "hourly_sales", "source": "orders", "status": "ready", "rows": 123456},
            {"name": "customer_summary", "source": "customers", "status": "ready", "rows": 45678},
        ]
    }))
}

async fn api_list_exports(State(_state): State<WebUiState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "exports": [
            {"name": "events-to-iceberg", "format": "iceberg", "status": "running", "rows": 1234567},
        ]
    }))
}

async fn api_get_lakehouse_stats(State(_state): State<WebUiState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "total_views": 15,
        "total_exports": 8,
        "data_size_bytes": 2534023189504_i64,
        "queries_per_hour": 1234
    }))
}

// ================================
// Integration Pipeline Pages
// ================================

async fn pipelines_dashboard_page(State(state): State<WebUiState>) -> impl IntoResponse {
    Html(state.templates.feature_dashboard(
        "Integration Pipelines",
        "Feature Integration",
        "End-to-end data pipelines connecting CDC, AI, and Lakehouse.",
        &[
            ("Active", "6 pipelines", "git-branch"),
            ("Throughput", "45K events/s", "activity"),
            ("Latency", "15ms avg", "clock"),
            ("Errors", "0.01%", "alert-circle"),
        ],
        &["/pipelines/create", "/pipelines/templates"],
    ))
}

async fn pipeline_details_page(
    State(state): State<WebUiState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    Html(state.templates.pipeline_details_page(
        &name,
        &[
            ("CDC Source", "postgres-main", "running"),
            ("AI Enrichment", "gpt-4", "running"),
            ("AI Classification", "classifier-v1", "running"),
            ("Lakehouse Sink", "iceberg-warehouse", "running"),
        ],
        &[
            ("Records Processed", "1,234,567"),
            ("Bytes Processed", "2.3 GB"),
            ("Avg Latency", "15ms"),
            ("Error Rate", "0.01%"),
        ],
    ))
}

async fn api_list_pipelines(State(_state): State<WebUiState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "pipelines": [
            {"name": "orders-analytics", "status": "running", "stages": 4, "throughput": 12345},
            {"name": "logs-enrichment", "status": "running", "stages": 3, "throughput": 45678},
        ]
    }))
}

async fn api_get_pipeline(
    State(_state): State<WebUiState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    Json(serde_json::json!({
        "name": name,
        "status": "running",
        "stages": ["cdc_source", "ai_enrichment", "ai_classification", "lakehouse_sink"],
        "records_processed": 1234567,
        "bytes_processed": 2534023189_i64,
        "avg_latency_ms": 15,
        "error_rate": 0.0001
    }))
}

async fn api_get_pipeline_metrics(
    State(_state): State<WebUiState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    Json(serde_json::json!({
        "pipeline": name,
        "throughput_rps": 12345,
        "latency_p50_ms": 10,
        "latency_p99_ms": 45,
        "error_rate": 0.0001,
        "uptime_seconds": 86400
    }))
}

// ── StreamQL Editor ──────────────────────────────────────────────────────────

async fn streamql_editor_page(State(state): State<WebUiState>) -> impl IntoResponse {
    let content = r#"<div class="page-header"><h1>📊 StreamQL Editor</h1>
        <p>Execute SQL queries on streaming data with real-time results.</p></div>
        <div class="card">
        <textarea id="query-input" class="form-control" rows="5" placeholder="SELECT * FROM streamline_topic('events') LIMIT 10"
            style="font-family: monospace; width: 100%;"></textarea>
        <div style="margin-top: 8px;">
            <button id="run-btn" class="btn btn-primary" onclick="runQuery()">▶ Run Query</button>
            <button class="btn btn-secondary" onclick="validateQuery()">✓ Validate</button>
            <span id="query-status" style="margin-left: 12px;"></span>
        </div>
        <div id="query-results" style="margin-top: 16px;"></div>
        </div>
        <script>
        async function runQuery() {
            const sql = document.getElementById('query-input').value;
            document.getElementById('query-status').textContent = 'Executing...';
            try {
                const res = await fetch('/api/v1/streamql/execute', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({query: sql})});
                const data = await res.json();
                document.getElementById('query-status').textContent = data.error ? '❌ ' + data.error : '✅ ' + (data.row_count || 0) + ' rows';
                if (data.columns && data.rows) {
                    let html = '<table class="table"><thead><tr>' + data.columns.map(c => '<th>'+c+'</th>').join('') + '</tr></thead><tbody>' +
                        data.rows.map(r => '<tr>' + r.map(v => '<td>'+v+'</td>').join('') + '</tr>').join('') + '</tbody></table>';
                    document.getElementById('query-results').innerHTML = html;
                }
            } catch(e) { document.getElementById('query-status').textContent = '❌ ' + e.message; }
        }
        async function validateQuery() {
            const sql = document.getElementById('query-input').value;
            const res = await fetch('/api/v1/streamql/validate', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({query: sql})});
            const data = await res.json();
            document.getElementById('query-status').textContent = data.valid ? '✅ Valid query' : '❌ ' + data.error;
        }
        </script>"#;
    Html(state.templates.layout("StreamQL Editor", content, "streamql"))
}

async fn streamql_history_page(State(state): State<WebUiState>) -> impl IntoResponse {
    let content = r#"<div class="page-header"><h1>📋 Query History</h1></div>
        <div id="history" class="card"><p>Loading...</p></div>
        <script>
        fetch('/api/v1/streamql/history').then(r => r.json()).then(data => {
            const el = document.getElementById('history');
            if (data.queries && data.queries.length > 0) {
                el.innerHTML = '<table class="table"><thead><tr><th>Time</th><th>Query</th><th>Duration</th><th>Rows</th></tr></thead><tbody>' +
                    data.queries.map(q => `<tr><td>${q.executed_at}</td><td><code>${q.query}</code></td><td>${q.duration_ms}ms</td><td>${q.row_count}</td></tr>`).join('') +
                    '</tbody></table>';
            } else { el.innerHTML = '<p class="text-muted">No queries executed yet.</p>'; }
        });
        </script>"#;
    Html(state.templates.layout("Query History", content, "streamql"))
}

async fn api_execute_streamql(
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let query = payload
        .get("query")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    Json(serde_json::json!({
        "query": query,
        "columns": ["placeholder"],
        "rows": [],
        "row_count": 0,
        "duration_ms": 0,
        "error": null
    }))
}

async fn api_validate_streamql(
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let query = payload
        .get("query")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let valid = !query.trim().is_empty();
    Json(serde_json::json!({
        "valid": valid,
        "error": if valid { serde_json::Value::Null } else { serde_json::json!("Empty query") }
    }))
}

async fn api_get_streamql_history() -> Json<serde_json::Value> {
    Json(serde_json::json!({"queries": []}))
}

async fn api_list_materialized_views() -> Json<serde_json::Value> {
    Json(serde_json::json!({"views": []}))
}

// ── Message Inspector ────────────────────────────────────────────────────────

async fn message_inspector_page(State(state): State<WebUiState>) -> impl IntoResponse {
    let content = r#"<div class="page-header"><h1>🔍 Message Inspector</h1>
        <p>Deep-dive into individual messages: decode, search, and analyze.</p></div>
        <div class="card">
        <div class="form-group">
            <label>Topic:</label>
            <input type="text" id="inspect-topic" class="form-control" placeholder="topic-name" />
            <label>Partition:</label>
            <input type="number" id="inspect-partition" class="form-control" value="0" />
            <label>Offset:</label>
            <input type="number" id="inspect-offset" class="form-control" value="0" />
            <button class="btn btn-primary" onclick="inspectMessage()" style="margin-top:8px;">Inspect</button>
        </div>
        <div id="inspect-result" style="margin-top:16px;"></div>
        </div>"#;
    Html(state.templates.layout("Message Inspector", content, "messages"))
}

async fn api_decode_message(
    Json(payload): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let value = payload
        .get("value")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    // Try JSON parsing
    let decoded = serde_json::from_str::<serde_json::Value>(value)
        .map(|v| serde_json::json!({"format": "json", "decoded": v}))
        .unwrap_or_else(|_| serde_json::json!({"format": "raw", "decoded": value}));
    Json(decoded)
}

async fn api_search_messages(
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> Json<serde_json::Value> {
    let topic = params.get("topic").cloned().unwrap_or_default();
    let query = params.get("q").cloned().unwrap_or_default();
    Json(serde_json::json!({
        "topic": topic,
        "query": query,
        "matches": [],
        "total": 0
    }))
}

// ── Connector Management UI ─────────────────────────────────────────────────

async fn connectors_page(State(state): State<WebUiState>) -> impl IntoResponse {
    let content = r#"<div class="page-header"><h1>🔌 Connectors</h1>
        <p>Manage Kafka Connect-compatible connectors.</p></div>
        <div class="card">
        <div id="connectors-list"><p>Loading connectors...</p></div>
        <script>
        fetch('/api/v1/connectors').then(r => r.json()).then(data => {
            const el = document.getElementById('connectors-list');
            if (data.connectors && data.connectors.length > 0) {
                el.innerHTML = '<table class="table"><thead><tr><th>Name</th><th>Type</th><th>State</th><th>Tasks</th><th>Actions</th></tr></thead><tbody>' +
                    data.connectors.map(c => `<tr><td><a href="/connectors/${c.name}">${c.name}</a></td><td>${c.type}</td><td>${c.state}</td><td>${c.tasks}</td><td><button class="btn btn-sm btn-danger" onclick="deleteConnector('${c.name}')">Delete</button></td></tr>`).join('') +
                    '</tbody></table>';
            } else { el.innerHTML = '<p>No connectors configured. <a href="#" onclick="showCreateForm()">Create one</a>.</p>'; }
        });
        </script></div>"#;
    Html(state.templates.layout("Connectors", content, "connectors"))
}

async fn connector_details_page(
    State(state): State<WebUiState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let content = format!(
        r#"<div class="page-header"><h1>🔌 Connector: {name}</h1></div>
        <div class="grid grid-cols-2 gap-4">
            <div class="card"><h3>Status</h3><div id="status">Loading...</div></div>
            <div class="card"><h3>Configuration</h3><pre id="config">Loading...</pre></div>
        </div>
        <script>
        fetch('/api/v1/connectors/{name}').then(r => r.json()).then(data => {{
            document.getElementById('status').innerHTML = '<span class="badge">' + data.state + '</span>';
            document.getElementById('config').textContent = JSON.stringify(data.config, null, 2);
        }});
        </script>"#
    );
    Html(state.templates.layout(&format!("Connector: {}", name), &content, "connectors"))
}

async fn api_list_connectors_ui() -> Json<serde_json::Value> {
    Json(serde_json::json!({"connectors": []}))
}

async fn api_create_connector_ui(
    Json(payload): Json<serde_json::Value>,
) -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::CREATED, Json(payload))
}

async fn api_get_connector_ui(Path(name): Path<String>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "name": name,
        "state": "RUNNING",
        "config": {},
        "tasks": []
    }))
}

async fn api_delete_connector_ui(Path(name): Path<String>) -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn api_restart_connector_ui(Path(name): Path<String>) -> StatusCode {
    StatusCode::NO_CONTENT
}

// ── SPA Shell ────────────────────────────────────────────────────────────────

async fn spa_shell_page(State(state): State<WebUiState>) -> impl IntoResponse {
    let content = r#"<div id="spa-root">
        <nav class="spa-nav">
            <a href="/app/topics" class="nav-link" data-route="topics">Topics</a>
            <a href="/app/groups" class="nav-link" data-route="groups">Consumer Groups</a>
            <a href="/app/streamql" class="nav-link" data-route="streamql">StreamQL</a>
            <a href="/app/connectors" class="nav-link" data-route="connectors">Connectors</a>
            <a href="/app/schemas" class="nav-link" data-route="schemas">Schemas</a>
            <a href="/app/lag" class="nav-link" data-route="lag">Lag Heatmap</a>
        </nav>
        <main id="spa-content"><p>Loading...</p></main>
    </div>
    <script>
    // Minimal SPA router — intercepts nav clicks and loads content via API
    document.querySelectorAll('.nav-link').forEach(link => {
        link.addEventListener('click', e => {
            e.preventDefault();
            const route = link.dataset.route;
            history.pushState({route}, '', link.href);
            loadRoute(route);
        });
    });
    window.onpopstate = e => { if (e.state) loadRoute(e.state.route); };
    async function loadRoute(route) {
        const el = document.getElementById('spa-content');
        el.innerHTML = '<p>Loading ' + route + '...</p>';
        try {
            const endpoints = {
                topics: '/api/topics', groups: '/api/consumer-groups',
                streamql: '/api/v1/streamql/views', connectors: '/api/v1/connectors',
                schemas: '/api/schemas', lag: '/api/v1/lag-heatmap/data',
            };
            const res = await fetch(endpoints[route] || '/api/topics');
            const data = await res.json();
            el.innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
        } catch(err) { el.innerHTML = '<p class="error">' + err.message + '</p>'; }
    }
    // Load initial route from URL
    const initial = window.location.pathname.replace('/app/', '') || 'topics';
    loadRoute(initial);
    </script>"#;
    Html(state.templates.layout("Streamline Dashboard", content, "app"))
}

// ── Canvas Lag Heatmap API ───────────────────────────────────────────────────

async fn api_lag_heatmap_data() -> Json<serde_json::Value> {
    // Returns structured data for a canvas-based heatmap renderer
    Json(serde_json::json!({
        "groups": [],
        "topics": [],
        "cells": [],
        "meta": {
            "severity_thresholds": {
                "low": 100,
                "medium": 1000,
                "high": 10000,
                "critical": 100000
            },
            "colors": {
                "low": "#22c55e",
                "medium": "#eab308",
                "high": "#f97316",
                "critical": "#ef4444",
                "empty": "#374151"
            }
        }
    }))
}

async fn api_cluster_topology() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "nodes": [{
            "id": 0,
            "host": "localhost",
            "port": 9092,
            "role": "leader",
            "status": "online",
            "partitions_led": 0,
            "partitions_followed": 0,
        }],
        "topics": [],
        "connections": 0,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_router() {
        let config = super::super::WebUiConfig::default();
        let state = WebUiState::new(config).unwrap();
        let router = create_router(state);
        // Router creation should not panic
        assert!(std::mem::size_of_val(&router) > 0);
    }

    #[test]
    fn test_search_query_fields() {
        // Test that SearchQuery struct has the expected fields
        let query = SearchQuery {
            q: "test".to_string(),
            entity_type: None,
        };
        assert_eq!(query.q, "test");
        assert!(query.entity_type.is_none());

        let query_with_type = SearchQuery {
            q: "topic".to_string(),
            entity_type: Some("topic".to_string()),
        };
        assert_eq!(query_with_type.q, "topic");
        assert_eq!(query_with_type.entity_type, Some("topic".to_string()));
    }

    #[test]
    fn test_search_result_types() {
        let result = SearchResult {
            result_type: SearchResultType::Topic,
            name: "test-topic".to_string(),
            url: "/topics/test-topic".to_string(),
            description: "3 partitions".to_string(),
            icon: "database".to_string(),
        };
        assert_eq!(result.result_type, SearchResultType::Topic);
        assert_eq!(result.name, "test-topic");
    }
}
