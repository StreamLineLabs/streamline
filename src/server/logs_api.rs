//! REST API for server log access
//!
//! This module provides endpoints for viewing and streaming server logs.
//!
//! ## Endpoints
//!
//! - `GET /api/v1/logs` - Get recent log entries
//! - `GET /api/v1/logs/stream` - SSE endpoint for real-time log streaming
//! - `GET /api/v1/logs/search` - Search log entries
//! - `DELETE /api/v1/logs` - Clear log buffer

use crate::server::log_buffer::{LogBuffer, LogEntry, LogLevel};
use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::sse::{Event, Sse},
    routing::get,
    Json, Router,
};
use futures_util::stream::Stream;
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, pin::Pin, sync::Arc, task::Poll, time::Duration};

/// Shared state for logs API
#[derive(Clone)]
pub(crate) struct LogsApiState {
    /// Log buffer
    pub buffer: Arc<LogBuffer>,
}

/// Query parameters for log retrieval
#[derive(Debug, Deserialize)]
pub struct LogsQuery {
    /// Minimum log level filter
    #[serde(default)]
    pub level: Option<String>,
    /// Maximum number of entries to return
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Only return entries after this ID
    #[serde(default)]
    pub after: Option<u64>,
}

fn default_limit() -> usize {
    100
}

/// Query parameters for log search
#[derive(Debug, Deserialize)]
pub struct LogSearchQuery {
    /// Search pattern
    pub q: String,
    /// Minimum log level filter
    #[serde(default)]
    pub level: Option<String>,
    /// Maximum number of entries to return
    #[serde(default = "default_limit")]
    pub limit: usize,
}

/// Response for log entries
#[derive(Debug, Serialize)]
pub struct LogsResponse {
    /// Log entries
    pub entries: Vec<LogEntry>,
    /// Total entries in buffer
    pub total: usize,
    /// Latest entry ID (for polling)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_id: Option<u64>,
}

/// Response for buffer clear
#[derive(Debug, Serialize)]
pub struct ClearLogsResponse {
    pub message: String,
    pub cleared_count: usize,
}

/// Create the logs API router
pub(crate) fn create_logs_api_router(state: LogsApiState) -> Router {
    Router::new()
        .route("/api/v1/logs", get(get_logs).delete(clear_logs))
        .route("/api/v1/logs/stream", get(stream_logs))
        .route("/api/v1/logs/search", get(search_logs))
        .with_state(state)
}

/// Get recent log entries
///
/// Query parameters:
/// - `level` - Minimum log level (TRACE, DEBUG, INFO, WARN, ERROR)
/// - `limit` - Maximum entries to return (default: 100)
/// - `after` - Only return entries with ID > this value
async fn get_logs(
    State(state): State<LogsApiState>,
    Query(query): Query<LogsQuery>,
) -> Json<LogsResponse> {
    let min_level = query.level.as_deref().and_then(LogLevel::parse);

    let entries = state
        .buffer
        .get_entries(min_level, query.limit, query.after);

    let response = LogsResponse {
        entries,
        total: state.buffer.len(),
        latest_id: state.buffer.latest_id(),
    };

    Json(response)
}

/// Search log entries by pattern
async fn search_logs(
    State(state): State<LogsApiState>,
    Query(query): Query<LogSearchQuery>,
) -> Json<LogsResponse> {
    let min_level = query.level.as_deref().and_then(LogLevel::parse);

    let entries = state.buffer.search(&query.q, min_level, query.limit);

    let response = LogsResponse {
        entries,
        total: state.buffer.len(),
        latest_id: state.buffer.latest_id(),
    };

    Json(response)
}

/// Clear log buffer
async fn clear_logs(State(state): State<LogsApiState>) -> (StatusCode, Json<ClearLogsResponse>) {
    let count = state.buffer.len();
    state.buffer.clear();

    (
        StatusCode::OK,
        Json(ClearLogsResponse {
            message: "Log buffer cleared".to_string(),
            cleared_count: count,
        }),
    )
}

/// Stream log entries via Server-Sent Events
///
/// Query parameters:
/// - `level` - Minimum log level (TRACE, DEBUG, INFO, WARN, ERROR)
async fn stream_logs(
    State(state): State<LogsApiState>,
    Query(query): Query<LogsQuery>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let min_level = query.level.as_deref().and_then(LogLevel::parse);

    // Create a stream using unfold for manual async iteration
    let stream = LogStream::new(state.buffer.clone(), min_level);

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

/// Custom stream for log entries
struct LogStream {
    buffer: Arc<LogBuffer>,
    min_level: Option<LogLevel>,
    last_id: u64,
    sent_initial: bool,
    sent_connected: bool,
    interval: Option<tokio::time::Interval>,
    poll_count: u8,
}

impl LogStream {
    fn new(buffer: Arc<LogBuffer>, min_level: Option<LogLevel>) -> Self {
        Self {
            buffer,
            min_level,
            last_id: 0,
            sent_initial: false,
            sent_connected: false,
            interval: None,
            poll_count: 0,
        }
    }
}

impl Stream for LogStream {
    type Item = Result<Event, Infallible>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // Send initial batch of recent entries
        if !self.sent_initial {
            self.sent_initial = true;
            let initial_entries = self.buffer.get_entries(self.min_level, 50, None);
            if !initial_entries.is_empty() {
                if let Some(last) = initial_entries.last() {
                    self.last_id = last.id;
                }
                let json = serde_json::to_string(&initial_entries).unwrap_or_default();
                return Poll::Ready(Some(Ok(Event::default().event("logs").data(json))));
            }
        }

        // Send connected event
        if !self.sent_connected {
            self.sent_connected = true;
            return Poll::Ready(Some(Ok(Event::default().event("connected").data("ok"))));
        }

        // Wait for interval (initialize if needed)
        let interval = self
            .interval
            .get_or_insert_with(|| tokio::time::interval(Duration::from_millis(500)));
        match Pin::new(interval).poll_tick(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(_) => {}
        }

        // Check for new entries
        let new_entries = self
            .buffer
            .get_entries(self.min_level, 100, Some(self.last_id));
        if !new_entries.is_empty() {
            if let Some(last) = new_entries.last() {
                self.last_id = last.id;
            }
            self.poll_count = 0;
            let json = serde_json::to_string(&new_entries).unwrap_or_default();
            return Poll::Ready(Some(Ok(Event::default().event("logs").data(json))));
        }

        // Send periodic heartbeat every 10 polls (5 seconds)
        self.poll_count = self.poll_count.saturating_add(1);
        if self.poll_count >= 10 {
            self.poll_count = 0;
            return Poll::Ready(Some(Ok(Event::default().comment("heartbeat"))));
        }

        // Reschedule wake
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::log_buffer::LogBufferConfig;

    fn create_test_state() -> LogsApiState {
        let config = LogBufferConfig {
            max_entries: 100,
            min_level: LogLevel::Trace,
        };
        LogsApiState {
            buffer: LogBuffer::with_config_shared(config),
        }
    }

    #[tokio::test]
    async fn test_get_logs() {
        let state = create_test_state();
        state.buffer.log(LogLevel::Info, "test", "Test message");

        let query = LogsQuery {
            level: None,
            limit: 10,
            after: None,
        };

        let response = get_logs(State(state), Query(query)).await;
        let Json(logs): Json<LogsResponse> = response;

        assert_eq!(logs.entries.len(), 1);
        assert_eq!(logs.entries[0].message, "Test message");
    }

    #[tokio::test]
    async fn test_search_logs() {
        let state = create_test_state();
        state.buffer.log(LogLevel::Info, "kafka", "Kafka connected");
        state.buffer.log(LogLevel::Info, "http", "HTTP request");
        state.buffer.log(LogLevel::Error, "kafka", "Kafka error");

        let query = LogSearchQuery {
            q: "kafka".to_string(),
            level: None,
            limit: 10,
        };

        let response = search_logs(State(state), Query(query)).await;
        let Json(logs): Json<LogsResponse> = response;

        assert_eq!(logs.entries.len(), 2);
    }

    #[tokio::test]
    async fn test_clear_logs() {
        let state = create_test_state();
        state.buffer.log(LogLevel::Info, "test", "Message 1");
        state.buffer.log(LogLevel::Info, "test", "Message 2");

        assert_eq!(state.buffer.len(), 2);

        let response = clear_logs(State(state.clone())).await;
        let (status, Json(result)): (StatusCode, Json<ClearLogsResponse>) = response;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(result.cleared_count, 2);
        assert_eq!(state.buffer.len(), 0);
    }
}
