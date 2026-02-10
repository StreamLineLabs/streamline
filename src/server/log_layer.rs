//! Custom tracing layer that captures log events into a LogBuffer.
//!
//! This layer integrates with the tracing subscriber to capture all log events
//! and store them in the LogBuffer for the Log Viewer UI.

use crate::server::log_buffer::{LogBuffer, LogEntry, LogLevel};
use std::sync::Arc;
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

/// A tracing layer that captures log events into a LogBuffer.
pub struct LogBufferLayer {
    buffer: Arc<LogBuffer>,
}

impl LogBufferLayer {
    /// Create a new LogBufferLayer with the given buffer.
    pub fn new(buffer: Arc<LogBuffer>) -> Self {
        Self { buffer }
    }
}

impl<S> Layer<S> for LogBufferLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        // Convert tracing level to our LogLevel
        let level = match *event.metadata().level() {
            Level::TRACE => LogLevel::Trace,
            Level::DEBUG => LogLevel::Debug,
            Level::INFO => LogLevel::Info,
            Level::WARN => LogLevel::Warn,
            Level::ERROR => LogLevel::Error,
        };

        // Get the target (module path)
        let target = event.metadata().target();

        // Extract the message and fields from the event
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);

        let message = visitor.message.unwrap_or_default();

        // Create and push the log entry
        if visitor.fields.is_empty() {
            self.buffer.log(level, target, &message);
        } else {
            let fields = serde_json::to_value(&visitor.fields).unwrap_or_default();
            let entry = LogEntry::with_fields(level, target, &message, fields);
            self.buffer.push(entry);
        }
    }
}

/// Visitor to extract message and fields from a tracing event.
#[derive(Default)]
struct MessageVisitor {
    message: Option<String>,
    fields: std::collections::HashMap<String, String>,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let value_str = format!("{:?}", value);
        if field.name() == "message" {
            self.message = Some(value_str);
        } else {
            self.fields.insert(field.name().to_string(), value_str);
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        } else {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.fields
            .insert(field.name().to_string(), value.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::layer::SubscriberExt;

    #[test]
    fn test_log_buffer_layer_captures_events() {
        let buffer = LogBuffer::new_shared();
        let layer = LogBufferLayer::new(buffer.clone());

        // Create a subscriber with our layer
        let subscriber = tracing_subscriber::registry().with(layer);

        // Use the subscriber for this scope
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("Test message");
            tracing::warn!(key = "value", "Warning with fields");
        });

        // Check that entries were captured
        let entries = buffer.get_entries(None, 100, None);
        assert_eq!(entries.len(), 2);

        // Check the first entry
        assert_eq!(entries[0].level, LogLevel::Info);
        assert!(entries[0].message.contains("Test message"));

        // Check the second entry
        assert_eq!(entries[1].level, LogLevel::Warn);
        assert!(entries[1].message.contains("Warning with fields"));
    }
}
