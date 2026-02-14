//! Message inspector for browsing and stepping through events.

use super::breakpoint::{Breakpoint, BreakpointMatch};
use crate::error::Result;
use crate::storage::{Record, TopicManager};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Configuration for the stream debugger.
#[derive(Debug, Clone)]
pub struct DebuggerConfig {
    /// Maximum records to load per fetch
    pub fetch_size: usize,
    /// Scroll buffer size for back-navigation
    pub buffer_size: usize,
    /// Enable JSON pretty-printing
    pub pretty_json: bool,
}

impl Default for DebuggerConfig {
    fn default() -> Self {
        Self {
            fetch_size: 100,
            buffer_size: 1000,
            pretty_json: true,
        }
    }
}

/// Navigation direction for stepping through events.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NavigationDirection {
    Forward,
    Backward,
    JumpToOffset(i64),
    JumpToTimestamp(i64),
    NextBreakpoint,
}

/// A view of a single event for display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventView {
    /// Topic name
    pub topic: String,
    /// Partition number
    pub partition: i32,
    /// Record offset
    pub offset: i64,
    /// Record timestamp (ms since epoch)
    pub timestamp: i64,
    /// Message key (as UTF-8 string if possible)
    pub key: Option<String>,
    /// Message value (as UTF-8 string if possible)
    pub value: Option<String>,
    /// Message headers
    pub headers: Vec<(String, String)>,
    /// Approximate size in bytes
    pub size_bytes: usize,
    /// Whether the value is valid JSON
    pub is_json: bool,
    /// Pretty-formatted JSON (if applicable)
    pub pretty_value: Option<String>,
}

impl EventView {
    /// Create an EventView from a Record.
    pub fn from_record(topic: &str, partition: i32, record: &Record, pretty_json: bool) -> Self {
        let key = record
            .key
            .as_ref()
            .and_then(|k| String::from_utf8(k.to_vec()).ok());

        let value_str = String::from_utf8(record.value.to_vec()).ok();

        let is_json = value_str
            .as_ref()
            .map(|v| serde_json::from_str::<serde_json::Value>(v).is_ok())
            .unwrap_or(false);

        let pretty_value = if pretty_json && is_json {
            value_str.as_ref().and_then(|v| {
                serde_json::from_str::<serde_json::Value>(v)
                    .ok()
                    .and_then(|j| serde_json::to_string_pretty(&j).ok())
            })
        } else {
            None
        };

        let size_bytes = record.value.len() + record.key.as_ref().map(|k| k.len()).unwrap_or(0);

        let headers = record
            .headers
            .iter()
            .map(|h| (h.key.clone(), String::from_utf8_lossy(&h.value).to_string()))
            .collect();

        Self {
            topic: topic.to_string(),
            partition,
            offset: record.offset,
            timestamp: record.timestamp,
            key,
            value: value_str,
            headers,
            size_bytes,
            is_json,
            pretty_value,
        }
    }
}

/// Result of an inspection operation.
#[derive(Debug)]
pub struct InspectionResult {
    /// Events in the current view
    pub events: Vec<EventView>,
    /// Current cursor offset
    pub cursor_offset: i64,
    /// Earliest available offset
    pub earliest_offset: i64,
    /// Latest available offset
    pub latest_offset: i64,
    /// Breakpoint matches found
    pub breakpoint_matches: Vec<BreakpointMatch>,
    /// Whether there are more events forward
    pub has_more_forward: bool,
    /// Whether there are more events backward
    pub has_more_backward: bool,
}

/// Interactive message inspector for stream debugging.
pub struct MessageInspector {
    config: DebuggerConfig,
    topic_manager: Arc<TopicManager>,
    breakpoints: Vec<Breakpoint>,
    topic: String,
    partition: i32,
    cursor: i64,
    buffer: Vec<Record>,
}

impl MessageInspector {
    /// Create a new message inspector.
    pub fn new(
        topic_manager: Arc<TopicManager>,
        topic: impl Into<String>,
        partition: i32,
        config: DebuggerConfig,
    ) -> Self {
        Self {
            config,
            topic_manager,
            breakpoints: Vec::new(),
            topic: topic.into(),
            partition,
            cursor: 0,
            buffer: Vec::new(),
        }
    }

    /// Add a breakpoint.
    pub fn add_breakpoint(&mut self, breakpoint: Breakpoint) {
        self.breakpoints.push(breakpoint);
    }

    /// Remove a breakpoint by ID.
    pub fn remove_breakpoint(&mut self, id: u64) -> bool {
        let before = self.breakpoints.len();
        self.breakpoints.retain(|bp| bp.id != id);
        self.breakpoints.len() < before
    }

    /// List all breakpoints.
    pub fn breakpoints(&self) -> &[Breakpoint] {
        &self.breakpoints
    }

    /// Navigate in the specified direction and return the resulting view.
    pub fn navigate(&mut self, direction: NavigationDirection) -> Result<InspectionResult> {
        let earliest = self
            .topic_manager
            .earliest_offset(&self.topic, self.partition)
            .unwrap_or(0);
        let latest = self
            .topic_manager
            .latest_offset(&self.topic, self.partition)
            .unwrap_or(0);

        match direction {
            NavigationDirection::Forward => {
                self.cursor = (self.cursor + self.config.fetch_size as i64).min(latest);
            }
            NavigationDirection::Backward => {
                self.cursor = (self.cursor - self.config.fetch_size as i64).max(earliest);
            }
            NavigationDirection::JumpToOffset(offset) => {
                self.cursor = offset.max(earliest).min(latest);
            }
            NavigationDirection::JumpToTimestamp(_ts) => {
                // Timestamp-based seeking uses the existing ListOffsets logic
                // For now, jump to earliest as fallback
                self.cursor = earliest;
            }
            NavigationDirection::NextBreakpoint => {
                self.cursor = self.find_next_breakpoint(self.cursor, latest)?;
            }
        }

        self.fetch_view(earliest, latest)
    }

    /// Inspect the current position.
    pub fn inspect(&mut self) -> Result<InspectionResult> {
        let earliest = self
            .topic_manager
            .earliest_offset(&self.topic, self.partition)
            .unwrap_or(0);
        let latest = self
            .topic_manager
            .latest_offset(&self.topic, self.partition)
            .unwrap_or(0);

        if self.cursor < earliest {
            self.cursor = earliest;
        }

        self.fetch_view(earliest, latest)
    }

    fn fetch_view(&mut self, earliest: i64, latest: i64) -> Result<InspectionResult> {
        let records = self.topic_manager.read(
            &self.topic,
            self.partition,
            self.cursor,
            self.config.fetch_size,
        )?;

        let mut breakpoint_matches = Vec::new();
        for record in &records {
            for bp in &mut self.breakpoints {
                let key_bytes: Option<&[u8]> = record.key.as_deref();
                let value_bytes: &[u8] = &record.value;
                if bp.matches(record.offset, key_bytes, Some(value_bytes)) {
                    bp.hit_count += 1;
                    breakpoint_matches.push(BreakpointMatch {
                        breakpoint_id: bp.id,
                        matched_content: String::from_utf8(record.value.to_vec())
                            .unwrap_or_default(),
                        offset: record.offset,
                        partition: self.partition,
                    });
                }
            }
        }

        let events: Vec<EventView> = records
            .iter()
            .map(|r| {
                EventView::from_record(&self.topic, self.partition, r, self.config.pretty_json)
            })
            .collect();

        let has_more_forward = records
            .last()
            .map(|r| r.offset + 1 < latest)
            .unwrap_or(false);
        let has_more_backward = self.cursor > earliest;

        self.buffer = records;

        Ok(InspectionResult {
            events,
            cursor_offset: self.cursor,
            earliest_offset: earliest,
            latest_offset: latest,
            breakpoint_matches,
            has_more_forward,
            has_more_backward,
        })
    }

    fn find_next_breakpoint(&self, from: i64, max: i64) -> Result<i64> {
        let mut offset = from + 1;
        let batch_size = 100;

        while offset < max {
            let records =
                self.topic_manager
                    .read(&self.topic, self.partition, offset, batch_size)?;

            if records.is_empty() {
                break;
            }

            for record in &records {
                for bp in &self.breakpoints {
                    let key_bytes: Option<&[u8]> = record.key.as_deref();
                    let value_bytes: &[u8] = &record.value;
                    if bp.matches(record.offset, key_bytes, Some(value_bytes)) {
                        return Ok(record.offset);
                    }
                }
            }

            offset = records.last().map(|r| r.offset + 1).unwrap_or(max);
        }

        Ok(offset.min(max))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedded::EmbeddedStreamline;
    use bytes::Bytes;

    #[test]
    fn test_event_view_from_record() {
        let record = Record {
            offset: 0,
            timestamp: 1000,
            key: Some(Bytes::from("key-1")),
            value: Bytes::from(r#"{"id":1,"name":"test"}"#),
            headers: Vec::new(),
            crc: None,
        };

        let view = EventView::from_record("test-topic", 0, &record, true);
        assert_eq!(view.offset, 0);
        assert!(view.is_json);
        assert!(view.pretty_value.is_some());
        assert_eq!(view.key.as_deref(), Some("key-1"));
    }

    #[test]
    fn test_inspector_navigate() {
        let instance = EmbeddedStreamline::in_memory().unwrap();
        instance.create_topic("debug-test", 1).unwrap();

        for i in 0..20 {
            instance
                .produce(
                    "debug-test",
                    0,
                    None,
                    Bytes::from(format!(r#"{{"seq":{}}}"#, i)),
                )
                .unwrap();
        }

        let mut inspector = MessageInspector::new(
            instance.topic_manager().clone(),
            "debug-test",
            0,
            DebuggerConfig {
                fetch_size: 5,
                ..Default::default()
            },
        );

        let result = inspector.inspect().unwrap();
        assert_eq!(result.events.len(), 5);
        assert_eq!(result.cursor_offset, 0);
        assert!(result.has_more_forward);

        let result = inspector.navigate(NavigationDirection::Forward).unwrap();
        assert!(result.cursor_offset > 0);
    }

    #[test]
    fn test_breakpoint_navigation() {
        let instance = EmbeddedStreamline::in_memory().unwrap();
        instance.create_topic("bp-test", 1).unwrap();

        for i in 0..10 {
            let msg = if i == 5 {
                r#"{"level":"error","msg":"fail"}"#.to_string()
            } else {
                format!(r#"{{"level":"info","msg":"ok-{}"}}"#, i)
            };
            instance
                .produce("bp-test", 0, None, Bytes::from(msg))
                .unwrap();
        }

        let mut inspector = MessageInspector::new(
            instance.topic_manager().clone(),
            "bp-test",
            0,
            DebuggerConfig::default(),
        );
        inspector.add_breakpoint(Breakpoint::pattern("error"));

        let result = inspector.inspect().unwrap();
        assert!(!result.breakpoint_matches.is_empty());
        assert_eq!(result.breakpoint_matches[0].offset, 5);
    }
}
