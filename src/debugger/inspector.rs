//! Message inspector for browsing and stepping through events.
//!
//! Provides deep message inspection including format detection, size breakdown,
//! header analysis, message diffing, and provenance tracing.

use super::breakpoint::{Breakpoint, BreakpointMatch};
use crate::error::Result;
use crate::storage::{Record, TopicManager};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration for the stream debugger.
#[derive(Debug, Clone)]
pub struct DebuggerConfig {
    pub fetch_size: usize,
    pub buffer_size: usize,
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
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
    pub key: Option<String>,
    pub value: Option<String>,
    pub headers: Vec<(String, String)>,
    pub size_bytes: usize,
    pub is_json: bool,
    pub pretty_value: Option<String>,
}

impl EventView {
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
    pub events: Vec<EventView>,
    pub cursor_offset: i64,
    pub earliest_offset: i64,
    pub latest_offset: i64,
    pub breakpoint_matches: Vec<BreakpointMatch>,
    pub has_more_forward: bool,
    pub has_more_backward: bool,
}

/// Detected encoding/format of a message value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DetectedFormat {
    Json,
    Avro { schema_id: u32 },
    Protobuf,
    Plaintext,
    Binary,
    Empty,
}

/// Byte-level size breakdown of a record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SizeBreakdown {
    pub key_bytes: usize,
    pub value_bytes: usize,
    pub header_bytes: usize,
    pub overhead_bytes: usize,
    pub total_bytes: usize,
}

/// Analysis of a single header.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeaderAnalysis {
    pub key: String,
    pub value_display: String,
    pub value_bytes: usize,
    pub is_standard: bool,
}

const STANDARD_HEADERS: &[&str] = &[
    "content-type",
    "correlation-id",
    "causation-id",
    "trace-id",
    "span-id",
    "message-id",
    "source",
    "type",
    "subject",
    "schema-id",
    "schema-version",
    "timestamp",
    "ce_type",
    "ce_source",
    "ce_id",
    "ce_specversion",
];

/// Deep inspection result for a single message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageInspection {
    pub offset: i64,
    pub timestamp: i64,
    pub format: DetectedFormat,
    pub decoded_value: String,
    pub headers: Vec<HeaderAnalysis>,
    pub size: SizeBreakdown,
    pub schema_id: Option<u32>,
}

/// Difference between two messages at the field level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageDiff {
    pub offset_a: i64,
    pub offset_b: i64,
    pub key_diff: Option<FieldDiff>,
    pub value_diffs: Vec<FieldDiff>,
    pub header_diffs: Vec<FieldDiff>,
    pub size_a: usize,
    pub size_b: usize,
}

/// A single field-level difference.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDiff {
    pub path: String,
    pub value_a: Option<String>,
    pub value_b: Option<String>,
    pub change_type: ChangeType,
}

/// Type of change observed between two values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ChangeType {
    Added,
    Removed,
    Modified,
    Unchanged,
}

/// Provenance trace following correlation headers across messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageTrace {
    pub origin_offset: i64,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub trace_id: Option<String>,
    pub source: Option<String>,
    pub message_type: Option<String>,
}

/// Detect the encoding format of a byte slice.
pub fn detect_format(data: &[u8]) -> DetectedFormat {
    if data.is_empty() {
        return DetectedFormat::Empty;
    }
    if data.len() >= 5 && data[0] == 0x00 {
        let schema_id = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
        if schema_id > 0 && schema_id < 1_000_000 {
            return DetectedFormat::Avro { schema_id };
        }
    }
    if let Ok(s) = std::str::from_utf8(data) {
        let trimmed = s.trim();
        if (trimmed.starts_with('{') || trimmed.starts_with('['))
            && serde_json::from_str::<serde_json::Value>(trimmed).is_ok()
        {
            return DetectedFormat::Json;
        }
        return DetectedFormat::Plaintext;
    }
    if !data.is_empty() {
        let wire_type = data[0] & 0x07;
        let field_number = data[0] >> 3;
        if field_number > 0 && wire_type <= 5 {
            return DetectedFormat::Protobuf;
        }
    }
    DetectedFormat::Binary
}

/// Perform a deep inspection of a single record.
pub fn inspect_message(record: &Record) -> MessageInspection {
    let format = detect_format(&record.value);
    let decoded_value = match &format {
        DetectedFormat::Json => serde_json::from_slice::<serde_json::Value>(&record.value)
            .ok()
            .and_then(|val| serde_json::to_string_pretty(&val).ok())
            .unwrap_or_else(|| String::from_utf8_lossy(&record.value).to_string()),
        DetectedFormat::Avro { schema_id } => {
            format!(
                "<avro schema_id={} payload={} bytes>",
                schema_id,
                record.value.len().saturating_sub(5)
            )
        }
        DetectedFormat::Protobuf => format!("<protobuf {} bytes>", record.value.len()),
        DetectedFormat::Plaintext => String::from_utf8_lossy(&record.value).to_string(),
        DetectedFormat::Binary => {
            let preview: String = record
                .value
                .iter()
                .take(32)
                .map(|b| format!("{b:02x}"))
                .collect::<Vec<_>>()
                .join(" ");
            if record.value.len() > 32 {
                format!("{} ... ({} bytes total)", preview, record.value.len())
            } else {
                preview
            }
        }
        DetectedFormat::Empty => "<empty>".to_string(),
    };
    let headers: Vec<HeaderAnalysis> = record
        .headers
        .iter()
        .map(|h| {
            let value_display = std::str::from_utf8(&h.value)
                .map(|s| s.to_string())
                .unwrap_or_else(|_| {
                    h.value
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<Vec<_>>()
                        .join(" ")
                });
            let is_standard = STANDARD_HEADERS
                .iter()
                .any(|&sk| h.key.eq_ignore_ascii_case(sk));
            HeaderAnalysis {
                key: h.key.clone(),
                value_display,
                value_bytes: h.value.len(),
                is_standard,
            }
        })
        .collect();
    let key_bytes = record.key.as_ref().map(|k| k.len()).unwrap_or(0);
    let value_bytes = record.value.len();
    let header_bytes: usize = record
        .headers
        .iter()
        .map(|h| h.key.len() + h.value.len())
        .sum();
    let overhead_bytes = 32;
    let schema_id = headers
        .iter()
        .find(|h| {
            h.key.eq_ignore_ascii_case("schema-id") || h.key.eq_ignore_ascii_case("schema-version")
        })
        .and_then(|h| h.value_display.parse::<u32>().ok())
        .or(match &format {
            DetectedFormat::Avro { schema_id } => Some(*schema_id),
            _ => None,
        });
    MessageInspection {
        offset: record.offset,
        timestamp: record.timestamp,
        format,
        decoded_value,
        headers,
        size: SizeBreakdown {
            key_bytes,
            value_bytes,
            header_bytes,
            overhead_bytes,
            total_bytes: key_bytes + value_bytes + header_bytes + overhead_bytes,
        },
        schema_id,
    }
}

/// Compute a field-level diff between two records.
pub fn diff_messages(a: &Record, b: &Record) -> MessageDiff {
    let key_diff = {
        let ka = a
            .key
            .as_ref()
            .map(|k| String::from_utf8_lossy(k).to_string());
        let kb = b
            .key
            .as_ref()
            .map(|k| String::from_utf8_lossy(k).to_string());
        if ka != kb {
            Some(FieldDiff {
                path: "key".to_string(),
                value_a: ka,
                value_b: kb,
                change_type: match (&a.key, &b.key) {
                    (None, Some(_)) => ChangeType::Added,
                    (Some(_), None) => ChangeType::Removed,
                    _ => ChangeType::Modified,
                },
            })
        } else {
            None
        }
    };
    let value_diffs = diff_values(&a.value, &b.value);
    let header_diffs = diff_headers(&a.headers, &b.headers);
    let size_a = a.value.len() + a.key.as_ref().map(|k| k.len()).unwrap_or(0);
    let size_b = b.value.len() + b.key.as_ref().map(|k| k.len()).unwrap_or(0);
    MessageDiff {
        offset_a: a.offset,
        offset_b: b.offset,
        key_diff,
        value_diffs,
        header_diffs,
        size_a,
        size_b,
    }
}

fn diff_values(a: &[u8], b: &[u8]) -> Vec<FieldDiff> {
    if let (Ok(ja), Ok(jb)) = (
        serde_json::from_slice::<serde_json::Value>(a),
        serde_json::from_slice::<serde_json::Value>(b),
    ) {
        let mut diffs = Vec::new();
        json_diff("$", &ja, &jb, &mut diffs);
        return diffs;
    }
    let va = String::from_utf8_lossy(a).to_string();
    let vb = String::from_utf8_lossy(b).to_string();
    if va != vb {
        vec![FieldDiff {
            path: "value".to_string(),
            value_a: Some(va),
            value_b: Some(vb),
            change_type: ChangeType::Modified,
        }]
    } else {
        Vec::new()
    }
}

fn json_diff(path: &str, a: &serde_json::Value, b: &serde_json::Value, diffs: &mut Vec<FieldDiff>) {
    use serde_json::Value;
    match (a, b) {
        (Value::Object(ma), Value::Object(mb)) => {
            let all_keys: std::collections::HashSet<&String> = ma.keys().chain(mb.keys()).collect();
            for key in all_keys {
                let child_path = format!("{path}.{key}");
                match (ma.get(key), mb.get(key)) {
                    (Some(va), Some(vb)) => json_diff(&child_path, va, vb, diffs),
                    (Some(va), None) => diffs.push(FieldDiff {
                        path: child_path,
                        value_a: Some(va.to_string()),
                        value_b: None,
                        change_type: ChangeType::Removed,
                    }),
                    (None, Some(vb)) => diffs.push(FieldDiff {
                        path: child_path,
                        value_a: None,
                        value_b: Some(vb.to_string()),
                        change_type: ChangeType::Added,
                    }),
                    (None, None) => {}
                }
            }
        }
        (Value::Array(aa), Value::Array(ab)) => {
            let max_len = aa.len().max(ab.len());
            for i in 0..max_len {
                let child_path = format!("{path}[{i}]");
                match (aa.get(i), ab.get(i)) {
                    (Some(va), Some(vb)) => json_diff(&child_path, va, vb, diffs),
                    (Some(va), None) => diffs.push(FieldDiff {
                        path: child_path,
                        value_a: Some(va.to_string()),
                        value_b: None,
                        change_type: ChangeType::Removed,
                    }),
                    (None, Some(vb)) => diffs.push(FieldDiff {
                        path: child_path,
                        value_a: None,
                        value_b: Some(vb.to_string()),
                        change_type: ChangeType::Added,
                    }),
                    (None, None) => {}
                }
            }
        }
        _ => {
            if a != b {
                diffs.push(FieldDiff {
                    path: path.to_string(),
                    value_a: Some(a.to_string()),
                    value_b: Some(b.to_string()),
                    change_type: ChangeType::Modified,
                });
            }
        }
    }
}

fn diff_headers(a: &[crate::storage::Header], b: &[crate::storage::Header]) -> Vec<FieldDiff> {
    let a_map: HashMap<&str, &[u8]> = a
        .iter()
        .map(|h| (h.key.as_str(), h.value.as_ref()))
        .collect();
    let b_map: HashMap<&str, &[u8]> = b
        .iter()
        .map(|h| (h.key.as_str(), h.value.as_ref()))
        .collect();
    let all_keys: std::collections::HashSet<&str> =
        a_map.keys().copied().chain(b_map.keys().copied()).collect();
    let mut diffs = Vec::new();
    for key in all_keys {
        let path = format!("header:{key}");
        match (a_map.get(key), b_map.get(key)) {
            (Some(va), Some(vb)) if va != vb => diffs.push(FieldDiff {
                path,
                value_a: Some(String::from_utf8_lossy(va).to_string()),
                value_b: Some(String::from_utf8_lossy(vb).to_string()),
                change_type: ChangeType::Modified,
            }),
            (Some(va), None) => diffs.push(FieldDiff {
                path,
                value_a: Some(String::from_utf8_lossy(va).to_string()),
                value_b: None,
                change_type: ChangeType::Removed,
            }),
            (None, Some(vb)) => diffs.push(FieldDiff {
                path,
                value_a: None,
                value_b: Some(String::from_utf8_lossy(vb).to_string()),
                change_type: ChangeType::Added,
            }),
            _ => {}
        }
    }
    diffs
}

/// Extract provenance / correlation info from a record's headers.
pub fn trace_message(record: &Record) -> MessageTrace {
    let get_header = |key: &str| -> Option<String> {
        record
            .headers
            .iter()
            .find(|h| h.key.eq_ignore_ascii_case(key))
            .and_then(|h| std::str::from_utf8(&h.value).ok())
            .map(|s| s.to_string())
    };
    MessageTrace {
        origin_offset: record.offset,
        correlation_id: get_header("correlation-id").or_else(|| get_header("ce_id")),
        causation_id: get_header("causation-id"),
        trace_id: get_header("trace-id").or_else(|| get_header("traceparent")),
        source: get_header("source").or_else(|| get_header("ce_source")),
        message_type: get_header("type").or_else(|| get_header("ce_type")),
    }
}

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

    pub fn add_breakpoint(&mut self, breakpoint: Breakpoint) {
        self.breakpoints.push(breakpoint);
    }

    pub fn remove_breakpoint(&mut self, id: u64) -> bool {
        let before = self.breakpoints.len();
        self.breakpoints.retain(|bp| bp.id != id);
        self.breakpoints.len() < before
    }

    pub fn breakpoints(&self) -> &[Breakpoint] {
        &self.breakpoints
    }

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
                self.cursor = earliest;
            }
            NavigationDirection::NextBreakpoint => {
                self.cursor = self.find_next_breakpoint(self.cursor, latest)?;
            }
        }
        self.fetch_view(earliest, latest)
    }

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

    pub fn inspect_record_at(&self, offset: i64) -> Option<MessageInspection> {
        self.buffer
            .iter()
            .find(|r| r.offset == offset)
            .map(inspect_message)
    }

    pub fn diff_records(&self, offset_a: i64, offset_b: i64) -> Option<MessageDiff> {
        let a = self.buffer.iter().find(|r| r.offset == offset_a)?;
        let b = self.buffer.iter().find(|r| r.offset == offset_b)?;
        Some(diff_messages(a, b))
    }

    pub fn trace_record(&self, offset: i64) -> Option<MessageTrace> {
        self.buffer
            .iter()
            .find(|r| r.offset == offset)
            .map(trace_message)
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
                        breakpoint_name: bp.name.clone(),
                        matched_content: String::from_utf8(record.value.to_vec())
                            .unwrap_or_default(),
                        offset: record.offset,
                        partition: self.partition,
                        action: bp.action.clone(),
                        hit_count: bp.hit_count,
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
    use crate::storage::Header;
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
        let instance = EmbeddedStreamline::in_memory().expect("in-memory instance");
        instance
            .create_topic("debug-test", 1)
            .expect("create topic");
        for i in 0..20 {
            instance
                .produce(
                    "debug-test",
                    0,
                    None,
                    Bytes::from(format!(r#"{{"seq":{i}}}"#)),
                )
                .expect("produce");
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
        let result = inspector.inspect().expect("inspect");
        assert_eq!(result.events.len(), 5);
        assert_eq!(result.cursor_offset, 0);
        assert!(result.has_more_forward);
        let result = inspector
            .navigate(NavigationDirection::Forward)
            .expect("navigate");
        assert!(result.cursor_offset > 0);
    }

    #[test]
    fn test_breakpoint_navigation() {
        let instance = EmbeddedStreamline::in_memory().expect("in-memory instance");
        instance.create_topic("bp-test", 1).expect("create topic");
        for i in 0..10 {
            let msg = if i == 5 {
                r#"{"level":"error","msg":"fail"}"#.to_string()
            } else {
                format!(r#"{{"level":"info","msg":"ok-{i}"}}"#)
            };
            instance
                .produce("bp-test", 0, None, Bytes::from(msg))
                .expect("produce");
        }
        let mut inspector = MessageInspector::new(
            instance.topic_manager().clone(),
            "bp-test",
            0,
            DebuggerConfig::default(),
        );
        inspector.add_breakpoint(Breakpoint::pattern("error"));
        let result = inspector.inspect().expect("inspect");
        assert!(!result.breakpoint_matches.is_empty());
        assert_eq!(result.breakpoint_matches[0].offset, 5);
    }

    #[test]
    fn test_detect_format_json() {
        assert_eq!(detect_format(br#"{"key":"value"}"#), DetectedFormat::Json);
        assert_eq!(detect_format(br#"[1,2,3]"#), DetectedFormat::Json);
    }

    #[test]
    fn test_detect_format_plaintext() {
        assert_eq!(detect_format(b"hello world"), DetectedFormat::Plaintext);
    }

    #[test]
    fn test_detect_format_empty() {
        assert_eq!(detect_format(b""), DetectedFormat::Empty);
    }

    #[test]
    fn test_detect_format_avro() {
        let data = vec![0x00, 0x00, 0x00, 0x00, 0x2a, 0x01, 0x02];
        assert_eq!(detect_format(&data), DetectedFormat::Avro { schema_id: 42 });
    }

    #[test]
    fn test_inspect_message_json() {
        let record = Record {
            offset: 10,
            timestamp: 2000,
            key: Some(Bytes::from("user-123")),
            value: Bytes::from(r#"{"name":"Alice","age":30}"#),
            headers: vec![Header {
                key: "content-type".to_string(),
                value: Bytes::from("application/json"),
            }],
            crc: None,
        };
        let inspection = inspect_message(&record);
        assert_eq!(inspection.format, DetectedFormat::Json);
        assert!(inspection.decoded_value.contains("Alice"));
        assert_eq!(inspection.size.key_bytes, 8);
        assert_eq!(inspection.size.value_bytes, 25);
        assert!(inspection.headers[0].is_standard);
    }

    #[test]
    fn test_inspect_message_size_breakdown() {
        let record = Record {
            offset: 0,
            timestamp: 1000,
            key: Some(Bytes::from("key")),
            value: Bytes::from("value-data"),
            headers: vec![
                Header {
                    key: "h1".to_string(),
                    value: Bytes::from("v1"),
                },
                Header {
                    key: "h2".to_string(),
                    value: Bytes::from("v2"),
                },
            ],
            crc: None,
        };
        let inspection = inspect_message(&record);
        assert_eq!(inspection.size.key_bytes, 3);
        assert_eq!(inspection.size.value_bytes, 10);
        assert_eq!(inspection.size.header_bytes, 8);
        assert_eq!(inspection.size.total_bytes, 3 + 10 + 8 + 32);
    }

    #[test]
    fn test_diff_messages_json() {
        let a = Record {
            offset: 0,
            timestamp: 1000,
            key: Some(Bytes::from("key-1")),
            value: Bytes::from(r#"{"name":"Alice","age":30}"#),
            headers: Vec::new(),
            crc: None,
        };
        let b = Record {
            offset: 1,
            timestamp: 1001,
            key: Some(Bytes::from("key-1")),
            value: Bytes::from(r#"{"name":"Bob","age":30}"#),
            headers: Vec::new(),
            crc: None,
        };
        let diff = diff_messages(&a, &b);
        assert!(diff.key_diff.is_none());
        assert!(!diff.value_diffs.is_empty());
        let name_diff = diff
            .value_diffs
            .iter()
            .find(|d| d.path == "$.name")
            .expect("name diff");
        assert_eq!(name_diff.change_type, ChangeType::Modified);
    }

    #[test]
    fn test_diff_messages_key_change() {
        let a = Record {
            offset: 0,
            timestamp: 1000,
            key: Some(Bytes::from("key-a")),
            value: Bytes::from("same"),
            headers: Vec::new(),
            crc: None,
        };
        let b = Record {
            offset: 1,
            timestamp: 1001,
            key: Some(Bytes::from("key-b")),
            value: Bytes::from("same"),
            headers: Vec::new(),
            crc: None,
        };
        let diff = diff_messages(&a, &b);
        assert!(diff.key_diff.is_some());
        assert!(diff.value_diffs.is_empty());
    }

    #[test]
    fn test_trace_message() {
        let record = Record {
            offset: 5,
            timestamp: 3000,
            key: None,
            value: Bytes::from("event-data"),
            headers: vec![
                Header {
                    key: "correlation-id".to_string(),
                    value: Bytes::from("corr-abc"),
                },
                Header {
                    key: "causation-id".to_string(),
                    value: Bytes::from("cause-xyz"),
                },
                Header {
                    key: "trace-id".to_string(),
                    value: Bytes::from("trace-123"),
                },
                Header {
                    key: "source".to_string(),
                    value: Bytes::from("order-service"),
                },
                Header {
                    key: "type".to_string(),
                    value: Bytes::from("OrderCreated"),
                },
            ],
            crc: None,
        };
        let trace = trace_message(&record);
        assert_eq!(trace.origin_offset, 5);
        assert_eq!(trace.correlation_id.as_deref(), Some("corr-abc"));
        assert_eq!(trace.causation_id.as_deref(), Some("cause-xyz"));
        assert_eq!(trace.trace_id.as_deref(), Some("trace-123"));
        assert_eq!(trace.source.as_deref(), Some("order-service"));
        assert_eq!(trace.message_type.as_deref(), Some("OrderCreated"));
    }

    #[test]
    fn test_trace_message_empty_headers() {
        let record = Record {
            offset: 0,
            timestamp: 1000,
            key: None,
            value: Bytes::from("data"),
            headers: Vec::new(),
            crc: None,
        };
        let trace = trace_message(&record);
        assert!(trace.correlation_id.is_none());
    }

    #[test]
    fn test_diff_messages_json_field_added() {
        let a = Record {
            offset: 0,
            timestamp: 1000,
            key: None,
            value: Bytes::from(r#"{"name":"Alice"}"#),
            headers: Vec::new(),
            crc: None,
        };
        let b = Record {
            offset: 1,
            timestamp: 1001,
            key: None,
            value: Bytes::from(r#"{"name":"Alice","email":"a@b.com"}"#),
            headers: Vec::new(),
            crc: None,
        };
        let diff = diff_messages(&a, &b);
        let added = diff
            .value_diffs
            .iter()
            .find(|d| d.path.contains("email"))
            .expect("email diff");
        assert_eq!(added.change_type, ChangeType::Added);
    }

    #[test]
    fn test_standard_header_detection() {
        let record = Record {
            offset: 0,
            timestamp: 1000,
            key: None,
            value: Bytes::from("data"),
            headers: vec![
                Header {
                    key: "content-type".to_string(),
                    value: Bytes::from("text/plain"),
                },
                Header {
                    key: "x-custom-header".to_string(),
                    value: Bytes::from("custom"),
                },
            ],
            crc: None,
        };
        let inspection = inspect_message(&record);
        assert!(inspection.headers[0].is_standard);
        assert!(!inspection.headers[1].is_standard);
    }
}
