//! Streaming Debugger & Time-Travel Inspector
//!
//! Provides interactive debugging tools for stream data:
//! - Browse events by topic/partition/offset
//! - Set breakpoints on message patterns with rich conditions
//! - Step through events forward/backward
//! - Deep-inspect message format, size, headers, and provenance
//! - Diff consumer group states and topic configs
//! - Capture and compare point-in-time snapshots
//! - Replay specific offset ranges for time-travel debugging
//!
//! # Example
//!
//! ```rust
//! use streamline::debugger::{DebuggerConfig, Breakpoint, BreakpointCondition, LogicalOp};
//!
//! let config = DebuggerConfig::default();
//! let bp = Breakpoint::pattern("error");
//! assert_eq!(config.buffer_size, 1000);
//!
//! let bp2 = Breakpoint::from_condition("errors-on-p0", BreakpointCondition::Composite {
//!     conditions: vec![
//!         BreakpointCondition::OnPartition { id: 0 },
//!         BreakpointCondition::OnValue { pattern: "error".to_string() },
//!     ],
//!     op: LogicalOp::And,
//! });
//! ```

pub mod breakpoint;
pub mod inspector;
pub mod state_diff;

pub use breakpoint::{
    Breakpoint, BreakpointAction, BreakpointCondition, BreakpointManager, BreakpointMatch,
    BreakpointType, LogicalOp, PatternBreakpoint, evaluate_condition,
};
pub use inspector::{
    ChangeType, DebuggerConfig, DetectedFormat, EventView, FieldDiff, HeaderAnalysis,
    InspectionResult, MessageDiff, MessageInspection, MessageInspector, MessageTrace,
    NavigationDirection, SizeBreakdown, detect_format, diff_messages, inspect_message,
    trace_message,
};
pub use state_diff::{
    ConfigChange, ConfigChangeType, ConfigDiff, ConsumerStateDiff, ConsumerStateSnapshot,
    DebugSnapshot, GroupSnapshot, GroupStateDiff, MemberChange, MemberChangeType, OffsetDiff,
    PartitionReassignment, SnapshotDiff, StateDiffEngine, TopicConfigSnapshot,
};

use crate::error::Result;
use crate::storage::TopicManager;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// State of the debugger for a single topic+partition.
#[derive(Debug, Clone, Copy, PartialEq)]
enum DebugState {
    Running,
    Paused { offset: i64 },
}

/// Identifies a topic+partition being debugged.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
struct DebugTarget {
    topic: String,
    partition: i32,
}

/// Coordinator for the streaming debugger.
pub struct StreamDebugger {
    topic_manager: Arc<TopicManager>,
    breakpoint_manager: BreakpointManager,
    targets: HashMap<DebugTarget, DebugState>,
    snapshots: Vec<DebugSnapshot>,
    session_log: Vec<DebugSessionEntry>,
    config: DebuggerConfig,
}

/// An entry in the debug session log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugSessionEntry {
    pub timestamp_ms: i64,
    pub event: String,
    pub topic: Option<String>,
    pub partition: Option<i32>,
    pub offset: Option<i64>,
}

/// Exported debug session (JSON-serializable).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugSessionExport {
    pub exported_at_ms: i64,
    pub breakpoints: Vec<Breakpoint>,
    pub attached_topics: Vec<(String, i32)>,
    pub snapshots: Vec<DebugSnapshot>,
    pub log: Vec<DebugSessionEntry>,
}

/// Result of stepping through a breakpoint.
#[derive(Debug)]
pub struct StepResult {
    pub record: Option<EventView>,
    pub matches: Vec<BreakpointMatch>,
    pub paused: bool,
}

impl StreamDebugger {
    pub fn new(topic_manager: Arc<TopicManager>, config: DebuggerConfig) -> Self {
        Self {
            topic_manager, breakpoint_manager: BreakpointManager::new(),
            targets: HashMap::new(), snapshots: Vec::new(), session_log: Vec::new(), config,
        }
    }

    pub fn with_rate_limit(topic_manager: Arc<TopicManager>, config: DebuggerConfig, max_hits_per_second: u64) -> Self {
        Self {
            topic_manager, breakpoint_manager: BreakpointManager::with_rate_limit(max_hits_per_second),
            targets: HashMap::new(), snapshots: Vec::new(), session_log: Vec::new(), config,
        }
    }

    pub fn attach_topic(&mut self, topic: impl Into<String>, partition: i32) {
        let topic = topic.into();
        let target = DebugTarget { topic: topic.clone(), partition };
        self.targets.insert(target, DebugState::Running);
        self.log_event(format!("Attached to {}:{}", topic, partition), Some(&topic), Some(partition), None);
    }

    pub fn detach_topic(&mut self, topic: &str, partition: i32) -> bool {
        let target = DebugTarget { topic: topic.to_string(), partition };
        let removed = self.targets.remove(&target).is_some();
        if removed {
            self.log_event(format!("Detached from {}:{}", topic, partition), Some(topic), Some(partition), None);
        }
        removed
    }

    pub fn attached_topics(&self) -> Vec<(String, i32)> {
        self.targets.keys().map(|t| (t.topic.clone(), t.partition)).collect()
    }

    pub fn is_attached(&self, topic: &str, partition: i32) -> bool {
        self.targets.contains_key(&DebugTarget { topic: topic.to_string(), partition })
    }

    pub fn add_breakpoint(&mut self, breakpoint: Breakpoint) -> u64 {
        let id = self.breakpoint_manager.add(breakpoint);
        self.log_event(format!("Added breakpoint {}", id), None, None, None);
        id
    }

    pub fn remove_breakpoint(&mut self, id: u64) -> bool {
        let removed = self.breakpoint_manager.remove(id);
        if removed { self.log_event(format!("Removed breakpoint {}", id), None, None, None); }
        removed
    }

    pub fn enable_breakpoint(&mut self, id: u64) -> bool { self.breakpoint_manager.enable(id) }
    pub fn disable_breakpoint(&mut self, id: u64) -> bool { self.breakpoint_manager.disable(id) }
    pub fn breakpoints(&self) -> &[Breakpoint] { self.breakpoint_manager.list() }
    pub fn breakpoint_manager(&mut self) -> &mut BreakpointManager { &mut self.breakpoint_manager }

    /// Advance one message past a breakpoint on the given topic+partition.
    pub fn step(&mut self, topic: &str, partition: i32) -> Result<StepResult> {
        let target = DebugTarget { topic: topic.to_string(), partition };
        let current_offset = match self.targets.get(&target) {
            Some(DebugState::Paused { offset }) => *offset,
            Some(DebugState::Running) => {
                self.topic_manager.earliest_offset(topic, partition).unwrap_or(0).saturating_sub(1)
            }
            None => return Ok(StepResult { record: None, matches: Vec::new(), paused: false }),
        };
        let next_offset = current_offset + 1;
        let records = self.topic_manager.read(topic, partition, next_offset, 1)?;
        if records.is_empty() {
            self.targets.insert(target, DebugState::Paused { offset: current_offset });
            return Ok(StepResult { record: None, matches: Vec::new(), paused: true });
        }
        let record = &records[0];
        let matches = self.breakpoint_manager.evaluate_all(record, partition);
        let event_view = EventView::from_record(topic, partition, record, self.config.pretty_json);
        self.targets.insert(target, DebugState::Paused { offset: record.offset });
        self.log_event(format!("Step to offset {}", record.offset), Some(topic), Some(partition), Some(record.offset));
        Ok(StepResult { record: Some(event_view), matches, paused: true })
    }

    /// Resume execution until the next breakpoint fires with Pause action.
    pub fn continue_(&mut self, topic: &str, partition: i32) -> Result<StepResult> {
        let target = DebugTarget { topic: topic.to_string(), partition };
        let current_offset = match self.targets.get(&target) {
            Some(DebugState::Paused { offset }) => *offset,
            Some(DebugState::Running) => {
                self.topic_manager.earliest_offset(topic, partition).unwrap_or(0).saturating_sub(1)
            }
            None => return Ok(StepResult { record: None, matches: Vec::new(), paused: false }),
        };
        let latest = self.topic_manager.latest_offset(topic, partition).unwrap_or(0);
        let batch_size = 100;
        let mut offset = current_offset + 1;
        while offset < latest {
            let records = self.topic_manager.read(topic, partition, offset, batch_size)?;
            if records.is_empty() { break; }
            for record in &records {
                let matches = self.breakpoint_manager.evaluate_all(record, partition);
                let should_pause = matches.iter().any(|m| matches!(m.action, BreakpointAction::Pause));
                if should_pause {
                    let event_view = EventView::from_record(topic, partition, record, self.config.pretty_json);
                    self.targets.insert(target, DebugState::Paused { offset: record.offset });
                    self.log_event(format!("Paused at offset {}", record.offset), Some(topic), Some(partition), Some(record.offset));
                    return Ok(StepResult { record: Some(event_view), matches, paused: true });
                }
            }
            offset = records.last().map(|r| r.offset + 1).unwrap_or(latest);
        }
        self.targets.insert(target, DebugState::Paused { offset: latest.saturating_sub(1) });
        self.log_event("Reached end of topic".to_string(), Some(topic), Some(partition), None);
        Ok(StepResult { record: None, matches: Vec::new(), paused: true })
    }

    /// Replay records from a topic+partition over an offset range.
    pub fn replay(&self, topic: &str, partition: i32, from_offset: i64, to_offset: i64) -> Result<Vec<EventView>> {
        let mut events = Vec::new();
        let batch_size = 100;
        let mut offset = from_offset;
        while offset <= to_offset {
            let records = self.topic_manager.read(topic, partition, offset, batch_size)?;
            if records.is_empty() { break; }
            for record in &records {
                if record.offset > to_offset { break; }
                events.push(EventView::from_record(topic, partition, record, self.config.pretty_json));
            }
            offset = records.last().map(|r| r.offset + 1).unwrap_or(to_offset + 1);
        }
        Ok(events)
    }

    /// Deep-inspect a single record at a given offset.
    pub fn inspect_at(&self, topic: &str, partition: i32, offset: i64) -> Result<Option<MessageInspection>> {
        let records = self.topic_manager.read(topic, partition, offset, 1)?;
        Ok(records.first().map(inspect_message))
    }

    /// Diff two records by topic+partition+offset.
    pub fn diff_at(&self, topic: &str, partition: i32, offset_a: i64, offset_b: i64) -> Result<Option<MessageDiff>> {
        let records_a = self.topic_manager.read(topic, partition, offset_a, 1)?;
        let records_b = self.topic_manager.read(topic, partition, offset_b, 1)?;
        match (records_a.first(), records_b.first()) {
            (Some(a), Some(b)) => Ok(Some(diff_messages(a, b))),
            _ => Ok(None),
        }
    }

    /// Trace a record's provenance at a given offset.
    pub fn trace_at(&self, topic: &str, partition: i32, offset: i64) -> Result<Option<MessageTrace>> {
        let records = self.topic_manager.read(topic, partition, offset, 1)?;
        Ok(records.first().map(trace_message))
    }

    /// Capture a debug snapshot.
    pub fn capture_snapshot(
        &mut self, label: impl Into<String>,
        consumer_state: ConsumerStateSnapshot,
        topic_configs: HashMap<String, TopicConfigSnapshot>,
    ) -> usize {
        let label = label.into();
        let mut high_watermarks: HashMap<String, HashMap<i32, i64>> = HashMap::new();
        for target in self.targets.keys() {
            let hw = self.topic_manager.latest_offset(&target.topic, target.partition).unwrap_or(0);
            high_watermarks.entry(target.topic.clone()).or_default().insert(target.partition, hw);
        }
        let timestamp_ms = chrono::Utc::now().timestamp_millis();
        let snapshot = StateDiffEngine::create_snapshot(&label, timestamp_ms, consumer_state, topic_configs, high_watermarks);
        self.snapshots.push(snapshot);
        let idx = self.snapshots.len() - 1;
        self.log_event(format!("Captured snapshot '{}' (index {})", label, idx), None, None, None);
        idx
    }

    pub fn compare_snapshots(&self, idx_a: usize, idx_b: usize) -> Option<SnapshotDiff> {
        let a = self.snapshots.get(idx_a)?;
        let b = self.snapshots.get(idx_b)?;
        Some(StateDiffEngine::compare_snapshots(a, b))
    }

    pub fn get_snapshot(&self, idx: usize) -> Option<&DebugSnapshot> { self.snapshots.get(idx) }
    pub fn snapshot_count(&self) -> usize { self.snapshots.len() }

    /// Export the entire debug session as a JSON-serializable struct.
    pub fn export_session(&self) -> DebugSessionExport {
        DebugSessionExport {
            exported_at_ms: chrono::Utc::now().timestamp_millis(),
            breakpoints: self.breakpoint_manager.list().to_vec(),
            attached_topics: self.attached_topics(),
            snapshots: self.snapshots.clone(),
            log: self.session_log.clone(),
        }
    }

    /// Export the session as a JSON string.
    pub fn export_session_json(&self) -> Result<String> {
        let export = self.export_session();
        serde_json::to_string_pretty(&export)
            .map_err(|e| crate::error::StreamlineError::InvalidData(e.to_string()))
    }

    fn log_event(&mut self, event: impl Into<String>, topic: Option<&str>, partition: Option<i32>, offset: Option<i64>) {
        self.session_log.push(DebugSessionEntry {
            timestamp_ms: chrono::Utc::now().timestamp_millis(),
            event: event.into(), topic: topic.map(|s| s.to_string()), partition, offset,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::embedded::EmbeddedStreamline;
    use bytes::Bytes;

    fn setup_instance(topic: &str, count: usize) -> EmbeddedStreamline {
        let instance = EmbeddedStreamline::in_memory().expect("in-memory instance");
        instance.create_topic(topic, 1).expect("create topic");
        for i in 0..count {
            let msg = if i == 5 { r#"{"level":"error","msg":"fail"}"#.to_string() }
                else { format!(r#"{{"level":"info","seq":{}}}"#, i) };
            instance.produce(topic, 0, None, Bytes::from(msg)).expect("produce");
        }
        instance
    }

    #[test]
    fn test_debugger_attach_detach() {
        let instance = setup_instance("dbg-test", 5);
        let mut dbg = StreamDebugger::new(instance.topic_manager().clone(), DebuggerConfig::default());
        dbg.attach_topic("dbg-test", 0);
        assert!(dbg.is_attached("dbg-test", 0));
        assert_eq!(dbg.attached_topics().len(), 1);
        assert!(dbg.detach_topic("dbg-test", 0));
        assert!(!dbg.is_attached("dbg-test", 0));
        assert!(!dbg.detach_topic("dbg-test", 0));
    }

    #[test]
    fn test_debugger_step() {
        let instance = setup_instance("step-test", 10);
        let mut dbg = StreamDebugger::new(instance.topic_manager().clone(), DebuggerConfig::default());
        dbg.attach_topic("step-test", 0);
        let result = dbg.step("step-test", 0).expect("step");
        assert!(result.record.is_some());
        assert!(result.paused);
        assert_eq!(result.record.as_ref().map(|r| r.offset), Some(0));
        let result = dbg.step("step-test", 0).expect("step");
        assert_eq!(result.record.as_ref().map(|r| r.offset), Some(1));
    }

    #[test]
    fn test_debugger_continue_to_breakpoint() {
        let instance = setup_instance("cont-test", 10);
        let mut dbg = StreamDebugger::new(instance.topic_manager().clone(), DebuggerConfig::default());
        dbg.attach_topic("cont-test", 0);
        dbg.add_breakpoint(Breakpoint::from_condition("error-bp",
            BreakpointCondition::OnValue { pattern: "error".to_string() }));
        let result = dbg.continue_("cont-test", 0).expect("continue");
        assert!(result.paused);
        assert!(result.record.is_some());
        assert_eq!(result.record.as_ref().map(|r| r.offset), Some(5));
        assert!(!result.matches.is_empty());
    }

    #[test]
    fn test_debugger_replay() {
        let instance = setup_instance("replay-test", 10);
        let dbg = StreamDebugger::new(instance.topic_manager().clone(), DebuggerConfig::default());
        let events = dbg.replay("replay-test", 0, 2, 6).expect("replay");
        assert_eq!(events.len(), 5);
        assert_eq!(events[0].offset, 2);
        assert_eq!(events[4].offset, 6);
    }

    #[test]
    fn test_debugger_inspect_at() {
        let instance = setup_instance("inspect-test", 10);
        let dbg = StreamDebugger::new(instance.topic_manager().clone(), DebuggerConfig::default());
        let inspection = dbg.inspect_at("inspect-test", 0, 0).expect("inspect").expect("record found");
        assert_eq!(inspection.format, DetectedFormat::Json);
        assert_eq!(inspection.offset, 0);
    }

    #[test]
    fn test_debugger_diff_at() {
        let instance = setup_instance("diff-test", 10);
        let dbg = StreamDebugger::new(instance.topic_manager().clone(), DebuggerConfig::default());
        let diff = dbg.diff_at("diff-test", 0, 0, 1).expect("diff").expect("both records found");
        assert_eq!(diff.offset_a, 0);
        assert_eq!(diff.offset_b, 1);
        assert!(!diff.value_diffs.is_empty());
    }

    #[test]
    fn test_debugger_snapshot_and_compare() {
        let instance = setup_instance("snap-test", 10);
        let mut dbg = StreamDebugger::new(instance.topic_manager().clone(), DebuggerConfig::default());
        dbg.attach_topic("snap-test", 0);
        let state1 = ConsumerStateSnapshot { timestamp_ms: 1000, groups: HashMap::new() };
        let idx1 = dbg.capture_snapshot("snapshot-1", state1, HashMap::new());
        let state2 = ConsumerStateSnapshot { timestamp_ms: 2000, groups: HashMap::new() };
        let idx2 = dbg.capture_snapshot("snapshot-2", state2, HashMap::new());
        assert_eq!(dbg.snapshot_count(), 2);
        assert!(dbg.compare_snapshots(idx1, idx2).is_some());
    }

    #[test]
    fn test_debugger_export_session() {
        let instance = setup_instance("export-test", 5);
        let mut dbg = StreamDebugger::new(instance.topic_manager().clone(), DebuggerConfig::default());
        dbg.attach_topic("export-test", 0);
        dbg.add_breakpoint(Breakpoint::pattern("test"));
        let export = dbg.export_session();
        assert_eq!(export.breakpoints.len(), 1);
        assert_eq!(export.attached_topics.len(), 1);
        assert!(!export.log.is_empty());
        let json = dbg.export_session_json().expect("json export");
        assert!(json.contains("export-test"));
    }

    #[test]
    fn test_debugger_breakpoint_management() {
        let instance = setup_instance("bp-mgmt", 5);
        let mut dbg = StreamDebugger::new(instance.topic_manager().clone(), DebuggerConfig::default());
        let id = dbg.add_breakpoint(Breakpoint::pattern("test"));
        assert_eq!(dbg.breakpoints().len(), 1);
        assert!(dbg.disable_breakpoint(id));
        assert!(!dbg.breakpoints()[0].enabled);
        assert!(dbg.enable_breakpoint(id));
        assert!(dbg.breakpoints()[0].enabled);
        assert!(dbg.remove_breakpoint(id));
        assert!(dbg.breakpoints().is_empty());
    }

    #[test]
    fn test_debugger_step_on_unattached_topic() {
        let instance = setup_instance("no-attach", 5);
        let mut dbg = StreamDebugger::new(instance.topic_manager().clone(), DebuggerConfig::default());
        let result = dbg.step("no-attach", 0).expect("step");
        assert!(result.record.is_none());
        assert!(!result.paused);
    }

    #[test]
    fn test_debugger_trace_at() {
        use crate::storage::Header;
        let instance = EmbeddedStreamline::in_memory().expect("instance");
        instance.create_topic("trace-test", 1).expect("create");
        instance.produce_with_headers("trace-test", 0, None, Bytes::from("event-data"),
            vec![Header { key: "correlation-id".to_string(), value: Bytes::from("corr-1") },
                 Header { key: "source".to_string(), value: Bytes::from("svc-a") }]).expect("produce");
        let dbg = StreamDebugger::new(instance.topic_manager().clone(), DebuggerConfig::default());
        let trace = dbg.trace_at("trace-test", 0, 0).expect("trace").expect("found");
        assert_eq!(trace.correlation_id.as_deref(), Some("corr-1"));
        assert_eq!(trace.source.as_deref(), Some("svc-a"));
    }
}
