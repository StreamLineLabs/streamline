//! Window Operations for Stream Processing
//!
//! Provides windowing support for stream aggregations including:
//! - Tumbling windows (fixed, non-overlapping)
//! - Sliding windows (fixed, overlapping)
//! - Session windows (activity-based, variable size)

use super::{DslError, DslRecord, DslResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Window types for stream processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WindowType {
    /// Fixed, non-overlapping windows
    Tumbling {
        /// Window duration in milliseconds
        duration_ms: u64,
    },
    /// Fixed, overlapping windows
    Sliding {
        /// Window size in milliseconds
        size_ms: u64,
        /// Slide interval in milliseconds
        slide_ms: u64,
    },
    /// Activity-based windows with gap timeout
    Session {
        /// Gap timeout in milliseconds
        gap_ms: u64,
        /// Maximum session duration (optional)
        max_duration_ms: Option<u64>,
    },
    /// Global window (all records)
    Global,
}

impl WindowType {
    /// Create a tumbling window
    pub fn tumbling(duration_ms: u64) -> Self {
        WindowType::Tumbling { duration_ms }
    }

    /// Create a sliding window
    pub fn sliding(size_ms: u64, slide_ms: u64) -> Self {
        WindowType::Sliding { size_ms, slide_ms }
    }

    /// Create a session window
    pub fn session(gap_ms: u64) -> Self {
        WindowType::Session {
            gap_ms,
            max_duration_ms: None,
        }
    }

    /// Create a session window with max duration
    pub fn session_with_max(gap_ms: u64, max_duration_ms: u64) -> Self {
        WindowType::Session {
            gap_ms,
            max_duration_ms: Some(max_duration_ms),
        }
    }
}

/// Window configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowConfig {
    /// Window type
    pub window_type: WindowType,
    /// Allowed lateness in milliseconds
    pub allowed_lateness_ms: u64,
    /// Watermark delay in milliseconds
    pub watermark_delay_ms: u64,
    /// Whether to emit early results
    pub emit_early: bool,
    /// Early emission interval (if emit_early is true)
    pub early_emission_interval_ms: Option<u64>,
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self {
            window_type: WindowType::Tumbling { duration_ms: 60000 },
            allowed_lateness_ms: 0,
            watermark_delay_ms: 0,
            emit_early: false,
            early_emission_interval_ms: None,
        }
    }
}

impl WindowConfig {
    /// Create a new window config
    pub fn new(window_type: WindowType) -> Self {
        Self {
            window_type,
            ..Default::default()
        }
    }

    /// Set allowed lateness
    pub fn with_allowed_lateness(mut self, lateness_ms: u64) -> Self {
        self.allowed_lateness_ms = lateness_ms;
        self
    }

    /// Set watermark delay
    pub fn with_watermark_delay(mut self, delay_ms: u64) -> Self {
        self.watermark_delay_ms = delay_ms;
        self
    }

    /// Enable early emissions
    pub fn with_early_emissions(mut self, interval_ms: u64) -> Self {
        self.emit_early = true;
        self.early_emission_interval_ms = Some(interval_ms);
        self
    }
}

/// Trait for window implementations
pub trait Window: Send + Sync {
    /// Add a record to the window
    fn add(&mut self, record: DslRecord) -> DslResult<()>;

    /// Get records in current window(s) that are ready to emit
    fn emit(&mut self, current_time: i64) -> DslResult<Vec<WindowOutput>>;

    /// Advance the watermark
    fn advance_watermark(&mut self, watermark: i64);

    /// Get current watermark
    fn watermark(&self) -> i64;

    /// Check if window has pending records
    fn has_pending(&self) -> bool;

    /// Clear all state
    fn clear(&mut self);
}

/// Output from a window
#[derive(Debug, Clone)]
pub struct WindowOutput {
    /// Window start time
    pub start: i64,
    /// Window end time
    pub end: i64,
    /// Records in the window
    pub records: Vec<DslRecord>,
    /// Is this a final emission (window closed)
    pub is_final: bool,
}

/// Tumbling window implementation
pub struct TumblingWindow {
    duration_ms: u64,
    allowed_lateness_ms: u64,
    watermark: i64,
    /// Windows indexed by start time
    windows: HashMap<i64, Vec<DslRecord>>,
}

impl TumblingWindow {
    pub fn new(duration_ms: u64) -> Self {
        Self {
            duration_ms,
            allowed_lateness_ms: 0,
            watermark: 0,
            windows: HashMap::new(),
        }
    }

    pub fn with_allowed_lateness(mut self, lateness_ms: u64) -> Self {
        self.allowed_lateness_ms = lateness_ms;
        self
    }

    fn window_start_for(&self, timestamp: i64) -> i64 {
        let duration = self.duration_ms as i64;
        (timestamp / duration) * duration
    }
}

impl Window for TumblingWindow {
    fn add(&mut self, record: DslRecord) -> DslResult<()> {
        let window_start = self.window_start_for(record.timestamp);

        // Check if record is too late
        let window_end = window_start + self.duration_ms as i64;
        if window_end + self.allowed_lateness_ms as i64 <= self.watermark {
            return Err(DslError::WindowError(format!(
                "Record too late: timestamp {} for window [{}, {}), watermark {}",
                record.timestamp, window_start, window_end, self.watermark
            )));
        }

        self.windows.entry(window_start).or_default().push(record);
        Ok(())
    }

    fn emit(&mut self, _current_time: i64) -> DslResult<Vec<WindowOutput>> {
        let mut outputs = Vec::new();
        let mut windows_to_remove = Vec::new();

        for (&start, records) in &self.windows {
            let end = start + self.duration_ms as i64;

            // Window is ready to emit if watermark has passed the end
            if end + self.allowed_lateness_ms as i64 <= self.watermark {
                outputs.push(WindowOutput {
                    start,
                    end,
                    records: records.clone(),
                    is_final: true,
                });
                windows_to_remove.push(start);
            }
        }

        for start in windows_to_remove {
            self.windows.remove(&start);
        }

        Ok(outputs)
    }

    fn advance_watermark(&mut self, watermark: i64) {
        self.watermark = watermark;
    }

    fn watermark(&self) -> i64 {
        self.watermark
    }

    fn has_pending(&self) -> bool {
        !self.windows.is_empty()
    }

    fn clear(&mut self) {
        self.windows.clear();
    }
}

/// Sliding window implementation
pub struct SlidingWindow {
    size_ms: u64,
    slide_ms: u64,
    allowed_lateness_ms: u64,
    watermark: i64,
    /// Records with their timestamps
    records: Vec<(i64, DslRecord)>,
    /// Last emitted slide
    last_emitted_slide: i64,
}

impl SlidingWindow {
    pub fn new(size_ms: u64, slide_ms: u64) -> Self {
        Self {
            size_ms,
            slide_ms,
            allowed_lateness_ms: 0,
            watermark: 0,
            records: Vec::new(),
            last_emitted_slide: 0,
        }
    }

    pub fn with_allowed_lateness(mut self, lateness_ms: u64) -> Self {
        self.allowed_lateness_ms = lateness_ms;
        self
    }

    #[allow(dead_code)]
    fn windows_for_timestamp(&self, timestamp: i64) -> Vec<(i64, i64)> {
        let slide = self.slide_ms as i64;
        let size = self.size_ms as i64;

        let mut windows = Vec::new();
        let earliest_window_start = ((timestamp - size) / slide + 1) * slide;
        let latest_window_start = (timestamp / slide) * slide;

        let mut start = earliest_window_start;
        while start <= latest_window_start {
            let end = start + size;
            if timestamp >= start && timestamp < end {
                windows.push((start, end));
            }
            start += slide;
        }

        windows
    }
}

impl Window for SlidingWindow {
    fn add(&mut self, record: DslRecord) -> DslResult<()> {
        // Check if record is within allowed lateness
        let oldest_valid = self.watermark - self.size_ms as i64 - self.allowed_lateness_ms as i64;
        if record.timestamp < oldest_valid {
            return Err(DslError::WindowError(format!(
                "Record too late: timestamp {}, oldest valid {}",
                record.timestamp, oldest_valid
            )));
        }

        self.records.push((record.timestamp, record));
        Ok(())
    }

    fn emit(&mut self, _current_time: i64) -> DslResult<Vec<WindowOutput>> {
        let mut outputs = Vec::new();
        let slide = self.slide_ms as i64;
        let size = self.size_ms as i64;

        // Calculate which slides should be emitted
        let current_slide = (self.watermark / slide) * slide;
        let mut slide_start = self.last_emitted_slide + slide;

        while slide_start <= current_slide - size {
            let window_start = slide_start;
            let window_end = window_start + size;

            // Collect records in this window
            let window_records: Vec<DslRecord> = self
                .records
                .iter()
                .filter(|(ts, _)| *ts >= window_start && *ts < window_end)
                .map(|(_, r)| r.clone())
                .collect();

            if !window_records.is_empty() {
                outputs.push(WindowOutput {
                    start: window_start,
                    end: window_end,
                    records: window_records,
                    is_final: true,
                });
            }

            self.last_emitted_slide = slide_start;
            slide_start += slide;
        }

        // Clean up old records
        let oldest_needed = self.watermark - size - self.allowed_lateness_ms as i64;
        self.records.retain(|(ts, _)| *ts >= oldest_needed);

        Ok(outputs)
    }

    fn advance_watermark(&mut self, watermark: i64) {
        self.watermark = watermark;
    }

    fn watermark(&self) -> i64 {
        self.watermark
    }

    fn has_pending(&self) -> bool {
        !self.records.is_empty()
    }

    fn clear(&mut self) {
        self.records.clear();
        self.last_emitted_slide = 0;
    }
}

/// Session window implementation
pub struct SessionWindow {
    gap_ms: u64,
    max_duration_ms: Option<u64>,
    allowed_lateness_ms: u64,
    watermark: i64,
    /// Sessions indexed by key
    sessions: HashMap<String, Session>,
    /// Key extractor field
    key_field: Option<String>,
}

/// A single session
#[derive(Debug, Clone)]
struct Session {
    start: i64,
    end: i64,
    records: Vec<DslRecord>,
}

impl SessionWindow {
    pub fn new(gap_ms: u64) -> Self {
        Self {
            gap_ms,
            max_duration_ms: None,
            allowed_lateness_ms: 0,
            watermark: 0,
            sessions: HashMap::new(),
            key_field: None,
        }
    }

    pub fn with_max_duration(mut self, max_duration_ms: u64) -> Self {
        self.max_duration_ms = Some(max_duration_ms);
        self
    }

    pub fn with_key_field(mut self, field: &str) -> Self {
        self.key_field = Some(field.to_string());
        self
    }

    pub fn with_allowed_lateness(mut self, lateness_ms: u64) -> Self {
        self.allowed_lateness_ms = lateness_ms;
        self
    }

    fn get_key(&self, record: &DslRecord) -> String {
        if let Some(ref field) = self.key_field {
            record
                .get(field)
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    _ => v.to_string(),
                })
                .unwrap_or_else(|| "default".to_string())
        } else {
            "default".to_string()
        }
    }
}

impl Window for SessionWindow {
    fn add(&mut self, record: DslRecord) -> DslResult<()> {
        let key = self.get_key(&record);
        let timestamp = record.timestamp;

        // Check if we need to handle an existing session
        let needs_new_session = if let Some(session) = self.sessions.get(&key) {
            // Check if record extends the session
            let extends = timestamp >= session.start - self.gap_ms as i64
                && timestamp <= session.end + self.gap_ms as i64;
            !extends
        } else {
            true
        };

        if needs_new_session {
            // If there was an old session, move it to closed state
            if let Some(old_session) = self.sessions.remove(&key) {
                let closed_key = format!("{}__closed_{}", key, old_session.end);
                self.sessions.insert(closed_key, old_session);
            }

            // Create new session
            self.sessions.insert(
                key,
                Session {
                    start: timestamp,
                    end: timestamp,
                    records: vec![record],
                },
            );
        } else {
            // Extend existing session
            if let Some(session) = self.sessions.get_mut(&key) {
                session.start = session.start.min(timestamp);
                session.end = session.end.max(timestamp);
                session.records.push(record);
            }
        }

        Ok(())
    }

    fn emit(&mut self, _current_time: i64) -> DslResult<Vec<WindowOutput>> {
        let mut outputs = Vec::new();
        let mut sessions_to_remove = Vec::new();

        for (key, session) in &self.sessions {
            let session_timeout = session.end + self.gap_ms as i64;
            let max_duration_exceeded = self
                .max_duration_ms
                .map(|max| session.end - session.start >= max as i64)
                .unwrap_or(false);

            if session_timeout + self.allowed_lateness_ms as i64 <= self.watermark
                || max_duration_exceeded
            {
                outputs.push(WindowOutput {
                    start: session.start,
                    end: session.end,
                    records: session.records.clone(),
                    is_final: true,
                });
                sessions_to_remove.push(key.clone());
            }
        }

        for key in sessions_to_remove {
            self.sessions.remove(&key);
        }

        Ok(outputs)
    }

    fn advance_watermark(&mut self, watermark: i64) {
        self.watermark = watermark;
    }

    fn watermark(&self) -> i64 {
        self.watermark
    }

    fn has_pending(&self) -> bool {
        !self.sessions.is_empty()
    }

    fn clear(&mut self) {
        self.sessions.clear();
    }
}

/// Create a window from configuration
pub fn create_window(config: &WindowConfig) -> Box<dyn Window> {
    match &config.window_type {
        WindowType::Tumbling { duration_ms } => {
            let mut window = TumblingWindow::new(*duration_ms);
            window.allowed_lateness_ms = config.allowed_lateness_ms;
            Box::new(window)
        }
        WindowType::Sliding { size_ms, slide_ms } => {
            let mut window = SlidingWindow::new(*size_ms, *slide_ms);
            window.allowed_lateness_ms = config.allowed_lateness_ms;
            Box::new(window)
        }
        WindowType::Session {
            gap_ms,
            max_duration_ms,
        } => {
            let mut window = SessionWindow::new(*gap_ms);
            if let Some(max_dur) = max_duration_ms {
                window.max_duration_ms = Some(*max_dur);
            }
            window.allowed_lateness_ms = config.allowed_lateness_ms;
            Box::new(window)
        }
        WindowType::Global => {
            // Global window is just a tumbling window with very large duration
            Box::new(TumblingWindow::new(i64::MAX as u64))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_record(timestamp: i64) -> DslRecord {
        DslRecord {
            key: None,
            value: serde_json::json!({"value": 1}),
            timestamp,
            headers: HashMap::new(),
            source_topic: "test".to_string(),
            source_partition: 0,
            source_offset: 0,
        }
    }

    #[test]
    fn test_tumbling_window() {
        let mut window = TumblingWindow::new(1000);

        // Add records at different times
        window.add(make_record(100)).unwrap();
        window.add(make_record(500)).unwrap();
        window.add(make_record(1100)).unwrap();
        window.add(make_record(1500)).unwrap();

        // Advance watermark past first window only (not including end of second)
        window.advance_watermark(1500);
        let outputs = window.emit(1500).unwrap();

        // Only first window [0, 1000) should emit (end + 0 <= 1500)
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].start, 0);
        assert_eq!(outputs[0].end, 1000);
        assert_eq!(outputs[0].records.len(), 2);

        // Advance watermark past second window
        window.advance_watermark(2500);
        let outputs = window.emit(2500).unwrap();

        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].start, 1000);
        assert_eq!(outputs[0].end, 2000);
        assert_eq!(outputs[0].records.len(), 2);
    }

    #[test]
    fn test_sliding_window() {
        let mut window = SlidingWindow::new(1000, 500);

        // Add records
        window.add(make_record(100)).unwrap();
        window.add(make_record(600)).unwrap();
        window.add(make_record(1100)).unwrap();

        // Advance watermark
        window.advance_watermark(2500);
        let outputs = window.emit(2500).unwrap();

        // Should have multiple overlapping windows
        assert!(!outputs.is_empty());
    }

    #[test]
    fn test_session_window() {
        let mut window = SessionWindow::new(500);

        // Add records in session
        window.add(make_record(100)).unwrap();
        window.add(make_record(300)).unwrap();
        window.add(make_record(600)).unwrap();

        // Gap > 500ms, new session
        window.add(make_record(1500)).unwrap();

        // Advance watermark past first session timeout (600 + 500 = 1100) but not second
        window.advance_watermark(1200);
        let outputs = window.emit(1200).unwrap();

        // Only the closed session should emit
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].records.len(), 3);
        assert_eq!(outputs[0].start, 100);
        assert_eq!(outputs[0].end, 600);

        // Now advance past the second session
        window.advance_watermark(2500);
        let outputs2 = window.emit(2500).unwrap();
        assert_eq!(outputs2.len(), 1);
        assert_eq!(outputs2[0].records.len(), 1);
    }

    #[test]
    fn test_tumbling_window_late_data() {
        let mut window = TumblingWindow::new(1000).with_allowed_lateness(500);

        window.add(make_record(1500)).unwrap();
        window.advance_watermark(1200);

        // Window [0, 1000) has end + lateness = 1500 > watermark 1200, so still accepted
        window.add(make_record(500)).unwrap();

        // Advance watermark past the lateness threshold
        window.advance_watermark(1600);

        // Window [0, 1000) has end + lateness = 1500 <= watermark 1600, so rejected
        let result = window.add(make_record(400));
        assert!(result.is_err());
    }

    #[test]
    fn test_window_config() {
        let config = WindowConfig::new(WindowType::tumbling(5000))
            .with_allowed_lateness(1000)
            .with_watermark_delay(500)
            .with_early_emissions(2000);

        assert_eq!(config.allowed_lateness_ms, 1000);
        assert_eq!(config.watermark_delay_ms, 500);
        assert!(config.emit_early);
        assert_eq!(config.early_emission_interval_ms, Some(2000));
    }
}
