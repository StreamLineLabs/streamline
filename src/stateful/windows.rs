//! Window Operations
//!
//! Provides various window types for stream processing.

use super::{OperatorError, StreamElement};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};

/// Window configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowConfig {
    /// Window type
    pub window_type: WindowType,
    /// Window size (ms)
    pub size_ms: u64,
    /// Slide interval (ms) - for sliding/hopping windows
    pub slide_ms: Option<u64>,
    /// Session gap (ms) - for session windows
    pub gap_ms: Option<u64>,
    /// Max elements per window
    pub max_elements: Option<usize>,
    /// Allow late data
    pub allowed_lateness_ms: u64,
}

impl Default for WindowConfig {
    fn default() -> Self {
        Self {
            window_type: WindowType::Tumbling,
            size_ms: 60000, // 1 minute
            slide_ms: None,
            gap_ms: None,
            max_elements: None,
            allowed_lateness_ms: 0,
        }
    }
}

/// Window type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum WindowType {
    /// Fixed-size non-overlapping windows
    #[default]
    Tumbling,
    /// Fixed-size overlapping windows
    Sliding,
    /// Activity-based windows
    Session,
    /// Fixed-size with custom slide
    Hopping,
    /// Count-based windows
    Count,
    /// Global window (all data)
    Global,
}

/// Window trait
pub trait Window<T>: Send + Sync {
    /// Add element to window
    fn add(&mut self, element: StreamElement<T>) -> Result<(), OperatorError>;

    /// Trigger window and get output
    fn trigger(&mut self, watermark: i64) -> Result<Vec<WindowOutput<T>>, OperatorError>;

    /// Get window start time
    fn start(&self) -> i64;

    /// Get window end time
    fn end(&self) -> i64;

    /// Check if window is complete
    fn is_complete(&self, watermark: i64) -> bool;

    /// Get element count
    fn count(&self) -> usize;

    /// Clear window
    fn clear(&mut self);
}

/// Window output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowOutput<T> {
    /// Window start time
    pub window_start: i64,
    /// Window end time
    pub window_end: i64,
    /// Elements in window
    pub elements: Vec<T>,
    /// Window key (for keyed windows)
    pub key: Option<String>,
    /// Trigger time
    pub trigger_time: i64,
    /// Is late fire
    pub is_late: bool,
}

impl<T> WindowOutput<T> {
    /// Create a new window output
    pub fn new(start: i64, end: i64, elements: Vec<T>) -> Self {
        Self {
            window_start: start,
            window_end: end,
            elements,
            key: None,
            trigger_time: chrono::Utc::now().timestamp_millis(),
            is_late: false,
        }
    }
}

/// Tumbling window (fixed-size, non-overlapping)
#[derive(Debug, Clone)]
pub struct TumblingWindow<T> {
    /// Window size (ms)
    size_ms: u64,
    /// Current window elements by window start
    windows: HashMap<i64, Vec<StreamElement<T>>>,
    /// Allowed lateness
    allowed_lateness_ms: u64,
    /// Max elements per window
    max_elements: Option<usize>,
}

impl<T: Clone> TumblingWindow<T> {
    /// Create a new tumbling window
    pub fn new(size_ms: u64) -> Self {
        Self {
            size_ms,
            windows: HashMap::new(),
            allowed_lateness_ms: 0,
            max_elements: None,
        }
    }

    /// Set allowed lateness
    pub fn with_lateness(mut self, lateness_ms: u64) -> Self {
        self.allowed_lateness_ms = lateness_ms;
        self
    }

    /// Set max elements
    pub fn with_max_elements(mut self, max: usize) -> Self {
        self.max_elements = Some(max);
        self
    }

    /// Get window start for timestamp
    fn window_start(&self, timestamp: i64) -> i64 {
        (timestamp / self.size_ms as i64) * self.size_ms as i64
    }

    /// Get window end for timestamp (reserved for future use)
    #[allow(dead_code)]
    fn window_end(&self, timestamp: i64) -> i64 {
        self.window_start(timestamp) + self.size_ms as i64
    }
}

impl<T: Clone + Send + Sync> Window<T> for TumblingWindow<T> {
    fn add(&mut self, element: StreamElement<T>) -> Result<(), OperatorError> {
        let window_start = self.window_start(element.timestamp);
        let window = self.windows.entry(window_start).or_default();

        if let Some(max) = self.max_elements {
            if window.len() >= max {
                return Err(OperatorError::WindowError(
                    "Window element limit exceeded".to_string(),
                ));
            }
        }

        window.push(element);
        Ok(())
    }

    fn trigger(&mut self, watermark: i64) -> Result<Vec<WindowOutput<T>>, OperatorError> {
        let mut outputs = Vec::new();

        // Find windows that are complete
        let complete_windows: Vec<i64> = self
            .windows
            .keys()
            .filter(|&&start| {
                let end = start + self.size_ms as i64;
                watermark >= end + self.allowed_lateness_ms as i64
            })
            .cloned()
            .collect();

        for window_start in complete_windows {
            if let Some(elements) = self.windows.remove(&window_start) {
                let output = WindowOutput::new(
                    window_start,
                    window_start + self.size_ms as i64,
                    elements.into_iter().map(|e| e.value).collect(),
                );
                outputs.push(output);
            }
        }

        Ok(outputs)
    }

    fn start(&self) -> i64 {
        self.windows.keys().min().copied().unwrap_or(0)
    }

    fn end(&self) -> i64 {
        self.windows
            .keys()
            .max()
            .map(|s| s + self.size_ms as i64)
            .unwrap_or(0)
    }

    fn is_complete(&self, watermark: i64) -> bool {
        self.windows.is_empty() || watermark >= self.end()
    }

    fn count(&self) -> usize {
        self.windows.values().map(|v| v.len()).sum()
    }

    fn clear(&mut self) {
        self.windows.clear();
    }
}

/// Sliding window (fixed-size, overlapping)
#[derive(Debug, Clone)]
pub struct SlidingWindow<T> {
    /// Window size (ms)
    size_ms: u64,
    /// Slide interval (ms)
    slide_ms: u64,
    /// Elements buffer
    elements: VecDeque<StreamElement<T>>,
    /// Allowed lateness
    allowed_lateness_ms: u64,
}

impl<T: Clone> SlidingWindow<T> {
    /// Create a new sliding window
    pub fn new(size_ms: u64, slide_ms: u64) -> Self {
        Self {
            size_ms,
            slide_ms,
            elements: VecDeque::new(),
            allowed_lateness_ms: 0,
        }
    }

    /// Set allowed lateness
    pub fn with_lateness(mut self, lateness_ms: u64) -> Self {
        self.allowed_lateness_ms = lateness_ms;
        self
    }

    /// Get window assignments for timestamp (reserved for future use)
    #[allow(dead_code)]
    fn window_assignments(&self, timestamp: i64) -> Vec<(i64, i64)> {
        let mut assignments = Vec::new();
        let slide = self.slide_ms as i64;
        let size = self.size_ms as i64;

        // Find all windows this timestamp belongs to
        let mut window_start = (timestamp / slide) * slide - size + slide;
        while window_start <= timestamp {
            let window_end = window_start + size;
            if timestamp >= window_start && timestamp < window_end {
                assignments.push((window_start, window_end));
            }
            window_start += slide;
        }

        assignments
    }
}

impl<T: Clone + Send + Sync> Window<T> for SlidingWindow<T> {
    fn add(&mut self, element: StreamElement<T>) -> Result<(), OperatorError> {
        self.elements.push_back(element);
        Ok(())
    }

    fn trigger(&mut self, watermark: i64) -> Result<Vec<WindowOutput<T>>, OperatorError> {
        let mut outputs = Vec::new();
        let slide = self.slide_ms as i64;
        let size = self.size_ms as i64;

        // Find window ends that are triggered
        let mut window_start = 0i64;
        if let Some(first) = self.elements.front() {
            window_start = (first.timestamp / slide) * slide;
        }

        while window_start + size + self.allowed_lateness_ms as i64 <= watermark {
            let window_end = window_start + size;

            // Collect elements in this window
            let window_elements: Vec<T> = self
                .elements
                .iter()
                .filter(|e| e.timestamp >= window_start && e.timestamp < window_end)
                .map(|e| e.value.clone())
                .collect();

            if !window_elements.is_empty() {
                outputs.push(WindowOutput::new(window_start, window_end, window_elements));
            }

            window_start += slide;
        }

        // Remove expired elements
        let min_time = watermark - size - self.allowed_lateness_ms as i64;
        while let Some(front) = self.elements.front() {
            if front.timestamp < min_time {
                self.elements.pop_front();
            } else {
                break;
            }
        }

        Ok(outputs)
    }

    fn start(&self) -> i64 {
        self.elements.front().map(|e| e.timestamp).unwrap_or(0)
    }

    fn end(&self) -> i64 {
        self.elements
            .back()
            .map(|e| e.timestamp + self.size_ms as i64)
            .unwrap_or(0)
    }

    fn is_complete(&self, watermark: i64) -> bool {
        self.elements.is_empty() || watermark >= self.end()
    }

    fn count(&self) -> usize {
        self.elements.len()
    }

    fn clear(&mut self) {
        self.elements.clear();
    }
}

/// Session window (activity-based)
#[derive(Debug, Clone)]
pub struct SessionWindow<T> {
    /// Session gap (ms)
    gap_ms: u64,
    /// Active sessions by key (multiple sessions per key supported)
    sessions: HashMap<String, Vec<Session<T>>>,
    /// Allowed lateness
    allowed_lateness_ms: u64,
}

/// Active session
#[derive(Debug, Clone)]
struct Session<T> {
    /// Session start
    start: i64,
    /// Session end (last element time)
    end: i64,
    /// Elements
    elements: Vec<StreamElement<T>>,
}

impl<T: Clone> SessionWindow<T> {
    /// Create a new session window
    pub fn new(gap_ms: u64) -> Self {
        Self {
            gap_ms,
            sessions: HashMap::new(),
            allowed_lateness_ms: 0,
        }
    }

    /// Set allowed lateness
    pub fn with_lateness(mut self, lateness_ms: u64) -> Self {
        self.allowed_lateness_ms = lateness_ms;
        self
    }
}

impl<T: Clone + Send + Sync> Window<T> for SessionWindow<T> {
    fn add(&mut self, element: StreamElement<T>) -> Result<(), OperatorError> {
        let key = element.key.clone().unwrap_or_default();
        let timestamp = element.timestamp;

        let sessions = self.sessions.entry(key).or_default();

        // Find a session that this element belongs to
        let mut merged_into = None;
        for (i, session) in sessions.iter_mut().enumerate() {
            // Check if within this session's range (with gap tolerance)
            if timestamp >= session.start - self.gap_ms as i64
                && timestamp <= session.end + self.gap_ms as i64
            {
                // Extend session
                session.start = session.start.min(timestamp);
                session.end = session.end.max(timestamp);
                session.elements.push(element.clone());
                merged_into = Some(i);
                break;
            }
        }

        // If not merged into any existing session, create a new one
        if merged_into.is_none() {
            sessions.push(Session {
                start: timestamp,
                end: timestamp,
                elements: vec![element],
            });
        }

        Ok(())
    }

    fn trigger(&mut self, watermark: i64) -> Result<Vec<WindowOutput<T>>, OperatorError> {
        let mut outputs = Vec::new();

        for (key, sessions) in self.sessions.iter_mut() {
            // Find and remove closed sessions
            let mut i = 0;
            while i < sessions.len() {
                if watermark
                    >= sessions[i].end + self.gap_ms as i64 + self.allowed_lateness_ms as i64
                {
                    let session = sessions.remove(i);
                    let mut output = WindowOutput::new(
                        session.start,
                        session.end + self.gap_ms as i64,
                        session.elements.into_iter().map(|e| e.value).collect(),
                    );
                    output.key = Some(key.clone());
                    outputs.push(output);
                } else {
                    i += 1;
                }
            }
        }

        // Clean up empty keys
        self.sessions.retain(|_, v| !v.is_empty());

        Ok(outputs)
    }

    fn start(&self) -> i64 {
        self.sessions
            .values()
            .flat_map(|v| v.iter())
            .map(|s| s.start)
            .min()
            .unwrap_or(0)
    }

    fn end(&self) -> i64 {
        self.sessions
            .values()
            .flat_map(|v| v.iter())
            .map(|s| s.end)
            .max()
            .unwrap_or(0)
    }

    fn is_complete(&self, watermark: i64) -> bool {
        self.sessions.is_empty()
            || self
                .sessions
                .values()
                .flat_map(|v| v.iter())
                .all(|s| watermark >= s.end + self.gap_ms as i64)
    }

    fn count(&self) -> usize {
        self.sessions
            .values()
            .flat_map(|v| v.iter())
            .map(|s| s.elements.len())
            .sum()
    }

    fn clear(&mut self) {
        self.sessions.clear();
    }
}

/// Hopping window (fixed-size with custom slide)
#[allow(dead_code)]
pub type HoppingWindow<T> = SlidingWindow<T>;

/// Window trigger
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum WindowTrigger {
    /// Trigger on watermark (default)
    #[default]
    Watermark,
    /// Trigger on element count
    Count(usize),
    /// Trigger on processing time
    ProcessingTime { interval_ms: u64 },
    /// Early and final triggers
    EarlyAndFinal { early_interval_ms: u64 },
    /// Custom trigger interval
    Custom { interval_ms: u64 },
}

/// Window manager
pub struct WindowManager<T: Clone + Send + Sync + 'static> {
    /// Configuration
    config: WindowConfig,
    /// Active windows by type
    tumbling_windows: HashMap<String, TumblingWindow<T>>,
    sliding_windows: HashMap<String, SlidingWindow<T>>,
    session_windows: HashMap<String, SessionWindow<T>>,
    /// Current watermark
    watermark: i64,
    /// Windows to recompute
    recompute_queue: Vec<i64>,
}

impl<T: Clone + Send + Sync + 'static> WindowManager<T> {
    /// Create a new window manager
    pub fn new(config: WindowConfig) -> Self {
        Self {
            config,
            tumbling_windows: HashMap::new(),
            sliding_windows: HashMap::new(),
            session_windows: HashMap::new(),
            watermark: 0,
            recompute_queue: Vec::new(),
        }
    }

    /// Add element to appropriate window
    pub fn add(
        &mut self,
        element: StreamElement<T>,
    ) -> Result<Vec<WindowOutput<T>>, OperatorError> {
        let key = element
            .key
            .clone()
            .unwrap_or_else(|| "__default__".to_string());

        match self.config.window_type {
            WindowType::Tumbling => {
                let window = self.tumbling_windows.entry(key).or_insert_with(|| {
                    TumblingWindow::new(self.config.size_ms)
                        .with_lateness(self.config.allowed_lateness_ms)
                });
                window.add(element)?;
            }
            WindowType::Sliding | WindowType::Hopping => {
                let slide = self.config.slide_ms.unwrap_or(self.config.size_ms / 2);
                let window = self.sliding_windows.entry(key).or_insert_with(|| {
                    SlidingWindow::new(self.config.size_ms, slide)
                        .with_lateness(self.config.allowed_lateness_ms)
                });
                window.add(element)?;
            }
            WindowType::Session => {
                let gap = self.config.gap_ms.unwrap_or(30000);
                let window = self.session_windows.entry(key).or_insert_with(|| {
                    SessionWindow::new(gap).with_lateness(self.config.allowed_lateness_ms)
                });
                window.add(element)?;
            }
            WindowType::Count | WindowType::Global => {
                // Handle count and global windows using tumbling with special logic
                let window = self.tumbling_windows.entry(key).or_insert_with(|| {
                    TumblingWindow::new(self.config.size_ms)
                        .with_max_elements(self.config.max_elements.unwrap_or(usize::MAX))
                });
                window.add(element)?;
            }
        }

        Ok(vec![])
    }

    /// Trigger windows based on watermark
    pub fn trigger(&mut self, watermark: i64) -> Result<Vec<WindowOutput<T>>, OperatorError> {
        let mut outputs = Vec::new();

        for window in self.tumbling_windows.values_mut() {
            outputs.extend(window.trigger(watermark)?);
        }

        for window in self.sliding_windows.values_mut() {
            outputs.extend(window.trigger(watermark)?);
        }

        for window in self.session_windows.values_mut() {
            outputs.extend(window.trigger(watermark)?);
        }

        Ok(outputs)
    }

    /// Advance watermark
    pub fn advance_watermark(
        &mut self,
        watermark: i64,
    ) -> Result<Vec<WindowOutput<T>>, OperatorError> {
        self.watermark = watermark;
        self.trigger(watermark)
    }

    /// Mark timestamp for recomputation
    pub fn mark_for_recompute(&mut self, timestamp: i64) -> Result<(), OperatorError> {
        self.recompute_queue.push(timestamp);
        Ok(())
    }

    /// Get current watermark
    pub fn watermark(&self) -> i64 {
        self.watermark
    }

    /// Get total element count across all windows
    pub fn element_count(&self) -> usize {
        let tumbling: usize = self.tumbling_windows.values().map(|w| w.count()).sum();
        let sliding: usize = self.sliding_windows.values().map(|w| w.count()).sum();
        let session: usize = self.session_windows.values().map(|w| w.count()).sum();
        tumbling + sliding + session
    }

    /// Clear all windows
    pub fn clear(&mut self) {
        self.tumbling_windows.clear();
        self.sliding_windows.clear();
        self.session_windows.clear();
        self.recompute_queue.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_window_config_default() {
        let config = WindowConfig::default();
        assert_eq!(config.window_type, WindowType::Tumbling);
        assert_eq!(config.size_ms, 60000);
    }

    #[test]
    fn test_tumbling_window() {
        let mut window: TumblingWindow<i32> = TumblingWindow::new(1000);

        // Add elements to same window
        window.add(StreamElement::new(1, 100)).unwrap();
        window.add(StreamElement::new(2, 500)).unwrap();
        window.add(StreamElement::new(3, 900)).unwrap();

        // Add element to next window
        window.add(StreamElement::new(4, 1100)).unwrap();

        assert_eq!(window.count(), 4);

        // Trigger first window
        let outputs = window.trigger(1500).unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].elements.len(), 3);
        assert_eq!(outputs[0].window_start, 0);
        assert_eq!(outputs[0].window_end, 1000);
    }

    #[test]
    fn test_sliding_window() {
        let mut window: SlidingWindow<i32> = SlidingWindow::new(1000, 500);

        window.add(StreamElement::new(1, 100)).unwrap();
        window.add(StreamElement::new(2, 600)).unwrap();
        window.add(StreamElement::new(3, 1100)).unwrap();

        let outputs = window.trigger(2000).unwrap();
        // Should have overlapping windows
        assert!(!outputs.is_empty());
    }

    #[test]
    fn test_session_window() {
        let mut window: SessionWindow<i32> = SessionWindow::new(500);

        // First session: elements 1 and 2 at timestamps 100 and 200
        window
            .add(StreamElement::new(1, 100).with_key("user1"))
            .unwrap();
        window
            .add(StreamElement::new(2, 200).with_key("user1"))
            .unwrap();

        // Gap > 500ms (1000 > 200+500=700), creates a new session
        window
            .add(StreamElement::new(3, 1000).with_key("user1"))
            .unwrap();

        // Trigger with high watermark - both sessions should close
        let outputs = window.trigger(2000).unwrap();
        assert_eq!(outputs.len(), 2); // Two sessions: [1,2] and [3]

        // Find the session with 2 elements (the first session)
        let first_session = outputs.iter().find(|o| o.elements.len() == 2).unwrap();
        assert_eq!(first_session.elements, vec![1, 2]);

        // Find the session with 1 element (the second session)
        let second_session = outputs.iter().find(|o| o.elements.len() == 1).unwrap();
        assert_eq!(second_session.elements, vec![3]);
    }

    #[test]
    fn test_window_manager() {
        let config = WindowConfig {
            window_type: WindowType::Tumbling,
            size_ms: 1000,
            ..Default::default()
        };

        let mut manager: WindowManager<String> = WindowManager::new(config);

        manager
            .add(StreamElement::new("a".to_string(), 100))
            .unwrap();
        manager
            .add(StreamElement::new("b".to_string(), 500))
            .unwrap();

        let outputs = manager.advance_watermark(1500).unwrap();
        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].elements.len(), 2);
    }

    #[test]
    fn test_window_output() {
        let output: WindowOutput<i32> = WindowOutput::new(0, 1000, vec![1, 2, 3]);
        assert_eq!(output.window_start, 0);
        assert_eq!(output.window_end, 1000);
        assert_eq!(output.elements.len(), 3);
        assert!(!output.is_late);
    }

    #[test]
    fn test_window_trigger_default() {
        let trigger = WindowTrigger::default();
        assert_eq!(trigger, WindowTrigger::Watermark);
    }
}
