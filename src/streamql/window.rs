//! Window functions for StreamlineQL
//!
//! Provides windowing operations for stream processing.

use super::types::{Row, Value};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Window type for stream processing
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowType {
    /// Tumbling window - fixed-size, non-overlapping intervals
    Tumbling { duration_ms: u64 },
    /// Hopping window - fixed-size, overlapping intervals
    Hopping { duration_ms: u64, hop_ms: u64 },
    /// Sliding window - fixed-size, triggered on every event
    Sliding { duration_ms: u64 },
    /// Session window - based on activity gaps
    Session { gap_ms: u64 },
    /// Count-based window
    Count { size: usize },
}

impl WindowType {
    /// Get the window duration in milliseconds (if applicable)
    pub fn duration_ms(&self) -> Option<u64> {
        match self {
            WindowType::Tumbling { duration_ms } => Some(*duration_ms),
            WindowType::Hopping { duration_ms, .. } => Some(*duration_ms),
            WindowType::Sliding { duration_ms } => Some(*duration_ms),
            WindowType::Session { gap_ms } => Some(*gap_ms),
            WindowType::Count { .. } => None,
        }
    }
}

/// A window of rows
#[derive(Debug, Clone)]
pub struct Window {
    /// Window type
    pub window_type: WindowType,
    /// Window start time (inclusive)
    pub start_time: i64,
    /// Window end time (exclusive)
    pub end_time: i64,
    /// Rows in this window
    pub rows: Vec<Row>,
}

impl Window {
    /// Create a new window
    pub fn new(window_type: WindowType, start_time: i64, end_time: i64) -> Self {
        Self {
            window_type,
            start_time,
            end_time,
            rows: Vec::new(),
        }
    }

    /// Add a row to the window
    pub fn add_row(&mut self, row: Row) {
        self.rows.push(row);
    }

    /// Get the number of rows in the window
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Check if the window is empty
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Check if an event time falls within this window
    pub fn contains_time(&self, event_time: i64) -> bool {
        event_time >= self.start_time && event_time < self.end_time
    }
}

/// Manager for windowing operations
pub struct WindowManager {
    /// Window type
    window_type: WindowType,
    /// Active windows
    windows: VecDeque<Window>,
    /// Current watermark (for event time processing)
    watermark: i64,
    /// Late event threshold in milliseconds
    late_threshold_ms: u64,
}

impl WindowManager {
    /// Create a new window manager
    pub fn new(window_type: WindowType) -> Self {
        Self {
            window_type,
            windows: VecDeque::new(),
            watermark: 0,
            late_threshold_ms: 0,
        }
    }

    /// Create with late event handling
    pub fn with_late_threshold(mut self, threshold_ms: u64) -> Self {
        self.late_threshold_ms = threshold_ms;
        self
    }

    /// Update the watermark
    pub fn update_watermark(&mut self, timestamp: i64) {
        if timestamp > self.watermark {
            self.watermark = timestamp;
        }
    }

    /// Add an event to the appropriate window(s)
    pub fn add_event(&mut self, event_time: i64, row: Row) -> Vec<usize> {
        let mut affected_windows = Vec::new();

        // Clone window_type to avoid borrow conflicts
        let window_type = self.window_type.clone();

        match window_type {
            WindowType::Tumbling { duration_ms } => {
                let window_start = (event_time / duration_ms as i64) * duration_ms as i64;
                let window_end = window_start + duration_ms as i64;

                // Find or create the window
                let window_idx = self.find_or_create_window(window_start, window_end);
                if let Some(idx) = window_idx {
                    self.windows[idx].add_row(row);
                    affected_windows.push(idx);
                }
            }
            WindowType::Hopping {
                duration_ms,
                hop_ms,
            } => {
                // An event can belong to multiple overlapping windows
                let num_windows = (duration_ms / hop_ms) as i64;

                for i in 0..num_windows {
                    let window_start =
                        ((event_time - i * hop_ms as i64) / hop_ms as i64) * hop_ms as i64;
                    let window_end = window_start + duration_ms as i64;

                    if event_time >= window_start && event_time < window_end {
                        let window_idx = self.find_or_create_window(window_start, window_end);
                        if let Some(idx) = window_idx {
                            self.windows[idx].add_row(row.clone());
                            affected_windows.push(idx);
                        }
                    }
                }
            }
            WindowType::Sliding { duration_ms } => {
                // Sliding windows are triggered on each event
                let window_start = event_time - duration_ms as i64;
                let window_end = event_time;

                let window_idx = self.find_or_create_window(window_start, window_end);
                if let Some(idx) = window_idx {
                    self.windows[idx].add_row(row);
                    affected_windows.push(idx);
                }
            }
            WindowType::Session { gap_ms } => {
                // Session windows merge based on activity gaps
                let mut merged = false;

                for (idx, window) in self.windows.iter_mut().enumerate() {
                    // Check if this event extends an existing session
                    if event_time >= window.start_time - gap_ms as i64
                        && event_time <= window.end_time + gap_ms as i64
                    {
                        window.add_row(row.clone());
                        // Extend the window if needed
                        if event_time > window.end_time {
                            window.end_time = event_time + gap_ms as i64;
                        }
                        if event_time < window.start_time {
                            window.start_time = event_time;
                        }
                        affected_windows.push(idx);
                        merged = true;
                        break;
                    }
                }

                if !merged {
                    // Create a new session window
                    let window_start = event_time;
                    let window_end = event_time + gap_ms as i64;
                    let mut window =
                        Window::new(self.window_type.clone(), window_start, window_end);
                    window.add_row(row);
                    self.windows.push_back(window);
                    affected_windows.push(self.windows.len() - 1);
                }
            }
            WindowType::Count { size } => {
                // Count-based windows
                if self.windows.is_empty()
                    || self
                        .windows
                        .back()
                        .map(|w| w.len() >= size)
                        .unwrap_or(false)
                {
                    let window = Window::new(self.window_type.clone(), event_time, i64::MAX);
                    self.windows.push_back(window);
                }
                if let Some(window) = self.windows.back_mut() {
                    window.add_row(row);
                    affected_windows.push(self.windows.len() - 1);
                }
            }
        }

        affected_windows
    }

    fn find_or_create_window(&mut self, start_time: i64, end_time: i64) -> Option<usize> {
        // Check for late events
        if self.watermark > 0 && end_time < self.watermark - self.late_threshold_ms as i64 {
            return None; // Drop late event
        }

        // Find existing window
        for (idx, window) in self.windows.iter().enumerate() {
            if window.start_time == start_time && window.end_time == end_time {
                return Some(idx);
            }
        }

        // Create new window
        let window = Window::new(self.window_type.clone(), start_time, end_time);
        self.windows.push_back(window);
        Some(self.windows.len() - 1)
    }

    /// Get windows that can be emitted (completed and past watermark)
    pub fn emit_ready_windows(&mut self) -> Vec<Window> {
        let mut ready = Vec::new();

        while let Some(window) = self.windows.front() {
            if window.end_time <= self.watermark {
                if let Some(w) = self.windows.pop_front() {
                    ready.push(w);
                }
            } else {
                break;
            }
        }

        ready
    }

    /// Force emit all windows (for end-of-stream)
    pub fn emit_all_windows(&mut self) -> Vec<Window> {
        self.windows.drain(..).collect()
    }

    /// Get current watermark
    pub fn watermark(&self) -> i64 {
        self.watermark
    }

    /// Get number of active windows
    pub fn active_window_count(&self) -> usize {
        self.windows.len()
    }
}

/// Window aggregate state
#[derive(Debug, Clone)]
pub struct WindowAggregateState {
    /// Partition key values
    pub partition_key: Vec<Value>,
    /// Window
    pub window: Window,
    /// Aggregate states
    pub aggregates: Vec<super::functions::AggregateState>,
}

impl WindowAggregateState {
    /// Create a new window aggregate state
    pub fn new(partition_key: Vec<Value>, window: Window) -> Self {
        Self {
            partition_key,
            window,
            aggregates: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tumbling_window() {
        let mut manager = WindowManager::new(WindowType::Tumbling { duration_ms: 1000 });

        // Add events at different times
        let row1 = Row::with_event_time(vec![Value::Int64(1)], 500);
        let row2 = Row::with_event_time(vec![Value::Int64(2)], 800);
        let row3 = Row::with_event_time(vec![Value::Int64(3)], 1200);

        manager.add_event(500, row1);
        manager.add_event(800, row2);
        manager.add_event(1200, row3);

        // Update watermark past first window
        manager.update_watermark(1500);

        let ready = manager.emit_ready_windows();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].rows.len(), 2);
        assert_eq!(ready[0].start_time, 0);
        assert_eq!(ready[0].end_time, 1000);
    }

    #[test]
    fn test_hopping_window() {
        let mut manager = WindowManager::new(WindowType::Hopping {
            duration_ms: 1000,
            hop_ms: 500,
        });

        let row = Row::with_event_time(vec![Value::Int64(1)], 750);
        let affected = manager.add_event(750, row);

        // Event at 750ms should be in windows [0-1000) and [500-1500)
        assert_eq!(affected.len(), 2);
    }

    #[test]
    fn test_count_window() {
        let mut manager = WindowManager::new(WindowType::Count { size: 3 });

        for i in 0..5 {
            let row = Row::new(vec![Value::Int64(i)]);
            manager.add_event(i * 100, row);
        }

        // Should have 2 windows: one full (3 events), one partial (2 events)
        assert_eq!(manager.active_window_count(), 2);
    }
}
