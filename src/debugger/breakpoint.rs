//! Breakpoint engine for the streaming debugger.

use regex::Regex;
use serde::{Deserialize, Serialize};

/// A breakpoint that triggers when a condition is met on a stream event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Breakpoint {
    /// Unique breakpoint ID
    pub id: u64,
    /// Human-readable name
    pub name: String,
    /// Whether the breakpoint is currently active
    pub enabled: bool,
    /// Type of breakpoint
    pub breakpoint_type: BreakpointType,
    /// Action to take when triggered
    pub action: BreakpointAction,
    /// Number of times this breakpoint has been hit
    #[serde(default)]
    pub hit_count: u64,
}

/// Types of breakpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BreakpointType {
    /// Match on message value regex pattern
    Pattern(PatternBreakpoint),
    /// Match on a specific offset
    Offset { offset: i64 },
    /// Match on a JSON field value
    JsonField { path: String, value: String },
    /// Match on message key
    KeyMatch { pattern: String },
    /// Match on header presence/value
    Header { name: String, value: Option<String> },
    /// Match on message size threshold
    SizeThreshold {
        min_bytes: Option<usize>,
        max_bytes: Option<usize>,
    },
    /// Match on time range
    TimeRange {
        start_ms: Option<i64>,
        end_ms: Option<i64>,
    },
}

/// Regex-based pattern breakpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternBreakpoint {
    /// Regex pattern to match
    pub pattern: String,
    /// Whether to match case-insensitively
    #[serde(default)]
    pub case_insensitive: bool,
    /// Whether to search in keys too
    #[serde(default)]
    pub search_keys: bool,
}

/// Action to take when a breakpoint is triggered.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BreakpointAction {
    /// Pause at this message (default)
    #[default]
    Pause,
    /// Log and continue
    Log,
    /// Count occurrences without stopping
    Count,
}

/// Result of evaluating a breakpoint against a message.
#[derive(Debug, Clone)]
pub struct BreakpointMatch {
    /// The breakpoint that matched
    pub breakpoint_id: u64,
    /// The matched content
    pub matched_content: String,
    /// Offset where the match occurred
    pub offset: i64,
    /// Partition where the match occurred
    pub partition: i32,
}

impl Breakpoint {
    /// Create a pattern breakpoint.
    pub fn pattern(pattern: impl Into<String>) -> Self {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            id,
            name: format!("pattern-{}", id),
            enabled: true,
            breakpoint_type: BreakpointType::Pattern(PatternBreakpoint {
                pattern: pattern.into(),
                case_insensitive: false,
                search_keys: false,
            }),
            action: BreakpointAction::Pause,
            hit_count: 0,
        }
    }

    /// Create an offset breakpoint.
    pub fn at_offset(offset: i64) -> Self {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            id,
            name: format!("offset-{}", offset),
            enabled: true,
            breakpoint_type: BreakpointType::Offset { offset },
            action: BreakpointAction::Pause,
            hit_count: 0,
        }
    }

    /// Create a JSON field breakpoint.
    pub fn json_field(path: impl Into<String>, value: impl Into<String>) -> Self {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let id = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            id,
            name: format!("json-{}", id),
            enabled: true,
            breakpoint_type: BreakpointType::JsonField {
                path: path.into(),
                value: value.into(),
            },
            action: BreakpointAction::Pause,
            hit_count: 0,
        }
    }

    /// Check if this breakpoint matches the given message.
    pub fn matches(&self, offset: i64, key: Option<&[u8]>, value: Option<&[u8]>) -> bool {
        if !self.enabled {
            return false;
        }

        match &self.breakpoint_type {
            BreakpointType::Pattern(p) => {
                let regex = if p.case_insensitive {
                    Regex::new(&format!("(?i){}", p.pattern)).ok()
                } else {
                    Regex::new(&p.pattern).ok()
                };

                if let Some(re) = regex {
                    if let Some(v) = value {
                        if let Ok(s) = std::str::from_utf8(v) {
                            if re.is_match(s) {
                                return true;
                            }
                        }
                    }
                    if p.search_keys {
                        if let Some(k) = key {
                            if let Ok(s) = std::str::from_utf8(k) {
                                if re.is_match(s) {
                                    return true;
                                }
                            }
                        }
                    }
                }
                false
            }
            BreakpointType::Offset { offset: target } => offset == *target,
            BreakpointType::JsonField {
                path,
                value: expected,
            } => {
                if let Some(v) = value {
                    if let Ok(json) = serde_json::from_slice::<serde_json::Value>(v) {
                        let parts: Vec<&str> = path.split('.').collect();
                        let mut current = &json;
                        for part in &parts {
                            match current.get(part) {
                                Some(next) => current = next,
                                None => return false,
                            }
                        }
                        return current.to_string().trim_matches('"') == expected;
                    }
                }
                false
            }
            BreakpointType::KeyMatch { pattern } => {
                if let Some(k) = key {
                    if let Ok(s) = std::str::from_utf8(k) {
                        return Regex::new(pattern)
                            .map(|re| re.is_match(s))
                            .unwrap_or(false);
                    }
                }
                false
            }
            BreakpointType::Header { .. } => {
                // Headers require access to Record metadata — checked at inspector level
                false
            }
            BreakpointType::SizeThreshold {
                min_bytes,
                max_bytes,
            } => {
                let size = value.map(|v| v.len()).unwrap_or(0);
                let above_min = min_bytes.map(|m| size >= m).unwrap_or(true);
                let below_max = max_bytes.map(|m| size <= m).unwrap_or(true);
                above_min && below_max
            }
            BreakpointType::TimeRange { .. } => {
                // Time range requires timestamp metadata — checked at inspector level
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_breakpoint() {
        let bp = Breakpoint::pattern("error");
        assert!(bp.matches(0, None, Some(b"an error occurred")));
        assert!(!bp.matches(0, None, Some(b"all good")));
    }

    #[test]
    fn test_offset_breakpoint() {
        let bp = Breakpoint::at_offset(42);
        assert!(bp.matches(42, None, None));
        assert!(!bp.matches(41, None, None));
    }

    #[test]
    fn test_json_field_breakpoint() {
        let bp = Breakpoint::json_field("status", "error");
        assert!(bp.matches(0, None, Some(br#"{"status":"error","msg":"fail"}"#)));
        assert!(!bp.matches(0, None, Some(br#"{"status":"ok"}"#)));
    }

    #[test]
    fn test_disabled_breakpoint() {
        let mut bp = Breakpoint::pattern("error");
        bp.enabled = false;
        assert!(!bp.matches(0, None, Some(b"an error occurred")));
    }

    #[test]
    fn test_size_threshold_breakpoint() {
        let bp = Breakpoint {
            id: 1,
            name: "size".to_string(),
            enabled: true,
            breakpoint_type: BreakpointType::SizeThreshold {
                min_bytes: Some(10),
                max_bytes: None,
            },
            action: BreakpointAction::Pause,
            hit_count: 0,
        };
        assert!(!bp.matches(0, None, Some(b"short")));
        assert!(bp.matches(0, None, Some(b"this is a longer message")));
    }
}
