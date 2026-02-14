//! Streaming Debugger & Time-Travel Inspector
//!
//! Provides interactive debugging tools for stream data:
//! - Browse events by topic/partition/offset
//! - Set breakpoints on message patterns
//! - Step through events forward/backward
//! - Diff consumer group states
//! - Replay specific offset ranges
//!
//! # Example
//!
//! ```rust
//! use streamline::debugger::{DebuggerConfig, Breakpoint};
//!
//! let config = DebuggerConfig::default();
//! let bp = Breakpoint::pattern("error");
//! assert_eq!(config.buffer_size, 1000);
//! ```

pub mod breakpoint;
pub mod inspector;
pub mod state_diff;

pub use breakpoint::{
    Breakpoint, BreakpointAction, BreakpointMatch, BreakpointType, PatternBreakpoint,
};
pub use inspector::{
    DebuggerConfig, EventView, InspectionResult, MessageInspector, NavigationDirection,
};
pub use state_diff::{ConsumerStateDiff, GroupStateDiff, OffsetDiff, StateDiffEngine};
