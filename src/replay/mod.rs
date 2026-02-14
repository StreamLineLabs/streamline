//! Instant Replay & Debug Mode for Streamline
//!
//! This module provides traffic capture and replay capabilities for debugging:
//! - Record production traffic without performance impact
//! - Replay captured traffic in isolated environments
//! - Mutate messages during replay for edge case testing
//! - Time manipulation (speed up, slow down, pause)
//! - Breakpoint support for step-by-step debugging
//!
//! # Usage
//!
//! ## Recording Traffic
//!
//! ```rust,ignore
//! use streamline::replay::{TrafficRecorder, RecordConfig};
//!
//! let recorder = TrafficRecorder::new(RecordConfig {
//!     topics: vec!["events".to_string()],
//!     output_dir: "./recordings".to_string(),
//!     ..Default::default()
//! });
//!
//! recorder.start().await?;
//! // ... traffic flows through ...
//! recorder.stop().await?;
//! ```
//!
//! ## Replaying Traffic
//!
//! ```rust,ignore
//! use streamline::replay::{TrafficReplayer, ReplayConfig};
//!
//! let replayer = TrafficReplayer::new(ReplayConfig {
//!     recording_path: "./recordings/2024-01-15_14-30-00".to_string(),
//!     speed: 1.0,  // normal speed
//!     ..Default::default()
//! });
//!
//! replayer.start().await?;
//! ```

pub mod capture;
pub mod playback;
pub mod storage;
pub mod transform;

pub use capture::{RecordConfig, RecorderStats, TrafficRecorder};
pub use playback::{ReplayConfig, ReplayMode, ReplayerStats, TrafficReplayer};
pub use storage::{Recording, RecordingMetadata, RecordingSegment};
