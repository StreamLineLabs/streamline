//! Streaming Data Lineage & Observability
//!
//! Automatic topic-level data lineage graph showing producer → topic → consumer
//! chains with latency, throughput, and error rates per hop.
//!
//! # Architecture
//!
//! ```text
//! Producer A ──► topic-1 ──► Consumer Group X
//!                    │
//!                    └──► Consumer Group Y ──► topic-2 ──► Consumer Group Z
//! ```

pub mod graph;
pub mod tracker;

pub use graph::{LineageEdge, LineageGraph, LineageNode, LineageNodeType};
pub use tracker::{ConnectionEvent, ConnectionEventType, LineageConfig, LineageTracker};
