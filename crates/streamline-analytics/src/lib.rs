//! Embedded analytics engine for Streamline (DuckDB-powered SQL on stream data).
//!
//! This crate is a workspace member that isolates the heavy `duckdb` (bundled C++)
//! dependency into its own compilation unit, preventing recompilation of DuckDB
//! when unrelated core code changes.
//!
//! # Stability
//!
//! **Stable** -- Breaking changes only in major versions.
//!
//! # Overview
//!
//! The analytics engine allows executing SQL queries directly on Streamline
//! topics without exporting data to an external system. It achieves this by:
//!
//! 1. Reading topic data through the [`TopicDataSource`] trait.
//! 2. Materialising records into temporary DuckDB tables.
//! 3. Executing standard SQL (including aggregations, joins, window functions).
//! 4. Returning results as JSON-encoded rows.
//!
//! # Modules
//!
//! - [`duckdb`] -- Core query engine, caching, materialized views.
//! - [`error`] -- Domain-specific error types.
//! - [`topic_source`] -- Abstraction trait for reading topic data.
//! - [`alerts`] -- Configurable alert rules engine for streaming metrics.

pub mod alerts;
pub mod duckdb;
pub mod error;
pub mod topic_source;

pub use duckdb::{
    CacheStats, DuckDBEngine, ExplainResult, MaterializedView, QueryOptions, QueryResult,
    QueryResultRow, RefreshMode, StreamTable, WindowSpec, WindowType,
};
pub use error::{AnalyticsError, Result};
pub use topic_source::{AnalyticsRecord, TopicDataSource};
