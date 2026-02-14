//! SQLite-based query interface for Streamline
//!
//! This module provides a lightweight, embedded SQLite query engine that maps
//! Streamline topics to SQLite tables. It is an alternative to the DuckDB-based
//! analytics engine -- SQLite is smaller (~1MB), requires zero configuration,
//! and works everywhere.
//!
//! # Stability
//!
//! **Warning: Experimental** - This module is under active development and may change
//! significantly or be removed in any version. Not recommended for production use.
//!
//! ## Features
//!
//! - Execute SQL queries on topic data via standard SQLite tables
//! - Automatic topic discovery (every topic becomes a table)
//! - JSON value extraction using SQLite's built-in `json_extract`
//! - Pagination (LIMIT/OFFSET) and query timeout support
//! - Lightweight: ~1MB additional binary size vs ~30MB for DuckDB

pub mod engine;
pub mod virtual_table;

pub use engine::{QueryResult, QueryResultRow, SQLiteQueryEngine};
