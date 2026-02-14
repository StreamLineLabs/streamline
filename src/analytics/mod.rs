//! Embedded analytics module for SQL queries on streaming data.
//!
//! This module provides zero-ETL analytics capabilities using DuckDB,
//! enabling SQL queries directly on Streamline topics without data export.
//!
//! # Stability
//!
//! **Stable** -- This module is production-ready. Breaking changes will only
//! occur in major version releases. See the project API Stability documentation
//! for the full compatibility matrix.
//!
//! # Features
//!
//! - Execute SQL queries on topic data using virtual tables
//! - Automatic schema inference from JSON messages
//! - Aggregation queries (COUNT, SUM, AVG, MIN, MAX, etc.)
//! - Window functions (ROW_NUMBER, RANK, LAG, LEAD, etc.)
//! - Materialized views for common query patterns
//! - Query result caching with configurable TTL
//! - Pagination support for large result sets
//! - Configurable query timeout
//!
//! # Quick Start
//!
//! ```bash
//! # Enable the analytics feature
//! cargo build --features analytics
//!
//! # Query via CLI
//! streamline-cli query "SELECT * FROM streamline_topic('events') LIMIT 10"
//! ```
//!
//! # HTTP API
//!
//! When enabled, the following endpoints are available on the HTTP server:
//!
//! - `POST /api/v1/query` -- Execute a SQL query
//! - `POST /api/v1/query/explain` -- Show query execution plan (EXPLAIN)
//! - `GET  /api/v1/query/cache/stats` -- Cache statistics
//! - `DELETE /api/v1/query/cache` -- Clear query cache
//! - `POST /api/v1/views` -- Create a materialized view
//! - `GET  /api/v1/views` -- List materialized views
//! - `GET  /api/v1/views/{name}` -- Get a specific view
//! - `DELETE /api/v1/views/{name}` -- Delete a view
//! - `POST /api/v1/views/{name}/refresh` -- Refresh a view
//! - `GET  /api/v1/views/{name}/query` -- Query a view

// When analytics feature is enabled, re-export from the workspace crate
#[cfg(feature = "analytics")]
pub use streamline_analytics::duckdb;

#[cfg(feature = "analytics")]
pub use streamline_analytics::duckdb::{
    DuckDBEngine, ExplainResult, MaterializedView, QueryOptions, QueryResult, QueryResultRow,
    StreamTable,
};

// Bridge: implement TopicDataSource for TopicManager so the workspace crate
// can read topic data without depending on the main crate's types.
#[cfg(feature = "analytics")]
mod bridge {
    use crate::storage::TopicManager;
    use streamline_analytics::error::{AnalyticsError, Result};
    use streamline_analytics::topic_source::{AnalyticsRecord, TopicDataSource};

    impl TopicDataSource for TopicManager {
        fn num_partitions(&self, topic: &str) -> Result<i32> {
            let metadata = self
                .get_topic_metadata(topic)
                .map_err(|e| AnalyticsError::Analytics(e.to_string()))?;
            Ok(metadata.num_partitions)
        }

        fn earliest_offset(&self, topic: &str, partition: i32) -> Result<i64> {
            self.earliest_offset(topic, partition)
                .map_err(|e| AnalyticsError::Analytics(e.to_string()))
        }

        fn latest_offset(&self, topic: &str, partition: i32) -> Result<i64> {
            self.latest_offset(topic, partition)
                .map_err(|e| AnalyticsError::Analytics(e.to_string()))
        }

        fn read_records(
            &self,
            topic: &str,
            partition: i32,
            offset: i64,
            max_records: usize,
        ) -> Result<Vec<AnalyticsRecord>> {
            let records = self
                .read(topic, partition, offset, max_records)
                .map_err(|e| AnalyticsError::Analytics(e.to_string()))?;

            Ok(records
                .into_iter()
                .map(|r| AnalyticsRecord {
                    offset: r.offset,
                    timestamp: r.timestamp,
                    key: r.key.map(|k| k.to_vec()),
                    value: r.value.to_vec(),
                    headers: r
                        .headers
                        .into_iter()
                        .map(|h| (h.key, h.value.to_vec()))
                        .collect(),
                })
                .collect())
        }
    }
}

// No-op stubs when analytics feature is disabled
#[cfg(not(feature = "analytics"))]
pub struct DuckDBEngine;

#[cfg(not(feature = "analytics"))]
impl DuckDBEngine {
    pub fn new(
        _topic_manager: std::sync::Arc<crate::storage::TopicManager>,
    ) -> crate::Result<Self> {
        Err(crate::error::StreamlineError::Config(
            "Analytics feature is not enabled. Compile with --features analytics".to_string(),
        ))
    }
}
