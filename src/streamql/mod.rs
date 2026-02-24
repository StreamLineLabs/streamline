//! StreamlineQL - SQL-like stream processing engine
//!
//! StreamlineQL provides a declarative way to process streaming data using
//! familiar SQL syntax with streaming extensions.
//!
//! ## Features
//!
//! - **SQL Syntax**: SELECT, FROM, WHERE, GROUP BY, ORDER BY
//! - **Window Functions**: Tumbling, Hopping, Sliding, Session windows
//! - **Aggregations**: COUNT, SUM, AVG, MIN, MAX, FIRST, LAST
//! - **Stream Operations**: Filter, Project, Join, Union
//! - **Time Functions**: Event time, Processing time, Watermarks
//!
//! ## Example
//!
//! ```sql
//! SELECT
//!     user_id,
//!     COUNT(*) as event_count,
//!     AVG(response_time) as avg_response
//! FROM events
//! WINDOW TUMBLING(INTERVAL '1 minute')
//! GROUP BY user_id
//! HAVING COUNT(*) > 10
//! ```
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────┐    ┌────────────┐    ┌─────────────┐
//! │   Parser    │───▶│  Planner   │───▶│  Executor   │
//! │  (SQL AST)  │    │ (LogPlan)  │    │ (PhysPlan)  │
//! └─────────────┘    └────────────┘    └─────────────┘
//!                                              │
//!                                              ▼
//!                    ┌──────────────────────────────────┐
//!                    │        Stream Operators          │
//!                    │  Filter │ Project │ Join │ Agg   │
//!                    └──────────────────────────────────┘
//! ```

pub mod ast;
pub mod cep;
pub mod checkpoint;
pub mod continuous;
pub mod engine;
pub mod executor;
pub mod functions;
pub mod join;
pub mod ksqldb;
pub mod lexer;
pub mod materialized;
pub mod operators;
pub mod parser;
pub mod planner;
pub mod types;
pub mod udf;
pub mod window;

use crate::error::Result;
use crate::storage::TopicManager;
use std::sync::Arc;
use tokio::sync::mpsc;

pub use executor::{QueryExecutor, QueryResult, StreamResult};
pub use join::{
    CrossJoinOperator, HashJoinOperator, IntervalJoinOperator, JoinBuilder, JoinConfig, JoinSide,
    NestedLoopJoinOperator, TemporalJoinOperator,
};
pub use ksqldb::{
    ColumnSchema as KsqlColumnSchema, ColumnType as KsqlColumnType,
    ContinuousQuery as KsqlContinuousQuery, ContinuousQueryStats as KsqlContinuousQueryStats,
    ContinuousQueryStatus as KsqlContinuousQueryStatus, DataFormat as KsqlDataFormat,
    KsqlAggregateFunction, KsqlWindowSpec, StreamDefinition, StreamqlEngine, StreamqlStatement,
    StatementResult as KsqlStatementResult, TableDefinition,
};
pub use operators::{FilterOperator, ProjectOperator, StreamOperator};
pub use parser::StreamQLParser;
pub use planner::QueryPlanner;
pub use types::{DataType, Value};

/// StreamlineQL query engine
///
/// The main entry point for executing StreamlineQL queries against
/// Streamline topics.
pub struct StreamQL {
    /// Topic manager for accessing data
    #[allow(dead_code)]
    topic_manager: Arc<TopicManager>,
    /// Query executor
    executor: QueryExecutor,
}

impl StreamQL {
    /// Create a new StreamlineQL engine
    pub fn new(topic_manager: Arc<TopicManager>) -> Self {
        Self {
            topic_manager: topic_manager.clone(),
            executor: QueryExecutor::new(topic_manager),
        }
    }

    /// Execute a query and return all results
    ///
    /// This is suitable for bounded queries or queries with LIMIT.
    pub async fn execute(&self, query: &str) -> Result<QueryResult> {
        // Parse the query
        let ast = StreamQLParser::parse(query)?;

        // Plan the query
        let planner = QueryPlanner::new();
        let plan = planner.plan(&ast)?;

        // Execute and collect results
        self.executor.execute(plan).await
    }

    /// Execute a streaming query and return a channel of results
    ///
    /// Results are streamed as they become available, suitable for
    /// continuous queries on unbounded streams.
    pub async fn execute_streaming(
        &self,
        query: &str,
    ) -> Result<mpsc::Receiver<Result<StreamResult>>> {
        // Parse the query
        let ast = StreamQLParser::parse(query)?;

        // Plan the query
        let planner = QueryPlanner::new();
        let plan = planner.plan(&ast)?;

        // Execute and return stream
        self.executor.execute_streaming(plan).await
    }

    /// Validate a query without executing it
    pub fn validate(&self, query: &str) -> Result<()> {
        let ast = StreamQLParser::parse(query)?;
        let planner = QueryPlanner::new();
        let _ = planner.plan(&ast)?;
        Ok(())
    }

    /// Get the execution plan for a query (for debugging/explain)
    pub fn explain(&self, query: &str) -> Result<String> {
        let ast = StreamQLParser::parse(query)?;
        let planner = QueryPlanner::new();
        let plan = planner.plan(&ast)?;
        Ok(plan.explain())
    }
}

/// Query options for customizing execution
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Maximum number of rows to return
    pub limit: Option<usize>,
    /// Starting offset (number of rows to skip)
    pub offset: Option<usize>,
    /// Timeout for query execution in milliseconds
    pub timeout_ms: Option<u64>,
    /// Maximum memory to use for query in bytes
    pub max_memory_bytes: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_options_default() {
        let opts = QueryOptions::default();
        assert!(opts.limit.is_none());
        assert!(opts.offset.is_none());
        assert!(opts.timeout_ms.is_none());
    }
}
