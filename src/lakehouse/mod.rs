//! Lakehouse-Native Streaming Module
//!
//! Provides columnar storage, materialized views, and SQL pushdown
//! for unified streaming and analytics.
//!
//! # Features
//!
//! - **Parquet Segments**: Store stream data in Parquet format for efficient analytics
//! - **Materialized Views**: Precomputed query results with incremental refresh
//! - **Query Optimizer**: SQL pushdown with predicate and projection pushdown
//! - **Zero-ETL Export**: Direct export to Iceberg, Delta Lake, and other formats
//! - **Statistics**: Column statistics for query optimization
//!
//! # Example
//!
//! ```sql
//! -- Create materialized view with incremental refresh
//! CREATE MATERIALIZED VIEW hourly_sales AS
//! SELECT
//!     date_trunc('hour', timestamp) as hour,
//!     product_id,
//!     SUM(amount) as total_sales,
//!     COUNT(*) as transaction_count
//! FROM topic('sales')
//! GROUP BY 1, 2
//! WITH (refresh = 'incremental', interval = '5 minutes');
//! ```

pub mod config;
pub mod export;
pub mod iceberg_topics;
pub mod incremental;
pub mod materialized_view;
pub mod parquet_segment;
pub mod query_optimizer;
pub mod scheduler;
pub mod statistics;
pub mod zero_etl;

pub use config::*;
pub use export::{ExportDestination, ExportFormat, ExportJob, ExportManager, ExportStatus};
pub use materialized_view::{
    MaterializedView, MaterializedViewManager, RefreshResult, RefreshType, ViewDefinition,
    ViewStatus,
};
pub use parquet_segment::{
    ColumnStatistics, ColumnStats, ParquetSegment, ParquetSegmentReader, ParquetSegmentWriter,
};
pub use query_optimizer::{PhysicalPlan, PhysicalPlanNode, Predicate, QueryOptimizer};
pub use statistics::StatisticsManager;
pub use zero_etl::{
    BindingStatus, ConflictResolution, FieldType, SchemaEvolution, SchemaField, SchemaSynchronizer,
    SyncBinding, SyncDirection, SyncResult, SyncSchema, SyncStats, SyncWatermark, ZeroEtlConfig,
    ZeroEtlManager,
};

use crate::error::Result;
use std::sync::Arc;

/// Lakehouse manager - coordinates all lakehouse operations
pub struct LakehouseManager {
    /// Configuration
    config: LakehouseConfig,
    /// Materialized view manager
    pub views: Arc<MaterializedViewManager>,
    /// Query optimizer
    pub optimizer: Arc<QueryOptimizer>,
    /// Export manager
    pub export: Arc<ExportManager>,
    /// Statistics manager
    pub statistics: Arc<StatisticsManager>,
}

impl LakehouseManager {
    /// Create a new lakehouse manager
    pub fn new(config: LakehouseConfig) -> Result<Self> {
        let statistics = Arc::new(StatisticsManager::new(config.statistics.clone())?);
        let optimizer = Arc::new(QueryOptimizer::new(
            statistics.clone(),
            config.query_optimizer.clone(),
        ));

        let views = Arc::new(MaterializedViewManager::new(
            config.materialized_views.clone(),
            optimizer.clone(),
        )?);

        let export = Arc::new(ExportManager::new(config.parquet.clone())?);

        Ok(Self {
            config,
            views,
            optimizer,
            export,
            statistics,
        })
    }

    /// Get lakehouse configuration
    pub fn config(&self) -> &LakehouseConfig {
        &self.config
    }

    /// Check if Parquet storage is enabled
    pub fn is_parquet_enabled(&self) -> bool {
        self.config.parquet_enabled
    }

    /// Check if materialized views are enabled
    pub fn are_views_enabled(&self) -> bool {
        self.config.materialized_views.enabled
    }
}
