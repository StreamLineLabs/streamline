//! Feature Integration Module
//!
//! Provides unified pipelines that connect Streamline's major features:
//! CDC, AI, Edge, and Lakehouse for end-to-end data workflows.
//!
//! # Pipelines
//!
//! ## CDC → AI → Lakehouse Pipeline
//! Captures database changes, enriches with AI, and stores in lakehouse format.
//!
//! ```rust,ignore
//! use streamline::integration::{Pipeline, PipelineBuilder};
//!
//! let pipeline = PipelineBuilder::new("cdc-ai-lakehouse")
//!     .cdc_source("postgres-main", postgres_config)
//!     .ai_enrichment(true)
//!     .ai_classification(&["customer_event", "system_event", "transaction"])
//!     .lakehouse_sink("events-warehouse", iceberg_config)
//!     .build()?;
//!
//! pipeline.start().await?;
//! ```

pub mod pipeline;

pub use pipeline::{
    Pipeline, PipelineBuilder, PipelineConfig, PipelineManager, PipelineMetrics, PipelineStage,
    PipelineStatus,
};
