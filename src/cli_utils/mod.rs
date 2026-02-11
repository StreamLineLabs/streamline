//! Streamline CLI Utilities - Shared components for command-line interface
//!
//! This module provides reusable components for CLI tools:
//!
//! - `context`: CLI context, output formatting, and shared utilities
//!
//! # Usage
//!
//! ```rust,ignore
//! use streamline::cli_utils::{CliContext, OutputFormat};
//!
//! let ctx = CliContext::new(
//!     PathBuf::from("./data"),
//!     OutputFormat::Text,
//!     false,  // no_color
//!     false,  // skip_confirm
//!     None,   // output_file
//! );
//!
//! ctx.success("Operation completed");
//! ctx.info("Processing...");
//! ```
//!
//! # Module Structure
//!
//! The main CLI entry point is at `src/cli.rs`. This module provides
//! shared utilities that can be used by both the main CLI and any
//! custom CLI tools built on top of Streamline.

pub mod benchmark;
pub mod completions;
pub mod context;
pub mod doctor;
pub mod explain;
pub mod fuzzy;
pub mod migrate;
pub mod mirror;
pub mod offset_sync;
pub mod profile_store;
pub mod quickstart;
pub mod quickstart_templates;
pub mod quickstart_wizard;
#[cfg(feature = "auth")]
pub mod schema_import;
pub mod switchover;
pub mod time_travel;
pub mod tui;
pub mod tui_state;
pub mod tune;
pub mod validator;

#[cfg(feature = "iceberg")]
pub mod sink_commands;

#[cfg(feature = "edge")]
pub mod edge_commands;

#[cfg(any(
    feature = "postgres-cdc",
    feature = "mysql-cdc",
    feature = "mongodb-cdc",
    feature = "sqlserver-cdc"
))]
pub mod cdc_commands;

#[cfg(feature = "ai")]
pub mod ai_commands;

#[cfg(feature = "lakehouse")]
pub mod lakehouse_commands;

pub mod autopilot;
#[cfg(feature = "repl")]
pub mod repl;
pub mod testcontainers;

pub use benchmark::{run_benchmark, BenchmarkConfig, BenchmarkProfile, BenchmarkResults};
pub use completions::{
    generate_completions, install_completions, print_dynamic_completion_script,
    print_installation_instructions, DynamicCompleter,
};
pub use context::{csv_escape, tsv_escape, CliContext, OutputFormat};
pub use doctor::{run_diagnostics, DiagnosticResult, DiagnosticStatus};
pub use explain::ExplainContext;
pub use fuzzy::{find_similar, suggest_correction};
pub use migrate::{run_migration, MigrationConfig};
pub use mirror::{run_mirror, MirrorConfig, MirrorStats};
pub use offset_sync::{run_offset_sync, GroupOffsetInfo, OffsetSyncConfig, OffsetSyncResult};
pub use profile_store::{
    get_default_profile, load_profile, load_profiles, save_profiles, set_default_profile,
    ConnectionProfile,
};
pub use quickstart::run_quickstart;
#[cfg(feature = "auth")]
pub use schema_import::{run_schema_import, SchemaImportConfig, SchemaImportResult, SchemaInfo};
pub use switchover::{
    generate_switchover_plan, run_switchover, SwitchoverConfig, SwitchoverPhase, SwitchoverResult,
};
pub use time_travel::parse_time_expression;
pub use tui::run_dashboard;
pub use tui_state::{SortConfig, SortPreferences, TuiPersistentState};
pub use tune::{
    format_tune_result, generate_tune_script, is_tuning_supported, run_tune, TuneConfig,
    TuneParameter, TuneProfile, TuneResult,
};
pub use validator::{
    run_validation, CheckStatus, ValidationCheck, ValidationResult, ValidatorConfig,
};

#[cfg(feature = "iceberg")]
pub use sink_commands::{
    handle_sink_create, handle_sink_delete, handle_sink_list, handle_sink_start,
    handle_sink_status, handle_sink_stop,
};

#[cfg(feature = "edge")]
pub use edge_commands::{
    handle_edge_config, handle_edge_conflicts, handle_edge_list_nodes, handle_edge_status,
    handle_edge_sync,
};

#[cfg(any(
    feature = "postgres-cdc",
    feature = "mysql-cdc",
    feature = "mongodb-cdc",
    feature = "sqlserver-cdc"
))]
pub use cdc_commands::{
    handle_cdc_create, handle_cdc_delete, handle_cdc_schema, handle_cdc_sources, handle_cdc_start,
    handle_cdc_status, handle_cdc_stop,
};

#[cfg(feature = "ai")]
pub use ai_commands::{
    handle_ai_anomalies, handle_ai_classify, handle_ai_embeddings, handle_ai_enrich,
    handle_ai_pipelines, handle_ai_route, handle_ai_search,
};

#[cfg(feature = "lakehouse")]
pub use lakehouse_commands::{
    handle_lakehouse_export, handle_lakehouse_jobs, handle_lakehouse_query, handle_lakehouse_stats,
    handle_lakehouse_tables, handle_lakehouse_view_create, handle_lakehouse_view_refresh,
    handle_lakehouse_views,
};

#[cfg(feature = "repl")]
pub use repl::{run_repl, ReplConfig};
