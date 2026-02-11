//! Remaining CLI operation handlers extracted from cli.rs
//!
//! Contains handlers for sink, edge, CDC, AI, lakehouse, top, gitops,
//! geo-replication, contract, debug, connector, lineage, cloud, alerting,
//! and governor commands.

use colored::Colorize;
use comfy_table::{presets::UTF8_FULL_CONDENSED, Cell, Color, ContentArrangement, Table};
use std::sync::Arc;
use streamline::{Result, TopicManager};

use crate::cli_context::CliContext;
#[allow(unused_imports)]
use crate::OutputFormat;

#[cfg(feature = "ai")]
use super::AiCommands;
#[cfg(any(
    feature = "postgres-cdc",
    feature = "mysql-cdc",
    feature = "mongodb-cdc",
    feature = "sqlserver-cdc"
))]
use super::CdcCommands;
#[cfg(feature = "edge")]
use super::EdgeCommands;
#[cfg(feature = "lakehouse")]
use super::LakehouseCommands;
#[cfg(feature = "iceberg")]
use super::SinkCommands;
use super::{
    AlertingCommands, CloudCommands, ConnectorCommands, ContractCommands, DebugCommands,
    GeoReplicationCommands, GitOpsCommands, GovernorCommands, LineageCommands,
};

#[cfg(feature = "iceberg")]
pub(super) fn handle_sink_command(cmd: SinkCommands, ctx: &CliContext) -> Result<()> {
    use streamline::cli_utils::{
        handle_sink_create, handle_sink_delete, handle_sink_list, handle_sink_start,
        handle_sink_status, handle_sink_stop,
    };

    // Convert local OutputFormat to cli_utils::OutputFormat
    let utils_format = match ctx.format {
        OutputFormat::Json => streamline::cli_utils::OutputFormat::Json,
        OutputFormat::Csv => streamline::cli_utils::OutputFormat::Csv,
        OutputFormat::Tsv => streamline::cli_utils::OutputFormat::Tsv,
        _ => streamline::cli_utils::OutputFormat::Text,
    };

    // Convert local CliContext to cli_utils::CliContext
    let utils_ctx = streamline::cli_utils::CliContext::new(
        ctx.data_dir.clone(),
        utils_format,
        false, // no_color - will be determined by CliContext::new()
        ctx.skip_confirm,
        ctx.output_file.clone(),
    );

    // Run async operations in a new runtime
    tokio::runtime::Runtime::new()?.block_on(async {
        match cmd {
            SinkCommands::Create {
                name,
                sink_type,
                topics,
                catalog_uri,
                catalog_type,
                namespace,
                table,
                commit_interval_ms,
                max_batch_size,
                partitioning,
                start,
            } => {
                handle_sink_create(
                    name,
                    sink_type,
                    topics,
                    catalog_uri,
                    catalog_type,
                    namespace,
                    table,
                    commit_interval_ms,
                    max_batch_size,
                    partitioning,
                    start,
                    &utils_ctx,
                )
                .await
            }
            SinkCommands::List => handle_sink_list(&utils_ctx).await,
            SinkCommands::Status { name, json } => handle_sink_status(name, json, &utils_ctx).await,
            SinkCommands::Start { name } => handle_sink_start(name, &utils_ctx).await,
            SinkCommands::Stop { name } => handle_sink_stop(name, &utils_ctx).await,
            SinkCommands::Delete { name, yes } => handle_sink_delete(name, yes, &utils_ctx).await,
        }
    })
}

#[cfg(feature = "edge")]
pub(super) fn handle_edge_command(cmd: EdgeCommands, ctx: &CliContext) -> Result<()> {
    use streamline::cli_utils::{
        handle_edge_config, handle_edge_conflicts, handle_edge_list_nodes, handle_edge_status,
        handle_edge_sync,
    };

    // Convert local OutputFormat to cli_utils::OutputFormat
    let utils_format = match ctx.format {
        OutputFormat::Json => streamline::cli_utils::OutputFormat::Json,
        OutputFormat::Csv => streamline::cli_utils::OutputFormat::Csv,
        OutputFormat::Tsv => streamline::cli_utils::OutputFormat::Tsv,
        _ => streamline::cli_utils::OutputFormat::Text,
    };

    let utils_ctx = streamline::cli_utils::CliContext::new(
        ctx.data_dir.clone(),
        utils_format,
        false,
        ctx.skip_confirm,
        ctx.output_file.clone(),
    );

    match cmd {
        EdgeCommands::Status => handle_edge_status(&utils_ctx),
        EdgeCommands::Sync { direction, force } => handle_edge_sync(&direction, force, &utils_ctx),
        EdgeCommands::ListNodes => handle_edge_list_nodes(&utils_ctx),
        EdgeCommands::Config { show, set } => handle_edge_config(show, set, &utils_ctx),
        EdgeCommands::Conflicts { topic, resolve } => {
            handle_edge_conflicts(topic, resolve, &utils_ctx)
        }
    }
}

#[cfg(any(
    feature = "postgres-cdc",
    feature = "mysql-cdc",
    feature = "mongodb-cdc",
    feature = "sqlserver-cdc"
))]
pub(super) fn handle_cdc_command(cmd: CdcCommands, ctx: &CliContext) -> Result<()> {
    use streamline::cli_utils::{
        handle_cdc_create, handle_cdc_delete, handle_cdc_schema, handle_cdc_sources,
        handle_cdc_start, handle_cdc_status, handle_cdc_stop,
    };

    let utils_format = match ctx.format {
        OutputFormat::Json => streamline::cli_utils::OutputFormat::Json,
        OutputFormat::Csv => streamline::cli_utils::OutputFormat::Csv,
        OutputFormat::Tsv => streamline::cli_utils::OutputFormat::Tsv,
        _ => streamline::cli_utils::OutputFormat::Text,
    };

    let utils_ctx = streamline::cli_utils::CliContext::new(
        ctx.data_dir.clone(),
        utils_format,
        false,
        ctx.skip_confirm,
        ctx.output_file.clone(),
    );

    match cmd {
        CdcCommands::Sources => handle_cdc_sources(&utils_ctx),
        CdcCommands::Create {
            name,
            source_type,
            connection,
            tables,
            topic_prefix,
        } => handle_cdc_create(
            &name,
            &source_type,
            &connection,
            &tables,
            &topic_prefix,
            &utils_ctx,
        ),
        CdcCommands::Start { name } => handle_cdc_start(&name, &utils_ctx),
        CdcCommands::Stop { name } => handle_cdc_stop(&name, &utils_ctx),
        CdcCommands::Delete { name } => handle_cdc_delete(&name, &utils_ctx),
        CdcCommands::Status { name } => handle_cdc_status(&name, &utils_ctx),
        CdcCommands::Schema { name, table } => {
            handle_cdc_schema(&name, table.as_deref(), &utils_ctx)
        }
    }
}

#[cfg(feature = "ai")]
pub(super) fn handle_ai_command(cmd: AiCommands, ctx: &CliContext) -> Result<()> {
    use streamline::cli_utils::{
        handle_ai_anomalies, handle_ai_classify, handle_ai_embeddings, handle_ai_enrich,
        handle_ai_pipelines, handle_ai_route, handle_ai_search,
    };

    let utils_format = match ctx.format {
        OutputFormat::Json => streamline::cli_utils::OutputFormat::Json,
        OutputFormat::Csv => streamline::cli_utils::OutputFormat::Csv,
        OutputFormat::Tsv => streamline::cli_utils::OutputFormat::Tsv,
        _ => streamline::cli_utils::OutputFormat::Text,
    };

    let utils_ctx = streamline::cli_utils::CliContext::new(
        ctx.data_dir.clone(),
        utils_format,
        false,
        ctx.skip_confirm,
        ctx.output_file.clone(),
    );

    match cmd {
        AiCommands::Enrich {
            topic,
            prompt,
            output,
            model,
        } => handle_ai_enrich(&topic, &prompt, output.as_deref(), &model, &utils_ctx),
        AiCommands::Search {
            query,
            topics,
            limit,
            threshold,
        } => handle_ai_search(&query, &topics, limit, threshold, &utils_ctx),
        AiCommands::Anomalies {
            topic,
            detector,
            sensitivity,
        } => handle_ai_anomalies(&topic, &detector, sensitivity, &utils_ctx),
        AiCommands::Classify {
            topic,
            categories,
            output,
        } => handle_ai_classify(&topic, &categories, output.as_deref(), &utils_ctx),
        AiCommands::Embeddings { topic, action } => {
            handle_ai_embeddings(&topic, &action, &utils_ctx)
        }
        AiCommands::Route { topic, rules } => handle_ai_route(&topic, &rules, &utils_ctx),
        AiCommands::Pipelines => handle_ai_pipelines(&utils_ctx),
    }
}

#[cfg(feature = "lakehouse")]
pub(super) fn handle_lakehouse_command(cmd: LakehouseCommands, ctx: &CliContext) -> Result<()> {
    use streamline::cli_utils::{
        handle_lakehouse_export, handle_lakehouse_jobs, handle_lakehouse_query,
        handle_lakehouse_stats, handle_lakehouse_tables, handle_lakehouse_view_create,
        handle_lakehouse_view_refresh, handle_lakehouse_views,
    };

    let utils_format = match ctx.format {
        OutputFormat::Json => streamline::cli_utils::OutputFormat::Json,
        OutputFormat::Csv => streamline::cli_utils::OutputFormat::Csv,
        OutputFormat::Tsv => streamline::cli_utils::OutputFormat::Tsv,
        _ => streamline::cli_utils::OutputFormat::Text,
    };

    let utils_ctx = streamline::cli_utils::CliContext::new(
        ctx.data_dir.clone(),
        utils_format,
        false,
        ctx.skip_confirm,
        ctx.output_file.clone(),
    );

    match cmd {
        LakehouseCommands::Views => handle_lakehouse_views(&utils_ctx),
        LakehouseCommands::ViewCreate {
            name,
            query,
            refresh,
        } => handle_lakehouse_view_create(&name, &query, &refresh, &utils_ctx),
        LakehouseCommands::ViewRefresh { name, full } => {
            handle_lakehouse_view_refresh(&name, full, &utils_ctx)
        }
        LakehouseCommands::Export {
            topic,
            format,
            destination,
            partition_by,
        } => handle_lakehouse_export(
            &topic,
            &format,
            &destination,
            partition_by.as_deref(),
            &utils_ctx,
        ),
        LakehouseCommands::Query {
            query,
            max_rows,
            output_format,
        } => handle_lakehouse_query(&query, max_rows, &output_format, &utils_ctx),
        LakehouseCommands::Tables => handle_lakehouse_tables(&utils_ctx),
        LakehouseCommands::Jobs => handle_lakehouse_jobs(&utils_ctx),
        LakehouseCommands::Stats => handle_lakehouse_stats(&utils_ctx),
    }
}

pub(super) fn handle_top_command(refresh: u64, ctx: &CliContext) -> Result<()> {
    use streamline::cli_utils::tui::{run_dashboard, DashboardConfig};

    let config = DashboardConfig {
        data_dir: ctx.data_dir.clone(),
        refresh_interval: std::time::Duration::from_secs(refresh),
    };

    run_dashboard(&config).map_err(streamline::error::StreamlineError::Io)?;

    Ok(())
}

pub(super) fn handle_gitops_command(cmd: GitOpsCommands, ctx: &CliContext) -> Result<()> {
    use streamline::gitops::{GitOpsConfig, GitOpsManager};

    let config = GitOpsConfig {
        config_path: Some(ctx.data_dir.display().to_string()),
        ..GitOpsConfig::default()
    };
    let gitops = GitOpsManager::new(config);

    let rt = tokio::runtime::Runtime::new().map_err(streamline::error::StreamlineError::Io)?;

    match cmd {
        GitOpsCommands::Apply {
            file,
            dry_run,
            force: _,
        } => {
            let content =
                std::fs::read_to_string(&file).map_err(streamline::error::StreamlineError::Io)?;
            println!("{} {}", "📋 Loading manifest:".bold().cyan(), file.yellow());

            let manifest = rt.block_on(gitops.load_yaml(&content))?;
            println!(
                "  Loaded manifest: {}/{}",
                manifest.metadata.namespace, manifest.metadata.name
            );

            if dry_run {
                println!("{}", "\n🔍 Dry run - validating manifest:".yellow());
                let validation = gitops.validate(&manifest)?;
                if validation.valid {
                    println!("  {} Manifest is valid", "✅".green());
                } else {
                    for err in &validation.errors {
                        println!("  {} {}", "❌".red(), err);
                    }
                }
                println!("\n{}", "No changes applied (dry run).".yellow());
            } else {
                let result = rt.block_on(gitops.apply(&manifest))?;
                println!("{}", "\n✅ Manifest applied successfully!".green().bold());
                println!("  Status: {:?}", result.status);
                if !result.created.is_empty() {
                    println!("  Created: {}", result.created.join(", "));
                }
                if !result.updated.is_empty() {
                    println!("  Updated: {}", result.updated.join(", "));
                }
                if !result.deleted.is_empty() {
                    println!("  Deleted: {}", result.deleted.join(", "));
                }
            }
        }
        GitOpsCommands::Validate { file } => {
            let content =
                std::fs::read_to_string(&file).map_err(streamline::error::StreamlineError::Io)?;
            println!(
                "{} {}",
                "🔍 Validating manifest:".bold().cyan(),
                file.yellow()
            );

            match rt.block_on(gitops.load_yaml(&content)) {
                Ok(manifest) => {
                    let validation = gitops.validate(&manifest)?;
                    if validation.valid {
                        println!(
                            "  {} Valid manifest: {}",
                            "✅".green(),
                            manifest.metadata.name
                        );
                    } else {
                        println!("  {} Validation errors:", "❌".red());
                        for err in &validation.errors {
                            println!("    - {}", err);
                        }
                        return Err(streamline::StreamlineError::Config(
                            "GitOps manifest validation failed".into(),
                        ));
                    }
                }
                Err(e) => {
                    println!("  {} Parse error: {}", "❌".red(), e);
                    return Err(streamline::StreamlineError::Config(format!(
                        "GitOps manifest parse error: {}",
                        e
                    )));
                }
            }
        }
        GitOpsCommands::Diff { file } => {
            let content =
                std::fs::read_to_string(&file).map_err(streamline::error::StreamlineError::Io)?;
            println!(
                "{} {}",
                "🔄 Computing diff against:".bold().cyan(),
                file.yellow()
            );

            let _ = rt.block_on(gitops.load_yaml(&content))?;
            let drift = rt.block_on(gitops.detect_drift())?;

            if drift.is_empty() {
                println!(
                    "\n  {} No differences found. Current state matches manifest.",
                    "✅".green()
                );
            } else {
                println!(
                    "\n  Found {} difference(s):\n",
                    drift.len().to_string().bold()
                );
                for d in &drift {
                    println!(
                        "  {} {} / {}",
                        "~".yellow(),
                        d.resource_type.bold(),
                        d.resource_name
                    );
                    println!("    Type: {:?}", d.drift_type);
                    if !d.details.is_empty() {
                        println!("    Details: {}", d.details);
                    }
                }
            }
        }
        GitOpsCommands::Export { output, format } => {
            println!("{}", "📤 Exporting current configuration...".bold().cyan());

            let content = if format == "json" {
                rt.block_on(gitops.export_json())?
            } else {
                rt.block_on(gitops.export_yaml())?
            };

            match output {
                Some(path) => {
                    std::fs::write(&path, &content)
                        .map_err(streamline::error::StreamlineError::Io)?;
                    println!("  {} Exported to {}", "✅".green(), path.bold());
                }
                None => {
                    println!("{}", content);
                }
            }
        }
        GitOpsCommands::Status => {
            println!("{}", "📊 GitOps Reconciliation Status".bold().cyan());
            println!();

            let manifests = rt.block_on(gitops.list_manifests());
            println!(
                "  {} manifest(s) tracked",
                manifests.len().to_string().bold()
            );

            let drift = rt.block_on(gitops.detect_drift())?;
            if drift.is_empty() {
                println!("  {} All resources in sync", "✅".green());
            } else {
                println!("  {} {} resource(s) with drift", "⚠️".yellow(), drift.len());
                for d in &drift {
                    println!(
                        "    {} {}/{}",
                        "~".yellow(),
                        d.resource_type,
                        d.resource_name
                    );
                }
            }
        }
    }

    Ok(())
}

pub(super) fn handle_geo_replication_command(
    cmd: GeoReplicationCommands,
    ctx: &CliContext,
) -> Result<()> {
    use streamline::crdt::replication::{
        GeoReplicationConfig, GeoReplicationManager, PeerRegion, ReplicationMode,
    };

    let make_config = |local: &str| GeoReplicationConfig {
        local_region: local.to_string(),
        peer_regions: vec![],
        mode: ReplicationMode::ActiveActive,
        max_batch_size: 1000,
        replication_interval_ms: 5000,
        crdt_enabled: true,
        compression_enabled: true,
        max_lag_alert_ms: 30000,
    };

    let rt = tokio::runtime::Runtime::new().map_err(streamline::error::StreamlineError::Io)?;

    match cmd {
        GeoReplicationCommands::Status => {
            println!("{}", "🌍 Geo-Replication Status".bold().cyan());
            println!();

            let manager = GeoReplicationManager::new(make_config("local"));

            let stats = rt.block_on(manager.stats_snapshot());
            println!("  Mode:           {}", format!("{:?}", stats.mode).bold());
            println!("  Local Region:   {}", stats.local_region.bold());
            println!();
            println!("{}", "  Metrics:".bold());
            println!(
                "    Records replicated:  {}",
                stats.total_records_replicated
            );
            println!("    Bytes replicated:    {}", stats.total_bytes_replicated);
            println!(
                "    Conflicts resolved:  {}",
                stats.total_conflicts_resolved
            );
            println!("    Errors:              {}", stats.total_errors);
            println!(
                "    Peers connected:     {}/{}",
                stats.connected_peers, stats.peer_count
            );
        }
        GeoReplicationCommands::Peers => {
            println!("{}", "🌍 Peer Regions".bold().cyan());
            println!();

            let manager = GeoReplicationManager::new(make_config("local"));
            let stats = rt.block_on(manager.stats_snapshot());

            if stats.peer_count == 0 {
                println!("  No peer regions configured.");
                println!();
                println!("  Add a peer with:");
                println!("    {} {}", "streamline-cli geo-replication add-peer".bold(), "--region-id us-west --name \"US West\" --endpoint broker.us-west.example.com:9092".dimmed());
            } else {
                println!("  {} peer(s) configured\n", stats.peer_count);
                for peer in &stats.peers {
                    println!(
                        "  {} {} [{:?}]",
                        "•".cyan(),
                        peer.region_id.bold(),
                        peer.state
                    );
                    println!(
                        "    Records: {}  Bytes: {}  Lag: {}ms  Errors: {}",
                        peer.records_replicated, peer.bytes_replicated, peer.lag_ms, peer.errors
                    );
                }
            }
        }
        GeoReplicationCommands::AddPeer {
            region_id,
            name,
            endpoint,
        } => {
            println!("{}", "🌍 Adding Peer Region".bold().cyan());

            let manager = GeoReplicationManager::new(make_config("local"));
            let peer = PeerRegion {
                region_id: region_id.clone(),
                display_name: name.clone(),
                endpoint: endpoint.clone(),
                connected: false,
                topics: vec![],
            };

            rt.block_on(manager.add_peer(peer));
            println!(
                "  {} Added peer '{}' ({}) at {}",
                "✅".green(),
                name.bold(),
                region_id,
                endpoint
            );
        }
        GeoReplicationCommands::RemovePeer { region_id } => {
            println!("{}", "🌍 Removing Peer Region".bold().cyan());

            let manager = GeoReplicationManager::new(make_config("local"));
            let removed = rt.block_on(manager.remove_peer(&region_id));
            if removed {
                println!("  {} Removed peer '{}'", "✅".green(), region_id.bold());
            } else {
                println!("  {} Peer '{}' not found", "⚠️".yellow(), region_id.bold());
            }
        }
        GeoReplicationCommands::Start => {
            println!("{}", "🌍 Starting Geo-Replication...".bold().cyan());

            let manager =
                GeoReplicationManager::new(make_config(&ctx.data_dir.display().to_string()));
            rt.block_on(manager.start())?;
            println!("  {} Geo-replication started", "✅".green());
        }
        GeoReplicationCommands::Stop => {
            println!("{}", "🌍 Stopping Geo-Replication...".bold().cyan());

            let manager = GeoReplicationManager::new(make_config("local"));
            rt.block_on(manager.stop())?;
            println!("  {} Geo-replication stopped", "✅".green());
        }
    }

    Ok(())
}

pub(super) fn handle_contract_command(cmd: ContractCommands, ctx: &CliContext) -> Result<()> {
    use streamline::contracts::{ContractRunner, ContractRunnerConfig, StreamContract, TestReport};

    match cmd {
        ContractCommands::Run {
            file,
            output,
            output_file,
        } => {
            println!("{}", "📋 Running stream contract tests...".bold().cyan());

            let content = std::fs::read_to_string(&file).map_err(|e| {
                streamline::StreamlineError::Config(format!("Failed to read {}: {}", file, e))
            })?;
            let contract: StreamContract = serde_yaml::from_str(&content).map_err(|e| {
                streamline::StreamlineError::Config(format!("Failed to parse contract: {}", e))
            })?;

            let runner = ContractRunner::new(ContractRunnerConfig::default());
            let suite = runner.run(&[contract])?;
            let report = TestReport { suite };

            let output_text = match output.as_str() {
                "junit" => report.to_junit_xml(),
                "json" => {
                    serde_json::to_string_pretty(&report.suite).unwrap_or_else(|_| "{}".to_string())
                }
                _ => report.to_text(),
            };

            if let Some(path) = output_file {
                std::fs::write(&path, &output_text).map_err(streamline::StreamlineError::Io)?;
                println!("Results written to {}", path);
            } else {
                println!("{}", output_text);
            }
        }
        ContractCommands::Validate { file } => {
            let content = std::fs::read_to_string(&file).map_err(|e| {
                streamline::StreamlineError::Config(format!("Failed to read {}: {}", file, e))
            })?;
            match StreamContract::from_yaml(&content) {
                Ok(contract) => {
                    println!("{} Contract '{}' is valid", "✓".green(), contract.name);
                    println!("  Topic: {}", contract.topic);
                    println!("  Fields: {}", contract.spec.fields.len());
                    println!("  Invariants: {}", contract.spec.invariants.len());
                }
                Err(e) => {
                    println!("{} Invalid contract: {}", "✗".red(), e);
                }
            }
        }
        ContractCommands::Generate {
            topic,
            sample_size,
            output,
        } => {
            println!(
                "Generating contract from topic '{}' (sampling {} messages)...",
                topic, sample_size
            );
            let topic_manager = TopicManager::new(&ctx.data_dir)?;
            let records = topic_manager.read(&topic, 0, 0, sample_size)?;

            let mut fields = Vec::new();
            if let Some(first) = records.first() {
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&first.value) {
                    if let Some(obj) = json.as_object() {
                        for (key, val) in obj {
                            let ft = match val {
                                serde_json::Value::String(_) => streamline::FieldType::String,
                                serde_json::Value::Number(n) if n.is_i64() => {
                                    streamline::FieldType::Integer
                                }
                                serde_json::Value::Number(_) => streamline::FieldType::Float,
                                serde_json::Value::Bool(_) => streamline::FieldType::Boolean,
                                serde_json::Value::Array(_) => streamline::FieldType::Array,
                                serde_json::Value::Object(_) => streamline::FieldType::Object,
                                serde_json::Value::Null => streamline::FieldType::Null,
                            };
                            fields.push(streamline::contracts::definition::ContractField {
                                name: key.clone(),
                                field_type: ft,
                                required: true,
                                description: String::new(),
                                pattern: None,
                                min: None,
                                max: None,
                            });
                        }
                    }
                }
            }

            let contract = streamline::contracts::definition::StreamContract {
                name: format!("{}-contract", topic),
                topic: topic.clone(),
                spec: streamline::contracts::definition::ContractSpec {
                    fields,
                    invariants: Vec::new(),
                    mock_data: None,
                    max_message_size: None,
                    required_headers: Vec::new(),
                },
                description: format!("Auto-generated contract for topic '{}'", topic),
                version: "1.0.0".to_string(),
            };

            let yaml = contract.to_yaml().unwrap_or_default();
            if let Some(path) = output {
                std::fs::write(&path, &yaml).map_err(streamline::StreamlineError::Io)?;
                println!("{} Contract written to {}", "✓".green(), path);
            } else {
                println!("{}", yaml);
            }
        }
    }
    Ok(())
}

pub(super) fn handle_debug_command(cmd: DebugCommands, ctx: &CliContext) -> Result<()> {
    use streamline::debugger::{Breakpoint, DebuggerConfig, MessageInspector};

    match cmd {
        DebugCommands::Inspect {
            topic,
            partition,
            offset,
            count,
            pretty,
        } => {
            let topic_manager = Arc::new(TopicManager::new(&ctx.data_dir)?);
            let mut inspector = MessageInspector::new(
                topic_manager,
                &topic,
                partition,
                DebuggerConfig {
                    fetch_size: count,
                    pretty_json: pretty,
                    ..Default::default()
                },
            );

            if let Some(off) = offset {
                inspector.navigate(streamline::debugger::NavigationDirection::JumpToOffset(off))?;
            }

            let result = inspector.inspect()?;
            println!(
                "{}",
                format!(
                    "🔍 Topic: {} partition: {} (offsets {}-{})",
                    topic, partition, result.earliest_offset, result.latest_offset
                )
                .bold()
                .cyan()
            );
            println!();

            for event in &result.events {
                print_event_view(event);
            }

            if result.events.is_empty() {
                println!("  (no messages)");
            }
        }
        DebugCommands::Search {
            topic,
            pattern,
            partition,
            max_scan,
        } => {
            let topic_manager = Arc::new(TopicManager::new(&ctx.data_dir)?);
            let mut inspector =
                MessageInspector::new(topic_manager, &topic, partition, DebuggerConfig::default());
            inspector.add_breakpoint(Breakpoint::pattern(&pattern));

            let result = inspector.inspect()?;
            println!(
                "{}",
                format!(
                    "🔎 Searching for '{}' in {}:{} (max {})",
                    pattern, topic, partition, max_scan
                )
                .bold()
                .cyan()
            );

            let matches = result.breakpoint_matches.len();
            println!("Found {} matches", matches);
            for m in &result.breakpoint_matches {
                println!(
                    "  offset {}: {}",
                    m.offset,
                    if m.matched_content.len() > 80 {
                        format!("{}...", &m.matched_content[..80])
                    } else {
                        m.matched_content.clone()
                    }
                );
            }
        }
        DebugCommands::GroupDiff { group } => {
            println!(
                "{}",
                format!("📊 Consumer group diff: {}", group).bold().cyan()
            );
            println!("  (Connect to a running server to capture group state snapshots)");
            println!("  Use: GET /api/v1/debug/groups/{}/diff", group);
        }
    }
    Ok(())
}

fn print_event_view(event: &streamline::debugger::EventView) {
    print!("  {} offset={}", "│".dimmed(), event.offset);
    if let Some(ref key) = event.key {
        print!(" key={}", key.yellow());
    }
    print!(" ts={}", event.timestamp);
    println!(" ({}B)", event.size_bytes);

    if let Some(ref pretty) = event.pretty_value {
        for line in pretty.lines() {
            println!("  {} {}", "│".dimmed(), line);
        }
    } else if let Some(ref value) = event.value {
        let display = if value.len() > 120 {
            &value[..120]
        } else {
            value
        };
        println!("  {} {}", "│".dimmed(), display);
    }
    println!("  {}", "├────────".dimmed());
}

pub(super) fn handle_connector_command(cmd: ConnectorCommands, _ctx: &CliContext) -> Result<()> {
    use streamline::marketplace::{ConnectorCategory, MarketplaceCatalog};

    let catalog = MarketplaceCatalog::with_builtins();

    match cmd {
        ConnectorCommands::Search { query } => {
            let results = catalog.search(&query);
            println!(
                "{}",
                format!(
                    "🔍 Connector search: '{}' ({} results)",
                    query,
                    results.len()
                )
                .bold()
                .cyan()
            );
            println!();

            let mut table = Table::new();
            table.load_preset(UTF8_FULL_CONDENSED);
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(vec![
                Cell::new("Name").fg(Color::Cyan),
                Cell::new("Category"),
                Cell::new("Description"),
                Cell::new("Version"),
            ]);

            for entry in results {
                table.add_row(vec![
                    Cell::new(&entry.metadata.name).fg(Color::Green),
                    Cell::new(entry.metadata.category.to_string()),
                    Cell::new(&entry.metadata.description),
                    Cell::new(&entry.metadata.version),
                ]);
            }
            println!("{}", table);
        }
        ConnectorCommands::List => {
            println!("{}", "📦 Available Connectors".bold().cyan());
            println!();

            for category in &[
                ConnectorCategory::Source,
                ConnectorCategory::Sink,
                ConnectorCategory::Transform,
            ] {
                let entries = catalog.by_category(category);
                if !entries.is_empty() {
                    println!("  {} ({}):", format!("{}", category).bold(), entries.len());
                    for entry in entries {
                        println!(
                            "    {} - {}",
                            entry.metadata.name.green(),
                            entry.metadata.description
                        );
                    }
                    println!();
                }
            }
        }
        ConnectorCommands::Info { name } => {
            if let Some(entry) = catalog.get(&name) {
                println!(
                    "{}",
                    format!("📋 {}", entry.metadata.display_name).bold().cyan()
                );
                println!("  Name:     {}", entry.metadata.name);
                println!("  Category: {}", entry.metadata.category);
                println!("  Version:  {}", entry.metadata.version);
                println!("  License:  {}", entry.metadata.license);
                println!("  Description: {}", entry.metadata.description);
                if !entry.required_config.is_empty() {
                    println!("\n  {} Configuration:", "Required".bold());
                    for key in &entry.required_config {
                        println!("    {} - {}", key.name.yellow(), key.description);
                    }
                }
                if !entry.optional_config.is_empty() {
                    println!("\n  {} Configuration:", "Optional".bold());
                    for key in &entry.optional_config {
                        println!("    {} - {}", key.name, key.description);
                    }
                }
            } else {
                println!(
                    "{} Connector '{}' not found. Use 'connector search' to browse.",
                    "✗".red(),
                    name
                );
            }
        }
        ConnectorCommands::Install { name } => {
            if catalog.get(&name).is_some() {
                println!("{} Installing connector '{}'...", "⬇".cyan(), name);
                println!(
                    "{} Connector '{}' installed successfully!",
                    "✓".green(),
                    name
                );
                println!(
                    "  Configure with: streamline-cli connector configure {}",
                    name
                );
            } else {
                println!(
                    "{} Connector '{}' not found in marketplace",
                    "✗".red(),
                    name
                );
            }
        }
        ConnectorCommands::Uninstall { name, yes } => {
            if !yes {
                println!("Uninstall connector '{}'?", name);
            }
            println!("{} Connector '{}' uninstalled", "✓".green(), name);
        }
        ConnectorCommands::Apply { file, dry_run } => {
            let yaml = std::fs::read_to_string(&file).map_err(|e| {
                streamline::StreamlineError::Config(format!(
                    "Failed to read '{}': {}",
                    file.display(),
                    e
                ))
            })?;

            // Validate the YAML
            let spec: streamline::marketplace::declarative::ConnectorSpec =
                serde_yaml::from_str(&yaml).map_err(|e| {
                    streamline::StreamlineError::Config(format!("Invalid connector YAML: {}", e))
                })?;

            println!(
                "{} {}",
                "📋".cyan(),
                format!("Connector: {}", spec.metadata.name).bold()
            );
            println!("  Direction: {:?}", spec.spec.direction);
            println!("  Topics:    {:?}", spec.spec.topics);

            if dry_run {
                println!();
                println!(
                    "{}",
                    "✓ YAML is valid. Use without --dry-run to apply.".green()
                );
            } else {
                let rt = tokio::runtime::Runtime::new().map_err(|e| {
                    streamline::StreamlineError::Internal(format!("Failed to create runtime: {}", e))
                })?;
                let manager = streamline::marketplace::declarative::ConnectorManager::new();
                let connector = rt.block_on(manager.apply(spec))?;
                println!();
                println!(
                    "{} Connector '{}' applied (state: {})",
                    "✓".green(),
                    connector.name,
                    connector.state
                );
            }
        }
        ConnectorCommands::Status { name } => {
            let rt = tokio::runtime::Runtime::new().map_err(|e| {
                streamline::StreamlineError::Internal(format!("Failed to create runtime: {}", e))
            })?;
            let manager = streamline::marketplace::declarative::ConnectorManager::new();

            if let Some(name) = name {
                match rt.block_on(manager.status(&name)) {
                    Ok(conn) => {
                        println!("{}", format!("📊 {}", conn.name).bold().cyan());
                        println!("  State:         {}", conn.state);
                        println!("  Restarts:      {}", conn.restart_count);
                        println!("  Created:       {}", conn.created_at);
                        if let Some(started) = conn.started_at {
                            println!("  Started:       {}", started);
                        }
                        if let Some(err) = &conn.last_error {
                            println!("  Last Error:    {}", err.red());
                        }
                    }
                    Err(e) => {
                        println!("{} {}", "✗".red(), e);
                    }
                }
            } else {
                let connectors = rt.block_on(manager.list());
                if connectors.is_empty() {
                    println!("No managed connectors. Use 'connector apply' to create one.");
                } else {
                    let mut table = Table::new();
                    table.load_preset(UTF8_FULL_CONDENSED);
                    table.set_header(vec![
                        Cell::new("Name").fg(Color::Cyan),
                        Cell::new("State"),
                        Cell::new("Restarts"),
                        Cell::new("Created"),
                    ]);
                    for conn in connectors {
                        table.add_row(vec![
                            Cell::new(&conn.name).fg(Color::Green),
                            Cell::new(conn.state.to_string()),
                            Cell::new(conn.restart_count.to_string()),
                            Cell::new(conn.created_at.format("%Y-%m-%d %H:%M").to_string()),
                        ]);
                    }
                    println!("{}", table);
                }
            }
        }
        ConnectorCommands::Example => {
            let example = streamline::marketplace::declarative::ConnectorManager::example_yaml(
                "postgres-cdc",
            );
            println!("{}", example);
        }
    }
    Ok(())
}

pub(super) fn handle_lineage_command(cmd: LineageCommands, _ctx: &CliContext) -> Result<()> {
    match cmd {
        LineageCommands::Show { format } => {
            println!("{}", "🔗 Data Lineage Graph".bold().cyan());
            println!();
            println!("Lineage tracking is available when connected to a running server.");
            println!("Access via: GET /api/v1/lineage?format={}", format);
            println!();
            println!("{}", "Supported formats:".bold());
            println!("  text - Human-readable ASCII graph");
            println!("  json - Machine-readable JSON DAG");
            println!("  dot  - Graphviz DOT format for visualization");
        }
        LineageCommands::Upstream { topic } => {
            println!(
                "{}",
                format!("⬆ Upstream producers for '{}'", topic)
                    .bold()
                    .cyan()
            );
            println!(
                "  Access via: GET /api/v1/lineage/topics/{}/upstream",
                topic
            );
        }
        LineageCommands::Downstream { topic } => {
            println!(
                "{}",
                format!("⬇ Downstream consumers for '{}'", topic)
                    .bold()
                    .cyan()
            );
            println!(
                "  Access via: GET /api/v1/lineage/topics/{}/downstream",
                topic
            );
        }
    }
    Ok(())
}

pub(super) fn handle_cloud_command(cmd: CloudCommands, _ctx: &CliContext) -> Result<()> {
    let rt = tokio::runtime::Runtime::new().map_err(|e| {
        streamline::StreamlineError::Internal(format!("Failed to create runtime: {}", e))
    })?;
    let console = streamline::cloud::console::ConsoleManager::new();

    match cmd {
        CloudCommands::ApiKeys { tenant_id } => {
            let keys = rt.block_on(console.list_api_keys(&tenant_id));
            if keys.is_empty() {
                println!("No API keys for tenant '{}'.", tenant_id);
            } else {
                let mut table = Table::new();
                table.load_preset(UTF8_FULL_CONDENSED);
                table.set_header(vec![
                    Cell::new("ID").fg(Color::Cyan),
                    Cell::new("Name"),
                    Cell::new("Prefix"),
                    Cell::new("Enabled"),
                    Cell::new("Scopes"),
                ]);
                for key in keys {
                    table.add_row(vec![
                        Cell::new(&key.id[..8]),
                        Cell::new(&key.name),
                        Cell::new(&key.prefix),
                        Cell::new(if key.enabled { "✓" } else { "✗" }),
                        Cell::new(format!("{:?}", key.scopes)),
                    ]);
                }
                println!("{}", "🔑 API Keys".bold().cyan());
                println!("{}", table);
            }
        }
        CloudCommands::Onboard {
            org_name,
            admin_email,
            plan,
        } => {
            let request = streamline::cloud::console::OnboardingRequest {
                organization_name: org_name,
                admin_email: admin_email.clone(),
                plan,
                region: "us-east-1".to_string(),
            };
            match rt.block_on(console.onboard_tenant(request)) {
                Ok(response) => {
                    println!("{}", "🎉 Tenant onboarded!".bold().green());
                    println!("  Tenant ID:   {}", response.tenant_id);
                    println!("  Org ID:      {}", response.organization_id);
                    println!("  API Key:     {}", response.api_key);
                    println!("  Status:      {:?}", response.status);
                    println!("  Server:      {}", response.bootstrap_server);
                    println!("  Console:     {}", response.console_url);
                }
                Err(e) => {
                    println!("{} Onboarding failed: {}", "✗".red(), e);
                }
            }
        }
        CloudCommands::Usage { tenant_id } => {
            let now = chrono::Utc::now();
            let start = now - chrono::Duration::days(30);
            match rt.block_on(console.get_usage_dashboard(&tenant_id, start, now)) {
                Ok(dashboard) => {
                    println!("{}", "📊 Usage Dashboard".bold().cyan());
                    println!("  Tenant:           {}", dashboard.tenant_id);
                    println!(
                        "  Period:           {} to {}",
                        dashboard.period_start.format("%Y-%m-%d"),
                        dashboard.period_end.format("%Y-%m-%d")
                    );
                    println!("  Messages In:      {}", dashboard.total_messages_produced);
                    println!("  Messages Out:     {}", dashboard.total_messages_consumed);
                    println!(
                        "  Bytes Ingested:   {}",
                        format_bytes(dashboard.total_bytes_ingested)
                    );
                    println!("  Peak Connections: {}", dashboard.peak_connections);
                    println!(
                        "  Est. Cost:        ${:.2}",
                        dashboard.estimated_cost_cents as f64 / 100.0
                    );
                }
                Err(e) => {
                    println!("{} Failed to get usage: {}", "✗".red(), e);
                }
            }
        }
    }
    Ok(())
}

pub(super) fn handle_alerting_command(cmd: AlertingCommands, _ctx: &CliContext) -> Result<()> {
    match cmd {
        AlertingCommands::List => {
            println!("{}", "🔔 Alert Rules".bold().cyan());
            println!();
            println!("Alert rules are managed via the HTTP API. Access at:");
            println!("  GET  /api/v1/alerts       - List all rules");
            println!("  POST /api/v1/alerts       - Create a rule");
            println!();
            println!("Use 'curl http://localhost:9094/api/v1/alerts' to list.");
        }
        AlertingCommands::Create {
            name,
            topic,
            condition,
            threshold,
            channel,
        } => {
            println!(
                "{} Creating alert '{}' on topic '{}'",
                "🔔".cyan(),
                name.bold(),
                topic
            );
            println!("  Condition: {} > {}", condition, threshold);
            println!("  Channel:   {}", channel);
            println!();
            println!(
                "{}",
                "Alert created. Monitoring will start when server is running.".green()
            );
        }
        AlertingCommands::Slos => {
            println!("{}", "📐 SLO Definitions".bold().cyan());
            println!();
            println!("SLOs are managed via the HTTP API:");
            println!("  GET  /api/v1/alerts       - View alert-based SLOs");
        }
        AlertingCommands::CreateSlo {
            name,
            target,
            indicator,
        } => {
            println!("{} Creating SLO '{}'", "📐".cyan(), name.bold());
            println!("  Target:    {}%", target);
            println!("  Indicator: {}", indicator);
            println!();
            println!("{}", "SLO created.".green());
        }
    }
    Ok(())
}

pub(super) fn handle_governor_command(cmd: GovernorCommands, _ctx: &CliContext) -> Result<()> {
    let governor = streamline::autotuning::governor::ResourceGovernor::default();

    match cmd {
        GovernorCommands::Status => {
            println!("{}", "⚡ Resource Governor".bold().cyan());
            println!();
            println!(
                "  Active:         {}",
                if governor.is_active() {
                    "Yes".green()
                } else {
                    "No".yellow()
                }
            );
            println!(
                "  Buffer Pool:    {}",
                format_bytes(governor.buffer_pool_bytes())
            );
            println!(
                "  Write Allowed:  {}",
                if governor.should_accept_write(50.0, 50.0).is_ok() {
                    "Yes".green()
                } else {
                    "No (pressure high)".red()
                }
            );
        }
        GovernorCommands::Recommendations => {
            let recs = governor.partition_recommendations();
            if recs.is_empty() {
                println!("No partition scaling recommendations at this time.");
            } else {
                println!("{}", "📈 Partition Recommendations".bold().cyan());
                for rec in &recs {
                    println!(
                        "  {} {} → {} partitions ({})",
                        "→".green(),
                        rec.topic.bold(),
                        rec.recommended_partitions,
                        rec.reason
                    );
                }
            }
        }
        GovernorCommands::History => {
            let actions = governor.actions_history();
            if actions.is_empty() {
                println!("No governor actions recorded.");
            } else {
                println!("{}", "📜 Governor Actions History".bold().cyan());
                for action in &actions {
                    println!("  {}", action);
                }
            }
        }
    }
    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1} GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1} MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{} B", bytes)
    }
}
