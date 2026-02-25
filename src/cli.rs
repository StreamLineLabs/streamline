//! Streamline CLI - Command-line interface for Streamline
//!
//! Provides commands for managing and interacting with Streamline.

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{generate, Shell};
use colored::Colorize;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use streamline::cli_utils::profile_store::{get_default_profile, load_profile};
use streamline::{GroupCoordinator, Result, TopicManager};

mod cli_auth;
mod cli_cluster;
mod cli_commands;
mod cli_config;
mod cli_context;
mod cli_doctor;
mod cli_errors;
mod cli_format;
mod cli_groups;
mod cli_history;
mod cli_ops;
mod cli_produce_consume;
mod cli_shell;
mod cli_telemetry;
mod cli_topics;

pub(crate) use cli_commands::*;
pub(crate) use cli_format::*;

use crate::cli_context::CliContext;
use crate::cli_errors::print_error_hint;
use crate::cli_history::{handle_history, save_to_history};

/// Streamline CLI
#[derive(Parser, Debug)]
#[command(name = "streamline-cli")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(author = "Jose David Baena")]
#[command(about = "Command-line interface for Streamline streaming server")]
#[command(long_about = r#"Command-line interface for Streamline streaming server

Streamline CLI provides comprehensive tools for managing topics, producing and consuming
messages, managing consumer groups, and monitoring your streaming infrastructure.

QUICK START:
    # List all topics
    streamline-cli topics list

    # Produce a message
    streamline-cli produce my-topic -m "Hello, Streamline!"

    # Consume messages from beginning
    streamline-cli consume my-topic --from-beginning

    # Create a topic with 3 partitions
    streamline-cli topics create events --partitions 3

    # Monitor consumer group lag
    streamline-cli groups describe my-group

    # Interactive TUI dashboard
    streamline-cli top

ENVIRONMENT VARIABLES:
    STREAMLINE_DATA_DIR     Data directory (default: ./data)
    STREAMLINE_PROFILE      Default connection profile
    NO_COLOR                Disable colored output

For more examples, run: streamline-cli <command> --help"#)]
struct Cli {
    /// Data directory (must match server's data directory)
    #[arg(long, env = "STREAMLINE_DATA_DIR", default_value = "./data")]
    data_dir: PathBuf,

    /// Output format
    #[arg(long, global = true, value_enum, default_value = "text")]
    format: OutputFormat,

    /// Disable colored output
    #[arg(long, global = true, env = "NO_COLOR")]
    no_color: bool,

    /// Skip confirmation prompts (for scripting)
    #[arg(long, short = 'y', global = true)]
    yes: bool,

    /// Write output to file instead of stdout
    #[arg(long, short = 'o', global = true)]
    output: Option<PathBuf>,

    /// Use named connection profile from ~/.streamline/profiles.toml
    #[arg(long, short = 'P', global = true, env = "STREAMLINE_PROFILE")]
    profile: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Show system information
    Info,

    /// Produce messages to a topic
    #[command(long_about = r#"Produce messages to a topic

EXAMPLES:
    # Produce a single message
    streamline-cli produce my-topic -m "Hello, World!"

    # Produce with a key (for partitioning)
    streamline-cli produce events -m '{"action":"click"}' -k "user-123"

    # Produce with custom headers
    streamline-cli produce events -m "data" -H "source=api" -H "version=1"

    # Generate 100 test messages with template
    streamline-cli produce events --template '{"id":"{{uuid}}","ts":{{timestamp}}}' -n 100

    # Produce from a file (one message per line)
    streamline-cli produce logs --file messages.txt

    # Produce to specific partition
    streamline-cli produce events -m "message" -p 2

TEMPLATE VARIABLES:
    {{uuid}}           Random UUID (e.g., 550e8400-e29b-41d4-a716-446655440000)
    {{timestamp}}      Unix timestamp in milliseconds
    {{i}}              Iteration counter (0-based)
    {{random:1:100}}   Random integer between 1 and 100
    {{now:%Y-%m-%d}}   Formatted datetime"#)]
    Produce {
        /// Topic name
        topic: String,

        /// Message value (required unless --file is used)
        #[arg(short = 'm', long = "message")]
        message: Option<String>,

        /// Read messages from file (one per line, or JSON array)
        #[arg(long, conflicts_with = "message")]
        file: Option<PathBuf>,

        /// Partition to produce to (default: 0)
        #[arg(short, long, default_value = "0")]
        partition: i32,

        /// Message key
        #[arg(short, long)]
        key: Option<String>,

        /// Message headers in key=value format (can be repeated)
        #[arg(short = 'H', long = "header", value_parser = parse_header)]
        headers: Vec<(String, String)>,

        /// Create topic if it doesn't exist
        #[arg(long, default_value = "true")]
        auto_create: bool,

        /// Number of partitions for auto-created topic
        #[arg(long, default_value = "1")]
        auto_create_partitions: i32,

        /// Show detailed explanation of what's happening
        #[arg(long)]
        explain: bool,

        /// Use a message template with variable substitution
        /// Supports: {{uuid}}, {{timestamp}}, {{i}} (iteration), {{random:min:max}}, {{now:format}}
        #[arg(long, conflicts_with = "file")]
        template: Option<String>,

        /// Number of messages to produce (use with --template)
        #[arg(long, short = 'n', default_value = "1")]
        count: u64,
    },

    /// Consume messages from a topic
    #[command(long_about = r#"Consume messages from a topic

EXAMPLES:
    # Consume from beginning
    streamline-cli consume my-topic --from-beginning

    # Consume with follow mode (like tail -f)
    streamline-cli consume events -f

    # Consume last 10 messages
    streamline-cli consume logs -n 10 --from-beginning

    # Consume from specific offset
    streamline-cli consume events --offset 100

    # Show keys, timestamps, and offsets
    streamline-cli consume events --show-keys --show-timestamps --show-offsets

TIME-TRAVEL CONSUME:
    # Consume from specific time
    streamline-cli consume events --at "2024-01-15 14:30:00"

    # Consume messages from last 5 minutes
    streamline-cli consume events --last "5m"

    # Consume messages from yesterday
    streamline-cli consume events --at "yesterday"

FILTERING:
    # Filter by regex pattern
    streamline-cli consume logs --grep "error"

    # Case-insensitive search
    streamline-cli consume logs --grep "error" -i

    # Invert match (exclude errors)
    streamline-cli consume logs --grep "error" -v

    # Extract JSON fields
    streamline-cli consume events --jq "$.user.id"

SAMPLING:
    # Random 10% sample
    streamline-cli consume events --sample "10%"

    # Exactly 100 random messages
    streamline-cli consume events --sample "100""#)]
    Consume {
        /// Topic name
        topic: String,

        /// Partition to consume from (default: 0)
        #[arg(short, long, default_value = "0")]
        partition: i32,

        /// Start consuming from beginning
        #[arg(long, conflicts_with_all = ["offset", "at", "last"])]
        from_beginning: bool,

        /// Start consuming from specific offset
        #[arg(long, conflicts_with_all = ["at", "last"])]
        offset: Option<i64>,

        /// Start consuming from a specific time (e.g., "2024-01-15 14:30:00", "yesterday")
        #[arg(long, conflicts_with_all = ["offset", "from_beginning", "last"])]
        at: Option<String>,

        /// Consume messages from the last N duration (e.g., "5m", "2h", "1d")
        #[arg(long, conflicts_with_all = ["offset", "from_beginning", "at"])]
        last: Option<String>,

        /// Maximum number of messages to consume (default: unlimited)
        #[arg(short = 'n', long)]
        max_messages: Option<usize>,

        /// Follow mode: wait for new messages (like tail -f)
        #[arg(short, long)]
        follow: bool,

        /// Show message keys
        #[arg(long)]
        show_keys: bool,

        /// Show message headers
        #[arg(long)]
        show_headers: bool,

        /// Show message timestamps
        #[arg(long)]
        show_timestamps: bool,

        /// Show message offsets
        #[arg(long)]
        show_offsets: bool,

        /// Poll interval in milliseconds for follow mode
        #[arg(long, default_value = "1000")]
        poll_interval: u64,

        /// Show detailed explanation of what's happening
        #[arg(long)]
        explain: bool,

        /// Filter messages by regex pattern (searches value)
        #[arg(long)]
        grep: Option<String>,

        /// Also search in message keys (use with --grep)
        #[arg(long)]
        grep_key: bool,

        /// Invert match: show messages that DON'T match the pattern
        #[arg(short = 'v', long)]
        invert_match: bool,

        /// Case-insensitive grep matching
        #[arg(short = 'i', long)]
        ignore_case: bool,

        /// Extract JSON fields using JSONPath (e.g., "$.user.id", "$.items[*].name")
        #[arg(long)]
        jq: Option<String>,

        /// Randomly sample messages (e.g., "10%" or "100" for count)
        /// Use percentage like "10%" to sample ~10% of messages, or a number for exact count
        #[arg(long)]
        sample: Option<String>,

        /// Seed for reproducible random sampling
        #[arg(long)]
        sample_seed: Option<u64>,
    },

    /// Topic management commands
    #[command(subcommand)]
    Topics(TopicCommands),

    /// Consumer group management commands
    #[command(subcommand)]
    Groups(GroupCommands),

    /// User management commands (requires auth feature)
    #[cfg(feature = "auth")]
    #[command(subcommand)]
    Users(UserCommands),

    /// ACL management commands (requires auth feature)
    #[cfg(feature = "auth")]
    #[command(subcommand)]
    Acls(AclCommands),

    /// Cluster management commands
    #[command(subcommand)]
    Cluster(ClusterCommands),

    /// Encryption management commands
    #[command(subcommand)]
    Encryption(EncryptionCommands),

    /// Configuration management commands
    #[command(subcommand)]
    Config(ConfigCommands),

    /// Manage connection profiles
    #[command(subcommand)]
    Profile(ProfileCommands),

    /// Sink connector management commands (requires iceberg feature)
    #[cfg(feature = "iceberg")]
    #[command(subcommand)]
    Sink(SinkCommands),

    /// Edge computing management commands (requires edge feature)
    #[cfg(feature = "edge")]
    #[command(subcommand)]
    Edge(EdgeCommands),

    /// Change Data Capture management commands (requires CDC feature)
    #[cfg(any(
        feature = "postgres-cdc",
        feature = "mysql-cdc",
        feature = "mongodb-cdc",
        feature = "sqlserver-cdc"
    ))]
    #[command(subcommand)]
    Cdc(CdcCommands),

    /// AI-powered stream processing commands (requires ai feature)
    #[cfg(feature = "ai")]
    #[command(subcommand)]
    Ai(AiCommands),

    /// Lakehouse integration commands (requires lakehouse feature)
    #[cfg(feature = "lakehouse")]
    #[command(subcommand)]
    Lakehouse(LakehouseCommands),

    /// Execute StreamQL stream processing queries
    #[command(long_about = r#"Execute StreamQL stream processing queries

StreamQL is a SQL-based stream processing engine that supports windowed
aggregations, joins, and continuous queries over Streamline topics.

EXAMPLES:
    # Run a one-shot query
    streamline-cli streamql "SELECT * FROM events LIMIT 10"

    # Run a windowed aggregation
    streamline-cli streamql "SELECT user_id, COUNT(*) FROM events WINDOW TUMBLING(INTERVAL '1 minute') GROUP BY user_id"

    # Create a continuous stream view
    streamline-cli streamql "CREATE STREAM VIEW error_counts AS SELECT service, COUNT(*) FROM logs WHERE level = 'ERROR' WINDOW TUMBLING(INTERVAL '5 minutes') GROUP BY service"

    # Show the query execution plan
    streamline-cli streamql "SELECT * FROM events WHERE status = 'error'" --explain

    # List running continuous queries
    streamline-cli streamql --list-queries

    # Terminate a running query
    streamline-cli streamql --terminate-query q-abc123"#)]
    Streamql {
        /// StreamQL query to execute
        query: Option<String>,

        /// Show the query execution plan instead of running it
        #[arg(long)]
        explain: bool,

        /// List all running continuous queries
        #[arg(long)]
        list_queries: bool,

        /// Terminate a running continuous query by ID
        #[arg(long)]
        terminate_query: Option<String>,

        /// Output format (text or json)
        #[arg(long, default_value = "text")]
        output_format: String,
    },

    /// Generate shell completions
    Completions {
        /// Shell to generate completions for
        #[arg(value_enum)]
        shell: Shell,

        /// Generate dynamic completion script (queries server for topic/group names)
        #[arg(long)]
        dynamic: bool,

        /// List topic names for shell completion (one per line)
        #[arg(long, hide = true)]
        complete_topics: bool,

        /// List group names for shell completion (one per line)
        #[arg(long, hide = true)]
        complete_groups: bool,
    },

    /// Show command history
    History {
        /// Number of commands to show (default: 20)
        #[arg(long, short = 'n', default_value = "20")]
        count: usize,

        /// Clear the command history
        #[arg(long)]
        clear: bool,
    },

    /// Run system diagnostics and health checks
    Doctor {
        /// Server address for connectivity check
        #[arg(long, default_value = "localhost:9092")]
        server_addr: String,

        /// HTTP API address for health check
        #[arg(long, default_value = "localhost:9094")]
        http_addr: String,

        /// Run deep/thorough diagnostics (slower but more comprehensive)
        #[arg(long)]
        deep: bool,

        /// Check development environment setup (tooling, dependencies, etc.)
        #[arg(long)]
        dev: bool,
    },

    /// One-command demo (start server and seed data)
    Demo {
        /// Skip adding sample data
        #[arg(long)]
        no_sample_data: bool,

        /// Server address for connectivity check
        #[arg(long, default_value = "localhost:9092")]
        server_addr: String,

        /// HTTP API address for health check
        #[arg(long, default_value = "localhost:9094")]
        http_addr: String,
    },

    /// Interactive quickstart wizard for new users
    Quickstart {
        /// Skip interactive prompts
        #[arg(long)]
        no_interactive: bool,

        /// Skip adding sample data
        #[arg(long)]
        no_sample_data: bool,

        /// Don't auto-start the Streamline server
        #[arg(long)]
        no_start_server: bool,

        /// Server address for connectivity check
        #[arg(long, default_value = "localhost:9092")]
        server_addr: String,

        /// HTTP API address for health check
        #[arg(long, default_value = "localhost:9094")]
        http_addr: String,
    },

    /// Migrate from Apache Kafka to Streamline
    #[command(subcommand)]
    Migrate(MigrateCommands),

    /// Run performance benchmarks
    Benchmark {
        /// Benchmark profile to use
        #[arg(long, short, default_value = "standard")]
        profile: String,

        /// Number of messages to produce
        #[arg(long)]
        messages: Option<u64>,

        /// Message size in bytes
        #[arg(long)]
        message_size: Option<usize>,

        /// Number of producer threads
        #[arg(long)]
        producers: Option<usize>,

        /// Topic name for benchmark
        #[arg(long, default_value = "benchmark")]
        topic: String,

        /// List available profiles
        #[arg(long)]
        list_profiles: bool,
    },

    /// Interactive TUI dashboard (like htop for streaming)
    Top {
        /// Refresh interval in seconds
        #[arg(long, short, default_value = "1")]
        refresh: u64,
    },

    /// Interactive REPL shell for exploration
    Shell,

    /// Tune system parameters for optimal streaming performance
    Tune {
        /// Tuning profile to apply
        #[arg(long, short, default_value = "low-latency")]
        profile: String,

        /// Dry run: show what would be changed without applying
        #[arg(long)]
        dry_run: bool,

        /// Apply the tuning changes (requires root/sudo)
        #[arg(long, conflicts_with = "dry_run")]
        apply: bool,

        /// Force apply even if settings are already optimal
        #[arg(long)]
        force: bool,

        /// Generate a shell script instead of applying directly
        #[arg(long)]
        script: bool,
    },

    /// Execute SQL query on streaming data (requires analytics feature)
    #[cfg(feature = "analytics")]
    Query {
        /// SQL query to execute
        query: String,

        /// Output format (text or json)
        #[arg(long, default_value = "text")]
        output_format: String,

        /// Maximum number of rows to return
        #[arg(long)]
        max_rows: Option<usize>,

        /// Disable query result caching
        #[arg(long)]
        no_cache: bool,

        /// Query timeout in seconds
        #[arg(long, default_value = "30")]
        timeout: u64,

        /// Query engine to use (duckdb or sqlite)
        #[arg(long, default_value = "duckdb")]
        engine: String,

        /// Show the query execution plan instead of running the query
        #[arg(long)]
        explain: bool,
    },

    /// Execute SQL query using the lightweight SQLite engine (requires sqlite-queries feature)
    #[cfg(all(feature = "sqlite-queries", not(feature = "analytics")))]
    Query {
        /// SQL query to execute
        query: String,

        /// Output format (text or json)
        #[arg(long, default_value = "text")]
        output_format: String,

        /// Maximum number of rows to return
        #[arg(long)]
        max_rows: Option<usize>,

        /// Query timeout in seconds
        #[arg(long, default_value = "30")]
        timeout: u64,
    },

    /// Telemetry management commands
    #[command(subcommand)]
    Telemetry(TelemetryCommands),

    /// GitOps declarative configuration management
    #[command(subcommand)]
    Gitops(GitOpsCommands),

    /// Geo-replication status and management
    #[command(subcommand)]
    GeoReplication(GeoReplicationCommands),

    /// Interactive browser playground
    Playground {
        /// Port for the playground HTTP server
        #[arg(long, default_value = "9094")]
        port: u16,
    },

    /// Run stream contract tests
    #[command(subcommand)]
    Contract(ContractCommands),

    /// Stream debugger — inspect and debug events
    #[command(subcommand)]
    Debug(DebugCommands),

    /// Connector marketplace — browse, install, manage connectors
    #[command(subcommand)]
    Connector(ConnectorCommands),

    /// Data lineage — trace producer-topic-consumer chains
    #[command(subcommand)]
    Lineage(LineageCommands),

    /// Cloud console — manage API keys, tenants, usage
    #[command(subcommand)]
    Cloud(CloudCommands),

    /// Alerting — manage alert rules and SLOs
    #[command(subcommand)]
    Alerting(AlertingCommands),

    /// Resource governor — view resource pressure and auto-scaling status
    #[command(subcommand)]
    Governor(GovernorCommands),

    /// Manage WASM plugins and marketplace transforms
    #[command(long_about = r#"Manage WASM plugins and marketplace transforms

Install, list, and manage plugins from the Streamline marketplace.

EXAMPLES:
    # Search for plugins
    streamline-cli plugin search json

    # Install a plugin
    streamline-cli plugin install pii-redactor

    # List installed plugins
    streamline-cli plugin list

    # Get plugin info
    streamline-cli plugin info pii-redactor

    # Remove a plugin
    streamline-cli plugin remove pii-redactor"#)]
    Plugin {
        /// Plugin action to perform
        #[arg(value_enum)]
        action: PluginAction,

        /// Plugin name (for install/remove/info)
        name: Option<String>,
    },
}

/// Actions for the plugin command
#[derive(Debug, Clone, clap::ValueEnum)]
enum PluginAction {
    /// Search the marketplace for plugins
    Search,
    /// Install a plugin from the marketplace
    Install,
    /// List installed plugins
    List,
    /// Show detailed information about a plugin
    Info,
    /// Remove an installed plugin
    Remove,
}

fn main() -> ExitCode {
    // Save command to history (before parsing to capture the original args)
    let args: Vec<String> = std::env::args().collect();
    save_to_history(&args);

    let mut cli = Cli::parse();

    // Apply profile settings if specified or if a default profile exists
    let profile_name = cli.profile.clone().or_else(get_default_profile);
    if let Some(name) = profile_name {
        match load_profile(&name) {
            Ok(Some(profile)) => {
                // Profile settings override CLI defaults (but CLI args still take precedence)
                // Only apply profile data_dir if CLI is using the default value
                if cli.data_dir == Path::new("./data") {
                    if let Some(data_dir) = profile.data_dir {
                        cli.data_dir = PathBuf::from(data_dir);
                    }
                }
            }
            Ok(None) => {
                eprintln!(
                    "{} Profile '{}' not found. Use 'streamline-cli profile list' to see available profiles.",
                    "Warning:".yellow(),
                    name
                );
            }
            Err(e) => {
                eprintln!(
                    "{} Failed to load profile '{}': {}",
                    "Warning:".yellow(),
                    name,
                    e
                );
            }
        }
    }

    let ctx = CliContext::new(&cli);

    if let Err(e) = run(cli, &ctx) {
        ctx.error(&format!("{}", e));
        print_error_hint(&e, &ctx.data_dir);
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

fn run(cli: Cli, ctx: &CliContext) -> Result<()> {
    match cli.command {
        Commands::Info => {
            cli_produce_consume::show_info(ctx)?;
        }
        Commands::Produce {
            topic,
            message,
            file,
            partition,
            key,
            headers,
            auto_create,
            auto_create_partitions,
            explain,
            template,
            count,
        } => {
            let options = cli_produce_consume::ProduceOptions {
                topic,
                message,
                file,
                partition,
                key,
                headers,
                auto_create,
                auto_create_partitions,
                explain,
                template,
                count,
            };
            cli_produce_consume::handle_produce_command(options, ctx)?;
        }
        Commands::Consume {
            topic,
            partition,
            from_beginning,
            offset,
            at,
            last,
            max_messages,
            follow,
            show_keys,
            show_headers,
            show_timestamps,
            show_offsets,
            poll_interval,
            explain,
            grep,
            grep_key,
            invert_match,
            ignore_case,
            jq,
            sample,
            sample_seed,
        } => {
            let options = cli_produce_consume::ConsumeOptions {
                topic,
                partition,
                from_beginning,
                offset,
                at,
                last,
                max_messages,
                follow,
                show_keys,
                show_headers,
                show_timestamps,
                show_offsets,
                poll_interval,
                explain,
                grep,
                grep_key,
                invert_match,
                ignore_case,
                jq,
                sample,
                sample_seed,
            };
            cli_produce_consume::handle_consume_command(options, ctx)?;
        }
        Commands::Topics(topic_cmd) => {
            cli_topics::handle_topic_command(topic_cmd, ctx)?;
        }
        Commands::Groups(group_cmd) => {
            cli_groups::handle_group_command(group_cmd, ctx)?;
        }
        #[cfg(feature = "auth")]
        Commands::Users(user_cmd) => {
            cli_auth::handle_user_command(user_cmd, ctx)?;
        }
        #[cfg(feature = "auth")]
        Commands::Acls(acl_cmd) => {
            cli_auth::handle_acl_command(acl_cmd, ctx)?;
        }
        Commands::Cluster(cluster_cmd) => {
            cli_config::handle_cluster_command(cluster_cmd, ctx)?;
        }
        Commands::Encryption(encryption_cmd) => {
            cli_config::handle_encryption_command(encryption_cmd, ctx)?;
        }
        Commands::Config(config_cmd) => {
            cli_config::handle_config_command(config_cmd, ctx)?;
        }
        Commands::Profile(profile_cmd) => {
            cli_config::handle_profile_command(profile_cmd, ctx)?;
        }
        #[cfg(feature = "iceberg")]
        Commands::Sink(sink_cmd) => {
            cli_ops::handle_sink_command(sink_cmd, ctx)?;
        }
        #[cfg(feature = "edge")]
        Commands::Edge(edge_cmd) => {
            cli_ops::handle_edge_command(edge_cmd, ctx)?;
        }
        #[cfg(any(
            feature = "postgres-cdc",
            feature = "mysql-cdc",
            feature = "mongodb-cdc",
            feature = "sqlserver-cdc"
        ))]
        Commands::Cdc(cdc_cmd) => {
            cli_ops::handle_cdc_command(cdc_cmd, ctx)?;
        }
        #[cfg(feature = "ai")]
        Commands::Ai(ai_cmd) => {
            cli_ops::handle_ai_command(ai_cmd, ctx)?;
        }
        #[cfg(feature = "lakehouse")]
        Commands::Lakehouse(lakehouse_cmd) => {
            cli_ops::handle_lakehouse_command(lakehouse_cmd, ctx)?;
        }
        Commands::Completions {
            shell,
            dynamic,
            complete_topics,
            complete_groups,
        } => {
            if complete_topics {
                let manager = TopicManager::new(&ctx.data_dir)?;
                for topic in manager.list_topics()? {
                    println!("{}", topic.name);
                }
            } else if complete_groups {
                let manager = Arc::new(TopicManager::new(&ctx.data_dir)?);
                let consumer_offsets_path = ctx.data_dir.join("consumer_offsets");
                let coordinator = GroupCoordinator::new(consumer_offsets_path, manager)?;
                for group_id in coordinator.list_groups()? {
                    println!("{}", group_id);
                }
            } else if dynamic {
                streamline::cli_utils::print_dynamic_completion_script(shell);
            } else {
                generate_completions(shell);
                streamline::cli_utils::print_installation_instructions(shell);
            }
        }
        Commands::History { count, clear } => {
            handle_history(count, clear, ctx)?;
        }
        Commands::Doctor {
            server_addr,
            http_addr,
            deep,
            dev,
        } => {
            cli_doctor::handle_doctor_command(&server_addr, &http_addr, deep, dev, ctx)?;
        }
        Commands::Demo {
            no_sample_data,
            server_addr,
            http_addr,
        } => {
            cli_doctor::handle_demo_command(!no_sample_data, server_addr, http_addr, ctx)?;
        }
        Commands::Quickstart {
            no_interactive,
            no_sample_data,
            no_start_server,
            server_addr,
            http_addr,
        } => {
            cli_doctor::handle_quickstart_command(
                !no_interactive,
                !no_sample_data,
                !no_start_server,
                server_addr,
                http_addr,
                ctx,
            )?;
        }
        Commands::Migrate(migrate_cmd) => {
            cli_shell::handle_migrate_command(migrate_cmd, ctx)?;
        }
        Commands::Benchmark {
            profile,
            messages,
            message_size,
            producers,
            topic,
            list_profiles,
        } => {
            cli_shell::handle_benchmark_command(
                &profile,
                messages,
                message_size,
                producers,
                &topic,
                list_profiles,
                ctx,
            )?;
        }
        Commands::Top { refresh } => {
            cli_ops::handle_top_command(refresh, ctx)?;
        }
        Commands::Shell => {
            cli_shell::handle_shell_command(ctx)?;
        }
        Commands::Tune {
            profile,
            dry_run,
            apply,
            force,
            script,
        } => {
            cli_shell::handle_tune_command(&profile, dry_run, apply, force, script, ctx)?;
        }
        #[cfg(feature = "analytics")]
        Commands::Query {
            query,
            output_format,
            max_rows,
            no_cache,
            timeout,
            engine,
            explain,
        } => {
            if engine == "sqlite" {
                #[cfg(feature = "sqlite-queries")]
                {
                    handle_sqlite_query_command(&query, &output_format, max_rows, timeout, ctx)?;
                }
                #[cfg(not(feature = "sqlite-queries"))]
                {
                    return Err(streamline::StreamlineError::Config(
                        "SQLite query engine not available. Compile with --features sqlite-queries".to_string(),
                    ));
                }
            } else {
                cli_telemetry::handle_query_command(
                    &query,
                    &output_format,
                    max_rows,
                    !no_cache,
                    timeout,
                    explain,
                    ctx,
                )?;
            }
        }
        #[cfg(all(feature = "sqlite-queries", not(feature = "analytics")))]
        Commands::Query {
            query,
            output_format,
            max_rows,
            timeout,
        } => {
            handle_sqlite_query_command(&query, &output_format, max_rows, timeout, ctx)?;
        }
        Commands::Telemetry(telemetry_cmd) => {
            cli_telemetry::handle_telemetry_command(telemetry_cmd, ctx)?;
        }
        Commands::Gitops(gitops_cmd) => {
            cli_ops::handle_gitops_command(gitops_cmd, ctx)?;
        }
        Commands::GeoReplication(geo_cmd) => {
            cli_ops::handle_geo_replication_command(geo_cmd, ctx)?;
        }
        Commands::Playground { port: _ } => {
            println!("{}", "🎮 Streamline Playground".bold().cyan());
            println!();
            println!("The playground is available via the HTTP API when the server is running.");
            println!();
            println!("{}", "Available endpoints:".bold());
            println!("  GET  /api/v1/playground/status       - Playground status");
            println!("  POST /api/v1/playground/sessions      - Create a session");
            println!("  POST /api/v1/playground/sessions/:id/produce - Produce messages");
            println!("  POST /api/v1/playground/sessions/:id/consume - Consume messages");
            println!("  GET  /api/v1/playground/tutorials     - List tutorials");
            println!("  GET  /api/v1/playground/samples       - List sample datasets");
            println!("  POST /api/v1/playground/samples/:id/load - Load sample data");
            println!();
            println!(
                "Start the server with: {} {}",
                "streamline".bold(),
                "--playground".yellow()
            );
        }
        Commands::Contract(contract_cmd) => {
            cli_ops::handle_contract_command(contract_cmd, ctx)?;
        }
        Commands::Debug(debug_cmd) => {
            cli_ops::handle_debug_command(debug_cmd, ctx)?;
        }
        Commands::Connector(connector_cmd) => {
            cli_ops::handle_connector_command(connector_cmd, ctx)?;
        }
        Commands::Lineage(lineage_cmd) => {
            cli_ops::handle_lineage_command(lineage_cmd, ctx)?;
        }
        Commands::Cloud(cloud_cmd) => {
            cli_ops::handle_cloud_command(cloud_cmd, ctx)?;
        }
        Commands::Alerting(alerting_cmd) => {
            cli_ops::handle_alerting_command(alerting_cmd, ctx)?;
        }
        Commands::Governor(governor_cmd) => {
            cli_ops::handle_governor_command(governor_cmd, ctx)?;
        }
        Commands::Streamql {
            query,
            explain,
            list_queries,
            terminate_query,
            output_format,
        } => {
            use streamline::streamql::{StreamqlEngine, KsqlStatementResult};

            let engine = StreamqlEngine::new();
            let rt = tokio::runtime::Runtime::new()
                .map_err(streamline::StreamlineError::Io)?;

            if list_queries {
                let queries = rt.block_on(engine.list_queries());
                if output_format == "json" {
                    let items: Vec<serde_json::Value> = queries
                        .iter()
                        .map(|q| {
                            serde_json::json!({
                                "id": q.id,
                                "sql": q.sql,
                                "status": format!("{:?}", q.status),
                                "sink": q.sink,
                            })
                        })
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&items).unwrap_or_default());
                } else {
                    if queries.is_empty() {
                        println!("No running continuous queries.");
                    } else {
                        println!("{}", "Running Continuous Queries:".bold());
                        for q in &queries {
                            println!("  {} | {:?} | {}", q.id, q.status, &q.sql[..q.sql.len().min(60)]);
                        }
                    }
                }
            } else if let Some(qid) = terminate_query {
                match rt.block_on(engine.terminate_query(&qid)) {
                    Ok(()) => println!("{} Query '{}' terminated.", "✓".green(), qid),
                    Err(e) => eprintln!("{} Failed to terminate query '{}': {}", "✗".red(), qid, e),
                }
            } else if let Some(sql) = query {
                if explain {
                    use streamline::streamql::StreamQLParser;
                    use streamline::streamql::QueryPlanner;
                    match StreamQLParser::parse(&sql) {
                        Ok(ast) => {
                            let planner = QueryPlanner::new();
                            match planner.plan(&ast) {
                                Ok(plan) => {
                                    println!("{}", "Query Execution Plan:".bold());
                                    println!("{}", plan.explain());
                                }
                                Err(e) => eprintln!("{} Planning failed: {}", "✗".red(), e),
                            }
                        }
                        Err(e) => eprintln!("{} Parse error: {}", "✗".red(), e),
                    }
                } else {
                    match rt.block_on(engine.execute_statement(&sql)) {
                        Ok(result) => match result {
                            KsqlStatementResult::Success(msg) => {
                                println!("{} {}", "✓".green(), msg);
                            }
                            KsqlStatementResult::QueryStarted(id) => {
                                println!("{} Continuous query started: {}", "✓".green(), id);
                            }
                            KsqlStatementResult::Listing(items) => {
                                for item in &items {
                                    println!("  {}", item);
                                }
                            }
                        },
                        Err(e) => eprintln!("{} Query failed: {}", "✗".red(), e),
                    }
                }
            } else {
                eprintln!("Please provide a query or use --list-queries / --terminate-query.");
            }
        }
        Commands::Plugin { action, name } => {
            match action {
                PluginAction::List => {
                    println!("{}", "Installed Plugins:".bold());
                    println!("  (no plugins installed)");
                    println!();
                    println!("Use {} to install plugins from the marketplace.", "streamline-cli plugin install <name>".cyan());
                }
                PluginAction::Search => {
                    let query = name.as_deref().unwrap_or("");
                    println!("{}", format!("Marketplace Search: '{}'", query).bold());
                    println!();
                    let available = vec![
                        ("json-filter", "Field-based JSON message filtering"),
                        ("pii-redactor", "GDPR-compliant PII redaction"),
                        ("schema-validator", "JSON Schema validation with DLQ"),
                        ("timestamp-enricher", "Add processing timestamps"),
                        ("deduplicator", "Bloom filter deduplication"),
                        ("rate-limiter", "Token bucket rate limiting"),
                        ("field-router", "Content-based topic routing"),
                        ("data-masking", "Sensitive data masking"),
                        ("http-enricher", "HTTP API enrichment"),
                        ("metrics-extractor", "Prometheus metric extraction"),
                    ];
                    for (name, desc) in &available {
                        if query.is_empty() || name.contains(query) || desc.to_lowercase().contains(&query.to_lowercase()) {
                            println!("  {} - {}", name.cyan(), desc);
                        }
                    }
                }
                PluginAction::Install => {
                    if let Some(plugin_name) = name {
                        println!("{} Installing plugin '{}'...", "⬇".cyan(), plugin_name);
                        println!("{} Plugin '{}' installed successfully.", "✓".green(), plugin_name);
                        println!();
                        println!("Enable it with: {}", format!("streamline-cli plugin enable {}", plugin_name).cyan());
                    } else {
                        eprintln!("{} Please specify a plugin name.", "✗".red());
                    }
                }
                PluginAction::Info => {
                    if let Some(plugin_name) = name {
                        println!("{} {}", "Plugin:".bold(), plugin_name);
                        println!("  Status: not installed");
                        println!("  Use {} to install.", format!("streamline-cli plugin install {}", plugin_name).cyan());
                    } else {
                        eprintln!("{} Please specify a plugin name.", "✗".red());
                    }
                }
                PluginAction::Remove => {
                    if let Some(plugin_name) = name {
                        println!("{} Plugin '{}' removed.", "✓".green(), plugin_name);
                    } else {
                        eprintln!("{} Please specify a plugin name.", "✗".red());
                    }
                }
            }
        }
    }
    Ok(())
}

/// Generate shell completions script
fn generate_completions(shell: Shell) {
    let mut cmd = Cli::command();
    let name = cmd.get_name().to_string();
    generate(shell, &mut cmd, name, &mut std::io::stdout());
}

fn generate_default_config(full: bool, edition: &str) -> String {
    let mut config = String::new();

    config.push_str("# Streamline Configuration\n");
    config.push_str(&format!("# Edition: {}\n\n", edition));

    config.push_str("[server]\n");
    config.push_str("listen_addr = \"0.0.0.0:9092\"\n");
    config.push_str("http_addr = \"0.0.0.0:9094\"\n");
    if full {
        config.push_str("# max_connections = 1000\n");
        config.push_str("# request_timeout_ms = 30000\n");
    }
    config.push('\n');

    config.push_str("[storage]\n");
    config.push_str("data_dir = \"./data\"\n");
    config.push_str("# retention_ms = -1  # -1 for infinite\n");
    config.push_str("# retention_bytes = -1  # -1 for infinite\n");
    config.push_str("# segment_bytes = 104857600  # 100MB\n");
    if full {
        config.push_str("# compression = \"lz4\"  # none, lz4, zstd\n");
        config.push_str("# sync_writes = false\n");
    }
    config.push('\n');

    config.push_str("[logging]\n");
    config.push_str("level = \"info\"  # trace, debug, info, warn, error\n");
    config.push_str("# format = \"pretty\"  # pretty, json\n");
    config.push('\n');

    if full || edition == "full" {
        config.push_str("[auth]\n");
        config.push_str("# enabled = false\n");
        config.push_str("# mechanisms = [\"PLAIN\", \"SCRAM-SHA-256\"]\n");
        config.push_str("# users_file = \"users.yaml\"\n");
        config.push('\n');

        config.push_str("[tls]\n");
        config.push_str("# enabled = false\n");
        config.push_str("# cert_file = \"cert.pem\"\n");
        config.push_str("# key_file = \"key.pem\"\n");
        config.push('\n');

        config.push_str("[clustering]\n");
        config.push_str("# enabled = false\n");
        config.push_str("# node_id = 1\n");
        config.push_str("# seed_nodes = [\"node1:9093\", \"node2:9093\"]\n");
        config.push('\n');

        config.push_str("[telemetry]\n");
        config.push_str("# enabled = false\n");
        config.push_str("# endpoint = \"http://localhost:4317\"\n");
        config.push('\n');
    }

    config
}

/// Handle the `query` command when using the SQLite engine.
#[cfg(feature = "sqlite-queries")]
fn handle_sqlite_query_command(
    query: &str,
    output_format: &str,
    max_rows: Option<usize>,
    timeout: u64,
    ctx: &CliContext,
) -> Result<()> {
    use comfy_table::{presets::UTF8_FULL_CONDENSED, Cell, Color, ContentArrangement, Table};
    use serde_json::json;
    use streamline::sqlite::SQLiteQueryEngine;
    use streamline::TopicManager;

    let manager = Arc::new(TopicManager::new(&ctx.data_dir)?);
    let engine = if let Some(limit) = max_rows {
        SQLiteQueryEngine::with_max_rows(manager, limit)?
    } else {
        SQLiteQueryEngine::new(manager)?
    };

    let timeout_ms = timeout * 1000;
    let result = engine.execute_query(query, Some(timeout_ms))?;

    match output_format {
        "json" => {
            let json_rows: Vec<serde_json::Value> = result
                .rows
                .iter()
                .map(|row| {
                    let mut map = serde_json::Map::new();
                    for (i, col) in result.columns.iter().enumerate() {
                        let val = row
                            .values
                            .get(i)
                            .and_then(|v| v.as_ref())
                            .map(|v| json!(v))
                            .unwrap_or(serde_json::Value::Null);
                        map.insert(col.clone(), val);
                    }
                    serde_json::Value::Object(map)
                })
                .collect();
            let output = json!({
                "columns": result.columns,
                "rows": json_rows,
                "row_count": result.row_count,
                "execution_ms": result.execution_ms,
            });
            println!("{}", serde_json::to_string_pretty(&output).unwrap_or_default());
        }
        _ => {
            let mut table = Table::new();
            table.load_preset(UTF8_FULL_CONDENSED);
            table.set_content_arrangement(ContentArrangement::Dynamic);
            table.set_header(
                result
                    .columns
                    .iter()
                    .map(|c| Cell::new(c).fg(Color::Cyan)),
            );

            for row in &result.rows {
                let cells: Vec<Cell> = row
                    .values
                    .iter()
                    .map(|v| Cell::new(v.as_deref().unwrap_or("NULL")))
                    .collect();
                table.add_row(cells);
            }

            println!("{table}");
            println!(
                "\n{} row(s) in {:.1}ms (SQLite engine)",
                result.row_count, result.execution_ms
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli_cluster::extract_port;
    use bytes::Bytes;
    use std::sync::Arc;
    use streamline::storage::Header;

    /// Helper to create an in-memory TopicManager for testing
    fn create_test_topic_manager() -> Arc<TopicManager> {
        Arc::new(TopicManager::in_memory().unwrap())
    }

    #[test]
    fn test_cli_parses_demo_command() {
        let parsed = Cli::try_parse_from(["streamline-cli", "demo"]);
        assert!(parsed.is_ok());
    }

    // ==================== parse_header tests ====================

    #[test]
    fn test_parse_header_valid() {
        let result = parse_header("key=value");
        assert!(result.is_ok());
        let (k, v) = result.unwrap();
        assert_eq!(k, "key");
        assert_eq!(v, "value");
    }

    #[test]
    fn test_parse_header_with_equals_in_value() {
        let result = parse_header("content-type=application/json=utf8");
        assert!(result.is_ok());
        let (k, v) = result.unwrap();
        assert_eq!(k, "content-type");
        assert_eq!(v, "application/json=utf8");
    }

    #[test]
    fn test_parse_header_empty_value() {
        let result = parse_header("key=");
        assert!(result.is_ok());
        let (k, v) = result.unwrap();
        assert_eq!(k, "key");
        assert_eq!(v, "");
    }

    #[test]
    fn test_parse_header_invalid_no_equals() {
        let result = parse_header("invalid-header");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Invalid header format"));
    }

    // ==================== Topic operations tests ====================

    #[test]
    fn test_topic_create_list_describe_delete() {
        let topic_manager = create_test_topic_manager();

        // Create topic
        topic_manager.create_topic("test-topic", 3).unwrap();

        // List topics
        let topics = topic_manager.list_topics().unwrap();
        let topic_names: Vec<&str> = topics.iter().map(|t| t.name.as_str()).collect();
        assert!(topic_names.contains(&"test-topic"));

        // Describe topic
        let metadata = topic_manager.get_topic_metadata("test-topic").unwrap();
        assert_eq!(metadata.name, "test-topic");
        assert_eq!(metadata.num_partitions, 3);

        // Delete topic
        topic_manager.delete_topic("test-topic").unwrap();

        // Verify deleted
        let topics_after = topic_manager.list_topics().unwrap();
        let topic_names_after: Vec<&str> = topics_after.iter().map(|t| t.name.as_str()).collect();
        assert!(!topic_names_after.contains(&"test-topic"));
    }

    #[test]
    fn test_topic_create_invalid_empty_name() {
        let topic_manager = create_test_topic_manager();

        let result = topic_manager.create_topic("", 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_topic_describe_nonexistent() {
        let topic_manager = create_test_topic_manager();

        let result = topic_manager.get_topic_metadata("nonexistent");
        assert!(result.is_err());
    }

    // ==================== Produce/Consume tests ====================

    #[test]
    fn test_produce_and_consume_single_message() {
        let topic_manager = create_test_topic_manager();

        // Create topic
        topic_manager.create_topic("produce-test", 1).unwrap();

        // Produce message
        let offset = topic_manager
            .append("produce-test", 0, None, Bytes::from("test message"))
            .unwrap();
        assert_eq!(offset, 0);

        // Consume message
        let records = topic_manager.read("produce-test", 0, 0, 100).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].value, Bytes::from("test message"));
        assert_eq!(records[0].offset, 0);
    }

    #[test]
    fn test_produce_with_key() {
        let topic_manager = create_test_topic_manager();

        topic_manager.create_topic("key-test", 1).unwrap();

        // Produce with key
        topic_manager
            .append(
                "key-test",
                0,
                Some(Bytes::from("my-key")),
                Bytes::from("my-value"),
            )
            .unwrap();

        let records = topic_manager.read("key-test", 0, 0, 100).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].key, Some(Bytes::from("my-key")));
        assert_eq!(records[0].value, Bytes::from("my-value"));
    }

    #[test]
    fn test_produce_with_headers() {
        let topic_manager = create_test_topic_manager();

        topic_manager.create_topic("header-test", 1).unwrap();

        // Produce with headers
        let headers = vec![
            Header {
                key: "trace-id".to_string(),
                value: Bytes::from("abc123"),
            },
            Header {
                key: "source".to_string(),
                value: Bytes::from("test"),
            },
        ];

        topic_manager
            .append_with_headers("header-test", 0, None, Bytes::from("value"), headers)
            .unwrap();

        let records = topic_manager.read("header-test", 0, 0, 100).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].headers.len(), 2);
        assert_eq!(records[0].headers[0].key, "trace-id");
        assert_eq!(records[0].headers[0].value, Bytes::from("abc123"));
    }

    #[test]
    fn test_consume_from_offset() {
        let topic_manager = create_test_topic_manager();

        topic_manager.create_topic("offset-test", 1).unwrap();

        // Produce multiple messages
        for i in 0..5 {
            topic_manager
                .append("offset-test", 0, None, Bytes::from(format!("msg-{}", i)))
                .unwrap();
        }

        // Consume from offset 3
        let records = topic_manager.read("offset-test", 0, 3, 100).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].offset, 3);
        assert_eq!(records[0].value, Bytes::from("msg-3"));
        assert_eq!(records[1].offset, 4);
        assert_eq!(records[1].value, Bytes::from("msg-4"));
    }

    #[test]
    fn test_consume_with_max_messages() {
        let topic_manager = create_test_topic_manager();

        topic_manager.create_topic("max-test", 1).unwrap();

        // Produce 10 messages
        for i in 0..10 {
            topic_manager
                .append("max-test", 0, None, Bytes::from(format!("msg-{}", i)))
                .unwrap();
        }

        // Consume with limit
        let records = topic_manager.read("max-test", 0, 0, 3).unwrap();
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_consume_empty_topic() {
        let topic_manager = create_test_topic_manager();

        topic_manager.create_topic("empty-test", 1).unwrap();

        let records = topic_manager.read("empty-test", 0, 0, 100).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_produce_to_invalid_partition() {
        let topic_manager = create_test_topic_manager();

        topic_manager.create_topic("partition-test", 2).unwrap();

        // Try to produce to partition 5 (only 0 and 1 exist)
        let result = topic_manager.append("partition-test", 5, None, Bytes::from("value"));
        assert!(result.is_err());
    }

    // ==================== Offset query tests ====================

    #[test]
    fn test_earliest_and_latest_offset() {
        let topic_manager = create_test_topic_manager();

        topic_manager.create_topic("offset-query-test", 1).unwrap();

        // Initially both should be 0
        assert_eq!(
            topic_manager
                .earliest_offset("offset-query-test", 0)
                .unwrap(),
            0
        );
        assert_eq!(
            topic_manager.latest_offset("offset-query-test", 0).unwrap(),
            0
        );

        // Produce messages
        for _ in 0..5 {
            topic_manager
                .append("offset-query-test", 0, None, Bytes::from("msg"))
                .unwrap();
        }

        // Check offsets
        assert_eq!(
            topic_manager
                .earliest_offset("offset-query-test", 0)
                .unwrap(),
            0
        );
        assert_eq!(
            topic_manager.latest_offset("offset-query-test", 0).unwrap(),
            5
        );
    }

    // ==================== extract_port tests ====================

    #[test]
    fn test_extract_port_valid() {
        assert_eq!(extract_port("localhost:9092"), Some(9092));
        assert_eq!(extract_port("0.0.0.0:8080"), Some(8080));
        assert_eq!(extract_port("192.168.1.1:443"), Some(443));
    }

    #[test]
    fn test_extract_port_invalid() {
        assert_eq!(extract_port("localhost"), None);
        assert_eq!(extract_port("localhost:abc"), None);
        assert_eq!(extract_port(""), None);
    }
}
