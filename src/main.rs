//! Streamline - The Redis of Streaming
//!
//! A developer-first, operationally simple streaming solution.

use clap::Parser;
use std::process::ExitCode;
use std::sync::Arc;
use streamline::config::{merge_config_with_args, ConfigFile, RuntimeMode};
use streamline::runtime::ShardedRuntime;
use streamline::server::log_buffer::LogBuffer;
use streamline::server::log_layer::LogBufferLayer;
use streamline::{Result, Server, ServerArgs, ServerConfig, StreamlineError};
use tracing::{error, info, warn};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Demo topics to create in playground mode
const PLAYGROUND_TOPICS: &[(&str, i32)] = &[("demo", 1), ("events", 3), ("logs", 1)];

fn print_playground_banner(config: &ServerConfig) {
    println!();
    println!("  ____  _                            _ _            ");
    println!(" / ___|| |_ _ __ ___  __ _ _ __ ___ | (_)_ __   ___ ");
    println!(" \\___ \\| __| '__/ _ \\/ _` | '_ ` _ \\| | | '_ \\ / _ \\");
    println!("  ___) | |_| | |  __/ (_| | | | | | | | | | | |  __/");
    println!(" |____/ \\__|_|  \\___|\\__,_|_| |_| |_|_|_|_| |_|\\___|");
    println!();
    println!("  PLAYGROUND MODE - For development and exploration");
    println!();
    println!("  Endpoints:");
    println!("    Kafka API:   localhost:{}", config.listen_addr.port());
    println!(
        "    HTTP API:    http://localhost:{}",
        config.http_addr.port()
    );
    println!(
        "    Health:      http://localhost:{}/health",
        config.http_addr.port()
    );
    println!(
        "    Metrics:     http://localhost:{}/metrics",
        config.http_addr.port()
    );
    println!();
    println!("  Demo Topics:");
    for (topic, partitions) in PLAYGROUND_TOPICS {
        println!(
            "    - {} ({} partition{})",
            topic,
            partitions,
            if *partitions > 1 { "s" } else { "" }
        );
    }
    println!();
    println!("  Quick Start (Streamline CLI):");
    println!(
        "    streamline-cli --data-dir {} produce demo -m 'Hello, Streamline!'",
        config.data_dir.display()
    );
    println!(
        "    streamline-cli --data-dir {} consume demo --from-beginning",
        config.data_dir.display()
    );
    println!();
    println!("  Kafka Clients (zero code changes):");
    println!(
        "    kcat:     echo 'Hello!' | kcat -b localhost:{} -t demo -P",
        config.listen_addr.port()
    );
    println!("    Python:   See examples/external-clients/python/",);
    println!("    Node.js:  See examples/external-clients/nodejs/",);
    println!("    Go:       See examples/external-clients/go/",);
    println!();
    println!("  HTTP API:");
    println!(
        "    curl http://localhost:{}/health",
        config.http_addr.port()
    );
    println!(
        "    curl http://localhost:{}/api/v1/topics",
        config.http_addr.port()
    );
    println!();
    println!("  Note: Running in-memory mode. Data will not persist after restart.");
    println!();
}

fn main() -> ExitCode {
    if let Err(e) = run() {
        eprintln!("Streamline failed to start: {e}");
        return ExitCode::from(1);
    }
    ExitCode::SUCCESS
}

fn run() -> Result<()> {
    // Parse command-line arguments (before runtime creation)
    let mut args = ServerArgs::parse();

    // Handle --generate-config flag
    if args.generate_config {
        println!("{}", ConfigFile::generate_example());
        return Ok(());
    }

    // Load configuration file if specified or from default locations
    let config_file = if let Some(ref path) = args.config {
        match ConfigFile::load(path) {
            Ok(config) => {
                eprintln!("Loaded configuration from {:?}", path);
                Some(config)
            }
            Err(e) => {
                eprintln!("Error loading configuration file: {}", e);
                return Err(e);
            }
        }
    } else {
        ConfigFile::load_default()
    };

    // Merge config file values with CLI args (CLI takes precedence)
    if let Some(ref config) = config_file {
        args = merge_config_with_args(args, config);
    }

    let is_playground = args.playground;

    // In playground mode, enable in-memory mode by default
    if is_playground {
        if args.data_dir != std::path::Path::new("./data") && !args.in_memory {
            warn!(
                "Playground mode overrides --data-dir to use in-memory storage. \
                 Specified data directory '{}' will not be used for persistence.",
                args.data_dir.display()
            );
        }
        args.in_memory = true;
    }

    // Determine runtime mode before creating the runtime
    let runtime_mode_str = args.runtime_mode.clone();
    let runtime_mode = runtime_mode_str
        .parse::<RuntimeMode>()
        .map_err(|e| StreamlineError::Config(format!("Invalid runtime mode: {}", e)))?;

    // Create log buffer for real-time log viewing
    let log_buffer = LogBuffer::new_shared();

    // Initialize logging with log buffer layer
    let log_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));

    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(LogBufferLayer::new(log_buffer.clone()))
        .with(log_filter)
        .init();

    // Log config file usage
    if config_file.is_some() {
        info!("Configuration loaded from file");
    }

    // Create server configuration
    let config = match ServerConfig::from_args(args) {
        Ok(config) => config,
        Err(e) => {
            error!(error = %e, "Failed to create configuration");
            return Err(e);
        }
    };

    // Validate configuration before starting server
    if let Err(e) = config.validate() {
        error!(error = %e, "Invalid configuration");
        return Err(e);
    }

    // Run server with appropriate runtime
    match runtime_mode {
        RuntimeMode::Tokio => {
            info!(mode = "tokio", "Starting with Tokio multi-threaded runtime");
            run_with_tokio(config, log_buffer, is_playground)?;
        }
        RuntimeMode::Sharded => {
            info!(
                mode = "sharded",
                shard_count = config.runtime.effective_shard_count(),
                "Starting with thread-per-core sharded runtime"
            );
            run_with_sharded_runtime(config, log_buffer, is_playground)?;
        }
    }
    Ok(())
}

/// Run server with standard Tokio multi-threaded runtime
fn run_with_tokio(
    config: ServerConfig,
    log_buffer: Arc<LogBuffer>,
    is_playground: bool,
) -> Result<()> {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            error!(error = %e, "Failed to create Tokio runtime");
            return Err(StreamlineError::Server(format!(
                "Failed to create Tokio runtime: {}",
                e
            )));
        }
    };

    runtime.block_on(async { run_server(config, log_buffer, is_playground, None).await })
}

/// Run server with thread-per-core sharded runtime
fn run_with_sharded_runtime(
    config: ServerConfig,
    log_buffer: Arc<LogBuffer>,
    is_playground: bool,
) -> Result<()> {
    // Create sharded runtime from config
    let sharded_config = config.runtime.to_sharded_config();
    let sharded_runtime = match ShardedRuntime::new(sharded_config) {
        Ok(rt) => Arc::new(rt),
        Err(e) => {
            error!(error = %e, "Failed to create sharded runtime");
            return Err(StreamlineError::Server(format!(
                "Failed to create sharded runtime: {}",
                e
            )));
        }
    };

    // Start sharded runtime threads (enables CPU affinity)
    if let Err(e) = sharded_runtime.start() {
        error!(error = %e, "Failed to start sharded runtime");
        return Err(StreamlineError::Server(format!(
            "Failed to start sharded runtime: {}",
            e
        )));
    }

    info!(
        shard_count = sharded_runtime.shard_count(),
        "Sharded runtime started with CPU affinity"
    );

    // Create Tokio runtime for async I/O
    // In sharded mode, we still use Tokio but the ShardedRuntime
    // provides CPU affinity and partition routing
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            error!(error = %e, "Failed to create Tokio runtime");
            return Err(StreamlineError::Server(format!(
                "Failed to create Tokio runtime: {}",
                e
            )));
        }
    };

    let sharded_runtime_clone = sharded_runtime.clone();
    let result = runtime.block_on(async {
        run_server(
            config,
            log_buffer,
            is_playground,
            Some(sharded_runtime_clone),
        )
        .await
    });

    // Stop sharded runtime on exit
    if let Err(e) = sharded_runtime.stop() {
        error!(error = %e, "Failed to stop sharded runtime");
    }
    result
}

/// Common server initialization and run logic
async fn run_server(
    config: ServerConfig,
    log_buffer: Arc<LogBuffer>,
    is_playground: bool,
    sharded_runtime: Option<Arc<ShardedRuntime>>,
) -> Result<()> {
    // Print security warnings (skip in playground mode - it's expected to be insecure)
    if !is_playground {
        #[cfg(feature = "auth")]
        if !config.auth.enabled {
            warn!(
                "Authentication is DISABLED. Anyone can connect to this server. \
                 Enable with --auth-enabled for production use."
            );
        }
        #[cfg(not(feature = "auth"))]
        warn!(
            "Authentication is DISABLED (auth feature not compiled). \
             Build with --features auth for production use."
        );
        if !config.tls.enabled {
            warn!(
                "TLS is DISABLED. Connections are not encrypted. \
                 Enable with --tls-enabled for production use."
            );
        }
        #[cfg(feature = "auth")]
        if config.auth.enabled && config.auth.allow_anonymous {
            warn!(
                "Authentication is enabled but anonymous connections are allowed. \
                 This may pose a security risk. Disable with --auth-allow-anonymous=false."
            );
        }
        #[cfg(feature = "auth")]
        if config.acl.enabled && config.acl.allow_if_no_acls {
            warn!(
                "ACL authorization is enabled but operations are allowed when no ACLs match. \
                 Consider setting --acl-allow-if-no-acls=false for stricter security."
            );
        }
    }

    // Display telemetry notice if telemetry is enabled and this is first run
    if config.telemetry.enabled {
        // Check if telemetry ID file was just created (first-run detection)
        let telemetry_id_file = config.data_dir.join(".telemetry_id");
        let telemetry_notice_file = config.data_dir.join(".telemetry_notice_shown");

        // Show notice if we haven't shown it before for this installation
        if telemetry_id_file.exists() && !telemetry_notice_file.exists() {
            streamline::telemetry::display_telemetry_notice(&config.telemetry.installation_id);
            // Mark that we've shown the notice
            let _ = std::fs::write(&telemetry_notice_file, "1");
        }

        info!(
            installation_id = %config.telemetry.installation_id,
            interval_hours = config.telemetry.interval_hours,
            dry_run = config.telemetry.dry_run,
            "Anonymous telemetry enabled"
        );
    }

    // Print playground banner if in playground mode
    if is_playground {
        print_playground_banner(&config);
    }

    // Log sharded runtime info if enabled
    if let Some(ref rt) = sharded_runtime {
        info!(
            shard_count = rt.shard_count(),
            "Server using sharded runtime for partition routing"
        );
    }

    // Create and run server (pass sharded runtime for thread-per-core execution)
    let mut server = match Server::new(config, sharded_runtime).await {
        Ok(server) => server,
        Err(e) => {
            error!(error = %e, "Failed to create server");
            return Err(e);
        }
    };

    // Set up log buffer for real-time log viewing
    server.set_log_buffer(log_buffer);

    // In playground mode, create demo topics before starting
    if is_playground {
        for (topic_name, partitions) in PLAYGROUND_TOPICS {
            match server.topic_manager().create_topic(topic_name, *partitions) {
                Ok(_) => {
                    tracing::info!(topic = %topic_name, partitions = %partitions, "Created demo topic")
                }
                Err(e) => {
                    tracing::warn!(topic = %topic_name, error = %e, "Failed to create demo topic")
                }
            }
        }
    }

    if let Err(e) = server.run().await {
        error!(error = %e, "Server error");
        return Err(e);
    }
    Ok(())
}
