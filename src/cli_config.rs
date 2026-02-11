use colored::Colorize;
use serde_json::json;
use std::path::PathBuf;

use streamline::cli_utils::profile_store::{
    get_default_profile, load_profiles, save_profiles, set_default_profile, ConnectionProfile,
};
use streamline::Result;

use super::{ClusterCommands, ConfigCommands, EncryptionCommands, OutputFormat, ProfileCommands};
use crate::cli_cluster;
use crate::cli_context::CliContext;

pub(super) fn handle_cluster_command(cmd: ClusterCommands, ctx: &CliContext) -> Result<()> {
    match cmd {
        ClusterCommands::Init {
            nodes,
            kafka_port,
            cluster_port,
            http_port,
            host,
            output_dir,
            docker,
        } => {
            cli_cluster::init_cluster(
                nodes,
                kafka_port,
                cluster_port,
                http_port,
                &host,
                &output_dir,
                docker,
                ctx,
            )?;
        }
        ClusterCommands::Validate { config_dir } => {
            cli_cluster::validate_cluster(&config_dir, ctx)?;
        }
    }
    Ok(())
}

pub(super) fn handle_encryption_command(cmd: EncryptionCommands, ctx: &CliContext) -> Result<()> {
    match cmd {
        EncryptionCommands::GenerateKey { output, format } => {
            generate_encryption_key(output, format, ctx)?;
        }
    }
    Ok(())
}

fn generate_encryption_key(
    output: Option<PathBuf>,
    key_format: String,
    ctx: &CliContext,
) -> Result<()> {
    use streamline::storage::encryption::{generate_key_hex, DataEncryptionKey, KEY_SIZE};

    let spinner = ctx.spinner("Generating encryption key...");
    let key = DataEncryptionKey::generate();

    if let Some(s) = &spinner {
        s.finish_and_clear();
    }

    match key_format.as_str() {
        "hex" => {
            let hex_key = generate_key_hex();
            if let Some(output_path) = output {
                std::fs::write(&output_path, format!("{}\n", hex_key)).map_err(|e| {
                    streamline::error::StreamlineError::storage_msg(format!(
                        "Failed to write key file: {}",
                        e
                    ))
                })?;

                match ctx.format {
                    OutputFormat::Json => {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&json!({
                                "status": "created",
                                "output_file": output_path.display().to_string(),
                                "format": "hex",
                                "key_length": 64
                            }))?
                        );
                    }
                    _ => {
                        ctx.success(&format!(
                            "Encryption key written to: {}",
                            output_path.display().to_string().cyan()
                        ));
                        println!("  {}: hex-encoded (64 characters)", "Format".dimmed());
                    }
                }
            } else {
                match ctx.format {
                    OutputFormat::Json => {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&json!({
                                "key": hex_key,
                                "format": "hex"
                            }))?
                        );
                    }
                    _ => {
                        println!("{}", hex_key);
                    }
                }
            }
        }
        "raw" => {
            if let Some(output_path) = output {
                std::fs::write(&output_path, key.key()).map_err(|e| {
                    streamline::error::StreamlineError::storage_msg(format!(
                        "Failed to write key file: {}",
                        e
                    ))
                })?;

                match ctx.format {
                    OutputFormat::Json => {
                        println!(
                            "{}",
                            serde_json::to_string_pretty(&json!({
                                "status": "created",
                                "output_file": output_path.display().to_string(),
                                "format": "raw",
                                "key_size_bytes": KEY_SIZE
                            }))?
                        );
                    }
                    _ => {
                        ctx.success(&format!(
                            "Encryption key written to: {}",
                            output_path.display().to_string().cyan()
                        ));
                        println!("  {}: raw binary ({} bytes)", "Format".dimmed(), KEY_SIZE);
                    }
                }
            } else {
                return Err(streamline::error::StreamlineError::Config(
                    "Raw format requires --output file path (cannot print binary to terminal)"
                        .to_string(),
                ));
            }
        }
        _ => {
            return Err(streamline::error::StreamlineError::Config(format!(
                "Invalid format '{}'. Use 'hex' or 'raw'",
                key_format
            )));
        }
    }

    if matches!(ctx.format, OutputFormat::Text) {
        println!();
        println!("{}", "Usage:".bold().underline());
        println!(
            "  {} streamline --encryption-enabled --encryption-key-file <path>",
            "File-based:".dimmed()
        );
        println!(
            "  {} STREAMLINE_ENCRYPTION_KEY=<hex-key> streamline --encryption-enabled --encryption-key-env-var STREAMLINE_ENCRYPTION_KEY",
            "Environment:".dimmed()
        );
        println!();
        ctx.warn(
            "Keep this key secure! Data encrypted with this key cannot be decrypted without it.",
        );
    }

    Ok(())
}

pub(super) fn handle_config_command(cmd: ConfigCommands, ctx: &CliContext) -> Result<()> {
    match cmd {
        ConfigCommands::Validate { config, verbose } => {
            use std::io::Read;

            if !config.exists() {
                return Err(streamline::StreamlineError::Config(format!(
                    "Configuration file not found: {}",
                    config.display()
                )));
            }

            let spinner = ctx.spinner(&format!("Validating '{}'...", config.display()));

            let mut file = std::fs::File::open(&config)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;

            // Parse TOML
            let parsed = toml::from_str::<toml::Value>(&contents);

            if let Some(s) = &spinner {
                s.finish_and_clear();
            }

            match parsed {
                Ok(value) => {
                    let mut warnings: Vec<String> = Vec::new();
                    let mut errors: Vec<String> = Vec::new();

                    // Validate known sections
                    if let Some(table) = value.as_table() {
                        let known_sections = [
                            "server",
                            "storage",
                            "auth",
                            "tls",
                            "clustering",
                            "telemetry",
                            "metrics",
                            "replication",
                            "logging",
                        ];

                        for key in table.keys() {
                            if !known_sections.contains(&key.as_str()) {
                                warnings.push(format!("Unknown section: [{}]", key));
                            }
                        }

                        // Validate server section
                        if let Some(server) = table.get("server").and_then(|v| v.as_table()) {
                            if let Some(port) = server.get("port").and_then(|v| v.as_integer()) {
                                if !(1..=65535).contains(&port) {
                                    errors
                                        .push(format!("server.port must be 1-65535, got {}", port));
                                }
                            }
                        }

                        // Validate storage section
                        if let Some(storage) = table.get("storage").and_then(|v| v.as_table()) {
                            if let Some(retention) =
                                storage.get("retention_ms").and_then(|v| v.as_integer())
                            {
                                if retention < -1 {
                                    errors.push(format!(
                                        "storage.retention_ms must be >= -1, got {}",
                                        retention
                                    ));
                                }
                            }
                            if let Some(segment_bytes) =
                                storage.get("segment_bytes").and_then(|v| v.as_integer())
                            {
                                if segment_bytes < 1024 {
                                    warnings.push(format!("storage.segment_bytes is very small ({}), recommended >= 1MB", segment_bytes));
                                }
                            }
                        }
                    }

                    let is_valid = errors.is_empty();

                    match ctx.format {
                        OutputFormat::Json => {
                            println!(
                                "{}",
                                serde_json::to_string_pretty(&json!({
                                    "valid": is_valid,
                                    "file": config.display().to_string(),
                                    "errors": errors,
                                    "warnings": warnings
                                }))?
                            );
                        }
                        _ => {
                            if is_valid {
                                ctx.success(&format!(
                                    "Configuration file '{}' is valid",
                                    config.display()
                                ));
                            } else {
                                ctx.error(&format!(
                                    "Configuration file '{}' has errors",
                                    config.display()
                                ));
                            }

                            if !errors.is_empty() {
                                println!();
                                println!("{}", "Errors:".red().bold());
                                for err in &errors {
                                    println!("  {} {}", "✗".red(), err);
                                }
                            }

                            if verbose && !warnings.is_empty() {
                                println!();
                                println!("{}", "Warnings:".yellow().bold());
                                for warn in &warnings {
                                    println!("  {} {}", "!".yellow(), warn);
                                }
                            }
                        }
                    }

                    if !is_valid {
                        return Err(streamline::StreamlineError::Config(format!(
                            "Configuration validation failed for {}",
                            config.display()
                        )));
                    }
                }
                Err(e) => {
                    match ctx.format {
                        OutputFormat::Json => {
                            println!(
                                "{}",
                                serde_json::to_string_pretty(&json!({
                                    "valid": false,
                                    "file": config.display().to_string(),
                                    "error": e.to_string()
                                }))?
                            );
                        }
                        _ => {
                            ctx.error(&format!("Failed to parse '{}': {}", config.display(), e));
                        }
                    }
                    return Err(streamline::StreamlineError::Config(format!(
                        "Failed to parse '{}': {}",
                        config.display(),
                        e
                    )));
                }
            }
        }

        ConfigCommands::Show { config, format } => {
            let config_content = if let Some(path) = config {
                if !path.exists() {
                    return Err(streamline::StreamlineError::Config(format!(
                        "Configuration file not found: {}",
                        path.display()
                    )));
                }
                std::fs::read_to_string(&path)?
            } else {
                // Generate default config
                super::generate_default_config(false, "lite")
            };

            match format.as_str() {
                "toml" => println!("{}", config_content),
                "json" => {
                    if let Ok(parsed) = toml::from_str::<toml::Value>(&config_content) {
                        println!("{}", serde_json::to_string_pretty(&parsed)?);
                    } else {
                        println!("{}", config_content);
                    }
                }
                _ => {
                    return Err(streamline::StreamlineError::Config(format!(
                        "Unknown format '{}'. Use 'toml' or 'json'",
                        format
                    )));
                }
            }
        }

        ConfigCommands::Generate {
            output,
            full,
            edition,
        } => {
            let config = super::generate_default_config(full, &edition);

            if let Some(path) = output {
                std::fs::write(&path, &config)?;
                ctx.success(&format!("Generated configuration file: {}", path.display()));
            } else {
                println!("{}", config);
            }
        }

        ConfigCommands::Env { set_only } => {
            let env_vars = [
                (
                    "STREAMLINE_LISTEN_ADDR",
                    "Server listen address",
                    "0.0.0.0:9092",
                ),
                (
                    "STREAMLINE_HTTP_ADDR",
                    "HTTP/metrics address",
                    "0.0.0.0:9094",
                ),
                ("STREAMLINE_DATA_DIR", "Data directory", "./data"),
                ("STREAMLINE_LOG_LEVEL", "Log level", "info"),
                ("STREAMLINE_IN_MEMORY", "In-memory mode", "false"),
                ("STREAMLINE_CONFIG", "Configuration file path", ""),
                ("STREAMLINE_AUTH_ENABLED", "Enable authentication", "false"),
                ("STREAMLINE_TLS_ENABLED", "Enable TLS", "false"),
                (
                    "STREAMLINE_ENCRYPTION_ENABLED",
                    "Enable encryption at rest",
                    "false",
                ),
            ];

            match ctx.format {
                OutputFormat::Json => {
                    let vars: Vec<serde_json::Value> = env_vars
                        .iter()
                        .filter(|(name, _, _)| {
                            if set_only {
                                std::env::var(name).is_ok()
                            } else {
                                true
                            }
                        })
                        .map(|(name, desc, default)| {
                            let current = std::env::var(name).ok();
                            json!({
                                "name": name,
                                "description": desc,
                                "default": default,
                                "current": current,
                                "is_set": current.is_some()
                            })
                        })
                        .collect();
                    println!("{}", serde_json::to_string_pretty(&vars)?);
                }
                _ => {
                    println!("{}", "Environment Variables:".bold().underline());
                    println!();

                    for (name, desc, default) in env_vars {
                        let current = std::env::var(name).ok();

                        if set_only && current.is_none() {
                            continue;
                        }

                        let status = if current.is_some() {
                            "✓".green()
                        } else {
                            "○".dimmed()
                        };

                        println!("  {} {}", status, name.cyan());
                        println!("    {}", desc.dimmed());
                        if let Some(val) = &current {
                            println!("    Current: {}", val.green());
                        } else if !default.is_empty() {
                            println!("    Default: {}", default.dimmed());
                        }
                        println!();
                    }
                }
            }
        }
    }
    Ok(())
}

pub(super) fn handle_profile_command(cmd: ProfileCommands, ctx: &CliContext) -> Result<()> {
    match cmd {
        ProfileCommands::List => {
            let profiles = load_profiles()?;
            let default_profile = get_default_profile();

            match ctx.format {
                OutputFormat::Json => {
                    let mut entries: Vec<serde_json::Value> = profiles
                        .iter()
                        .map(|(name, profile)| {
                            json!({
                                "name": name,
                                "data_dir": profile.data_dir,
                                "server_addr": profile.server_addr,
                                "http_addr": profile.http_addr,
                                "description": profile.description,
                                "is_default": default_profile.as_ref() == Some(name)
                            })
                        })
                        .collect();
                    entries.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
                    println!("{}", serde_json::to_string_pretty(&entries)?);
                }
                _ => {
                    if profiles.is_empty() {
                        println!("{}", "No profiles configured.".dimmed());
                        println!();
                        println!("Create one with:");
                        println!("  {} profile add <name> --data-dir ./data --server-addr localhost:9092", "streamline-cli".cyan());
                        return Ok(());
                    }

                    println!("{}", "Connection Profiles:".bold().underline());
                    println!();

                    let mut names: Vec<_> = profiles.keys().collect();
                    names.sort();

                    for name in names {
                        let profile = &profiles[name];
                        let is_default = default_profile.as_ref() == Some(name);

                        let marker = if is_default {
                            "★".yellow()
                        } else {
                            "○".dimmed()
                        };
                        let name_display = if is_default {
                            format!("{} (default)", name).yellow().bold().to_string()
                        } else {
                            name.cyan().to_string()
                        };

                        println!("  {} {}", marker, name_display);

                        if let Some(desc) = &profile.description {
                            println!("    {}", desc.dimmed());
                        }
                        if let Some(data_dir) = &profile.data_dir {
                            println!("    Data dir:    {}", data_dir);
                        }
                        if let Some(server) = &profile.server_addr {
                            println!("    Server:      {}", server);
                        }
                        if let Some(http) = &profile.http_addr {
                            println!("    HTTP:        {}", http);
                        }
                        println!();
                    }
                }
            }
        }

        ProfileCommands::Show { name } => {
            let profiles = load_profiles()?;
            let default_profile = get_default_profile();

            let profile = profiles.get(&name).ok_or_else(|| {
                streamline::StreamlineError::Config(format!("Profile '{}' not found", name))
            })?;

            match ctx.format {
                OutputFormat::Json => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(&json!({
                            "name": name,
                            "data_dir": profile.data_dir,
                            "server_addr": profile.server_addr,
                            "http_addr": profile.http_addr,
                            "description": profile.description,
                            "is_default": default_profile.as_ref() == Some(&name)
                        }))?
                    );
                }
                _ => {
                    let is_default = default_profile.as_ref() == Some(&name);

                    println!("{}", format!("Profile: {}", name).bold().cyan());
                    if is_default {
                        println!("  {}", "(default profile)".yellow());
                    }
                    println!();

                    if let Some(desc) = &profile.description {
                        println!("  Description:   {}", desc);
                    }
                    if let Some(data_dir) = &profile.data_dir {
                        println!("  Data dir:      {}", data_dir);
                    }
                    if let Some(server) = &profile.server_addr {
                        println!("  Server addr:   {}", server);
                    }
                    if let Some(http) = &profile.http_addr {
                        println!("  HTTP addr:     {}", http);
                    }
                    println!();

                    println!("{}", "Usage:".dimmed());
                    println!(
                        "  {} --profile {} topics list",
                        "streamline-cli".cyan(),
                        name.cyan()
                    );
                }
            }
        }

        ProfileCommands::Init {
            name,
            data_dir,
            server_addr,
            http_addr,
            description,
            set_default,
        } => {
            let mut profiles = load_profiles()?;
            let is_update = profiles.contains_key(&name);

            let profile = ConnectionProfile {
                name: Some(name.clone()),
                data_dir: Some(data_dir),
                server_addr: Some(server_addr),
                http_addr: Some(http_addr),
                description,
            };

            profiles.insert(name.clone(), profile);
            save_profiles(&profiles)?;

            if is_update {
                ctx.success(&format!("Updated profile '{}'", name));
            } else {
                ctx.success(&format!("Created profile '{}'", name));
            }

            if set_default || get_default_profile().is_none() {
                set_default_profile(&name)?;
                println!("  {}", "(set as default profile)".dimmed());
            }

            println!();
            println!("Usage:");
            println!(
                "  {} --profile {} topics list",
                "streamline-cli".cyan(),
                name.cyan()
            );
        }

        ProfileCommands::Add {
            name,
            data_dir,
            server_addr,
            http_addr,
            description,
        } => {
            let mut profiles = load_profiles()?;
            let is_update = profiles.contains_key(&name);

            let profile = ConnectionProfile {
                name: Some(name.clone()),
                data_dir,
                server_addr,
                http_addr,
                description,
            };

            profiles.insert(name.clone(), profile);
            save_profiles(&profiles)?;

            if is_update {
                ctx.success(&format!("Updated profile '{}'", name));
            } else {
                ctx.success(&format!("Created profile '{}'", name));

                // Set as default if it's the first profile
                if profiles.len() == 1 {
                    set_default_profile(&name)?;
                    println!("  {}", "(set as default profile)".dimmed());
                }
            }

            println!();
            println!("Usage:");
            println!(
                "  {} --profile {} topics list",
                "streamline-cli".cyan(),
                name.cyan()
            );
        }

        ProfileCommands::Remove { name } => {
            let mut profiles = load_profiles()?;

            if !profiles.contains_key(&name) {
                return Err(streamline::StreamlineError::Config(format!(
                    "Profile '{}' not found",
                    name
                )));
            }

            profiles.remove(&name);
            save_profiles(&profiles)?;

            // Clear default if removed profile was default
            if get_default_profile().as_ref() == Some(&name) {
                let default_path = dirs::home_dir()
                    .map(|h| h.join(".streamline").join("default_profile"))
                    .unwrap_or_else(|| PathBuf::from(".streamline/default_profile"));
                let _ = std::fs::remove_file(&default_path);
            }

            ctx.success(&format!("Removed profile '{}'", name));
        }

        ProfileCommands::Default { name } => {
            let profiles = load_profiles()?;

            if !profiles.contains_key(&name) {
                return Err(streamline::StreamlineError::Config(format!(
                    "Profile '{}' not found",
                    name
                )));
            }

            set_default_profile(&name)?;
            ctx.success(&format!("Set '{}' as default profile", name));
        }
    }

    Ok(())
}
