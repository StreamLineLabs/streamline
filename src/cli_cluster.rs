use colored::Colorize;
use serde_json::json;
use std::path::Path;
use streamline::{Result, StreamlineError};

use crate::cli_context::CliContext;
use crate::OutputFormat;

#[allow(clippy::too_many_arguments)]
pub(super) fn init_cluster(
    nodes: usize,
    kafka_port: u16,
    cluster_port: u16,
    http_port: u16,
    host: &str,
    output_dir: &Path,
    docker: bool,
    ctx: &CliContext,
) -> Result<()> {
    // Validate node count
    if !(1..=9).contains(&nodes) {
        return Err(StreamlineError::Config(
            "Node count must be between 1 and 9".into(),
        ));
    }

    if nodes % 2 == 0 {
        ctx.warn(&format!(
            "Even number of nodes ({}). Odd numbers (3, 5, 7) are recommended for consensus.",
            nodes
        ));
    }

    // Create output directory
    std::fs::create_dir_all(output_dir)?;

    let spinner = ctx.spinner(&format!(
        "Initializing {}-node cluster configuration...",
        nodes
    ));

    // Generate configuration for each node
    let mut seed_nodes = Vec::new();
    for i in 1..=nodes {
        let node_cluster_port = cluster_port + ((i - 1) * 10) as u16;
        seed_nodes.push(format!("{}:{}", host, node_cluster_port));
    }

    let mut created_files = Vec::new();

    for i in 1..=nodes {
        let node_id = i;
        let node_kafka_port = kafka_port + ((i - 1) * 10) as u16;
        let node_cluster_port = cluster_port + ((i - 1) * 10) as u16;
        let node_http_port = http_port + ((i - 1) * 10) as u16;

        // Generate environment file
        let env_content = generate_env_file(
            node_id,
            host,
            node_kafka_port,
            node_cluster_port,
            node_http_port,
            &seed_nodes,
            i == 1, // First node bootstraps
        );

        let env_path = output_dir.join(format!("node{}.env", node_id));
        std::fs::write(&env_path, &env_content)?;
        created_files.push(env_path.display().to_string());

        // Generate shell script
        let script_content = generate_start_script(node_id, &env_path);
        let script_path = output_dir.join(format!("start-node{}.sh", node_id));
        std::fs::write(&script_path, &script_content)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(&script_path)?.permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&script_path, perms)?;
        }
        created_files.push(script_path.display().to_string());
    }

    // Generate docker-compose.yml if requested
    if docker {
        let compose_content = generate_docker_compose(nodes, kafka_port, cluster_port, http_port);
        let compose_path = output_dir.join("docker-compose.yml");
        std::fs::write(&compose_path, &compose_content)?;
        created_files.push(compose_path.display().to_string());
    }

    // Generate README
    let readme_content =
        generate_cluster_readme(nodes, host, kafka_port, cluster_port, http_port, docker);
    let readme_path = output_dir.join("README.md");
    std::fs::write(&readme_path, &readme_content)?;
    created_files.push(readme_path.display().to_string());

    if let Some(s) = spinner {
        s.finish_and_clear();
    }

    match ctx.format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "status": "created",
                    "nodes": nodes,
                    "output_dir": output_dir.display().to_string(),
                    "files": created_files
                }))?
            );
        }
        _ => {
            ctx.success("Cluster configuration generated successfully!");
            println!();
            println!("{}", "Created files:".bold());
            for file in &created_files {
                println!("  {} {}", "→".dimmed(), file.cyan());
            }
            println!();
            println!("{}", "To start the cluster locally:".bold().underline());
            println!(
                "  1. Start node 1 first (bootstrap node):\n     {}",
                format!("{}/start-node1.sh", output_dir.display()).cyan()
            );
            println!("  2. Start remaining nodes:");
            for i in 2..=nodes {
                println!(
                    "     {}",
                    format!("{}/start-node{}.sh", output_dir.display(), i).cyan()
                );
            }

            if docker {
                println!();
                println!("{}", "Or use Docker Compose:".bold());
                println!(
                    "  {}",
                    format!(
                        "docker-compose -f {}/docker-compose.yml up -d",
                        output_dir.display()
                    )
                    .cyan()
                );
            }
        }
    }

    Ok(())
}

fn generate_env_file(
    node_id: usize,
    host: &str,
    kafka_port: u16,
    cluster_port: u16,
    http_port: u16,
    seed_nodes: &[String],
    is_bootstrap: bool,
) -> String {
    let mut env = format!(
        r#"# Streamline Node {} Configuration
# Generated by streamline-cli cluster init

# Node identity
STREAMLINE_NODE_ID={}

# Network addresses
STREAMLINE_LISTEN_ADDR=0.0.0.0:{}
STREAMLINE_ADVERTISED_ADDR={}:{}
STREAMLINE_INTER_BROKER_ADDR=0.0.0.0:{}
STREAMLINE_HTTP_ADDR=0.0.0.0:{}

# Data directory
STREAMLINE_DATA_DIR=./data/node{}

# Log level
STREAMLINE_LOG_LEVEL=info
"#,
        node_id, node_id, kafka_port, host, kafka_port, cluster_port, http_port, node_id
    );

    if is_bootstrap {
        env.push_str("\n# Bootstrap flag (only for first node)\nSTREAMLINE_BOOTSTRAP=true\n");
    } else {
        // Non-bootstrap nodes need seed nodes
        let seeds: Vec<&str> = seed_nodes
            .iter()
            .filter(|s| !s.ends_with(&format!(":{}", cluster_port)))
            .map(|s| s.as_str())
            .collect();
        if !seeds.is_empty() {
            env.push_str(&format!(
                "\n# Seed nodes to join\nSTREAMLINE_SEED_NODES={}\n",
                seeds.join(",")
            ));
        }
    }

    env
}

fn generate_start_script(node_id: usize, env_path: &Path) -> String {
    format!(
        r#"#!/bin/bash
# Start script for Streamline Node {}
# Generated by streamline-cli cluster init

set -e

# Load environment
set -a
source {}
set +a

# Create data directory
mkdir -p $STREAMLINE_DATA_DIR

echo "Starting Streamline Node {}..."
 echo "  Listen: $STREAMLINE_LISTEN_ADDR"
 echo "  Advertised: $STREAMLINE_ADVERTISED_ADDR"
 echo "  Inter-broker: $STREAMLINE_INTER_BROKER_ADDR"
 echo "  HTTP: $STREAMLINE_HTTP_ADDR"
 echo "  Data: $STREAMLINE_DATA_DIR"

# Build arguments
ARGS="--node-id $STREAMLINE_NODE_ID"
ARGS="$ARGS --listen-addr $STREAMLINE_LISTEN_ADDR"
ARGS="$ARGS --advertised-addr $STREAMLINE_ADVERTISED_ADDR"
ARGS="$ARGS --inter-broker-addr $STREAMLINE_INTER_BROKER_ADDR"
ARGS="$ARGS --http-addr $STREAMLINE_HTTP_ADDR"
ARGS="$ARGS --data-dir $STREAMLINE_DATA_DIR"
ARGS="$ARGS --log-level $STREAMLINE_LOG_LEVEL"

if [ "$STREAMLINE_BOOTSTRAP" = "true" ]; then
    ARGS="$ARGS --bootstrap"
fi

if [ -n "$STREAMLINE_SEED_NODES" ]; then
    ARGS="$ARGS --seed-nodes $STREAMLINE_SEED_NODES"
fi

exec streamline $ARGS
"#,
        node_id,
        env_path.display(),
        node_id
    )
}

fn generate_docker_compose(
    nodes: usize,
    kafka_port: u16,
    cluster_port: u16,
    http_port: u16,
) -> String {
    let mut compose = String::from(
        r#"# Streamline Cluster Docker Compose
# Generated by streamline-cli cluster init

version: '3.8'

services:
"#,
    );

    for i in 1..=nodes {
        let node_kafka_port = kafka_port + ((i - 1) * 10) as u16;
        let node_cluster_port = cluster_port + ((i - 1) * 10) as u16;
        let node_http_port = http_port + ((i - 1) * 10) as u16;

        let mut seed_nodes = Vec::new();
        for j in 1..=nodes {
            if j != i {
                let port = cluster_port + ((j - 1) * 10) as u16;
                seed_nodes.push(format!("streamline-node{}:{}", j, port));
            }
        }

        compose.push_str(&format!(
            r#"
  streamline-node{}:
    image: streamline:latest
    container_name: streamline-node{}
    hostname: streamline-node{}
    ports:
      - "{}:{}"
      - "{}:{}"
      - "{}:{}"
    environment:
      - STREAMLINE_NODE_ID={}
      - STREAMLINE_LISTEN_ADDR=0.0.0.0:{}
      - STREAMLINE_ADVERTISED_ADDR=streamline-node{}:{}
      - STREAMLINE_INTER_BROKER_ADDR=0.0.0.0:{}
      - STREAMLINE_HTTP_ADDR=0.0.0.0:{}
      - STREAMLINE_DATA_DIR=/data
      - STREAMLINE_LOG_LEVEL=info
"#,
            i,
            i,
            i,
            node_kafka_port,
            node_kafka_port,
            node_cluster_port,
            node_cluster_port,
            node_http_port,
            node_http_port,
            i,
            node_kafka_port,
            i,
            node_kafka_port,
            node_cluster_port,
            node_http_port,
        ));

        if i == 1 {
            compose.push_str("      - STREAMLINE_BOOTSTRAP=true\n");
        } else {
            compose.push_str(&format!(
                "      - STREAMLINE_SEED_NODES={}\n",
                seed_nodes.join(",")
            ));
        }

        compose.push_str(&format!(
            r#"    volumes:
      - streamline-data-{}:/data
    networks:
      - streamline-net
    restart: unless-stopped
"#,
            i
        ));

        if i > 1 {
            compose.push_str("    depends_on:\n      - streamline-node1\n");
        }
    }

    compose.push_str(
        r#"
networks:
  streamline-net:
    driver: bridge

volumes:
"#,
    );

    for i in 1..=nodes {
        compose.push_str(&format!("  streamline-data-{}:\n", i));
    }

    compose
}

fn generate_cluster_readme(
    nodes: usize,
    host: &str,
    kafka_port: u16,
    cluster_port: u16,
    http_port: u16,
    docker: bool,
) -> String {
    let mut readme = format!(
        r#"# Streamline Cluster Configuration

This directory contains configuration files for a {}-node Streamline cluster.

## Node Configuration

    | Node | Kafka Port | Inter-broker Port | HTTP Port |
|------|------------|--------------|-----------|
"#,
        nodes
    );

    for i in 1..=nodes {
        let node_kafka_port = kafka_port + ((i - 1) * 10) as u16;
        let node_cluster_port = cluster_port + ((i - 1) * 10) as u16;
        let node_http_port = http_port + ((i - 1) * 10) as u16;
        readme.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            i, node_kafka_port, node_cluster_port, node_http_port
        ));
    }

    readme.push_str(
        r#"
## Starting the Cluster Locally

1. **Start the bootstrap node first:**
   ```bash
   ./start-node1.sh
   ```

2. **Wait for node 1 to initialize (5-10 seconds), then start remaining nodes:**
"#,
    );

    for i in 2..=nodes {
        readme.push_str(&format!("   ```bash\n   ./start-node{}.sh\n   ```\n", i));
    }

    if docker {
        readme.push_str(
            r#"
## Starting with Docker Compose

```bash
docker-compose up -d
```

To view logs:
```bash
docker-compose logs -f
```

To stop:
```bash
docker-compose down
```
"#,
        );
    }

    readme.push_str(
        r#"
## Connecting Kafka Clients

Use any of the broker addresses:
"#,
    );

    for i in 1..=nodes {
        let node_kafka_port = kafka_port + ((i - 1) * 10) as u16;
        readme.push_str(&format!("- `{}:{}`\n", host, node_kafka_port));
    }

    readme.push_str(
        r#"
Example with kafka-console-producer:
```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
```

## Health Checks

Check cluster health:
"#,
    );

    for i in 1..=nodes {
        let node_http_port = http_port + ((i - 1) * 10) as u16;
        readme.push_str(&format!(
            "```bash\ncurl http://{}:{}/health\n```\n",
            host, node_http_port
        ));
    }

    readme
}

pub(super) fn validate_cluster(config_dir: &Path, ctx: &CliContext) -> Result<()> {
    if !config_dir.exists() {
        return Err(StreamlineError::Config(format!(
            "Configuration directory not found: {}",
            config_dir.display()
        )));
    }

    let spinner = ctx.spinner(&format!(
        "Validating cluster configuration in {}...",
        config_dir.display()
    ));

    let mut node_ids = std::collections::HashSet::new();
    let mut kafka_ports = std::collections::HashSet::new();
    let mut cluster_ports = std::collections::HashSet::new();
    let mut http_ports = std::collections::HashSet::new();
    let mut errors = Vec::new();
    let mut checked_files = Vec::new();

    // Find all .env files
    let entries: Vec<_> = std::fs::read_dir(config_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "env"))
        .collect();

    if entries.is_empty() {
        if let Some(s) = spinner {
            s.finish_and_clear();
        }
        return Err(StreamlineError::Config(format!(
            "No .env files found in {}",
            config_dir.display()
        )));
    }

    for entry in entries {
        let path = entry.path();
        let content = std::fs::read_to_string(&path)?;
        checked_files.push(path.display().to_string());

        // Parse key-value pairs
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((key, value)) = line.split_once('=') {
                match key {
                    "STREAMLINE_NODE_ID" => {
                        if let Ok(id) = value.parse::<usize>() {
                            if !node_ids.insert(id) {
                                errors.push(format!("Duplicate node ID: {}", id));
                            }
                        }
                    }
                    "STREAMLINE_LISTEN_ADDR" => {
                        if let Some(port) = extract_port(value) {
                            if !kafka_ports.insert(port) {
                                errors.push(format!("Duplicate listen port: {}", port));
                            }
                        }
                    }
                    "STREAMLINE_INTER_BROKER_ADDR" => {
                        if let Some(port) = extract_port(value) {
                            if !cluster_ports.insert(port) {
                                errors.push(format!("Duplicate inter-broker port: {}", port));
                            }
                        }
                    }
                    "STREAMLINE_HTTP_ADDR" => {
                        if let Some(port) = extract_port(value) {
                            if !http_ports.insert(port) {
                                errors.push(format!("Duplicate HTTP port: {}", port));
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    if let Some(s) = spinner {
        s.finish_and_clear();
    }

    match ctx.format {
        OutputFormat::Json => {
            let node_ids_vec: Vec<_> = node_ids.iter().collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "valid": errors.is_empty(),
                    "nodes": node_ids.len(),
                    "node_ids": node_ids_vec,
                    "files_checked": checked_files,
                    "errors": errors
                }))?
            );
            if !errors.is_empty() {
                return Err(StreamlineError::Config(format!(
                    "Cluster validation failed with {} error(s)",
                    errors.len()
                )));
            }
        }
        _ => {
            if errors.is_empty() {
                ctx.success("Validation successful!");
                println!(
                    "  {}: {}",
                    "Nodes".bold(),
                    node_ids.len().to_string().green()
                );
                println!(
                    "  {}: {:?}",
                    "Node IDs".bold(),
                    node_ids.iter().collect::<Vec<_>>()
                );
            } else {
                ctx.error(&format!(
                    "Validation failed with {} error(s):",
                    errors.len()
                ));
                for error in &errors {
                    println!("  {} {}", "•".red(), error);
                }
                return Err(StreamlineError::Config(format!(
                    "Cluster validation failed with {} error(s)",
                    errors.len()
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn extract_port(addr: &str) -> Option<u16> {
    addr.rsplit(':').next()?.parse().ok()
}
