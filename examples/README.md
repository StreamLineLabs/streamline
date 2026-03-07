# Streamline Examples

Runnable examples demonstrating Streamline features and integrations.

## Prerequisites

```bash
# Start Streamline in playground mode (creates demo topics)
cargo run -- --playground
```

## Core Examples (Rust)

| Example | Description | Run |
|---------|-------------|-----|
| `basic_producer.rs` | Produce messages to a topic | `cargo run --example basic_producer` |
| `basic_consumer.rs` | Consume messages with offset tracking | `cargo run --example basic_consumer` |
| `topic_management.rs` | Create, list, describe, and delete topics | `cargo run --example topic_management` |
| `full_pipeline.rs` | End-to-end produce → consume pipeline | `cargo run --example full_pipeline` |
| `ai_stream_processing.rs` | AI/ML stream processing with embeddings | `cargo run --example ai_stream_processing` |
| `cdc_to_lakehouse.rs` | CDC pipeline to Iceberg/Delta Lake | `cargo run --example cdc_to_lakehouse` |
| `edge_sync.rs` | Edge deployment with sync to cloud | `cargo run --example edge_sync` |

## AI Examples

| Example | Description |
|---------|-------------|
| `ai/semantic-router/` | Route messages by semantic similarity |
| `ai/anomaly-alerting/` | Real-time anomaly detection on streams |
| `ai/rag-pipeline/` | RAG pipeline with streaming context |

Each AI example has its own README with setup instructions.

## Kafka Client Compatibility

The `external-clients/` directory demonstrates Streamline working with standard Kafka clients (zero code changes):

| Language | Path |
|----------|------|
| Python | `external-clients/python/` |
| Go | `external-clients/go/` |
| Node.js | `external-clients/nodejs/` |

See `external-clients/README.md` for setup instructions.

## Operations Scripts

| Script | Description |
|--------|-------------|
| `auth_setup.sh` | Configure SASL authentication and ACLs |
| `tls_setup.sh` | Generate TLS certificates and enable encryption |
| `cluster_setup.sh` | Set up a multi-node cluster |
| `backup_restore.sh` | Backup and restore topic data |

## Configuration Files

| File | Description |
|------|-------------|
| `streamline-dev.toml` | Development server configuration |
| `streamline.service` | systemd unit file for Linux |
| `grafana-dashboard.json` | Importable Grafana dashboard |
