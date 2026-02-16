# Add Integration Tests for Server and CLI Binaries

## Priority: Medium

## Summary

Add end-to-end integration tests that exercise the main server binary and CLI tool. Current tests focus on library functions but don't test the actual binaries users will run.

## Current State

- `tests/integration_test.rs` - Tests storage layer directly
- `tests/cluster_test.rs` - Tests cluster via library APIs
- `tests/auth_test.rs` - Tests auth via library APIs
- No tests that start the actual `streamline` binary
- No tests for `streamline-cli` commands
- No tests for configuration file loading

## Requirements

### Server Binary Tests

1. **Startup and Shutdown**
   - Server starts with default configuration
   - Server starts with custom port
   - Server starts with TLS enabled
   - Server shuts down cleanly on SIGTERM
   - Server rejects invalid configuration

2. **Kafka Protocol Compatibility**
   - Real Kafka client can connect
   - Produce/consume roundtrip works
   - Consumer groups work end-to-end

3. **HTTP Endpoints**
   - `/health` returns 200
   - `/health/ready` returns correct status
   - `/metrics` returns Prometheus format

### CLI Binary Tests

1. **Topic Management**
   ```bash
   streamline-cli topics create test-topic --partitions 3
   streamline-cli topics list
   streamline-cli topics describe test-topic
   streamline-cli topics delete test-topic
   ```

2. **Error Handling**
   - Helpful error when server not running
   - Helpful error for invalid arguments
   - Non-zero exit code on failure

### Test Framework

```rust
// tests/binary_integration/mod.rs

struct TestServer {
    process: Child,
    kafka_addr: SocketAddr,
    http_addr: SocketAddr,
    data_dir: TempDir,
}

impl TestServer {
    pub async fn start() -> Self { ... }
    pub async fn start_with_config(config: &str) -> Self { ... }
    pub async fn stop(&mut self) { ... }
}

struct TestCli {
    server_addr: SocketAddr,
}

impl TestCli {
    pub fn run(&self, args: &[&str]) -> Output { ... }
}
```

### Example Tests

```rust
#[tokio::test]
async fn test_server_starts_and_accepts_connections() {
    let server = TestServer::start().await;

    let client = KafkaClient::connect(&server.kafka_addr).await.unwrap();
    let metadata = client.metadata().await.unwrap();

    assert!(metadata.brokers.len() > 0);

    server.stop().await;
}

#[tokio::test]
async fn test_produce_consume_roundtrip() {
    let server = TestServer::start().await;

    // Create topic via CLI
    let cli = TestCli::new(&server.kafka_addr);
    cli.run(&["topics", "create", "test", "--partitions", "1"]);

    // Produce via Kafka client
    let producer = KafkaProducer::connect(&server.kafka_addr).await;
    producer.send("test", b"hello").await.unwrap();

    // Consume via Kafka client
    let consumer = KafkaConsumer::connect(&server.kafka_addr).await;
    let messages = consumer.fetch("test", 0, 0).await.unwrap();

    assert_eq!(messages[0].value, b"hello");
}

#[test]
fn test_cli_topic_lifecycle() {
    let server = TestServer::start().await;
    let cli = TestCli::new(&server.kafka_addr);

    // Create
    let output = cli.run(&["topics", "create", "my-topic", "--partitions", "2"]);
    assert!(output.status.success());

    // List
    let output = cli.run(&["topics", "list"]);
    assert!(output.stdout.contains("my-topic"));

    // Describe
    let output = cli.run(&["topics", "describe", "my-topic"]);
    assert!(output.stdout.contains("partitions: 2"));

    // Delete
    let output = cli.run(&["topics", "delete", "my-topic"]);
    assert!(output.status.success());
}
```

### Implementation Tasks

1. Create `tests/binary_integration/` directory
2. Implement `TestServer` helper
3. Implement `TestCli` helper
4. Write server startup/shutdown tests
5. Write Kafka protocol tests with real client
6. Write CLI command tests
7. Write HTTP endpoint tests
8. Add to CI pipeline

### Acceptance Criteria

- [ ] Server binary starts and stops correctly
- [ ] Real Kafka client can produce/consume
- [ ] CLI commands work correctly
- [ ] HTTP health endpoints tested
- [ ] Tests run in CI (may need longer timeout)
- [ ] Tests clean up temp directories

## Related Files

- `src/main.rs` - Server binary
- `src/cli.rs` - CLI binary
- `tests/` - Existing test files

## Labels

`medium-priority`, `testing`, `production-readiness`
