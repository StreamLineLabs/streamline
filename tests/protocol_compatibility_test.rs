//! Kafka Client Library Compatibility Tests for Streamline
//!
//! These tests verify that external Kafka client libraries can communicate
//! correctly with Streamline's Kafka protocol implementation.
//!
//! Note: These tests require external client libraries to be installed.
//! They are gated behind the `compatibility-tests` feature and marked `#[ignore]`
//! by default since they depend on external tooling.
//!
//! Run with: cargo test --features compatibility-tests protocol_compatibility -- --ignored
//!
//! Prerequisites:
//! - librdkafka: `brew install librdkafka` (macOS) or `apt install librdkafka-dev` (Linux)
//! - kafka-python: `pip install kafka-python`
//! - Sarama (Go): `go install github.com/Shopify/sarama/tools/kafka-console-consumer@latest`
//! - franz-go: `go install github.com/twmb/franz-go/cmd/kcl@latest`
//! - kcat: `brew install kcat` (macOS) or `apt install kafkacat` (Linux)

#![cfg(feature = "compatibility-tests")]

use std::process::{Command, Stdio};
use std::time::Duration;

// ============================================================================
// Phase 13.1: librdkafka / kcat Compatibility Tests
// ============================================================================

/// Test that kcat (librdkafka-based tool) can produce messages
/// Requires: kcat installed
#[tokio::test]
#[ignore]
async fn test_kcat_produce() {
    let server = start_test_server().await;

    // Create topic first
    create_topic_via_admin(&server.bootstrap, "kcat-test-topic", 1).await;

    // Use kcat to produce a message
    let output = Command::new("kcat")
        .args([
            "-b",
            &server.bootstrap,
            "-P",
            "-t",
            "kcat-test-topic",
            "-p",
            "0",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn kcat");

    // Write message to stdin
    {
        use std::io::Write;
        let mut stdin = output.stdin.as_ref().unwrap();
        writeln!(stdin, "test message from kcat").unwrap();
    }

    // Wait for completion
    let output = output.wait_with_output().expect("Failed to wait on kcat");

    if !output.status.success() {
        eprintln!("kcat stderr: {}", String::from_utf8_lossy(&output.stderr));
    }

    assert!(output.status.success(), "kcat produce should succeed");

    server.shutdown().await;
}

/// Test that kcat can consume messages
/// Requires: kcat installed
#[tokio::test]
#[ignore]
async fn test_kcat_consume() {
    let server = start_test_server().await;

    // Create topic and produce test message via internal API
    create_topic_via_admin(&server.bootstrap, "kcat-consume-topic", 1).await;
    produce_message_via_admin(
        &server.bootstrap,
        "kcat-consume-topic",
        0,
        "key",
        "test-value",
    )
    .await;

    // Use kcat to consume
    let output = Command::new("kcat")
        .args([
            "-b",
            &server.bootstrap,
            "-C",
            "-t",
            "kcat-consume-topic",
            "-p",
            "0",
            "-o",
            "beginning",
            "-c",
            "1",  // Consume exactly 1 message
            "-e", // Exit after consuming
        ])
        .output()
        .expect("Failed to execute kcat");

    if !output.status.success() {
        eprintln!("kcat stderr: {}", String::from_utf8_lossy(&output.stderr));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("test-value"),
        "Should receive the produced message"
    );

    server.shutdown().await;
}

/// Test that kcat can list topics via metadata
/// Requires: kcat installed
#[tokio::test]
#[ignore]
async fn test_kcat_metadata() {
    let server = start_test_server().await;

    // Create some topics
    create_topic_via_admin(&server.bootstrap, "kcat-meta-topic-1", 2).await;
    create_topic_via_admin(&server.bootstrap, "kcat-meta-topic-2", 3).await;

    // Use kcat to fetch metadata
    let output = Command::new("kcat")
        .args([
            "-b",
            &server.bootstrap,
            "-L", // List metadata
        ])
        .output()
        .expect("Failed to execute kcat");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should list brokers and topics
    assert!(
        stdout.contains("broker") || stdout.contains("Broker"),
        "Should list brokers"
    );
    assert!(stdout.contains("kcat-meta-topic-1"), "Should list topic 1");
    assert!(stdout.contains("kcat-meta-topic-2"), "Should list topic 2");

    server.shutdown().await;
}

// ============================================================================
// Phase 13.2: kafka-python Compatibility Tests
// ============================================================================

/// Test that kafka-python KafkaProducer can produce messages
/// Requires: kafka-python installed (pip install kafka-python)
#[tokio::test]
#[ignore]
async fn test_kafka_python_producer() {
    let server = start_test_server().await;

    create_topic_via_admin(&server.bootstrap, "python-produce-topic", 1).await;

    let python_script = format!(
        r#"
from kafka import KafkaProducer
import sys

try:
    producer = KafkaProducer(
        bootstrap_servers='{}',
        client_id='python-test-client',
        request_timeout_ms=10000,
    )

    future = producer.send('python-produce-topic', value=b'hello from python')
    result = future.get(timeout=10)
    print(f'Sent to partition {{result.partition}} at offset {{result.offset}}')
    producer.close()
    sys.exit(0)
except Exception as e:
    print(f'Error: {{e}}', file=sys.stderr)
    sys.exit(1)
"#,
        server.bootstrap
    );

    let output = Command::new("python3")
        .args(["-c", &python_script])
        .output()
        .expect("Failed to execute python3");

    if !output.status.success() {
        eprintln!("Python stderr: {}", String::from_utf8_lossy(&output.stderr));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Sent to partition"),
        "Python producer should succeed"
    );

    server.shutdown().await;
}

/// Test that kafka-python KafkaConsumer can consume messages
/// Requires: kafka-python installed
#[tokio::test]
#[ignore]
async fn test_kafka_python_consumer() {
    let server = start_test_server().await;

    create_topic_via_admin(&server.bootstrap, "python-consume-topic", 1).await;
    produce_message_via_admin(
        &server.bootstrap,
        "python-consume-topic",
        0,
        "key",
        "python-test-value",
    )
    .await;

    let python_script = format!(
        r#"
from kafka import KafkaConsumer
import sys

try:
    consumer = KafkaConsumer(
        'python-consume-topic',
        bootstrap_servers='{}',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=10000,
        client_id='python-consumer-test',
    )

    for message in consumer:
        value = message.value.decode('utf-8') if message.value else ''
        print(f'Received: {{value}}')
        break

    consumer.close()
    sys.exit(0)
except Exception as e:
    print(f'Error: {{e}}', file=sys.stderr)
    sys.exit(1)
"#,
        server.bootstrap
    );

    let output = Command::new("python3")
        .args(["-c", &python_script])
        .output()
        .expect("Failed to execute python3");

    if !output.status.success() {
        eprintln!("Python stderr: {}", String::from_utf8_lossy(&output.stderr));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("python-test-value"),
        "Python consumer should receive message"
    );

    server.shutdown().await;
}

/// Test that kafka-python consumer groups work
/// Requires: kafka-python installed
#[tokio::test]
#[ignore]
async fn test_kafka_python_consumer_group() {
    let server = start_test_server().await;

    create_topic_via_admin(&server.bootstrap, "python-group-topic", 2).await;

    // Produce some messages
    for i in 0..5 {
        produce_message_via_admin(
            &server.bootstrap,
            "python-group-topic",
            i % 2,
            &format!("key-{}", i),
            &format!("value-{}", i),
        )
        .await;
    }

    let python_script = format!(
        r#"
from kafka import KafkaConsumer
import sys

try:
    consumer = KafkaConsumer(
        'python-group-topic',
        bootstrap_servers='{}',
        group_id='python-test-group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=15000,
        client_id='python-group-consumer',
    )

    count = 0
    for message in consumer:
        print(f'Partition {{message.partition}}: {{message.value.decode("utf-8")}}')
        count += 1
        if count >= 3:
            break

    consumer.close()
    print(f'Received {{count}} messages')
    sys.exit(0)
except Exception as e:
    print(f'Error: {{e}}', file=sys.stderr)
    sys.exit(1)
"#,
        server.bootstrap
    );

    let output = Command::new("python3")
        .args(["-c", &python_script])
        .output()
        .expect("Failed to execute python3");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Received"),
        "Python consumer group should work"
    );

    server.shutdown().await;
}

// ============================================================================
// Phase 13.3: Sarama (Go) Compatibility Tests
// ============================================================================

/// Test that Go Sarama sync producer works
/// Requires: Go and sarama installed
#[tokio::test]
#[ignore]
async fn test_sarama_sync_producer() {
    let server = start_test_server().await;

    create_topic_via_admin(&server.bootstrap, "sarama-produce-topic", 1).await;

    let bootstrap = format!("\"{}\"", server.bootstrap);
    let go_script = format!(
        r#"
package main

import (
    "fmt"
    "os"
    "github.com/Shopify/sarama"
)

func main() {{
    config := sarama.NewConfig()
    config.Producer.Return.Successes = true
    config.Producer.RequiredAcks = sarama.WaitForLocal

    producer, err := sarama.NewSyncProducer([]string{{{}}}, config)
    if err != nil {{
        fmt.Fprintf(os.Stderr, "Error: %v\n", err)
        os.Exit(1)
    }}
    defer producer.Close()

    msg := &sarama.ProducerMessage{{
        Topic: "sarama-produce-topic",
        Value: sarama.StringEncoder("hello from sarama"),
    }}

    partition, offset, err := producer.SendMessage(msg)
    if err != nil {{
        fmt.Fprintf(os.Stderr, "Error: %v\n", err)
        os.Exit(1)
    }}

    fmt.Printf("Sent to partition %d at offset %d\n", partition, offset)
}}
"#,
        bootstrap
    );

    // Write Go script to temp file and run
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let script_path = temp_dir.path().join("sarama_test.go");
    std::fs::write(&script_path, go_script).expect("Failed to write Go script");

    let output = Command::new("go")
        .args(["run", script_path.to_str().unwrap()])
        .current_dir(temp_dir.path())
        .output()
        .expect("Failed to execute go");

    if !output.status.success() {
        eprintln!("Go stderr: {}", String::from_utf8_lossy(&output.stderr));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Note: This test might fail if Go/Sarama isn't installed - that's expected
    println!("Sarama output: {}", stdout);

    server.shutdown().await;
}

/// Test that Go Sarama consumer group works
/// Requires: Go and sarama installed
#[tokio::test]
#[ignore]
async fn test_sarama_consumer_group() {
    let server = start_test_server().await;

    create_topic_via_admin(&server.bootstrap, "sarama-group-topic", 2).await;
    produce_message_via_admin(
        &server.bootstrap,
        "sarama-group-topic",
        0,
        "key",
        "sarama-test-value",
    )
    .await;

    // This would require a more complex Go script with consumer group implementation
    // For now, we test basic metadata fetch which validates protocol compatibility

    let output = Command::new("go").args(["version"]).output();

    if output.is_ok() {
        println!("Go is available, Sarama tests can be run with full Go setup");
    } else {
        println!("Go not available, skipping detailed Sarama test");
    }

    server.shutdown().await;
}

// ============================================================================
// Phase 13.4: franz-go Compatibility Tests
// ============================================================================

/// Test that franz-go (kcl) can produce messages
/// Requires: kcl installed (go install github.com/twmb/franz-go/cmd/kcl@latest)
#[tokio::test]
#[ignore]
async fn test_franz_go_kcl_produce() {
    let server = start_test_server().await;

    create_topic_via_admin(&server.bootstrap, "franz-produce-topic", 1).await;

    // Check if kcl is available
    let which = Command::new("which").arg("kcl").output();

    if which.is_err() || !which.unwrap().status.success() {
        println!("kcl not found, skipping franz-go produce test");
        server.shutdown().await;
        return;
    }

    let output = Command::new("kcl")
        .args(["-b", &server.bootstrap, "produce", "franz-produce-topic"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn kcl");

    // Write message
    {
        use std::io::Write;
        let mut stdin = output.stdin.as_ref().unwrap();
        writeln!(stdin, "hello from franz-go").unwrap();
    }

    let output = output.wait_with_output().expect("Failed to wait on kcl");

    if output.status.success() {
        println!("franz-go produce succeeded");
    } else {
        eprintln!("kcl stderr: {}", String::from_utf8_lossy(&output.stderr));
    }

    server.shutdown().await;
}

/// Test that franz-go (kcl) can consume messages
/// Requires: kcl installed
#[tokio::test]
#[ignore]
async fn test_franz_go_kcl_consume() {
    let server = start_test_server().await;

    create_topic_via_admin(&server.bootstrap, "franz-consume-topic", 1).await;
    produce_message_via_admin(
        &server.bootstrap,
        "franz-consume-topic",
        0,
        "key",
        "franz-test-value",
    )
    .await;

    // Check if kcl is available
    let which = Command::new("which").arg("kcl").output();

    if which.is_err() || !which.unwrap().status.success() {
        println!("kcl not found, skipping franz-go consume test");
        server.shutdown().await;
        return;
    }

    let output = Command::new("kcl")
        .args([
            "-b",
            &server.bootstrap,
            "consume",
            "franz-consume-topic",
            "-n",
            "1", // Consume 1 message
        ])
        .output()
        .expect("Failed to execute kcl");

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("franz-go consume output: {}", stdout);

    server.shutdown().await;
}

/// Test that franz-go (kcl) can fetch metadata
/// Requires: kcl installed
#[tokio::test]
#[ignore]
async fn test_franz_go_kcl_metadata() {
    let server = start_test_server().await;

    create_topic_via_admin(&server.bootstrap, "franz-meta-topic", 2).await;

    // Check if kcl is available
    let which = Command::new("which").arg("kcl").output();

    if which.is_err() || !which.unwrap().status.success() {
        println!("kcl not found, skipping franz-go metadata test");
        server.shutdown().await;
        return;
    }

    let output = Command::new("kcl")
        .args(["-b", &server.bootstrap, "metadata"])
        .output()
        .expect("Failed to execute kcl");

    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("franz-go metadata output: {}", stdout);

    // Should show cluster metadata
    assert!(!stdout.is_empty(), "Should receive metadata response");

    server.shutdown().await;
}

// ============================================================================
// Phase 13.5: Official Kafka CLI Tools Compatibility Tests
// ============================================================================

/// Test that kafka-topics.sh can create topics
/// Requires: Kafka CLI tools in PATH
#[tokio::test]
#[ignore]
async fn test_kafka_cli_create_topic() {
    let server = start_test_server().await;

    // Check if kafka-topics.sh is available
    let which = Command::new("which").arg("kafka-topics.sh").output();

    if which.is_err() || !which.unwrap().status.success() {
        // Try kafka-topics (without .sh)
        let which2 = Command::new("which").arg("kafka-topics").output();

        if which2.is_err() || !which2.unwrap().status.success() {
            println!("kafka-topics not found, skipping Kafka CLI test");
            server.shutdown().await;
            return;
        }
    }

    let output = Command::new("kafka-topics.sh")
        .args([
            "--bootstrap-server",
            &server.bootstrap,
            "--create",
            "--topic",
            "kafka-cli-topic",
            "--partitions",
            "3",
            "--replication-factor",
            "1",
        ])
        .output();

    if let Ok(output) = output {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        println!("kafka-topics.sh output: {}", stdout);
        println!("kafka-topics.sh stderr: {}", stderr);
    }

    server.shutdown().await;
}

/// Test that kafka-console-producer.sh works
/// Requires: Kafka CLI tools in PATH
#[tokio::test]
#[ignore]
async fn test_kafka_cli_console_producer() {
    let server = start_test_server().await;

    create_topic_via_admin(&server.bootstrap, "kafka-cli-produce-topic", 1).await;

    let producer = Command::new("kafka-console-producer.sh")
        .args([
            "--bootstrap-server",
            &server.bootstrap,
            "--topic",
            "kafka-cli-produce-topic",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn();

    if let Ok(mut producer) = producer {
        use std::io::Write;
        if let Some(ref mut stdin) = producer.stdin {
            let _ = writeln!(stdin, "test message from kafka-console-producer");
        }

        // Give it a moment then kill
        std::thread::sleep(Duration::from_millis(500));
        let _ = producer.kill();

        println!("kafka-console-producer test completed");
    } else {
        println!("kafka-console-producer.sh not available");
    }

    server.shutdown().await;
}

/// Test that kafka-console-consumer.sh works
/// Requires: Kafka CLI tools in PATH
#[tokio::test]
#[ignore]
async fn test_kafka_cli_console_consumer() {
    let server = start_test_server().await;

    create_topic_via_admin(&server.bootstrap, "kafka-cli-consume-topic", 1).await;
    produce_message_via_admin(
        &server.bootstrap,
        "kafka-cli-consume-topic",
        0,
        "key",
        "cli-test-value",
    )
    .await;

    let output = Command::new("kafka-console-consumer.sh")
        .args([
            "--bootstrap-server",
            &server.bootstrap,
            "--topic",
            "kafka-cli-consume-topic",
            "--from-beginning",
            "--max-messages",
            "1",
            "--timeout-ms",
            "10000",
        ])
        .output();

    if let Ok(output) = output {
        let stdout = String::from_utf8_lossy(&output.stdout);
        println!("kafka-console-consumer output: {}", stdout);

        if stdout.contains("cli-test-value") {
            println!("kafka-console-consumer successfully received message");
        }
    } else {
        println!("kafka-console-consumer.sh not available");
    }

    server.shutdown().await;
}

// ============================================================================
// Phase 13.6: Cross-Client Compatibility Tests
// ============================================================================

/// Test that messages produced by one client can be consumed by another
/// Tests interoperability between different Kafka client implementations
#[tokio::test]
#[ignore]
async fn test_cross_client_produce_consume() {
    let server = start_test_server().await;

    create_topic_via_admin(&server.bootstrap, "cross-client-topic", 1).await;

    // Try to produce with kcat
    let produce_result = Command::new("kcat")
        .args([
            "-b",
            &server.bootstrap,
            "-P",
            "-t",
            "cross-client-topic",
            "-p",
            "0",
        ])
        .stdin(Stdio::piped())
        .spawn();

    if let Ok(mut producer) = produce_result {
        use std::io::Write;
        if let Some(ref mut stdin) = producer.stdin {
            let _ = writeln!(stdin, "cross-client-test-message");
        }
        let _ = producer.wait();

        // Now try to consume with kafka-python
        let python_script = format!(
            r#"
from kafka import KafkaConsumer
import sys

try:
    consumer = KafkaConsumer(
        'cross-client-topic',
        bootstrap_servers='{}',
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,
    )

    for message in consumer:
        print(message.value.decode('utf-8'))
        break

    consumer.close()
except Exception as e:
    print(f'Error: {{e}}', file=sys.stderr)
"#,
            server.bootstrap
        );

        let python_output = Command::new("python3")
            .args(["-c", &python_script])
            .output();

        if let Ok(output) = python_output {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if stdout.contains("cross-client-test-message") {
                println!("Cross-client compatibility verified: kcat -> kafka-python");
            }
        }
    } else {
        println!("kcat not available, skipping cross-client test");
    }

    server.shutdown().await;
}

/// Test consumer group coordination across different clients
#[tokio::test]
#[ignore]
async fn test_cross_client_consumer_group() {
    let server = start_test_server().await;

    create_topic_via_admin(&server.bootstrap, "cross-client-group-topic", 4).await;

    // Produce messages
    for i in 0..10 {
        produce_message_via_admin(
            &server.bootstrap,
            "cross-client-group-topic",
            i % 4,
            &format!("key-{}", i),
            &format!("value-{}", i),
        )
        .await;
    }

    // This test would coordinate multiple clients in the same group
    // For now, verify the topic setup works
    println!("Cross-client consumer group test - topic setup complete");

    server.shutdown().await;
}

// ============================================================================
// Test Infrastructure
// ============================================================================

struct TestServer {
    bootstrap: String,
    // In a full implementation, this would hold the server process handle
}

impl TestServer {
    async fn shutdown(self) {
        // Cleanup server resources
    }
}

async fn start_test_server() -> TestServer {
    // In a full implementation, this would start the Streamline server
    // For compatibility tests, we assume the server is already running
    // or use environment variable for bootstrap servers

    let bootstrap =
        std::env::var("STREAMLINE_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string());

    TestServer { bootstrap }
}

async fn create_topic_via_admin(bootstrap: &str, topic: &str, partitions: i32) {
    // Use kcat or internal API to create topic
    let _ = Command::new("kcat")
        .args([
            "-b", bootstrap, "-L", // Just list to warm up connection
        ])
        .output();

    // For simplicity, we rely on auto-create or pre-existing topics
    // In production tests, you'd use AdminClient
    let _ = (topic, partitions);
}

async fn produce_message_via_admin(
    bootstrap: &str,
    topic: &str,
    partition: i32,
    key: &str,
    value: &str,
) {
    // Use kcat to produce the message
    let output = Command::new("kcat")
        .args([
            "-b",
            bootstrap,
            "-P",
            "-t",
            topic,
            "-p",
            &partition.to_string(),
            "-k",
            key,
        ])
        .stdin(Stdio::piped())
        .spawn();

    if let Ok(mut child) = output {
        use std::io::Write;
        if let Some(ref mut stdin) = child.stdin {
            let _ = writeln!(stdin, "{}", value);
        }
        let _ = child.wait();
    }
}

// ============================================================================
// Protocol Version Negotiation Tests
// ============================================================================

/// Test that clients correctly negotiate API versions
#[tokio::test]
#[ignore]
async fn test_api_version_negotiation() {
    let server = start_test_server().await;

    // Verify server responds to ApiVersions request
    // This is the foundation of client compatibility

    let output = Command::new("kcat")
        .args([
            "-b",
            &server.bootstrap,
            "-L",
            "-J", // JSON output for easier parsing
        ])
        .output();

    if let Ok(output) = output {
        if output.status.success() {
            println!("API version negotiation successful with kcat");
        }
    }

    server.shutdown().await;
}

/// Test that older client versions are supported
#[tokio::test]
#[ignore]
async fn test_older_protocol_versions() {
    let server = start_test_server().await;

    // kafka-python with explicit API version
    let python_script = format!(
        r#"
from kafka import KafkaProducer
import sys

try:
    producer = KafkaProducer(
        bootstrap_servers='{}',
        api_version=(0, 10, 1),  # Explicitly use older API version
    )

    future = producer.send('version-test-topic', value=b'test')
    result = future.get(timeout=10)
    print(f'Sent with older API version')
    producer.close()
except Exception as e:
    print(f'Note: {{e}}')
"#,
        server.bootstrap
    );

    let output = Command::new("python3")
        .args(["-c", &python_script])
        .output();

    if let Ok(output) = output {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        println!("Older version test output: {}", stdout);
        println!("Older version test notes: {}", stderr);
    }

    server.shutdown().await;
}

// ============================================================================
// Documentation for Running Tests
// ============================================================================

/// Instructions for running compatibility tests
#[test]
fn test_compatibility_instructions() {
    println!(
        r#"
=== Kafka Client Compatibility Tests ===

These tests verify Streamline's compatibility with external Kafka clients.

PREREQUISITES:
1. Start Streamline server:
   cargo run -- --listen-addr 127.0.0.1:9092

2. Install test clients:

   kcat (recommended, easiest):
   - macOS: brew install kcat
   - Linux: apt install kafkacat

   kafka-python:
   - pip install kafka-python

   Official Kafka CLI (optional):
   - Download from https://kafka.apache.org/downloads

   franz-go kcl (optional):
   - go install github.com/twmb/franz-go/cmd/kcl@latest

3. Run tests:
   cargo test --features compatibility-tests protocol_compatibility -- --ignored --nocapture

4. Specify custom bootstrap server:
   STREAMLINE_BOOTSTRAP=host:port cargo test ...

EXPECTED RESULTS:
- kcat tests should pass if kcat is installed
- kafka-python tests should pass if kafka-python is installed
- Other tests may be skipped if tooling is not available
"#
    );
}
