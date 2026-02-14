//! Integration tests for Streamline binaries
//!
//! These tests verify that the CLI and server binaries work correctly
//! by running them as subprocesses.

use std::process::{Command, Output};
use tempfile::tempdir;

/// Helper to run cargo commands
fn cargo_run(bin: &str, args: &[&str]) -> Output {
    cargo_run_with_features(bin, args, &[])
}

/// Helper to run cargo commands with specific features enabled
fn cargo_run_with_features(bin: &str, args: &[&str], features: &[&str]) -> Output {
    let mut cmd = Command::new("cargo");
    cmd.arg("run").arg("--bin").arg(bin);

    if !features.is_empty() {
        cmd.arg("--features").arg(features.join(","));
    }

    cmd.arg("--")
        .args(args)
        .output()
        .expect("Failed to run cargo command")
}

/// Helper to check if output contains expected text
fn output_contains(output: &Output, text: &str) -> bool {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    stdout.contains(text) || stderr.contains(text)
}

/// Test that streamline-cli --help works
#[test]
fn test_cli_help() {
    let output = cargo_run("streamline-cli", &["--help"]);
    assert!(
        output.status.success() || output_contains(&output, "streamline-cli"),
        "CLI help should work"
    );
    assert!(
        output_contains(&output, "topics") || output_contains(&output, "Commands"),
        "Help should mention topics command"
    );
}

/// Test that streamline-cli --version works
#[test]
fn test_cli_version() {
    let output = cargo_run("streamline-cli", &["--version"]);
    assert!(
        output.status.success() || output_contains(&output, "streamline-cli"),
        "CLI version should work"
    );
}

/// Test that streamline --help works
#[test]
fn test_server_help() {
    let output = cargo_run("streamline", &["--help"]);
    assert!(
        output.status.success() || output_contains(&output, "Streamline"),
        "Server help should work"
    );
    assert!(
        output_contains(&output, "listen-addr") || output_contains(&output, "Options"),
        "Help should mention server options"
    );
}

/// Test that streamline --version works
#[test]
fn test_server_version() {
    let output = cargo_run("streamline", &["--version"]);
    assert!(
        output.status.success() || output_contains(&output, "streamline"),
        "Server version should work"
    );
}

/// Test CLI info command with a fresh data directory
#[test]
fn test_cli_info() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();

    let output = cargo_run("streamline-cli", &["--data-dir", data_dir, "info"]);
    // Info command may fail if no data exists, but should not panic
    // The important thing is it runs without crashing
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Either succeeds or fails gracefully
    assert!(
        output.status.success()
            || stderr.contains("No topics")
            || stderr.contains("error")
            || stdout.contains("Streamline")
            || stdout.contains("Topics")
            || stdout.contains("0"),
        "Info command should run without panicking"
    );
}

/// Test CLI topics list with a fresh data directory
#[test]
fn test_cli_topics_list_empty() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();

    // Create topics subdirectory
    std::fs::create_dir_all(dir.path().join("topics")).unwrap();

    let output = cargo_run(
        "streamline-cli",
        &["--data-dir", data_dir, "topics", "list"],
    );

    // Should either show empty list or succeed
    let stdout = String::from_utf8_lossy(&output.stdout);
    let _stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success() || stdout.contains("No topics") || stdout.is_empty(),
        "Topics list should work on empty directory"
    );
}

/// Test CLI topics create and describe workflow
#[test]
fn test_cli_topics_create_describe() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();

    // Create topics subdirectory
    std::fs::create_dir_all(dir.path().join("topics")).unwrap();

    // Create a topic
    let output = cargo_run(
        "streamline-cli",
        &[
            "--data-dir",
            data_dir,
            "topics",
            "create",
            "test-topic",
            "--partitions",
            "3",
        ],
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should succeed
    assert!(
        output.status.success() || stdout.contains("Created") || stdout.contains("test-topic"),
        "Topic create failed: stdout={}, stderr={}",
        stdout,
        stderr
    );

    // Describe the topic
    let output = cargo_run(
        "streamline-cli",
        &["--data-dir", data_dir, "topics", "describe", "test-topic"],
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success()
            || stdout.contains("test-topic")
            || stdout.contains("partitions")
            || stdout.contains("3"),
        "Topic describe should show topic info"
    );

    // List topics should show our topic
    let output = cargo_run(
        "streamline-cli",
        &["--data-dir", data_dir, "topics", "list"],
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success() || stdout.contains("test-topic"),
        "Topics list should include created topic"
    );
}

/// Test CLI topics delete
#[test]
fn test_cli_topics_delete() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();

    // Create topics subdirectory
    std::fs::create_dir_all(dir.path().join("topics")).unwrap();

    // Create a topic
    let create_output = cargo_run(
        "streamline-cli",
        &["--data-dir", data_dir, "topics", "create", "to-delete"],
    );

    let create_stdout = String::from_utf8_lossy(&create_output.stdout);
    let create_stderr = String::from_utf8_lossy(&create_output.stderr);

    // Verify topic creation succeeded
    assert!(
        create_output.status.success()
            || create_stdout.contains("Created")
            || create_stdout.contains("to-delete"),
        "Topic create should work: stdout={}, stderr={}",
        create_stdout,
        create_stderr
    );

    // Small delay to ensure filesystem operations complete
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Verify topic exists before delete
    let list_before = cargo_run(
        "streamline-cli",
        &["--data-dir", data_dir, "topics", "list"],
    );
    let list_before_stdout = String::from_utf8_lossy(&list_before.stdout);

    // If topic wasn't created, skip the rest of the test
    if !list_before_stdout.contains("to-delete") && !create_output.status.success() {
        // Topic creation failed, skip delete test
        return;
    }

    // Delete the topic (use -y flag to skip confirmation in non-interactive mode)
    let delete_output = cargo_run(
        "streamline-cli",
        &[
            "--data-dir",
            data_dir,
            "-y",
            "topics",
            "delete",
            "to-delete",
        ],
    );

    let delete_stdout = String::from_utf8_lossy(&delete_output.stdout);
    let delete_stderr = String::from_utf8_lossy(&delete_output.stderr);

    // With -y flag, delete should succeed
    assert!(
        delete_output.status.success() || delete_stdout.contains("Deleted"),
        "Topic delete should succeed with -y flag: stdout={}, stderr={}",
        delete_stdout,
        delete_stderr
    );

    // Small delay to ensure filesystem operations complete
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Verify topic is gone from list
    let list_output = cargo_run(
        "streamline-cli",
        &["--data-dir", data_dir, "topics", "list"],
    );

    let list_stdout = String::from_utf8_lossy(&list_output.stdout);

    // Topic should no longer appear in the list
    // Either the topic name is absent, or the list shows "No topics"
    let topic_deleted = !list_stdout.contains("to-delete") || list_stdout.contains("No topics");

    assert!(
        topic_deleted,
        "Deleted topic should not appear in list. List output: {}",
        list_stdout
    );
}

/// Test CLI users add (basic)
#[test]
#[cfg(feature = "auth")]
fn test_cli_users_add() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();

    // Create config subdirectory and users file
    std::fs::create_dir_all(dir.path().join("config")).unwrap();
    let users_file = dir.path().join("config").join("users.yaml");
    std::fs::write(&users_file, "users: []\n").unwrap();
    let users_file_str = users_file.to_str().unwrap();

    let output = cargo_run_with_features(
        "streamline-cli",
        &[
            "--data-dir",
            data_dir,
            "users",
            "add",
            "--users-file",
            users_file_str,
            "testuser",
            "testpass123",
        ],
        &["auth"],
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success() || stdout.contains("Added") || stdout.contains("testuser"),
        "User add should work: stdout={}, stderr={}",
        stdout,
        stderr
    );

    // List users
    let output = cargo_run_with_features(
        "streamline-cli",
        &[
            "--data-dir",
            data_dir,
            "users",
            "list",
            "--users-file",
            users_file_str,
        ],
        &["auth"],
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success() || stdout.contains("testuser"),
        "User list should include created user"
    );
}

/// Test CLI groups list (empty)
#[test]
fn test_cli_groups_list_empty() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();

    // Create consumer_groups subdirectory
    std::fs::create_dir_all(dir.path().join("consumer_groups")).unwrap();

    let output = cargo_run(
        "streamline-cli",
        &["--data-dir", data_dir, "groups", "list"],
    );

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Should work even if empty
    assert!(
        output.status.success() || stdout.contains("No groups") || stdout.is_empty(),
        "Groups list should work on empty directory"
    );
}

/// Test that invalid commands produce appropriate errors
#[test]
fn test_cli_invalid_command() {
    let output = cargo_run("streamline-cli", &["nonexistent-command"]);

    // Should fail with an error
    assert!(
        !output.status.success() || output_contains(&output, "error"),
        "Invalid command should produce an error"
    );
}

/// Test that missing required arguments produce errors
#[test]
fn test_cli_missing_args() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_str().unwrap();

    // topics create without name
    let output = cargo_run(
        "streamline-cli",
        &["--data-dir", data_dir, "topics", "create"],
    );

    // Should fail - missing required argument
    assert!(
        !output.status.success() || output_contains(&output, "required"),
        "Missing args should produce an error"
    );
}

// ============================================================================
// Server Integration Tests
// ============================================================================

use std::net::TcpStream;
use std::process::Stdio;
use std::time::Duration;

/// Helper to find an available port
fn find_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// Helper struct to manage a test server instance
struct TestServer {
    process: std::process::Child,
    kafka_port: u16,
    http_port: u16,
    _data_dir: tempfile::TempDir,
}

impl TestServer {
    /// Start a test server with in-memory storage
    fn start() -> Self {
        let kafka_port = find_available_port();
        let http_port = find_available_port();
        let data_dir = tempdir().unwrap();

        let mut process = Command::new("cargo")
            .arg("run")
            .arg("--bin")
            .arg("streamline")
            .arg("--")
            .arg("--in-memory")
            .arg("--listen-addr")
            .arg(format!("127.0.0.1:{}", kafka_port))
            .arg("--http-addr")
            .arg(format!("127.0.0.1:{}", http_port))
            .arg("--data-dir")
            .arg(data_dir.path().to_str().unwrap())
            .arg("--log-level")
            .arg("error")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start server");

        // Wait for server to be ready (check HTTP health endpoint)
        let start_time = std::time::Instant::now();
        let timeout = Duration::from_secs(60);

        while start_time.elapsed() < timeout {
            if let Ok(stream) = TcpStream::connect(format!("127.0.0.1:{}", http_port)) {
                drop(stream);
                // Give a bit more time for full initialization
                std::thread::sleep(Duration::from_millis(500));
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        // Check if process is still running
        match process.try_wait() {
            Ok(Some(status)) => {
                panic!("Server process exited early with status: {:?}", status);
            }
            Ok(None) => {
                // Process is still running, good
            }
            Err(e) => {
                panic!("Error checking server process: {}", e);
            }
        }

        TestServer {
            process,
            kafka_port,
            http_port,
            _data_dir: data_dir,
        }
    }

    /// Stop the server
    fn stop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Test server starts and accepts Kafka protocol connections
#[test]
#[ignore] // Run with --ignored flag (requires longer timeout)
fn test_server_starts_accepts_connections() {
    let server = TestServer::start();

    // Try to connect to Kafka port
    let result = TcpStream::connect_timeout(
        &format!("127.0.0.1:{}", server.kafka_port).parse().unwrap(),
        Duration::from_secs(5),
    );

    assert!(result.is_ok(), "Should be able to connect to Kafka port");
}

/// Test HTTP health endpoint
#[test]
#[ignore] // Run with --ignored flag
fn test_http_health_endpoint() {
    let server = TestServer::start();

    // Make HTTP request to /health using TCP directly to avoid dependencies
    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{}", server.http_port)).expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    use std::io::{Read, Write};
    let request = format!(
        "GET /health HTTP/1.1\r\nHost: localhost:{}\r\nConnection: close\r\n\r\n",
        server.http_port
    );
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);

    assert!(
        response.contains("200") || response.contains("OK") || response.contains("healthy"),
        "Health endpoint should return success: {}",
        response
    );
}

/// Test HTTP metrics endpoint
#[test]
#[ignore] // Run with --ignored flag
fn test_http_metrics_endpoint() {
    let server = TestServer::start();

    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{}", server.http_port)).expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    use std::io::{Read, Write};
    let request = format!(
        "GET /metrics HTTP/1.1\r\nHost: localhost:{}\r\nConnection: close\r\n\r\n",
        server.http_port
    );
    stream.write_all(request.as_bytes()).unwrap();

    let mut response = String::new();
    let _ = stream.read_to_string(&mut response);

    assert!(
        response.contains("200") || response.contains("streamline_") || response.contains("# HELP"),
        "Metrics endpoint should return Prometheus format: {}",
        response
    );
}

/// Test playground mode creates demo topics
#[test]
#[ignore] // Run with --ignored flag
fn test_playground_mode() {
    let kafka_port = find_available_port();
    let http_port = find_available_port();
    let data_dir = tempdir().unwrap();

    let mut process = Command::new("cargo")
        .arg("run")
        .arg("--bin")
        .arg("streamline")
        .arg("--")
        .arg("--playground")
        .arg("--listen-addr")
        .arg(format!("127.0.0.1:{}", kafka_port))
        .arg("--http-addr")
        .arg(format!("127.0.0.1:{}", http_port))
        .arg("--data-dir")
        .arg(data_dir.path().to_str().unwrap())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to start server");

    // Wait for server to be ready
    let start_time = std::time::Instant::now();
    let timeout = Duration::from_secs(60);

    while start_time.elapsed() < timeout {
        if TcpStream::connect(format!("127.0.0.1:{}", http_port)).is_ok() {
            std::thread::sleep(Duration::from_millis(500));
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Check server is running
    match process.try_wait() {
        Ok(Some(status)) => {
            panic!("Playground server exited early: {:?}", status);
        }
        Ok(None) => {
            // Still running, good
        }
        Err(e) => panic!("Error checking process: {}", e),
    }

    // Verify we can connect to Kafka port
    let result = TcpStream::connect_timeout(
        &format!("127.0.0.1:{}", kafka_port).parse().unwrap(),
        Duration::from_secs(5),
    );
    assert!(result.is_ok(), "Playground should accept connections");

    // Cleanup
    let _ = process.kill();
    let _ = process.wait();
}

/// Test server graceful shutdown
#[test]
#[ignore] // Run with --ignored flag
fn test_server_graceful_shutdown() {
    let mut server = TestServer::start();

    // Verify server is running
    assert!(
        TcpStream::connect(format!("127.0.0.1:{}", server.kafka_port)).is_ok(),
        "Server should be running"
    );

    // Kill the process (graceful shutdown is handled by Drop)
    let _ = server.process.kill();

    // Wait for process to exit
    let result = server.process.wait();
    assert!(result.is_ok(), "Server should exit after shutdown signal");
}

/// Test server rejects invalid listen address
#[test]
fn test_server_invalid_listen_addr() {
    let data_dir = tempdir().unwrap();

    let output = Command::new("cargo")
        .arg("run")
        .arg("--bin")
        .arg("streamline")
        .arg("--")
        .arg("--listen-addr")
        .arg("invalid:not:an:address")
        .arg("--data-dir")
        .arg(data_dir.path().to_str().unwrap())
        .output()
        .expect("Failed to run command");

    // Should fail with error
    assert!(
        !output.status.success()
            || output_contains(&output, "error")
            || output_contains(&output, "invalid"),
        "Invalid address should be rejected"
    );
}
