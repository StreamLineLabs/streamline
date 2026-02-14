//! Shared test fixtures and utilities for Streamline integration tests
//!
//! This module provides common test infrastructure that can be reused across
//! multiple test files, reducing code duplication and ensuring consistent
//! test patterns.
//!
//! # Usage
//!
//! In your test file, add:
//! ```rust,ignore
//! mod common;
//! use common::*;
//! ```
//!
//! # Features
//!
//! - `TestServer`: Manages test server lifecycle (start/stop)
//! - `TestClient`: Makes Kafka protocol requests for testing
//! - Port allocation utilities
//! - Common test data fixtures
//! - Assertion helpers for protocol testing

#![allow(dead_code)]

use bytes::BytesMut;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;
use tempfile::TempDir;

// ============================================================================
// Network Utilities
// ============================================================================

/// Find an available TCP port for testing
///
/// Binds to port 0 and returns the OS-assigned port number.
/// The port is released when the function returns, so there's a small
/// window where another process could claim it.
pub fn find_available_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to port 0");
    listener
        .local_addr()
        .expect("Failed to get local address")
        .port()
}

/// Find multiple available ports at once
///
/// Useful when you need several ports (e.g., Kafka + HTTP + metrics).
pub fn find_available_ports(count: usize) -> Vec<u16> {
    let listeners: Vec<_> = (0..count)
        .map(|_| TcpListener::bind("127.0.0.1:0").expect("Failed to bind to port 0"))
        .collect();

    listeners
        .iter()
        .map(|l| l.local_addr().expect("Failed to get local address").port())
        .collect()
}

/// Wait for a TCP port to become available
///
/// Polls the port until a connection can be established or timeout is reached.
pub fn wait_for_port(port: u16, timeout: Duration) -> bool {
    use std::net::TcpStream;

    let start = std::time::Instant::now();
    let addr = format!("127.0.0.1:{}", port);

    while start.elapsed() < timeout {
        if TcpStream::connect(&addr).is_ok() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

// ============================================================================
// Test Server Management
// ============================================================================

/// A test server instance that manages the Streamline process lifecycle
///
/// Automatically cleans up the server process and temporary directory on drop.
pub struct TestServer {
    /// The server process handle
    process: Option<Child>,
    /// Kafka protocol port
    pub kafka_port: u16,
    /// HTTP API port
    pub http_port: u16,
    /// Metrics port (if enabled)
    pub metrics_port: Option<u16>,
    /// Temporary data directory
    _data_dir: TempDir,
    /// Bootstrap servers string for clients
    pub bootstrap: String,
}

impl TestServer {
    /// Start a new test server with default settings
    ///
    /// Uses in-memory storage for fast tests.
    pub fn start() -> Self {
        Self::start_with_options(TestServerOptions::default())
    }

    /// Start a new test server with custom options
    pub fn start_with_options(options: TestServerOptions) -> Self {
        let kafka_port = find_available_port();
        let http_port = find_available_port();
        let data_dir = tempfile::tempdir().expect("Failed to create temp dir");

        let mut cmd = Command::new("cargo");
        cmd.arg("run")
            .arg("--bin")
            .arg("streamline")
            .arg("--")
            .arg("--listen-addr")
            .arg(format!("127.0.0.1:{}", kafka_port))
            .arg("--http-addr")
            .arg(format!("127.0.0.1:{}", http_port))
            .arg("--data-dir")
            .arg(data_dir.path().to_str().unwrap())
            .arg("--log-level")
            .arg(&options.log_level);

        if options.in_memory {
            cmd.arg("--in-memory");
        }

        if let Some(ref extra_args) = options.extra_args {
            cmd.args(extra_args);
        }

        let process = cmd
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("Failed to start test server");

        // Wait for server to be ready
        let ready = wait_for_port(http_port, options.startup_timeout);
        if !ready {
            panic!(
                "Test server failed to start within {:?}",
                options.startup_timeout
            );
        }

        // Extra delay for full initialization
        std::thread::sleep(Duration::from_millis(200));

        TestServer {
            process: Some(process),
            kafka_port,
            http_port,
            metrics_port: None,
            _data_dir: data_dir,
            bootstrap: format!("127.0.0.1:{}", kafka_port),
        }
    }

    /// Get the Kafka bootstrap servers string
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap
    }

    /// Check if the server process is still running
    pub fn is_running(&mut self) -> bool {
        if let Some(ref mut process) = self.process {
            match process.try_wait() {
                Ok(Some(_)) => false, // Process exited
                Ok(None) => true,     // Still running
                Err(_) => false,      // Error checking
            }
        } else {
            false
        }
    }

    /// Stop the server gracefully
    pub fn stop(&mut self) {
        if let Some(ref mut process) = self.process {
            let _ = process.kill();
            let _ = process.wait();
        }
        self.process = None;
    }

    /// Shutdown the server (async version for compatibility)
    pub async fn shutdown(mut self) {
        self.stop();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Options for configuring a test server
#[derive(Clone)]
pub struct TestServerOptions {
    /// Use in-memory storage (default: true)
    pub in_memory: bool,
    /// Log level (default: "error")
    pub log_level: String,
    /// Startup timeout (default: 60s)
    pub startup_timeout: Duration,
    /// Additional command-line arguments
    pub extra_args: Option<Vec<String>>,
}

impl Default for TestServerOptions {
    fn default() -> Self {
        TestServerOptions {
            in_memory: true,
            log_level: "error".to_string(),
            startup_timeout: Duration::from_secs(60),
            extra_args: None,
        }
    }
}

// ============================================================================
// Lightweight Test Server (no actual process)
// ============================================================================

/// A lightweight test server stub for unit tests
///
/// Does not start an actual server process - useful for testing
/// components in isolation or when the server is started externally.
#[derive(Clone)]
pub struct TestServerStub {
    /// The port (may not have an actual server listening)
    pub port: u16,
    /// Bootstrap servers string
    pub bootstrap: String,
}

impl TestServerStub {
    /// Create a new stub pointing to a specific port
    pub fn new(port: u16) -> Self {
        TestServerStub {
            port,
            bootstrap: format!("127.0.0.1:{}", port),
        }
    }

    /// Create from environment variable or default
    pub fn from_env() -> Self {
        let bootstrap =
            std::env::var("STREAMLINE_BOOTSTRAP").unwrap_or_else(|_| "localhost:9092".to_string());
        let port = bootstrap
            .split(':')
            .nth(1)
            .and_then(|p| p.parse().ok())
            .unwrap_or(9092);
        TestServerStub { port, bootstrap }
    }
}

// ============================================================================
// Test Client
// ============================================================================

/// A test client for making Kafka protocol requests
///
/// Provides a high-level interface for common operations used in tests.
pub struct TestClient {
    correlation_id: AtomicU32,
    #[allow(dead_code)]
    port: u16,
    client_id: String,
}

impl TestClient {
    /// Create a new test client
    pub fn new(port: u16) -> Self {
        TestClient {
            correlation_id: AtomicU32::new(1),
            port,
            client_id: "test-client".to_string(),
        }
    }

    /// Create a new test client (async version for compatibility)
    pub async fn connect(port: u16) -> Self {
        Self::new(port)
    }

    /// Get the next correlation ID
    pub fn next_correlation_id(&self) -> i32 {
        self.correlation_id.fetch_add(1, Ordering::SeqCst) as i32
    }

    /// Set the client ID
    pub fn with_client_id(mut self, client_id: &str) -> Self {
        self.client_id = client_id.to_string();
        self
    }
}

// ============================================================================
// Protocol Message Builders
// ============================================================================

/// Build a size-prefixed Kafka request with proper header encoding
///
/// This is the core function for building raw protocol messages for testing.
pub fn build_request<T: Encodable>(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
    request: &T,
) -> Vec<u8> {
    let header_version = request_header_version(api_key, api_version);

    let mut header = RequestHeader::default()
        .with_request_api_key(api_key)
        .with_request_api_version(api_version)
        .with_correlation_id(correlation_id);

    if let Some(id) = client_id {
        header = header.with_client_id(Some(id.to_string().into()));
    }

    let mut header_buf = BytesMut::new();
    header
        .encode(&mut header_buf, header_version)
        .expect("Failed to encode request header");

    let mut body_buf = BytesMut::new();
    request
        .encode(&mut body_buf, api_version)
        .expect("Failed to encode request body");

    let total_size = header_buf.len() + body_buf.len();
    let mut result = Vec::with_capacity(4 + total_size);
    result.extend_from_slice(&(total_size as u32).to_be_bytes());
    result.extend_from_slice(&header_buf);
    result.extend_from_slice(&body_buf);

    result
}

/// Build an ApiVersions request
pub fn build_api_versions_request(api_version: i16, correlation_id: i32) -> Vec<u8> {
    let request = ApiVersionsRequest::default();
    build_request(
        18,
        api_version,
        correlation_id,
        Some("test-client"),
        &request,
    )
}

/// Build a Metadata request
pub fn build_metadata_request(
    api_version: i16,
    correlation_id: i32,
    topics: Option<Vec<&str>>,
) -> Vec<u8> {
    use kafka_protocol::messages::metadata_request::MetadataRequestTopic;

    let topic_list = topics.map(|t| {
        t.into_iter()
            .map(|name| {
                MetadataRequestTopic::default().with_name(Some(TopicName::from(
                    StrBytes::from_string(name.to_string()),
                )))
            })
            .collect()
    });

    let request = MetadataRequest::default().with_topics(topic_list);
    build_request(
        3,
        api_version,
        correlation_id,
        Some("test-client"),
        &request,
    )
}

/// Build a CreateTopics request
pub fn build_create_topics_request(
    api_version: i16,
    correlation_id: i32,
    topic_name: &str,
    num_partitions: i32,
    replication_factor: i16,
) -> Vec<u8> {
    use kafka_protocol::messages::create_topics_request::CreatableTopic;

    let topic = CreatableTopic::default()
        .with_name(TopicName::from(StrBytes::from_string(
            topic_name.to_string(),
        )))
        .with_num_partitions(num_partitions)
        .with_replication_factor(replication_factor);

    let request = CreateTopicsRequest::default()
        .with_topics(vec![topic])
        .with_timeout_ms(30000);

    build_request(
        19,
        api_version,
        correlation_id,
        Some("test-client"),
        &request,
    )
}

// ============================================================================
// Response Parsing
// ============================================================================

/// Parse a size-prefixed response, returning the response body
pub fn parse_response(data: &[u8]) -> Option<&[u8]> {
    if data.len() < 4 {
        return None;
    }

    let size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if data.len() < 4 + size {
        return None;
    }

    Some(&data[4..4 + size])
}

/// Extract correlation ID from a response
pub fn extract_correlation_id(response_body: &[u8]) -> Option<i32> {
    if response_body.len() < 4 {
        return None;
    }

    Some(i32::from_be_bytes([
        response_body[0],
        response_body[1],
        response_body[2],
        response_body[3],
    ]))
}

/// Parse an ApiVersionsResponse from raw response bytes
pub fn parse_api_versions_response(
    response_body: &[u8],
    api_version: i16,
) -> Result<ApiVersionsResponse, String> {
    let response_header_ver = response_header_version(18, api_version);
    let mut buf = bytes::Bytes::copy_from_slice(response_body);

    let _header = ResponseHeader::decode(&mut buf, response_header_ver)
        .map_err(|e| format!("Failed to decode response header: {}", e))?;

    ApiVersionsResponse::decode(&mut buf, api_version)
        .map_err(|e| format!("Failed to decode ApiVersionsResponse: {}", e))
}

/// Parse a MetadataResponse from raw response bytes
pub fn parse_metadata_response(
    response_body: &[u8],
    api_version: i16,
) -> Result<MetadataResponse, String> {
    let response_header_ver = response_header_version(3, api_version);
    let mut buf = bytes::Bytes::copy_from_slice(response_body);

    let _header = ResponseHeader::decode(&mut buf, response_header_ver)
        .map_err(|e| format!("Failed to decode response header: {}", e))?;

    MetadataResponse::decode(&mut buf, api_version)
        .map_err(|e| format!("Failed to decode MetadataResponse: {}", e))
}

// ============================================================================
// Header Version Helpers
// ============================================================================

/// Get the request header version for a given API key and version
pub fn request_header_version(api_key: i16, api_version: i16) -> i16 {
    match api_key {
        18 if api_version >= 3 => 2, // ApiVersions
        18 => 1,
        3 if api_version >= 9 => 2,  // Metadata
        0 if api_version >= 9 => 2,  // Produce
        1 if api_version >= 12 => 2, // Fetch
        2 if api_version >= 6 => 2,  // ListOffsets
        19 if api_version >= 5 => 2, // CreateTopics
        20 if api_version >= 4 => 2, // DeleteTopics
        17 => 1,                     // SaslHandshake (never flexible)
        36 if api_version >= 2 => 2, // SaslAuthenticate
        10 if api_version >= 3 => 2, // FindCoordinator
        11 if api_version >= 6 => 2, // JoinGroup
        14 if api_version >= 4 => 2, // SyncGroup
        12 if api_version >= 4 => 2, // Heartbeat
        13 if api_version >= 4 => 2, // LeaveGroup
        9 if api_version >= 6 => 2,  // OffsetFetch
        8 if api_version >= 8 => 2,  // OffsetCommit
        15 if api_version >= 5 => 2, // DescribeGroups
        16 if api_version >= 3 => 2, // ListGroups
        42 if api_version >= 2 => 2, // DeleteGroups
        29 if api_version >= 2 => 2, // DescribeAcls
        30 if api_version >= 2 => 2, // CreateAcls
        31 if api_version >= 2 => 2, // DeleteAcls
        22 if api_version >= 2 => 2, // InitProducerId
        24 if api_version >= 3 => 2, // AddPartitionsToTxn
        25 if api_version >= 3 => 2, // AddOffsetsToTxn
        26 if api_version >= 3 => 2, // EndTxn
        28 if api_version >= 3 => 2, // TxnOffsetCommit
        32 if api_version >= 4 => 2, // DescribeConfigs
        33 if api_version >= 2 => 2, // AlterConfigs
        37 if api_version >= 2 => 2, // CreatePartitions
        60 => 2,                     // DescribeCluster (always flexible)
        21 if api_version >= 2 => 2, // DeleteRecords
        44 => 2,                     // IncrementalAlterConfigs (always flexible)
        23 if api_version >= 4 => 2, // OffsetForLeaderEpoch
        _ => 1,
    }
}

/// Get the response header version for a given API key and version
pub fn response_header_version(api_key: i16, api_version: i16) -> i16 {
    match api_key {
        18 => 0,                     // ApiVersions (always v0 for bootstrap)
        3 if api_version >= 9 => 1,  // Metadata
        0 if api_version >= 9 => 1,  // Produce
        1 if api_version >= 12 => 1, // Fetch
        2 if api_version >= 6 => 1,  // ListOffsets
        19 if api_version >= 5 => 1, // CreateTopics
        20 if api_version >= 4 => 1, // DeleteTopics
        17 => 0,                     // SaslHandshake (never flexible)
        36 if api_version >= 2 => 1, // SaslAuthenticate
        10 if api_version >= 3 => 1, // FindCoordinator
        11 if api_version >= 6 => 1, // JoinGroup
        14 if api_version >= 4 => 1, // SyncGroup
        12 if api_version >= 4 => 1, // Heartbeat
        13 if api_version >= 4 => 1, // LeaveGroup
        9 if api_version >= 6 => 1,  // OffsetFetch
        8 if api_version >= 8 => 1,  // OffsetCommit
        15 if api_version >= 5 => 1, // DescribeGroups
        16 if api_version >= 3 => 1, // ListGroups
        42 if api_version >= 2 => 1, // DeleteGroups
        29 if api_version >= 2 => 1, // DescribeAcls
        30 if api_version >= 2 => 1, // CreateAcls
        31 if api_version >= 2 => 1, // DeleteAcls
        22 if api_version >= 2 => 1, // InitProducerId
        24 if api_version >= 3 => 1, // AddPartitionsToTxn
        25 if api_version >= 3 => 1, // AddOffsetsToTxn
        26 if api_version >= 3 => 1, // EndTxn
        28 if api_version >= 3 => 1, // TxnOffsetCommit
        32 if api_version >= 4 => 1, // DescribeConfigs
        33 if api_version >= 2 => 1, // AlterConfigs
        37 if api_version >= 2 => 1, // CreatePartitions
        60 => 1,                     // DescribeCluster (always flexible)
        21 if api_version >= 2 => 1, // DeleteRecords
        44 => 1,                     // IncrementalAlterConfigs (always flexible)
        23 if api_version >= 4 => 1, // OffsetForLeaderEpoch
        _ => 0,
    }
}

// ============================================================================
// Test Data Fixtures
// ============================================================================

/// Common test topic names
pub mod topics {
    pub const TEST_TOPIC: &str = "test-topic";
    pub const MULTI_PARTITION_TOPIC: &str = "multi-partition-test";
    pub const REPLICATION_TOPIC: &str = "replication-test";
    pub const TRANSACTIONAL_TOPIC: &str = "transactional-test";
}

/// Common test record data
pub mod records {
    use bytes::Bytes;

    pub fn test_key(n: usize) -> Bytes {
        Bytes::from(format!("key-{}", n))
    }

    pub fn test_value(n: usize) -> Bytes {
        Bytes::from(format!("value-{}", n))
    }

    pub fn large_value(size: usize) -> Bytes {
        Bytes::from(vec![b'x'; size])
    }
}

/// Common test consumer group names
pub mod groups {
    pub const TEST_GROUP: &str = "test-group";
    pub const REBALANCE_GROUP: &str = "rebalance-test-group";
}

// ============================================================================
// Assertion Helpers
// ============================================================================

/// Assert that a response has no error (error_code == 0)
#[macro_export]
macro_rules! assert_no_error {
    ($response:expr) => {
        assert_eq!(
            $response.error_code, 0,
            "Expected no error, got error code {}",
            $response.error_code
        );
    };
    ($response:expr, $msg:expr) => {
        assert_eq!(
            $response.error_code, 0,
            "{}: error code {}",
            $msg, $response.error_code
        );
    };
}

/// Assert that a response has a specific error code
#[macro_export]
macro_rules! assert_error_code {
    ($response:expr, $expected:expr) => {
        assert_eq!(
            $response.error_code, $expected,
            "Expected error code {}, got {}",
            $expected, $response.error_code
        );
    };
}

// ============================================================================
// API Key Constants
// ============================================================================

/// All supported API keys for testing
pub const SUPPORTED_API_KEYS: &[i16] = &[
    0,  // Produce
    1,  // Fetch
    2,  // ListOffsets
    3,  // Metadata
    8,  // OffsetCommit
    9,  // OffsetFetch
    10, // FindCoordinator
    11, // JoinGroup
    12, // Heartbeat
    13, // LeaveGroup
    14, // SyncGroup
    15, // DescribeGroups
    16, // ListGroups
    17, // SaslHandshake
    18, // ApiVersions
    19, // CreateTopics
    20, // DeleteTopics
    21, // DeleteRecords
    22, // InitProducerId
    23, // OffsetForLeaderEpoch
    24, // AddPartitionsToTxn
    25, // AddOffsetsToTxn
    26, // EndTxn
    28, // TxnOffsetCommit
    29, // DescribeAcls
    30, // CreateAcls
    31, // DeleteAcls
    32, // DescribeConfigs
    33, // AlterConfigs
    36, // SaslAuthenticate
    37, // CreatePartitions
    42, // DeleteGroups
    44, // IncrementalAlterConfigs
    60, // DescribeCluster
];

/// Get the human-readable name for an API key
pub fn api_key_name(api_key: i16) -> &'static str {
    match api_key {
        0 => "Produce",
        1 => "Fetch",
        2 => "ListOffsets",
        3 => "Metadata",
        8 => "OffsetCommit",
        9 => "OffsetFetch",
        10 => "FindCoordinator",
        11 => "JoinGroup",
        12 => "Heartbeat",
        13 => "LeaveGroup",
        14 => "SyncGroup",
        15 => "DescribeGroups",
        16 => "ListGroups",
        17 => "SaslHandshake",
        18 => "ApiVersions",
        19 => "CreateTopics",
        20 => "DeleteTopics",
        21 => "DeleteRecords",
        22 => "InitProducerId",
        23 => "OffsetForLeaderEpoch",
        24 => "AddPartitionsToTxn",
        25 => "AddOffsetsToTxn",
        26 => "EndTxn",
        28 => "TxnOffsetCommit",
        29 => "DescribeAcls",
        30 => "CreateAcls",
        31 => "DeleteAcls",
        32 => "DescribeConfigs",
        33 => "AlterConfigs",
        36 => "SaslAuthenticate",
        37 => "CreatePartitions",
        42 => "DeleteGroups",
        44 => "IncrementalAlterConfigs",
        60 => "DescribeCluster",
        _ => "Unknown",
    }
}

/// Get the maximum supported version for an API key
pub fn max_supported_version(api_key: i16) -> i16 {
    match api_key {
        0 => 9,  // Produce
        1 => 12, // Fetch
        2 => 7,  // ListOffsets
        3 => 12, // Metadata
        8 => 8,  // OffsetCommit
        9 => 8,  // OffsetFetch
        10 => 4, // FindCoordinator
        11 => 9, // JoinGroup
        12 => 4, // Heartbeat
        13 => 5, // LeaveGroup
        14 => 5, // SyncGroup
        15 => 5, // DescribeGroups
        16 => 4, // ListGroups
        17 => 1, // SaslHandshake
        18 => 3, // ApiVersions
        19 => 7, // CreateTopics
        20 => 6, // DeleteTopics
        21 => 2, // DeleteRecords
        22 => 4, // InitProducerId
        23 => 4, // OffsetForLeaderEpoch
        24 => 4, // AddPartitionsToTxn
        25 => 3, // AddOffsetsToTxn
        26 => 3, // EndTxn
        28 => 3, // TxnOffsetCommit
        29 => 3, // DescribeAcls
        30 => 3, // CreateAcls
        31 => 3, // DeleteAcls
        32 => 4, // DescribeConfigs
        33 => 2, // AlterConfigs
        36 => 2, // SaslAuthenticate
        37 => 3, // CreatePartitions
        42 => 2, // DeleteGroups
        44 => 1, // IncrementalAlterConfigs
        60 => 0, // DescribeCluster
        _ => 0,
    }
}

// ============================================================================
// Tests for the common module itself
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_available_port() {
        let port = find_available_port();
        assert!(port > 0);
        assert!(port > 1024, "Should not use privileged ports");
    }

    #[test]
    fn test_find_available_ports() {
        let ports = find_available_ports(3);
        assert_eq!(ports.len(), 3);

        // All ports should be unique
        let unique: std::collections::HashSet<_> = ports.iter().collect();
        assert_eq!(unique.len(), 3, "Ports should be unique");
    }

    #[test]
    fn test_request_header_versions() {
        // ApiVersions v0-2 -> header v1, v3+ -> header v2
        assert_eq!(request_header_version(18, 0), 1);
        assert_eq!(request_header_version(18, 2), 1);
        assert_eq!(request_header_version(18, 3), 2);

        // Metadata v0-8 -> header v1, v9+ -> header v2
        assert_eq!(request_header_version(3, 0), 1);
        assert_eq!(request_header_version(3, 8), 1);
        assert_eq!(request_header_version(3, 9), 2);

        // SaslHandshake never flexible
        assert_eq!(request_header_version(17, 0), 1);
        assert_eq!(request_header_version(17, 1), 1);

        // DescribeCluster always flexible
        assert_eq!(request_header_version(60, 0), 2);
    }

    #[test]
    fn test_response_header_versions() {
        // ApiVersions always v0
        assert_eq!(response_header_version(18, 0), 0);
        assert_eq!(response_header_version(18, 3), 0);

        // Metadata v0-8 -> v0, v9+ -> v1
        assert_eq!(response_header_version(3, 0), 0);
        assert_eq!(response_header_version(3, 8), 0);
        assert_eq!(response_header_version(3, 9), 1);
    }

    #[test]
    fn test_build_api_versions_request() {
        let request = build_api_versions_request(3, 1234);

        // Should have size prefix
        assert!(request.len() > 4);

        let size = u32::from_be_bytes([request[0], request[1], request[2], request[3]]) as usize;
        assert_eq!(size, request.len() - 4);

        // API key should be 18
        let api_key = i16::from_be_bytes([request[4], request[5]]);
        assert_eq!(api_key, 18);

        // Correlation ID
        let corr_id = i32::from_be_bytes([request[8], request[9], request[10], request[11]]);
        assert_eq!(corr_id, 1234);
    }

    #[test]
    fn test_extract_correlation_id() {
        let data = [0x00, 0x00, 0x04, 0xD2, 0x00]; // 1234 in big-endian
        assert_eq!(extract_correlation_id(&data), Some(1234));

        let short = [0x00, 0x00];
        assert_eq!(extract_correlation_id(&short), None);
    }

    #[test]
    fn test_api_key_name() {
        assert_eq!(api_key_name(0), "Produce");
        assert_eq!(api_key_name(18), "ApiVersions");
        assert_eq!(api_key_name(999), "Unknown");
    }

    #[test]
    fn test_test_data_fixtures() {
        use records::*;

        assert_eq!(test_key(1).as_ref(), b"key-1");
        assert_eq!(test_value(2).as_ref(), b"value-2");
        assert_eq!(large_value(100).len(), 100);
    }
}
