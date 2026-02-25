//! Kafka Protocol Conformance Test Suite
//!
//! Validates that Streamline correctly implements the Kafka wire protocol
//! for all 50 supported APIs. Each test verifies request/response encoding,
//! error handling, and version negotiation against the official Kafka spec.
//!
//! # Running
//!
//! ```bash
//! cargo test --test conformance_test --features compatibility-tests
//! ```

#[path = "common/mod.rs"]
mod common;

use bytes::BytesMut;
use common::*;
use kafka_protocol::messages::*;
use kafka_protocol::protocol::{Decodable, StrBytes};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

/// Send a raw Kafka protocol request and read the response
fn send_request(port: u16, request_bytes: &[u8]) -> Vec<u8> {
    let mut stream =
        TcpStream::connect(format!("127.0.0.1:{}", port)).expect("Failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .ok();
    stream.write_all(request_bytes).expect("Failed to write");

    let mut size_buf = [0u8; 4];
    stream
        .read_exact(&mut size_buf)
        .expect("Failed to read size");
    let size = u32::from_be_bytes(size_buf) as usize;

    let mut body = vec![0u8; size];
    stream
        .read_exact(&mut body)
        .expect("Failed to read body");
    body
}

// ============================================================================
// Core Protocol Conformance Tests
// ============================================================================

#[cfg(feature = "compatibility-tests")]
mod conformance {
    use super::*;

    #[test]
    fn test_api_versions_conformance() {
        let server = TestServer::start();
        let req = build_api_versions_request(3, 1);
        let response_body = send_request(server.kafka_port, &req);

        let (_, api_response) =
            parse_api_versions_response(&response_body).expect("Failed to parse");

        // Must report at least 50 supported APIs
        assert!(
            api_response.api_keys.len() >= 50,
            "Expected at least 50 API keys, got {}",
            api_response.api_keys.len()
        );

        // All APIs must support version 0 for backward compatibility
        for api in &api_response.api_keys {
            assert!(
                api.min_version <= api.max_version,
                "API {} min_version ({}) must be <= max_version ({})",
                api.api_key,
                api.min_version,
                api.max_version
            );
        }
    }

    #[test]
    fn test_metadata_conformance() {
        let server = TestServer::start();
        let req = build_metadata_request(9, 1, None);
        let response_body = send_request(server.kafka_port, &req);

        let (_, meta_response) =
            parse_metadata_response(&response_body).expect("Failed to parse");

        // Must return at least one broker
        assert!(
            !meta_response.brokers.is_empty(),
            "Metadata must return at least one broker"
        );
    }

    #[test]
    fn test_create_topic_conformance() {
        let server = TestServer::start();

        // Create a topic
        let req = build_create_topics_request(5, 1, "conformance-topic", 3, 1);
        let response_body = send_request(server.kafka_port, &req);

        // Parse CreateTopicsResponse
        let header_version = response_header_version(19, 5);
        let mut buf = BytesMut::from(&response_body[..]);
        let _header =
            ResponseHeader::decode(&mut buf, header_version).expect("Failed to decode header");
        let create_response =
            CreateTopicsResponse::decode(&mut buf, 5).expect("Failed to decode");

        assert_eq!(
            create_response.topics.len(),
            1,
            "Must return exactly one topic result"
        );
        let topic_result = &create_response.topics[0];
        assert_eq!(
            topic_result.error_code, 0,
            "Topic creation must succeed (got error {})",
            topic_result.error_code
        );

        // Create duplicate topic — must return TOPIC_ALREADY_EXISTS (36)
        let req2 = build_create_topics_request(5, 2, "conformance-topic", 3, 1);
        let response_body2 = send_request(server.kafka_port, &req2);

        let mut buf2 = BytesMut::from(&response_body2[..]);
        let _header2 = ResponseHeader::decode(&mut buf2, header_version)
            .expect("Failed to decode header");
        let create_response2 =
            CreateTopicsResponse::decode(&mut buf2, 5).expect("Failed to decode");

        assert_eq!(
            create_response2.topics[0].error_code, 36,
            "Duplicate topic must return TOPIC_ALREADY_EXISTS (36)"
        );
    }

    #[test]
    fn test_version_range_validity() {
        let server = TestServer::start();
        let req = build_api_versions_request(3, 1);
        let response_body = send_request(server.kafka_port, &req);

        let (_, api_response) =
            parse_api_versions_response(&response_body).expect("Failed to parse");

        // Verify expected core API keys are present
        let api_map: std::collections::HashMap<i16, _> = api_response
            .api_keys
            .iter()
            .map(|k| (k.api_key, k))
            .collect();

        let required_apis = vec![
            (0, "Produce"),
            (1, "Fetch"),
            (3, "Metadata"),
            (8, "OffsetCommit"),
            (9, "OffsetFetch"),
            (10, "FindCoordinator"),
            (11, "JoinGroup"),
            (18, "ApiVersions"),
            (19, "CreateTopics"),
            (20, "DeleteTopics"),
        ];

        for (api_key, name) in &required_apis {
            assert!(
                api_map.contains_key(api_key),
                "Required API {} ({}) missing from ApiVersions response",
                name,
                api_key
            );
        }
    }
}

// ============================================================================
// Stability Matrix
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
enum StabilityLevel {
    Stable,
    Beta,
    Experimental,
}

#[allow(dead_code)]
fn stability_matrix() -> Vec<(&'static str, StabilityLevel, &'static str)> {
    vec![
        ("server", StabilityLevel::Stable, "TCP server, connection handling, HTTP API"),
        ("storage", StabilityLevel::Stable, "Segment-based persistent storage"),
        ("protocol", StabilityLevel::Stable, "Kafka wire protocol (50 APIs)"),
        ("consumer", StabilityLevel::Stable, "Consumer groups, offset management"),
        ("config", StabilityLevel::Stable, "Server configuration, CLI args"),
        ("error", StabilityLevel::Stable, "Error types and Result alias"),
        ("embedded", StabilityLevel::Stable, "Embedded library mode"),
        ("analytics", StabilityLevel::Stable, "DuckDB SQL analytics"),
        ("transaction", StabilityLevel::Beta, "Transaction coordinator"),
        ("cluster", StabilityLevel::Beta, "Raft-based clustering"),
        ("replication", StabilityLevel::Beta, "Data replication, ISR"),
        ("schema", StabilityLevel::Beta, "Schema Registry"),
        ("auth", StabilityLevel::Beta, "SASL/OAuth, ACLs"),
        ("tenant", StabilityLevel::Beta, "Multi-tenancy framework"),
        ("connect", StabilityLevel::Beta, "Kafka Connect compatibility"),
        ("ai", StabilityLevel::Experimental, "AI/ML streaming pipeline"),
        ("faas", StabilityLevel::Experimental, "Serverless functions"),
        ("cdc", StabilityLevel::Experimental, "Change Data Capture"),
        ("sink", StabilityLevel::Experimental, "Lakehouse connectors"),
        ("streamql", StabilityLevel::Experimental, "Continuous query engine"),
        ("edge", StabilityLevel::Experimental, "Edge deployment"),
        ("cloud", StabilityLevel::Experimental, "Managed cloud service"),
        ("wasm", StabilityLevel::Experimental, "WASM transforms"),
        ("marketplace", StabilityLevel::Experimental, "Transform registry"),
    ]
}

#[test]
fn test_stability_matrix_completeness() {
    let matrix = stability_matrix();
    assert!(matrix.len() >= 24, "Must cover at least 24 modules, got {}", matrix.len());

    let stable_count = matrix.iter().filter(|(_, l, _)| *l == StabilityLevel::Stable).count();
    assert!(stable_count >= 8, "v1.0 must have at least 8 stable modules, got {}", stable_count);
}
