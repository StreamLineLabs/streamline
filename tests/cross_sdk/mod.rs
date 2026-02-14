//! Cross-SDK Integration Test Suite
//!
//! Validates that all Streamline SDKs (Java, Python, Go, Node.js, Rust, .NET)
//! can communicate correctly with the Streamline server by testing the core
//! Kafka protocol operations that every SDK relies on.
//!
//! These tests use the embedded `TopicManager` and `GroupCoordinator` directly
//! to verify protocol-level compatibility without needing a running server.
//!
//! Run with: `cargo test --test cross_sdk_test`

mod protocol_compat;
