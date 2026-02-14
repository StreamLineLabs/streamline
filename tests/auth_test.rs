//! Integration tests for SASL authentication
//!
//! This test requires the `auth` feature to be enabled.
#![cfg(feature = "auth")]

use streamline::{AuthConfig, SaslMechanism, ScramAuthenticator, User, UserStore};
use tempfile::NamedTempFile;

/// Test SASL/PLAIN authentication flow
#[test]
fn test_sasl_plain_authentication() {
    // Create a test users file
    let temp_file = NamedTempFile::new().unwrap();
    let mut store = UserStore::new();

    let user = User::new(
        "testuser".to_string(),
        "testpass",
        vec!["admin".to_string()],
    )
    .unwrap();

    store.add_user(user);
    store.save_to_file(temp_file.path()).unwrap();

    // Load the store
    let loaded_store = UserStore::from_file(temp_file.path()).unwrap();

    // Verify user can authenticate
    assert!(loaded_store.authenticate("testuser", "testpass").is_ok());
    assert!(loaded_store.authenticate("testuser", "wrongpass").is_err());
    assert!(loaded_store.authenticate("unknown", "testpass").is_err());
}

/// Test user permissions
#[test]
fn test_user_permissions() {
    let admin = User {
        username: "admin".to_string(),
        password_hash: "hash".to_string(),
        permissions: vec!["admin".to_string()],
        scram_sha256: None,
        scram_sha512: None,
    };

    let producer = User {
        username: "producer".to_string(),
        password_hash: "hash".to_string(),
        permissions: vec!["produce:*".to_string(), "describe:*".to_string()],
        scram_sha256: None,
        scram_sha512: None,
    };

    let consumer = User {
        username: "consumer".to_string(),
        password_hash: "hash".to_string(),
        permissions: vec!["consume:*".to_string(), "describe:*".to_string()],
        scram_sha256: None,
        scram_sha512: None,
    };

    // Admin has all permissions
    assert!(admin.has_permission("produce:orders"));
    assert!(admin.has_permission("consume:orders"));
    assert!(admin.has_permission("describe:orders"));

    // Producer has produce and describe permissions
    assert!(producer.has_permission("produce:orders"));
    assert!(producer.has_permission("describe:orders"));
    assert!(!producer.has_permission("consume:orders"));

    // Consumer has consume and describe permissions
    assert!(consumer.has_permission("consume:orders"));
    assert!(consumer.has_permission("describe:orders"));
    assert!(!consumer.has_permission("produce:orders"));
}

/// Test auth config
#[test]
fn test_auth_config() {
    let config = AuthConfig {
        enabled: true,
        sasl_mechanisms: vec!["PLAIN".to_string(), "SCRAM-SHA-256".to_string()],
        users_file: None,
        allow_anonymous: false,
        oauth: streamline::auth::OAuthConfig::default(),
    };

    assert!(config.enabled);
    assert_eq!(config.sasl_mechanisms.len(), 2);
    assert!(!config.allow_anonymous);
}

/// Test auth config default
#[test]
fn test_auth_config_default() {
    let config = AuthConfig::default();

    assert!(!config.enabled);
    assert_eq!(config.sasl_mechanisms, vec!["PLAIN".to_string()]);
    assert!(!config.allow_anonymous);
}

/// Test SCRAM-SHA-256 credential generation
#[test]
fn test_scram_sha256_credentials() {
    let user = User::new(
        "scramuser".to_string(),
        "testpassword",
        vec!["admin".to_string()],
    )
    .unwrap();

    // Verify SCRAM-SHA-256 credentials were generated
    assert!(user.scram_sha256.is_some());
    let creds = user.scram_sha256.as_ref().unwrap();

    // Check credential fields
    assert!(!creds.salt.is_empty());
    assert!(!creds.stored_key.is_empty());
    assert!(!creds.server_key.is_empty());
    assert_eq!(creds.iterations, 4096); // Default iteration count
}

/// Test SCRAM-SHA-512 credential generation
#[test]
fn test_scram_sha512_credentials() {
    let user = User::new(
        "scramuser".to_string(),
        "testpassword",
        vec!["admin".to_string()],
    )
    .unwrap();

    // Verify SCRAM-SHA-512 credentials were generated
    assert!(user.scram_sha512.is_some());
    let creds = user.scram_sha512.as_ref().unwrap();

    // Check credential fields
    assert!(!creds.salt.is_empty());
    assert!(!creds.stored_key.is_empty());
    assert!(!creds.server_key.is_empty());
    assert_eq!(creds.iterations, 4096); // Default iteration count
}

/// Test SCRAM authenticator creation
#[test]
fn test_scram_authenticator_creation() {
    // Valid SCRAM mechanisms
    assert!(ScramAuthenticator::new(SaslMechanism::ScramSha256).is_ok());
    assert!(ScramAuthenticator::new(SaslMechanism::ScramSha512).is_ok());

    // PLAIN is not a valid SCRAM mechanism
    assert!(ScramAuthenticator::new(SaslMechanism::Plain).is_err());
}

/// Test SCRAM mechanism names
#[test]
fn test_scram_mechanism_names() {
    assert_eq!(SaslMechanism::ScramSha256.name(), "SCRAM-SHA-256");
    assert_eq!(SaslMechanism::ScramSha512.name(), "SCRAM-SHA-512");
    assert_eq!(SaslMechanism::Plain.name(), "PLAIN");
}

/// Test SCRAM mechanism from name parsing
#[test]
fn test_scram_mechanism_from_name() {
    assert_eq!(
        SaslMechanism::from_name("SCRAM-SHA-256"),
        Some(SaslMechanism::ScramSha256)
    );
    assert_eq!(
        SaslMechanism::from_name("SCRAM-SHA-512"),
        Some(SaslMechanism::ScramSha512)
    );
    assert_eq!(
        SaslMechanism::from_name("PLAIN"),
        Some(SaslMechanism::Plain)
    );
    assert_eq!(SaslMechanism::from_name("UNKNOWN"), None);
}

/// Test SCRAM client-first message processing
#[test]
fn test_scram_client_first_processing() {
    // Create a user store with a test user
    let mut store = UserStore::new();
    let user = User::new(
        "testuser".to_string(),
        "testpassword",
        vec!["admin".to_string()],
    )
    .unwrap();
    store.add_user(user);

    // Create authenticator
    let authenticator = ScramAuthenticator::new(SaslMechanism::ScramSha256).unwrap();

    // Valid client-first message: n,,n=testuser,r=client-nonce
    let client_first = b"n,,n=testuser,r=ABC123XYZ";
    let result = authenticator.process_client_first(client_first, &store);

    assert!(result.is_ok());
    let (server_first, state) = result.unwrap();

    // Verify server-first contains required parts
    assert!(server_first.starts_with("r=")); // Combined nonce
    assert!(server_first.contains(",s=")); // Salt
    assert!(server_first.contains(",i=")); // Iterations

    // Verify state was captured
    assert_eq!(state.username, "testuser");
    assert!(state.server_nonce.starts_with("ABC123XYZ")); // Combined nonce starts with client nonce
}

/// Test SCRAM with unknown user
#[test]
fn test_scram_unknown_user() {
    let store = UserStore::new(); // Empty store

    let authenticator = ScramAuthenticator::new(SaslMechanism::ScramSha256).unwrap();
    let client_first = b"n,,n=unknownuser,r=ABC123XYZ";
    let result = authenticator.process_client_first(client_first, &store);

    assert!(result.is_err());
}

/// Test SCRAM with invalid client-first message format
#[test]
fn test_scram_invalid_client_first() {
    let mut store = UserStore::new();
    let user = User::new(
        "testuser".to_string(),
        "testpassword",
        vec!["admin".to_string()],
    )
    .unwrap();
    store.add_user(user);

    let authenticator = ScramAuthenticator::new(SaslMechanism::ScramSha256).unwrap();

    // Missing GS2 header
    let invalid_msg = b"n=testuser,r=ABC123XYZ";
    assert!(authenticator
        .process_client_first(invalid_msg, &store)
        .is_err());

    // Missing username
    let invalid_msg = b"n,,r=ABC123XYZ";
    assert!(authenticator
        .process_client_first(invalid_msg, &store)
        .is_err());

    // Missing nonce
    let invalid_msg = b"n,,n=testuser";
    assert!(authenticator
        .process_client_first(invalid_msg, &store)
        .is_err());
}

/// Test that users with SCRAM credentials can be persisted and loaded
#[test]
fn test_scram_user_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let mut store = UserStore::new();

    let user = User::new(
        "scramuser".to_string(),
        "testpassword",
        vec!["admin".to_string()],
    )
    .unwrap();

    // Store original credentials for comparison
    let original_salt = user.scram_sha256.as_ref().unwrap().salt.clone();

    store.add_user(user);
    store.save_to_file(temp_file.path()).unwrap();

    // Load and verify
    let loaded_store = UserStore::from_file(temp_file.path()).unwrap();
    let loaded_user = loaded_store.get_user("scramuser").unwrap();

    // Verify SCRAM credentials were persisted
    assert!(loaded_user.scram_sha256.is_some());
    assert!(loaded_user.scram_sha512.is_some());

    // Verify the credentials are the same
    assert_eq!(
        loaded_user.scram_sha256.as_ref().unwrap().salt,
        original_salt
    );
}
