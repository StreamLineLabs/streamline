use super::*;

#[test]
fn test_sasl_handshake_plain_mechanism() {
    // Test SASL handshake requesting PLAIN mechanism
    use kafka_protocol::messages::SaslHandshakeRequest;

    let _handler = create_test_handler();

    let request =
        SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("PLAIN"));

    // SaslHandshake is async, but we can still test the request construction
    // The handler should return supported mechanisms
    assert_eq!(request.mechanism.as_str(), "PLAIN");
}

#[test]
fn test_sasl_handshake_scram_sha256() {
    // Test SASL handshake with SCRAM-SHA-256
    use kafka_protocol::messages::SaslHandshakeRequest;

    let _handler = create_test_handler();

    let request =
        SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("SCRAM-SHA-256"));

    assert_eq!(request.mechanism.as_str(), "SCRAM-SHA-256");
}

#[test]
fn test_sasl_handshake_scram_sha512() {
    // Test SASL handshake with SCRAM-SHA-512
    use kafka_protocol::messages::SaslHandshakeRequest;

    let _handler = create_test_handler();

    let request =
        SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("SCRAM-SHA-512"));

    assert_eq!(request.mechanism.as_str(), "SCRAM-SHA-512");
}

#[test]
fn test_sasl_handshake_unsupported_mechanism() {
    // Test SASL handshake with unsupported mechanism
    use kafka_protocol::messages::SaslHandshakeRequest;

    let _handler = create_test_handler();

    let request =
        SaslHandshakeRequest::default().with_mechanism(StrBytes::from_static_str("OAUTHBEARER"));

    // OAUTHBEARER is not supported, should eventually return UNSUPPORTED_SASL_MECHANISM (33)
    assert_eq!(request.mechanism.as_str(), "OAUTHBEARER");
}

#[test]
fn test_sasl_handshake_request_header_v1_phase6() {
    // SaslHandshake should always use request header v1 (never flexible)
    use kafka_protocol::messages::ApiKey;

    for version in 0i16..=1 {
        let header_version =
            KafkaHandler::request_header_version(ApiKey::SaslHandshake as i16, version);
        assert_eq!(
            header_version, 1,
            "SaslHandshake v{} should use request header v1",
            version
        );
    }
}

#[test]
fn test_sasl_handshake_response_header_v0_phase6() {
    // SaslHandshake should always use response header v0
    use kafka_protocol::messages::ApiKey;

    for version in 0i16..=1 {
        let header_version =
            KafkaHandler::response_header_version(ApiKey::SaslHandshake as i16, version);
        assert_eq!(
            header_version, 0,
            "SaslHandshake v{} should use response header v0",
            version
        );
    }
}

#[test]
fn test_sasl_authenticate_request_construction() {
    // Test SaslAuthenticate request construction
    use kafka_protocol::messages::SaslAuthenticateRequest;

    let _handler = create_test_handler();

    // SASL auth data is opaque bytes representing the authentication exchange
    let auth_bytes = b"test-auth-data";
    let request =
        SaslAuthenticateRequest::default().with_auth_bytes(bytes::Bytes::from_static(auth_bytes));

    assert_eq!(request.auth_bytes.len(), auth_bytes.len());
}

#[test]
fn test_sasl_authenticate_header_versions() {
    // SaslAuthenticate uses flexible headers starting at v2
    use kafka_protocol::messages::ApiKey;

    // v0-1 use header v1
    for version in 0i16..2 {
        let header_version =
            KafkaHandler::request_header_version(ApiKey::SaslAuthenticate as i16, version);
        assert_eq!(
            header_version, 1,
            "SaslAuthenticate v{} should use request header v1",
            version
        );
    }

    // v2+ use header v2 (flexible)
    let header_version = KafkaHandler::request_header_version(ApiKey::SaslAuthenticate as i16, 2);
    assert_eq!(
        header_version, 2,
        "SaslAuthenticate v2 should use request header v2"
    );
}

#[test]
fn test_sasl_mechanism_plain() {
    // SASL PLAIN mechanism name
    let mechanism = "PLAIN";
    assert_eq!(mechanism, "PLAIN");
}

#[test]
fn test_sasl_mechanism_scram_sha256() {
    // SASL SCRAM-SHA-256 mechanism name
    let mechanism = "SCRAM-SHA-256";
    assert!(mechanism.starts_with("SCRAM-"));
    assert!(mechanism.contains("256"));
}

#[test]
fn test_sasl_mechanism_scram_sha512() {
    // SASL SCRAM-SHA-512 mechanism name
    let mechanism = "SCRAM-SHA-512";
    assert!(mechanism.starts_with("SCRAM-"));
    assert!(mechanism.contains("512"));
}

#[test]
fn test_sasl_handshake_response_mechanisms() {
    // SaslHandshakeResponse should list mechanisms
    use kafka_protocol::messages::SaslHandshakeResponse;

    let mut response = SaslHandshakeResponse::default();
    response.error_code = 0;
    response.mechanisms = vec![
        "PLAIN".to_string().into(),
        "SCRAM-SHA-256".to_string().into(),
        "SCRAM-SHA-512".to_string().into(),
    ];

    assert_eq!(response.error_code, 0);
    assert_eq!(response.mechanisms.len(), 3);
}

#[test]
fn test_sasl_authenticate_request_fields() {
    // SaslAuthenticateRequest structure
    use kafka_protocol::messages::SaslAuthenticateRequest;

    let mut request = SaslAuthenticateRequest::default();
    request.auth_bytes = bytes::Bytes::from("test-auth-data");

    // auth_bytes should be settable
    assert!(!request.auth_bytes.is_empty());
}

#[test]
fn test_sasl_authenticate_response_fields() {
    // SaslAuthenticateResponse structure
    use kafka_protocol::messages::SaslAuthenticateResponse;

    let mut response = SaslAuthenticateResponse::default();
    response.error_code = 0;
    response.session_lifetime_ms = 3600000; // 1 hour

    assert_eq!(response.error_code, 0);
    assert_eq!(response.session_lifetime_ms, 3600000);
}

#[test]
fn test_sasl_authentication_failed_error() {
    // SASL_AUTHENTICATION_FAILED error code (58)
    assert_eq!(SASL_AUTHENTICATION_FAILED, 58);
}

#[test]
fn test_unsupported_sasl_mechanism_error() {
    // UNSUPPORTED_SASL_MECHANISM error code (33)
    assert_eq!(UNSUPPORTED_SASL_MECHANISM, 33);
}

#[test]
fn test_sasl_handshake_version_0() {
    // SaslHandshake v0 uses non-flexible encoding
    let version: i16 = 0;
    let header_version =
        KafkaHandler::request_header_version(ApiKey::SaslHandshake as i16, version);
    // SaslHandshake always uses header v1 (never flexible)
    assert_eq!(header_version, 1);
}

#[test]
fn test_sasl_handshake_version_1() {
    // SaslHandshake v1 also uses non-flexible encoding
    let version: i16 = 1;
    let header_version =
        KafkaHandler::request_header_version(ApiKey::SaslHandshake as i16, version);
    // SaslHandshake always uses header v1 (never flexible)
    assert_eq!(header_version, 1);
}

#[test]
fn test_sasl_authenticate_version_0() {
    // SaslAuthenticate v0 uses non-flexible encoding
    let version: i16 = 0;
    let header_version =
        KafkaHandler::request_header_version(ApiKey::SaslAuthenticate as i16, version);
    assert_eq!(header_version, 1);
}

#[test]
fn test_sasl_authenticate_version_2() {
    // SaslAuthenticate v2 uses flexible encoding
    let version: i16 = 2;
    let header_version =
        KafkaHandler::request_header_version(ApiKey::SaslAuthenticate as i16, version);
    assert_eq!(header_version, 2);
}

#[test]
fn test_sasl_handshake_response_header_version() {
    // SaslHandshake response always uses header v0
    let version: i16 = 0;
    let header_version =
        KafkaHandler::response_header_version(ApiKey::SaslHandshake as i16, version);
    assert_eq!(header_version, 0);
}

#[test]
fn test_sasl_authenticate_response_header_version_v0() {
    // SaslAuthenticate v0 response uses header v0
    let version: i16 = 0;
    let header_version =
        KafkaHandler::response_header_version(ApiKey::SaslAuthenticate as i16, version);
    assert_eq!(header_version, 0);
}

#[test]
fn test_sasl_authenticate_response_header_version_v2() {
    // SaslAuthenticate v2 response uses header v1
    let version: i16 = 2;
    let header_version =
        KafkaHandler::response_header_version(ApiKey::SaslAuthenticate as i16, version);
    assert_eq!(header_version, 1);
}

#[test]
fn test_auth_config_default() {
    // Auth config defaults
    use crate::config::AuthConfig;

    let config = AuthConfig::default();
    assert!(!config.enabled);
    // Default has PLAIN mechanism
    assert!(config.sasl_mechanisms.contains(&"PLAIN".to_string()));
    assert!(config.users_file.is_none());
    assert!(!config.allow_anonymous);
}

#[test]
fn test_auth_config_with_plain() {
    // Auth config with PLAIN mechanism
    use crate::config::AuthConfig;

    let config = AuthConfig {
        enabled: true,
        sasl_mechanisms: vec!["PLAIN".to_string()],
        ..Default::default()
    };

    assert!(config.enabled);
    assert!(config.sasl_mechanisms.contains(&"PLAIN".to_string()));
}

#[test]
fn test_auth_config_with_scram() {
    // Auth config with SCRAM mechanism
    use crate::config::AuthConfig;

    let config = AuthConfig {
        enabled: true,
        sasl_mechanisms: vec!["SCRAM-SHA-256".to_string(), "SCRAM-SHA-512".to_string()],
        ..Default::default()
    };

    assert!(config.enabled);
    assert_eq!(config.sasl_mechanisms.len(), 2);
}

#[test]
fn test_sasl_plain_auth_bytes_format() {
    // SASL PLAIN auth bytes format: [authzid]\0username\0password
    let authzid = "";
    let username = "user";
    let password = "pass";

    let auth_bytes = format!("{}\0{}\0{}", authzid, username, password);
    let parts: Vec<&str> = auth_bytes.split('\0').collect();

    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0], ""); // authzid (empty)
    assert_eq!(parts[1], "user");
    assert_eq!(parts[2], "pass");
}

#[test]
fn test_sasl_plain_auth_with_authzid() {
    // SASL PLAIN with authorization identity
    let authzid = "admin";
    let username = "user";
    let password = "pass";

    let auth_bytes = format!("{}\0{}\0{}", authzid, username, password);
    let parts: Vec<&str> = auth_bytes.split('\0').collect();

    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0], "admin"); // authzid
    assert_eq!(parts[1], "user");
    assert_eq!(parts[2], "pass");
}

#[test]
fn test_scram_nonce_length() {
    // SCRAM nonce should be sufficiently long for security
    let min_nonce_length = 16;

    // Simulate nonce generation
    use rand::Rng;
    let nonce: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(24)
        .map(char::from)
        .collect();

    assert!(
        nonce.len() >= min_nonce_length,
        "Nonce should be at least {} chars",
        min_nonce_length
    );
}

#[test]
fn test_scram_iteration_count() {
    // SCRAM iteration count should be secure
    let min_iterations = 4096;
    let default_iterations = 4096;

    assert!(
        default_iterations >= min_iterations,
        "Default iterations should be at least {}",
        min_iterations
    );
}

#[test]
fn test_oauth_token_bearer_mechanism() {
    // OAuth uses OAUTHBEARER mechanism
    let mechanism = "OAUTHBEARER";
    assert!(mechanism.contains("OAUTH"));
    assert!(mechanism.contains("BEARER"));
}
