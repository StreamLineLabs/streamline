//! Phase 19: SASL Authentication Protocol Tests
//!
//! Tests for SASL authentication mechanism encoding/decoding:
//! - SaslHandshake (API Key 17, v0-v1)
//! - SaslAuthenticate (API Key 36, v0-v2)
//!
//! These tests verify proper encoding and decoding of SASL authentication
//! messages used for broker authentication.

use bytes::BytesMut;
use kafka_protocol::messages::{
    SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

// ============================================================================
// SaslHandshake Request Tests (API Key 17, v0-v1)
// ============================================================================

/// Test SaslHandshakeRequest v0 with PLAIN mechanism
#[test]
fn test_sasl_handshake_request_v0_plain() {
    let mut request = SaslHandshakeRequest::default();
    request.mechanism = StrBytes::from_static_str("PLAIN");

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslHandshakeRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.mechanism.as_str(), "PLAIN");
}

/// Test SaslHandshakeRequest v0 with SCRAM-SHA-256 mechanism
#[test]
fn test_sasl_handshake_request_v0_scram_sha256() {
    let mut request = SaslHandshakeRequest::default();
    request.mechanism = StrBytes::from_static_str("SCRAM-SHA-256");

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslHandshakeRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.mechanism.as_str(), "SCRAM-SHA-256");
}

/// Test SaslHandshakeRequest v0 with SCRAM-SHA-512 mechanism
#[test]
fn test_sasl_handshake_request_v0_scram_sha512() {
    let mut request = SaslHandshakeRequest::default();
    request.mechanism = StrBytes::from_static_str("SCRAM-SHA-512");

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslHandshakeRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.mechanism.as_str(), "SCRAM-SHA-512");
}

/// Test SaslHandshakeRequest v0 with OAUTHBEARER mechanism
#[test]
fn test_sasl_handshake_request_v0_oauthbearer() {
    let mut request = SaslHandshakeRequest::default();
    request.mechanism = StrBytes::from_static_str("OAUTHBEARER");

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslHandshakeRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.mechanism.as_str(), "OAUTHBEARER");
}

/// Test SaslHandshakeRequest v0 with GSSAPI mechanism
#[test]
fn test_sasl_handshake_request_v0_gssapi() {
    let mut request = SaslHandshakeRequest::default();
    request.mechanism = StrBytes::from_static_str("GSSAPI");

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslHandshakeRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.mechanism.as_str(), "GSSAPI");
}

/// Test SaslHandshakeRequest v1 with PLAIN mechanism
#[test]
fn test_sasl_handshake_request_v1_plain() {
    let mut request = SaslHandshakeRequest::default();
    request.mechanism = StrBytes::from_static_str("PLAIN");

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslHandshakeRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.mechanism.as_str(), "PLAIN");
}

/// Test SaslHandshakeRequest version roundtrip
#[test]
fn test_sasl_handshake_request_version_roundtrip() {
    for version in 0..=1 {
        let mut request = SaslHandshakeRequest::default();
        request.mechanism = StrBytes::from_static_str("SCRAM-SHA-256");

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = SaslHandshakeRequest::decode(&mut read_buf, version).unwrap();

        assert_eq!(
            decoded.mechanism.as_str(),
            "SCRAM-SHA-256",
            "Version {} failed",
            version
        );
    }
}

// ============================================================================
// SaslHandshake Response Tests (API Key 17, v0-v1)
// ============================================================================

/// Test SaslHandshakeResponse v0 success with supported mechanisms
#[test]
fn test_sasl_handshake_response_v0_success() {
    let mut response = SaslHandshakeResponse::default();
    response.error_code = 0;
    response.mechanisms.push(StrBytes::from_static_str("PLAIN"));
    response
        .mechanisms
        .push(StrBytes::from_static_str("SCRAM-SHA-256"));
    response
        .mechanisms
        .push(StrBytes::from_static_str("SCRAM-SHA-512"));

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslHandshakeResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.mechanisms.len(), 3);
}

/// Test SaslHandshakeResponse v0 with unsupported mechanism error
#[test]
fn test_sasl_handshake_response_v0_unsupported_mechanism() {
    let mut response = SaslHandshakeResponse::default();
    response.error_code = 33; // UNSUPPORTED_SASL_MECHANISM
    response.mechanisms.push(StrBytes::from_static_str("PLAIN"));
    response
        .mechanisms
        .push(StrBytes::from_static_str("SCRAM-SHA-256"));

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslHandshakeResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 33);
    assert_eq!(decoded.mechanisms.len(), 2);
}

/// Test SaslHandshakeResponse v0 with single mechanism
#[test]
fn test_sasl_handshake_response_v0_single_mechanism() {
    let mut response = SaslHandshakeResponse::default();
    response.error_code = 0;
    response.mechanisms.push(StrBytes::from_static_str("PLAIN"));

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslHandshakeResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.mechanisms.len(), 1);
    assert_eq!(decoded.mechanisms[0].as_str(), "PLAIN");
}

/// Test SaslHandshakeResponse v1 with all mechanisms
#[test]
fn test_sasl_handshake_response_v1_all_mechanisms() {
    let mut response = SaslHandshakeResponse::default();
    response.error_code = 0;
    response.mechanisms.push(StrBytes::from_static_str("PLAIN"));
    response
        .mechanisms
        .push(StrBytes::from_static_str("SCRAM-SHA-256"));
    response
        .mechanisms
        .push(StrBytes::from_static_str("SCRAM-SHA-512"));
    response
        .mechanisms
        .push(StrBytes::from_static_str("OAUTHBEARER"));
    response
        .mechanisms
        .push(StrBytes::from_static_str("GSSAPI"));

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslHandshakeResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.mechanisms.len(), 5);
}

/// Test SaslHandshakeResponse version roundtrip
#[test]
fn test_sasl_handshake_response_version_roundtrip() {
    for version in 0..=1 {
        let mut response = SaslHandshakeResponse::default();
        response.error_code = 0;
        response
            .mechanisms
            .push(StrBytes::from_static_str("SCRAM-SHA-256"));

        let mut buf = BytesMut::new();
        response.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = SaslHandshakeResponse::decode(&mut read_buf, version).unwrap();

        assert_eq!(decoded.error_code, 0, "Version {} failed", version);
        assert_eq!(decoded.mechanisms.len(), 1, "Version {} failed", version);
    }
}

// ============================================================================
// SaslAuthenticate Request Tests (API Key 36, v0-v2)
// ============================================================================

/// Test SaslAuthenticateRequest v0 with PLAIN auth bytes
/// PLAIN format: \0username\0password
#[test]
fn test_sasl_authenticate_request_v0_plain() {
    let mut request = SaslAuthenticateRequest::default();
    // PLAIN mechanism: \0username\0password
    let auth_bytes = b"\0testuser\0testpassword";
    request.auth_bytes = bytes::Bytes::from_static(auth_bytes);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.auth_bytes.as_ref(), auth_bytes);
}

/// Test SaslAuthenticateRequest v0 with SCRAM client-first message
#[test]
fn test_sasl_authenticate_request_v0_scram_client_first() {
    let mut request = SaslAuthenticateRequest::default();
    // SCRAM client-first message format: n,,n=user,r=nonce
    let auth_bytes = b"n,,n=testuser,r=fyko+d2lbbFgONRv9qkxdawL";
    request.auth_bytes = bytes::Bytes::from_static(auth_bytes);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.auth_bytes.as_ref(), auth_bytes);
}

/// Test SaslAuthenticateRequest v0 with SCRAM client-final message
#[test]
fn test_sasl_authenticate_request_v0_scram_client_final() {
    let mut request = SaslAuthenticateRequest::default();
    // SCRAM client-final message: c=biws,r=nonce,p=proof
    let auth_bytes =
        b"c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=";
    request.auth_bytes = bytes::Bytes::from_static(auth_bytes);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.auth_bytes.as_ref(), auth_bytes);
}

/// Test SaslAuthenticateRequest v0 with OAUTHBEARER token
#[test]
fn test_sasl_authenticate_request_v0_oauthbearer() {
    let mut request = SaslAuthenticateRequest::default();
    // OAUTHBEARER format: n,,\x01auth=Bearer <token>\x01\x01
    let auth_bytes =
        b"n,,\x01auth=Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.test.signature\x01\x01";
    request.auth_bytes = bytes::Bytes::from_static(auth_bytes);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateRequest::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.auth_bytes.as_ref(), auth_bytes);
}

/// Test SaslAuthenticateRequest v0 with empty auth bytes
#[test]
fn test_sasl_authenticate_request_v0_empty() {
    let mut request = SaslAuthenticateRequest::default();
    request.auth_bytes = bytes::Bytes::new();

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateRequest::decode(&mut read_buf, 0).unwrap();

    assert!(decoded.auth_bytes.is_empty());
}

/// Test SaslAuthenticateRequest v1 with PLAIN auth bytes
#[test]
fn test_sasl_authenticate_request_v1_plain() {
    let mut request = SaslAuthenticateRequest::default();
    let auth_bytes = b"\0admin\0admin-secret";
    request.auth_bytes = bytes::Bytes::from_static(auth_bytes);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateRequest::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.auth_bytes.as_ref(), auth_bytes);
}

/// Test SaslAuthenticateRequest v2 with flexible encoding
#[test]
fn test_sasl_authenticate_request_v2_flexible() {
    let mut request = SaslAuthenticateRequest::default();
    let auth_bytes = b"\0kafka-user\0kafka-password";
    request.auth_bytes = bytes::Bytes::from_static(auth_bytes);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.auth_bytes.as_ref(), auth_bytes);
}

/// Test SaslAuthenticateRequest version roundtrip
#[test]
fn test_sasl_authenticate_request_version_roundtrip() {
    for version in 0..=2 {
        let mut request = SaslAuthenticateRequest::default();
        let auth_bytes = b"\0user\0pass";
        request.auth_bytes = bytes::Bytes::from_static(auth_bytes);

        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = SaslAuthenticateRequest::decode(&mut read_buf, version).unwrap();

        assert_eq!(
            decoded.auth_bytes.as_ref(),
            auth_bytes,
            "Version {} failed",
            version
        );
    }
}

// ============================================================================
// SaslAuthenticate Response Tests (API Key 36, v0-v2)
// ============================================================================

/// Test SaslAuthenticateResponse v0 success
#[test]
fn test_sasl_authenticate_response_v0_success() {
    let mut response = SaslAuthenticateResponse::default();
    response.error_code = 0;
    response.auth_bytes = bytes::Bytes::new();

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 0);
}

/// Test SaslAuthenticateResponse v0 with SCRAM server-first message
#[test]
fn test_sasl_authenticate_response_v0_scram_server_first() {
    let mut response = SaslAuthenticateResponse::default();
    response.error_code = 0;
    // SCRAM server-first message: r=nonce,s=salt,i=iterations
    let auth_bytes = b"r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
    response.auth_bytes = bytes::Bytes::from_static(auth_bytes);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.auth_bytes.as_ref(), auth_bytes);
}

/// Test SaslAuthenticateResponse v0 with SCRAM server-final message
#[test]
fn test_sasl_authenticate_response_v0_scram_server_final() {
    let mut response = SaslAuthenticateResponse::default();
    response.error_code = 0;
    // SCRAM server-final message: v=signature
    let auth_bytes = b"v=rmF9pqV8S7suAoZWja4dJRkFsKQ=";
    response.auth_bytes = bytes::Bytes::from_static(auth_bytes);

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.auth_bytes.as_ref(), auth_bytes);
}

/// Test SaslAuthenticateResponse v0 with authentication failed error
#[test]
fn test_sasl_authenticate_response_v0_auth_failed() {
    let mut response = SaslAuthenticateResponse::default();
    response.error_code = 58; // SASL_AUTHENTICATION_FAILED
    response.error_message = Some(StrBytes::from_static_str("Authentication failed"));

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 0).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateResponse::decode(&mut read_buf, 0).unwrap();

    assert_eq!(decoded.error_code, 58);
}

/// Test SaslAuthenticateResponse v1 with session lifetime
#[test]
fn test_sasl_authenticate_response_v1_session_lifetime() {
    let mut response = SaslAuthenticateResponse::default();
    response.error_code = 0;
    response.auth_bytes = bytes::Bytes::new();
    response.session_lifetime_ms = 3600000; // 1 hour

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.session_lifetime_ms, 3600000);
}

/// Test SaslAuthenticateResponse v1 with zero session lifetime
#[test]
fn test_sasl_authenticate_response_v1_no_session_lifetime() {
    let mut response = SaslAuthenticateResponse::default();
    response.error_code = 0;
    response.auth_bytes = bytes::Bytes::new();
    response.session_lifetime_ms = 0;

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 1).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateResponse::decode(&mut read_buf, 1).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.session_lifetime_ms, 0);
}

/// Test SaslAuthenticateResponse v2 flexible encoding
#[test]
fn test_sasl_authenticate_response_v2_flexible() {
    let mut response = SaslAuthenticateResponse::default();
    response.error_code = 0;
    response.auth_bytes = bytes::Bytes::from_static(b"server-response-data");
    response.session_lifetime_ms = 7200000; // 2 hours

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateResponse::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.error_code, 0);
    assert_eq!(decoded.session_lifetime_ms, 7200000);
}

/// Test SaslAuthenticateResponse version roundtrip
#[test]
fn test_sasl_authenticate_response_version_roundtrip() {
    for version in 0..=2 {
        let mut response = SaslAuthenticateResponse::default();
        response.error_code = 0;
        response.auth_bytes = bytes::Bytes::from_static(b"test-auth-bytes");
        if version >= 1 {
            response.session_lifetime_ms = 1800000; // 30 minutes
        }

        let mut buf = BytesMut::new();
        response.encode(&mut buf, version).unwrap();

        let mut read_buf = buf.freeze();
        let decoded = SaslAuthenticateResponse::decode(&mut read_buf, version).unwrap();

        assert_eq!(decoded.error_code, 0, "Version {} failed", version);
    }
}

// ============================================================================
// SASL Mechanism-Specific Tests
// ============================================================================

/// Test complete PLAIN authentication flow encoding
#[test]
fn test_plain_auth_flow_encoding() {
    // Step 1: Handshake request
    let mut handshake_req = SaslHandshakeRequest::default();
    handshake_req.mechanism = StrBytes::from_static_str("PLAIN");

    let mut buf = BytesMut::new();
    handshake_req.encode(&mut buf, 1).unwrap();
    assert!(!buf.is_empty());

    // Step 2: Handshake response
    let mut handshake_resp = SaslHandshakeResponse::default();
    handshake_resp.error_code = 0;
    handshake_resp
        .mechanisms
        .push(StrBytes::from_static_str("PLAIN"));

    let mut buf = BytesMut::new();
    handshake_resp.encode(&mut buf, 1).unwrap();
    assert!(!buf.is_empty());

    // Step 3: Authenticate request
    let mut auth_req = SaslAuthenticateRequest::default();
    auth_req.auth_bytes = bytes::Bytes::from_static(b"\0user\0password");

    let mut buf = BytesMut::new();
    auth_req.encode(&mut buf, 2).unwrap();
    assert!(!buf.is_empty());

    // Step 4: Authenticate response
    let mut auth_resp = SaslAuthenticateResponse::default();
    auth_resp.error_code = 0;

    let mut buf = BytesMut::new();
    auth_resp.encode(&mut buf, 2).unwrap();
    assert!(!buf.is_empty());
}

/// Test complete SCRAM-SHA-256 authentication flow encoding
#[test]
fn test_scram_sha256_auth_flow_encoding() {
    // Step 1: Handshake
    let mut handshake_req = SaslHandshakeRequest::default();
    handshake_req.mechanism = StrBytes::from_static_str("SCRAM-SHA-256");

    let mut buf = BytesMut::new();
    handshake_req.encode(&mut buf, 1).unwrap();
    assert!(!buf.is_empty());

    // Step 2: Client-first message
    let mut auth_req1 = SaslAuthenticateRequest::default();
    auth_req1.auth_bytes = bytes::Bytes::from_static(b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO");

    let mut buf = BytesMut::new();
    auth_req1.encode(&mut buf, 2).unwrap();
    assert!(!buf.is_empty());

    // Step 3: Server-first response
    let mut auth_resp1 = SaslAuthenticateResponse::default();
    auth_resp1.error_code = 0;
    auth_resp1.auth_bytes = bytes::Bytes::from_static(
        b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096",
    );

    let mut buf = BytesMut::new();
    auth_resp1.encode(&mut buf, 2).unwrap();
    assert!(!buf.is_empty());

    // Step 4: Client-final message
    let mut auth_req2 = SaslAuthenticateRequest::default();
    auth_req2.auth_bytes = bytes::Bytes::from_static(
        b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
    );

    let mut buf = BytesMut::new();
    auth_req2.encode(&mut buf, 2).unwrap();
    assert!(!buf.is_empty());

    // Step 5: Server-final response
    let mut auth_resp2 = SaslAuthenticateResponse::default();
    auth_resp2.error_code = 0;
    auth_resp2.auth_bytes =
        bytes::Bytes::from_static(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=");

    let mut buf = BytesMut::new();
    auth_resp2.encode(&mut buf, 2).unwrap();
    assert!(!buf.is_empty());
}

/// Test PLAIN mechanism with authzid (authorization identity)
#[test]
fn test_plain_auth_with_authzid() {
    let mut request = SaslAuthenticateRequest::default();
    // PLAIN format with authzid: authzid\0authn\0password
    let auth_bytes = b"admin-user\0proxy-user\0proxy-password";
    request.auth_bytes = bytes::Bytes::from_static(auth_bytes);

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.auth_bytes.as_ref(), auth_bytes);
}

/// Test OAUTHBEARER error response
#[test]
fn test_oauthbearer_error_response() {
    let mut response = SaslAuthenticateResponse::default();
    response.error_code = 58; // SASL_AUTHENTICATION_FAILED
    response.error_message = Some(StrBytes::from_static_str(
        "{\"status\":\"invalid_token\",\"scope\":\"kafka\",\"openid-configuration\":\"https://auth.example.com/.well-known/openid-configuration\"}",
    ));

    let mut buf = BytesMut::new();
    response.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateResponse::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.error_code, 58);
    assert!(decoded.error_message.is_some());
}

/// Test large auth bytes handling
#[test]
fn test_large_auth_bytes() {
    let mut request = SaslAuthenticateRequest::default();
    // Create a large OAuth token (common in enterprise environments)
    let large_token = vec![b'A'; 8192];
    request.auth_bytes = bytes::Bytes::from(large_token.clone());

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.auth_bytes.len(), 8192);
}

/// Test binary auth bytes (non-UTF8)
#[test]
fn test_binary_auth_bytes() {
    let mut request = SaslAuthenticateRequest::default();
    // Binary data that's not valid UTF-8
    let binary_data: Vec<u8> = (0u8..=255u8).collect();
    request.auth_bytes = bytes::Bytes::from(binary_data.clone());

    let mut buf = BytesMut::new();
    request.encode(&mut buf, 2).unwrap();

    let mut read_buf = buf.freeze();
    let decoded = SaslAuthenticateRequest::decode(&mut read_buf, 2).unwrap();

    assert_eq!(decoded.auth_bytes.as_ref(), binary_data.as_slice());
}
