//! TLS configuration and utilities for secure connections

use crate::config::TlsConfig;
use crate::error::{Result, StreamlineError};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::version::{TLS12, TLS13};
use rustls::{ServerConfig as RustlsServerConfig, SupportedProtocolVersion};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::TlsAcceptor;
use tracing::info;

/// Get supported TLS versions based on minimum version configuration
fn get_supported_versions(min_version: &str) -> Vec<&'static SupportedProtocolVersion> {
    match min_version {
        "1.3" => vec![&TLS13],
        _ => vec![&TLS12, &TLS13], // "1.2" or default
    }
}

/// Load TLS configuration and create a TlsAcceptor
pub fn load_tls_config(config: &TlsConfig) -> Result<TlsAcceptor> {
    if !config.enabled {
        return Err(StreamlineError::Config("TLS not enabled".to_string()));
    }

    info!("Loading TLS configuration");

    // Load certificates
    let certs = load_certs(&config.cert_path)?;
    info!(
        cert_path = %config.cert_path.display(),
        cert_count = certs.len(),
        "Loaded TLS certificates"
    );

    // Load private key
    let key = load_private_key(&config.key_path)?;
    info!(
        key_path = %config.key_path.display(),
        "Loaded TLS private key"
    );

    // Validate minimum TLS version
    if config.min_version != "1.2" && config.min_version != "1.3" {
        return Err(StreamlineError::Config(format!(
            "Invalid TLS minimum version: {}. Must be '1.2' or '1.3'",
            config.min_version
        )));
    }

    // Build rustls config with enforced TLS version
    let versions = get_supported_versions(&config.min_version);
    let rustls_config = RustlsServerConfig::builder_with_protocol_versions(&versions)
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| StreamlineError::Config(format!("Invalid TLS configuration: {}", e)))?;

    info!(
        min_version = %config.min_version,
        "TLS configuration loaded successfully"
    );

    Ok(TlsAcceptor::from(Arc::new(rustls_config)))
}

/// Load TLS configuration with client authentication (mTLS)
pub fn load_mtls_config(config: &TlsConfig) -> Result<TlsAcceptor> {
    if !config.enabled {
        return Err(StreamlineError::Config("TLS not enabled".to_string()));
    }

    if !config.require_client_cert {
        return load_tls_config(config);
    }

    info!("Loading mTLS configuration");

    // Load certificates
    let certs = load_certs(&config.cert_path)?;
    info!(
        cert_path = %config.cert_path.display(),
        cert_count = certs.len(),
        "Loaded TLS certificates"
    );

    // Load private key
    let key = load_private_key(&config.key_path)?;
    info!(
        key_path = %config.key_path.display(),
        "Loaded TLS private key"
    );

    // Load CA certificates for client verification
    let ca_cert_path = config.ca_cert_path.as_ref().ok_or_else(|| {
        StreamlineError::Config(
            "Client certificate verification enabled but CA cert path not provided".to_string(),
        )
    })?;

    let ca_certs = load_certs(ca_cert_path)?;
    info!(
        ca_cert_path = %ca_cert_path.display(),
        ca_cert_count = ca_certs.len(),
        "Loaded CA certificates"
    );

    // Create certificate verifier
    let mut root_store = rustls::RootCertStore::empty();
    for cert in ca_certs {
        root_store
            .add(cert)
            .map_err(|e| StreamlineError::Config(format!("Failed to add CA certificate: {}", e)))?;
    }

    let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
        .build()
        .map_err(|e| StreamlineError::Config(format!("Failed to build client verifier: {}", e)))?;

    // Build rustls config with client auth and enforced TLS version
    let versions = get_supported_versions(&config.min_version);
    let rustls_config = RustlsServerConfig::builder_with_protocol_versions(&versions)
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(certs, key)
        .map_err(|e| StreamlineError::Config(format!("Invalid mTLS configuration: {}", e)))?;

    info!(
        min_version = %config.min_version,
        "mTLS configuration loaded successfully"
    );

    Ok(TlsAcceptor::from(Arc::new(rustls_config)))
}

/// Load certificates from a PEM file
fn load_certs(path: &std::path::Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path)
        .map_err(|e| StreamlineError::Config(format!("Failed to open certificate file: {}", e)))?;

    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| StreamlineError::Config(format!("Failed to parse certificates: {}", e)))?;

    if certs.is_empty() {
        return Err(StreamlineError::Config(
            "No certificates found in file".to_string(),
        ));
    }

    Ok(certs)
}

/// Load private key from a PEM file
fn load_private_key(path: &std::path::Path) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(path)
        .map_err(|e| StreamlineError::Config(format!("Failed to open private key file: {}", e)))?;

    let mut reader = BufReader::new(file);

    // Try to read as PKCS8 first, then RSA, then EC
    let keys = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| StreamlineError::Config(format!("Failed to parse private key: {}", e)))?
        .ok_or_else(|| StreamlineError::Config("No private key found in file".to_string()))?;

    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Test certificate (self-signed, for testing only)
    const TEST_CERT: &str = r#"-----BEGIN CERTIFICATE-----
MIICljCCAX4CCQCKz8Qh3jPZQDANBgkqhkiG9w0BAQsFADANMQswCQYDVQQGEwJV
UzAeFw0yNDEyMDkxMjAwMDBaFw0yNTEyMDkxMjAwMDBaMA0xCzAJBgNVBAYTAlVT
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAvOSwBbfKRZCQN3NqYKLV
I/vwzFwC6pjJN+LL8rMLCMqYbr8qN9OYJ2+rL1TqJfG2KgD7pZhFLh8EoJEXlrBr
ZRNEGnZ3r1cGvGpSMvQDaWoGbOBmxPZv3yDxLpQJVk5wN1ER7YNnN8QPmWQvVJ1Y
7cQbGPV7hZTN7RqNlXZHfGdQ6mF7jDKjHfQRxNxZNlO1LNDdN+vLQqwMjL8mZlGS
KXcBGfQN7yzFTL8c3JNHnQk5eqKoNQ+MhOPqO5F7pE2vLQ1RQWF7nPQTmFc2+Qnq
EHQqN8F7pE2vLQ1RQWF7nPQTmFc2+QnqEHQqN8F7pE2vLQ1RQWF7nPQTmFc2+Qnq
EwIDAQABMA0GCSqGSIb3DQEBCwUAA4IBAQCqJ+J0vVKGn1F2VwPJN8qF2vBQ7Q1a
qJ+J0vVKGn1F2VwPJN8qF2vBQ7Q1aqJ+J0vVKGn1F2VwPJN8qF2vBQ7Q1aqJ+J0v
VKGn1F2VwPJN8qF2vBQ7Q1aqJ+J0vVKGn1F2VwPJN8qF2vBQ7Q1aqJ+J0vVKGn1F
2VwPJN8qF2vBQ7Q1aqJ+J0vVKGn1F2VwPJN8qF2vBQ7Q1aqJ+J0vVKGn1F2VwPJ
N8qF2vBQ7Q1aqJ+J0vVKGn1F2VwPJN8qF2vBQ7Q1aqJ+J0vVKGn1F2VwPJN8qF2
vBQ7Q1aqJ+J0vVKGn1F2VwPJN8qF2vBQ7Q1a
-----END CERTIFICATE-----"#;

    const TEST_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
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
1a
-----END PRIVATE KEY-----"#;

    #[test]
    fn test_load_certs() {
        let mut cert_file = NamedTempFile::new().unwrap();
        cert_file.write_all(TEST_CERT.as_bytes()).unwrap();
        cert_file.flush().unwrap();

        let certs = load_certs(cert_file.path()).unwrap();
        assert!(!certs.is_empty());
    }

    #[test]
    fn test_load_private_key() {
        let mut key_file = NamedTempFile::new().unwrap();
        key_file.write_all(TEST_KEY.as_bytes()).unwrap();
        key_file.flush().unwrap();

        let key = load_private_key(key_file.path());
        assert!(key.is_ok());
    }

    #[test]
    fn test_load_certs_empty_file() {
        let cert_file = NamedTempFile::new().unwrap();
        let result = load_certs(cert_file.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_load_private_key_empty_file() {
        let key_file = NamedTempFile::new().unwrap();
        let result = load_private_key(key_file.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_tls_config_default() {
        let config = TlsConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.min_version, "1.2");
        assert!(!config.require_client_cert);
    }
}
