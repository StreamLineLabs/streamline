//! TLS utilities for inter-broker communication
//!
//! This module provides TLS configuration and utilities for secure communication
//! between cluster nodes, including both server (accepting connections) and
//! client (initiating connections) TLS contexts.

use crate::cluster::config::InterBrokerTlsConfig;
use crate::error::{Result, StreamlineError};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::version::{TLS12, TLS13};
use rustls::ClientConfig as RustlsClientConfig;
use rustls::ServerConfig as RustlsServerConfig;
use rustls::SupportedProtocolVersion;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{debug, info};

/// Get supported TLS versions based on minimum version configuration
fn get_supported_versions(min_version: &str) -> Vec<&'static SupportedProtocolVersion> {
    match min_version {
        "1.3" => vec![&TLS13],
        _ => vec![&TLS12, &TLS13], // "1.2" or default
    }
}

/// TLS context for inter-broker communication
///
/// Provides both server (acceptor) and client (connector) TLS contexts
/// for establishing secure connections between cluster nodes.
#[derive(Clone)]
pub struct InterBrokerTls {
    /// TLS acceptor for incoming connections
    acceptor: TlsAcceptor,
    /// TLS connector for outgoing connections
    connector: TlsConnector,
    /// Whether peer verification is enabled
    verify_peer: bool,
}

impl std::fmt::Debug for InterBrokerTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InterBrokerTls")
            .field("verify_peer", &self.verify_peer)
            .finish_non_exhaustive()
    }
}

impl InterBrokerTls {
    /// Create a new inter-broker TLS context from configuration
    pub fn new(config: &InterBrokerTlsConfig) -> Result<Self> {
        if !config.enabled {
            return Err(StreamlineError::Config(
                "Inter-broker TLS not enabled".to_string(),
            ));
        }

        info!("Loading inter-broker TLS configuration");

        // Load this node's certificate and key
        let cert_path = config.cert_path.as_ref().ok_or_else(|| {
            StreamlineError::Config("Inter-broker TLS cert_path not provided".to_string())
        })?;
        let key_path = config.key_path.as_ref().ok_or_else(|| {
            StreamlineError::Config("Inter-broker TLS key_path not provided".to_string())
        })?;

        let certs = load_certs(cert_path)?;
        debug!(
            cert_path = %cert_path.display(),
            cert_count = certs.len(),
            "Loaded inter-broker certificates"
        );

        let key = load_private_key(key_path)?;
        debug!(
            key_path = %key_path.display(),
            "Loaded inter-broker private key"
        );

        // Build server (acceptor) config
        let server_config = if config.verify_peer {
            // mTLS: Verify client certificates
            let ca_cert_path = config.ca_cert_path.as_ref().ok_or_else(|| {
                StreamlineError::Config(
                    "Inter-broker TLS peer verification enabled but ca_cert_path not provided"
                        .to_string(),
                )
            })?;

            let ca_certs = load_certs(ca_cert_path)?;
            debug!(
                ca_cert_path = %ca_cert_path.display(),
                ca_cert_count = ca_certs.len(),
                "Loaded inter-broker CA certificates"
            );

            let mut root_store = rustls::RootCertStore::empty();
            for cert in &ca_certs {
                root_store.add(cert.clone()).map_err(|e| {
                    StreamlineError::Config(format!(
                        "Failed to add inter-broker CA certificate: {}",
                        e
                    ))
                })?;
            }

            let client_verifier =
                rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store.clone()))
                    .build()
                    .map_err(|e| {
                        StreamlineError::Config(format!(
                            "Failed to build inter-broker client verifier: {}",
                            e
                        ))
                    })?;

            let versions = get_supported_versions(&config.min_version);
            RustlsServerConfig::builder_with_protocol_versions(&versions)
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(certs.clone(), key.clone_key())
                .map_err(|e| {
                    StreamlineError::Config(format!(
                        "Invalid inter-broker mTLS server config: {}",
                        e
                    ))
                })?
        } else {
            // No client auth
            let versions = get_supported_versions(&config.min_version);
            RustlsServerConfig::builder_with_protocol_versions(&versions)
                .with_no_client_auth()
                .with_single_cert(certs.clone(), key.clone_key())
                .map_err(|e| {
                    StreamlineError::Config(format!(
                        "Invalid inter-broker TLS server config: {}",
                        e
                    ))
                })?
        };

        // Build client (connector) config
        let client_config = if config.verify_peer {
            let ca_cert_path = config.ca_cert_path.as_ref().ok_or_else(|| {
                StreamlineError::Config(
                    "Inter-broker TLS peer verification enabled but ca_cert_path not provided"
                        .to_string(),
                )
            })?;

            let ca_certs = load_certs(ca_cert_path)?;

            let mut root_store = rustls::RootCertStore::empty();
            for cert in ca_certs {
                root_store.add(cert).map_err(|e| {
                    StreamlineError::Config(format!(
                        "Failed to add inter-broker CA certificate: {}",
                        e
                    ))
                })?;
            }

            // Use mTLS - provide client certificate
            let versions = get_supported_versions(&config.min_version);
            RustlsClientConfig::builder_with_protocol_versions(&versions)
                .with_root_certificates(root_store)
                .with_client_auth_cert(certs, key)
                .map_err(|e| {
                    StreamlineError::Config(format!(
                        "Invalid inter-broker mTLS client config: {}",
                        e
                    ))
                })?
        } else {
            // No server verification (for testing/development)
            // Use dangerous verifier that accepts any certificate
            let verifier = NoServerVerification;
            let versions = get_supported_versions(&config.min_version);
            RustlsClientConfig::builder_with_protocol_versions(&versions)
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_client_auth_cert(certs, key)
                .map_err(|e| {
                    StreamlineError::Config(format!(
                        "Invalid inter-broker TLS client config (no verify): {}",
                        e
                    ))
                })?
        };

        info!(
            verify_peer = config.verify_peer,
            min_version = %config.min_version,
            "Inter-broker TLS configuration loaded"
        );

        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(server_config)),
            connector: TlsConnector::from(Arc::new(client_config)),
            verify_peer: config.verify_peer,
        })
    }

    /// Accept an incoming TLS connection
    pub async fn accept(&self, stream: TcpStream) -> Result<InterBrokerTlsStream<TcpStream>> {
        let tls_stream = self
            .acceptor
            .accept(stream)
            .await
            .map_err(|e| StreamlineError::Server(format!("TLS accept failed: {}", e)))?;

        Ok(InterBrokerTlsStream::Server(tls_stream))
    }

    /// Accept an incoming TLS connection and return the server stream directly
    pub async fn accept_server(
        &self,
        stream: TcpStream,
    ) -> Result<tokio_rustls::server::TlsStream<TcpStream>> {
        self.acceptor
            .accept(stream)
            .await
            .map_err(|e| StreamlineError::Server(format!("TLS accept failed: {}", e)))
    }

    /// Connect to a remote node with TLS
    pub async fn connect(
        &self,
        stream: TcpStream,
        addr: &SocketAddr,
    ) -> Result<tokio_rustls::client::TlsStream<TcpStream>> {
        // For inter-broker communication, we use the IP address as the server name
        // In production, this would be configured per-node or use DNS names
        let server_name = ServerName::try_from(addr.ip().to_string()).map_err(|e| {
            StreamlineError::Server(format!("Invalid server name from address {}: {}", addr, e))
        })?;

        self.connector
            .connect(server_name, stream)
            .await
            .map_err(|e| StreamlineError::Server(format!("TLS connect failed: {}", e)))
    }

    /// Get the TLS acceptor for use with external code
    pub fn acceptor(&self) -> &TlsAcceptor {
        &self.acceptor
    }

    /// Get the TLS connector for use with external code
    pub fn connector(&self) -> &TlsConnector {
        &self.connector
    }

    /// Check if peer verification is enabled
    pub fn verify_peer(&self) -> bool {
        self.verify_peer
    }
}

/// A TLS stream that can be either client or server
pub enum InterBrokerTlsStream<T> {
    Server(tokio_rustls::server::TlsStream<T>),
    Client(tokio_rustls::client::TlsStream<T>),
}

impl<T: AsyncRead + AsyncWrite + Unpin> AsyncRead for InterBrokerTlsStream<T> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            InterBrokerTlsStream::Server(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            InterBrokerTlsStream::Client(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl<T: AsyncRead + AsyncWrite + Unpin> AsyncWrite for InterBrokerTlsStream<T> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            InterBrokerTlsStream::Server(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            InterBrokerTlsStream::Client(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            InterBrokerTlsStream::Server(s) => std::pin::Pin::new(s).poll_flush(cx),
            InterBrokerTlsStream::Client(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            InterBrokerTlsStream::Server(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            InterBrokerTlsStream::Client(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Certificate verifier that accepts any certificate (for development/testing)
#[derive(Debug)]
struct NoServerVerification;

impl rustls::client::danger::ServerCertVerifier for NoServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // Accept any certificate
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::ECDSA_NISTP521_SHA512,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

/// Load certificates from a PEM file
fn load_certs(path: &std::path::Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path).map_err(|e| {
        StreamlineError::Config(format!(
            "Failed to open inter-broker certificate file {}: {}",
            path.display(),
            e
        ))
    })?;

    let mut reader = BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| {
            StreamlineError::Config(format!(
                "Failed to parse inter-broker certificates from {}: {}",
                path.display(),
                e
            ))
        })?;

    if certs.is_empty() {
        return Err(StreamlineError::Config(format!(
            "No certificates found in {}",
            path.display()
        )));
    }

    Ok(certs)
}

/// Load private key from a PEM file
fn load_private_key(path: &std::path::Path) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(path).map_err(|e| {
        StreamlineError::Config(format!(
            "Failed to open inter-broker private key file {}: {}",
            path.display(),
            e
        ))
    })?;

    let mut reader = BufReader::new(file);

    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| {
            StreamlineError::Config(format!(
                "Failed to parse inter-broker private key from {}: {}",
                path.display(),
                e
            ))
        })?
        .ok_or_else(|| {
            StreamlineError::Config(format!("No private key found in {}", path.display()))
        })?;

    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Test certificate and key for testing (self-signed)
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

    fn create_test_files() -> (NamedTempFile, NamedTempFile, NamedTempFile) {
        let mut cert_file = NamedTempFile::new().unwrap();
        cert_file.write_all(TEST_CERT.as_bytes()).unwrap();
        cert_file.flush().unwrap();

        let mut key_file = NamedTempFile::new().unwrap();
        key_file.write_all(TEST_KEY.as_bytes()).unwrap();
        key_file.flush().unwrap();

        let mut ca_file = NamedTempFile::new().unwrap();
        ca_file.write_all(TEST_CERT.as_bytes()).unwrap(); // Use same cert as CA for testing
        ca_file.flush().unwrap();

        (cert_file, key_file, ca_file)
    }

    #[test]
    fn test_inter_broker_tls_disabled() {
        let config = InterBrokerTlsConfig::default();
        assert!(!config.enabled);

        let result = InterBrokerTls::new(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_inter_broker_tls_missing_cert() {
        let config = InterBrokerTlsConfig {
            enabled: true,
            cert_path: None,
            key_path: Some(std::path::PathBuf::from("/tmp/key.pem")),
            ca_cert_path: None,
            verify_peer: false,
            min_version: "1.2".to_string(),
        };

        let result = InterBrokerTls::new(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("cert_path"));
    }

    #[test]
    fn test_inter_broker_tls_missing_key() {
        let (cert_file, _, _) = create_test_files();

        let config = InterBrokerTlsConfig {
            enabled: true,
            cert_path: Some(cert_file.path().to_path_buf()),
            key_path: None,
            ca_cert_path: None,
            verify_peer: false,
            min_version: "1.2".to_string(),
        };

        let result = InterBrokerTls::new(&config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("key_path"));
    }

    #[test]
    fn test_load_certs_success() {
        let (cert_file, _, _) = create_test_files();
        let certs = load_certs(cert_file.path()).unwrap();
        assert!(!certs.is_empty());
    }

    #[test]
    fn test_load_certs_empty_file() {
        let empty_file = NamedTempFile::new().unwrap();
        let result = load_certs(empty_file.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_load_private_key_success() {
        let (_, key_file, _) = create_test_files();
        let key = load_private_key(key_file.path());
        assert!(key.is_ok());
    }

    #[test]
    fn test_load_private_key_empty_file() {
        let empty_file = NamedTempFile::new().unwrap();
        let result = load_private_key(empty_file.path());
        assert!(result.is_err());
    }
}
