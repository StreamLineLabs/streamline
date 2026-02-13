//! QUIC transport implementation
//!
//! Provides QUIC transport for Streamline using the quinn library.
//! Features:
//! - Connection migration
//! - 0-RTT connection resumption
//! - Multiplexed streams without head-of-line blocking
//! - Built-in TLS 1.3 encryption

use super::ConnectionStats;
use crate::error::{Result, StreamlineError};
use parking_lot::RwLock;
use quinn::{
    ClientConfig, Connection, Endpoint, RecvStream, SendStream, ServerConfig, TransportConfig,
};
use rcgen::{CertificateParams, KeyPair};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

/// QUIC server configuration
#[derive(Debug, Clone)]
pub struct QuicServerConfig {
    /// Address to bind to
    pub bind_addr: SocketAddr,
    /// Maximum concurrent connections
    pub max_connections: u32,
    /// Maximum concurrent bidirectional streams per connection
    pub max_bi_streams: u32,
    /// Maximum concurrent unidirectional streams per connection
    pub max_uni_streams: u32,
    /// Idle timeout in milliseconds
    pub idle_timeout_ms: u64,
    /// Keep-alive interval in milliseconds (0 to disable)
    pub keep_alive_interval_ms: u64,
    /// Initial RTT estimate in milliseconds
    pub initial_rtt_ms: u64,
    /// Maximum receive window
    pub receive_window: u64,
    /// Maximum stream receive window
    pub stream_receive_window: u64,
    /// Enable 0-RTT
    pub enable_0rtt: bool,
    /// Custom certificate PEM (if None, generates self-signed)
    pub cert_pem: Option<String>,
    /// Custom private key PEM (if None, generates self-signed)
    pub key_pem: Option<String>,
    /// Server name for certificate generation
    pub server_name: String,
}

impl Default for QuicServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from(([0, 0, 0, 0], 9093)),
            max_connections: 10000,
            max_bi_streams: 100,
            max_uni_streams: 100,
            idle_timeout_ms: 30000,
            keep_alive_interval_ms: 5000,
            initial_rtt_ms: 100,
            receive_window: 16 * 1024 * 1024,   // 16 MB
            stream_receive_window: 1024 * 1024, // 1 MB
            enable_0rtt: true,
            cert_pem: None,
            key_pem: None,
            server_name: "streamline".to_string(),
        }
    }
}

/// QUIC client configuration
#[derive(Debug, Clone)]
pub struct QuicClientConfig {
    /// Server address to connect to
    pub server_addr: SocketAddr,
    /// Server name for TLS verification
    pub server_name: String,
    /// Skip certificate verification (for self-signed certs)
    pub skip_verification: bool,
    /// Idle timeout in milliseconds
    pub idle_timeout_ms: u64,
    /// Keep-alive interval in milliseconds
    pub keep_alive_interval_ms: u64,
    /// Enable 0-RTT
    pub enable_0rtt: bool,
}

impl Default for QuicClientConfig {
    fn default() -> Self {
        Self {
            server_addr: SocketAddr::from(([127, 0, 0, 1], 9093)),
            server_name: "streamline".to_string(),
            skip_verification: true,
            idle_timeout_ms: 30000,
            keep_alive_interval_ms: 5000,
            enable_0rtt: true,
        }
    }
}

/// QUIC server
#[allow(dead_code)]
pub struct QuicServer {
    /// Quinn endpoint
    endpoint: Endpoint,
    /// Configuration
    config: QuicServerConfig,
    /// Running flag
    running: AtomicBool,
    /// Connection count
    connection_count: AtomicU64,
    /// Server metrics
    metrics: RwLock<QuicServerMetrics>,
}

/// QUIC server metrics
#[derive(Debug, Clone, Default)]
pub struct QuicServerMetrics {
    /// Total connections accepted
    pub connections_accepted: u64,
    /// Active connections
    pub active_connections: u64,
    /// Total bytes received
    pub bytes_received: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Connection errors
    pub connection_errors: u64,
}

impl QuicServer {
    /// Create a new QUIC server
    pub fn new(config: QuicServerConfig) -> Result<Self> {
        let server_config = Self::build_server_config(&config)?;

        let endpoint = Endpoint::server(server_config, config.bind_addr).map_err(|e| {
            StreamlineError::Config(format!("Failed to create QUIC endpoint: {}", e))
        })?;

        info!("QUIC server listening on {}", config.bind_addr);

        Ok(Self {
            endpoint,
            config,
            running: AtomicBool::new(true),
            connection_count: AtomicU64::new(0),
            metrics: RwLock::new(QuicServerMetrics::default()),
        })
    }

    /// Build server configuration
    fn build_server_config(config: &QuicServerConfig) -> Result<ServerConfig> {
        // Generate or load certificate
        let (cert_der, key_der) =
            if let (Some(cert_pem), Some(key_pem)) = (&config.cert_pem, &config.key_pem) {
                // Parse provided certificates
                let cert_chain = rustls_pemfile::certs(&mut cert_pem.as_bytes())
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| StreamlineError::Config(format!("Invalid certificate: {}", e)))?;
                let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())
                    .map_err(|e| StreamlineError::Config(format!("Invalid private key: {}", e)))?
                    .ok_or_else(|| StreamlineError::Config("No private key found".to_string()))?;
                (cert_chain, key)
            } else {
                // Generate self-signed certificate
                let key_pair = KeyPair::generate().map_err(|e| {
                    StreamlineError::Config(format!("Failed to generate key pair: {}", e))
                })?;

                let mut params =
                    CertificateParams::new(vec![config.server_name.clone()]).map_err(|e| {
                        StreamlineError::Config(format!("Failed to create cert params: {}", e))
                    })?;
                params.distinguished_name.push(
                    rcgen::DnType::CommonName,
                    rcgen::DnValue::Utf8String(config.server_name.clone()),
                );

                let cert = params.self_signed(&key_pair).map_err(|e| {
                    StreamlineError::Config(format!("Failed to generate certificate: {}", e))
                })?;

                let cert_der = vec![cert.der().clone()];
                let key_der = rustls::pki_types::PrivatePkcs8KeyDer::from(key_pair.serialize_der());

                (cert_der, rustls::pki_types::PrivateKeyDer::Pkcs8(key_der))
            };

        // Build rustls config
        let rustls_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_der, key_der)
            .map_err(|e| StreamlineError::Config(format!("Failed to build TLS config: {}", e)))?;

        let mut server_config = ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(rustls_config).map_err(|e| {
                StreamlineError::Config(format!("Failed to create QUIC crypto config: {}", e))
            })?,
        ));

        // Configure transport
        let mut transport = TransportConfig::default();
        transport.max_concurrent_bidi_streams(config.max_bi_streams.into());
        transport.max_concurrent_uni_streams(config.max_uni_streams.into());
        transport.max_idle_timeout(Some(
            Duration::from_millis(config.idle_timeout_ms)
                .try_into()
                .map_err(|e| StreamlineError::Config(format!("Invalid idle timeout: {}", e)))?,
        ));
        if config.keep_alive_interval_ms > 0 {
            transport
                .keep_alive_interval(Some(Duration::from_millis(config.keep_alive_interval_ms)));
        }
        transport.initial_rtt(Duration::from_millis(config.initial_rtt_ms));
        transport.receive_window(
            config
                .receive_window
                .try_into()
                .map_err(|e| StreamlineError::Config(format!("Invalid receive window: {}", e)))?,
        );
        transport.stream_receive_window(
            config
                .stream_receive_window
                .try_into()
                .map_err(|e| {
                    StreamlineError::Config(format!("Invalid stream receive window: {}", e))
                })?,
        );

        server_config.transport_config(Arc::new(transport));

        Ok(server_config)
    }

    /// Accept a connection
    pub async fn accept(&self) -> Result<QuicConnection> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(StreamlineError::Config("Server is not running".to_string()));
        }

        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| StreamlineError::Config("Endpoint closed".to_string()))?;

        let connection = incoming.await.map_err(|e| {
            self.metrics.write().connection_errors += 1;
            StreamlineError::protocol_msg(format!("Connection failed: {}", e))
        })?;

        self.connection_count.fetch_add(1, Ordering::Relaxed);
        self.metrics.write().connections_accepted += 1;
        self.metrics.write().active_connections += 1;

        info!(
            "Accepted QUIC connection from {}",
            connection.remote_address()
        );

        Ok(QuicConnection::new(connection))
    }

    /// Get the local address
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.endpoint.local_addr().ok()
    }

    /// Get server metrics
    pub fn metrics(&self) -> QuicServerMetrics {
        self.metrics.read().clone()
    }

    /// Shutdown the server
    pub async fn shutdown(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        self.endpoint.close(0u32.into(), b"server shutdown");
        info!("QUIC server shutdown");
        Ok(())
    }
}

/// QUIC connection wrapper
#[allow(dead_code)]
pub struct QuicConnection {
    /// Quinn connection
    connection: Connection,
    /// Bytes sent counter
    bytes_sent: AtomicU64,
    /// Bytes received counter
    bytes_received: AtomicU64,
    /// Closed flag
    closed: AtomicBool,
}

impl QuicConnection {
    /// Create a new QUIC connection
    pub fn new(connection: Connection) -> Self {
        Self {
            connection,
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        }
    }

    /// Open a new bidirectional stream
    pub async fn open_bi(&self) -> Result<QuicBiStream> {
        let (send, recv) =
            self.connection.open_bi().await.map_err(|e| {
                StreamlineError::protocol_msg(format!("Failed to open stream: {}", e))
            })?;
        Ok(QuicBiStream::new(send, recv))
    }

    /// Accept an incoming bidirectional stream
    pub async fn accept_bi(&self) -> Result<QuicBiStream> {
        let (send, recv) = self.connection.accept_bi().await.map_err(|e| {
            StreamlineError::protocol_msg(format!("Failed to accept stream: {}", e))
        })?;
        Ok(QuicBiStream::new(send, recv))
    }

    /// Open a new unidirectional send stream
    pub async fn open_uni(&self) -> Result<QuicSendStream> {
        let send = self.connection.open_uni().await.map_err(|e| {
            StreamlineError::protocol_msg(format!("Failed to open uni stream: {}", e))
        })?;
        Ok(QuicSendStream::new(send))
    }

    /// Accept an incoming unidirectional receive stream
    pub async fn accept_uni(&self) -> Result<QuicRecvStream> {
        let recv = self.connection.accept_uni().await.map_err(|e| {
            StreamlineError::protocol_msg(format!("Failed to accept uni stream: {}", e))
        })?;
        Ok(QuicRecvStream::new(recv))
    }

    /// Get remote address
    pub fn remote_addr(&self) -> SocketAddr {
        self.connection.remote_address()
    }

    /// Get connection statistics
    pub fn stats(&self) -> ConnectionStats {
        let quinn_stats = self.connection.stats();
        ConnectionStats {
            bytes_sent: quinn_stats.udp_tx.bytes,
            bytes_received: quinn_stats.udp_rx.bytes,
            packets_sent: quinn_stats.udp_tx.datagrams,
            packets_received: quinn_stats.udp_rx.datagrams,
            rtt_us: self
                .connection
                .rtt()
                .as_micros()
                .try_into()
                .unwrap_or(u64::MAX),
            cwnd: quinn_stats.path.cwnd,
            lost_packets: quinn_stats.path.lost_packets,
            retransmissions: 0, // Not directly available
        }
    }

    /// Close the connection
    pub fn close(&self, code: u32, reason: &[u8]) {
        self.connection.close(code.into(), reason);
        self.closed.store(true, Ordering::Relaxed);
    }

    /// Check if connection is closed
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    /// Wait for the connection to close
    pub async fn closed(&self) {
        self.connection.closed().await;
    }

    /// Get stable ID for this connection
    pub fn stable_id(&self) -> usize {
        self.connection.stable_id()
    }
}

/// QUIC bidirectional stream
pub struct QuicBiStream {
    /// Send stream
    send: SendStream,
    /// Receive stream
    recv: RecvStream,
    /// Stream ID
    stream_id: u64,
}

impl QuicBiStream {
    /// Create a new bidirectional stream
    fn new(send: SendStream, recv: RecvStream) -> Self {
        let stream_id = send.id().index();
        Self {
            send,
            recv,
            stream_id,
        }
    }

    /// Get the stream ID
    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    /// Read data from the stream
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>> {
        self.recv
            .read(buf)
            .await
            .map_err(|e| StreamlineError::protocol_msg(format!("Read error: {}", e)))
    }

    /// Read exact amount of data
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.recv
            .read_exact(buf)
            .await
            .map_err(|e| StreamlineError::protocol_msg(format!("Read exact error: {}", e)))
    }

    /// Write data to the stream
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.send
            .write_all(data)
            .await
            .map_err(|e| StreamlineError::protocol_msg(format!("Write error: {}", e)))
    }

    /// Finish the send side of the stream
    pub async fn finish(&mut self) -> Result<()> {
        self.send
            .finish()
            .map_err(|e| StreamlineError::protocol_msg(format!("Finish error: {}", e)))
    }

    /// Reset the stream
    pub fn reset(&mut self, error_code: u32) -> Result<()> {
        self.send
            .reset(error_code.into())
            .map_err(|e| StreamlineError::protocol_msg(format!("Reset error: {}", e)))
    }

    /// Split into send and receive halves
    pub fn split(self) -> (QuicSendStream, QuicRecvStream) {
        (
            QuicSendStream::new(self.send),
            QuicRecvStream::new(self.recv),
        )
    }
}

/// QUIC send stream
pub struct QuicSendStream {
    /// Send stream
    send: SendStream,
    /// Stream ID
    stream_id: u64,
}

impl QuicSendStream {
    /// Create a new send stream
    fn new(send: SendStream) -> Self {
        let stream_id = send.id().index();
        Self { send, stream_id }
    }

    /// Get the stream ID
    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    /// Write data to the stream
    pub async fn write(&mut self, data: &[u8]) -> Result<()> {
        self.send
            .write_all(data)
            .await
            .map_err(|e| StreamlineError::protocol_msg(format!("Write error: {}", e)))
    }

    /// Finish the stream
    pub async fn finish(&mut self) -> Result<()> {
        self.send
            .finish()
            .map_err(|e| StreamlineError::protocol_msg(format!("Finish error: {}", e)))
    }

    /// Reset the stream
    pub fn reset(&mut self, error_code: u32) -> Result<()> {
        self.send
            .reset(error_code.into())
            .map_err(|e| StreamlineError::protocol_msg(format!("Reset error: {}", e)))
    }
}

/// QUIC receive stream
pub struct QuicRecvStream {
    /// Receive stream
    recv: RecvStream,
    /// Stream ID
    stream_id: u64,
}

impl QuicRecvStream {
    /// Create a new receive stream
    fn new(recv: RecvStream) -> Self {
        let stream_id = recv.id().index();
        Self { recv, stream_id }
    }

    /// Get the stream ID
    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    /// Read data from the stream
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>> {
        self.recv
            .read(buf)
            .await
            .map_err(|e| StreamlineError::protocol_msg(format!("Read error: {}", e)))
    }

    /// Read exact amount of data
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.recv
            .read_exact(buf)
            .await
            .map_err(|e| StreamlineError::protocol_msg(format!("Read exact error: {}", e)))
    }

    /// Read to end
    pub async fn read_to_end(&mut self, max_size: usize) -> Result<Vec<u8>> {
        self.recv
            .read_to_end(max_size)
            .await
            .map_err(|e| StreamlineError::protocol_msg(format!("Read to end error: {}", e)))
    }

    /// Stop receiving (send STOP_SENDING frame)
    pub fn stop(&mut self, error_code: u32) -> Result<()> {
        self.recv
            .stop(error_code.into())
            .map_err(|e| StreamlineError::protocol_msg(format!("Stop error: {}", e)))
    }
}

/// QUIC client for connecting to servers
pub struct QuicClient {
    /// Quinn endpoint
    endpoint: Endpoint,
    /// Configuration
    config: QuicClientConfig,
}

impl QuicClient {
    /// Create a new QUIC client
    pub fn new(config: QuicClientConfig) -> Result<Self> {
        let mut endpoint = Endpoint::client(SocketAddr::from(([0, 0, 0, 0], 0))).map_err(|e| {
            StreamlineError::Config(format!("Failed to create client endpoint: {}", e))
        })?;

        let client_config = Self::build_client_config(&config)?;
        endpoint.set_default_client_config(client_config);

        Ok(Self { endpoint, config })
    }

    /// Build client configuration
    fn build_client_config(config: &QuicClientConfig) -> Result<ClientConfig> {
        let crypto = if config.skip_verification {
            // Skip certificate verification (for self-signed certs)
            rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
                .with_no_client_auth()
        } else {
            // Use default certificate verification
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth()
        };

        let mut client_config = ClientConfig::new(Arc::new(
            quinn::crypto::rustls::QuicClientConfig::try_from(crypto).map_err(|e| {
                StreamlineError::Config(format!("Failed to create QUIC crypto config: {}", e))
            })?,
        ));

        // Configure transport
        let mut transport = TransportConfig::default();
        transport.max_idle_timeout(Some(
            Duration::from_millis(config.idle_timeout_ms)
                .try_into()
                .map_err(|e| StreamlineError::Config(format!("Invalid idle timeout: {}", e)))?,
        ));
        if config.keep_alive_interval_ms > 0 {
            transport
                .keep_alive_interval(Some(Duration::from_millis(config.keep_alive_interval_ms)));
        }

        client_config.transport_config(Arc::new(transport));

        Ok(client_config)
    }

    /// Connect to a server
    pub async fn connect(&self) -> Result<QuicConnection> {
        let connection = self
            .endpoint
            .connect(self.config.server_addr, &self.config.server_name)
            .map_err(|e| StreamlineError::Config(format!("Failed to initiate connection: {}", e)))?
            .await
            .map_err(|e| StreamlineError::protocol_msg(format!("Connection failed: {}", e)))?;

        info!("Connected to QUIC server at {}", self.config.server_addr);

        Ok(QuicConnection::new(connection))
    }

    /// Close the client endpoint
    pub fn close(&self) {
        self.endpoint.close(0u32.into(), b"client shutdown");
    }
}

/// Skip server certificate verification (for self-signed certs)
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_crypto() {
        // Install the ring crypto provider for rustls
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    #[test]
    fn test_quic_server_config_default() {
        let config = QuicServerConfig::default();
        assert_eq!(config.bind_addr.port(), 9093);
        assert_eq!(config.max_connections, 10000);
        assert_eq!(config.max_bi_streams, 100);
        assert!(config.enable_0rtt);
    }

    #[test]
    fn test_quic_client_config_default() {
        let config = QuicClientConfig::default();
        assert_eq!(config.server_addr.port(), 9093);
        assert!(config.skip_verification);
        assert!(config.enable_0rtt);
    }

    #[test]
    fn test_quic_server_metrics_default() {
        let metrics = QuicServerMetrics::default();
        assert_eq!(metrics.connections_accepted, 0);
        assert_eq!(metrics.active_connections, 0);
        assert_eq!(metrics.bytes_received, 0);
    }

    #[tokio::test]
    async fn test_quic_server_creation() {
        setup_crypto();

        let config = QuicServerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        };

        let server = QuicServer::new(config);
        assert!(server.is_ok());

        let server = server.unwrap();
        assert!(server.local_addr().is_some());

        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_quic_client_creation() {
        setup_crypto();

        let config = QuicClientConfig::default();
        let client = QuicClient::new(config);
        assert!(client.is_ok());
    }
}
