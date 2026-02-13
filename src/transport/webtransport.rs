//! WebTransport implementation
//!
//! Provides WebTransport support for browser-based streaming using HTTP/3.
//! WebTransport enables low-latency bidirectional communication between
//! browsers and Streamline servers.
//!
//! Features:
//! - HTTP/3 based on QUIC
//! - Multiplexed bidirectional streams
//! - Datagram support for unreliable messaging
//! - Session management with origin validation
//! - Browser-compatible protocol

use super::ConnectionStats;
use crate::error::{Result, StreamlineError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use parking_lot::RwLock;
use quinn::{Connection, Endpoint, RecvStream, SendStream, ServerConfig, TransportConfig};
use rcgen::{CertificateParams, KeyPair};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

/// WebTransport server configuration
#[derive(Debug, Clone)]
pub struct WebTransportConfig {
    /// Address to bind to
    pub bind_addr: SocketAddr,
    /// Allowed origins (empty means all origins allowed)
    pub allowed_origins: Vec<String>,
    /// Maximum concurrent sessions
    pub max_sessions: u32,
    /// Maximum concurrent streams per session
    pub max_streams_per_session: u32,
    /// Session idle timeout in milliseconds
    pub session_idle_timeout_ms: u64,
    /// Enable datagram support
    pub enable_datagrams: bool,
    /// Maximum datagram size
    pub max_datagram_size: u16,
    /// Custom certificate PEM (if None, generates self-signed)
    pub cert_pem: Option<String>,
    /// Custom private key PEM (if None, generates self-signed)
    pub key_pem: Option<String>,
    /// Server name for certificate generation
    pub server_name: String,
    /// ALPN protocol (default: h3)
    pub alpn_protocols: Vec<String>,
}

impl Default for WebTransportConfig {
    fn default() -> Self {
        Self {
            bind_addr: std::net::SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0)),
                9095,
            ),
            allowed_origins: Vec::new(),
            max_sessions: 10000,
            max_streams_per_session: 100,
            session_idle_timeout_ms: 30000,
            enable_datagrams: true,
            max_datagram_size: 65535,
            cert_pem: None,
            key_pem: None,
            server_name: "streamline".to_string(),
            alpn_protocols: vec!["h3".to_string()],
        }
    }
}

/// WebTransport server
#[allow(dead_code)]
pub struct WebTransportServer {
    /// Quinn endpoint
    endpoint: Endpoint,
    /// Configuration
    config: WebTransportConfig,
    /// Active sessions
    sessions: RwLock<HashMap<u64, Arc<WebTransportSession>>>,
    /// Session counter
    session_counter: AtomicU64,
    /// Running flag
    running: AtomicBool,
    /// Server metrics
    metrics: RwLock<WebTransportMetrics>,
}

/// WebTransport server metrics
#[derive(Debug, Clone, Default)]
pub struct WebTransportMetrics {
    /// Total sessions created
    pub sessions_created: u64,
    /// Active sessions
    pub active_sessions: u64,
    /// Total streams opened
    pub streams_opened: u64,
    /// Total datagrams sent
    pub datagrams_sent: u64,
    /// Total datagrams received
    pub datagrams_received: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Total bytes received
    pub bytes_received: u64,
    /// Connection errors
    pub errors: u64,
}

impl WebTransportServer {
    /// Create a new WebTransport server
    pub fn new(config: WebTransportConfig) -> Result<Self> {
        let server_config = Self::build_server_config(&config)?;

        let endpoint = Endpoint::server(server_config, config.bind_addr).map_err(|e| {
            StreamlineError::Config(format!("Failed to create WebTransport endpoint: {}", e))
        })?;

        info!("WebTransport server listening on {}", config.bind_addr);

        Ok(Self {
            endpoint,
            config,
            sessions: RwLock::new(HashMap::new()),
            session_counter: AtomicU64::new(0),
            running: AtomicBool::new(true),
            metrics: RwLock::new(WebTransportMetrics::default()),
        })
    }

    /// Build server configuration with H3 ALPN
    fn build_server_config(config: &WebTransportConfig) -> Result<ServerConfig> {
        // Generate or load certificate
        let (cert_der, key_der) =
            if let (Some(cert_pem), Some(key_pem)) = (&config.cert_pem, &config.key_pem) {
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

        // Build rustls config with H3 ALPN
        let alpn: Vec<Vec<u8>> = config
            .alpn_protocols
            .iter()
            .map(|p| p.as_bytes().to_vec())
            .collect();

        let mut rustls_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_der, key_der)
            .map_err(|e| StreamlineError::Config(format!("Failed to build TLS config: {}", e)))?;

        rustls_config.alpn_protocols = alpn;

        let mut server_config = ServerConfig::with_crypto(Arc::new(
            quinn::crypto::rustls::QuicServerConfig::try_from(rustls_config).map_err(|e| {
                StreamlineError::Config(format!("Failed to create QUIC crypto config: {}", e))
            })?,
        ));

        // Configure transport
        let mut transport = TransportConfig::default();
        transport.max_concurrent_bidi_streams(config.max_streams_per_session.into());
        transport.max_concurrent_uni_streams(config.max_streams_per_session.into());
        transport.max_idle_timeout(Some(
            Duration::from_millis(config.session_idle_timeout_ms)
                .try_into()
                .map_err(|e| {
                    StreamlineError::Config(format!("Invalid idle timeout value: {}", e))
                })?,
        ));

        if config.enable_datagrams {
            transport.datagram_receive_buffer_size(Some(config.max_datagram_size.into()));
        }

        server_config.transport_config(Arc::new(transport));

        Ok(server_config)
    }

    /// Accept a new WebTransport session
    pub async fn accept(&self) -> Result<WebTransportSession> {
        if !self.running.load(Ordering::Relaxed) {
            return Err(StreamlineError::Config("Server is not running".to_string()));
        }

        let incoming = self
            .endpoint
            .accept()
            .await
            .ok_or_else(|| StreamlineError::Config("Endpoint closed".to_string()))?;

        let connection = incoming.await.map_err(|e| {
            self.metrics.write().errors += 1;
            StreamlineError::protocol_msg(format!("WebTransport connection failed: {}", e))
        })?;

        // Perform WebTransport handshake
        let session = self.perform_handshake(connection).await?;

        let session_id = self.session_counter.fetch_add(1, Ordering::Relaxed);
        self.metrics.write().sessions_created += 1;
        self.metrics.write().active_sessions += 1;

        let session_arc = Arc::new(session);
        self.sessions
            .write()
            .insert(session_id, session_arc.clone());

        info!(
            "WebTransport session {} established from {}",
            session_id,
            session_arc.remote_addr()
        );

        Ok(WebTransportSession {
            session_id,
            connection: session_arc.connection.clone(),
            origin: session_arc.origin.clone(),
            path: session_arc.path.clone(),
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        })
    }

    /// Perform WebTransport handshake (simplified)
    async fn perform_handshake(&self, connection: Connection) -> Result<WebTransportSession> {
        // Accept the connect stream (first bidirectional stream)
        let (mut send, mut recv) = connection.accept_bi().await.map_err(|e| {
            StreamlineError::protocol_msg(format!("Failed to accept handshake stream: {}", e))
        })?;

        // Read CONNECT request (simplified - in production would parse full HTTP/3 frames)
        let mut buf = vec![0u8; 1024];
        let n = recv
            .read(&mut buf)
            .await
            .map_err(|e| StreamlineError::protocol_msg(format!("Failed to read handshake: {}", e)))?
            .unwrap_or(0);

        let request = String::from_utf8_lossy(&buf[..n]);
        debug!("WebTransport handshake request: {}", request);

        // Parse origin and path from request (simplified)
        let (origin, path) = self.parse_connect_request(&request)?;

        // Validate origin
        if !self.validate_origin(&origin) {
            return Err(StreamlineError::protocol_msg(format!(
                "Origin '{}' not allowed",
                origin
            )));
        }

        // Send 200 OK response (simplified HTTP/3 response)
        let response = b"HTTP/3 200 OK\r\n\r\n";
        send.write_all(response).await.map_err(|e| {
            StreamlineError::protocol_msg(format!("Failed to send handshake response: {}", e))
        })?;

        Ok(WebTransportSession {
            session_id: 0,
            connection,
            origin,
            path,
            bytes_sent: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            closed: AtomicBool::new(false),
        })
    }

    /// Parse CONNECT request (simplified)
    fn parse_connect_request(&self, request: &str) -> Result<(String, String)> {
        // Very simplified parsing - in production would use proper HTTP/3 frame parsing
        let origin = request
            .lines()
            .find(|l| l.to_lowercase().starts_with("origin:"))
            .map(|l| {
                l.split(':')
                    .skip(1)
                    .collect::<Vec<_>>()
                    .join(":")
                    .trim()
                    .to_string()
            })
            .unwrap_or_else(|| "unknown".to_string());

        let path = request
            .lines()
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .unwrap_or("/")
            .to_string();

        Ok((origin, path))
    }

    /// Validate origin
    fn validate_origin(&self, origin: &str) -> bool {
        if self.config.allowed_origins.is_empty() {
            return true;
        }
        self.config
            .allowed_origins
            .iter()
            .any(|o| o == origin || o == "*")
    }

    /// Get local address
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.endpoint.local_addr().ok()
    }

    /// Get server metrics
    pub fn metrics(&self) -> WebTransportMetrics {
        self.metrics.read().clone()
    }

    /// Shutdown the server
    pub async fn shutdown(&self) -> Result<()> {
        self.running.store(false, Ordering::Relaxed);
        self.endpoint.close(0u32.into(), b"server shutdown");

        // Close all sessions
        for (_, session) in self.sessions.write().drain() {
            session.close(0, b"server shutdown");
        }

        info!("WebTransport server shutdown");
        Ok(())
    }
}

/// WebTransport session
#[allow(dead_code)]
pub struct WebTransportSession {
    /// Session ID
    session_id: u64,
    /// Underlying QUIC connection
    connection: Connection,
    /// Origin (for CORS)
    origin: String,
    /// Request path
    path: String,
    /// Bytes sent counter
    bytes_sent: AtomicU64,
    /// Bytes received counter
    bytes_received: AtomicU64,
    /// Closed flag
    closed: AtomicBool,
}

impl WebTransportSession {
    /// Get the session ID
    pub fn session_id(&self) -> u64 {
        self.session_id
    }

    /// Get the origin
    pub fn origin(&self) -> &str {
        &self.origin
    }

    /// Get the request path
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Get remote address
    pub fn remote_addr(&self) -> SocketAddr {
        self.connection.remote_address()
    }

    /// Open a new bidirectional stream
    pub async fn open_bi(&self) -> Result<WebTransportBiStream> {
        let (send, recv) =
            self.connection.open_bi().await.map_err(|e| {
                StreamlineError::protocol_msg(format!("Failed to open stream: {}", e))
            })?;
        Ok(WebTransportBiStream::new(send, recv))
    }

    /// Accept an incoming bidirectional stream
    pub async fn accept_bi(&self) -> Result<WebTransportBiStream> {
        let (send, recv) = self.connection.accept_bi().await.map_err(|e| {
            StreamlineError::protocol_msg(format!("Failed to accept stream: {}", e))
        })?;
        Ok(WebTransportBiStream::new(send, recv))
    }

    /// Open a unidirectional send stream
    pub async fn open_uni(&self) -> Result<WebTransportSendStream> {
        let send = self.connection.open_uni().await.map_err(|e| {
            StreamlineError::protocol_msg(format!("Failed to open uni stream: {}", e))
        })?;
        Ok(WebTransportSendStream::new(send))
    }

    /// Accept an incoming unidirectional receive stream
    pub async fn accept_uni(&self) -> Result<WebTransportRecvStream> {
        let recv = self.connection.accept_uni().await.map_err(|e| {
            StreamlineError::protocol_msg(format!("Failed to accept uni stream: {}", e))
        })?;
        Ok(WebTransportRecvStream::new(recv))
    }

    /// Send a datagram (unreliable)
    pub fn send_datagram(&self, data: Bytes) -> Result<()> {
        self.connection
            .send_datagram(data)
            .map_err(|e| StreamlineError::protocol_msg(format!("Failed to send datagram: {}", e)))
    }

    /// Receive a datagram (unreliable)
    pub async fn receive_datagram(&self) -> Result<Bytes> {
        self.connection.read_datagram().await.map_err(|e| {
            StreamlineError::protocol_msg(format!("Failed to receive datagram: {}", e))
        })
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
            retransmissions: 0,
        }
    }

    /// Close the session
    pub fn close(&self, code: u32, reason: &[u8]) {
        self.connection.close(code.into(), reason);
        self.closed.store(true, Ordering::Relaxed);
    }

    /// Check if session is closed
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }
}

/// WebTransport bidirectional stream
pub struct WebTransportBiStream {
    /// Send stream
    send: SendStream,
    /// Receive stream
    recv: RecvStream,
    /// Stream ID
    stream_id: u64,
}

impl WebTransportBiStream {
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
}

/// WebTransport send stream
pub struct WebTransportSendStream {
    /// Send stream
    send: SendStream,
    /// Stream ID
    stream_id: u64,
}

impl WebTransportSendStream {
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
}

/// WebTransport receive stream
pub struct WebTransportRecvStream {
    /// Receive stream
    recv: RecvStream,
    /// Stream ID
    stream_id: u64,
}

impl WebTransportRecvStream {
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

    /// Read to end
    pub async fn read_to_end(&mut self, max_size: usize) -> Result<Vec<u8>> {
        self.recv
            .read_to_end(max_size)
            .await
            .map_err(|e| StreamlineError::protocol_msg(format!("Read to end error: {}", e)))
    }
}

/// WebTransport message types for Streamline protocol
#[derive(Debug, Clone)]
pub enum WebTransportMessage {
    /// Produce message
    Produce {
        topic: String,
        partition: Option<i32>,
        key: Option<Bytes>,
        value: Bytes,
        headers: Vec<(String, Bytes)>,
    },
    /// Consume subscription
    Subscribe {
        topic: String,
        partition: Option<i32>,
        offset: Option<i64>,
        group_id: Option<String>,
    },
    /// Consumed message
    Message {
        topic: String,
        partition: i32,
        offset: i64,
        key: Option<Bytes>,
        value: Bytes,
        timestamp: i64,
        headers: Vec<(String, Bytes)>,
    },
    /// Acknowledgment
    Ack {
        topic: String,
        partition: i32,
        offset: i64,
    },
    /// Error response
    Error { code: i32, message: String },
    /// Ping
    Ping,
    /// Pong
    Pong,
}

impl WebTransportMessage {
    /// Message type identifiers
    const TYPE_PRODUCE: u8 = 1;
    const TYPE_SUBSCRIBE: u8 = 2;
    const TYPE_MESSAGE: u8 = 3;
    const TYPE_ACK: u8 = 4;
    const TYPE_ERROR: u8 = 5;
    const TYPE_PING: u8 = 6;
    const TYPE_PONG: u8 = 7;

    /// Encode message to bytes
    pub fn encode(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(256);

        match self {
            WebTransportMessage::Produce {
                topic,
                partition,
                key,
                value,
                headers,
            } => {
                buf.put_u8(Self::TYPE_PRODUCE);
                Self::encode_string(&mut buf, topic);
                buf.put_i32(partition.unwrap_or(-1));
                Self::encode_optional_bytes(&mut buf, key.as_ref());
                Self::encode_bytes(&mut buf, value);
                buf.put_u16(headers.len() as u16);
                for (k, v) in headers {
                    Self::encode_string(&mut buf, k);
                    Self::encode_bytes(&mut buf, v);
                }
            }
            WebTransportMessage::Subscribe {
                topic,
                partition,
                offset,
                group_id,
            } => {
                buf.put_u8(Self::TYPE_SUBSCRIBE);
                Self::encode_string(&mut buf, topic);
                buf.put_i32(partition.unwrap_or(-1));
                buf.put_i64(offset.unwrap_or(-1));
                Self::encode_optional_string(&mut buf, group_id.as_ref());
            }
            WebTransportMessage::Message {
                topic,
                partition,
                offset,
                key,
                value,
                timestamp,
                headers,
            } => {
                buf.put_u8(Self::TYPE_MESSAGE);
                Self::encode_string(&mut buf, topic);
                buf.put_i32(*partition);
                buf.put_i64(*offset);
                Self::encode_optional_bytes(&mut buf, key.as_ref());
                Self::encode_bytes(&mut buf, value);
                buf.put_i64(*timestamp);
                buf.put_u16(headers.len() as u16);
                for (k, v) in headers {
                    Self::encode_string(&mut buf, k);
                    Self::encode_bytes(&mut buf, v);
                }
            }
            WebTransportMessage::Ack {
                topic,
                partition,
                offset,
            } => {
                buf.put_u8(Self::TYPE_ACK);
                Self::encode_string(&mut buf, topic);
                buf.put_i32(*partition);
                buf.put_i64(*offset);
            }
            WebTransportMessage::Error { code, message } => {
                buf.put_u8(Self::TYPE_ERROR);
                buf.put_i32(*code);
                Self::encode_string(&mut buf, message);
            }
            WebTransportMessage::Ping => {
                buf.put_u8(Self::TYPE_PING);
            }
            WebTransportMessage::Pong => {
                buf.put_u8(Self::TYPE_PONG);
            }
        }

        buf.freeze()
    }

    /// Decode message from bytes
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Err(StreamlineError::protocol_msg("Empty message".to_string()));
        }

        let mut buf = data;
        let msg_type = buf.get_u8();

        match msg_type {
            Self::TYPE_PRODUCE => {
                let topic = Self::decode_string(&mut buf)?;
                let partition = buf.get_i32();
                let key = Self::decode_optional_bytes(&mut buf)?;
                let value = Self::decode_bytes(&mut buf)?;
                let header_count = buf.get_u16() as usize;
                let mut headers = Vec::with_capacity(header_count);
                for _ in 0..header_count {
                    let k = Self::decode_string(&mut buf)?;
                    let v = Self::decode_bytes(&mut buf)?;
                    headers.push((k, v));
                }
                Ok(WebTransportMessage::Produce {
                    topic,
                    partition: if partition == -1 {
                        None
                    } else {
                        Some(partition)
                    },
                    key,
                    value,
                    headers,
                })
            }
            Self::TYPE_SUBSCRIBE => {
                let topic = Self::decode_string(&mut buf)?;
                let partition = buf.get_i32();
                let offset = buf.get_i64();
                let group_id = Self::decode_optional_string(&mut buf)?;
                Ok(WebTransportMessage::Subscribe {
                    topic,
                    partition: if partition == -1 {
                        None
                    } else {
                        Some(partition)
                    },
                    offset: if offset == -1 { None } else { Some(offset) },
                    group_id,
                })
            }
            Self::TYPE_MESSAGE => {
                let topic = Self::decode_string(&mut buf)?;
                let partition = buf.get_i32();
                let offset = buf.get_i64();
                let key = Self::decode_optional_bytes(&mut buf)?;
                let value = Self::decode_bytes(&mut buf)?;
                let timestamp = buf.get_i64();
                let header_count = buf.get_u16() as usize;
                let mut headers = Vec::with_capacity(header_count);
                for _ in 0..header_count {
                    let k = Self::decode_string(&mut buf)?;
                    let v = Self::decode_bytes(&mut buf)?;
                    headers.push((k, v));
                }
                Ok(WebTransportMessage::Message {
                    topic,
                    partition,
                    offset,
                    key,
                    value,
                    timestamp,
                    headers,
                })
            }
            Self::TYPE_ACK => {
                let topic = Self::decode_string(&mut buf)?;
                let partition = buf.get_i32();
                let offset = buf.get_i64();
                Ok(WebTransportMessage::Ack {
                    topic,
                    partition,
                    offset,
                })
            }
            Self::TYPE_ERROR => {
                let code = buf.get_i32();
                let message = Self::decode_string(&mut buf)?;
                Ok(WebTransportMessage::Error { code, message })
            }
            Self::TYPE_PING => Ok(WebTransportMessage::Ping),
            Self::TYPE_PONG => Ok(WebTransportMessage::Pong),
            _ => Err(StreamlineError::protocol_msg(format!(
                "Unknown message type: {}",
                msg_type
            ))),
        }
    }

    fn encode_string(buf: &mut BytesMut, s: &str) {
        buf.put_u16(s.len() as u16);
        buf.put_slice(s.as_bytes());
    }

    fn decode_string(buf: &mut &[u8]) -> Result<String> {
        let len = buf.get_u16() as usize;
        if buf.len() < len {
            return Err(StreamlineError::protocol_msg(
                "Buffer underflow".to_string(),
            ));
        }
        let s = String::from_utf8(buf[..len].to_vec())
            .map_err(|e| StreamlineError::protocol_msg(format!("Invalid UTF-8: {}", e)))?;
        buf.advance(len);
        Ok(s)
    }

    fn encode_bytes(buf: &mut BytesMut, b: &Bytes) {
        buf.put_u32(b.len() as u32);
        buf.put_slice(b);
    }

    fn decode_bytes(buf: &mut &[u8]) -> Result<Bytes> {
        let len = buf.get_u32() as usize;
        if buf.len() < len {
            return Err(StreamlineError::protocol_msg(
                "Buffer underflow".to_string(),
            ));
        }
        let b = Bytes::copy_from_slice(&buf[..len]);
        buf.advance(len);
        Ok(b)
    }

    fn encode_optional_bytes(buf: &mut BytesMut, b: Option<&Bytes>) {
        match b {
            Some(bytes) => {
                buf.put_u8(1);
                Self::encode_bytes(buf, bytes);
            }
            None => {
                buf.put_u8(0);
            }
        }
    }

    fn decode_optional_bytes(buf: &mut &[u8]) -> Result<Option<Bytes>> {
        if buf.get_u8() == 0 {
            Ok(None)
        } else {
            Self::decode_bytes(buf).map(Some)
        }
    }

    fn encode_optional_string(buf: &mut BytesMut, s: Option<&String>) {
        match s {
            Some(str) => {
                buf.put_u8(1);
                Self::encode_string(buf, str);
            }
            None => {
                buf.put_u8(0);
            }
        }
    }

    fn decode_optional_string(buf: &mut &[u8]) -> Result<Option<String>> {
        if buf.get_u8() == 0 {
            Ok(None)
        } else {
            Self::decode_string(buf).map(Some)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_crypto() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    #[test]
    fn test_webtransport_config_default() {
        let config = WebTransportConfig::default();
        assert_eq!(config.bind_addr.port(), 9095);
        assert!(config.allowed_origins.is_empty());
        assert!(config.enable_datagrams);
        assert_eq!(config.alpn_protocols, vec!["h3".to_string()]);
    }

    #[test]
    fn test_webtransport_metrics_default() {
        let metrics = WebTransportMetrics::default();
        assert_eq!(metrics.sessions_created, 0);
        assert_eq!(metrics.active_sessions, 0);
    }

    #[tokio::test]
    async fn test_webtransport_server_creation() {
        setup_crypto();

        let config = WebTransportConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            ..Default::default()
        };

        let server = WebTransportServer::new(config);
        assert!(server.is_ok());

        let server = server.unwrap();
        assert!(server.local_addr().is_some());

        server.shutdown().await.unwrap();
    }

    #[test]
    fn test_webtransport_message_encode_decode_ping() {
        let msg = WebTransportMessage::Ping;
        let encoded = msg.encode();
        let decoded = WebTransportMessage::decode(&encoded).unwrap();

        assert!(matches!(decoded, WebTransportMessage::Ping));
    }

    #[test]
    fn test_webtransport_message_encode_decode_produce() {
        let msg = WebTransportMessage::Produce {
            topic: "test-topic".to_string(),
            partition: Some(0),
            key: Some(Bytes::from("key")),
            value: Bytes::from("value"),
            headers: vec![("h1".to_string(), Bytes::from("v1"))],
        };
        let encoded = msg.encode();
        let decoded = WebTransportMessage::decode(&encoded).unwrap();

        if let WebTransportMessage::Produce {
            topic,
            partition,
            key,
            value,
            headers,
        } = decoded
        {
            assert_eq!(topic, "test-topic");
            assert_eq!(partition, Some(0));
            assert_eq!(key, Some(Bytes::from("key")));
            assert_eq!(value, Bytes::from("value"));
            assert_eq!(headers.len(), 1);
        } else {
            panic!("Expected Produce message");
        }
    }

    #[test]
    fn test_webtransport_message_encode_decode_subscribe() {
        let msg = WebTransportMessage::Subscribe {
            topic: "test-topic".to_string(),
            partition: None,
            offset: Some(100),
            group_id: Some("my-group".to_string()),
        };
        let encoded = msg.encode();
        let decoded = WebTransportMessage::decode(&encoded).unwrap();

        if let WebTransportMessage::Subscribe {
            topic,
            partition,
            offset,
            group_id,
        } = decoded
        {
            assert_eq!(topic, "test-topic");
            assert_eq!(partition, None);
            assert_eq!(offset, Some(100));
            assert_eq!(group_id, Some("my-group".to_string()));
        } else {
            panic!("Expected Subscribe message");
        }
    }

    #[test]
    fn test_webtransport_message_encode_decode_error() {
        let msg = WebTransportMessage::Error {
            code: 404,
            message: "Not found".to_string(),
        };
        let encoded = msg.encode();
        let decoded = WebTransportMessage::decode(&encoded).unwrap();

        if let WebTransportMessage::Error { code, message } = decoded {
            assert_eq!(code, 404);
            assert_eq!(message, "Not found");
        } else {
            panic!("Expected Error message");
        }
    }
}
