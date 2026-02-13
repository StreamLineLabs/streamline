//! Transport layer abstraction
//!
//! This module provides transport protocol abstractions for Streamline,
//! supporting multiple transport protocols including TCP and QUIC.

pub mod mqtt;

#[cfg(feature = "quic")]
pub mod quic;

#[cfg(feature = "quic")]
pub mod webtransport;

use crate::error::Result;
use async_trait::async_trait;
use std::net::SocketAddr;

/// Transport connection trait
#[async_trait]
pub trait TransportConnection: Send + Sync {
    /// Get the remote address
    fn remote_addr(&self) -> Option<SocketAddr>;

    /// Get the local address
    fn local_addr(&self) -> Option<SocketAddr>;

    /// Read data from the connection
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize>;

    /// Write data to the connection
    async fn write(&mut self, data: &[u8]) -> Result<usize>;

    /// Write all data to the connection
    async fn write_all(&mut self, data: &[u8]) -> Result<()>;

    /// Flush the connection
    async fn flush(&mut self) -> Result<()>;

    /// Close the connection gracefully
    async fn close(&mut self) -> Result<()>;

    /// Check if the connection is closed
    fn is_closed(&self) -> bool;

    /// Get connection statistics
    fn stats(&self) -> ConnectionStats;
}

/// Transport stream for multiplexed protocols (QUIC)
#[async_trait]
pub trait TransportStream: Send + Sync {
    /// Get the stream ID
    fn stream_id(&self) -> u64;

    /// Read data from the stream
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize>;

    /// Write data to the stream
    async fn write(&mut self, data: &[u8]) -> Result<usize>;

    /// Write all data to the stream
    async fn write_all(&mut self, data: &[u8]) -> Result<()>;

    /// Finish sending on this stream
    async fn finish(&mut self) -> Result<()>;

    /// Stop the stream (send reset)
    fn stop(&mut self) -> Result<()>;
}

/// Transport server trait
#[async_trait]
pub trait TransportServer: Send + Sync {
    /// The connection type this server produces
    type Connection: TransportConnection;

    /// Accept an incoming connection
    async fn accept(&self) -> Result<Self::Connection>;

    /// Get the local address the server is bound to
    fn local_addr(&self) -> Option<SocketAddr>;

    /// Shutdown the server
    async fn shutdown(&self) -> Result<()>;
}

/// Connection statistics
#[derive(Debug, Clone, Default)]
pub struct ConnectionStats {
    /// Bytes sent
    pub bytes_sent: u64,
    /// Bytes received
    pub bytes_received: u64,
    /// Packets sent (for QUIC)
    pub packets_sent: u64,
    /// Packets received (for QUIC)
    pub packets_received: u64,
    /// RTT in microseconds
    pub rtt_us: u64,
    /// Congestion window
    pub cwnd: u64,
    /// Lost packets
    pub lost_packets: u64,
    /// Retransmissions
    pub retransmissions: u64,
}

/// Transport type enum
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportType {
    /// TCP transport
    Tcp,
    /// QUIC transport
    Quic,
    /// WebTransport (QUIC-based)
    WebTransport,
    /// MQTT 5.0 protocol gateway
    Mqtt,
}

impl std::fmt::Display for TransportType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportType::Tcp => write!(f, "TCP"),
            TransportType::Quic => write!(f, "QUIC"),
            TransportType::WebTransport => write!(f, "WebTransport"),
            TransportType::Mqtt => write!(f, "MQTT"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transport_type_display() {
        assert_eq!(TransportType::Tcp.to_string(), "TCP");
        assert_eq!(TransportType::Quic.to_string(), "QUIC");
        assert_eq!(TransportType::WebTransport.to_string(), "WebTransport");
    }

    #[test]
    fn test_connection_stats_default() {
        let stats = ConnectionStats::default();
        assert_eq!(stats.bytes_sent, 0);
        assert_eq!(stats.bytes_received, 0);
        assert_eq!(stats.rtt_us, 0);
    }
}
