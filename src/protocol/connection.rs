//! Connection context for zero-copy optimization decisions
//!
//! This module provides connection-level context that determines which
//! optimizations can be applied. TLS connections automatically disable
//! sendfile() since encryption must happen in user space.

use crate::storage::ZeroCopyConfig;
use std::net::SocketAddr;

/// Raw file descriptor type (platform-specific)
#[cfg(unix)]
pub type RawFd = std::os::unix::io::RawFd;

/// Raw file descriptor type (non-Unix fallback)
#[cfg(not(unix))]
pub type RawFd = i32;

/// Connection properties that affect optimization strategies
///
/// Created at connection acceptance time and propagated through the
/// request/response pipeline to enable context-aware optimizations.
#[derive(Debug, Clone)]
pub struct ConnectionContext {
    /// Whether TLS is enabled on this connection
    pub is_tls: bool,
    /// Peer address (for metrics/debugging)
    pub peer_addr: Option<SocketAddr>,
    /// Zero-copy config for this connection
    pub zerocopy_config: ZeroCopyConfig,
    /// Raw socket file descriptor for sendfile (Unix only)
    ///
    /// This is stored before the stream is split so sendfile can use it.
    /// Only valid for plain TCP connections on Unix platforms.
    pub socket_fd: Option<RawFd>,
}

impl ConnectionContext {
    /// Create context for a plain TCP connection
    ///
    /// All zero-copy optimizations are available for plain TCP connections.
    pub fn plain_tcp(peer_addr: Option<SocketAddr>, config: ZeroCopyConfig) -> Self {
        Self {
            is_tls: false,
            peer_addr,
            zerocopy_config: config,
            socket_fd: None,
        }
    }

    /// Create context for a plain TCP connection with socket fd for sendfile
    ///
    /// This variant stores the raw socket fd to enable sendfile transfers.
    /// The fd must be extracted from the TcpStream before it is split.
    #[cfg(unix)]
    pub fn plain_tcp_with_fd(
        peer_addr: Option<SocketAddr>,
        config: ZeroCopyConfig,
        socket_fd: RawFd,
    ) -> Self {
        Self {
            is_tls: false,
            peer_addr,
            zerocopy_config: config,
            socket_fd: Some(socket_fd),
        }
    }

    /// Create context for a TLS connection
    ///
    /// Sendfile is automatically disabled because TLS encryption must happen
    /// in user space. Other optimizations (mmap, buffer pool) still apply.
    pub fn tls(peer_addr: Option<SocketAddr>, config: ZeroCopyConfig) -> Self {
        Self {
            is_tls: true,
            peer_addr,
            zerocopy_config: ZeroCopyConfig {
                enable_sendfile: false, // Auto-disable for TLS
                ..config
            },
            socket_fd: None, // Never use sendfile for TLS
        }
    }

    /// Whether sendfile() can be used on this connection
    ///
    /// Returns true only if:
    /// - Not a TLS connection
    /// - Zero-copy is enabled
    /// - Sendfile is enabled in config
    /// - We have a valid socket fd
    pub fn can_sendfile(&self) -> bool {
        !self.is_tls
            && self.zerocopy_config.enabled
            && self.zerocopy_config.enable_sendfile
            && self.socket_fd.is_some()
    }

    /// Get the socket fd for sendfile operations
    pub fn get_socket_fd(&self) -> Option<RawFd> {
        self.socket_fd
    }

    /// Whether memory-mapped I/O can be used
    ///
    /// Works for both TLS and plain TCP connections.
    pub fn can_mmap(&self) -> bool {
        self.zerocopy_config.enabled && self.zerocopy_config.enable_mmap
    }

    /// Whether buffer pooling is enabled
    pub fn can_use_buffer_pool(&self) -> bool {
        self.zerocopy_config.enabled && self.zerocopy_config.enable_buffer_pool
    }

    /// Whether response caching is enabled
    pub fn can_use_response_cache(&self) -> bool {
        self.zerocopy_config.enabled && self.zerocopy_config.enable_response_cache
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> ZeroCopyConfig {
        ZeroCopyConfig::default()
    }

    #[test]
    fn test_plain_tcp_context_without_fd() {
        let addr = "127.0.0.1:9092".parse().ok();
        let ctx = ConnectionContext::plain_tcp(addr, test_config());

        assert!(!ctx.is_tls);
        // Without socket_fd, sendfile is not available
        assert!(!ctx.can_sendfile());
        assert!(ctx.socket_fd.is_none());
        assert!(ctx.can_mmap());
        assert!(ctx.can_use_buffer_pool());
    }

    #[cfg(unix)]
    #[test]
    fn test_plain_tcp_context_with_fd() {
        let addr = "127.0.0.1:9092".parse().ok();
        // Use a fake fd for testing (not actually valid, but tests the logic)
        let ctx = ConnectionContext::plain_tcp_with_fd(addr, test_config(), 42);

        assert!(!ctx.is_tls);
        assert!(ctx.can_sendfile()); // With socket_fd, sendfile is available
        assert_eq!(ctx.socket_fd, Some(42));
        assert_eq!(ctx.get_socket_fd(), Some(42));
        assert!(ctx.can_mmap());
        assert!(ctx.can_use_buffer_pool());
    }

    #[test]
    fn test_tls_context_disables_sendfile() {
        let ctx = ConnectionContext::tls(None, test_config());

        assert!(ctx.is_tls);
        assert!(!ctx.can_sendfile()); // Sendfile disabled for TLS
        assert!(ctx.socket_fd.is_none()); // No socket_fd for TLS
        assert!(ctx.can_mmap()); // mmap still works
        assert!(ctx.can_use_buffer_pool()); // Buffer pool still works
    }

    #[test]
    fn test_master_switch_disables_all() {
        let mut config = test_config();
        config.enabled = false;

        let ctx = ConnectionContext::plain_tcp(None, config);

        assert!(!ctx.can_sendfile());
        assert!(!ctx.can_mmap());
        assert!(!ctx.can_use_buffer_pool());
        assert!(!ctx.can_use_response_cache());
    }

    #[cfg(unix)]
    #[test]
    fn test_sendfile_requires_all_conditions() {
        // Test that sendfile requires: !is_tls && enabled && enable_sendfile && socket_fd.is_some()

        // Missing socket_fd
        let ctx1 = ConnectionContext::plain_tcp(None, test_config());
        assert!(!ctx1.can_sendfile());

        // Is TLS (even with fd, it would be ignored)
        let ctx2 = ConnectionContext::tls(None, test_config());
        assert!(!ctx2.can_sendfile());

        // Disabled via config
        let mut config = test_config();
        config.enable_sendfile = false;
        let ctx3 = ConnectionContext::plain_tcp_with_fd(None, config, 42);
        assert!(!ctx3.can_sendfile());

        // Master switch disabled
        let mut config = test_config();
        config.enabled = false;
        let ctx4 = ConnectionContext::plain_tcp_with_fd(None, config, 42);
        assert!(!ctx4.can_sendfile());

        // All conditions met
        let ctx5 = ConnectionContext::plain_tcp_with_fd(None, test_config(), 42);
        assert!(ctx5.can_sendfile());
    }
}
