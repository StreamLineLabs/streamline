# Implement TLS/SSL Support for Secure Connections

## Summary

Add TLS/SSL support to encrypt traffic between Kafka clients and Streamline server. Currently, all communication is unencrypted plaintext over TCP.

## Current State

- `src/server/mod.rs:55` - Server binds directly to TCP without TLS wrapper
- `src/protocol/kafka.rs:43` - `handle_connection` accepts raw `TcpStream`
- No TLS configuration options exist in `ServerConfig`

## Requirements

### Configuration Options

Add to `src/config/mod.rs`:
```rust
pub struct TlsConfig {
    /// Enable TLS (default: false)
    pub enabled: bool,
    /// Path to certificate file (PEM format)
    pub cert_path: PathBuf,
    /// Path to private key file (PEM format)
    pub key_path: PathBuf,
    /// Minimum TLS version (1.2 or 1.3)
    pub min_version: String,
    /// Enable client certificate verification (mTLS)
    pub require_client_cert: bool,
    /// Path to CA certificate for client verification
    pub ca_cert_path: Option<PathBuf>,
}
```

CLI arguments:
- `--tls-enabled` / `STREAMLINE_TLS_ENABLED`
- `--tls-cert` / `STREAMLINE_TLS_CERT`
- `--tls-key` / `STREAMLINE_TLS_KEY`
- `--tls-min-version` / `STREAMLINE_TLS_MIN_VERSION`
- `--tls-require-client-cert` / `STREAMLINE_TLS_REQUIRE_CLIENT_CERT`
- `--tls-ca-cert` / `STREAMLINE_TLS_CA_CERT`

### Implementation Tasks

1. Add `rustls` and `tokio-rustls` dependencies to `Cargo.toml`
2. Create `src/server/tls.rs` module for TLS configuration loading
3. Modify `Server::run()` to wrap `TcpListener` with `TlsAcceptor` when TLS enabled
4. Update `KafkaHandler::handle_connection()` to accept `tokio_rustls::server::TlsStream<TcpStream>` or use generic `AsyncRead + AsyncWrite`
5. Add TLS status to server banner output
6. Add integration tests with self-signed certificates

### Kafka Client Compatibility

Must support standard Kafka client TLS configuration:
```properties
security.protocol=SSL
ssl.truststore.location=/path/to/truststore.jks
ssl.keystore.location=/path/to/keystore.jks
```

### Acceptance Criteria

- [ ] Server starts with TLS when configured
- [ ] Standard Kafka clients (librdkafka, kafka-python, franz-go) connect over TLS
- [ ] mTLS (mutual TLS) works when enabled
- [ ] Graceful error when cert/key files missing or invalid
- [ ] TLS 1.2 and 1.3 supported
- [ ] Performance impact documented (<5% overhead expected)

### Related Files

- `src/server/mod.rs` - Server implementation
- `src/protocol/kafka.rs` - Connection handling
- `src/config/mod.rs` - Configuration
- `Dockerfile` - May need to mount certificates

## Labels

`enhancement`, `security`, `production-readiness`
