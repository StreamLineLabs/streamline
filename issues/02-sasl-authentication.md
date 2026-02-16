# Implement SASL Authentication

## Summary

Add authentication support using SASL (Simple Authentication and Security Layer) to control access to the Streamline server. Currently, any client can connect without credentials.

## Current State

- `src/protocol/kafka.rs:43-84` - Connections accepted without any authentication
- No user/credential management exists
- No authentication-related error codes returned

## Requirements

### Supported SASL Mechanisms

1. **SASL/PLAIN** (username/password) - Priority 1
2. **SASL/SCRAM-SHA-256** - Priority 2
3. **SASL/SCRAM-SHA-512** - Priority 2

### Configuration Options

Add to `src/config/mod.rs`:
```rust
pub struct AuthConfig {
    /// Enable authentication (default: false)
    pub enabled: bool,
    /// SASL mechanisms to enable
    pub sasl_mechanisms: Vec<String>,
    /// Path to users file (YAML/JSON format)
    pub users_file: Option<PathBuf>,
    /// Allow anonymous connections (when auth enabled but no creds provided)
    pub allow_anonymous: bool,
}
```

Users file format:
```yaml
users:
  - username: admin
    password_hash: "$argon2id$..."  # Argon2 hashed
    permissions: ["admin"]
  - username: producer
    password_hash: "$argon2id$..."
    permissions: ["produce:*", "describe:*"]
  - username: consumer
    password_hash: "$argon2id$..."
    permissions: ["consume:*", "describe:*"]
```

### Kafka Protocol Implementation

Add handlers in `src/protocol/kafka.rs`:

1. **SaslHandshake (API Key 17)**
   - Return supported SASL mechanisms
   - Version range: 0-2

2. **SaslAuthenticate (API Key 36)**
   - Process SASL authentication frames
   - Return auth result with session token
   - Version range: 0-2

Update `handle_api_versions()` at line 218 to advertise:
```rust
ApiVersion::default()
    .with_api_key(ApiKey::SaslHandshake as i16)
    .with_min_version(0)
    .with_max_version(2),
ApiVersion::default()
    .with_api_key(ApiKey::SaslAuthenticate as i16)
    .with_min_version(0)
    .with_max_version(2),
```

### Implementation Tasks

1. Add `argon2` crate for password hashing
2. Create `src/auth/mod.rs` module structure:
   - `src/auth/sasl.rs` - SASL mechanism implementations
   - `src/auth/users.rs` - User credential storage/loading
   - `src/auth/session.rs` - Authenticated session management
3. Implement `SaslHandshakeRequest`/`SaslHandshakeResponse` handlers
4. Implement `SaslAuthenticateRequest`/`SaslAuthenticateResponse` handlers
5. Track authenticated state per connection in `KafkaHandler`
6. Reject unauthenticated requests when auth required (return error code 58: `SASL_AUTHENTICATION_FAILED`)
7. Add CLI command: `streamline-cli users add/remove/list`
8. Add integration tests

### Kafka Client Compatibility

Must work with:
```properties
security.protocol=SASL_PLAINTEXT  # or SASL_SSL
sasl.mechanism=PLAIN
sasl.username=myuser
sasl.password=mypassword
```

### Acceptance Criteria

- [ ] SASL/PLAIN authentication works
- [ ] Invalid credentials return proper Kafka error (code 58)
- [ ] Unauthenticated requests rejected when auth enabled
- [ ] Users can be added/removed via CLI
- [ ] Passwords stored securely (Argon2 hashed)
- [ ] Session maintained across requests on same connection
- [ ] Compatible with kafka-console-producer/consumer with SASL

### Related Files

- `src/protocol/kafka.rs:218-246` - API versions (add SASL APIs)
- `src/protocol/kafka.rs:127-201` - Message routing (add SASL handlers)
- `src/cli.rs` - Add user management commands
- `src/error.rs` - Add authentication error variants

## Labels

`enhancement`, `security`, `production-readiness`
