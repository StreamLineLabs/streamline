# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.2.x   | :white_check_mark: |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

**Please do NOT create public GitHub issues for security vulnerabilities.**

To report a security vulnerability, please email **security@streamline.dev** with:

- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Any suggested fixes (optional)

We will acknowledge receipt within 48 hours and provide a detailed response within 7 days.

## Security Features

Streamline includes the following security features:

### Transport Security

- **TLS 1.2/1.3** for all client connections
- **mTLS (mutual TLS)** support for client certificate verification
- **Inter-broker TLS** for cluster communication
- See [TLS Guide](docs/TLS_GUIDE.md) for configuration details

### Authentication

*Requires `auth` feature flag: `cargo build --features auth`*

- **SASL/PLAIN** authentication (development only)
- **SASL/SCRAM-SHA-256** and **SASL/SCRAM-SHA-512** (production)
- **OAuth 2.0 / OIDC** with JWKS validation
- Password hashing with **Argon2**
- Session management and delegation tokens
- Configurable via `--auth-enabled` and `--auth-users-file`

### Authorization

*Requires `auth` feature flag: `cargo build --features auth`*

- **ACL-based authorization** for fine-grained access control
- **RBAC (Role-Based Access Control)** for simplified management
- Per-topic access control
- Super user configuration for administrative access
- Configurable via `--acl-enabled` and `--acl-file`

### Encryption at Rest

*Requires `encryption` feature flag: `cargo build --features encryption`*

- **AES-256-GCM** encryption for stored data
- Configurable via `--encryption-enabled` and `--encryption-key-file`

### Audit Logging

- Security event logging for authentication attempts
- ACL decision logging
- Connection tracking
- Configurable via `--audit-enabled`

### Resource Limits

- Connection limits (total and per-IP)
- Request size limits
- Connection idle timeout
- Rate limiting for producers

## Security Best Practices

1. **Always enable TLS in production**
   ```bash
   streamline --tls-enabled --tls-cert server.crt --tls-key server.key
   ```

2. **Enable authentication** - A warning is logged when authentication is disabled in production
   ```bash
   streamline --auth-enabled --auth-users-file users.yaml
   ```

3. **Use SCRAM authentication** for stronger password verification
   ```bash
   streamline --auth-sasl-mechanisms SCRAM-SHA-256
   ```

4. **Enable ACLs** for fine-grained access control
   ```bash
   streamline --acl-enabled --acl-file acls.yaml
   ```

5. **Restrict network access** to Kafka (9092) and HTTP (9094) ports

6. **Enable audit logging** for security monitoring
   ```bash
   streamline --audit-enabled --audit-log-path /var/log/streamline/audit.log
   ```

7. **Regular updates** - Keep Streamline updated for security patches

## Dependency Security

We use automated security scanning:

- **cargo-audit** runs in CI to check for known vulnerabilities in dependencies
- Dependencies are regularly updated
- Security advisories are monitored via GitHub Dependabot

## Secure Development

- All code changes require review
- CI pipeline includes security audit checks
- No use of `unsafe` Rust without justification and review
- Input validation at protocol boundaries

## Disclosure Policy

When a security vulnerability is reported:

1. We will confirm receipt within 48 hours
2. We will investigate and determine impact within 7 days
3. We will develop a fix and coordinate disclosure
4. Security fixes will be released as patch versions
5. Public disclosure will occur after the fix is available

## Contact

For security-related inquiries, please email **security@streamline.dev**.
