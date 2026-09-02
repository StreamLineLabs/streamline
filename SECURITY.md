# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.2.x   | :white_check_mark: |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

**Please do NOT create public GitHub issues for security vulnerabilities.**

To report a security vulnerability, please email **security@streamlinelabs.dev** with:

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

Automated, fail-closed security scanning runs in CI
(`.github/workflows/security-scan.yml`):

- **cargo-audit** (`cargo audit`) — known vulnerability advisories in the
  committed lockfile. Any vulnerability or tool failure fails the build;
  warning-class findings remain visible.
- **cargo-deny** (`--all-features check advisories bans licenses sources`) —
  enabled-feature advisory policy, licenses, banned crates and source
  restrictions, per `deny.toml`. Any denial fails the build.
- **clippy** with `-D warnings` across all features.
- An `unsafe`-without-`// SAFETY:` budget that fails the build when exceeded.

The same audit and dependency policy checks gate every release through
`.github/workflows/release-gate.yml`, which `release.yml` requires before it
publishes anything.

Dependency updates are proposed automatically by GitHub Dependabot
(`.github/dependabot.yml`).

### Known state of the dependency audit

These checks are fail-closed, which means they report the real state of the
tree rather than a green tick. The release-preparation dependency update
cleared the AWS-LC, bytes, crossbeam, h2, LZ4, PostgreSQL, Quinn, rustls,
tar/time, and Wasmtime vulnerability findings.

At the time of writing, `cargo audit` reports six remaining vulnerability
findings: four `quick-xml` advisories reached through the experimental
Iceberg/Delta dependency stacks, plus two `rkyv 0.7` findings that are recorded
in the lockfile but have no enabled dependency path. The all-feature
`cargo-deny` gate also reports the reachable `quick-xml` findings and the
directly used but unmaintained `bincode` and `rustls-pemfile` crates.

**A release cannot pass the release gate until these are remediated.** Remediate
with `cargo update -p <crate>` (or a minor-version bump in `Cargo.toml` where
the fix is behind a semver bump), then re-run `cargo audit` and
`cargo deny check`. Advisories that genuinely do not apply may be waived, one at
a time and with a written justification, via `[advisories] ignore` in
`deny.toml` — never by weakening the CI invocation. No such waiver is present
for the remaining release blockers.

Note: this project is pure Rust. A previous CodeQL workflow targeted `cpp` and
reported on a language this repository does not contain. CodeQL now analyzes
the Rust source and GitHub Actions workflows in `.github/workflows/codeql.yml`;
the Rust-native tooling above remains the fail-closed dependency and lint layer.

## Secure Development

- All code changes require review
- CI pipeline includes fail-closed security audit checks (see above)
- No use of `unsafe` Rust without a `// SAFETY:` justification and review; the
  count of undocumented `unsafe` blocks is capped in CI
- Input validation at protocol boundaries

### Release integrity

Releases produced by `.github/workflows/release.yml`:

- are built only after the full release gate passes (stability, fail-closed
  security audit, Kafka compatibility, tests, documentation);
- ship a `checksums.txt` covering every published artifact, signed with
  keyless Sigstore `cosign`, alongside per-artifact signatures;
- ship a CycloneDX SBOM (`sbom.cdx.json`) generated with `cargo-cyclonedx`,
  which is validated to be non-empty — the release fails if it is not produced;
- carry GitHub-native build-provenance and SBOM attestations, which are
  required steps, not best-effort ones;
- carry SLSA Level 3 provenance for stable (non pre-release) tags.

No SPDX SBOM is published; only CycloneDX.

## Disclosure Policy

When a security vulnerability is reported:

1. We will confirm receipt within 48 hours
2. We will investigate and determine impact within 7 days
3. We will develop a fix and coordinate disclosure
4. Security fixes will be released as patch versions
5. Public disclosure will occur after the fix is available

## Vulnerability Response Process

### Disclosure Timeline
- **Acknowledgment:** Within 48 hours of report
- **Initial Assessment:** Within 5 business days
- **Fix Development:** Based on severity (see below)
- **Public Disclosure:** 90 days after report, or when fix is released (whichever is first)

### Severity Classification and Patch SLA
| Severity | CVSS Score | Patch SLA | Example |
|----------|-----------|-----------|---------|
| Critical | 9.0-10.0 | 48 hours | Remote code execution, auth bypass |
| High | 7.0-8.9 | 7 days | Privilege escalation, data exposure |
| Medium | 4.0-6.9 | 30 days | DoS, information disclosure |
| Low | 0.1-3.9 | Next release | Minor info leak, non-default config |

### Reporting
- **Email:** security@streamline.dev
- **PGP Key:** Available at https://streamline.dev/.well-known/security.txt
- **Bug Bounty:** Not currently offered

### Process
1. Reporter submits vulnerability to security@streamlinelabs.dev
2. Team acknowledges within 48 hours with tracking ID
3. Team assesses severity and impact within 5 business days
4. Team develops and tests fix per SLA
5. Fix is released as a security patch
6. CVE is filed (if applicable)
7. Advisory is published on GitHub Security Advisories
8. Reporter is credited (unless they prefer anonymity)

## Contact

For security-related inquiries, please email **security@streamlinelabs.dev**.

## Security Audit

For details on our security audit scope and methodology, see [Security Audit Scope](docs/SECURITY_AUDIT_SCOPE.md).
