# Security Audit — Scope Document

## Audit Type
External penetration test + source code audit

## Scope

### In-Scope Components
1. **Authentication system** (src/auth/)
   - SASL PLAIN, SCRAM-SHA-256/512, OAuth/OIDC
   - Session management and token lifecycle
   - Delegation tokens
2. **Authorization system** (src/auth/acl.rs, rbac.rs)
   - ACL enforcement across all API endpoints
   - RBAC role inheritance
3. **Network layer** (src/server/, src/protocol/)
   - Kafka protocol parsing (50+ APIs)
   - HTTP API (Axum) — injection attacks, CORS
   - TLS configuration and certificate validation
4. **Encryption at rest** (src/encryption/)
   - AES-256-GCM implementation
   - Key management
5. **Data access patterns**
   - Cross-tenant data isolation (src/multitenancy/)
   - Consumer group isolation

### Out of Scope
- Third-party dependencies (covered by cargo-deny)
- Cloud infrastructure (streamline-cloud)
- Client SDKs (separate audit track)

### Focus Areas
| Area | Priority | Risk |
|------|----------|------|
| Auth bypass | Critical | Remote code execution |
| Privilege escalation | Critical | Data exposure |
| Kafka protocol fuzzing | High | DoS, buffer overflow |
| HTTP injection (XSS, SSRF) | High | Information disclosure |
| TLS downgrade attacks | High | MitM |
| Memory safety (unsafe blocks) | Medium | RCE |
| DoS via resource exhaustion | Medium | Availability |

### Deliverables
1. Executive summary with risk ratings
2. Detailed finding report (CVSS scored)
3. Remediation recommendations
4. Re-test validation (after fixes)

### Timeline
- Audit window: 2 weeks
- Report delivery: 1 week after audit
- Remediation: Per severity SLA (see SECURITY.md)
- Re-test: 1 week after remediation
