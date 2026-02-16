# ADR-0005: Managed Cloud Service

## Status

Proposed

## Context

Self-hosted streaming infrastructure requires significant operational expertise — capacity planning, upgrades, monitoring, security patching, and disaster recovery. Competitors (Confluent Cloud, Redpanda Cloud, Amazon MSK) all offer managed services that eliminate this burden.

Streamline's single-binary architecture (see [ADR-0002](0002-single-binary-architecture.md)) provides a structural advantage: provisioning an instance is orders of magnitude simpler than deploying a multi-node Kafka cluster. A binary under 50MB with zero external dependencies enables faster provisioning and lower per-tenant costs.

A managed service also provides the revenue model needed for sustainable open-source development.

## Decision

Build **Streamline Cloud**, a managed multi-tenant streaming service with control plane / data plane separation, leveraging the existing Kubernetes operator for orchestration.

### Control Plane

Manages tenant lifecycle, authentication, and billing:

- **Tenant Manager**: Organizations, users, RBAC (Owner/Admin/Editor/Viewer)
- **Provisioner**: Creates Streamline instances via the Kubernetes operator CRDs
- **API Gateway**: Rate limiting, authentication, routing
- **Billing**: Usage metering with Stripe integration
- **Key Management**: API key generation, rotation, revocation

### Data Plane

Two isolation models:

- **Shared clusters** (Free/Pro): Namespace-level isolation via topic prefixes, SASL/SCRAM auth, per-tenant resource quotas
- **Dedicated instances** (Pro/Enterprise): Separate pods with dedicated CPU, memory, storage, and optional customer-managed encryption keys

### Service Tiers

| | Free | Pro | Enterprise |
|---|---|---|---|
| Isolation | Shared | Dedicated | Dedicated + VPC |
| Throughput | 1 MB/s | 50 MB/s | Custom |
| Storage | 1 GB | Up to 10 TB | Custom |
| SLA | Best effort | 99.95% | 99.99% |
| Compliance | — | — | SOC 2, HIPAA, GDPR |

### Security

- TLS 1.3 in transit, AES-256-GCM at rest with automatic key rotation
- Kubernetes NetworkPolicies for cross-tenant network isolation
- Enterprise: SSO/SAML, BYOK encryption, VPC peering

### Implementation Phases

1. **Phase 1** (Months 1–4): Single-region, API keys, shared + dedicated clusters, billing
2. **Phase 2** (Months 5–8): Multi-region geo-replication, automated failover, backup/restore
3. **Phase 3** (Months 9–12): Self-service portal, enterprise features, marketplace listings

## Consequences

### Positive

- Developers start streaming in minutes instead of days
- Usage-based revenue funds continued open-source development
- Single-binary architecture yields lower per-tenant costs than competitors
- Managed service adoption drives open-source community growth

### Negative

- Significant engineering investment in control plane, billing, and multi-tenancy
- 99.99% SLA requires 24/7 on-call and incident response capability
- Multi-tenancy introduces isolation risks not present in self-hosted deployments
- Must carefully balance managed-only features vs. open-source core

### Neutral

- The Streamline binary remains fully open source; only the control plane is proprietary
- Existing Kubernetes operator is reused, minimizing new orchestration code
