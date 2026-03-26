# Streamline Protocol Extensions

> Cross-cutting prereq deliverable (`pre-cap-negotiation` in session
> `plan.md`). Authoritative registry for Streamline-specific Kafka API key
> extensions.

## Background

Streamline speaks the Kafka wire protocol and supports the standard Apache
Kafka `ApiVersions` (API key 18) handshake. Standard Apache Kafka API keys
are in the range **0–N** (currently up to ~75 as of Kafka 3.7).

To layer Streamline-only capabilities (semantic search, attestation
verification, branch operations, agent-memory tools) onto the same wire
protocol *without colliding with future Apache Kafka assignments*, we
reserve a private range and negotiate availability via `ApiVersions`.

## Reserved Range

| Range        | Owner       | Purpose                                  |
|--------------|-------------|------------------------------------------|
| **0–79**     | Apache Kafka| Standard protocol — never assign here    |
| **80–119**   | Streamline  | Streamline extension API keys            |
| **120–9999** | Reserved    | Do not assign without an ADR             |

Assignments inside 80–119 are tracked in this file; PRs that add a new key
must update this table and the corresponding handler module.

## Current Assignments

| API Key | Name                  | Min Ver | Max Ver | Module                                          | Owner Moonshot |
|---------|-----------------------|---------|---------|-------------------------------------------------|----------------|
| 80      | `StreamlineSearch`    | 0       | 0       | `streamline/src/protocol/api/search.rs` (M2 P1) | M2             |
| 81      | `StreamlineAttestVerify` | 0    | 0       | `streamline/src/protocol/api/attest.rs` (M4 P2) | M4             |
| 82      | `StreamlineBranch`    | 0       | 0       | `streamline/src/protocol/api/branch.rs` (M5 P1) | M5             |
| 83      | `StreamlineMemRecall` | 0       | 0       | `streamline/src/protocol/api/mem.rs` (M1 P2)    | M1             |
| 84      | `StreamlineMemRemember` | 0     | 0       | same as 83                                      | M1             |
| 85–119  | *unassigned*          |         |         |                                                 |                |

## Negotiation Rules

1. Servers MUST advertise only the extension keys they actually serve in the
   `ApiVersions` response. A key advertised but not handled is a bug.
2. Clients MUST treat extension keys as optional. Missing key = feature
   unavailable; clients should degrade gracefully (e.g., M2 SDKs fall back
   to BM25-only search if key 80 is not advertised).
3. Server responses to an unknown key MUST be Apache-Kafka-compatible
   `UNSUPPORTED_VERSION` (error code 35) so vanilla Kafka clients are not
   surprised.
4. Stability tier (per `streamline/docs/API_STABILITY.md`) of each key is
   recorded in the table above as the "Min/Max Ver" pair stabilizes.

## Workflow for Adding a New Key

1. Open an ADR proposing the key (template at `docs/adr/template.md`).
2. Reserve the next free key in the range 80–119 by editing this file.
3. Implement the handler under `streamline/src/protocol/api/`.
4. Wire the key into `ApiVersions` advertising in
   `streamline/src/protocol/handlers/api_versions.rs`.
5. Add SDK feature-detection + graceful fallback in at least one SDK before
   the key leaves Experimental.

## References

- Apache Kafka protocol guide: <https://kafka.apache.org/protocol>
- ADR-0001 Kafka Protocol Compatibility
- `streamline/docs/API_STABILITY.md`
- `streamline/docs/SDK_CONFORMANCE_SPEC.md`
