# ADR-0017: Event Attestation Format

## Status

Proposed (M4 P0 deliverable)

## Context

M4 requires every accepted record to carry a verifiable signature so
downstream consumers (and auditors) can prove:

1. The record was admitted by a Streamline broker that knew the contract.
2. The contract version asserted at admit time matches what was checked.
3. The lineage parents asserted by the producer were honored.

Constraints:

- Header overhead must stay small enough not to blow up Kafka message size
  budgets at high fan-out (target: ≤ 256 bytes per record amortized).
- Verification must be possible offline (consumer holds broker public key).
- Format must be SDK-language-agnostic.

## Decision

- **Envelope:** SLSA in-toto v1 attestation envelope
  (DSSE PAE — Pre-Authentication Encoding) so we inherit a standardized
  signing wrapper.
- **Payload:** CBOR map keyed by short ints to keep on-wire bytes low:
  ```
  {1: contract_id, 2: contract_version, 3: schema_hash,
   4: lineage_root, 5: ts_admit_ms, 6: broker_id}
  ```
- **Signature:** Ed25519 (32-byte sig) by default; algorithm field allows
  future P-256 / Dilithium for post-quantum.
- **Header on record:** single Kafka header `streamline-attest`, value =
  base64(DSSE envelope).
- **Per-batch signing:** for high-fanout topics, sign a batch root over a
  Merkle tree of records; per-record header carries `streamline-attest-ref`
  pointing into the batch attestation. Documented as an optimization; v1
  ships per-record only.

Verification crate exposed to SDKs first as Rust + Java (M4 P1
deliverables); other SDK languages follow in M4 P2.

## Consequences

### Positive
- Reuses an existing standard rather than inventing a wire format.
- Short integer keys keep CBOR payload < 96 bytes typical.
- DSSE envelope means tools like `cosign verify` can validate broker
  signatures with stock tooling.

### Negative
- DSSE adds ~40 bytes overhead per record, non-trivial for very small
  events. Mitigated by batched-attest mode (post-v1).
- Ed25519 is not post-quantum; tracked in §M4 risks for future migration.

### Neutral
- Broker key rotation is a separate concern handled by the KMS plugin
  (ADR-0018).

## References

- in-toto attestation: <https://github.com/in-toto/attestation>
- DSSE: <https://github.com/secure-systems-lab/dsse>
- SLSA: <https://slsa.dev>
- `ed25519-dalek`: <https://crates.io/crates/ed25519-dalek>
