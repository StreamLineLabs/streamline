# ADR-0018: KMS Plugin Interface

## Status

Proposed (cross-cutting prereq; consumed by M1 + M4)

## Context

Both M1 (Agent Memory Fabric — per-agent envelope keys) and M4 (Event
Attestation — broker signing keys) need a key-management story that:

- Works out of the box with a local fallback (no Vault required for laptop /
  small-ops).
- Plugs into HashiCorp Vault, AWS KMS, GCP KMS, Azure Key Vault for
  enterprise.
- Supports key rotation without consumer downtime.
- Is auditable (every sign / decrypt logged to `__audit` topic).

Existing `streamline/src/auth/` covers SASL/auth; key custody is unrelated.

## Decision

A new `streamline/src/security/` module with:

```rust
pub trait KeyProvider: Send + Sync {
    fn sign(&self, key_id: &str, payload: &[u8]) -> Result<Signature>;
    fn verify(&self, key_id: &str, payload: &[u8], sig: &Signature) -> Result<()>;
    fn encrypt(&self, key_id: &str, plaintext: &[u8]) -> Result<Ciphertext>;
    fn decrypt(&self, key_id: &str, ciphertext: &Ciphertext) -> Result<Vec<u8>>;
    fn current_key_id(&self, alias: &str) -> Result<String>;
}
```

Adapters implemented as feature-gated impls:

| Adapter            | Feature flag    | Ships in v1?              |
|--------------------|-----------------|---------------------------|
| `LocalAge`         | (default)       | Yes — fallback            |
| `Vault`            | `kms-vault`     | Yes — enterprise default  |
| `AwsKms`           | `kms-aws`       | Phase 2                   |
| `GcpKms`           | `kms-gcp`       | Phase 2                   |
| `AzureKeyVault`    | `kms-azure`     | Phase 2                   |

- **`LocalAge`** stores keys encrypted under an OS-keyring-derived KEK using
  the `age` format. Acceptable for single-node and CI; not for production
  multi-node.
- **`Vault`** is the recommended production default; rotation handled by
  Vault TTL.
- **Audit:** every operation emits an event to `__audit.security` with key
  ID, op, caller, timestamp; never the plaintext.

**Open question (deferred):** whether to ship the AWS/GCP/Azure adapters in
v1 alongside Vault or wait for Phase 2. Plan currently says Phase 2 to keep
M1+M4 P1 scope tight.

## Consequences

### Positive
- One trait, multiple backends — keeps the rest of the codebase agnostic.
- Local fallback keeps the laptop dev loop intact (single-binary promise).
- Audit topic is consistent regardless of backend.

### Negative
- Local-age fallback is a foot-gun if used in production; mitigation is to
  warn at startup unless user sets `STREAMLINE_ALLOW_LOCAL_KMS=1`.
- Five backends multiplies QA surface; gated behind features so default
  build only carries one.

### Neutral
- Crypto primitives consolidated on `ed25519-dalek` (sign), `age` (envelope
  encrypt), `ring` (HMAC for audit chain).

## References

- age: <https://age-encryption.org>
- HashiCorp Vault transit secrets: <https://developer.hashicorp.com/vault/docs/secrets/transit>
- AWS KMS: <https://aws.amazon.com/kms/>
- Internal: `MOONSHOT_PLAN.md` Features M1, M4
