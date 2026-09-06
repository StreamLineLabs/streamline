//! Event attestation: Ed25519 signing + verification of accepted records
//! (M4 P1).
//!
//! When a topic has `contracts.attest = true`, every accepted append is
//! signed and the signature is added as a Kafka header `streamline-attest`.
//! Verifiers (SDKs, downstream services) can fetch the broker's public key
//! via `/v1/keys` and verify offline.
//!
//! Format: SLSA in-toto attestation envelope (DSSE), see
//! `docs/adr/0017-event-attestation-format.md`. The current canonical
//! form is a deterministic pipe-delimited string; JSON canonicalization
//! lands in M4 P2 when Schema Registry interop is wired.
//!
//! Stability tier: **Experimental**.

use sha2::{Digest, Sha256};

use crate::security::kms::Signature;
use crate::security::{KeyProvider, KmsError};

/// Header name carrying the DSSE-encoded signature.
pub const ATTEST_HEADER: &str = "streamline-attest";

/// Minimal attestation envelope (Subject + Predicate fields are inlined for
/// the Streamline single-event case).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attestation {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    /// Hex-encoded SHA-256 of the record value.
    pub payload_sha256: String,
    /// Producer-claimed schema id (from `schema-id` header) or 0.
    pub schema_id: u32,
    pub timestamp_ms: i64,
    pub key_id: String,
}

impl Attestation {
    /// Build an attestation for a record with `value` as the raw payload bytes.
    pub fn for_record(
        topic: &str,
        partition: i32,
        offset: i64,
        value: &[u8],
        schema_id: u32,
        timestamp_ms: i64,
        key_id: &str,
    ) -> Self {
        Self {
            topic: topic.to_string(),
            partition,
            offset,
            payload_sha256: hex_sha256(value),
            schema_id,
            timestamp_ms,
            key_id: key_id.to_string(),
        }
    }

    /// Canonical bytes to sign — deterministic concat of all fields.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        format!(
            "{}|{}|{}|{}|{}|{}|{}",
            self.topic,
            self.partition,
            self.offset,
            self.payload_sha256,
            self.schema_id,
            self.timestamp_ms,
            self.key_id
        )
        .into_bytes()
    }
}

/// Compute lowercase hex SHA-256 of `value`.
pub fn hex_sha256(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let mut s = String::with_capacity(digest.len() * 2);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Sign an attestation with the configured key provider. Returns the
/// raw signature bytes that go into the `streamline-attest` header.
pub fn sign_attestation(
    provider: &dyn KeyProvider,
    key_id: &str,
    att: &Attestation,
) -> Result<Vec<u8>, KmsError> {
    let bytes = att.canonical_bytes();
    let sig = provider.sign(&key_id.to_string(), &bytes)?;
    Ok(sig.bytes)
}

/// Verify an attestation: rebuild the canonical bytes from the envelope and
/// check the signature against the broker's public key for `key_id`. The
/// `algorithm` argument identifies the signature curve so the provider can
/// pick the right verification routine.
pub fn verify_attestation(
    provider: &dyn KeyProvider,
    att: &Attestation,
    sig_bytes: &[u8],
    algorithm: crate::security::kms::Algorithm,
) -> Result<(), KmsError> {
    let bytes = att.canonical_bytes();
    let signature = Signature {
        algorithm,
        bytes: sig_bytes.to_vec(),
    };
    provider.verify(&att.key_id, &bytes, &signature)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::kms::Algorithm;
    use crate::security::local_age::LocalAgeProvider;

    fn provider_with_key(key_id: &str) -> LocalAgeProvider {
        let p = LocalAgeProvider::new();
        p.register_key(key_id, Algorithm::Ed25519).expect("keygen");
        p
    }

    fn att(key_id: &str, value: &[u8]) -> Attestation {
        Attestation::for_record("orders", 0, 42, value, 7, 1_700_000_000_000, key_id)
    }

    #[test]
    fn canonical_bytes_are_deterministic() {
        let a = att("k", b"x");
        assert_eq!(a.canonical_bytes(), att("k", b"x").canonical_bytes());
    }

    #[test]
    fn header_name_constant() {
        assert_eq!(ATTEST_HEADER, "streamline-attest");
    }

    #[test]
    fn payload_sha256_is_lowercase_hex_64_chars() {
        let a = att("k", b"hello");
        assert_eq!(a.payload_sha256.len(), 64);
        assert!(a
            .payload_sha256
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    #[test]
    fn sign_then_verify_roundtrip() {
        let p = provider_with_key("broker-1");
        let a = att("broker-1", b"payload");
        let sig = sign_attestation(&p, "broker-1", &a).expect("sign");
        verify_attestation(&p, &a, &sig, Algorithm::Ed25519).expect("verify");
    }

    #[test]
    fn verify_rejects_tampered_offset() {
        let p = provider_with_key("broker-1");
        let a = att("broker-1", b"payload");
        let sig = sign_attestation(&p, "broker-1", &a).expect("sign");
        let mut tampered = a.clone();
        tampered.offset += 1;
        let err = verify_attestation(&p, &tampered, &sig, Algorithm::Ed25519).unwrap_err();
        assert!(matches!(err, KmsError::InvalidSignature));
    }

    #[test]
    fn verify_rejects_tampered_payload_hash() {
        let p = provider_with_key("broker-1");
        let a = att("broker-1", b"original");
        let sig = sign_attestation(&p, "broker-1", &a).expect("sign");
        // Forge an attestation that claims to be over different bytes.
        let mut forged = a.clone();
        forged.payload_sha256 = hex_sha256(b"different");
        let err = verify_attestation(&p, &forged, &sig, Algorithm::Ed25519).unwrap_err();
        assert!(matches!(err, KmsError::InvalidSignature));
    }

    #[test]
    fn verify_rejects_unknown_key() {
        let p = provider_with_key("broker-1");
        let mut a = att("broker-1", b"payload");
        let sig = sign_attestation(&p, "broker-1", &a).expect("sign");
        // Re-tag the attestation as if it had been signed by another broker.
        a.key_id = "broker-other".into();
        let err = verify_attestation(&p, &a, &sig, Algorithm::Ed25519).unwrap_err();
        assert!(matches!(err, KmsError::KeyNotFound(_)));
    }
}
