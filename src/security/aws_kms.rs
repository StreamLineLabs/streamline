//! AWS KMS provider (M4 P2).
//!
//! Uses AWS KMS Sign/Verify API with asymmetric signing keys.
//!
//! Current implementation is a realistic stub that validates
//! configuration, returns meaningful errors, and falls back to
//! [`LocalAgeProvider`] for actual cryptographic operations.  When the
//! real AWS SDK is available, the methods should call the KMS
//! `Sign`/`Verify`/`Encrypt`/`Decrypt` APIs via the configured client.
//!
//! Stability tier: **Experimental** (gated by feature `attestation`).

use crate::security::audit::{AuditEvent, KmsOp};
use crate::security::error::{KmsError, KmsResult};
use crate::security::kms::{Algorithm, KeyId, KeyProvider, Signature};
use crate::security::local_age::LocalAgeProvider;

const BACKEND: &str = "aws-kms";

/// Configuration for the AWS KMS backend.
#[derive(Debug, Clone)]
pub struct AwsKmsConfig {
    /// AWS region (e.g. `"us-east-1"`).
    pub region: String,
    /// KMS key ARN or alias (e.g. `"arn:aws:kms:us-east-1:123456:key/abcd-1234"`
    /// or `"alias/streamline"`).
    pub key_id: String,
    /// Signing algorithm to request from KMS.  Maps to AWS
    /// `SigningAlgorithmSpec`; currently `ECDSA_SHA_256` (P-256) or
    /// custom Ed25519 via external key store.
    pub signing_algorithm: String,
    /// Optional endpoint override (for LocalStack / testing).
    pub endpoint_url: Option<String>,
    /// Optional AWS profile name (defaults to default credential chain).
    pub profile: Option<String>,
}

impl Default for AwsKmsConfig {
    fn default() -> Self {
        Self {
            region: "us-east-1".into(),
            key_id: String::new(),
            signing_algorithm: "ECDSA_SHA_256".into(),
            endpoint_url: None,
            profile: None,
        }
    }
}

impl AwsKmsConfig {
    /// Validate required configuration fields.
    pub fn validate(&self) -> KmsResult<()> {
        if self.region.is_empty() {
            return Err(KmsError::Configuration(
                "aws region must not be empty".into(),
            ));
        }
        if self.key_id.is_empty() {
            return Err(KmsError::Configuration(
                "aws key_id (ARN or alias) must not be empty".into(),
            ));
        }
        if self.signing_algorithm.is_empty() {
            return Err(KmsError::Configuration(
                "aws signing_algorithm must not be empty".into(),
            ));
        }
        Ok(())
    }
}

/// AWS KMS key provider.
///
/// Wraps a [`LocalAgeProvider`] for the actual crypto today.  The real
/// implementation would call `kms:Sign`, `kms:Verify`, `kms:Encrypt`,
/// and `kms:Decrypt` via the AWS SDK for Rust.
pub struct AwsKmsProvider {
    config: AwsKmsConfig,
    /// Fallback: local provider used until the real AWS SDK is wired.
    local: LocalAgeProvider,
}

impl AwsKmsProvider {
    /// Create a new AWS KMS provider.
    ///
    /// Validates the configuration eagerly; returns an error if required
    /// fields are missing.  Registers a default signing key under
    /// `config.key_id` so the local-fallback code path works immediately.
    pub fn new(config: AwsKmsConfig) -> KmsResult<Self> {
        config.validate()?;

        let local = LocalAgeProvider::new();
        local.register_key(&config.key_id, Algorithm::Ed25519)?;
        local.alias("broker-signing", &config.key_id)?;

        Ok(Self { config, local })
    }

    /// Return the configured AWS region.
    pub fn region(&self) -> &str {
        &self.config.region
    }

    /// Return the configured KMS key ARN / alias.
    pub fn key_arn(&self) -> &str {
        &self.config.key_id
    }
}

impl KeyProvider for AwsKmsProvider {
    fn sign(&self, key_id: &KeyId, payload: &[u8]) -> KmsResult<Signature> {
        // Real implementation:
        //   kms::Sign {
        //     KeyId: self.config.key_id,
        //     Message: payload,
        //     MessageType: RAW,
        //     SigningAlgorithm: self.config.signing_algorithm,
        //   }
        let _ev = AuditEvent::now(BACKEND, KmsOp::Sign, key_id, true);
        self.local.sign(key_id, payload)
    }

    fn verify(&self, key_id: &KeyId, payload: &[u8], sig: &Signature) -> KmsResult<()> {
        // Real implementation:
        //   kms::Verify {
        //     KeyId: self.config.key_id,
        //     Message: payload,
        //     MessageType: RAW,
        //     Signature: sig.bytes,
        //     SigningAlgorithm: self.config.signing_algorithm,
        //   }
        let _ev = AuditEvent::now(BACKEND, KmsOp::Verify, key_id, true);
        self.local.verify(key_id, payload, sig)
    }

    fn encrypt(&self, key_id: &KeyId, plaintext: &[u8]) -> KmsResult<Vec<u8>> {
        // Real implementation:
        //   kms::Encrypt {
        //     KeyId: self.config.key_id,
        //     Plaintext: plaintext,
        //     EncryptionAlgorithm: SYMMETRIC_DEFAULT,
        //   }
        let _ev = AuditEvent::now(BACKEND, KmsOp::Encrypt, key_id, true);
        self.local.encrypt(key_id, plaintext)
    }

    fn decrypt(&self, key_id: &KeyId, ciphertext: &[u8]) -> KmsResult<Vec<u8>> {
        // Real implementation:
        //   kms::Decrypt {
        //     KeyId: self.config.key_id,
        //     CiphertextBlob: ciphertext,
        //     EncryptionAlgorithm: SYMMETRIC_DEFAULT,
        //   }
        let _ev = AuditEvent::now(BACKEND, KmsOp::Decrypt, key_id, true);
        self.local.decrypt(key_id, ciphertext)
    }

    fn current_key_id(&self, alias: &str) -> KmsResult<KeyId> {
        self.local.current_key_id(alias)
    }

    fn backend_name(&self) -> &'static str {
        BACKEND
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> AwsKmsConfig {
        AwsKmsConfig {
            region: "us-east-1".into(),
            key_id: "arn:aws:kms:us-east-1:123456789012:key/abcd-1234-efgh-5678".into(),
            signing_algorithm: "ECDSA_SHA_256".into(),
            endpoint_url: None,
            profile: None,
        }
    }

    #[test]
    fn config_validation_accepts_valid() {
        assert!(valid_config().validate().is_ok());
    }

    #[test]
    fn config_rejects_empty_region() {
        let mut cfg = valid_config();
        cfg.region = String::new();
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, KmsError::Configuration(_)));
    }

    #[test]
    fn config_rejects_empty_key_id() {
        let mut cfg = valid_config();
        cfg.key_id = String::new();
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, KmsError::Configuration(_)));
    }

    #[test]
    fn config_rejects_empty_signing_algorithm() {
        let mut cfg = valid_config();
        cfg.signing_algorithm = String::new();
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, KmsError::Configuration(_)));
    }

    #[test]
    fn provider_creation_succeeds_with_valid_config() {
        let provider = AwsKmsProvider::new(valid_config());
        assert!(provider.is_ok());
    }

    #[test]
    fn backend_name_is_aws_kms() {
        let provider = AwsKmsProvider::new(valid_config()).unwrap();
        assert_eq!(provider.backend_name(), "aws-kms");
    }

    #[test]
    fn region_and_key_arn_accessors() {
        let provider = AwsKmsProvider::new(valid_config()).unwrap();
        assert_eq!(provider.region(), "us-east-1");
        assert!(provider.key_arn().starts_with("arn:aws:kms:"));
    }

    #[test]
    fn sign_verify_roundtrip_via_fallback() {
        let cfg = valid_config();
        let key = cfg.key_id.clone();
        let provider = AwsKmsProvider::new(cfg).unwrap();
        let payload = b"hello aws kms";
        let sig = provider.sign(&key, payload).expect("sign");
        provider.verify(&key, payload, &sig).expect("verify");
    }

    #[test]
    fn encrypt_decrypt_roundtrip_via_fallback() {
        let cfg = valid_config();
        let key = cfg.key_id.clone();
        let provider = AwsKmsProvider::new(cfg).unwrap();
        let plaintext = b"secret data for aws";
        let ct = provider.encrypt(&key, plaintext).expect("encrypt");
        let pt = provider.decrypt(&key, &ct).expect("decrypt");
        assert_eq!(pt, plaintext);
    }

    #[test]
    fn current_key_id_resolves_alias() {
        let cfg = valid_config();
        let expected_key = cfg.key_id.clone();
        let provider = AwsKmsProvider::new(cfg).unwrap();
        let kid = provider.current_key_id("broker-signing").expect("alias");
        assert_eq!(kid, expected_key);
    }

    #[test]
    fn config_with_endpoint_override() {
        let mut cfg = valid_config();
        cfg.endpoint_url = Some("http://localhost:4566".into());
        let provider = AwsKmsProvider::new(cfg);
        assert!(provider.is_ok());
    }

    #[test]
    fn config_with_profile() {
        let mut cfg = valid_config();
        cfg.profile = Some("streamline-prod".into());
        let provider = AwsKmsProvider::new(cfg);
        assert!(provider.is_ok());
    }

    #[test]
    fn default_config_has_us_east_1() {
        let cfg = AwsKmsConfig::default();
        assert_eq!(cfg.region, "us-east-1");
        assert_eq!(cfg.signing_algorithm, "ECDSA_SHA_256");
    }
}
