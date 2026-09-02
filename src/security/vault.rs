//! HashiCorp Vault KMS provider (M4 P2).
//!
//! Uses Vault Transit engine for signing and verification.
//! Authentication via token, AppRole, or Kubernetes auth.
//!
//! Current implementation is a realistic stub that validates configuration,
//! returns meaningful errors, and falls back to [`LocalAgeProvider`] for
//! actual cryptographic operations.  When a real Vault cluster is
//! available, the `sign` / `verify` / `encrypt` / `decrypt` methods
//! should call the Transit secret-engine HTTP API (`/v1/{mount}/sign/{key}`
//! etc.) via the configured `reqwest` client.
//!
//! Stability tier: **Experimental** (gated by feature `attestation`).

use crate::security::audit::{AuditEvent, KmsOp};
use crate::security::error::{KmsError, KmsResult};
use crate::security::kms::{Algorithm, KeyId, KeyProvider, Signature};
use crate::security::local_age::LocalAgeProvider;

const BACKEND: &str = "vault-transit";

/// Configuration for the Vault Transit KMS backend.
#[derive(Debug, Clone)]
pub struct VaultConfig {
    /// Vault server URL (e.g. `https://vault.corp:8200`).
    pub url: String,
    /// Static token for authentication.  In production, prefer AppRole or
    /// Kubernetes auth — those flows would exchange credentials for a
    /// short-lived token during `VaultKeyProvider::new`.
    pub token: Option<String>,
    /// Transit engine mount path (default `"transit"`).
    pub mount_path: String,
    /// Key name within the Transit engine (default `"streamline"`).
    pub key_name: String,
    /// Vault enterprise namespace (optional).
    pub namespace: Option<String>,
}

impl Default for VaultConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            token: None,
            mount_path: "transit".into(),
            key_name: "streamline".into(),
            namespace: None,
        }
    }
}

impl VaultConfig {
    /// Validate required configuration fields.
    pub fn validate(&self) -> KmsResult<()> {
        if self.url.is_empty() {
            return Err(KmsError::Configuration(
                "vault url must not be empty".into(),
            ));
        }
        if self.token.is_none() {
            return Err(KmsError::Configuration(
                "vault token is required (AppRole/K8s auth not yet implemented)".into(),
            ));
        }
        if self.mount_path.is_empty() {
            return Err(KmsError::Configuration(
                "vault mount_path must not be empty".into(),
            ));
        }
        if self.key_name.is_empty() {
            return Err(KmsError::Configuration(
                "vault key_name must not be empty".into(),
            ));
        }
        Ok(())
    }
}

/// HashiCorp Vault Transit KMS provider.
///
/// Wraps a [`LocalAgeProvider`] for the actual crypto today.  The real
/// implementation would call `/v1/{mount}/sign/{key}` and
/// `/v1/{mount}/verify/{key}` via `self.http_client`.
pub struct VaultKeyProvider {
    config: VaultConfig,
    /// HTTP client for Vault API calls (used by the future real impl).
    #[allow(dead_code)]
    http_client: reqwest::blocking::Client,
    /// Fallback: local provider used until the real Vault API is wired.
    local: LocalAgeProvider,
}

impl VaultKeyProvider {
    /// Create a new Vault provider.
    ///
    /// Validates the configuration eagerly; returns an error if required
    /// fields are missing.  Registers a default signing key under
    /// `config.key_name` so the local-fallback code path works
    /// immediately.
    pub fn new(config: VaultConfig) -> KmsResult<Self> {
        config.validate()?;

        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(ref token) = config.token {
            headers.insert(
                "X-Vault-Token",
                reqwest::header::HeaderValue::from_str(token)
                    .map_err(|e| KmsError::Configuration(format!("invalid token header: {e}")))?,
            );
        }
        if let Some(ref ns) = config.namespace {
            headers.insert(
                "X-Vault-Namespace",
                reqwest::header::HeaderValue::from_str(ns).map_err(|e| {
                    KmsError::Configuration(format!("invalid namespace header: {e}"))
                })?,
            );
        }

        let http_client = reqwest::blocking::Client::builder()
            .default_headers(headers)
            .build()
            .map_err(|e| KmsError::Backend(format!("http client init: {e}")))?;

        let local = LocalAgeProvider::new();
        local.register_key(&config.key_name, Algorithm::Ed25519)?;
        local.alias("broker-signing", &config.key_name)?;

        Ok(Self {
            config,
            http_client,
            local,
        })
    }

    /// Build the Transit API URL for a given operation.
    ///
    /// Real implementation would POST to this endpoint:
    /// `{vault_url}/v1/{mount}/{op}/{key_name}`
    #[allow(dead_code)]
    fn transit_url(&self, op: &str) -> String {
        format!(
            "{}/v1/{}/{}/{}",
            self.config.url, self.config.mount_path, op, self.config.key_name
        )
    }
}

impl KeyProvider for VaultKeyProvider {
    fn sign(&self, key_id: &KeyId, payload: &[u8]) -> KmsResult<Signature> {
        // Real implementation:
        //   POST {vault_url}/v1/{mount}/sign/{key_name}
        //   Body: { "input": base64(payload), "hash_algorithm": "sha2-256" }
        //   Response: { "data": { "signature": "vault:v1:..." } }
        let _ev = AuditEvent::now(BACKEND, KmsOp::Sign, key_id, true);
        self.local.sign(key_id, payload)
    }

    fn verify(&self, key_id: &KeyId, payload: &[u8], sig: &Signature) -> KmsResult<()> {
        // Real implementation:
        //   POST {vault_url}/v1/{mount}/verify/{key_name}
        //   Body: { "input": base64(payload), "signature": "vault:v1:..." }
        let _ev = AuditEvent::now(BACKEND, KmsOp::Verify, key_id, true);
        self.local.verify(key_id, payload, sig)
    }

    fn encrypt(&self, key_id: &KeyId, plaintext: &[u8]) -> KmsResult<Vec<u8>> {
        // Real implementation:
        //   POST {vault_url}/v1/{mount}/encrypt/{key_name}
        //   Body: { "plaintext": base64(plaintext) }
        //   Response: { "data": { "ciphertext": "vault:v1:..." } }
        let _ev = AuditEvent::now(BACKEND, KmsOp::Encrypt, key_id, true);
        self.local.encrypt(key_id, plaintext)
    }

    fn decrypt(&self, key_id: &KeyId, ciphertext: &[u8]) -> KmsResult<Vec<u8>> {
        // Real implementation:
        //   POST {vault_url}/v1/{mount}/decrypt/{key_name}
        //   Body: { "ciphertext": "vault:v1:..." }
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

    fn valid_config() -> VaultConfig {
        VaultConfig {
            url: "https://vault.example.com:8200".into(),
            token: Some("s.test-token".into()),
            mount_path: "transit".into(),
            key_name: "streamline".into(),
            namespace: None,
        }
    }

    #[test]
    fn config_validation_accepts_valid() {
        assert!(valid_config().validate().is_ok());
    }

    #[test]
    fn config_rejects_empty_url() {
        let mut cfg = valid_config();
        cfg.url = String::new();
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, KmsError::Configuration(_)));
    }

    #[test]
    fn config_rejects_missing_token() {
        let mut cfg = valid_config();
        cfg.token = None;
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, KmsError::Configuration(_)));
    }

    #[test]
    fn config_rejects_empty_mount_path() {
        let mut cfg = valid_config();
        cfg.mount_path = String::new();
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, KmsError::Configuration(_)));
    }

    #[test]
    fn config_rejects_empty_key_name() {
        let mut cfg = valid_config();
        cfg.key_name = String::new();
        let err = cfg.validate().unwrap_err();
        assert!(matches!(err, KmsError::Configuration(_)));
    }

    #[test]
    fn provider_creation_succeeds_with_valid_config() {
        let provider = VaultKeyProvider::new(valid_config());
        assert!(provider.is_ok());
    }

    #[test]
    fn backend_name_is_vault_transit() {
        let provider = VaultKeyProvider::new(valid_config()).unwrap();
        assert_eq!(provider.backend_name(), "vault-transit");
    }

    #[test]
    fn sign_verify_roundtrip_via_fallback() {
        let provider = VaultKeyProvider::new(valid_config()).unwrap();
        let key = "streamline".to_string();
        let payload = b"hello vault";
        let sig = provider.sign(&key, payload).expect("sign");
        provider.verify(&key, payload, &sig).expect("verify");
    }

    #[test]
    fn encrypt_decrypt_roundtrip_via_fallback() {
        let provider = VaultKeyProvider::new(valid_config()).unwrap();
        let key = "streamline".to_string();
        let plaintext = b"secret data for vault";
        let ct = provider.encrypt(&key, plaintext).expect("encrypt");
        let pt = provider.decrypt(&key, &ct).expect("decrypt");
        assert_eq!(pt, plaintext);
    }

    #[test]
    fn current_key_id_resolves_alias() {
        let provider = VaultKeyProvider::new(valid_config()).unwrap();
        let kid = provider.current_key_id("broker-signing").expect("alias");
        assert_eq!(kid, "streamline");
    }

    #[test]
    fn transit_url_format() {
        let provider = VaultKeyProvider::new(valid_config()).unwrap();
        assert_eq!(
            provider.transit_url("sign"),
            "https://vault.example.com:8200/v1/transit/sign/streamline"
        );
    }

    #[test]
    fn config_with_namespace() {
        let mut cfg = valid_config();
        cfg.namespace = Some("admin/prod".into());
        let provider = VaultKeyProvider::new(cfg);
        assert!(provider.is_ok());
    }
}
