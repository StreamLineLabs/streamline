//! Local development KMS backend with **real** crypto (M4 P1 / M1 P3).
//!
//! * Signing/verification: Ed25519 via [`ed25519_dalek`].
//! * Symmetric envelope: [`age`] (X25519 recipient).
//!
//! All keys are held in process memory. Suitable for tests, dev clusters,
//! and air-gapped single-broker installs. Production deployments should
//! configure `kms_vault.rs` or a cloud-KMS adapter.
//!
//! Stability tier: **Experimental** (gated by feature `kms`).

use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::RwLock;

use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey, SECRET_KEY_LENGTH};
use rand::rngs::OsRng;
use rand::RngCore;

use crate::security::audit::{AuditEvent, KmsOp};
use crate::security::error::{KmsError, KmsResult};
use crate::security::kms::{Algorithm, KeyId, KeyProvider, Signature};

const BACKEND: &str = "local-age";

/// In-memory KMS provider with real Ed25519 + age crypto.
pub struct LocalAgeProvider {
    keys: RwLock<HashMap<KeyId, KeyMaterial>>,
    aliases: RwLock<HashMap<String, KeyId>>,
}

#[derive(Clone)]
struct KeyMaterial {
    algorithm: Algorithm,
    /// 32-byte Ed25519 secret seed (used for sign keys).
    sign_seed: Option<[u8; SECRET_KEY_LENGTH]>,
    /// X25519 identity (used as `age` recipient + identity for envelope crypto).
    age_identity: Option<age::x25519::Identity>,
}

impl Default for LocalAgeProvider {
    fn default() -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
            aliases: RwLock::new(HashMap::new()),
        }
    }
}

impl LocalAgeProvider {
    pub fn new() -> Self {
        Self::default()
    }

    /// Generate a new Ed25519 + X25519 key pair under `key_id`. Algorithm
    /// must be `Ed25519` today; `EcdsaP256` returns `UnsupportedAlgorithm`.
    pub fn register_key(&self, key_id: impl Into<KeyId>, algorithm: Algorithm) -> KmsResult<()> {
        if algorithm != Algorithm::Ed25519 {
            return Err(KmsError::UnsupportedAlgorithm(algorithm.as_str().into()));
        }
        let key_id = key_id.into();
        let mut seed = [0u8; SECRET_KEY_LENGTH];
        OsRng.fill_bytes(&mut seed);
        let identity = age::x25519::Identity::generate();
        let mat = KeyMaterial {
            algorithm,
            sign_seed: Some(seed),
            age_identity: Some(identity),
        };
        let mut keys = self
            .keys
            .write()
            .map_err(|_| KmsError::Backend("key store poisoned".into()))?;
        keys.insert(key_id, mat);
        Ok(())
    }

    pub fn alias(&self, alias: impl Into<String>, key_id: impl Into<KeyId>) -> KmsResult<()> {
        let mut a = self
            .aliases
            .write()
            .map_err(|_| KmsError::Backend("alias map poisoned".into()))?;
        a.insert(alias.into(), key_id.into());
        Ok(())
    }

    fn material(&self, key_id: &str) -> KmsResult<KeyMaterial> {
        let keys = self
            .keys
            .read()
            .map_err(|_| KmsError::Backend("key store poisoned".into()))?;
        keys.get(key_id)
            .cloned()
            .ok_or_else(|| KmsError::KeyNotFound(key_id.to_string()))
    }

    fn signing_key(mat: &KeyMaterial) -> KmsResult<SigningKey> {
        let seed = mat
            .sign_seed
            .ok_or_else(|| KmsError::Configuration("no signing seed".into()))?;
        Ok(SigningKey::from_bytes(&seed))
    }

    fn verifying_key(mat: &KeyMaterial) -> KmsResult<VerifyingKey> {
        Ok(Self::signing_key(mat)?.verifying_key())
    }
}

impl KeyProvider for LocalAgeProvider {
    fn sign(&self, key_id: &KeyId, payload: &[u8]) -> KmsResult<Signature> {
        let mat = self.material(key_id)?;
        let sk = Self::signing_key(&mat)?;
        let sig = sk.sign(payload);
        let _ev = AuditEvent::now(BACKEND, KmsOp::Sign, key_id, true).with_algorithm(mat.algorithm);
        Ok(Signature {
            algorithm: mat.algorithm,
            bytes: sig.to_bytes().to_vec(),
        })
    }

    fn verify(&self, key_id: &KeyId, payload: &[u8], sig: &Signature) -> KmsResult<()> {
        let mat = self.material(key_id)?;
        if mat.algorithm != sig.algorithm {
            return Err(KmsError::UnsupportedAlgorithm(
                sig.algorithm.as_str().into(),
            ));
        }
        let vk = Self::verifying_key(&mat)?;
        let bytes: &[u8; 64] = sig
            .bytes
            .as_slice()
            .try_into()
            .map_err(|_| KmsError::InvalidSignature)?;
        let signature = ed25519_dalek::Signature::from_bytes(bytes);
        vk.verify(payload, &signature)
            .map_err(|_| KmsError::InvalidSignature)
    }

    fn encrypt(&self, key_id: &KeyId, plaintext: &[u8]) -> KmsResult<Vec<u8>> {
        let mat = self.material(key_id)?;
        let identity = mat
            .age_identity
            .as_ref()
            .ok_or_else(|| KmsError::Configuration("no age identity".into()))?;
        let recipient = identity.to_public();
        let encryptor = age::Encryptor::with_recipients(vec![Box::new(recipient)])
            .ok_or_else(|| KmsError::Backend("encryptor with no recipients".into()))?;
        let mut out = Vec::with_capacity(plaintext.len() + 256);
        let mut writer = encryptor
            .wrap_output(&mut out)
            .map_err(|e| KmsError::Backend(format!("age wrap: {e}")))?;
        writer
            .write_all(plaintext)
            .map_err(|e| KmsError::Backend(format!("age write: {e}")))?;
        writer
            .finish()
            .map_err(|e| KmsError::Backend(format!("age finish: {e}")))?;
        Ok(out)
    }

    fn decrypt(&self, key_id: &KeyId, ciphertext: &[u8]) -> KmsResult<Vec<u8>> {
        let mat = self.material(key_id)?;
        let identity = mat
            .age_identity
            .as_ref()
            .ok_or_else(|| KmsError::Configuration("no age identity".into()))?;
        let decryptor = match age::Decryptor::new(ciphertext)
            .map_err(|e| KmsError::Backend(format!("age decryptor: {e}")))?
        {
            age::Decryptor::Recipients(d) => d,
            age::Decryptor::Passphrase(_) => {
                return Err(KmsError::Backend("passphrase ciphertext rejected".into()))
            }
        };
        let mut reader = decryptor
            .decrypt(std::iter::once(identity as &dyn age::Identity))
            .map_err(|e| KmsError::Backend(format!("age decrypt: {e}")))?;
        let mut out = Vec::new();
        reader
            .read_to_end(&mut out)
            .map_err(|e| KmsError::Backend(format!("age read: {e}")))?;
        Ok(out)
    }

    fn current_key_id(&self, alias: &str) -> KmsResult<KeyId> {
        let aliases = self
            .aliases
            .read()
            .map_err(|_| KmsError::Backend("alias map poisoned".into()))?;
        aliases
            .get(alias)
            .cloned()
            .ok_or_else(|| KmsError::Configuration(format!("alias not registered: {alias}")))
    }

    fn backend_name(&self) -> &'static str {
        BACKEND
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn provider() -> LocalAgeProvider {
        let p = LocalAgeProvider::new();
        p.register_key("k1", Algorithm::Ed25519).expect("register");
        p.alias("broker-signing", "k1").expect("alias");
        p
    }

    #[test]
    fn sign_verify_roundtrip_real_ed25519() {
        let p = provider();
        let payload = b"hello streamline";
        let sig = p.sign(&"k1".to_string(), payload).expect("sign");
        assert_eq!(sig.bytes.len(), 64, "Ed25519 sigs are 64 bytes");
        p.verify(&"k1".to_string(), payload, &sig).expect("verify");
    }

    #[test]
    fn verify_rejects_tampered_payload() {
        let p = provider();
        let sig = p.sign(&"k1".to_string(), b"a").expect("sign");
        let err = p.verify(&"k1".to_string(), b"b", &sig).unwrap_err();
        assert!(matches!(err, KmsError::InvalidSignature));
    }

    #[test]
    fn verify_rejects_wrong_size_signature() {
        let p = provider();
        let bad = Signature {
            algorithm: Algorithm::Ed25519,
            bytes: vec![0u8; 10],
        };
        assert!(matches!(
            p.verify(&"k1".to_string(), b"x", &bad).unwrap_err(),
            KmsError::InvalidSignature
        ));
    }

    #[test]
    fn unknown_key_errors() {
        let p = provider();
        let err = p.sign(&"missing".to_string(), b"x").unwrap_err();
        assert!(matches!(err, KmsError::KeyNotFound(_)));
    }

    #[test]
    fn encrypt_decrypt_roundtrip_real_age() {
        let p = provider();
        let pt = b"secret payload that is non-trivial in length";
        let ct = p.encrypt(&"k1".to_string(), pt).expect("encrypt");
        // age ciphertext is non-trivial header + body; must not equal plaintext.
        assert_ne!(&ct, pt);
        let back = p.decrypt(&"k1".to_string(), &ct).expect("decrypt");
        assert_eq!(back, pt);
    }

    #[test]
    fn alias_resolves() {
        let p = provider();
        let kid = p.current_key_id("broker-signing").expect("alias");
        assert_eq!(kid, "k1");
    }

    #[test]
    fn alias_unknown_errors() {
        let p = provider();
        let err = p.current_key_id("nope").unwrap_err();
        assert!(matches!(err, KmsError::Configuration(_)));
    }

    #[test]
    fn rejects_unsupported_algorithm_at_keygen() {
        let p = LocalAgeProvider::new();
        let err = p.register_key("k2", Algorithm::EcdsaP256).unwrap_err();
        assert!(matches!(err, KmsError::UnsupportedAlgorithm(_)));
    }
}
