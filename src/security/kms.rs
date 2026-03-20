//! [`KeyProvider`] trait — pluggable cryptographic backend.

use crate::security::error::KmsResult;

pub type KeyId = String;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signature {
    pub algorithm: Algorithm,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Algorithm {
    Ed25519,
    EcdsaP256,
}

impl Algorithm {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ed25519 => "ed25519",
            Self::EcdsaP256 => "ecdsa-p256",
        }
    }
}

/// Backend-agnostic key management interface.
///
/// All operations are synchronous — backends that need network I/O should
/// run their own internal pool and present a sync facade here.
pub trait KeyProvider: Send + Sync {
    fn sign(&self, key_id: &KeyId, payload: &[u8]) -> KmsResult<Signature>;
    fn verify(&self, key_id: &KeyId, payload: &[u8], sig: &Signature) -> KmsResult<()>;
    fn encrypt(&self, key_id: &KeyId, plaintext: &[u8]) -> KmsResult<Vec<u8>>;
    fn decrypt(&self, key_id: &KeyId, ciphertext: &[u8]) -> KmsResult<Vec<u8>>;
    fn current_key_id(&self, alias: &str) -> KmsResult<KeyId>;
    fn backend_name(&self) -> &'static str;
}
