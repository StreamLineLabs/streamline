//! Encryption at rest for Streamline storage
//!
//! This module provides AES-256-GCM encryption for segment data,
//! supporting multiple key providers (file-based, environment, or custom).

use crate::error::{Result, StreamlineError};
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// AES-256-GCM nonce size in bytes
pub const NONCE_SIZE: usize = 12;

/// AES-256 key size in bytes
pub const KEY_SIZE: usize = 32;

/// Authentication tag size for GCM
pub const TAG_SIZE: usize = 16;

/// Encryption algorithm types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum EncryptionAlgorithm {
    /// No encryption
    #[default]
    None,
    /// AES-256-GCM (recommended)
    Aes256Gcm,
}

impl EncryptionAlgorithm {
    /// Get algorithm from byte value
    pub fn from_byte(b: u8) -> Self {
        match b {
            1 => Self::Aes256Gcm,
            _ => Self::None,
        }
    }

    /// Convert to byte value
    pub fn to_byte(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Aes256Gcm => 1,
        }
    }
}

/// Configuration for encryption at rest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    /// Whether encryption is enabled
    pub enabled: bool,
    /// Encryption algorithm to use
    pub algorithm: EncryptionAlgorithm,
    /// Key provider type
    pub key_provider: KeyProviderType,
    /// Path to key file (for file-based key provider)
    pub key_file: Option<std::path::PathBuf>,
    /// Environment variable name for key (for env-based provider)
    pub key_env_var: Option<String>,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            algorithm: EncryptionAlgorithm::None,
            key_provider: KeyProviderType::None,
            key_file: None,
            key_env_var: None,
        }
    }
}

impl EncryptionConfig {
    /// Create a new encryption config with file-based key
    pub fn with_key_file(key_file: impl AsRef<Path>) -> Self {
        Self {
            enabled: true,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_provider: KeyProviderType::File,
            key_file: Some(key_file.as_ref().to_path_buf()),
            key_env_var: None,
        }
    }

    /// Create a new encryption config with environment variable key
    pub fn with_env_key(env_var: impl Into<String>) -> Self {
        Self {
            enabled: true,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_provider: KeyProviderType::Environment,
            key_file: None,
            key_env_var: Some(env_var.into()),
        }
    }
}

/// Key provider types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum KeyProviderType {
    /// No key provider (encryption disabled)
    #[default]
    None,
    /// Key from file
    File,
    /// Key from environment variable
    Environment,
    /// Static key (for testing only)
    Static,
}

/// Data encryption key with metadata
#[derive(Clone)]
pub struct DataEncryptionKey {
    /// The raw key bytes
    key: [u8; KEY_SIZE],
    /// Key version for rotation support
    version: u32,
}

impl DataEncryptionKey {
    /// Create a new DEK from raw bytes
    pub fn new(key: [u8; KEY_SIZE], version: u32) -> Self {
        Self { key, version }
    }

    /// Get the key bytes
    pub fn key(&self) -> &[u8; KEY_SIZE] {
        &self.key
    }

    /// Get the key version
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Generate a random DEK
    pub fn generate() -> Self {
        let mut key = [0u8; KEY_SIZE];
        OsRng.fill_bytes(&mut key);
        Self { key, version: 1 }
    }
}

/// Trait for key providers
pub trait KeyProvider: Send + Sync {
    /// Get the current data encryption key
    fn get_key(&self) -> Result<DataEncryptionKey>;

    /// Get a key by version (for reading older data)
    fn get_key_by_version(&self, version: u32) -> Result<DataEncryptionKey>;
}

/// File-based key provider
pub struct FileKeyProvider {
    key_file: std::path::PathBuf,
    cached_key: RwLock<Option<DataEncryptionKey>>,
}

impl FileKeyProvider {
    /// Create a new file-based key provider
    pub fn new(key_file: impl AsRef<Path>) -> Self {
        Self {
            key_file: key_file.as_ref().to_path_buf(),
            cached_key: RwLock::new(None),
        }
    }

    /// Load the key from file
    fn load_key(&self) -> Result<DataEncryptionKey> {
        let contents = std::fs::read(&self.key_file).map_err(|e| {
            StreamlineError::Config(format!(
                "Failed to read encryption key file {}: {}",
                self.key_file.display(),
                e
            ))
        })?;

        // Support hex-encoded keys or raw 32-byte keys
        let key_bytes = if contents.len() == KEY_SIZE {
            // Raw 32-byte key
            let mut key = [0u8; KEY_SIZE];
            key.copy_from_slice(&contents);
            key
        } else if contents.len() == KEY_SIZE * 2 || contents.len() == KEY_SIZE * 2 + 1 {
            // Hex-encoded key (with optional newline)
            let hex_str = std::str::from_utf8(&contents)
                .map_err(|_| StreamlineError::Config("Invalid key file encoding".into()))?
                .trim();
            let decoded = hex::decode(hex_str).map_err(|e| {
                StreamlineError::Config(format!("Invalid hex key in key file: {}", e))
            })?;
            if decoded.len() != KEY_SIZE {
                return Err(StreamlineError::Config(format!(
                    "Key must be {} bytes, got {}",
                    KEY_SIZE,
                    decoded.len()
                )));
            }
            let mut key = [0u8; KEY_SIZE];
            key.copy_from_slice(&decoded);
            key
        } else {
            return Err(StreamlineError::Config(format!(
                "Invalid key file size: expected {} or {} bytes, got {}",
                KEY_SIZE,
                KEY_SIZE * 2,
                contents.len()
            )));
        };

        Ok(DataEncryptionKey::new(key_bytes, 1))
    }
}

impl KeyProvider for FileKeyProvider {
    fn get_key(&self) -> Result<DataEncryptionKey> {
        // Try to use cached key first
        {
            let cached = self.cached_key.blocking_read();
            if let Some(ref key) = *cached {
                return Ok(key.clone());
            }
        }

        // Load and cache the key
        let key = self.load_key()?;
        {
            let mut cached = self.cached_key.blocking_write();
            *cached = Some(key.clone());
        }
        Ok(key)
    }

    fn get_key_by_version(&self, _version: u32) -> Result<DataEncryptionKey> {
        // File-based provider only supports single version
        self.get_key()
    }
}

/// Environment variable key provider
pub struct EnvKeyProvider {
    env_var: String,
    cached_key: RwLock<Option<DataEncryptionKey>>,
}

impl EnvKeyProvider {
    /// Create a new environment variable key provider
    pub fn new(env_var: impl Into<String>) -> Self {
        Self {
            env_var: env_var.into(),
            cached_key: RwLock::new(None),
        }
    }

    fn load_key(&self) -> Result<DataEncryptionKey> {
        let hex_key = std::env::var(&self.env_var).map_err(|_| {
            StreamlineError::Config(format!(
                "Encryption key environment variable {} not set",
                self.env_var
            ))
        })?;

        let decoded = hex::decode(hex_key.trim()).map_err(|e| {
            StreamlineError::Config(format!(
                "Invalid hex key in environment variable {}: {}",
                self.env_var, e
            ))
        })?;

        if decoded.len() != KEY_SIZE {
            return Err(StreamlineError::Config(format!(
                "Key must be {} bytes, got {}",
                KEY_SIZE,
                decoded.len()
            )));
        }

        let mut key = [0u8; KEY_SIZE];
        key.copy_from_slice(&decoded);
        Ok(DataEncryptionKey::new(key, 1))
    }
}

impl KeyProvider for EnvKeyProvider {
    fn get_key(&self) -> Result<DataEncryptionKey> {
        {
            let cached = self.cached_key.blocking_read();
            if let Some(ref key) = *cached {
                return Ok(key.clone());
            }
        }

        let key = self.load_key()?;
        {
            let mut cached = self.cached_key.blocking_write();
            *cached = Some(key.clone());
        }
        Ok(key)
    }

    fn get_key_by_version(&self, _version: u32) -> Result<DataEncryptionKey> {
        self.get_key()
    }
}

/// Static key provider (for testing)
pub struct StaticKeyProvider {
    key: DataEncryptionKey,
}

impl StaticKeyProvider {
    /// Create a new static key provider
    pub fn new(key: [u8; KEY_SIZE]) -> Self {
        Self {
            key: DataEncryptionKey::new(key, 1),
        }
    }

    /// Create with a random key
    pub fn random() -> Self {
        Self {
            key: DataEncryptionKey::generate(),
        }
    }
}

impl KeyProvider for StaticKeyProvider {
    fn get_key(&self) -> Result<DataEncryptionKey> {
        Ok(self.key.clone())
    }

    fn get_key_by_version(&self, _version: u32) -> Result<DataEncryptionKey> {
        Ok(self.key.clone())
    }
}

/// Encryption manager for handling segment encryption/decryption
pub struct EncryptionManager {
    config: EncryptionConfig,
    key_provider: Option<Arc<dyn KeyProvider>>,
}

impl EncryptionManager {
    /// Create a new encryption manager
    pub fn new(config: EncryptionConfig) -> Result<Self> {
        let key_provider = if config.enabled {
            Some(Self::create_key_provider(&config)?)
        } else {
            None
        };

        Ok(Self {
            config,
            key_provider,
        })
    }

    /// Create a disabled encryption manager
    pub fn disabled() -> Self {
        Self {
            config: EncryptionConfig::default(),
            key_provider: None,
        }
    }

    fn create_key_provider(config: &EncryptionConfig) -> Result<Arc<dyn KeyProvider>> {
        match config.key_provider {
            KeyProviderType::File => {
                let key_file = config.key_file.as_ref().ok_or_else(|| {
                    StreamlineError::Config("Key file path not configured".into())
                })?;
                Ok(Arc::new(FileKeyProvider::new(key_file)))
            }
            KeyProviderType::Environment => {
                let env_var = config.key_env_var.as_ref().ok_or_else(|| {
                    StreamlineError::Config("Key environment variable not configured".into())
                })?;
                Ok(Arc::new(EnvKeyProvider::new(env_var)))
            }
            KeyProviderType::Static => Ok(Arc::new(StaticKeyProvider::random())),
            KeyProviderType::None => Err(StreamlineError::Config(
                "No key provider configured but encryption is enabled".into(),
            )),
        }
    }

    /// Check if encryption is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Get the encryption algorithm
    pub fn algorithm(&self) -> EncryptionAlgorithm {
        self.config.algorithm
    }

    /// Encrypt data
    ///
    /// Returns (nonce, ciphertext) where ciphertext includes the authentication tag
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        if !self.config.enabled {
            return Ok((Vec::new(), plaintext.to_vec()));
        }

        let provider = self.key_provider.as_ref().ok_or_else(|| {
            StreamlineError::storage_msg("No key provider available for encryption".into())
        })?;

        let dek = provider.get_key()?;

        match self.config.algorithm {
            EncryptionAlgorithm::None => Ok((Vec::new(), plaintext.to_vec())),
            EncryptionAlgorithm::Aes256Gcm => self.encrypt_aes_gcm(plaintext, dek.key()),
        }
    }

    /// Decrypt data
    pub fn decrypt(&self, nonce: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>> {
        if !self.config.enabled || nonce.is_empty() {
            return Ok(ciphertext.to_vec());
        }

        let provider = self.key_provider.as_ref().ok_or_else(|| {
            StreamlineError::storage_msg("No key provider available for decryption".into())
        })?;

        let dek = provider.get_key()?;

        match self.config.algorithm {
            EncryptionAlgorithm::None => Ok(ciphertext.to_vec()),
            EncryptionAlgorithm::Aes256Gcm => self.decrypt_aes_gcm(nonce, ciphertext, dek.key()),
        }
    }

    /// AES-256-GCM encryption
    #[cfg(feature = "encryption")]
    fn encrypt_aes_gcm(
        &self,
        plaintext: &[u8],
        key: &[u8; KEY_SIZE],
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        use aes_gcm::{
            aead::{Aead, KeyInit},
            Aes256Gcm, Nonce,
        };

        // Generate random nonce
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        OsRng.fill_bytes(&mut nonce_bytes);

        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to create cipher: {}", e)))?;

        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| StreamlineError::storage_msg(format!("Encryption failed: {}", e)))?;

        Ok((nonce_bytes.to_vec(), ciphertext))
    }

    #[cfg(not(feature = "encryption"))]
    fn encrypt_aes_gcm(
        &self,
        _plaintext: &[u8],
        _key: &[u8; KEY_SIZE],
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        Err(StreamlineError::storage_msg(
            "AES-256-GCM encryption requires the 'encryption' feature".to_string(),
        ))
    }

    /// AES-256-GCM decryption
    #[cfg(feature = "encryption")]
    fn decrypt_aes_gcm(
        &self,
        nonce: &[u8],
        ciphertext: &[u8],
        key: &[u8; KEY_SIZE],
    ) -> Result<Vec<u8>> {
        use aes_gcm::{
            aead::{Aead, KeyInit},
            Aes256Gcm, Nonce,
        };

        if nonce.len() != NONCE_SIZE {
            return Err(StreamlineError::storage_msg(format!(
                "Invalid nonce size: expected {}, got {}",
                NONCE_SIZE,
                nonce.len()
            )));
        }

        let cipher = Aes256Gcm::new_from_slice(key)
            .map_err(|e| StreamlineError::storage_msg(format!("Failed to create cipher: {}", e)))?;

        let nonce = Nonce::from_slice(nonce);
        let plaintext = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| StreamlineError::storage_msg(format!("Decryption failed: {}", e)))?;

        Ok(plaintext)
    }

    #[cfg(not(feature = "encryption"))]
    fn decrypt_aes_gcm(
        &self,
        _nonce: &[u8],
        _ciphertext: &[u8],
        _key: &[u8; KEY_SIZE],
    ) -> Result<Vec<u8>> {
        Err(StreamlineError::storage_msg(
            "AES-256-GCM decryption requires the 'encryption' feature".to_string(),
        ))
    }
}

/// Generate a random encryption key as hex string
pub fn generate_key_hex() -> String {
    let key = DataEncryptionKey::generate();
    hex::encode(key.key())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_encryption_algorithm_conversion() {
        assert_eq!(EncryptionAlgorithm::from_byte(0), EncryptionAlgorithm::None);
        assert_eq!(
            EncryptionAlgorithm::from_byte(1),
            EncryptionAlgorithm::Aes256Gcm
        );
        assert_eq!(EncryptionAlgorithm::None.to_byte(), 0);
        assert_eq!(EncryptionAlgorithm::Aes256Gcm.to_byte(), 1);
    }

    #[test]
    fn test_disabled_encryption() {
        let manager = EncryptionManager::disabled();
        assert!(!manager.is_enabled());

        let data = b"test data";
        let (nonce, ciphertext) = manager.encrypt(data).unwrap();
        assert!(nonce.is_empty());
        assert_eq!(ciphertext, data);

        let decrypted = manager.decrypt(&nonce, &ciphertext).unwrap();
        assert_eq!(decrypted, data);
    }

    #[test]
    fn test_static_key_provider() {
        let key = [42u8; KEY_SIZE];
        let provider = StaticKeyProvider::new(key);

        let dek = provider.get_key().unwrap();
        assert_eq!(dek.key(), &key);
        assert_eq!(dek.version(), 1);
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_aes_gcm_encryption_roundtrip() {
        let config = EncryptionConfig {
            enabled: true,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_provider: KeyProviderType::Static,
            key_file: None,
            key_env_var: None,
        };

        let manager = EncryptionManager::new(config).unwrap();
        assert!(manager.is_enabled());

        let plaintext = b"Hello, World! This is a test message for encryption.";

        let (nonce, ciphertext) = manager.encrypt(plaintext).unwrap();
        assert!(!nonce.is_empty());
        assert_ne!(ciphertext, plaintext);

        let decrypted = manager.decrypt(&nonce, &ciphertext).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_file_key_provider_hex() {
        let dir = tempdir().unwrap();
        let key_file = dir.path().join("encryption.key");

        // Generate a random key and write as hex
        let key = DataEncryptionKey::generate();
        let hex_key = hex::encode(key.key());
        std::fs::write(&key_file, &hex_key).unwrap();

        let provider = FileKeyProvider::new(&key_file);
        let loaded_key = provider.get_key().unwrap();
        assert_eq!(loaded_key.key(), key.key());
    }

    #[test]
    fn test_file_key_provider_raw() {
        let dir = tempdir().unwrap();
        let key_file = dir.path().join("encryption.key");

        // Write raw 32-byte key
        let key = [0xAB; KEY_SIZE];
        std::fs::write(&key_file, key).unwrap();

        let provider = FileKeyProvider::new(&key_file);
        let loaded_key = provider.get_key().unwrap();
        assert_eq!(loaded_key.key(), &key);
    }

    #[test]
    fn test_env_key_provider() {
        let key = DataEncryptionKey::generate();
        let hex_key = hex::encode(key.key());

        std::env::set_var("TEST_ENCRYPTION_KEY", &hex_key);

        let provider = EnvKeyProvider::new("TEST_ENCRYPTION_KEY");
        let loaded_key = provider.get_key().unwrap();
        assert_eq!(loaded_key.key(), key.key());

        std::env::remove_var("TEST_ENCRYPTION_KEY");
    }

    #[test]
    fn test_generate_key_hex() {
        let hex_key = generate_key_hex();
        assert_eq!(hex_key.len(), KEY_SIZE * 2);

        // Verify it's valid hex
        let decoded = hex::decode(&hex_key).unwrap();
        assert_eq!(decoded.len(), KEY_SIZE);
    }

    #[test]
    #[cfg(feature = "encryption")]
    fn test_tampered_ciphertext_fails() {
        let config = EncryptionConfig {
            enabled: true,
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_provider: KeyProviderType::Static,
            key_file: None,
            key_env_var: None,
        };

        let manager = EncryptionManager::new(config).unwrap();
        let plaintext = b"test data";

        let (nonce, mut ciphertext) = manager.encrypt(plaintext).unwrap();

        // Tamper with ciphertext
        if !ciphertext.is_empty() {
            ciphertext[0] ^= 0xFF;
        }

        // Decryption should fail
        let result = manager.decrypt(&nonce, &ciphertext);
        assert!(result.is_err());
    }
}
