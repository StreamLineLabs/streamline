//! Encryption key rotation support.
//!
//! Provides zero-downtime key rotation for encryption at rest.
//! New segments are encrypted with the new key while old segments
//! are re-encrypted in the background.

use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::path::PathBuf;

/// Key metadata for tracking active and retired keys.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyMetadata {
    /// Unique key identifier
    pub key_id: String,
    /// When the key was created
    pub created_at: DateTime<Utc>,
    /// When the key was activated for new writes
    pub activated_at: Option<DateTime<Utc>>,
    /// When the key was retired (no longer used for new writes)
    pub retired_at: Option<DateTime<Utc>>,
    /// Key state
    pub state: KeyState,
}

/// State of an encryption key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum KeyState {
    /// Key is pending activation
    Pending,
    /// Key is active and used for new writes
    Active,
    /// Key is being rotated out (old segments being re-encrypted)
    Rotating,
    /// Key is retired but may still be needed to read old segments
    Retired,
    /// Key is destroyed and cannot be used
    Destroyed,
}

/// Key rotation configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyRotationConfig {
    /// Automatic rotation interval (None = manual only)
    pub auto_rotate_interval: Option<std::time::Duration>,
    /// Maximum number of retired keys to keep
    pub max_retired_keys: usize,
    /// Number of segments to re-encrypt per batch
    pub reencrypt_batch_size: usize,
    /// Delay between re-encryption batches
    pub reencrypt_delay: std::time::Duration,
}

impl Default for KeyRotationConfig {
    fn default() -> Self {
        Self {
            auto_rotate_interval: None,
            max_retired_keys: 5,
            reencrypt_batch_size: 100,
            reencrypt_delay: std::time::Duration::from_millis(100),
        }
    }
}

/// Manages encryption key lifecycle and rotation.
pub struct KeyRotationManager {
    /// Path to key store
    key_store_path: PathBuf,
    /// Configuration
    config: KeyRotationConfig,
    /// All known keys (active + retired)
    keys: Vec<KeyMetadata>,
}

impl KeyRotationManager {
    /// Create a new key rotation manager.
    pub fn new(key_store_path: PathBuf, config: KeyRotationConfig) -> Self {
        Self {
            key_store_path,
            config,
            keys: Vec::new(),
        }
    }

    /// Get the currently active key ID.
    pub fn active_key_id(&self) -> Option<&str> {
        self.keys
            .iter()
            .find(|k| k.state == KeyState::Active)
            .map(|k| k.key_id.as_str())
    }

    /// Get all key metadata.
    pub fn keys(&self) -> &[KeyMetadata] {
        &self.keys
    }

    /// Get the key store path.
    pub fn key_store_path(&self) -> &PathBuf {
        &self.key_store_path
    }

    /// Get the configuration.
    pub fn config(&self) -> &KeyRotationConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_key_rotation_config_default() {
        let config = KeyRotationConfig::default();
        assert!(config.auto_rotate_interval.is_none());
        assert_eq!(config.max_retired_keys, 5);
        assert_eq!(config.reencrypt_batch_size, 100);
    }

    #[test]
    fn test_key_rotation_manager_new() {
        let mgr = KeyRotationManager::new(
            PathBuf::from("/tmp/keys"),
            KeyRotationConfig::default(),
        );
        assert!(mgr.active_key_id().is_none());
        assert!(mgr.keys().is_empty());
    }

    #[test]
    fn test_key_state_serialization() {
        let state = KeyState::Active;
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, "\"active\"");

        let state = KeyState::Rotating;
        let json = serde_json::to_string(&state).unwrap();
        assert_eq!(json, "\"rotating\"");
    }

    #[test]
    fn test_key_metadata_serialization() {
        let meta = KeyMetadata {
            key_id: "key-001".to_string(),
            created_at: chrono::Utc::now(),
            activated_at: None,
            retired_at: None,
            state: KeyState::Pending,
        };
        let json = serde_json::to_string(&meta).unwrap();
        assert!(json.contains("key-001"));
        assert!(json.contains("pending"));
    }
}
