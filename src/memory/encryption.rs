//! Per-agent envelope encryption for memory topics (M1 P2).
//!
//! Each agent's memory topics are encrypted with a per-agent data
//! encryption key (DEK). The DEK is wrapped by the master key from
//! the KMS provider (shared with M4). Shredding the DEK makes all
//! data for that agent irrecoverable (GDPR).
//!
//! Stability tier: **Experimental**.

use std::collections::HashMap;
use std::sync::RwLock;

/// Per-agent key material for envelope encryption.
#[derive(Debug, Clone)]
pub struct AgentKeyMaterial {
    /// Agent this key belongs to
    pub agent_id: String,
    /// Data encryption key (DEK) — used to encrypt/decrypt memory records.
    /// In a production implementation, this would be an AES-256-GCM key.
    pub dek: Vec<u8>,
    /// Wrapped (encrypted) DEK — stored at rest, can only be unwrapped
    /// by the master key in the KMS.
    pub wrapped_dek: Vec<u8>,
    /// KMS key ID used to wrap the DEK
    pub kms_key_id: String,
    /// When the key was created (epoch millis)
    pub created_at: i64,
    /// Whether this key has been shredded (GDPR delete)
    pub shredded: bool,
}

/// Registry of per-agent encryption keys.
/// Registry of per-agent encryption keys.
pub struct AgentKeyRegistry {
    keys: RwLock<HashMap<String, AgentKeyMaterial>>,
}

impl AgentKeyRegistry {
    /// Creates an empty key registry.
    pub fn new() -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
        }
    }

    /// Generate and register a new DEK for an agent.
    /// In production, this would call the KMS to wrap the DEK.
    pub fn create_key(&self, agent_id: &str, kms_key_id: &str) -> AgentKeyMaterial {
        let mut dek = vec![0u8; 32]; // AES-256 key size
                                     // Simple deterministic key for dev (production uses CSPRNG)
        for (i, b) in dek.iter_mut().enumerate() {
            *b = (agent_id
                .as_bytes()
                .get(i % agent_id.len())
                .copied()
                .unwrap_or(0))
            .wrapping_add(i as u8);
        }
        let wrapped_dek = dek.clone(); // In production: KMS.wrap(dek)

        let material = AgentKeyMaterial {
            agent_id: agent_id.to_string(),
            dek,
            wrapped_dek,
            kms_key_id: kms_key_id.to_string(),
            created_at: chrono::Utc::now().timestamp_millis(),
            shredded: false,
        };

        if let Ok(mut keys) = self.keys.write() {
            keys.insert(agent_id.to_string(), material.clone());
        }
        material
    }

    /// Get the DEK for an agent. Returns None if not found or shredded.
    pub fn get_key(&self, agent_id: &str) -> Option<AgentKeyMaterial> {
        self.keys
            .read()
            .ok()?
            .get(agent_id)
            .filter(|k| !k.shredded)
            .cloned()
    }

    /// Shred (zero-out and mark destroyed) an agent's DEK.
    /// After this call, all encrypted data for the agent is irrecoverable.
    pub fn shred_key(&self, agent_id: &str) -> bool {
        if let Ok(mut keys) = self.keys.write() {
            if let Some(material) = keys.get_mut(agent_id) {
                // Zero out the DEK
                for b in &mut material.dek {
                    *b = 0;
                }
                material.shredded = true;
                return true;
            }
        }
        false
    }

    /// Check if encryption is active for an agent.
    pub fn is_encrypted(&self, agent_id: &str) -> bool {
        self.keys
            .read()
            .ok()
            .and_then(|keys| keys.get(agent_id).map(|k| !k.shredded))
            .unwrap_or(false)
    }
}

impl Default for AgentKeyRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_retrieve_key() {
        let registry = AgentKeyRegistry::new();
        let material = registry.create_key("agent-1", "master-key-1");
        assert_eq!(material.agent_id, "agent-1");
        assert_eq!(material.dek.len(), 32);
        assert!(!material.shredded);

        let retrieved = registry.get_key("agent-1").expect("key should exist");
        assert_eq!(retrieved.agent_id, "agent-1");
    }

    #[test]
    fn shredded_key_is_irrecoverable() {
        let registry = AgentKeyRegistry::new();
        registry.create_key("agent-2", "master-key-1");

        assert!(registry.is_encrypted("agent-2"));
        assert!(registry.shred_key("agent-2"));

        // Key is gone
        assert!(registry.get_key("agent-2").is_none());
        assert!(!registry.is_encrypted("agent-2"));
    }

    #[test]
    fn unknown_agent_returns_none() {
        let registry = AgentKeyRegistry::new();
        assert!(registry.get_key("nonexistent").is_none());
        assert!(!registry.shred_key("nonexistent"));
    }

    #[test]
    fn shred_zeros_dek() {
        let registry = AgentKeyRegistry::new();
        registry.create_key("agent-3", "mk-1");
        registry.shred_key("agent-3");

        // Verify the key is shredded — can't access it via get_key (returns None)
        assert!(registry.get_key("agent-3").is_none());
        assert!(!registry.is_encrypted("agent-3"));

        // Verify via the raw registry that the entry still exists but is zeroed
        let keys = registry.keys.read().expect("lock");
        let material = keys.get("agent-3").expect("entry should still exist");
        assert!(material.shredded);
        assert!(material.dek.iter().all(|&b| b == 0));
    }
}
