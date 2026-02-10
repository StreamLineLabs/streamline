//! User credential storage and management

use crate::error::{Result, StreamlineError};
use argon2::{
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hmac::{digest::KeyInit, Hmac, Mac};
use rand::Rng;
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Sha512};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Default number of PBKDF2 iterations for SCRAM (Kafka default is 4096)
pub const DEFAULT_SCRAM_ITERATIONS: u32 = 4096;

/// SCRAM credentials for a specific hash algorithm
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScramCredentials {
    /// Base64-encoded salt
    pub salt: String,

    /// PBKDF2 iteration count
    pub iterations: u32,

    /// Base64-encoded StoredKey = H(ClientKey)
    pub stored_key: String,

    /// Base64-encoded ServerKey = HMAC(SaltedPassword, "Server Key")
    pub server_key: String,
}

impl ScramCredentials {
    /// Generate SCRAM-SHA-256 credentials from a plaintext password
    pub fn generate_sha256(password: &str) -> Result<Self> {
        // Generate random salt (16 bytes)
        let mut salt_bytes = [0u8; 16];
        rand::thread_rng().fill(&mut salt_bytes);
        let salt = BASE64.encode(salt_bytes);

        let iterations = DEFAULT_SCRAM_ITERATIONS;

        // Derive SaltedPassword using PBKDF2-HMAC-SHA256
        let mut salted_password = [0u8; 32];
        pbkdf2::pbkdf2_hmac::<Sha256>(
            password.as_bytes(),
            &salt_bytes,
            iterations,
            &mut salted_password,
        );

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        let client_key = Self::hmac_sha256(&salted_password, b"Client Key")?;

        // StoredKey = SHA256(ClientKey)
        use sha2::Digest;
        let stored_key = Sha256::digest(&client_key).to_vec();

        // ServerKey = HMAC(SaltedPassword, "Server Key")
        let server_key = Self::hmac_sha256(&salted_password, b"Server Key")?;

        Ok(Self {
            salt,
            iterations,
            stored_key: BASE64.encode(&stored_key),
            server_key: BASE64.encode(&server_key),
        })
    }

    /// Generate SCRAM-SHA-512 credentials from a plaintext password
    pub fn generate_sha512(password: &str) -> Result<Self> {
        // Generate random salt (16 bytes)
        let mut salt_bytes = [0u8; 16];
        rand::thread_rng().fill(&mut salt_bytes);
        let salt = BASE64.encode(salt_bytes);

        let iterations = DEFAULT_SCRAM_ITERATIONS;

        // Derive SaltedPassword using PBKDF2-HMAC-SHA512
        let mut salted_password = [0u8; 64];
        pbkdf2::pbkdf2_hmac::<Sha512>(
            password.as_bytes(),
            &salt_bytes,
            iterations,
            &mut salted_password,
        );

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        let client_key = Self::hmac_sha512(&salted_password, b"Client Key")?;

        // StoredKey = SHA512(ClientKey)
        use sha2::Digest;
        let stored_key = Sha512::digest(&client_key).to_vec();

        // ServerKey = HMAC(SaltedPassword, "Server Key")
        let server_key = Self::hmac_sha512(&salted_password, b"Server Key")?;

        Ok(Self {
            salt,
            iterations,
            stored_key: BASE64.encode(&stored_key),
            server_key: BASE64.encode(&server_key),
        })
    }

    /// Compute HMAC-SHA256
    fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
        let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(key)
            .map_err(|e| StreamlineError::AuthenticationFailed(e.to_string()))?;
        mac.update(data);
        Ok(mac.finalize().into_bytes().to_vec())
    }

    /// Compute HMAC-SHA512
    fn hmac_sha512(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
        let mut mac = <Hmac<Sha512> as KeyInit>::new_from_slice(key)
            .map_err(|e| StreamlineError::AuthenticationFailed(e.to_string()))?;
        mac.update(data);
        Ok(mac.finalize().into_bytes().to_vec())
    }

    /// Get the salt as raw bytes
    pub fn salt_bytes(&self) -> Result<Vec<u8>> {
        BASE64
            .decode(&self.salt)
            .map_err(|e| StreamlineError::AuthenticationFailed(format!("Invalid salt: {}", e)))
    }

    /// Get the stored key as raw bytes
    pub fn stored_key_bytes(&self) -> Result<Vec<u8>> {
        BASE64.decode(&self.stored_key).map_err(|e| {
            StreamlineError::AuthenticationFailed(format!("Invalid stored key: {}", e))
        })
    }

    /// Get the server key as raw bytes
    pub fn server_key_bytes(&self) -> Result<Vec<u8>> {
        BASE64.decode(&self.server_key).map_err(|e| {
            StreamlineError::AuthenticationFailed(format!("Invalid server key: {}", e))
        })
    }
}

/// User with credentials and permissions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    /// Username
    pub username: String,

    /// Argon2 hashed password
    pub password_hash: String,

    /// User permissions (e.g., "admin", "produce:*", "consume:topic-name")
    #[serde(default)]
    pub permissions: Vec<String>,

    /// SCRAM-SHA-256 credentials (optional for backwards compatibility)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scram_sha256: Option<ScramCredentials>,

    /// SCRAM-SHA-512 credentials (optional for backwards compatibility)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scram_sha512: Option<ScramCredentials>,
}

impl User {
    /// Create a new user with a plaintext password (will be hashed)
    /// This generates both Argon2 and SCRAM credentials for maximum compatibility
    pub fn new(username: String, password: &str, permissions: Vec<String>) -> Result<Self> {
        let password_hash = Self::hash_password(password)?;
        let scram_sha256 = Some(ScramCredentials::generate_sha256(password)?);
        let scram_sha512 = Some(ScramCredentials::generate_sha512(password)?);

        Ok(Self {
            username,
            password_hash,
            permissions,
            scram_sha256,
            scram_sha512,
        })
    }

    /// Create a new user with only Argon2 password (no SCRAM credentials)
    /// Use this for PLAIN-only authentication
    pub fn new_plain_only(
        username: String,
        password: &str,
        permissions: Vec<String>,
    ) -> Result<Self> {
        let password_hash = Self::hash_password(password)?;
        Ok(Self {
            username,
            password_hash,
            permissions,
            scram_sha256: None,
            scram_sha512: None,
        })
    }

    /// Create a user from an OAuth/OIDC principal
    /// This creates a passwordless user for token-based authentication
    pub fn oauth_user(principal: String) -> Self {
        Self {
            username: principal,
            // Use empty string for password hash since OAuth users don't have passwords
            password_hash: String::new(),
            // OAuth users get basic permissions by default
            // Actual permissions should be derived from token claims/scopes
            permissions: vec!["oauth".to_string()],
            scram_sha256: None,
            scram_sha512: None,
        }
    }

    /// Create an OAuth user with specific permissions derived from token claims
    pub fn oauth_user_with_permissions(principal: String, permissions: Vec<String>) -> Self {
        Self {
            username: principal,
            password_hash: String::new(),
            permissions,
            scram_sha256: None,
            scram_sha512: None,
        }
    }

    /// Hash a password using Argon2
    pub fn hash_password(password: &str) -> Result<String> {
        use argon2::Params;

        let salt = SaltString::generate(&mut argon2::password_hash::rand_core::OsRng);

        // Configure Argon2 with production-ready parameters
        // Memory cost: 19MB (19456 KiB)
        // Time cost: 3 iterations (increased from 2 for stronger security)
        // Parallelism: 1 thread
        let params = Params::new(19456, 3, 1, None).map_err(|e| {
            StreamlineError::AuthenticationFailed(format!("Failed to create Argon2 params: {}", e))
        })?;

        let argon2 = Argon2::new(argon2::Algorithm::Argon2id, argon2::Version::V0x13, params);

        let password_hash = argon2
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| {
                StreamlineError::AuthenticationFailed(format!("Failed to hash password: {}", e))
            })?;
        Ok(password_hash.to_string())
    }

    /// Verify a password against the stored hash
    pub fn verify_password(&self, password: &str) -> Result<bool> {
        let parsed_hash = PasswordHash::new(&self.password_hash).map_err(|e| {
            StreamlineError::AuthenticationFailed(format!("Invalid password hash: {}", e))
        })?;

        let argon2 = Argon2::default();
        Ok(argon2
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok())
    }

    /// Check if user has a specific permission
    pub fn has_permission(&self, permission: &str) -> bool {
        // Check for admin permission
        if self.permissions.contains(&"admin".to_string()) {
            return true;
        }

        // Check for exact match
        if self.permissions.contains(&permission.to_string()) {
            return true;
        }

        // Check for wildcard matches (e.g., "produce:*" matches "produce:topic-name")
        for perm in &self.permissions {
            if perm.ends_with(":*") {
                let prefix = &perm[..perm.len() - 1]; // Remove '*'
                if permission.starts_with(prefix) {
                    return true;
                }
            }
        }

        false
    }
}

/// Users file format
#[derive(Debug, Serialize, Deserialize)]
struct UsersFile {
    users: Vec<User>,
}

/// Pre-computed dummy password hash for constant-time authentication.
/// This is a valid Argon2id hash used when a user doesn't exist, ensuring
/// we still perform the same computational work to prevent timing attacks.
/// The password "dummy_password_for_timing_attack_prevention" was hashed with
/// the same parameters as production passwords (m=19456, t=3, p=1).
const DUMMY_PASSWORD_HASH: &str = "$argon2id$v=19$m=19456,t=3,p=1$c29tZXNhbHR2YWx1ZWhlcmU$RLOlG+bqaPmZpqqtqkk0lJ95GXDyOQD3kPKcInYvQIk";

/// User credential store
#[derive(Debug, Clone)]
pub struct UserStore {
    users: HashMap<String, User>,
}

impl UserStore {
    /// Create a new empty user store
    pub fn new() -> Self {
        Self {
            users: HashMap::new(),
        }
    }

    /// Load users from a YAML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref())
            .map_err(|e| StreamlineError::Config(format!("Failed to read users file: {}", e)))?;

        let users_file: UsersFile = serde_yaml::from_str(&content)
            .map_err(|e| StreamlineError::Config(format!("Failed to parse users file: {}", e)))?;

        let mut users = HashMap::new();
        for user in users_file.users {
            users.insert(user.username.clone(), user);
        }

        Ok(Self { users })
    }

    /// Save users to a YAML file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let users_file = UsersFile {
            users: self.users.values().cloned().collect(),
        };

        let content = serde_yaml::to_string(&users_file)
            .map_err(|e| StreamlineError::Config(format!("Failed to serialize users: {}", e)))?;

        fs::write(path.as_ref(), content)
            .map_err(|e| StreamlineError::Config(format!("Failed to write users file: {}", e)))?;

        Ok(())
    }

    /// Add a user to the store
    pub fn add_user(&mut self, user: User) {
        self.users.insert(user.username.clone(), user);
    }

    /// Remove a user from the store
    pub fn remove_user(&mut self, username: &str) -> Option<User> {
        self.users.remove(username)
    }

    /// Get a user by username
    pub fn get_user(&self, username: &str) -> Option<&User> {
        self.users.get(username)
    }

    /// Authenticate a user with username and password
    ///
    /// SECURITY: This function uses constant-time behavior to prevent timing attacks.
    /// Even if the username doesn't exist, we still perform password hashing work
    /// to ensure the response time doesn't leak information about valid usernames.
    pub fn authenticate(&self, username: &str, password: &str) -> Result<&User> {
        // Look up user (this is fast, no timing concern here)
        let maybe_user = self.get_user(username);

        // SECURITY: Always perform password verification work to prevent timing attacks.
        // If user doesn't exist, we verify against a dummy hash to ensure consistent timing.
        // This prevents attackers from enumerating valid usernames by measuring response times.
        let password_valid = match maybe_user {
            Some(user) => user.verify_password(password).unwrap_or(false),
            None => {
                // Perform dummy password verification with the same computational cost
                // This ensures attackers can't distinguish "user not found" from "wrong password"
                // by measuring response time.
                let _ = Self::verify_against_hash(password, DUMMY_PASSWORD_HASH);
                false
            }
        };

        // Return result (user must exist AND password must be valid)
        match (maybe_user, password_valid) {
            (Some(user), true) => Ok(user),
            _ => Err(StreamlineError::InvalidCredentials),
        }
    }

    /// Verify password against a hash string (helper for constant-time authentication)
    fn verify_against_hash(password: &str, hash: &str) -> bool {
        let parsed_hash = match PasswordHash::new(hash) {
            Ok(h) => h,
            Err(_) => return false,
        };
        let argon2 = Argon2::default();
        argon2
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
    }

    /// List all usernames
    pub fn list_users(&self) -> Vec<String> {
        let mut usernames: Vec<String> = self.users.keys().cloned().collect();
        usernames.sort();
        usernames
    }
}

impl Default for UserStore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_user_password_hash_and_verify() {
        let user = User::new(
            "testuser".to_string(),
            "password123",
            vec!["admin".to_string()],
        )
        .unwrap();

        assert!(user.verify_password("password123").unwrap());
        assert!(!user.verify_password("wrongpassword").unwrap());
    }

    #[test]
    fn test_user_permissions() {
        let user = User {
            username: "testuser".to_string(),
            password_hash: "hash".to_string(),
            permissions: vec!["produce:orders".to_string(), "consume:*".to_string()],
            scram_sha256: None,
            scram_sha512: None,
        };

        assert!(user.has_permission("produce:orders"));
        assert!(user.has_permission("consume:anything"));
        assert!(user.has_permission("consume:orders"));
        assert!(!user.has_permission("produce:events"));
    }

    #[test]
    fn test_user_admin_permission() {
        let user = User {
            username: "admin".to_string(),
            password_hash: "hash".to_string(),
            permissions: vec!["admin".to_string()],
            scram_sha256: None,
            scram_sha512: None,
        };

        assert!(user.has_permission("anything"));
        assert!(user.has_permission("produce:orders"));
        assert!(user.has_permission("consume:orders"));
    }

    #[test]
    fn test_scram_credentials_generation() {
        let creds = ScramCredentials::generate_sha256("password123").unwrap();

        // Verify fields are properly base64 encoded
        assert!(!creds.salt.is_empty());
        assert!(!creds.stored_key.is_empty());
        assert!(!creds.server_key.is_empty());
        assert_eq!(creds.iterations, DEFAULT_SCRAM_ITERATIONS);

        // Verify we can decode the base64 values
        assert!(creds.salt_bytes().is_ok());
        assert!(creds.stored_key_bytes().is_ok());
        assert!(creds.server_key_bytes().is_ok());

        // Salt should be 16 bytes
        assert_eq!(creds.salt_bytes().unwrap().len(), 16);

        // SHA-256 produces 32-byte hashes
        assert_eq!(creds.stored_key_bytes().unwrap().len(), 32);
        assert_eq!(creds.server_key_bytes().unwrap().len(), 32);
    }

    #[test]
    fn test_scram_sha512_credentials_generation() {
        let creds = ScramCredentials::generate_sha512("password123").unwrap();

        // SHA-512 produces 64-byte hashes
        assert_eq!(creds.stored_key_bytes().unwrap().len(), 64);
        assert_eq!(creds.server_key_bytes().unwrap().len(), 64);
    }

    #[test]
    fn test_user_with_scram_credentials() {
        let user = User::new(
            "scramuser".to_string(),
            "password123",
            vec!["admin".to_string()],
        )
        .unwrap();

        // Should have both SCRAM credential types
        assert!(user.scram_sha256.is_some());
        assert!(user.scram_sha512.is_some());

        // Should also have Argon2 hash for PLAIN auth
        assert!(!user.password_hash.is_empty());
        assert!(user.verify_password("password123").unwrap());
    }

    #[test]
    fn test_user_store_add_remove() {
        let mut store = UserStore::new();
        let user = User::new(
            "testuser".to_string(),
            "password123",
            vec!["admin".to_string()],
        )
        .unwrap();

        store.add_user(user.clone());
        assert_eq!(store.list_users().len(), 1);
        assert!(store.get_user("testuser").is_some());

        store.remove_user("testuser");
        assert_eq!(store.list_users().len(), 0);
    }

    #[test]
    fn test_user_store_authenticate() {
        let mut store = UserStore::new();
        let user = User::new(
            "testuser".to_string(),
            "password123",
            vec!["admin".to_string()],
        )
        .unwrap();

        store.add_user(user);

        assert!(store.authenticate("testuser", "password123").is_ok());
        assert!(store.authenticate("testuser", "wrongpassword").is_err());
        assert!(store.authenticate("unknown", "password123").is_err());
    }

    #[test]
    fn test_user_store_file_io() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut store = UserStore::new();

        let user1 = User::new("user1".to_string(), "password1", vec!["admin".to_string()]).unwrap();
        let user2 = User::new(
            "user2".to_string(),
            "password2",
            vec!["produce:*".to_string()],
        )
        .unwrap();

        store.add_user(user1);
        store.add_user(user2);

        store.save_to_file(temp_file.path()).unwrap();

        let loaded_store = UserStore::from_file(temp_file.path()).unwrap();
        assert_eq!(loaded_store.list_users().len(), 2);
        assert!(loaded_store.get_user("user1").is_some());
        assert!(loaded_store.get_user("user2").is_some());
    }
}
