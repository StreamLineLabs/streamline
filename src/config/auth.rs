//! Authentication and authorization configuration
//!
//! This module provides configuration for SASL authentication mechanisms
//! and ACL-based authorization.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Enable authentication (default: false)
    pub enabled: bool,

    /// SASL mechanisms to enable
    pub sasl_mechanisms: Vec<String>,

    /// Path to users file (YAML format)
    pub users_file: Option<PathBuf>,

    /// Allow anonymous connections (when auth enabled but no creds provided)
    pub allow_anonymous: bool,

    /// OAuth 2.0 / OIDC configuration (requires auth feature)
    #[cfg(feature = "auth")]
    pub oauth: crate::auth::OAuthConfig,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            sasl_mechanisms: vec!["PLAIN".to_string()],
            users_file: None,
            allow_anonymous: false,
            #[cfg(feature = "auth")]
            oauth: crate::auth::OAuthConfig::default(),
        }
    }
}

/// Authorization (ACL) configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclConfig {
    /// Enable ACL-based authorization (default: false)
    pub enabled: bool,

    /// Path to ACL file (YAML format)
    pub acl_file: Option<PathBuf>,

    /// Super users who bypass ACL checks (e.g., "User:admin")
    pub super_users: Vec<String>,

    /// Allow all operations if no ACLs are configured (default: true)
    pub allow_if_no_acls: bool,
}

impl Default for AclConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            acl_file: None,
            super_users: Vec::new(),
            allow_if_no_acls: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // AuthConfig tests

    #[test]
    fn test_auth_config_default_values() {
        let config = AuthConfig::default();

        assert!(!config.enabled);
        assert_eq!(config.sasl_mechanisms, vec!["PLAIN".to_string()]);
        assert!(config.users_file.is_none());
        assert!(!config.allow_anonymous);
    }

    #[test]
    fn test_auth_config_serde_roundtrip() {
        let config = AuthConfig {
            enabled: true,
            sasl_mechanisms: vec!["PLAIN".to_string(), "SCRAM-SHA-256".to_string()],
            users_file: Some(PathBuf::from("/etc/streamline/users.yaml")),
            allow_anonymous: true,
            #[cfg(feature = "auth")]
            oauth: crate::auth::OAuthConfig::default(),
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: AuthConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config.enabled, deserialized.enabled);
        assert_eq!(config.sasl_mechanisms, deserialized.sasl_mechanisms);
        assert_eq!(config.users_file, deserialized.users_file);
        assert_eq!(config.allow_anonymous, deserialized.allow_anonymous);
    }

    #[test]
    #[cfg(not(feature = "auth"))]
    fn test_auth_config_deserialize_minimal() {
        // Test deserializing with only required fields (no auth feature)
        let json = r#"{
            "enabled": false,
            "sasl_mechanisms": [],
            "users_file": null,
            "allow_anonymous": false
        }"#;

        let config: AuthConfig = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert!(config.sasl_mechanisms.is_empty());
    }

    #[test]
    #[cfg(feature = "auth")]
    fn test_auth_config_deserialize_minimal() {
        // Test deserializing with auth feature - oauth field required
        let json = r#"{
            "enabled": false,
            "sasl_mechanisms": [],
            "users_file": null,
            "allow_anonymous": false,
            "oauth": {
                "enabled": false,
                "issuer_url": null,
                "audience": null,
                "jwks_url": null,
                "expected_claims": {}
            }
        }"#;

        let config: AuthConfig = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert!(config.sasl_mechanisms.is_empty());
    }

    #[test]
    fn test_auth_config_multiple_mechanisms() {
        let config = AuthConfig {
            enabled: true,
            sasl_mechanisms: vec![
                "PLAIN".to_string(),
                "SCRAM-SHA-256".to_string(),
                "SCRAM-SHA-512".to_string(),
            ],
            users_file: None,
            allow_anonymous: false,
            #[cfg(feature = "auth")]
            oauth: crate::auth::OAuthConfig::default(),
        };

        assert_eq!(config.sasl_mechanisms.len(), 3);
        assert!(config.sasl_mechanisms.contains(&"PLAIN".to_string()));
        assert!(config
            .sasl_mechanisms
            .contains(&"SCRAM-SHA-256".to_string()));
        assert!(config
            .sasl_mechanisms
            .contains(&"SCRAM-SHA-512".to_string()));
    }

    // AclConfig tests

    #[test]
    fn test_acl_config_default_values() {
        let config = AclConfig::default();

        assert!(!config.enabled);
        assert!(config.acl_file.is_none());
        assert!(config.super_users.is_empty());
        assert!(config.allow_if_no_acls);
    }

    #[test]
    fn test_acl_config_serde_roundtrip() {
        let config = AclConfig {
            enabled: true,
            acl_file: Some(PathBuf::from("/etc/streamline/acls.yaml")),
            super_users: vec!["User:admin".to_string(), "User:root".to_string()],
            allow_if_no_acls: false,
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: AclConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config.enabled, deserialized.enabled);
        assert_eq!(config.acl_file, deserialized.acl_file);
        assert_eq!(config.super_users, deserialized.super_users);
        assert_eq!(config.allow_if_no_acls, deserialized.allow_if_no_acls);
    }

    #[test]
    fn test_acl_config_deserialize_with_super_users() {
        let json = r#"{
            "enabled": true,
            "acl_file": "/var/lib/streamline/acls.yaml",
            "super_users": ["User:admin", "User:operator"],
            "allow_if_no_acls": false
        }"#;

        let config: AclConfig = serde_json::from_str(json).unwrap();

        assert!(config.enabled);
        assert_eq!(
            config.acl_file,
            Some(PathBuf::from("/var/lib/streamline/acls.yaml"))
        );
        assert_eq!(config.super_users.len(), 2);
        assert!(config.super_users.contains(&"User:admin".to_string()));
        assert!(config.super_users.contains(&"User:operator".to_string()));
        assert!(!config.allow_if_no_acls);
    }

    #[test]
    fn test_acl_config_empty_super_users() {
        let config = AclConfig {
            enabled: true,
            acl_file: Some(PathBuf::from("acls.yaml")),
            super_users: Vec::new(),
            allow_if_no_acls: true,
        };

        assert!(config.super_users.is_empty());
        assert!(config.allow_if_no_acls);
    }

    #[test]
    fn test_acl_config_deserialize_minimal() {
        let json = r#"{
            "enabled": false,
            "acl_file": null,
            "super_users": [],
            "allow_if_no_acls": true
        }"#;

        let config: AclConfig = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
        assert!(config.acl_file.is_none());
        assert!(config.super_users.is_empty());
        assert!(config.allow_if_no_acls);
    }

    #[test]
    fn test_auth_config_clone() {
        let original = AuthConfig {
            enabled: true,
            sasl_mechanisms: vec!["PLAIN".to_string()],
            users_file: Some(PathBuf::from("/users.yaml")),
            allow_anonymous: true,
            #[cfg(feature = "auth")]
            oauth: crate::auth::OAuthConfig::default(),
        };

        let cloned = original.clone();

        assert_eq!(original.enabled, cloned.enabled);
        assert_eq!(original.sasl_mechanisms, cloned.sasl_mechanisms);
        assert_eq!(original.users_file, cloned.users_file);
        assert_eq!(original.allow_anonymous, cloned.allow_anonymous);
    }

    #[test]
    fn test_acl_config_clone() {
        let original = AclConfig {
            enabled: true,
            acl_file: Some(PathBuf::from("/acls.yaml")),
            super_users: vec!["User:admin".to_string()],
            allow_if_no_acls: false,
        };

        let cloned = original.clone();

        assert_eq!(original.enabled, cloned.enabled);
        assert_eq!(original.acl_file, cloned.acl_file);
        assert_eq!(original.super_users, cloned.super_users);
        assert_eq!(original.allow_if_no_acls, cloned.allow_if_no_acls);
    }
}
