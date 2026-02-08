//! TLS configuration for Streamline
//!
//! This module provides TLS-related configuration for enabling
//! encrypted connections and mutual TLS (mTLS) authentication.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use super::defaults::DEFAULT_TLS_MIN_VERSION;

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Enable TLS (default: false)
    pub enabled: bool,
    /// Path to certificate file (PEM format)
    pub cert_path: PathBuf,
    /// Path to private key file (PEM format)
    pub key_path: PathBuf,
    /// Minimum TLS version (1.2 or 1.3)
    pub min_version: String,
    /// Enable client certificate verification (mTLS)
    pub require_client_cert: bool,
    /// Path to CA certificate for client verification
    pub ca_cert_path: Option<PathBuf>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            cert_path: PathBuf::new(),
            key_path: PathBuf::new(),
            min_version: DEFAULT_TLS_MIN_VERSION.to_string(),
            require_client_cert: false,
            ca_cert_path: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_default_values() {
        let config = TlsConfig::default();

        assert!(!config.enabled);
        assert_eq!(config.cert_path, PathBuf::new());
        assert_eq!(config.key_path, PathBuf::new());
        assert_eq!(config.min_version, "1.2");
        assert!(!config.require_client_cert);
        assert!(config.ca_cert_path.is_none());
    }

    #[test]
    fn test_tls_config_with_enabled() {
        let config = TlsConfig {
            enabled: true,
            cert_path: PathBuf::from("/etc/streamline/server.crt"),
            key_path: PathBuf::from("/etc/streamline/server.key"),
            min_version: "1.3".to_string(),
            require_client_cert: false,
            ca_cert_path: None,
        };

        assert!(config.enabled);
        assert_eq!(
            config.cert_path,
            PathBuf::from("/etc/streamline/server.crt")
        );
        assert_eq!(config.key_path, PathBuf::from("/etc/streamline/server.key"));
        assert_eq!(config.min_version, "1.3");
    }

    #[test]
    fn test_tls_config_mtls() {
        let config = TlsConfig {
            enabled: true,
            cert_path: PathBuf::from("server.crt"),
            key_path: PathBuf::from("server.key"),
            min_version: "1.2".to_string(),
            require_client_cert: true,
            ca_cert_path: Some(PathBuf::from("ca.crt")),
        };

        assert!(config.require_client_cert);
        assert_eq!(config.ca_cert_path, Some(PathBuf::from("ca.crt")));
    }

    #[test]
    fn test_tls_config_clone() {
        let original = TlsConfig {
            enabled: true,
            cert_path: PathBuf::from("cert.pem"),
            key_path: PathBuf::from("key.pem"),
            min_version: "1.3".to_string(),
            require_client_cert: true,
            ca_cert_path: Some(PathBuf::from("ca.pem")),
        };

        let cloned = original.clone();

        assert_eq!(original.enabled, cloned.enabled);
        assert_eq!(original.cert_path, cloned.cert_path);
        assert_eq!(original.key_path, cloned.key_path);
        assert_eq!(original.min_version, cloned.min_version);
        assert_eq!(original.require_client_cert, cloned.require_client_cert);
        assert_eq!(original.ca_cert_path, cloned.ca_cert_path);
    }

    #[test]
    fn test_tls_config_serde_roundtrip() {
        let config = TlsConfig {
            enabled: true,
            cert_path: PathBuf::from("/path/to/cert.pem"),
            key_path: PathBuf::from("/path/to/key.pem"),
            min_version: "1.3".to_string(),
            require_client_cert: true,
            ca_cert_path: Some(PathBuf::from("/path/to/ca.pem")),
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: TlsConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(config.enabled, deserialized.enabled);
        assert_eq!(config.cert_path, deserialized.cert_path);
        assert_eq!(config.key_path, deserialized.key_path);
        assert_eq!(config.min_version, deserialized.min_version);
        assert_eq!(config.require_client_cert, deserialized.require_client_cert);
        assert_eq!(config.ca_cert_path, deserialized.ca_cert_path);
    }

    #[test]
    fn test_tls_config_deserialize_json() {
        let json = r#"{
            "enabled": true,
            "cert_path": "/ssl/server.crt",
            "key_path": "/ssl/server.key",
            "min_version": "1.2",
            "require_client_cert": false,
            "ca_cert_path": null
        }"#;

        let config: TlsConfig = serde_json::from_str(json).unwrap();

        assert!(config.enabled);
        assert_eq!(config.cert_path, PathBuf::from("/ssl/server.crt"));
        assert_eq!(config.key_path, PathBuf::from("/ssl/server.key"));
        assert_eq!(config.min_version, "1.2");
        assert!(!config.require_client_cert);
        assert!(config.ca_cert_path.is_none());
    }

    #[test]
    fn test_tls_config_deserialize_with_ca() {
        let json = r#"{
            "enabled": true,
            "cert_path": "server.crt",
            "key_path": "server.key",
            "min_version": "1.3",
            "require_client_cert": true,
            "ca_cert_path": "client-ca.crt"
        }"#;

        let config: TlsConfig = serde_json::from_str(json).unwrap();

        assert!(config.require_client_cert);
        assert_eq!(config.ca_cert_path, Some(PathBuf::from("client-ca.crt")));
        assert_eq!(config.min_version, "1.3");
    }

    #[test]
    fn test_tls_config_tls12_version() {
        let config = TlsConfig {
            min_version: "1.2".to_string(),
            ..Default::default()
        };

        assert_eq!(config.min_version, "1.2");
    }

    #[test]
    fn test_tls_config_tls13_version() {
        let config = TlsConfig {
            min_version: "1.3".to_string(),
            ..Default::default()
        };

        assert_eq!(config.min_version, "1.3");
    }
}
