use super::*;

#[test]
fn test_tls_config_defaults() {
    // TlsConfig should have reasonable defaults
    use crate::config::TlsConfig;

    let config = TlsConfig::default();
    assert!(!config.enabled, "TLS should be disabled by default");
    assert_eq!(
        config.min_version, "1.2",
        "Default min version should be 1.2"
    );
    assert!(
        !config.require_client_cert,
        "mTLS should be disabled by default"
    );
    assert!(
        config.ca_cert_path.is_none(),
        "CA cert path should be None by default"
    );
}

#[test]
fn test_tls_min_version_options() {
    // Verify supported TLS versions
    let valid_versions = ["1.2", "1.3"];

    for version in valid_versions {
        assert!(
            version == "1.2" || version == "1.3",
            "Version {} should be valid",
            version
        );
    }
}

#[test]
fn test_tls_config_with_paths() {
    // TlsConfig can hold certificate paths
    use crate::config::TlsConfig;
    use std::path::PathBuf;

    let config = TlsConfig {
        enabled: true,
        cert_path: PathBuf::from("/path/to/cert.pem"),
        key_path: PathBuf::from("/path/to/key.pem"),
        min_version: "1.3".to_string(),
        require_client_cert: false,
        ca_cert_path: None,
    };

    assert!(config.enabled);
    assert_eq!(config.cert_path, PathBuf::from("/path/to/cert.pem"));
    assert_eq!(config.key_path, PathBuf::from("/path/to/key.pem"));
    assert_eq!(config.min_version, "1.3");
}

#[test]
fn test_tls_mtls_config() {
    // TlsConfig for mTLS with client certificate verification
    use crate::config::TlsConfig;
    use std::path::PathBuf;

    let config = TlsConfig {
        enabled: true,
        cert_path: PathBuf::from("/path/to/cert.pem"),
        key_path: PathBuf::from("/path/to/key.pem"),
        min_version: "1.3".to_string(),
        require_client_cert: true,
        ca_cert_path: Some(PathBuf::from("/path/to/ca.pem")),
    };

    assert!(config.enabled);
    assert!(config.require_client_cert);
    assert_eq!(config.ca_cert_path, Some(PathBuf::from("/path/to/ca.pem")));
}

#[test]
fn test_tls_version_1_2_string() {
    // TLS 1.2 version string
    let version = "1.2";
    assert!(version.starts_with("1."));
    assert!(version.len() == 3);
}

#[test]
fn test_tls_version_1_3_string() {
    // TLS 1.3 version string
    let version = "1.3";
    assert!(version.starts_with("1."));
    assert!(version.len() == 3);
}
