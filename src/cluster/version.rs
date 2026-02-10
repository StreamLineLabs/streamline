//! Version tracking and compatibility for rolling upgrades
//!
//! This module provides version information and compatibility checking
//! to support zero-downtime rolling upgrades across cluster nodes.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

/// Current version of Streamline
pub const CURRENT_VERSION: StreamlineVersion = StreamlineVersion {
    major: 0,
    minor: 1,
    patch: 0,
    protocol_version: 1,
};

/// Minimum compatible version for this node
pub const MIN_COMPATIBLE_VERSION: StreamlineVersion = StreamlineVersion {
    major: 0,
    minor: 1,
    patch: 0,
    protocol_version: 1,
};

/// Represents a Streamline software version
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct StreamlineVersion {
    /// Major version - incompatible API changes
    pub major: u32,
    /// Minor version - backwards-compatible functionality
    pub minor: u32,
    /// Patch version - backwards-compatible bug fixes
    pub patch: u32,
    /// Internal protocol version for inter-broker communication
    pub protocol_version: u32,
}

impl StreamlineVersion {
    /// Create a new version
    pub const fn new(major: u32, minor: u32, patch: u32, protocol_version: u32) -> Self {
        Self {
            major,
            minor,
            patch,
            protocol_version,
        }
    }

    /// Get the current version
    pub const fn current() -> Self {
        CURRENT_VERSION
    }

    /// Parse a version string (e.g., "1.2.3")
    pub fn parse(s: &str) -> Option<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() < 3 {
            return None;
        }

        Some(Self {
            major: parts[0].parse().ok()?,
            minor: parts[1].parse().ok()?,
            patch: parts[2].parse().ok()?,
            protocol_version: CURRENT_VERSION.protocol_version,
        })
    }

    /// Check if this version is compatible with another version
    ///
    /// Two versions are compatible if they can communicate and operate
    /// together in the same cluster during a rolling upgrade.
    pub fn is_compatible_with(&self, other: &Self) -> bool {
        // Same major version is required
        if self.major != other.major {
            return false;
        }

        // Protocol versions must be compatible
        // Newer protocol can talk to older protocol within same generation
        let (newer, older) = if self.protocol_version >= other.protocol_version {
            (self, other)
        } else {
            (other, self)
        };

        // Protocol version difference of more than 1 is not supported
        // This ensures gradual upgrades
        newer.protocol_version - older.protocol_version <= 1
    }

    /// Check if this version is newer than another
    pub fn is_newer_than(&self, other: &Self) -> bool {
        match self.major.cmp(&other.major) {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => match self.minor.cmp(&other.minor) {
                Ordering::Greater => true,
                Ordering::Less => false,
                Ordering::Equal => self.patch > other.patch,
            },
        }
    }

    /// Get the semantic version string
    pub fn semver(&self) -> String {
        format!("{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl fmt::Display for StreamlineVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.{} (protocol v{})",
            self.major, self.minor, self.patch, self.protocol_version
        )
    }
}

impl PartialOrd for StreamlineVersion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamlineVersion {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.major.cmp(&other.major) {
            Ordering::Equal => match self.minor.cmp(&other.minor) {
                Ordering::Equal => self.patch.cmp(&other.patch),
                ord => ord,
            },
            ord => ord,
        }
    }
}

impl Default for StreamlineVersion {
    fn default() -> Self {
        CURRENT_VERSION
    }
}

/// Feature flags for gradual rollout during rolling upgrades
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FeatureFlags {
    /// Features and their enablement state
    features: std::collections::HashMap<String, FeatureState>,
}

/// State of a feature during rollout
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum FeatureState {
    /// Feature is disabled
    Disabled,
    /// Feature is enabled only for testing
    Testing,
    /// Feature is enabled
    Enabled,
}

impl FeatureFlags {
    /// Create empty feature flags
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if a feature is enabled
    pub fn is_enabled(&self, feature: &str) -> bool {
        matches!(
            self.features.get(feature),
            Some(FeatureState::Enabled) | Some(FeatureState::Testing)
        )
    }

    /// Check if a feature is fully enabled (not just testing)
    pub fn is_fully_enabled(&self, feature: &str) -> bool {
        matches!(self.features.get(feature), Some(FeatureState::Enabled))
    }

    /// Enable a feature
    pub fn enable(&mut self, feature: &str) {
        self.features
            .insert(feature.to_string(), FeatureState::Enabled);
    }

    /// Disable a feature
    pub fn disable(&mut self, feature: &str) {
        self.features
            .insert(feature.to_string(), FeatureState::Disabled);
    }

    /// Set feature to testing mode
    pub fn set_testing(&mut self, feature: &str) {
        self.features
            .insert(feature.to_string(), FeatureState::Testing);
    }

    /// Get the state of a feature
    pub fn get_state(&self, feature: &str) -> FeatureState {
        self.features
            .get(feature)
            .copied()
            .unwrap_or(FeatureState::Disabled)
    }

    /// List all features
    pub fn list_features(&self) -> impl Iterator<Item = (&String, &FeatureState)> {
        self.features.iter()
    }

    /// Get the minimum version required for a feature
    pub fn min_version_for_feature(feature: &str) -> Option<StreamlineVersion> {
        match feature {
            "connection_pipelining" => Some(StreamlineVersion::new(0, 1, 0, 1)),
            "sparse_index" => Some(StreamlineVersion::new(0, 1, 0, 1)),
            "encryption_at_rest" => Some(StreamlineVersion::new(0, 2, 0, 2)),
            _ => None,
        }
    }
}

/// Version compatibility result
#[derive(Debug, Clone)]
pub struct CompatibilityResult {
    /// Whether the versions are compatible
    pub compatible: bool,
    /// Reason for incompatibility (if any)
    pub reason: Option<String>,
    /// Features that need to be disabled for compatibility
    pub disabled_features: Vec<String>,
}

impl CompatibilityResult {
    /// Create a compatible result
    pub fn compatible() -> Self {
        Self {
            compatible: true,
            reason: None,
            disabled_features: Vec::new(),
        }
    }

    /// Create an incompatible result
    pub fn incompatible(reason: impl Into<String>) -> Self {
        Self {
            compatible: false,
            reason: Some(reason.into()),
            disabled_features: Vec::new(),
        }
    }

    /// Create a result with feature restrictions
    pub fn compatible_with_restrictions(disabled_features: Vec<String>) -> Self {
        Self {
            compatible: true,
            reason: None,
            disabled_features,
        }
    }
}

/// Check full compatibility between two nodes
pub fn check_compatibility(
    local_version: &StreamlineVersion,
    remote_version: &StreamlineVersion,
) -> CompatibilityResult {
    // Check major version match
    if local_version.major != remote_version.major {
        return CompatibilityResult::incompatible(format!(
            "Major version mismatch: local={}, remote={}",
            local_version.major, remote_version.major
        ));
    }

    // Check protocol compatibility
    let protocol_diff = (local_version.protocol_version as i32
        - remote_version.protocol_version as i32)
        .unsigned_abs();
    if protocol_diff > 1 {
        return CompatibilityResult::incompatible(format!(
            "Protocol version too different: local={}, remote={} (max diff is 1)",
            local_version.protocol_version, remote_version.protocol_version
        ));
    }

    // Determine which features need to be disabled for compatibility
    let mut disabled_features = Vec::new();

    // Features that require both nodes to be on the same or newer version
    let version_gated_features = [
        ("connection_pipelining", StreamlineVersion::new(0, 1, 0, 1)),
        ("sparse_index", StreamlineVersion::new(0, 1, 0, 1)),
    ];

    let min_version = std::cmp::min(local_version, remote_version);

    for (feature, required_version) in version_gated_features {
        if min_version < &required_version {
            disabled_features.push(feature.to_string());
        }
    }

    if disabled_features.is_empty() {
        CompatibilityResult::compatible()
    } else {
        CompatibilityResult::compatible_with_restrictions(disabled_features)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_comparison() {
        let v1 = StreamlineVersion::new(1, 0, 0, 1);
        let v2 = StreamlineVersion::new(1, 1, 0, 1);
        let v3 = StreamlineVersion::new(1, 1, 1, 1);
        let v4 = StreamlineVersion::new(2, 0, 0, 2);

        assert!(v2.is_newer_than(&v1));
        assert!(v3.is_newer_than(&v2));
        assert!(v4.is_newer_than(&v3));
        assert!(!v1.is_newer_than(&v2));
    }

    #[test]
    fn test_version_compatibility() {
        let v1 = StreamlineVersion::new(1, 0, 0, 1);
        let v2 = StreamlineVersion::new(1, 1, 0, 1);
        let v3 = StreamlineVersion::new(1, 1, 0, 2);
        let v4 = StreamlineVersion::new(2, 0, 0, 2);
        let v5 = StreamlineVersion::new(1, 0, 0, 3);

        // Same major version is compatible
        assert!(v1.is_compatible_with(&v2));

        // Different major versions are not compatible
        assert!(!v1.is_compatible_with(&v4));

        // Protocol version diff of 1 is compatible
        assert!(v2.is_compatible_with(&v3));

        // Protocol version diff of 2 is not compatible
        assert!(!v1.is_compatible_with(&v5));
    }

    #[test]
    fn test_version_parse() {
        let v = StreamlineVersion::parse("1.2.3").unwrap();
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 3);

        assert!(StreamlineVersion::parse("invalid").is_none());
        assert!(StreamlineVersion::parse("1.2").is_none());
    }

    #[test]
    fn test_feature_flags() {
        let mut flags = FeatureFlags::new();

        assert!(!flags.is_enabled("test_feature"));

        flags.enable("test_feature");
        assert!(flags.is_enabled("test_feature"));
        assert!(flags.is_fully_enabled("test_feature"));

        flags.set_testing("test_feature");
        assert!(flags.is_enabled("test_feature"));
        assert!(!flags.is_fully_enabled("test_feature"));

        flags.disable("test_feature");
        assert!(!flags.is_enabled("test_feature"));
    }

    #[test]
    fn test_check_compatibility() {
        let v1 = StreamlineVersion::new(0, 1, 0, 1);
        let v2 = StreamlineVersion::new(0, 1, 0, 1);

        let result = check_compatibility(&v1, &v2);
        assert!(result.compatible);
        assert!(result.disabled_features.is_empty());

        let v3 = StreamlineVersion::new(1, 0, 0, 1);
        let result = check_compatibility(&v1, &v3);
        assert!(!result.compatible);
    }

    #[test]
    fn test_version_display() {
        let v = StreamlineVersion::new(1, 2, 3, 4);
        assert_eq!(format!("{}", v), "1.2.3 (protocol v4)");
        assert_eq!(v.semver(), "1.2.3");
    }

    #[test]
    fn test_version_ordering() {
        let mut versions = [
            StreamlineVersion::new(1, 2, 0, 1),
            StreamlineVersion::new(1, 0, 0, 1),
            StreamlineVersion::new(2, 0, 0, 1),
            StreamlineVersion::new(1, 1, 5, 1),
        ];

        versions.sort();

        assert_eq!(versions[0], StreamlineVersion::new(1, 0, 0, 1));
        assert_eq!(versions[1], StreamlineVersion::new(1, 1, 5, 1));
        assert_eq!(versions[2], StreamlineVersion::new(1, 2, 0, 1));
        assert_eq!(versions[3], StreamlineVersion::new(2, 0, 0, 1));
    }
}
