use crate::{Result, StreamlineError};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Connection profile for saving server configurations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionProfile {
    /// Display name for the profile
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Data directory
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_dir: Option<String>,
    /// Server address for connectivity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_addr: Option<String>,
    /// HTTP API address
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_addr: Option<String>,
    /// Description
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Get the profiles file path (~/.streamline/profiles.toml)
pub fn get_profiles_path() -> PathBuf {
    let config_dir = dirs::home_dir()
        .map(|h| h.join(".streamline"))
        .unwrap_or_else(|| PathBuf::from(".streamline"));
    config_dir.join("profiles.toml")
}

/// Load all profiles from the profiles file
pub fn load_profiles() -> Result<HashMap<String, ConnectionProfile>> {
    let path = get_profiles_path();
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let contents = std::fs::read_to_string(&path)?;
    let profiles: HashMap<String, ConnectionProfile> = toml::from_str(&contents)
        .map_err(|e| StreamlineError::Config(format!("Failed to parse profiles: {}", e)))?;
    Ok(profiles)
}

/// Save all profiles to the profiles file
pub fn save_profiles(profiles: &HashMap<String, ConnectionProfile>) -> Result<()> {
    let path = get_profiles_path();

    // Create parent directory if needed
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let contents = toml::to_string_pretty(profiles)
        .map_err(|e| StreamlineError::Config(format!("Failed to serialize profiles: {}", e)))?;
    std::fs::write(&path, contents)?;
    Ok(())
}

/// Load a specific profile by name
pub fn load_profile(name: &str) -> Result<Option<ConnectionProfile>> {
    let profiles = load_profiles()?;
    Ok(profiles.get(name).cloned())
}

/// Get the default profile name (stored in ~/.streamline/default_profile)
pub fn get_default_profile() -> Option<String> {
    let default_path = dirs::home_dir()
        .map(|h| h.join(".streamline").join("default_profile"))
        .unwrap_or_else(|| PathBuf::from(".streamline/default_profile"));

    std::fs::read_to_string(&default_path)
        .ok()
        .map(|s| s.trim().to_string())
}

/// Set the default profile name
pub fn set_default_profile(name: &str) -> Result<()> {
    let config_dir = dirs::home_dir()
        .map(|h| h.join(".streamline"))
        .unwrap_or_else(|| PathBuf::from(".streamline"));
    std::fs::create_dir_all(&config_dir)?;

    let default_path = config_dir.join("default_profile");
    std::fs::write(&default_path, name)?;
    Ok(())
}
