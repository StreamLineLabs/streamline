//! Connector registry — manages installed connectors.

use super::catalog::ConnectorCatalogEntry;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

/// Marketplace configuration.
#[derive(Debug, Clone)]
pub struct MarketplaceConfig {
    /// Directory for installed connectors
    pub install_dir: PathBuf,
    /// Registry URL for fetching connectors
    pub registry_url: String,
    /// Whether to allow unsigned connectors
    pub allow_unsigned: bool,
}

impl Default for MarketplaceConfig {
    fn default() -> Self {
        Self {
            install_dir: PathBuf::from("./connectors"),
            registry_url: "https://registry.streamline.dev".to_string(),
            allow_unsigned: false,
        }
    }
}

/// Installation status of a connector.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum InstallationStatus {
    Available,
    Installing,
    Installed,
    UpdateAvailable,
    Failed(String),
}

/// An installed connector instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstalledConnector {
    /// Connector name
    pub name: String,
    /// Installed version
    pub version: String,
    /// Installation status
    pub status: InstallationStatus,
    /// Configuration overrides
    pub config: HashMap<String, String>,
    /// Whether the connector is currently running
    pub running: bool,
    /// Installation timestamp
    pub installed_at: i64,
    /// Last started timestamp
    pub last_started_at: Option<i64>,
}

/// Registry for managing installed connectors.
pub struct MarketplaceRegistry {
    config: MarketplaceConfig,
    installed: HashMap<String, InstalledConnector>,
}

impl MarketplaceRegistry {
    /// Create a new registry.
    pub fn new(config: MarketplaceConfig) -> Self {
        Self {
            config,
            installed: HashMap::new(),
        }
    }

    /// Install a connector from the catalog.
    pub fn install(&mut self, entry: &ConnectorCatalogEntry) -> Result<(), String> {
        if self.installed.contains_key(&entry.metadata.name) {
            return Err(format!(
                "Connector '{}' is already installed",
                entry.metadata.name
            ));
        }

        let connector = InstalledConnector {
            name: entry.metadata.name.clone(),
            version: entry.metadata.version.clone(),
            status: InstallationStatus::Installed,
            config: HashMap::new(),
            running: false,
            installed_at: chrono::Utc::now().timestamp(),
            last_started_at: None,
        };

        self.installed
            .insert(entry.metadata.name.clone(), connector);
        Ok(())
    }

    /// Uninstall a connector.
    pub fn uninstall(&mut self, name: &str) -> Result<(), String> {
        if let Some(conn) = self.installed.get(name) {
            if conn.running {
                return Err(format!(
                    "Cannot uninstall running connector '{}'. Stop it first.",
                    name
                ));
            }
        }
        self.installed
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| format!("Connector '{}' is not installed", name))
    }

    /// Configure an installed connector.
    pub fn configure(&mut self, name: &str, config: HashMap<String, String>) -> Result<(), String> {
        let connector = self
            .installed
            .get_mut(name)
            .ok_or_else(|| format!("Connector '{}' is not installed", name))?;
        connector.config = config;
        Ok(())
    }

    /// Mark a connector as running.
    pub fn start(&mut self, name: &str) -> Result<(), String> {
        let connector = self
            .installed
            .get_mut(name)
            .ok_or_else(|| format!("Connector '{}' is not installed", name))?;

        if connector.running {
            return Err(format!("Connector '{}' is already running", name));
        }

        connector.running = true;
        connector.last_started_at = Some(chrono::Utc::now().timestamp());
        Ok(())
    }

    /// Mark a connector as stopped.
    pub fn stop(&mut self, name: &str) -> Result<(), String> {
        let connector = self
            .installed
            .get_mut(name)
            .ok_or_else(|| format!("Connector '{}' is not installed", name))?;
        connector.running = false;
        Ok(())
    }

    /// List all installed connectors.
    pub fn list_installed(&self) -> Vec<&InstalledConnector> {
        self.installed.values().collect()
    }

    /// Get an installed connector by name.
    pub fn get(&self, name: &str) -> Option<&InstalledConnector> {
        self.installed.get(name)
    }

    /// Get install directory.
    pub fn install_dir(&self) -> &PathBuf {
        &self.config.install_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::marketplace::catalog::MarketplaceCatalog;

    #[test]
    fn test_install_and_list() {
        let catalog = MarketplaceCatalog::with_builtins();
        let mut registry = MarketplaceRegistry::new(MarketplaceConfig::default());

        let entry = catalog.get("http-webhook").unwrap();
        registry.install(entry).unwrap();

        let installed = registry.list_installed();
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].name, "http-webhook");
    }

    #[test]
    fn test_start_stop() {
        let catalog = MarketplaceCatalog::with_builtins();
        let mut registry = MarketplaceRegistry::new(MarketplaceConfig::default());

        let entry = catalog.get("http-webhook").unwrap();
        registry.install(entry).unwrap();

        registry.start("http-webhook").unwrap();
        assert!(registry.get("http-webhook").unwrap().running);

        registry.stop("http-webhook").unwrap();
        assert!(!registry.get("http-webhook").unwrap().running);
    }

    #[test]
    fn test_cannot_uninstall_running() {
        let catalog = MarketplaceCatalog::with_builtins();
        let mut registry = MarketplaceRegistry::new(MarketplaceConfig::default());

        let entry = catalog.get("http-webhook").unwrap();
        registry.install(entry).unwrap();
        registry.start("http-webhook").unwrap();

        assert!(registry.uninstall("http-webhook").is_err());
    }
}
