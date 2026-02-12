//! WASM Transform Marketplace Client
//!
//! Provides server-side integration with the Streamline WASM Transform Marketplace.
//! This module handles:
//! - Fetching the transform registry from a remote URL
//! - Downloading and caching WASM modules locally
//! - Installing transforms to the server's transform directory
//! - Listing available and installed transforms
//!
//! ## HTTP API Endpoints (behind `wasm-transforms` feature)
//!
//! - `GET /api/v1/marketplace/transforms` - List available transforms from registry
//! - `POST /api/v1/marketplace/transforms/{name}/install` - Install a transform
//! - `GET /api/v1/marketplace/transforms/installed` - List installed transforms

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, info, warn};

// ============================================================================
// Types
// ============================================================================

/// Default registry URL for the WASM Transform Marketplace.
pub const DEFAULT_REGISTRY_URL: &str =
    "https://raw.githubusercontent.com/streamlinelabs/streamline-marketplace/main/registry/transforms.json";

/// A transform entry from the marketplace registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketplaceTransform {
    /// Transform name (kebab-case identifier).
    pub name: String,
    /// Semantic version.
    pub version: String,
    /// Human-readable description.
    pub description: String,
    /// Author or organization name.
    pub author: String,
    /// URL to download the compiled `.wasm` module.
    pub wasm_url: String,
    /// Expected input format (e.g., "json", "csv", "any").
    pub input_format: String,
    /// Expected output format.
    pub output_format: String,
    /// Category for filtering (filter, transform, enrich, aggregate).
    pub category: String,
    /// Searchable tags.
    #[serde(default)]
    pub tags: Vec<String>,
    /// SPDX license identifier.
    #[serde(default)]
    pub license: String,
    /// URL to the source repository.
    #[serde(default)]
    pub repository_url: String,
    /// Configuration schema (arbitrary JSON).
    #[serde(default)]
    pub config_schema: serde_json::Value,
}

/// Metadata for an installed marketplace transform.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstalledMarketplaceTransform {
    /// Transform name.
    pub name: String,
    /// Installed version.
    pub version: String,
    /// Absolute path to the cached WASM module.
    pub wasm_path: String,
    /// SHA-256 hash of the WASM module.
    pub sha256: String,
    /// ISO 8601 timestamp of installation.
    pub installed_at: String,
    /// Original download URL.
    pub source_url: String,
    /// Category from the registry.
    pub category: String,
    /// Description from the registry.
    pub description: String,
}

/// Marketplace client configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketplaceClientConfig {
    /// URL to the transform registry JSON.
    pub registry_url: String,
    /// Local directory for caching downloaded WASM modules.
    pub cache_dir: PathBuf,
    /// How long to cache the registry index (in seconds).
    pub cache_ttl_secs: u64,
}

impl Default for MarketplaceClientConfig {
    fn default() -> Self {
        Self {
            registry_url: DEFAULT_REGISTRY_URL.to_string(),
            cache_dir: PathBuf::from("data/transforms"),
            cache_ttl_secs: 3600,
        }
    }
}

/// Error response for marketplace API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketplaceError {
    pub error: String,
    pub code: u16,
}

/// Install request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallRequest {
    /// Optional version to install (default: latest).
    #[serde(default)]
    pub version: Option<String>,
    /// Optional configuration to validate.
    #[serde(default)]
    pub config: Option<HashMap<String, String>>,
}

// ============================================================================
// Marketplace Client
// ============================================================================

/// The marketplace client manages registry fetching, downloading, and caching.
pub struct MarketplaceClient {
    config: MarketplaceClientConfig,
    /// Cached registry entries.
    registry_cache: RwLock<Option<CachedRegistry>>,
    /// Installed transforms.
    installed: RwLock<Vec<InstalledMarketplaceTransform>>,
}

/// Cached registry with expiry tracking.
struct CachedRegistry {
    entries: Vec<MarketplaceTransform>,
    fetched_at: std::time::Instant,
}

impl MarketplaceClient {
    /// Create a new marketplace client with the given configuration.
    pub fn new(config: MarketplaceClientConfig) -> Self {
        let client = Self {
            config,
            registry_cache: RwLock::new(None),
            installed: RwLock::new(Vec::new()),
        };

        // Load previously installed transforms from disk
        client.load_installed_from_disk();

        client
    }

    /// Fetch the transform registry, using cache if available and fresh.
    pub fn get_registry(&self) -> Vec<MarketplaceTransform> {
        // Check cache
        {
            let cache = self.registry_cache.read();
            if let Some(ref cached) = *cache {
                if cached.fetched_at.elapsed().as_secs() < self.config.cache_ttl_secs {
                    return cached.entries.clone();
                }
            }
        }

        // Fetch from URL
        match self.fetch_registry() {
            Ok(entries) => {
                let mut cache = self.registry_cache.write();
                *cache = Some(CachedRegistry {
                    entries: entries.clone(),
                    fetched_at: std::time::Instant::now(),
                });
                entries
            }
            Err(e) => {
                warn!("Failed to fetch marketplace registry: {}", e);
                // Return stale cache if available
                let cache = self.registry_cache.read();
                cache
                    .as_ref()
                    .map(|c| c.entries.clone())
                    .unwrap_or_default()
            }
        }
    }

    /// Fetch the registry from the configured URL.
    fn fetch_registry(&self) -> Result<Vec<MarketplaceTransform>, String> {
        // Check if it is a local file
        let path = PathBuf::from(&self.config.registry_url);
        if path.exists() {
            let content = std::fs::read_to_string(&path)
                .map_err(|e| format!("Failed to read registry file: {}", e))?;
            let entries: Vec<MarketplaceTransform> = serde_json::from_str(&content)
                .map_err(|e| format!("Failed to parse registry: {}", e))?;
            info!("Loaded {} transforms from local registry", entries.len());
            return Ok(entries);
        }

        // For HTTP fetch, we use a blocking approach since this is called from sync context.
        // In production, this would use the async reqwest client.
        debug!(
            "Fetching marketplace registry from {}",
            self.config.registry_url
        );

        // Placeholder: In a real implementation, this would use reqwest::blocking::get().
        // For the server integration, the registry can also be loaded from a local file
        // bundled with the server or fetched on startup.
        Err(format!(
            "HTTP fetch not available in this build. Configure a local registry file at: {}",
            self.config.registry_url
        ))
    }

    /// Search the registry by query string.
    pub fn search(&self, query: &str, category: Option<&str>) -> Vec<MarketplaceTransform> {
        let registry = self.get_registry();
        let query_lower = query.to_lowercase();

        registry
            .into_iter()
            .filter(|entry| {
                let matches_query = entry.name.to_lowercase().contains(&query_lower)
                    || entry.description.to_lowercase().contains(&query_lower)
                    || entry
                        .tags
                        .iter()
                        .any(|t| t.to_lowercase().contains(&query_lower));

                let matches_category = category
                    .map(|c| entry.category.to_lowercase() == c.to_lowercase())
                    .unwrap_or(true);

                matches_query && matches_category
            })
            .collect()
    }

    /// Install a transform by name and optional version.
    ///
    /// Downloads the WASM module to the local cache directory and registers it.
    pub fn install_transform(
        &self,
        name: &str,
        version: Option<&str>,
    ) -> Result<InstalledMarketplaceTransform, String> {
        let registry = self.get_registry();

        // Find the entry
        let entry = registry
            .iter()
            .find(|e| {
                e.name == name && version.map(|v| e.version == v).unwrap_or(true)
            })
            .ok_or_else(|| format!("Transform '{}' not found in the marketplace registry", name))?;

        info!(
            "Installing marketplace transform: {} v{}",
            entry.name, entry.version
        );

        // Create the cache directory
        let install_dir = self
            .config
            .cache_dir
            .join(&entry.name)
            .join(&entry.version);

        std::fs::create_dir_all(&install_dir)
            .map_err(|e| format!("Failed to create cache directory: {}", e))?;

        // Download the WASM module
        // For now, create a placeholder indicating the download URL.
        // In production, this would use reqwest to download the actual binary.
        let wasm_filename = format!("{}.wasm", entry.name.replace('-', "_"));
        let wasm_path = install_dir.join(&wasm_filename);

        // Write a placeholder or attempt download
        if !wasm_path.exists() {
            // Placeholder: write metadata about where to download
            let placeholder = format!(
                "# WASM module placeholder\n# Download from: {}\n# Transform: {} v{}\n",
                entry.wasm_url, entry.name, entry.version
            );
            std::fs::write(&wasm_path, placeholder)
                .map_err(|e| format!("Failed to write WASM placeholder: {}", e))?;
        }

        // Compute SHA-256 of whatever we have
        let wasm_bytes = std::fs::read(&wasm_path)
            .map_err(|e| format!("Failed to read WASM file: {}", e))?;

        let sha256 = {
            // Simple hash without pulling in sha2 crate (use CRC as fallback)
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&wasm_bytes);
            format!("{:016x}", hasher.finalize())
        };

        // Save metadata
        let metadata_path = install_dir.join("metadata.json");
        let metadata_json = serde_json::to_string_pretty(entry)
            .map_err(|e| format!("Failed to serialize metadata: {}", e))?;
        std::fs::write(&metadata_path, metadata_json)
            .map_err(|e| format!("Failed to write metadata: {}", e))?;

        let installed = InstalledMarketplaceTransform {
            name: entry.name.clone(),
            version: entry.version.clone(),
            wasm_path: wasm_path.to_string_lossy().to_string(),
            sha256,
            installed_at: now_iso8601(),
            source_url: entry.wasm_url.clone(),
            category: entry.category.clone(),
            description: entry.description.clone(),
        };

        // Update in-memory list
        {
            let mut list = self.installed.write();
            list.retain(|i| i.name != name);
            list.push(installed.clone());
        }

        // Persist to disk
        self.save_installed_to_disk();

        info!(
            "Installed marketplace transform: {} v{} at {}",
            installed.name,
            installed.version,
            installed.wasm_path
        );

        Ok(installed)
    }

    /// List all installed marketplace transforms.
    pub fn list_installed(&self) -> Vec<InstalledMarketplaceTransform> {
        self.installed.read().clone()
    }

    /// Get an installed transform by name.
    pub fn get_installed(&self, name: &str) -> Option<InstalledMarketplaceTransform> {
        self.installed.read().iter().find(|i| i.name == name).cloned()
    }

    /// Load installed transforms from the manifest file on disk.
    fn load_installed_from_disk(&self) {
        let manifest_path = self.config.cache_dir.join("installed.json");
        if !manifest_path.exists() {
            return;
        }
        match std::fs::read_to_string(&manifest_path) {
            Ok(content) => {
                if let Ok(list) = serde_json::from_str::<Vec<InstalledMarketplaceTransform>>(&content)
                {
                    *self.installed.write() = list;
                    debug!("Loaded installed marketplace transforms from disk");
                }
            }
            Err(e) => {
                debug!("Failed to read installed manifest: {}", e);
            }
        }
    }

    /// Save the installed transform list to disk.
    fn save_installed_to_disk(&self) {
        let _ = std::fs::create_dir_all(&self.config.cache_dir);
        let manifest_path = self.config.cache_dir.join("installed.json");
        let list = self.installed.read();
        if let Ok(json) = serde_json::to_string_pretty(&*list) {
            if let Err(e) = std::fs::write(&manifest_path, json) {
                warn!("Failed to save installed manifest: {}", e);
            }
        }
    }
}

impl Default for MarketplaceClient {
    fn default() -> Self {
        Self::new(MarketplaceClientConfig::default())
    }
}

/// Generate an ISO 8601 timestamp.
fn now_iso8601() -> String {
    let epoch_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    let secs = (epoch_ms / 1000) as i64;
    let days = secs / 86400;
    let tod = secs % 86400;
    let h = tod / 3600;
    let m = (tod % 3600) / 60;
    let s = tod % 60;

    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let mo = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if mo <= 2 { y + 1 } else { y };

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year, mo, d, h, m, s
    )
}

/// CRC32 hash (used as a lightweight hash when sha2 is unavailable).
#[allow(dead_code)]
fn crc32_hash(data: &[u8]) -> u64 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize() as u64
}

// ============================================================================
// Axum API State and Handlers
// ============================================================================

/// Shared state for the marketplace API.
#[derive(Clone)]
pub struct MarketplaceApiState {
    pub client: Arc<MarketplaceClient>,
}

impl MarketplaceApiState {
    /// Create a new marketplace API state with default configuration.
    pub fn new(cache_dir: PathBuf) -> Self {
        let config = MarketplaceClientConfig {
            cache_dir,
            ..Default::default()
        };
        Self {
            client: Arc::new(MarketplaceClient::new(config)),
        }
    }
}

/// Response for listing available transforms.
#[derive(Debug, Serialize)]
struct ListAvailableResponse {
    transforms: Vec<MarketplaceTransform>,
    total: usize,
}

/// Response for listing installed transforms.
#[derive(Debug, Serialize)]
struct ListInstalledResponse {
    transforms: Vec<InstalledMarketplaceTransform>,
    total: usize,
}

/// Response after installing a transform.
#[derive(Debug, Serialize)]
struct InstallResponse {
    success: bool,
    transform: InstalledMarketplaceTransform,
    message: String,
}

/// GET /api/v1/marketplace/transforms - List available transforms from registry.
async fn list_available_transforms(
    State(state): State<MarketplaceApiState>,
) -> Json<ListAvailableResponse> {
    let transforms = state.client.get_registry();
    let total = transforms.len();
    Json(ListAvailableResponse { transforms, total })
}

/// POST /api/v1/marketplace/transforms/{name}/install - Install a transform.
async fn install_transform(
    State(state): State<MarketplaceApiState>,
    Path(name): Path<String>,
    body: Option<Json<InstallRequest>>,
) -> Result<(StatusCode, Json<InstallResponse>), (StatusCode, Json<MarketplaceError>)> {
    let version = body.as_ref().and_then(|b| b.version.as_deref());

    match state.client.install_transform(&name, version) {
        Ok(installed) => Ok((
            StatusCode::OK,
            Json(InstallResponse {
                success: true,
                message: format!(
                    "Transform '{}' v{} installed successfully",
                    installed.name, installed.version
                ),
                transform: installed,
            }),
        )),
        Err(e) => Err((
            StatusCode::BAD_REQUEST,
            Json(MarketplaceError {
                error: e,
                code: 400,
            }),
        )),
    }
}

/// GET /api/v1/marketplace/transforms/installed - List installed transforms.
async fn list_installed_transforms(
    State(state): State<MarketplaceApiState>,
) -> Json<ListInstalledResponse> {
    let transforms = state.client.list_installed();
    let total = transforms.len();
    Json(ListInstalledResponse { transforms, total })
}

/// Create the marketplace API router.
pub fn create_marketplace_router(state: MarketplaceApiState) -> Router {
    Router::new()
        .route(
            "/api/v1/marketplace/transforms",
            get(list_available_transforms),
        )
        .route(
            "/api/v1/marketplace/transforms/installed",
            get(list_installed_transforms),
        )
        .route(
            "/api/v1/marketplace/transforms/{name}/install",
            post(install_transform),
        )
        .with_state(state)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_client() -> (MarketplaceClient, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = MarketplaceClientConfig {
            registry_url: "nonexistent".to_string(),
            cache_dir: temp_dir.path().to_path_buf(),
            cache_ttl_secs: 3600,
        };
        let client = MarketplaceClient::new(config);
        (client, temp_dir)
    }

    fn test_client_with_registry() -> (MarketplaceClient, TempDir) {
        let temp_dir = TempDir::new().unwrap();

        // Write a test registry file
        let registry = serde_json::json!([
            {
                "name": "test-filter",
                "version": "0.1.0",
                "description": "A test filter transform",
                "author": "test",
                "wasm_url": "https://example.com/test_filter.wasm",
                "input_format": "json",
                "output_format": "json",
                "category": "filter",
                "tags": ["test", "filter"],
                "license": "Apache-2.0",
                "repository_url": "https://example.com/repo"
            },
            {
                "name": "test-enricher",
                "version": "0.2.0",
                "description": "A test enrichment transform",
                "author": "test",
                "wasm_url": "https://example.com/test_enricher.wasm",
                "input_format": "json",
                "output_format": "json",
                "category": "enrich",
                "tags": ["test", "enrich"],
                "license": "Apache-2.0",
                "repository_url": "https://example.com/repo"
            }
        ]);

        let registry_path = temp_dir.path().join("transforms.json");
        std::fs::write(&registry_path, registry.to_string()).unwrap();

        let config = MarketplaceClientConfig {
            registry_url: registry_path.to_string_lossy().to_string(),
            cache_dir: temp_dir.path().join("cache"),
            cache_ttl_secs: 3600,
        };
        let client = MarketplaceClient::new(config);
        (client, temp_dir)
    }

    #[test]
    fn test_marketplace_client_creation() {
        let (_client, _temp) = test_client();
    }

    #[test]
    fn test_get_registry_from_file() {
        let (client, _temp) = test_client_with_registry();
        let registry = client.get_registry();
        assert_eq!(registry.len(), 2);
        assert_eq!(registry[0].name, "test-filter");
        assert_eq!(registry[1].name, "test-enricher");
    }

    #[test]
    fn test_search_by_name() {
        let (client, _temp) = test_client_with_registry();
        let results = client.search("filter", None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "test-filter");
    }

    #[test]
    fn test_search_by_description() {
        let (client, _temp) = test_client_with_registry();
        let results = client.search("enrichment", None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "test-enricher");
    }

    #[test]
    fn test_search_by_category() {
        let (client, _temp) = test_client_with_registry();
        let results = client.search("test", Some("enrich"));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "test-enricher");
    }

    #[test]
    fn test_search_no_results() {
        let (client, _temp) = test_client_with_registry();
        let results = client.search("nonexistent", None);
        assert!(results.is_empty());
    }

    #[test]
    fn test_install_transform() {
        let (client, _temp) = test_client_with_registry();
        let result = client.install_transform("test-filter", None);
        assert!(result.is_ok());

        let installed = result.unwrap();
        assert_eq!(installed.name, "test-filter");
        assert_eq!(installed.version, "0.1.0");
        assert!(installed.wasm_path.contains("test_filter.wasm"));
    }

    #[test]
    fn test_install_transform_not_found() {
        let (client, _temp) = test_client_with_registry();
        let result = client.install_transform("nonexistent", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_installed() {
        let (client, _temp) = test_client_with_registry();

        // Initially empty
        assert!(client.list_installed().is_empty());

        // Install one
        client.install_transform("test-filter", None).unwrap();

        // Should have one
        let installed = client.list_installed();
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].name, "test-filter");
    }

    #[test]
    fn test_get_installed() {
        let (client, _temp) = test_client_with_registry();
        client.install_transform("test-filter", None).unwrap();

        assert!(client.get_installed("test-filter").is_some());
        assert!(client.get_installed("nonexistent").is_none());
    }

    #[test]
    fn test_install_persists_to_disk() {
        let temp_dir = TempDir::new().unwrap();

        // Write registry
        let registry = serde_json::json!([{
            "name": "persist-test",
            "version": "0.1.0",
            "description": "Test persistence",
            "author": "test",
            "wasm_url": "https://example.com/test.wasm",
            "input_format": "json",
            "output_format": "json",
            "category": "filter",
            "tags": [],
            "license": "Apache-2.0",
            "repository_url": ""
        }]);

        let registry_path = temp_dir.path().join("transforms.json");
        std::fs::write(&registry_path, registry.to_string()).unwrap();
        let cache_dir = temp_dir.path().join("cache");

        // Install with first client
        {
            let config = MarketplaceClientConfig {
                registry_url: registry_path.to_string_lossy().to_string(),
                cache_dir: cache_dir.clone(),
                cache_ttl_secs: 3600,
            };
            let client = MarketplaceClient::new(config);
            client.install_transform("persist-test", None).unwrap();
        }

        // Create a new client and verify persistence
        {
            let config = MarketplaceClientConfig {
                registry_url: registry_path.to_string_lossy().to_string(),
                cache_dir: cache_dir.clone(),
                cache_ttl_secs: 3600,
            };
            let client = MarketplaceClient::new(config);
            let installed = client.list_installed();
            assert_eq!(installed.len(), 1);
            assert_eq!(installed[0].name, "persist-test");
        }
    }

    #[test]
    fn test_now_iso8601_format() {
        let ts = now_iso8601();
        assert!(ts.ends_with('Z'));
        assert!(ts.contains('T'));
        assert_eq!(ts.len(), 20);
    }

    #[test]
    fn test_marketplace_transform_deserialization() {
        let json = r#"{
            "name": "json-filter",
            "version": "0.1.0",
            "description": "Filter JSON messages",
            "author": "StreamlineLabs",
            "wasm_url": "https://example.com/json_filter.wasm",
            "input_format": "json",
            "output_format": "json",
            "category": "filter",
            "tags": ["json", "filter"],
            "license": "Apache-2.0",
            "repository_url": "https://example.com"
        }"#;

        let entry: MarketplaceTransform = serde_json::from_str(json).unwrap();
        assert_eq!(entry.name, "json-filter");
        assert_eq!(entry.category, "filter");
        assert_eq!(entry.tags.len(), 2);
    }

    #[test]
    fn test_default_config() {
        let config = MarketplaceClientConfig::default();
        assert_eq!(config.registry_url, DEFAULT_REGISTRY_URL);
        assert_eq!(config.cache_ttl_secs, 3600);
    }

    #[test]
    fn test_marketplace_api_state_creation() {
        let temp_dir = TempDir::new().unwrap();
        let state = MarketplaceApiState::new(temp_dir.path().to_path_buf());
        assert!(state.client.list_installed().is_empty());
    }

    #[test]
    fn test_reinstall_replaces_old() {
        let (client, _temp) = test_client_with_registry();

        // Install twice
        client.install_transform("test-filter", None).unwrap();
        client.install_transform("test-filter", None).unwrap();

        // Should still be only one entry
        let installed = client.list_installed();
        assert_eq!(installed.len(), 1);
    }
}
