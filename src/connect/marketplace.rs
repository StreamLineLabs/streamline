//! WASM Transform Marketplace Client
//!
//! # Status: the marketplace is NOT implemented in this build
//!
//! There is no registry client, no downloader and no signature/digest
//! verification here. Every operation that would require one fails closed with
//! [`MarketplaceClientError::Unsupported`], which the HTTP layer maps to
//! `501 Not Implemented`. Nothing is fetched, written or executed as a result
//! of a marketplace request.
//!
//! What does work:
//!
//! - reading a registry index from a **local JSON file**, when an embedder
//!   constructs [`MarketplaceClientConfig`] with `registry_url` set to that
//!   path, and
//! - registering a WASM module an embedder has already placed on disk, via
//!   [`MarketplaceClient::register_local_transform`], which makes no integrity
//!   claim it cannot back up.
//!
//! ## HTTP API Endpoints (behind `wasm-transforms` feature)
//!
//! The HTTP surface always builds its client with the built-in remote registry
//! URL ([`DEFAULT_REGISTRY_URL`]) and exposes no setting to point it at a local
//! file, so **the HTTP marketplace is unsupported end to end**:
//!
//! - `GET /api/v1/marketplace/transforms` — `501 Not Implemented` (the remote
//!   registry cannot be fetched).
//! - `POST /api/v1/marketplace/transforms/{name}/install` — `501 Not
//!   Implemented`.
//! - `GET /api/v1/marketplace/transforms/installed` — works; lists what an
//!   embedder registered locally (usually empty).
//!
//! Adding a configuration knob or a "register a local module" endpoint is
//! deliberately out of scope: exposing third-party module loading over HTTP
//! needs a trust model (published digests, signature verification, size and
//! path limits) that must be designed rather than bolted on.

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
    /// SHA-256 hash of the WASM module, when one has actually been computed and
    /// verified.
    ///
    /// This is `None` for modules that were registered from local disk: this
    /// build has no SHA-256 implementation available unconditionally, and
    /// reporting a non-cryptographic digest (a CRC32 was previously written
    /// into this field) under the name `sha256` misrepresents the integrity
    /// guarantee. See [`InstalledMarketplaceTransform::verified`].
    #[serde(default)]
    pub sha256: Option<String>,
    /// Whether the module's integrity was cryptographically verified against a
    /// digest published by the registry. Always `false` today.
    #[serde(default)]
    pub verified: bool,
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

/// Errors produced by [`MarketplaceClient`].
///
/// The marketplace deliberately fails closed: operations that cannot be
/// performed safely return [`MarketplaceClientError::Unsupported`] rather than
/// fabricating a successful result.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MarketplaceClientError {
    /// The operation is not implemented in this build.
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// The registry could not be read or parsed.
    #[error("registry error: {0}")]
    Registry(String),
    /// The requested transform is not in the registry.
    #[error("transform not found: {0}")]
    NotFound(String),
    /// A filesystem operation failed.
    #[error("io error: {0}")]
    Io(String),
    /// The module on disk is not a usable WASM module.
    #[error("integrity error: {0}")]
    Integrity(String),
}

impl MarketplaceClientError {
    /// HTTP status that best represents this error.
    fn status_code(&self) -> StatusCode {
        match self {
            // 501: the server understands the request but has not implemented it.
            Self::Unsupported(_) => StatusCode::NOT_IMPLEMENTED,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            // 503: the registry is a dependency we could not consult.
            Self::Registry(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Io(_) | Self::Integrity(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn into_response_pair(self) -> (StatusCode, Json<MarketplaceError>) {
        let status = self.status_code();
        (
            status,
            Json(MarketplaceError {
                error: self.to_string(),
                code: status.as_u16(),
            }),
        )
    }
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
    ///
    /// Returns an error rather than an empty list when the registry cannot be
    /// consulted: "the registry is unreachable" and "the registry is empty" are
    /// very different answers, and conflating them made a broken configuration
    /// look like an empty marketplace.
    pub fn get_registry(&self) -> Result<Vec<MarketplaceTransform>, MarketplaceClientError> {
        // Check cache
        {
            let cache = self.registry_cache.read();
            if let Some(ref cached) = *cache {
                if cached.fetched_at.elapsed().as_secs() < self.config.cache_ttl_secs {
                    return Ok(cached.entries.clone());
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
                Ok(entries)
            }
            Err(e) => {
                warn!("Failed to fetch marketplace registry: {}", e);
                // Serving a stale cache is acceptable; silently serving an empty
                // list is not.
                let cache = self.registry_cache.read();
                match cache.as_ref() {
                    Some(c) => Ok(c.entries.clone()),
                    None => Err(e),
                }
            }
        }
    }

    /// Fetch the registry from the configured URL.
    fn fetch_registry(&self) -> Result<Vec<MarketplaceTransform>, MarketplaceClientError> {
        // A local file path is the supported configuration.
        let path = PathBuf::from(&self.config.registry_url);
        if path.exists() {
            let content = std::fs::read_to_string(&path)
                .map_err(|e| MarketplaceClientError::Io(format!("read registry file: {e}")))?;
            let entries: Vec<MarketplaceTransform> = serde_json::from_str(&content)
                .map_err(|e| MarketplaceClientError::Registry(format!("parse registry: {e}")))?;
            info!("Loaded {} transforms from local registry", entries.len());
            return Ok(entries);
        }

        debug!(
            "Marketplace registry is not a local file: {}",
            self.config.registry_url
        );

        // Remote registry fetching is NOT implemented. It previously returned an
        // error that callers swallowed into an empty list. Fetching a registry
        // over HTTP requires TLS verification, response size limits, and a
        // published-digest trust model that this build does not have, so the
        // operation is refused rather than partially implemented.
        //
        // The message deliberately does not tell the reader to "configure a
        // local registry file": the HTTP server always constructs this client
        // with the built-in remote URL (see `MarketplaceApiState::new`) and
        // exposes no setting to change it, so that advice would point at a knob
        // the reader does not have.
        Err(MarketplaceClientError::Unsupported(format!(
            "the remote WASM transform marketplace is not implemented in this \
             build: '{}' is not a local path and no registry client exists to \
             fetch it",
            self.config.registry_url
        )))
    }

    /// Search the registry by query string.
    pub fn search(
        &self,
        query: &str,
        category: Option<&str>,
    ) -> Result<Vec<MarketplaceTransform>, MarketplaceClientError> {
        let registry = self.get_registry()?;
        let query_lower = query.to_lowercase();

        Ok(registry
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
            .collect())
    }

    /// Look up a registry entry by name and optional version.
    fn find_entry(
        &self,
        name: &str,
        version: Option<&str>,
    ) -> Result<MarketplaceTransform, MarketplaceClientError> {
        let registry = self.get_registry()?;
        registry
            .into_iter()
            .find(|e| e.name == name && version.map(|v| e.version == v).unwrap_or(true))
            .ok_or_else(|| {
                MarketplaceClientError::NotFound(format!(
                    "'{name}' is not in the marketplace registry"
                ))
            })
    }

    /// Directory a transform's module is expected to live in.
    fn install_dir(&self, name: &str, version: &str) -> PathBuf {
        self.config.cache_dir.join(name).join(version)
    }

    /// Conventional module file name for a transform.
    fn wasm_file_name(name: &str) -> String {
        format!("{}.wasm", name.replace('-', "_"))
    }

    /// Install a transform by name and optional version.
    ///
    /// **Not implemented.** Installing means downloading a third-party binary
    /// and executing it inside the broker, which requires TLS-verified
    /// downloads and verification against a digest published by the registry.
    /// None of that exists in this build.
    ///
    /// The previous implementation wrote a *text placeholder* to
    /// `<name>.wasm`, hashed it with CRC32, stored the CRC in a field named
    /// `sha256`, and reported "installed successfully". Operators had no way to
    /// tell that nothing had been installed and that no integrity check had
    /// been performed.
    ///
    /// Embedders that construct a [`MarketplaceClient`] themselves can place a
    /// module on disk and call
    /// [`MarketplaceClient::register_local_transform`]. That path is *not*
    /// reachable through the HTTP API, so the error returned here does not
    /// suggest it.
    pub fn install_transform(
        &self,
        name: &str,
        version: Option<&str>,
    ) -> Result<InstalledMarketplaceTransform, MarketplaceClientError> {
        // Resolve first so the caller gets "not found" rather than a confusing
        // "unsupported" for a transform that does not exist at all.
        let entry = self.find_entry(name, version)?;

        Err(MarketplaceClientError::Unsupported(format!(
            "installing marketplace transforms is not implemented in this build, \
             so '{}' v{} was not installed: there is no verified download path \
             for {} and no publisher digest to check it against",
            entry.name, entry.version, entry.wasm_url,
        )))
    }

    /// Register a WASM module that an operator has already placed in the cache
    /// directory.
    ///
    /// This is the supported installation path. It performs the checks it can
    /// actually perform — the file exists, is non-empty, and starts with the
    /// WASM magic number — and makes no integrity claim it cannot back up: the
    /// returned record has `sha256: None` and `verified: false`.
    pub fn register_local_transform(
        &self,
        name: &str,
        version: Option<&str>,
    ) -> Result<InstalledMarketplaceTransform, MarketplaceClientError> {
        let entry = self.find_entry(name, version)?;

        let wasm_path = self
            .install_dir(&entry.name, &entry.version)
            .join(Self::wasm_file_name(&entry.name));

        let wasm_bytes = std::fs::read(&wasm_path).map_err(|e| {
            MarketplaceClientError::Io(format!(
                "no module at {}: {e}. Place the verified .wasm module there first",
                wasm_path.display()
            ))
        })?;

        // WASM binaries start with the four-byte magic number `\0asm`. This
        // rejects the text placeholders the old install path used to write.
        if !wasm_bytes.starts_with(b"\0asm") {
            return Err(MarketplaceClientError::Integrity(format!(
                "{} is not a WebAssembly module (missing \\0asm magic number)",
                wasm_path.display()
            )));
        }

        // Persist the registry metadata alongside the module.
        let metadata_path = self
            .install_dir(&entry.name, &entry.version)
            .join("metadata.json");
        let metadata_json = serde_json::to_string_pretty(&entry)
            .map_err(|e| MarketplaceClientError::Io(format!("serialize metadata: {e}")))?;
        std::fs::write(&metadata_path, metadata_json)
            .map_err(|e| MarketplaceClientError::Io(format!("write metadata: {e}")))?;

        let installed = InstalledMarketplaceTransform {
            name: entry.name.clone(),
            version: entry.version.clone(),
            wasm_path: wasm_path.to_string_lossy().to_string(),
            // No cryptographic digest is computed, so none is reported.
            sha256: None,
            verified: false,
            installed_at: now_iso8601(),
            source_url: entry.wasm_url.clone(),
            category: entry.category.clone(),
            description: entry.description.clone(),
        };

        {
            let mut list = self.installed.write();
            list.retain(|i| i.name != entry.name);
            list.push(installed.clone());
        }

        self.save_installed_to_disk();

        info!(
            "Registered local marketplace transform: {} v{} at {} (unverified)",
            installed.name, installed.version, installed.wasm_path
        );

        Ok(installed)
    }

    /// List all installed marketplace transforms.
    pub fn list_installed(&self) -> Vec<InstalledMarketplaceTransform> {
        self.installed.read().clone()
    }

    /// Get an installed transform by name.
    pub fn get_installed(&self, name: &str) -> Option<InstalledMarketplaceTransform> {
        self.installed
            .read()
            .iter()
            .find(|i| i.name == name)
            .cloned()
    }

    /// Load installed transforms from the manifest file on disk.
    fn load_installed_from_disk(&self) {
        let manifest_path = self.config.cache_dir.join("installed.json");
        if !manifest_path.exists() {
            return;
        }
        match std::fs::read_to_string(&manifest_path) {
            Ok(content) => {
                if let Ok(list) =
                    serde_json::from_str::<Vec<InstalledMarketplaceTransform>>(&content)
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

    format!("{year:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

// NOTE: a `crc32_hash` helper used to live here and its output was written into
// the `sha256` field of `InstalledMarketplaceTransform`. CRC32 is a
// non-cryptographic error-detection code, not a digest: it is trivially
// forgeable and 32 bits wide. Presenting it as a SHA-256 gave a false integrity
// signal, so both the helper and the field's unconditional population were
// removed. If a real digest is needed, add a cryptographic hash implementation
// and populate `sha256` only from that.
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
    ///
    /// The registry URL is always [`DEFAULT_REGISTRY_URL`]; there is
    /// intentionally no way to override it from here, because the code that
    /// would fetch it does not exist. The endpoints therefore fail closed with
    /// `501 Not Implemented` rather than pretending the marketplace is one
    /// configuration change away from working.
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
///
/// Returns `501 Not Implemented` in this build: the state below always points
/// the client at the remote [`DEFAULT_REGISTRY_URL`], which cannot be fetched.
async fn list_available_transforms(
    State(state): State<MarketplaceApiState>,
) -> Result<Json<ListAvailableResponse>, (StatusCode, Json<MarketplaceError>)> {
    // Surface registry failures instead of returning `{"transforms": [], ...}`,
    // which made an unreachable registry indistinguishable from an empty one.
    let transforms = state
        .client
        .get_registry()
        .map_err(MarketplaceClientError::into_response_pair)?;
    let total = transforms.len();
    Ok(Json(ListAvailableResponse { transforms, total }))
}

/// POST /api/v1/marketplace/transforms/{name}/install - Install a transform.
///
/// Returns `501 Not Implemented`: this build cannot download and verify
/// third-party WASM modules. It previously returned `200 OK` with
/// `"success": true` after writing a text placeholder to disk.
async fn install_transform(
    State(state): State<MarketplaceApiState>,
    Path(name): Path<String>,
    body: Option<Json<InstallRequest>>,
) -> Result<(StatusCode, Json<InstallResponse>), (StatusCode, Json<MarketplaceError>)> {
    let version = body.as_ref().and_then(|b| b.version.as_deref());

    let installed = state
        .client
        .install_transform(&name, version)
        .map_err(MarketplaceClientError::into_response_pair)?;

    Ok((
        StatusCode::OK,
        Json(InstallResponse {
            success: true,
            message: format!(
                "Transform '{}' v{} installed successfully",
                installed.name, installed.version
            ),
            transform: installed,
        }),
    ))
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
            "/api/v1/marketplace/transforms/:name/install",
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

    /// Minimal valid WASM module header (magic + version).
    const WASM_HEADER: &[u8] = b"\0asm\x01\x00\x00\x00";

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

    /// Place a real WASM module where `register_local_transform` expects it.
    fn place_module(client: &MarketplaceClient, name: &str, version: &str, bytes: &[u8]) {
        let dir = client.install_dir(name, version);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(MarketplaceClient::wasm_file_name(name)), bytes).unwrap();
    }

    #[test]
    fn test_marketplace_client_creation() {
        let (_client, _temp) = test_client();
    }

    #[test]
    fn test_get_registry_from_file() {
        let (client, _temp) = test_client_with_registry();
        let registry = client.get_registry().unwrap();
        assert_eq!(registry.len(), 2);
        assert_eq!(registry[0].name, "test-filter");
        assert_eq!(registry[1].name, "test-enricher");
    }

    /// Regression: an unreachable registry must surface an error rather than
    /// masquerading as an empty marketplace.
    #[test]
    fn test_unreachable_registry_is_an_error_not_an_empty_list() {
        let (client, _temp) = test_client();
        let err = client.get_registry().unwrap_err();
        assert!(
            matches!(err, MarketplaceClientError::Unsupported(_)),
            "expected Unsupported, got {err:?}"
        );
        assert!(err.to_string().contains("not implemented"));
    }

    /// Regression: HTTP registry fetching is not implemented and must say so —
    /// without telling the reader to point `registry_url` at a local file. The
    /// HTTP surface (`MarketplaceApiState`) offers no such setting, so that
    /// advice would send operators after a knob that does not exist.
    #[test]
    fn test_http_registry_url_reports_unsupported() {
        let temp_dir = TempDir::new().unwrap();
        let client = MarketplaceClient::new(MarketplaceClientConfig {
            registry_url: DEFAULT_REGISTRY_URL.to_string(),
            cache_dir: temp_dir.path().to_path_buf(),
            cache_ttl_secs: 3600,
        });
        let err = client.get_registry().unwrap_err();
        assert_eq!(err.status_code(), StatusCode::NOT_IMPLEMENTED);

        let msg = err.to_string();
        assert!(msg.contains("not implemented"), "{msg}");
        assert!(
            !msg.to_lowercase().contains("configure"),
            "the 501 must not instruct operators to configure an unavailable \
             registry path: {msg}"
        );
    }

    /// The state used by the HTTP router is hard-wired to the remote registry,
    /// so every listing request fails closed with 501.
    #[test]
    fn test_api_state_registry_is_unsupported() {
        let temp_dir = TempDir::new().unwrap();
        let state = MarketplaceApiState::new(temp_dir.path().to_path_buf());
        let err = state.client.get_registry().unwrap_err();
        assert_eq!(err.status_code(), StatusCode::NOT_IMPLEMENTED);
    }

    #[test]
    fn test_search_by_name() {
        let (client, _temp) = test_client_with_registry();
        let results = client.search("filter", None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "test-filter");
    }

    #[test]
    fn test_search_by_description() {
        let (client, _temp) = test_client_with_registry();
        let results = client.search("enrichment", None).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "test-enricher");
    }

    #[test]
    fn test_search_by_category() {
        let (client, _temp) = test_client_with_registry();
        let results = client.search("test", Some("enrich")).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name, "test-enricher");
    }

    #[test]
    fn test_search_no_results() {
        let (client, _temp) = test_client_with_registry();
        let results = client.search("nonexistent", None).unwrap();
        assert!(results.is_empty());
    }

    /// Regression: installing must fail closed. It used to write a text
    /// placeholder named `*.wasm`, hash it with CRC32, store that in `sha256`
    /// and report success.
    #[test]
    fn test_install_transform_is_unsupported_and_writes_nothing() {
        let (client, _temp) = test_client_with_registry();
        let err = client.install_transform("test-filter", None).unwrap_err();

        assert!(
            matches!(err, MarketplaceClientError::Unsupported(_)),
            "expected Unsupported, got {err:?}"
        );
        assert_eq!(err.status_code(), StatusCode::NOT_IMPLEMENTED);

        // The 501 must not hand HTTP callers an "install it yourself" recipe:
        // `register_local_transform` is an embedder API with no HTTP route.
        let msg = err.to_string();
        assert!(msg.contains("not implemented"), "{msg}");
        assert!(!msg.contains("register_local_transform"), "{msg}");

        // Nothing may be created on disk, and nothing may be registered.
        let wasm_path = client
            .install_dir("test-filter", "0.1.0")
            .join(MarketplaceClient::wasm_file_name("test-filter"));
        assert!(
            !wasm_path.exists(),
            "install must not fabricate {}",
            wasm_path.display()
        );
        assert!(client.list_installed().is_empty());
    }

    #[test]
    fn test_install_transform_not_found() {
        let (client, _temp) = test_client_with_registry();
        let err = client.install_transform("nonexistent", None).unwrap_err();
        assert!(matches!(err, MarketplaceClientError::NotFound(_)));
        assert_eq!(err.status_code(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_register_local_transform_requires_the_module_on_disk() {
        let (client, _temp) = test_client_with_registry();
        let err = client
            .register_local_transform("test-filter", None)
            .unwrap_err();
        assert!(matches!(err, MarketplaceClientError::Io(_)), "{err:?}");
    }

    /// Regression: a text placeholder must be rejected, not accepted as a
    /// module.
    #[test]
    fn test_register_local_transform_rejects_non_wasm_content() {
        let (client, _temp) = test_client_with_registry();
        place_module(
            &client,
            "test-filter",
            "0.1.0",
            b"# WASM module placeholder\n# Download from: https://example.com\n",
        );

        let err = client
            .register_local_transform("test-filter", None)
            .unwrap_err();
        assert!(
            matches!(err, MarketplaceClientError::Integrity(_)),
            "expected Integrity, got {err:?}"
        );
        assert!(client.list_installed().is_empty());
    }

    #[test]
    fn test_register_local_transform_accepts_a_real_module() {
        let (client, _temp) = test_client_with_registry();
        place_module(&client, "test-filter", "0.1.0", WASM_HEADER);

        let installed = client
            .register_local_transform("test-filter", None)
            .unwrap();
        assert_eq!(installed.name, "test-filter");
        assert_eq!(installed.version, "0.1.0");
        assert!(installed.wasm_path.contains("test_filter.wasm"));

        // No integrity claim may be made without a real digest.
        assert!(
            installed.sha256.is_none(),
            "no digest is computed, so none may be reported"
        );
        assert!(!installed.verified);
    }

    #[test]
    fn test_list_installed() {
        let (client, _temp) = test_client_with_registry();

        // Initially empty
        assert!(client.list_installed().is_empty());

        place_module(&client, "test-filter", "0.1.0", WASM_HEADER);
        client
            .register_local_transform("test-filter", None)
            .unwrap();

        let installed = client.list_installed();
        assert_eq!(installed.len(), 1);
        assert_eq!(installed[0].name, "test-filter");
    }

    #[test]
    fn test_get_installed() {
        let (client, _temp) = test_client_with_registry();
        place_module(&client, "test-filter", "0.1.0", WASM_HEADER);
        client
            .register_local_transform("test-filter", None)
            .unwrap();

        assert!(client.get_installed("test-filter").is_some());
        assert!(client.get_installed("nonexistent").is_none());
    }

    #[test]
    fn test_registration_persists_to_disk() {
        let temp_dir = TempDir::new().unwrap();

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

        let config = || MarketplaceClientConfig {
            registry_url: registry_path.to_string_lossy().to_string(),
            cache_dir: cache_dir.clone(),
            cache_ttl_secs: 3600,
        };

        {
            let client = MarketplaceClient::new(config());
            place_module(&client, "persist-test", "0.1.0", WASM_HEADER);
            client
                .register_local_transform("persist-test", None)
                .unwrap();
        }

        {
            let client = MarketplaceClient::new(config());
            let installed = client.list_installed();
            assert_eq!(installed.len(), 1);
            assert_eq!(installed[0].name, "persist-test");
            assert!(installed[0].sha256.is_none());
        }
    }

    #[test]
    fn test_reregistration_replaces_old() {
        let (client, _temp) = test_client_with_registry();
        place_module(&client, "test-filter", "0.1.0", WASM_HEADER);

        client
            .register_local_transform("test-filter", None)
            .unwrap();
        client
            .register_local_transform("test-filter", None)
            .unwrap();

        assert_eq!(client.list_installed().len(), 1);
    }

    /// Records persisted before `sha256` became optional must still load.
    #[test]
    fn test_installed_record_deserializes_legacy_manifest() {
        let legacy = r#"[{
            "name": "legacy",
            "version": "0.1.0",
            "wasm_path": "/tmp/legacy.wasm",
            "installed_at": "2026-01-01T00:00:00Z",
            "source_url": "https://example.com/legacy.wasm",
            "category": "filter",
            "description": "legacy record without sha256"
        }]"#;
        let parsed: Vec<InstalledMarketplaceTransform> = serde_json::from_str(legacy).unwrap();
        assert_eq!(parsed.len(), 1);
        assert!(parsed[0].sha256.is_none());
        assert!(!parsed[0].verified);
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

    /// Router-level regression tests.
    ///
    /// The client-level tests above prove `install_transform` fails closed, but
    /// the false-success bug was observable at the *HTTP* boundary: the route
    /// answered `200 OK` with `{"success": true, "message": "... installed
    /// successfully"}` after writing a text placeholder to disk. These tests
    /// drive the real router so a future refactor cannot reintroduce a success
    /// response without failing here.
    mod http_routes {
        use super::*;
        use axum::body::Body;
        use axum::http::{Request, StatusCode};
        use http_body_util::BodyExt;
        use tower::ServiceExt;

        /// Build the router exactly as the server does, over an empty cache dir.
        fn router(temp: &TempDir) -> Router {
            create_marketplace_router(MarketplaceApiState::new(temp.path().to_path_buf()))
        }

        async fn call(app: Router, request: Request<Body>) -> (StatusCode, String) {
            let response = app.oneshot(request).await.expect("router call");
            let status = response.status();
            let bytes = response
                .into_body()
                .collect()
                .await
                .expect("collect body")
                .to_bytes();
            (status, String::from_utf8_lossy(&bytes).into_owned())
        }

        #[tokio::test]
        async fn install_route_reports_not_implemented_and_never_success() {
            let temp = TempDir::new().unwrap();
            let request = Request::builder()
                .method("POST")
                .uri("/api/v1/marketplace/transforms/json-filter/install")
                .body(Body::empty())
                .unwrap();

            let (status, body) = call(router(&temp), request).await;

            assert_eq!(
                status,
                StatusCode::NOT_IMPLEMENTED,
                "install must fail closed, got {status}: {body}"
            );
            assert!(
                !body.contains("\"success\""),
                "the install response must not carry a success flag: {body}"
            );
            assert!(
                !body.contains("installed successfully"),
                "the install response must not claim an installation happened: {body}"
            );

            // Nothing may be fabricated on disk: no placeholder module, no
            // installed-transform manifest.
            let mut created = Vec::new();
            let mut stack = vec![temp.path().to_path_buf()];
            while let Some(dir) = stack.pop() {
                for entry in std::fs::read_dir(&dir).expect("read cache dir") {
                    let path = entry.expect("dir entry").path();
                    if path.is_dir() {
                        stack.push(path);
                    } else {
                        created.push(path);
                    }
                }
            }
            assert!(
                created.is_empty(),
                "a failed install must not write anything, found {created:?}"
            );
        }

        #[tokio::test]
        async fn list_available_route_reports_not_implemented_not_an_empty_catalogue() {
            let temp = TempDir::new().unwrap();
            let request = Request::builder()
                .uri("/api/v1/marketplace/transforms")
                .body(Body::empty())
                .unwrap();

            let (status, body) = call(router(&temp), request).await;

            assert_eq!(
                status,
                StatusCode::NOT_IMPLEMENTED,
                "an unreachable registry must not look like an empty one: {body}"
            );
            assert!(
                !body.contains("\"transforms\""),
                "the error response must not be shaped like a catalogue: {body}"
            );
        }

        /// Listing what is installed locally needs no registry, so it keeps
        /// working — and truthfully reports nothing installed.
        #[tokio::test]
        async fn installed_route_still_answers() {
            let temp = TempDir::new().unwrap();
            let request = Request::builder()
                .uri("/api/v1/marketplace/transforms/installed")
                .body(Body::empty())
                .unwrap();

            let (status, body) = call(router(&temp), request).await;

            assert_eq!(status, StatusCode::OK, "{body}");
            assert!(body.contains("\"total\":0"), "{body}");
        }
    }
}
