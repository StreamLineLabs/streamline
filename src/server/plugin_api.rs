//! Plugin Management API - REST endpoints for the plugin marketplace
//!
//! ## Endpoints
//!
//! - `GET /api/v1/plugins` - List all installed plugins
//! - `POST /api/v1/plugins` - Install a plugin from manifest
//! - `GET /api/v1/plugins/:name` - Get plugin details
//! - `DELETE /api/v1/plugins/:name` - Uninstall a plugin
//! - `PUT /api/v1/plugins/:name/enable` - Enable a plugin
//! - `PUT /api/v1/plugins/:name/disable` - Disable a plugin
//! - `PUT /api/v1/plugins/:name/config` - Update plugin configuration
//! - `GET /api/v1/plugins/registry/search` - Search remote registry
//! - `GET /api/v1/plugins/stats` - Get plugin system statistics

use crate::plugin::{
    InstalledPlugin, PluginManager, PluginManagerStats, PluginManifest, PluginRegistry,
};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Shared state for Plugin API
#[derive(Clone)]
pub struct PluginApiState {
    pub manager: Arc<PluginManager>,
}

impl PluginApiState {
    pub fn new(plugins_dir: std::path::PathBuf) -> Self {
        let registry = Arc::new(PluginRegistry::new());
        Self {
            manager: Arc::new(PluginManager::new(plugins_dir, registry)),
        }
    }
}

/// Error response
#[derive(Debug, Serialize)]
struct PluginErrorResponse {
    error: String,
}

/// List plugins response
#[derive(Debug, Serialize)]
struct ListPluginsResponse {
    plugins: Vec<InstalledPlugin>,
    total: usize,
}

/// Search query parameters
#[derive(Debug, Deserialize)]
pub struct SearchQuery {
    pub q: Option<String>,
    #[serde(rename = "type")]
    pub plugin_type: Option<String>,
    pub page: Option<usize>,
    pub per_page: Option<usize>,
}

/// Registry search result
#[derive(Debug, Serialize)]
struct RegistrySearchResponse {
    plugins: Vec<RegistryEntry>,
    total: usize,
    page: usize,
    per_page: usize,
}

/// A plugin entry in the remote registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryEntry {
    pub name: String,
    pub version: String,
    pub description: String,
    pub author: String,
    pub plugin_type: String,
    pub downloads: u64,
    pub homepage: Option<String>,
    pub repository: Option<String>,
}

/// Create the Plugin API router
pub fn create_plugin_api_router(state: PluginApiState) -> Router {
    Router::new()
        .route("/api/v1/plugins", get(list_plugins).post(install_plugin))
        .route("/api/v1/plugins/stats", get(get_plugin_stats))
        .route("/api/v1/plugins/registry/search", get(search_registry))
        .route(
            "/api/v1/plugins/{name}",
            get(get_plugin).delete(uninstall_plugin),
        )
        .route("/api/v1/plugins/{name}/enable", put(enable_plugin))
        .route("/api/v1/plugins/{name}/disable", put(disable_plugin))
        .route("/api/v1/plugins/{name}/config", put(configure_plugin))
        .with_state(state)
}

/// GET /api/v1/plugins - List all installed plugins
async fn list_plugins(
    State(state): State<PluginApiState>,
) -> Result<Json<ListPluginsResponse>, (StatusCode, Json<PluginErrorResponse>)> {
    let plugins = state.manager.list_installed();
    let total = plugins.len();
    Ok(Json(ListPluginsResponse { plugins, total }))
}

/// POST /api/v1/plugins - Install a plugin from manifest
async fn install_plugin(
    State(state): State<PluginApiState>,
    Json(manifest): Json<PluginManifest>,
) -> Result<(StatusCode, Json<InstalledPlugin>), (StatusCode, Json<PluginErrorResponse>)> {
    state.manager.install(manifest).map_or_else(
        |e| {
            Err((
                StatusCode::CONFLICT,
                Json(PluginErrorResponse {
                    error: e.to_string(),
                }),
            ))
        },
        |installed| Ok((StatusCode::CREATED, Json(installed))),
    )
}

/// GET /api/v1/plugins/:name - Get plugin details
async fn get_plugin(
    State(state): State<PluginApiState>,
    Path(name): Path<String>,
) -> Result<Json<InstalledPlugin>, (StatusCode, Json<PluginErrorResponse>)> {
    state
        .manager
        .get_installed(&name)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(PluginErrorResponse {
                    error: format!("Plugin '{}' not found", name),
                }),
            )
        })
        .map(Json)
}

/// DELETE /api/v1/plugins/:name - Uninstall a plugin
async fn uninstall_plugin(
    State(state): State<PluginApiState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<PluginErrorResponse>)> {
    state.manager.uninstall(&name).map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(PluginErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    Ok(StatusCode::NO_CONTENT)
}

/// PUT /api/v1/plugins/:name/enable - Enable a plugin
async fn enable_plugin(
    State(state): State<PluginApiState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<PluginErrorResponse>)> {
    state.manager.enable(&name).map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(PluginErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    Ok(StatusCode::OK)
}

/// PUT /api/v1/plugins/:name/disable - Disable a plugin
async fn disable_plugin(
    State(state): State<PluginApiState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<PluginErrorResponse>)> {
    state.manager.disable(&name).map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(PluginErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    Ok(StatusCode::OK)
}

/// PUT /api/v1/plugins/:name/config - Update plugin configuration
async fn configure_plugin(
    State(state): State<PluginApiState>,
    Path(name): Path<String>,
    Json(config): Json<HashMap<String, serde_json::Value>>,
) -> Result<StatusCode, (StatusCode, Json<PluginErrorResponse>)> {
    state.manager.configure(&name, config).map_err(|e| {
        (
            StatusCode::NOT_FOUND,
            Json(PluginErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    Ok(StatusCode::OK)
}

/// GET /api/v1/plugins/stats - Get plugin system statistics
async fn get_plugin_stats(State(state): State<PluginApiState>) -> Json<PluginManagerStats> {
    Json(state.manager.stats())
}

/// GET /api/v1/plugins/registry/search - Search the remote registry
async fn search_registry(Query(query): Query<SearchQuery>) -> Json<RegistrySearchResponse> {
    // Built-in registry of first-party plugins
    let builtin_plugins = vec![
        RegistryEntry {
            name: "streamline-sink-s3".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            description: "Sink connector for Amazon S3".to_string(),
            author: "Streamline".to_string(),
            plugin_type: "sink".to_string(),
            downloads: 0,
            homepage: None,
            repository: Some("https://github.com/streamline-dev/plugins".to_string()),
        },
        RegistryEntry {
            name: "streamline-sink-elasticsearch".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            description: "Sink connector for Elasticsearch".to_string(),
            author: "Streamline".to_string(),
            plugin_type: "sink".to_string(),
            downloads: 0,
            homepage: None,
            repository: Some("https://github.com/streamline-dev/plugins".to_string()),
        },
        RegistryEntry {
            name: "streamline-transform-json".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            description: "JSON message transformation plugin".to_string(),
            author: "Streamline".to_string(),
            plugin_type: "transform".to_string(),
            downloads: 0,
            homepage: None,
            repository: Some("https://github.com/streamline-dev/plugins".to_string()),
        },
        RegistryEntry {
            name: "streamline-source-http".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            description: "HTTP webhook source connector".to_string(),
            author: "Streamline".to_string(),
            plugin_type: "source".to_string(),
            downloads: 0,
            homepage: None,
            repository: Some("https://github.com/streamline-dev/plugins".to_string()),
        },
        RegistryEntry {
            name: "streamline-auth-ldap".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            description: "LDAP authentication provider".to_string(),
            author: "Streamline".to_string(),
            plugin_type: "auth".to_string(),
            downloads: 0,
            homepage: None,
            repository: Some("https://github.com/streamline-dev/plugins".to_string()),
        },
    ];

    let page = query.page.unwrap_or(1);
    let per_page = query.per_page.unwrap_or(20).min(100);

    let filtered: Vec<RegistryEntry> = builtin_plugins
        .into_iter()
        .filter(|p| {
            if let Some(ref q) = query.q {
                let q_lower = q.to_lowercase();
                p.name.to_lowercase().contains(&q_lower)
                    || p.description.to_lowercase().contains(&q_lower)
            } else {
                true
            }
        })
        .filter(|p| {
            if let Some(ref pt) = query.plugin_type {
                p.plugin_type == *pt
            } else {
                true
            }
        })
        .collect();

    let total = filtered.len();
    let start = (page - 1) * per_page;
    let plugins = filtered.into_iter().skip(start).take(per_page).collect();

    Json(RegistrySearchResponse {
        plugins,
        total,
        page,
        per_page,
    })
}
