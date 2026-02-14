//! Tenant lifecycle manager
//!
//! Central management of tenant creation, updates, deletion, and lookups.

use super::quota::{QuotaConfig, QuotaManager};
use super::types::{Tenant, TenantConfig, TenantId, TenantState};
use dashmap::DashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info};

/// Manages tenant lifecycle and lookups
pub struct TenantManager {
    /// Active tenants
    tenants: DashMap<TenantId, Tenant>,
    /// Quota manager
    quota_manager: Arc<QuotaManager>,
    /// Data directory for persistence
    data_dir: PathBuf,
    /// Whether multi-tenancy is enabled
    enabled: bool,
}

impl TenantManager {
    /// Create a new tenant manager
    pub fn new(data_dir: &Path, enabled: bool) -> Self {
        let quota_manager = Arc::new(QuotaManager::new(QuotaConfig::default()));

        Self {
            tenants: DashMap::new(),
            quota_manager,
            data_dir: data_dir.to_path_buf(),
            enabled,
        }
    }

    /// Check if multi-tenancy is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get the quota manager
    pub fn quota_manager(&self) -> &Arc<QuotaManager> {
        &self.quota_manager
    }

    /// Create a new tenant
    pub fn create_tenant(
        &self,
        id: impl Into<TenantId>,
        name: impl Into<String>,
        config: TenantConfig,
    ) -> Result<Tenant, String> {
        let id = id.into();

        if self.tenants.contains_key(&id) {
            return Err(format!("Tenant '{}' already exists", id));
        }

        // Validate tenant ID
        if id.is_empty() || id.len() > 64 {
            return Err("Tenant ID must be 1-64 characters".to_string());
        }
        if !id
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            return Err(
                "Tenant ID must contain only alphanumeric, hyphen, or underscore characters"
                    .to_string(),
            );
        }

        let tenant = Tenant::new(id.clone(), name, config);
        self.quota_manager.register_tenant(&id);
        self.tenants.insert(id.clone(), tenant.clone());

        info!(tenant_id = %id, "Tenant created");
        Ok(tenant)
    }

    /// Get a tenant by ID
    pub fn get_tenant(&self, id: &str) -> Option<Tenant> {
        self.tenants.get(id).map(|t| t.value().clone())
    }

    /// List all tenants
    pub fn list_tenants(&self) -> Vec<Tenant> {
        self.tenants.iter().map(|t| t.value().clone()).collect()
    }

    /// Update tenant state
    pub fn update_state(&self, id: &str, state: TenantState) -> Result<(), String> {
        let mut entry = self
            .tenants
            .get_mut(id)
            .ok_or_else(|| format!("Tenant '{}' not found", id))?;

        entry.state = state;
        entry.updated_at_ms = chrono::Utc::now().timestamp_millis();

        info!(tenant_id = %id, state = %state, "Tenant state updated");
        Ok(())
    }

    /// Update tenant config
    pub fn update_config(&self, id: &str, config: TenantConfig) -> Result<(), String> {
        let mut entry = self
            .tenants
            .get_mut(id)
            .ok_or_else(|| format!("Tenant '{}' not found", id))?;

        entry.config = config;
        entry.updated_at_ms = chrono::Utc::now().timestamp_millis();

        info!(tenant_id = %id, "Tenant config updated");
        Ok(())
    }

    /// Delete a tenant
    pub fn delete_tenant(&self, id: &str) -> Result<Tenant, String> {
        let (_, tenant) = self
            .tenants
            .remove(id)
            .ok_or_else(|| format!("Tenant '{}' not found", id))?;

        self.quota_manager.remove_tenant(id);
        info!(tenant_id = %id, "Tenant deleted");
        Ok(tenant)
    }

    /// Resolve tenant from a topic name (extract tenant prefix)
    pub fn resolve_tenant_from_topic(&self, topic: &str) -> Option<Tenant> {
        if !self.enabled {
            return None;
        }

        // Topic format: {tenant_id}.{topic_name}
        let dot_pos = topic.find('.')?;
        let tenant_id = &topic[..dot_pos];
        self.get_tenant(tenant_id)
    }

    /// Map an external topic to internal namespaced topic
    pub fn namespace_topic(&self, tenant_id: &str, topic: &str) -> Result<String, String> {
        if !self.enabled {
            return Ok(topic.to_string());
        }

        let tenant = self
            .get_tenant(tenant_id)
            .ok_or_else(|| format!("Tenant '{}' not found", tenant_id))?;

        if !tenant.is_active() {
            return Err(format!("Tenant '{}' is {}", tenant_id, tenant.state));
        }

        Ok(tenant.namespace_topic(topic))
    }

    /// Get tenant count
    pub fn tenant_count(&self) -> usize {
        self.tenants.len()
    }

    /// Save tenants to disk
    pub fn save(&self) -> Result<(), String> {
        let tenants_file = self.data_dir.join("tenants.json");
        let tenants: Vec<Tenant> = self.list_tenants();
        let json = serde_json::to_string_pretty(&tenants)
            .map_err(|e| format!("Serialization error: {}", e))?;

        std::fs::create_dir_all(&self.data_dir)
            .map_err(|e| format!("Failed to create dir: {}", e))?;
        std::fs::write(&tenants_file, json)
            .map_err(|e| format!("Failed to write tenants: {}", e))?;

        debug!("Saved {} tenants to disk", tenants.len());
        Ok(())
    }

    /// Load tenants from disk
    pub fn load(&self) -> Result<usize, String> {
        let tenants_file = self.data_dir.join("tenants.json");
        if !tenants_file.exists() {
            return Ok(0);
        }

        let json = std::fs::read_to_string(&tenants_file)
            .map_err(|e| format!("Failed to read tenants: {}", e))?;

        let tenants: Vec<Tenant> =
            serde_json::from_str(&json).map_err(|e| format!("Parse error: {}", e))?;

        let count = tenants.len();
        for tenant in tenants {
            self.quota_manager.register_tenant(&tenant.id);
            self.tenants.insert(tenant.id.clone(), tenant);
        }

        info!("Loaded {} tenants from disk", count);
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_manager() -> (TenantManager, TempDir) {
        let tmp = TempDir::new().unwrap();
        let mgr = TenantManager::new(tmp.path(), true);
        (mgr, tmp)
    }

    #[test]
    fn test_create_tenant() {
        let (mgr, _tmp) = create_test_manager();
        let tenant = mgr
            .create_tenant("acme", "ACME Corp", TenantConfig::default())
            .unwrap();

        assert_eq!(tenant.id, "acme");
        assert_eq!(tenant.name, "ACME Corp");
        assert!(tenant.is_active());
    }

    #[test]
    fn test_create_duplicate_tenant() {
        let (mgr, _tmp) = create_test_manager();
        mgr.create_tenant("acme", "ACME Corp", TenantConfig::default())
            .unwrap();
        let err = mgr
            .create_tenant("acme", "ACME Again", TenantConfig::default())
            .unwrap_err();
        assert!(err.contains("already exists"));
    }

    #[test]
    fn test_invalid_tenant_id() {
        let (mgr, _tmp) = create_test_manager();
        assert!(mgr
            .create_tenant("", "Empty", TenantConfig::default())
            .is_err());
        assert!(mgr
            .create_tenant("has spaces", "Bad", TenantConfig::default())
            .is_err());
        assert!(mgr
            .create_tenant("has.dots", "Bad", TenantConfig::default())
            .is_err());
    }

    #[test]
    fn test_get_tenant() {
        let (mgr, _tmp) = create_test_manager();
        mgr.create_tenant("t1", "Test", TenantConfig::default())
            .unwrap();

        assert!(mgr.get_tenant("t1").is_some());
        assert!(mgr.get_tenant("nonexistent").is_none());
    }

    #[test]
    fn test_list_tenants() {
        let (mgr, _tmp) = create_test_manager();
        mgr.create_tenant("t1", "Test 1", TenantConfig::default())
            .unwrap();
        mgr.create_tenant("t2", "Test 2", TenantConfig::default())
            .unwrap();

        assert_eq!(mgr.list_tenants().len(), 2);
    }

    #[test]
    fn test_update_state() {
        let (mgr, _tmp) = create_test_manager();
        mgr.create_tenant("t1", "Test", TenantConfig::default())
            .unwrap();

        mgr.update_state("t1", TenantState::Suspended).unwrap();
        let tenant = mgr.get_tenant("t1").unwrap();
        assert_eq!(tenant.state, TenantState::Suspended);
    }

    #[test]
    fn test_delete_tenant() {
        let (mgr, _tmp) = create_test_manager();
        mgr.create_tenant("t1", "Test", TenantConfig::default())
            .unwrap();
        mgr.delete_tenant("t1").unwrap();
        assert!(mgr.get_tenant("t1").is_none());
        assert_eq!(mgr.tenant_count(), 0);
    }

    #[test]
    fn test_namespace_topic() {
        let (mgr, _tmp) = create_test_manager();
        mgr.create_tenant("acme", "ACME", TenantConfig::default())
            .unwrap();

        let topic = mgr.namespace_topic("acme", "events").unwrap();
        assert_eq!(topic, "acme.events");
    }

    #[test]
    fn test_namespace_topic_suspended_tenant() {
        let (mgr, _tmp) = create_test_manager();
        mgr.create_tenant("acme", "ACME", TenantConfig::default())
            .unwrap();
        mgr.update_state("acme", TenantState::Suspended).unwrap();

        let err = mgr.namespace_topic("acme", "events").unwrap_err();
        assert!(err.contains("suspended"));
    }

    #[test]
    fn test_resolve_tenant_from_topic() {
        let (mgr, _tmp) = create_test_manager();
        mgr.create_tenant("acme", "ACME", TenantConfig::default())
            .unwrap();

        let tenant = mgr.resolve_tenant_from_topic("acme.events").unwrap();
        assert_eq!(tenant.id, "acme");
        assert!(mgr.resolve_tenant_from_topic("unknown.events").is_none());
    }

    #[test]
    fn test_save_and_load() {
        let tmp = TempDir::new().unwrap();

        // Create and save
        {
            let mgr = TenantManager::new(tmp.path(), true);
            mgr.create_tenant("t1", "Test 1", TenantConfig::default())
                .unwrap();
            mgr.create_tenant("t2", "Test 2", TenantConfig::default())
                .unwrap();
            mgr.save().unwrap();
        }

        // Load
        {
            let mgr = TenantManager::new(tmp.path(), true);
            let count = mgr.load().unwrap();
            assert_eq!(count, 2);
            assert!(mgr.get_tenant("t1").is_some());
            assert!(mgr.get_tenant("t2").is_some());
        }
    }

    #[test]
    fn test_disabled_manager() {
        let (_, tmp) = create_test_manager();
        let disabled_mgr = TenantManager::new(tmp.path(), false);

        // When disabled, namespace_topic returns the topic as-is
        let topic = disabled_mgr.namespace_topic("anyone", "events").unwrap();
        assert_eq!(topic, "events");
    }
}
